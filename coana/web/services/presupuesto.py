"""Lectura, resumen y búsqueda de UC del traductor de presupuesto.

Las UC vienen del parquet ``data/fase1/uc presupuesto.parquet``. Los
detalles de un registro se enriquecen con las descripciones del árbol
de elementos de coste, centros de coste y actividades de
``data/fase1/*.tree``.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.util import Árbol
from coana.web.deps import DIR_AUX, DIR_FASE1, _mtime_ns, read_parquet
from coana.web.schemas.common import (
    ColumnSpec,
    FieldValue,
    Kpi,
    KpiPanel,
    ListResponse,
    RecordResponse,
    RecordSection,
)
from coana.web.services.query import QueryParams, apply_query

PATH_UC = DIR_FASE1 / "uc presupuesto.parquet"
PATH_SIN_UC = DIR_FASE1 / "presupuesto sin uc.parquet"
PATH_FILTRADOS = DIR_AUX / "filtrados_presupuesto.parquet"
PATH_SIN_CLASIFICAR = DIR_AUX / "sin_clasificar_presupuesto.parquet"
PATH_UC_SUMIN = DIR_FASE1 / "uc suministros.parquet"
PATH_REGLAS_ACT = DIR_AUX / "conteo_reglas_presupuesto.parquet"
PATH_REGLAS_CC = DIR_AUX / "conteo_cc_presupuesto.parquet"
PATH_REGLAS_EC = DIR_AUX / "conteo_ec_presupuesto.parquet"


# Columnas que se muestran en la tabla principal y su formato.
_COLUMNS: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID", format="text"),
    ColumnSpec(name="elemento_de_coste", label="Elemento de coste", format="text"),
    ColumnSpec(name="centro_de_coste", label="Centro de coste", format="text"),
    ColumnSpec(name="actividad", label="Actividad", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="origen", label="Origen", format="text"),
    ColumnSpec(name="origen_id", label="Origen ID", format="text"),
    ColumnSpec(name="origen_porción", label="Porción", format="float"),
    ColumnSpec(name="regla_actividad", label="Regla actividad", format="text"),
    ColumnSpec(name="regla_cc", label="Regla CC", format="text"),
    ColumnSpec(name="regla_ec", label="Regla EC", format="text"),
]
_COLUMN_NAMES = [c.name for c in _COLUMNS]
_SEARCH_COLUMNS = [
    "id", "elemento_de_coste", "centro_de_coste", "actividad",
    "origen", "origen_id", "regla_actividad", "regla_cc", "regla_ec",
]


@lru_cache(maxsize=8)
def _arbol(path: str, mtime_ns: int) -> Árbol | None:
    """Carga un árbol .tree con caché por mtime."""
    del mtime_ns
    p = Path(path)
    if not p.exists():
        return None
    return Árbol.from_file(p)


def _arbol_cached(name: str) -> Árbol | None:
    p = DIR_FASE1 / f"{name}.tree"
    return _arbol(str(p), _mtime_ns(p))


def _descripcion_nodo(árbol_name: str, identificador: str | None) -> str | None:
    if not identificador:
        return None
    arbol = _arbol_cached(árbol_name)
    if arbol is None:
        return None
    nodo = arbol._por_id.get(identificador)
    if nodo is None:
        return None
    return f"{nodo.código} · {nodo.descripción}"


def _safe_read_parquet(path: Path) -> pl.DataFrame | None:
    try:
        return read_parquet(path)
    except FileNotFoundError:
        return None


def resumen() -> KpiPanel:
    """Métricas globales de presupuesto."""
    uc = _safe_read_parquet(PATH_UC)
    sin_uc = _safe_read_parquet(PATH_SIN_UC)
    filtrados = _safe_read_parquet(PATH_FILTRADOS)

    n_uc = 0 if uc is None else uc.height
    importe_uc = 0.0 if uc is None else float(uc["importe"].sum() or 0)
    n_sin_uc = 0 if sin_uc is None else sin_uc.height
    importe_sin_uc = 0.0 if sin_uc is None else float(sin_uc["importe"].cast(pl.Float64).sum() or 0)
    n_filtrados = 0 if filtrados is None else filtrados.height
    importe_filtrados = 0.0 if filtrados is None else float(filtrados["importe"].sum() or 0)

    return KpiPanel(kpis=[
        Kpi(label="UC generadas", value=n_uc, format="int"),
        Kpi(label="Importe UC", value=importe_uc, format="euro"),
        Kpi(label="Apuntes sin UC", value=n_sin_uc, format="int"),
        Kpi(label="Importe sin UC", value=importe_sin_uc, format="euro"),
        Kpi(label="Apuntes filtrados", value=n_filtrados, format="int"),
        Kpi(label="Importe filtrado", value=importe_filtrados, format="euro"),
    ])


def listar_uc(params: QueryParams) -> ListResponse:
    """Listado paginado de UC del presupuesto."""
    uc = _safe_read_parquet(PATH_UC)
    if uc is None:
        return ListResponse(columns=_COLUMNS, rows=[], total=0)

    # Asegurar que solo servimos las columnas del contrato.
    cols_existentes = [c for c in _COLUMN_NAMES if c in uc.columns]
    uc = uc.select(cols_existentes)

    df, total = apply_query(uc, params, search_columns=_SEARCH_COLUMNS)
    rows = df.to_dicts()
    return ListResponse(columns=_COLUMNS, rows=rows, total=total)


def obtener_uc(uc_id: str) -> RecordResponse | None:
    """Ficha de una UC concreta, enriquecida con descripciones del árbol."""
    uc = _safe_read_parquet(PATH_UC)
    if uc is None:
        return None
    fila = uc.filter(pl.col("id") == uc_id)
    if fila.is_empty():
        return None
    row = fila.row(0, named=True)

    main = [
        FieldValue(name=c.name, label=c.label, value=row.get(c.name), format=c.format)
        for c in _COLUMNS if c.name in row
    ]

    sections: list[RecordSection] = []

    # Enriquecimiento con descripciones del árbol final
    enriquecidos: list[FieldValue] = []
    for col, árbol in (
        ("elemento_de_coste", "elementos de coste"),
        ("centro_de_coste", "centros de coste"),
        ("actividad", "actividades"),
    ):
        desc = _descripcion_nodo(árbol, row.get(col))
        if desc:
            enriquecidos.append(FieldValue(
                name=f"{col}__descripción",
                label=f"{col.replace('_', ' ').capitalize()} (descripción)",
                value=desc,
                format="text",
            ))
    if enriquecidos:
        sections.append(RecordSection(label="Enriquecimientos del árbol", fields=enriquecidos))

    return RecordResponse(main=main, sections=sections)


# ----------------------------------------------------------------------
# Sub-vistas adicionales del bloque
# ----------------------------------------------------------------------

# Apuntes (sin UC, filtrados, sin clasificar) — todos siguen el esquema
# del fichero de apuntes con algunas variantes.
_COLS_APUNTES_BASE: list[ColumnSpec] = [
    ColumnSpec(name="asiento", label="Asiento", format="text"),
    ColumnSpec(name="registro", label="Registro", format="id"),
    ColumnSpec(name="aplicación", label="Aplicación", format="text"),
    ColumnSpec(name="programa", label="Programa", format="text"),
    ColumnSpec(name="centro", label="Centro", format="text"),
    ColumnSpec(name="subcentro", label="Subcentro", format="text"),
    ColumnSpec(name="proyecto", label="Proyecto", format="text"),
    ColumnSpec(name="subproyecto", label="Subproy.", format="text"),
    ColumnSpec(name="línea", label="Línea", format="text"),
    ColumnSpec(name="fecha", label="Fecha", format="date"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="descripción", label="Descripción", format="text"),
]
_COLS_FILTRADOS = _COLS_APUNTES_BASE + [
    ColumnSpec(name="motivo", label="Motivo", format="text"),
]
_COLS_SIN_CLASIFICAR = _COLS_APUNTES_BASE + [
    ColumnSpec(name="tipo_proyecto", label="Tipo proyecto", format="text"),
]
_SEARCH_APUNTES = ["aplicación", "programa", "centro", "subcentro",
                    "proyecto", "subproyecto", "línea", "descripción"]


def _serialize(rows: list[dict]) -> list[dict]:
    return [
        {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()}
        for r in rows
    ]


def _listar(
    path: Path, cols: list[ColumnSpec], params,
    search: list[str] | None = None,
):
    df = _safe_read_parquet(path)
    if df is None:
        return ListResponse(columns=cols, rows=[], total=0)
    nombres = [c.name for c in cols if c.name in df.columns]
    df = df.select(nombres)
    df, total = apply_query(df, params, search_columns=search)
    return ListResponse(columns=cols, rows=_serialize(df.to_dicts()), total=total)


def listar_sin_uc(params) -> ListResponse:
    return _listar(PATH_SIN_UC, _COLS_APUNTES_BASE, params, _SEARCH_APUNTES)


def listar_filtrados(params) -> ListResponse:
    return _listar(
        PATH_FILTRADOS, _COLS_FILTRADOS, params,
        _SEARCH_APUNTES + ["motivo"],
    )


def listar_sin_clasificar(params) -> ListResponse:
    return _listar(
        PATH_SIN_CLASIFICAR, _COLS_SIN_CLASIFICAR, params,
        _SEARCH_APUNTES + ["tipo_proyecto"],
    )


# Resumen de filtrados por motivo
_COLS_FILTRADOS_POR_MOTIVO: list[ColumnSpec] = [
    ColumnSpec(name="motivo", label="Motivo", format="text"),
    ColumnSpec(name="n", label="N apuntes", format="int"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
]


def listar_filtrados_por_motivo(params) -> ListResponse:
    df = _safe_read_parquet(PATH_FILTRADOS)
    if df is None or df.is_empty():
        return ListResponse(columns=_COLS_FILTRADOS_POR_MOTIVO, rows=[], total=0)
    agg = (
        df.group_by("motivo")
        .agg(pl.len().alias("n"), pl.col("importe").sum().alias("importe"))
        .sort("importe", descending=True)
    )
    agg, total = apply_query(agg, params, search_columns=["motivo"])
    return ListResponse(
        columns=_COLS_FILTRADOS_POR_MOTIVO,
        rows=agg.to_dicts(), total=total,
    )


# Suministros: UC con esquema canónico
_COLS_UC_SUMIN: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID UC", format="text"),
    ColumnSpec(name="elemento_de_coste", label="Elemento", format="text"),
    ColumnSpec(name="centro_de_coste", label="Centro", format="text"),
    ColumnSpec(name="actividad", label="Actividad", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="origen", label="Origen", format="text"),
    ColumnSpec(name="origen_id", label="Origen ID", format="text"),
    ColumnSpec(name="origen_porción", label="Porción", format="float"),
]


def listar_uc_suministros(params) -> ListResponse:
    return _listar(
        PATH_UC_SUMIN, _COLS_UC_SUMIN, params,
        ["id", "elemento_de_coste", "centro_de_coste", "actividad",
         "origen", "origen_id"],
    )


# Reglas (3 vistas con la misma estructura)
_COLS_REGLAS: list[ColumnSpec] = [
    ColumnSpec(name="regla", label="Regla", format="text"),
    ColumnSpec(name="n", label="N apuntes", format="int"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
]


def listar_reglas_actividad(params) -> ListResponse:
    return _listar(PATH_REGLAS_ACT, _COLS_REGLAS, params, ["regla"])


def listar_reglas_cc(params) -> ListResponse:
    return _listar(PATH_REGLAS_CC, _COLS_REGLAS, params, ["regla"])


def listar_reglas_ec(params) -> ListResponse:
    return _listar(PATH_REGLAS_EC, _COLS_REGLAS, params, ["regla"])
