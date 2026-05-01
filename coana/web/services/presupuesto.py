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
