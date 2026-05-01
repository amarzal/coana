"""Servicio del bloque Cargos académicos.

Dos vistas:
- ``categoria_pdi_pvi``: por persona PDI/PVI, su categoría tras el último
  cobro de los conceptos retributivos 19/64.
- ``departamentos``: cargos académicos asociados a cada departamento.

Las dos enriquecen el ``per_id`` con el nombre de la persona en el
listado para evitar al frontend hacer N llamadas.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.web.deps import DIR_AUX, DIR_ENTRADA, _mtime_ns, read_parquet
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

PATH_CATEGORIA = DIR_AUX / "categoría_última_pdi_pvi.parquet"
PATH_DEPARTAMENTOS = DIR_AUX / "cargos_departamentos.parquet"
PATH_PERSONAS = DIR_ENTRADA / "nóminas" / "personas.xlsx"


# ----------------------------------------------------------------------
# Carga de personas con caché por mtime
# ----------------------------------------------------------------------

@lru_cache(maxsize=4)
def _personas_df_cached(path: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path)
    if not p.exists():
        return pl.DataFrame(schema={"per_id": pl.Int64, "persona": pl.Utf8})
    df = read_excel(p)
    return df.select(
        pl.col("per_id"),
        pl.concat_str(
            [pl.col("nombre"), pl.col("apellido1"), pl.col("apellido2")],
            separator=" ",
            ignore_nulls=True,
        ).alias("persona"),
    )


def _personas() -> pl.DataFrame:
    return _personas_df_cached(str(PATH_PERSONAS), _mtime_ns(PATH_PERSONAS))


def _enriquecer_per_id(df: pl.DataFrame) -> pl.DataFrame:
    """Si el DF tiene per_id, añade columna ``persona`` con el nombre completo."""
    if "per_id" not in df.columns:
        return df
    personas = _personas()
    return df.join(personas, on="per_id", how="left")


# ----------------------------------------------------------------------
# Categoría PDI/PVI
# ----------------------------------------------------------------------

_COLUMNS_CAT: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="categoría", label="Categoría", format="text"),
    ColumnSpec(name="fecha", label="Fecha último cobro", format="date"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="concepto_retributivo", label="Concepto", format="text"),
    ColumnSpec(name="proyecto", label="Proyecto", format="text"),
    ColumnSpec(name="centro", label="Centro presupuestario", format="text"),
    ColumnSpec(name="aplicación", label="Aplicación", format="text"),
    ColumnSpec(name="programa", label="Programa", format="text"),
]
_SEARCH_CAT = [
    "persona", "categoría", "concepto_retributivo", "proyecto",
    "centro", "aplicación", "programa",
]


# ----------------------------------------------------------------------
# Departamentos
# ----------------------------------------------------------------------

_COLUMNS_DEPT: list[ColumnSpec] = [
    ColumnSpec(name="idx", label="#", format="id", sortable=False),
    ColumnSpec(name="centro_cc", label="Centro de coste", format="text"),
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="cargo", label="Cargo", format="text"),
    ColumnSpec(name="servicio", label="Servicio", format="id"),
    ColumnSpec(name="fecha_inicio", label="Inicio", format="date"),
    ColumnSpec(name="fecha_fin", label="Fin", format="date"),
    ColumnSpec(name="fecha_inicio_cobra", label="Inicio cobra", format="date"),
    ColumnSpec(name="fecha_fin_cobra", label="Fin cobra", format="date"),
]
_SEARCH_DEPT = ["centro_cc", "persona", "cargo"]


def _con_idx(df: pl.DataFrame) -> pl.DataFrame:
    """Añade una columna ``idx`` con el índice global de la fila."""
    return df.with_row_index("idx")


# ----------------------------------------------------------------------
# Resumen
# ----------------------------------------------------------------------

def resumen() -> KpiPanel:
    cat = _safe_read(PATH_CATEGORIA)
    dept = _safe_read(PATH_DEPARTAMENTOS)

    n_personas_cat = 0 if cat is None else cat["per_id"].n_unique()
    n_categorías = 0 if cat is None else cat["categoría"].n_unique()
    n_cargos = 0 if dept is None else dept.height
    n_dpts = 0 if dept is None else dept["centro_cc"].n_unique()
    n_personas_cargo = 0 if dept is None else dept["per_id"].n_unique()

    return KpiPanel(kpis=[
        Kpi(label="Personas con categoría última", value=n_personas_cat, format="int"),
        Kpi(label="Categorías distintas", value=n_categorías, format="int"),
        Kpi(label="Cargos en departamentos", value=n_cargos, format="int"),
        Kpi(label="Departamentos con cargos", value=n_dpts, format="int"),
        Kpi(label="Personas con cargo", value=n_personas_cargo, format="int"),
    ])


def _safe_read(path: Path) -> pl.DataFrame | None:
    try:
        return read_parquet(path)
    except FileNotFoundError:
        return None


def listar_categoria(params: QueryParams) -> ListResponse:
    df = _safe_read(PATH_CATEGORIA)
    if df is None:
        return ListResponse(columns=_COLUMNS_CAT, rows=[], total=0)
    df = _enriquecer_per_id(df)
    df, total = apply_query(df, params, search_columns=_SEARCH_CAT)
    rows = df.to_dicts()
    # Las fechas Date salen como objetos date(); FastAPI las serializa OK,
    # pero las pasamos a string ISO para uniformidad con el resto.
    rows = [_serialize_dates(r) for r in rows]
    return ListResponse(columns=_COLUMNS_CAT, rows=rows, total=total)


def listar_departamentos(params: QueryParams) -> ListResponse:
    df = _safe_read(PATH_DEPARTAMENTOS)
    if df is None:
        return ListResponse(columns=_COLUMNS_DEPT, rows=[], total=0)
    df = _con_idx(_enriquecer_per_id(df))
    df, total = apply_query(df, params, search_columns=_SEARCH_DEPT)
    rows = df.to_dicts()
    rows = [_serialize_dates(r) for r in rows]
    return ListResponse(columns=_COLUMNS_DEPT, rows=rows, total=total)


def obtener_categoria(per_id: int) -> RecordResponse | None:
    df = _safe_read(PATH_CATEGORIA)
    if df is None:
        return None
    fila = df.filter(pl.col("per_id") == per_id)
    if fila.is_empty():
        return None
    fila = _enriquecer_per_id(fila)
    row = _serialize_dates(fila.row(0, named=True))
    main = [
        FieldValue(name=c.name, label=c.label, value=row.get(c.name), format=c.format)
        for c in _COLUMNS_CAT if c.name in row
    ]
    return RecordResponse(main=main, sections=[])


def obtener_cargo(idx: int) -> RecordResponse | None:
    """La tabla de departamentos no tiene PK natural; usamos el índice de fila."""
    df = _safe_read(PATH_DEPARTAMENTOS)
    if df is None or idx < 0 or idx >= df.height:
        return None
    fila = _enriquecer_per_id(df.slice(idx, 1))
    row = _serialize_dates(fila.row(0, named=True))
    main = [
        FieldValue(name=c.name, label=c.label, value=row.get(c.name), format=c.format)
        for c in _COLUMNS_DEPT if c.name in row
    ]

    # Sección con resto de cargos del mismo departamento
    sections: list[RecordSection] = []
    centro = row.get("centro_cc")
    if centro:
        otros = df.filter(pl.col("centro_cc") == centro).height
        sections.append(RecordSection(
            label="Departamento",
            fields=[
                FieldValue(name="centro_cc", label="Centro", value=centro, format="text"),
                FieldValue(name="n_cargos", label="Total cargos en este dpto.", value=otros, format="int"),
            ],
        ))
    return RecordResponse(main=main, sections=sections)


# ----------------------------------------------------------------------
# Utilidades
# ----------------------------------------------------------------------

def _serialize_dates(row: dict) -> dict:
    """Convierte objetos date/datetime a string ISO 8601 para JSON estable."""
    out = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out
