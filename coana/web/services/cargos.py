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
PATH_PERSONAS_CARGOS = DIR_ENTRADA / "nóminas" / "personas cargos.xlsx"
PATH_EXPEDIENTES_RH = DIR_ENTRADA / "nóminas" / "expedientes recursos humanos.xlsx"
PATH_CARGOS = DIR_ENTRADA / "nóminas" / "cargos.xlsx"


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
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_CAT)
    rows = df.to_dicts()
    # Las fechas Date salen como objetos date(); FastAPI las serializa OK,
    # pero las pasamos a string ISO para uniformidad con el resto.
    rows = [_serialize_dates(r) for r in rows]
    return ListResponse(columns=_COLUMNS_CAT, rows=rows, total=total, column_stats=stats)


def listar_departamentos(params: QueryParams) -> ListResponse:
    df = _safe_read(PATH_DEPARTAMENTOS)
    if df is None:
        return ListResponse(columns=_COLUMNS_DEPT, rows=[], total=0)
    df = _con_idx(_enriquecer_per_id(df))
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_DEPT)
    rows = df.to_dicts()
    rows = [_serialize_dates(r) for r in rows]
    return ListResponse(columns=_COLUMNS_DEPT, rows=rows, total=total, column_stats=stats)


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
# Personas cargos (fichero de entrada bruto, enriquecido)
# ----------------------------------------------------------------------

_COLUMNS_PC: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="expediente", label="Expediente", format="id"),
    ColumnSpec(name="cargo", label="Cargo", format="text"),
    ColumnSpec(name="servicio", label="Servicio", format="id"),
    ColumnSpec(name="titulación", label="Titulación", format="text"),
    ColumnSpec(name="fecha_inicio", label="Inicio", format="date"),
    ColumnSpec(name="fecha_fin", label="Fin", format="date"),
    ColumnSpec(name="fecha_inicio_cobra", label="Inicio cobra", format="date"),
    ColumnSpec(name="fecha_fin_cobra", label="Fin cobra", format="date"),
]
_SEARCH_PC = ["persona", "cargo"]


@lru_cache(maxsize=2)
def _personas_cargos_cached(path: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path)
    if not p.exists():
        return pl.DataFrame()
    return read_excel(p)


def _personas_cargos() -> pl.DataFrame:
    return _personas_cargos_cached(str(PATH_PERSONAS_CARGOS), _mtime_ns(PATH_PERSONAS_CARGOS))


@lru_cache(maxsize=2)
def _per_ids_pdi_pvi_cached(path: str, mtime_ns: int) -> set[int]:
    """per_ids con al menos un expediente de sector PDI o PVI (codificado como PI)."""
    del mtime_ns
    p = Path(path)
    if not p.exists():
        return set()
    df = read_excel(p)
    return set(
        df.filter(pl.col("sector").is_in(["PDI", "PI"]))
        .get_column("per_id")
        .drop_nulls()
        .unique()
        .to_list()
    )


def _per_ids_pdi_pvi() -> set[int]:
    return _per_ids_pdi_pvi_cached(str(PATH_EXPEDIENTES_RH), _mtime_ns(PATH_EXPEDIENTES_RH))


# Año analizado. TODO: parametrizar (mismo TODO que en otros sitios).
_AÑO_ANALIZADO = 2025


def listar_personas_cargos(params: QueryParams) -> ListResponse:
    df = _personas_cargos()
    if df.is_empty():
        return ListResponse(columns=_COLUMNS_PC, rows=[], total=0)
    # Filtro: solo filas con al menos un día de actividad en el año
    # analizado (fecha_inicio ≤ 31-dic-AÑO y (fecha_fin es null o
    # fecha_fin ≥ 1-ene-AÑO)).
    fin_año = pl.date(_AÑO_ANALIZADO, 12, 31)
    inicio_año = pl.date(_AÑO_ANALIZADO, 1, 1)
    activo = (
        pl.col("fecha_inicio").cast(pl.Date) <= fin_año
    ) & (
        pl.col("fecha_fin").is_null()
        | (pl.col("fecha_fin").cast(pl.Date) >= inicio_año)
    )
    df = df.filter(activo)
    # Filtro de sector: la persona ha de tener al menos un expediente
    # PDI o PVI (codificado como PI). Las personas con expedientes solo
    # de PTGAS u otros sectores quedan fuera.
    pdi_pvi = _per_ids_pdi_pvi()
    if pdi_pvi:
        df = df.filter(pl.col("per_id").is_in(list(pdi_pvi)))
    df = _enriquecer_per_id(df)
    nombres = [c.name for c in _COLUMNS_PC if c.name in df.columns]
    df = df.select(nombres)
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_PC)
    rows = [_serialize_dates(r) for r in df.to_dicts()]
    return ListResponse(columns=_COLUMNS_PC, rows=rows, total=total, column_stats=stats)


# ----------------------------------------------------------------------
# Catálogo de cargos (cargos.xlsx)
# ----------------------------------------------------------------------

_COLUMNS_CARGOS: list[ColumnSpec] = [
    ColumnSpec(name="cargo", label="Cargo", format="text"),
    ColumnSpec(name="nombre", label="Nombre", format="text"),
    ColumnSpec(name="cuantía", label="Cuantía", format="euro"),
    ColumnSpec(name="tipo_cargo", label="Tipo cargo", format="text"),
    ColumnSpec(name="n_pdi", label="PDI", format="int"),
    ColumnSpec(name="n_pvi", label="PVI", format="int"),
    ColumnSpec(name="n_ptgas", label="PTGAS", format="int"),
    ColumnSpec(name="n_otros", label="Otros", format="int"),
    ColumnSpec(name="n_total", label="TOTAL", format="int"),
]
_SEARCH_CARGOS = ["cargo", "nombre"]


@lru_cache(maxsize=2)
def _cargos_xlsx_cached(path: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path)
    if not p.exists():
        return pl.DataFrame()
    return read_excel(p)


# Año analizado (ya definido más arriba como _AÑO_ANALIZADO).
_PRELACIÓN_SECTOR_CARGOS = ["PTGAS", "PVI", "PDI", "Otros"]


@lru_cache(maxsize=2)
def _sector_principal_por_persona_cached(path_rh: str, mtime_rh: int) -> pl.DataFrame:
    """Para cada per_id, sector principal (prelación PTGAS > PVI > PDI > Otros)."""
    del mtime_rh
    p = Path(path_rh)
    if not p.exists():
        return pl.DataFrame(schema={"per_id": pl.Int64, "sector_principal": pl.Utf8})
    df = read_excel(p)
    mapeo = {"PAS": "PTGAS", "PI": "PVI"}
    df = df.with_columns(
        pl.col("sector").replace(mapeo).fill_null("Otros").alias("_s"),
    ).with_columns(
        pl.when(pl.col("_s").is_in(["PDI", "PTGAS", "PVI"]))
        .then(pl.col("_s"))
        .otherwise(pl.lit("Otros"))
        .alias("_s"),
    )
    prio = {s: i for i, s in enumerate(_PRELACIÓN_SECTOR_CARGOS)}
    df = df.with_columns(
        pl.col("_s").replace({k: str(v) for k, v in prio.items()}).alias("_p"),
    )
    return (
        df.sort("_p")
        .group_by("per_id")
        .first()
        .select("per_id", pl.col("_s").alias("sector_principal"))
    )


def _sector_principal_por_persona() -> pl.DataFrame:
    return _sector_principal_por_persona_cached(
        str(PATH_EXPEDIENTES_RH), _mtime_ns(PATH_EXPEDIENTES_RH),
    )


def _personas_por_cargo_en_año() -> pl.DataFrame:
    """Cuenta personas distintas por (cargo, sector) que han ocupado el cargo
    al menos un día en el año analizado, agregadas en columnas n_pdi, n_pvi,
    n_ptgas, n_total."""
    pc = _personas_cargos()
    if pc.is_empty():
        return pl.DataFrame(schema={
            "cargo": pl.Utf8,
            "n_pdi": pl.UInt32, "n_pvi": pl.UInt32,
            "n_ptgas": pl.UInt32, "n_total": pl.UInt32,
        })
    fin_año = pl.date(_AÑO_ANALIZADO, 12, 31)
    inicio_año = pl.date(_AÑO_ANALIZADO, 1, 1)
    activo = (
        pl.col("fecha_inicio").cast(pl.Date) <= fin_año
    ) & (
        pl.col("fecha_fin").is_null()
        | (pl.col("fecha_fin").cast(pl.Date) >= inicio_año)
    )
    sub = pc.filter(activo).select("per_id", pl.col("cargo").cast(pl.Utf8))
    sub = sub.join(
        _sector_principal_por_persona(), on="per_id", how="left",
    ).with_columns(pl.col("sector_principal").fill_null("Otros"))
    by_sector = (
        sub.group_by("cargo", "sector_principal")
        .agg(pl.col("per_id").n_unique().alias("n"))
        .pivot(on="sector_principal", index="cargo", values="n")
        .fill_null(0)
    )
    # Garantizar las columnas PDI/PVI/PTGAS/Otros (si no aparecen, ponerlas a 0).
    for sec in ["PDI", "PVI", "PTGAS", "Otros"]:
        if sec not in by_sector.columns:
            by_sector = by_sector.with_columns(pl.lit(0).cast(pl.UInt32).alias(sec))
    total = (
        sub.group_by("cargo")
        .agg(pl.col("per_id").n_unique().alias("n_total"))
    )
    out = by_sector.join(total, on="cargo", how="left").rename({
        "PDI": "n_pdi", "PVI": "n_pvi", "PTGAS": "n_ptgas", "Otros": "n_otros",
    })
    cols_final = ["cargo", "n_pdi", "n_pvi", "n_ptgas", "n_otros", "n_total"]
    return out.select(*cols_final)


_COLUMNS_CARGO_PERSONAS: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="sector", label="Sector", format="text"),
    ColumnSpec(name="expediente", label="Expediente", format="id"),
    ColumnSpec(name="fecha_inicio", label="Inicio", format="date"),
    ColumnSpec(name="fecha_fin", label="Fin", format="date"),
    ColumnSpec(name="fecha_inicio_cobra", label="Inicio cobra", format="date"),
    ColumnSpec(name="fecha_fin_cobra", label="Fin cobra", format="date"),
]


def listar_personas_de_cargo(cargo: str, params: QueryParams) -> ListResponse:
    """Personas/expedientes que han ocupado el cargo dado en el año analizado."""
    pc = _personas_cargos()
    if pc.is_empty():
        return ListResponse(columns=_COLUMNS_CARGO_PERSONAS, rows=[], total=0)
    fin_año = pl.date(_AÑO_ANALIZADO, 12, 31)
    inicio_año = pl.date(_AÑO_ANALIZADO, 1, 1)
    activo = (
        pl.col("fecha_inicio").cast(pl.Date) <= fin_año
    ) & (
        pl.col("fecha_fin").is_null()
        | (pl.col("fecha_fin").cast(pl.Date) >= inicio_año)
    )
    df = pc.filter((pl.col("cargo").cast(pl.Utf8) == str(cargo)) & activo)
    if df.is_empty():
        return ListResponse(columns=_COLUMNS_CARGO_PERSONAS, rows=[], total=0)
    df = df.join(_sector_principal_por_persona(), on="per_id", how="left").with_columns(
        pl.col("sector_principal").fill_null("Otros").alias("sector"),
    )
    df = _enriquecer_per_id(df)
    nombres = [c.name for c in _COLUMNS_CARGO_PERSONAS if c.name in df.columns]
    df = df.select(nombres)
    df, total, stats = apply_query(
        df, params, search_columns=["persona", "sector"],
    )
    rows = [_serialize_dates(r) for r in df.to_dicts()]
    return ListResponse(
        columns=_COLUMNS_CARGO_PERSONAS, rows=rows,
        total=total, column_stats=stats,
    )


def listar_cargos(params: QueryParams) -> ListResponse:
    df = _cargos_xlsx_cached(str(PATH_CARGOS), _mtime_ns(PATH_CARGOS))
    if df.is_empty():
        return ListResponse(columns=_COLUMNS_CARGOS, rows=[], total=0)
    df = df.with_columns(pl.col("cargo").cast(pl.Utf8))
    df = df.join(_personas_por_cargo_en_año(), on="cargo", how="left")
    for col in ["n_pdi", "n_pvi", "n_ptgas", "n_otros", "n_total"]:
        df = df.with_columns(pl.col(col).fill_null(0).cast(pl.Int64))
    nombres = [c.name for c in _COLUMNS_CARGOS if c.name in df.columns]
    df = df.select(nombres)
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_CARGOS)
    return ListResponse(
        columns=_COLUMNS_CARGOS, rows=df.to_dicts(),
        total=total, column_stats=stats,
    )


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
