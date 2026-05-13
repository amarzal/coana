"""Servicio del bloque Cargos académicos.

Vistas:
- ``Por persona`` (master-detail): personas con cargos remunerados activos
  en el año, con detalle de sus cargos y UC tentativas.
- ``Personas cargos``: vista bruta de ``personas cargos.xlsx``.
- ``Catálogo de cargos``: catálogo (`cargos.xlsx`) con conteos sintéticos por
  sector y `importe_rd` resuelto vía `cargos real decreto.xlsx`.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.web.deps import DIR_AUX, DIR_ENTRADA, _mtime_ns, read_parquet
from coana.web.schemas.common import (
    ColumnSpec,
    Kpi,
    KpiPanel,
    ListResponse,
)
from coana.web.services.query import QueryParams, apply_query

PATH_PERSONAS = DIR_ENTRADA / "nóminas" / "personas.xlsx"
PATH_PERSONAS_CARGOS = DIR_ENTRADA / "nóminas" / "personas cargos.xlsx"
PATH_EXPEDIENTES_RH = DIR_ENTRADA / "nóminas" / "expedientes recursos humanos.xlsx"
PATH_CARGOS = DIR_ENTRADA / "nóminas" / "cargos.xlsx"
PATH_CARGOS_UC = DIR_AUX / "nóminas" / "cargos_uc.parquet"


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
# Resumen
# ----------------------------------------------------------------------

def resumen() -> KpiPanel:
    cargos_uc = _safe_read(PATH_CARGOS_UC)
    if cargos_uc is None or cargos_uc.is_empty():
        return KpiPanel(kpis=[
            Kpi(label="UC de cargos académicos", value=0, format="int"),
        ])
    return KpiPanel(kpis=[
        Kpi(label="Personas con UC de cargos", value=cargos_uc["per_id"].n_unique(), format="int"),
        Kpi(label="UC de cargos académicos", value=cargos_uc.height, format="int"),
        Kpi(label="Importe imputado", value=float(cargos_uc["importe_uc"].sum() or 0), format="euro"),
    ])


def _safe_read(path: Path) -> pl.DataFrame | None:
    try:
        return read_parquet(path)
    except FileNotFoundError:
        return None


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
    ColumnSpec(name="cargo_asimilado", label="Tipo RD", format="id"),
    ColumnSpec(name="importe_rd", label="Cuantía RD/mes", format="euro"),
    ColumnSpec(name="dedicación", label="Dedicación", format="float"),
    ColumnSpec(name="actividad", label="Actividad", format="text"),
    ColumnSpec(name="centro", label="Centro", format="text"),
    ColumnSpec(name="n_pdi", label="PDI", format="int"),
    ColumnSpec(name="n_pvi", label="PVI", format="int"),
    ColumnSpec(name="n_ptgas", label="PTGAS", format="int"),
    ColumnSpec(name="n_otros", label="Otros", format="int"),
    ColumnSpec(name="n_total", label="TOTAL", format="int"),
]
_SEARCH_CARGOS = ["cargo", "nombre", "actividad", "centro"]
PATH_CARGOS_RD = DIR_ENTRADA / "nóminas" / "cargos real decreto.xlsx"


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
    # Enriquecer con importe mensual del RD vía cargo_asimilado.
    if PATH_CARGOS_RD.exists():
        rd = read_excel(PATH_CARGOS_RD).select(
            pl.col("cargo_real_decreto").alias("cargo_asimilado"),
            pl.col("importe_mensual").alias("importe_rd"),
        )
        df = df.join(rd, on="cargo_asimilado", how="left")
    else:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("importe_rd"))
    nombres = [c.name for c in _COLUMNS_CARGOS if c.name in df.columns]
    df = df.select(nombres)
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_CARGOS)
    return ListResponse(
        columns=_COLUMNS_CARGOS, rows=df.to_dicts(),
        total=total, column_stats=stats,
    )


# ----------------------------------------------------------------------
# Personas con cargos remunerados (master-detail por persona)
# ----------------------------------------------------------------------

_COLUMNS_PERSONAS_REMUN: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="sector", label="Sector", format="text"),
    ColumnSpec(name="n_cargos", label="Cargos remunerados", format="int"),
    ColumnSpec(name="importe_ord", label="Ordinaria CR 19/64", format="euro"),
    ColumnSpec(name="importe_extra", label="Extra (de CR 68)", format="euro"),
    ColumnSpec(name="importe_uc", label="Importe UC total", format="euro"),
    ColumnSpec(name="extra_no_aplicada", label="Extra no aplicada", format="euro"),
]
_SEARCH_PERSONAS_REMUN = ["persona"]


@lru_cache(maxsize=4)
def _resumen_personas_cargos_remunerados_cached(
    _mt_uc: int, _mt_extras: int,
) -> pl.DataFrame:
    """Resumen por persona leyendo de `cargos_uc.parquet` y
    `cargos_extras_aplicadas.parquet`. Caché por mtime."""
    del _mt_uc, _mt_extras
    if not PATH_CARGOS_UC.exists():
        return pl.DataFrame()
    uc = pl.read_parquet(PATH_CARGOS_UC)
    if uc.is_empty():
        return pl.DataFrame()
    agg = (
        uc.group_by("per_id")
        .agg(
            pl.len().alias("n_cargos"),
            pl.col("importe_uc_ord").sum().alias("importe_ord"),
            pl.col("importe_uc_extra").sum().alias("importe_extra"),
            pl.col("importe_uc").sum().alias("importe_uc"),
            pl.col("extra_no_aplicada").first().alias("extra_no_aplicada"),
        )
        .with_columns(
            pl.col("importe_ord").round(2),
            pl.col("importe_extra").round(2),
            pl.col("importe_uc").round(2),
            pl.col("extra_no_aplicada").round(2),
        )
    )
    return agg


def listar_personas_con_cargos_remunerados(params: QueryParams) -> ListResponse:
    """Personas con al menos un cargo remunerado en el año analizado.
    Lee de `cargos_uc.parquet` (fuente de verdad generada en fase 1)."""
    from coana.web.services.personal import (
        _sector_principal_personal_cached, PATH_PDI, PATH_PVI,
    )
    path_extras = DIR_AUX / "nóminas" / "cargos_extras_aplicadas.parquet"
    df = _resumen_personas_cargos_remunerados_cached(
        _mtime_ns(PATH_CARGOS_UC), _mtime_ns(path_extras),
    )
    if df.is_empty():
        return ListResponse(columns=_COLUMNS_PERSONAS_REMUN, rows=[], total=0)

    df = _enriquecer_per_id(df)
    sectores = _sector_principal_personal_cached(
        _mtime_ns(PATH_PDI), _mtime_ns(PATH_PVI),
    )
    df = df.with_columns(
        pl.col("per_id").map_elements(
            lambda v: sectores.get(int(v), ""), return_dtype=pl.Utf8,
        ).alias("sector")
    )
    nombres = [c.name for c in _COLUMNS_PERSONAS_REMUN if c.name in df.columns]
    df = df.select(nombres).sort("importe_uc", descending=True, nulls_last=True)
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_PERSONAS_REMUN)
    return ListResponse(
        columns=_COLUMNS_PERSONAS_REMUN, rows=df.to_dicts(),
        total=total, column_stats=stats,
    )


_COLS_CARGOS_UC: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID", format="text"),
    ColumnSpec(name="cargo", label="Cargo", format="text"),
    ColumnSpec(name="nombre_cargo", label="Nombre", format="text"),
    ColumnSpec(name="cargo_asimilado", label="Tipo RD", format="id"),
    ColumnSpec(name="importe_rd", label="Cuantía RD/mes", format="euro"),
    ColumnSpec(name="fecha_inicio_cobra", label="Inicio cobra", format="date"),
    ColumnSpec(name="fecha_fin_cobra", label="Fin cobra", format="date"),
    ColumnSpec(name="días", label="Días en año", format="int"),
    ColumnSpec(name="peso", label="Peso (días × RD)", format="float"),
    ColumnSpec(name="importe_uc_ord", label="Importe ord.", format="euro"),
    ColumnSpec(name="extra_estimada", label="Extra estimada", format="euro"),
    ColumnSpec(name="importe_uc_extra", label="Extra aplicada", format="euro"),
    ColumnSpec(name="importe_uc", label="Importe UC", format="euro"),
    ColumnSpec(name="extra_no_aplicada", label="Extra no aplicada", format="euro"),
    ColumnSpec(name="elemento_de_coste", label="UC · elemento", format="text"),
    ColumnSpec(name="centro_de_coste", label="UC · centro", format="text"),
    ColumnSpec(name="actividad", label="UC · actividad", format="text"),
    ColumnSpec(name="_anomalía_patrón", label="Anomalía patrón", format="text"),
]


def listar_cargos_de_persona(per_id: int, params: QueryParams) -> ListResponse:
    """Detalle de cargos de la persona dada (lectura de `cargos_uc.parquet`)."""
    if not PATH_CARGOS_UC.exists():
        return ListResponse(columns=_COLS_CARGOS_UC, rows=[], total=0)
    df = pl.read_parquet(PATH_CARGOS_UC).filter(pl.col("per_id") == per_id)
    if df.is_empty():
        return ListResponse(columns=_COLS_CARGOS_UC, rows=[], total=0)
    nombres = [c.name for c in _COLS_CARGOS_UC if c.name in df.columns]
    df = df.select(nombres)
    df, total, stats = apply_query(
        df, params, search_columns=["nombre_cargo", "cargo"],
    )
    return ListResponse(
        columns=_COLS_CARGOS_UC, rows=_serialize_dates_list(df.to_dicts()),
        total=total, column_stats=stats,
    )


def _serialize_dates_list(rows: list[dict]) -> list[dict]:
    return [_serialize_dates(r) for r in rows]


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
