"""Servicio del bloque «Reducciones sindicales».

Reúne en una sola vista las dos formas — independientes — de reducción
por representación sindical:

- **PDI**: tipos 37-40 de `reducciones docentes.xlsx`, expresados en
  créditos y traducidos a fracción de jornada por la fase 1
  (`reducciones_sindicales_pdi.parquet`).
- **PTGAS**: tipo 8 de `reducciones laborales.xlsx`, medido en días y
  porcentaje de jornada trabajada.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.util.configuración import cfg_int
from coana.web.deps import DIR_BASE, DIR_ENTRADA, DIR_FASE1, _mtime_ns, read_parquet
from coana.web.schemas.common import ColumnSpec, Kpi, KpiPanel, ListResponse
from coana.web.services.query import QueryParams, apply_query

PATH_PERSONAS = DIR_ENTRADA / "nóminas" / "personas.xlsx"
PATH_EXPEDIENTES_RH = DIR_ENTRADA / "nóminas" / "expedientes recursos humanos.xlsx"
PATH_RED_SIND_PDI = DIR_FASE1 / "regla23" / "reducciones_sindicales_pdi.parquet"
PATH_NORMALIZADA = DIR_FASE1 / "regla23" / "dedicación_pdi_normalizada.parquet"

# Año analizado. TODO: parametrizar (mismo TODO que en otros servicios).
_AÑO_ANALIZADO = 2025


# ----------------------------------------------------------------------
# Personas (nombre completo), con caché por mtime
# ----------------------------------------------------------------------

@lru_cache(maxsize=4)
def _personas_cached(path: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path)
    if not p.exists():
        return pl.DataFrame(schema={"per_id": pl.Int64, "persona": pl.Utf8})
    return read_excel(p).select(
        pl.col("per_id"),
        pl.concat_str(
            [pl.col("nombre"), pl.col("apellido1"), pl.col("apellido2")],
            separator=" ",
            ignore_nulls=True,
        ).alias("persona"),
    )


def _personas() -> pl.DataFrame:
    return _personas_cached(str(PATH_PERSONAS), _mtime_ns(PATH_PERSONAS))


def _enriquecer_per_id(df: pl.DataFrame) -> pl.DataFrame:
    if "per_id" not in df.columns:
        return df
    return df.join(_personas(), on="per_id", how="left")


def _safe_parquet(path: Path) -> pl.DataFrame | None:
    try:
        return read_parquet(path)
    except FileNotFoundError:
        return None


# ----------------------------------------------------------------------
# Resumen
# ----------------------------------------------------------------------

def resumen() -> KpiPanel:
    pdi = _safe_parquet(PATH_RED_SIND_PDI)
    ptgas = _ptgas_df()
    n_pdi = pdi.height if pdi is not None else 0
    h_pdi = float(pdi["horas_sindicales"].sum()) if pdi is not None and not pdi.is_empty() else 0.0
    return KpiPanel(kpis=[
        Kpi(label="Representantes sindicales PDI", value=n_pdi, format="int",
            hint="Tipos 37-40 de reducciones docentes"),
        Kpi(label="Horas a acción sindical (PDI)", value=round(h_pdi), format="int",
            hint="Suma de horas anuales imputadas a representación sindical"),
        Kpi(label="Representantes sindicales PTGAS", value=ptgas.height, format="int",
            hint="Tipo 8 de reducciones laborales"),
    ])


# ----------------------------------------------------------------------
# PDI (tipos 37-40, basado en créditos)
# ----------------------------------------------------------------------

_COLUMNS_PDI: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="sindicato", label="Sindicato", format="text"),
    ColumnSpec(name="es_asociado", label="Asociado", format="bool"),
    ColumnSpec(name="creditos_capacidad", label="Créd. capacidad", format="float"),
    ColumnSpec(name="creditos_reduccion", label="Créd. reducción", format="float"),
    ColumnSpec(name="creditos_sindicales", label="Créd. sindicales", format="float"),
    ColumnSpec(name="fraccion_sindical", label="Fracción jornada", format="float"),
    ColumnSpec(name="horas_sindicales", label="Horas/año", format="float"),
]
_SEARCH_PDI = ["persona", "sindicato"]


def _es_asociado_por_persona() -> pl.DataFrame:
    """per_id → es_asociado, leído de `dedicación_pdi_normalizada.parquet`."""
    norm = _safe_parquet(PATH_NORMALIZADA)
    if norm is None or norm.is_empty() or "es_asociado" not in norm.columns:
        return pl.DataFrame(schema={"per_id": pl.Int64, "es_asociado": pl.Boolean})
    return norm.select("per_id", "es_asociado").unique(subset="per_id")


def listar_pdi(params: QueryParams) -> ListResponse:
    df = _safe_parquet(PATH_RED_SIND_PDI)
    if df is None or df.is_empty():
        return ListResponse(columns=_COLUMNS_PDI, rows=[], total=0)
    df = _enriquecer_per_id(df)
    df = df.join(_es_asociado_por_persona(), on="per_id", how="left")
    nombres = [c.name for c in _COLUMNS_PDI if c.name in df.columns]
    df = df.select(nombres)
    if not params.sort_by:
        df = df.sort("fraccion_sindical", descending=True, nulls_last=True)
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_PDI)
    return ListResponse(
        columns=_COLUMNS_PDI, rows=df.to_dicts(), total=total, column_stats=stats,
    )


# ----------------------------------------------------------------------
# PTGAS (tipo 8, basado en días y % de jornada)
# ----------------------------------------------------------------------

_COLUMNS_PTGAS: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="expediente", label="Expediente", format="id"),
    ColumnSpec(name="fraccion_sindical", label="Fracción jornada", format="float"),
    ColumnSpec(name="horas_sindicales", label="Horas/año", format="float"),
]
_SEARCH_PTGAS = ["persona"]


def _ptgas_df() -> pl.DataFrame:
    """Reducción sindical del PTGAS por expediente (tipo 8).

    `factor_x_por_expediente` devuelve ``{expediente: X}`` con X = fracción
    del año trabajada; la fracción sindical es ``1 − X``.
    """
    from coana.fase1.nóminas.reducciones_sindicales import factor_x_por_expediente

    factores = factor_x_por_expediente(DIR_BASE, año=_AÑO_ANALIZADO)
    if not factores:
        return pl.DataFrame(schema={
            "per_id": pl.Int64, "expediente": pl.Int64,
            "fraccion_sindical": pl.Float64, "horas_sindicales": pl.Float64,
        })
    jornada = float(cfg_int("jornada_anual_pdi"))
    df = pl.DataFrame(
        {
            "expediente": list(factores.keys()),
            "_x": list(factores.values()),
        },
        schema={"expediente": pl.Int64, "_x": pl.Float64},
    ).with_columns(
        (1.0 - pl.col("_x")).alias("fraccion_sindical"),
    ).with_columns(
        (pl.col("fraccion_sindical") * jornada).round(2).alias("horas_sindicales"),
    )
    # expediente → per_id.
    if PATH_EXPEDIENTES_RH.exists():
        exp = read_excel(PATH_EXPEDIENTES_RH).select("expediente", "per_id").unique(
            subset="expediente",
        )
        df = df.join(exp, on="expediente", how="left")
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.Int64).alias("per_id"))
    return df.select("per_id", "expediente", "fraccion_sindical", "horas_sindicales")


def listar_ptgas(params: QueryParams) -> ListResponse:
    df = _ptgas_df()
    if df.is_empty():
        return ListResponse(columns=_COLUMNS_PTGAS, rows=[], total=0)
    df = _enriquecer_per_id(df)
    nombres = [c.name for c in _COLUMNS_PTGAS if c.name in df.columns]
    df = df.select(nombres)
    if not params.sort_by:
        df = df.sort("fraccion_sindical", descending=True, nulls_last=True)
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_PTGAS)
    return ListResponse(
        columns=_COLUMNS_PTGAS, rows=df.to_dicts(), total=total, column_stats=stats,
    )
