"""Servicio del bloque «Reparto de actividades» (costes dag).

Lee los artefactos que produce la fase de reparto
(``data/fase1/reparto/``): el conjunto de UC tras el reparto, la tabla de
porcentajes no-dag por centro y las anomalías del reparto.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from coana.web.deps import DIR_FASE1, read_parquet
from coana.web.schemas.common import ColumnSpec, Kpi, KpiPanel, ListResponse
from coana.web.services.query import QueryParams, apply_query

DIR_REPARTO = DIR_FASE1 / "reparto"
PATH_UC = DIR_REPARTO / "uc_post_reparto.parquet"
PATH_PCT = DIR_REPARTO / "porcentajes_centro.parquet"
PATH_ANOM = DIR_REPARTO / "anomalias.parquet"
PATH_RESUMEN = DIR_REPARTO / "resumen.parquet"

_ORIGEN_FRAG = "reparto-dag"


def _safe_read(path: Path) -> pl.DataFrame | None:
    try:
        return read_parquet(path)
    except FileNotFoundError:
        return None


# ----------------------------------------------------------------------
# Resumen
# ----------------------------------------------------------------------

def resumen() -> KpiPanel:
    r = _safe_read(PATH_RESUMEN)
    if r is None or r.is_empty():
        return KpiPanel(kpis=[Kpi(label="Sin reparto", value=0, format="int",
                                  hint="Ejecuta «Reparto actividades»")])
    d = r.row(0, named=True)
    return KpiPanel(kpis=[
        Kpi(label="UC al inicio", value=int(d["n_uc_entrada"]), format="int",
            hint=f"UC de la fase 1 · {d['imp_entrada']:,.2f} €"),
        Kpi(label="UC dag a repartir", value=int(d["n_uc_dag"]), format="int",
            hint=f"actividad dag · {d['imp_dag']:,.2f} €"),
        Kpi(label="Fragmentos generados", value=int(d["n_fragmentos"]), format="int",
            hint=f"repartido a finalistas · {d['imp_fragmentos']:,.2f} €"),
        Kpi(label="UC dag sin repartir", value=int(d["n_anomalias"]), format="int",
            hint=f"anomalías · {d['imp_anomalias']:,.2f} €"),
        Kpi(label="UC tras reparto", value=int(d["n_uc_post"]), format="int",
            hint=f"importe total = {d['imp_post']:,.2f} €"),
        Kpi(label="Coste dag total", value=round(float(d["imp_dag"]), 2), format="euro",
            hint="Importe de las UC dag de entrada"),
        Kpi(label="Repartido a actividades", value=round(float(d["imp_fragmentos"]), 2), format="euro",
            hint=f"{int(d['n_fragmentos']):,} fragmentos"),
        Kpi(label="Retenido en anomalías", value=round(float(d["imp_anomalias"]), 2), format="euro",
            hint=f"{int(d['n_anomalias']):,} UC dag sin repartir"),
    ])


# ----------------------------------------------------------------------
# UC tras el reparto
# ----------------------------------------------------------------------

_COLS_UC: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID UC", format="text"),
    ColumnSpec(name="elemento_de_coste", label="Elemento de coste", format="text"),
    ColumnSpec(name="centro_de_coste", label="Centro de coste", format="text"),
    ColumnSpec(name="actividad", label="Actividad", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="marca_dag", label="Marca dag", format="text"),
    ColumnSpec(name="origen", label="Origen", format="text"),
    ColumnSpec(name="origen_id", label="Origen ID", format="text"),
    ColumnSpec(name="origen_porción", label="Porción", format="float"),
]
_SEARCH_UC = ["id", "centro_de_coste", "actividad", "marca_dag", "origen", "origen_id"]


def listar_uc(params: QueryParams) -> ListResponse:
    df = _safe_read(PATH_UC)
    if df is None or df.is_empty():
        return ListResponse(columns=_COLS_UC, rows=[], total=0)
    df = df.select([c.name for c in _COLS_UC if c.name in df.columns])
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_UC)
    return ListResponse(columns=_COLS_UC, rows=df.to_dicts(), total=total, column_stats=stats)


# ----------------------------------------------------------------------
# Porcentajes no-dag por centro
# ----------------------------------------------------------------------

_COLS_PCT: list[ColumnSpec] = [
    ColumnSpec(name="centro_de_coste", label="Centro de coste", format="text"),
    ColumnSpec(name="actividad", label="Actividad (no-dag)", format="text"),
    ColumnSpec(name="importe_actividad", label="Importe actividad", format="euro"),
    ColumnSpec(name="total_no_dag_centro", label="Total no-dag centro", format="euro"),
    ColumnSpec(name="porcentaje", label="% sobre centro", format="float"),
]
_SEARCH_PCT = ["centro_de_coste", "actividad"]


def listar_porcentajes(params: QueryParams) -> ListResponse:
    df = _safe_read(PATH_PCT)
    if df is None or df.is_empty():
        return ListResponse(columns=_COLS_PCT, rows=[], total=0)
    df = df.select([c.name for c in _COLS_PCT if c.name in df.columns] + (["clave"] if "clave" in df.columns else []))
    if not params.sort_by:
        df = df.sort(["centro_de_coste", "importe_actividad"], descending=[False, True])
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_PCT)
    return ListResponse(columns=_COLS_PCT, rows=df.to_dicts(), total=total, column_stats=stats)


# ----------------------------------------------------------------------
# Anomalías del reparto
# ----------------------------------------------------------------------

_COLS_ANOM: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID UC", format="text"),
    ColumnSpec(name="actividad", label="Actividad dag", format="text"),
    ColumnSpec(name="centro_de_coste", label="Centro de coste", format="text"),
    ColumnSpec(name="centro_esperado", label="Centro esperado", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="motivo", label="Motivo", format="text"),
]
_SEARCH_ANOM = ["id", "actividad", "centro_de_coste", "centro_esperado", "motivo"]


def listar_anomalias(params: QueryParams) -> ListResponse:
    df = _safe_read(PATH_ANOM)
    if df is None or df.is_empty():
        return ListResponse(columns=_COLS_ANOM, rows=[], total=0)
    df = df.select([c.name for c in _COLS_ANOM if c.name in df.columns])
    if not params.sort_by:
        df = df.sort("importe", descending=True, nulls_last=True)
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_ANOM)
    return ListResponse(columns=_COLS_ANOM, rows=df.to_dicts(), total=total, column_stats=stats)
