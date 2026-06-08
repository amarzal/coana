"""Servicio del bloque «Reparto de actividades» (costes dag).

Lee los artefactos que produce la fase de reparto
(``data/fase1/reparto/``): el conjunto de UC tras el reparto, la tabla de
porcentajes no-dag por centro y las anomalías del reparto.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.web.deps import DIR_BASE, DIR_FASE1, read_parquet
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


# ----------------------------------------------------------------------
# Detalle del reparto por actividad dag (master-detalle)
# ----------------------------------------------------------------------

@lru_cache(maxsize=1)
def _meta_actividades() -> dict[str, tuple[str, str]]:
    """slug → (código, descripción) del árbol de actividades."""
    from coana.fase2.calculo import cargar_árbol_actividades
    arb = cargar_árbol_actividades(DIR_BASE)
    return {
        ident: (n.código, n.descripción)
        for ident, n in arb._por_id.items() if ident
    }


_COLS_DAG: list[ColumnSpec] = [
    ColumnSpec(name="código", label="Código", format="text"),
    ColumnSpec(name="marca_dag", label="Actividad dag", format="text"),
    ColumnSpec(name="descripción", label="Descripción", format="text"),
    ColumnSpec(name="n_destinos", label="Destinos", format="int"),
    ColumnSpec(name="n_fragmentos", label="Fragmentos", format="int"),
    ColumnSpec(name="importe", label="Importe repartido", format="euro"),
]
_SEARCH_DAG = ["marca_dag", "código", "descripción"]


def listar_dag(params: QueryParams) -> ListResponse:
    """Una fila por cada actividad dag que se repartió: total repartido y
    número de actividades finalistas destino."""
    df = _safe_read(PATH_UC)
    if df is None or df.is_empty():
        return ListResponse(columns=_COLS_DAG, rows=[], total=0)
    frags = df.filter(pl.col("origen") == _ORIGEN_FRAG)
    if frags.is_empty():
        return ListResponse(columns=_COLS_DAG, rows=[], total=0)
    agg = frags.group_by("marca_dag").agg(
        pl.col("actividad").n_unique().alias("n_destinos"),
        pl.len().alias("n_fragmentos"),
        pl.col("importe").sum().round(2).alias("importe"),
    )
    meta = _meta_actividades()
    agg = agg.with_columns(
        pl.col("marca_dag").replace_strict(
            {k: v[0] for k, v in meta.items()}, default="",
        ).alias("código"),
        pl.col("marca_dag").replace_strict(
            {k: v[1] for k, v in meta.items()}, default=pl.col("marca_dag"),
        ).alias("descripción"),
    )
    df = agg.select([c.name for c in _COLS_DAG])
    if not params.sort_by:
        df = df.sort("importe", descending=True, nulls_last=True)
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_DAG)
    return ListResponse(columns=_COLS_DAG, rows=df.to_dicts(), total=total, column_stats=stats)


_COLS_DAG_DETALLE: list[ColumnSpec] = [
    ColumnSpec(name="código", label="Código destino", format="text"),
    ColumnSpec(name="actividad", label="Actividad destino", format="text"),
    ColumnSpec(name="descripción", label="Descripción", format="text"),
    ColumnSpec(name="centro_de_coste", label="Centro de coste", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="porcentaje", label="% del dag", format="float"),
]
_SEARCH_DAG_DETALLE = ["actividad", "código", "descripción", "centro_de_coste"]


def detalle_dag(marca_dag: str, params: QueryParams) -> ListResponse:
    """Reparto de una actividad dag concreta: a qué actividades finalistas
    (y centros) fue a parar su coste, con importe y % del total del dag."""
    df = _safe_read(PATH_UC)
    if df is None or df.is_empty():
        return ListResponse(columns=_COLS_DAG_DETALLE, rows=[], total=0)
    frags = df.filter(
        (pl.col("origen") == _ORIGEN_FRAG) & (pl.col("marca_dag") == marca_dag)
    )
    if frags.is_empty():
        return ListResponse(columns=_COLS_DAG_DETALLE, rows=[], total=0)
    total_dag = float(frags["importe"].sum() or 0.0)
    agg = frags.group_by("centro_de_coste", "actividad").agg(
        pl.col("importe").sum().round(2).alias("importe"),
    ).with_columns(
        (100.0 * pl.col("importe") / total_dag if total_dag else pl.lit(0.0))
        .round(2).alias("porcentaje")
    )
    meta = _meta_actividades()
    agg = agg.with_columns(
        pl.col("actividad").replace_strict(
            {k: v[0] for k, v in meta.items()}, default="",
        ).alias("código"),
        pl.col("actividad").replace_strict(
            {k: v[1] for k, v in meta.items()}, default=pl.col("actividad"),
        ).alias("descripción"),
    )
    df = agg.select([c.name for c in _COLS_DAG_DETALLE])
    if not params.sort_by:
        df = df.sort("importe", descending=True, nulls_last=True)
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_DAG_DETALLE)
    return ListResponse(columns=_COLS_DAG_DETALLE, rows=df.to_dicts(), total=total, column_stats=stats)


_COLS_DAG_FRAGMENTOS: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID UC", format="text"),
    ColumnSpec(name="elemento_de_coste", label="Elemento de coste", format="text"),
    ColumnSpec(name="centro_de_coste", label="Centro de coste", format="text"),
    ColumnSpec(name="actividad", label="Actividad destino", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="origen_porción", label="Porción", format="float"),
    ColumnSpec(name="origen_id", label="Origen ID", format="text"),
]
_SEARCH_DAG_FRAGMENTOS = ["id", "elemento_de_coste", "centro_de_coste", "actividad", "origen_id"]


def fragmentos_dag(
    marca_dag: str,
    params: QueryParams,
    centro_de_coste: str | None = None,
    actividad: str | None = None,
) -> ListResponse:
    """Fragmentos individuales (una UC por fila) de una actividad dag.

    Opcionalmente acotados a un destino concreto (centro_de_coste +
    actividad), para encadenar tras `detalle_dag`. Cada fila es una UC
    con origen «reparto-dag»: el trozo de coste dag que cayó en ese par
    (centro, actividad) para un elemento de coste dado.
    """
    df = _safe_read(PATH_UC)
    if df is None or df.is_empty():
        return ListResponse(columns=_COLS_DAG_FRAGMENTOS, rows=[], total=0)
    frags = df.filter(
        (pl.col("origen") == _ORIGEN_FRAG) & (pl.col("marca_dag") == marca_dag)
    )
    if centro_de_coste is not None:
        frags = frags.filter(pl.col("centro_de_coste") == centro_de_coste)
    if actividad is not None:
        frags = frags.filter(pl.col("actividad") == actividad)
    if frags.is_empty():
        return ListResponse(columns=_COLS_DAG_FRAGMENTOS, rows=[], total=0)
    df = frags.select([c.name for c in _COLS_DAG_FRAGMENTOS])
    if not params.sort_by:
        df = df.sort("importe", descending=True, nulls_last=True)
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_DAG_FRAGMENTOS)
    return ListResponse(
        columns=_COLS_DAG_FRAGMENTOS, rows=df.to_dicts(), total=total, column_stats=stats,
    )
