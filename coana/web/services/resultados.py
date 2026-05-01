"""Servicio del bloque «Resultados Fase 1».

Visión consolidada de todas las UC generadas, agregaciones por nodo de
los árboles finales (actividades, centros de coste, elementos de coste)
y detección de anomalías de integridad referencial.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.util import Árbol
from coana.web.deps import DIR_AUX, DIR_FASE1, _mtime_ns, read_parquet
from coana.web.schemas.common import (
    ColumnSpec,
    Kpi,
    KpiPanel,
    ListResponse,
)
from coana.web.services.query import QueryParams, apply_query

DIR_NOMINAS = DIR_AUX / "nóminas"

# Las fuentes de UC: (etiqueta para columna `_origen`, ruta del parquet).
_FUENTES_UC: list[tuple[str, Path]] = [
    ("presupuesto",     DIR_FASE1 / "uc presupuesto.parquet"),
    ("amortizaciones",  DIR_FASE1 / "uc amortizaciones.parquet"),
    ("suministros",     DIR_FASE1 / "uc suministros.parquet"),
    ("nóminas-PTGAS",   DIR_NOMINAS / "uc_ptgas.parquet"),
    ("nóminas-PVI",     DIR_NOMINAS / "uc_pvi.parquet"),
    ("nóminas-PDI",     DIR_NOMINAS / "uc_pdi.parquet"),
    ("despidos",        DIR_NOMINAS / "uc_despidos.parquet"),
    ("indemnizaciones", DIR_NOMINAS / "uc_indemnizaciones_asistencias.parquet"),
    ("cargos",          DIR_NOMINAS / "uc_cargos.parquet"),
    ("seguridad-social", DIR_NOMINAS / "persona_ss.parquet"),
]


def _safe_read(path: Path) -> pl.DataFrame | None:
    try:
        return read_parquet(path)
    except FileNotFoundError:
        return None


def _normalize(df: pl.DataFrame, fuente: str) -> pl.DataFrame:
    """Asegura que el DF tiene las columnas mínimas para el listado consolidado."""
    cols_min = ["id", "elemento_de_coste", "centro_de_coste", "actividad", "importe"]
    out = df
    if "id" not in out.columns:
        # persona_ss no tiene id; lo construimos
        if fuente == "seguridad-social":
            out = out.with_row_index("_idx").with_columns(
                pl.format("SS-{}", pl.col("_idx").cast(pl.String)).alias("id"),
            ).drop("_idx")
        else:
            out = out.with_row_index("id").with_columns(pl.col("id").cast(pl.String))
    if "importe" not in out.columns:
        # persona_ss usa ss_proporcional como importe efectivo
        if "ss_proporcional" in out.columns:
            out = out.with_columns(pl.col("ss_proporcional").alias("importe"))
        else:
            out = out.with_columns(pl.lit(0.0).alias("importe"))
    for c in ("elemento_de_coste", "centro_de_coste", "actividad"):
        if c not in out.columns:
            out = out.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))
    return out.select(cols_min).with_columns(pl.lit(fuente).alias("_origen"))


# Caché de la concatenación: clave = tupla con los mtime_ns de todos los parquets.
def _key_mtimes() -> tuple[int, ...]:
    return tuple(_mtime_ns(p) for _, p in _FUENTES_UC)


@lru_cache(maxsize=4)
def _todas_uc_cached(key: tuple[int, ...]) -> pl.DataFrame:
    del key
    partes: list[pl.DataFrame] = []
    for fuente, path in _FUENTES_UC:
        df = _safe_read(path)
        if df is None or df.is_empty():
            continue
        partes.append(_normalize(df, fuente))
    if not partes:
        return pl.DataFrame(schema={
            "id": pl.Utf8, "elemento_de_coste": pl.Utf8,
            "centro_de_coste": pl.Utf8, "actividad": pl.Utf8,
            "importe": pl.Float64, "_origen": pl.Utf8,
        })
    return pl.concat(partes, how="diagonal")


def _todas_uc() -> pl.DataFrame:
    return _todas_uc_cached(_key_mtimes())


# ----------------------------------------------------------------------
# Resumen
# ----------------------------------------------------------------------

def resumen() -> KpiPanel:
    df = _todas_uc()
    n_total = df.height
    imp_total = float(df["importe"].sum() or 0)
    kpis: list[Kpi] = [
        Kpi(label="UC totales", value=n_total, format="int"),
        Kpi(label="Importe total", value=imp_total, format="euro"),
    ]
    # Desglose por origen
    if not df.is_empty():
        agreg = (
            df.group_by("_origen")
            .agg(pl.len().alias("n"), pl.col("importe").sum().alias("imp"))
            .sort("imp", descending=True)
        )
        for row in agreg.iter_rows(named=True):
            kpis.append(Kpi(
                label=f"  {row['_origen']}", value=row["n"], format="int",
                hint=f"importe = {row['imp']:,.2f} €",
            ))
    return KpiPanel(kpis=kpis)


# ----------------------------------------------------------------------
# Listado consolidado (Todas las UC)
# ----------------------------------------------------------------------

_COLS_TODAS: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID", format="text"),
    ColumnSpec(name="_origen", label="Origen", format="text"),
    ColumnSpec(name="elemento_de_coste", label="Elemento", format="text"),
    ColumnSpec(name="centro_de_coste", label="Centro", format="text"),
    ColumnSpec(name="actividad", label="Actividad", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
]


def listar_todas(params: QueryParams) -> ListResponse:
    df = _todas_uc()
    df = df.select([c.name for c in _COLS_TODAS if c.name in df.columns])
    df, total = apply_query(
        df, params,
        search_columns=["id", "_origen", "elemento_de_coste", "centro_de_coste", "actividad"],
    )
    return ListResponse(columns=_COLS_TODAS, rows=df.to_dicts(), total=total)


# ----------------------------------------------------------------------
# Agregación por nodo de árbol
# ----------------------------------------------------------------------

@lru_cache(maxsize=8)
def _arbol(path_str: str, mtime_ns: int) -> Árbol | None:
    del mtime_ns
    p = Path(path_str)
    if not p.exists():
        return None
    return Árbol.from_file(p)


def _arbol_cached(name: str) -> Árbol | None:
    p = DIR_FASE1 / f"{name}.tree"
    return _arbol(str(p), _mtime_ns(p))


def _por_nodo(col_uc: str, árbol_name: str) -> pl.DataFrame:
    """Devuelve, para cada nodo del árbol, importe desglosado por origen + total."""
    arbol = _arbol_cached(árbol_name)
    df = _todas_uc()

    # Desglose UC × origen → tabla pivote por nodo.
    if df.is_empty() or col_uc not in df.columns:
        nodos = []
        if arbol is not None:
            for ident, nodo in arbol._por_id.items():
                if nodo.código:  # excluir raíz
                    nodos.append({
                        "código": nodo.código,
                        "identificador": ident,
                        "descripción": nodo.descripción,
                        "total": 0.0,
                    })
        return pl.DataFrame(nodos) if nodos else pl.DataFrame(
            schema={"código": pl.Utf8, "identificador": pl.Utf8, "descripción": pl.Utf8, "total": pl.Float64})

    pivot = (
        df.filter(pl.col(col_uc).is_not_null())
        .group_by(col_uc, "_origen")
        .agg(pl.col("importe").sum().alias("imp"))
        .pivot(values="imp", index=col_uc, on="_origen")
        .with_columns(
            pl.sum_horizontal(pl.exclude(col_uc)).alias("total"),
        )
    )

    # Construir filas por nodo (todos los del árbol, también los con 0).
    if arbol is None:
        return pivot.rename({col_uc: "identificador"})

    filas = []
    for ident, nodo in arbol._por_id.items():
        if not nodo.código:
            continue
        filas.append({
            "código": nodo.código,
            "identificador": ident,
            "descripción": nodo.descripción,
        })
    base = pl.DataFrame(filas)

    return base.join(
        pivot.rename({col_uc: "identificador"}),
        on="identificador", how="left",
    ).with_columns(pl.col("total").fill_null(0.0))


def _columns_for(df: pl.DataFrame) -> list[ColumnSpec]:
    cols: list[ColumnSpec] = [
        ColumnSpec(name="código", label="Código", format="text"),
        ColumnSpec(name="identificador", label="Identificador", format="text"),
        ColumnSpec(name="descripción", label="Descripción", format="text"),
    ]
    # añadir cada origen presente como columna euro
    for c in df.columns:
        if c in ("código", "identificador", "descripción", "total"):
            continue
        cols.append(ColumnSpec(name=c, label=c, format="euro"))
    cols.append(ColumnSpec(name="total", label="Total", format="euro"))
    return cols


def _listar_nodos(col_uc: str, árbol_name: str, params: QueryParams) -> ListResponse:
    df = _por_nodo(col_uc, árbol_name)
    cols = _columns_for(df)
    nombres = [c.name for c in cols if c.name in df.columns]
    df = df.select(nombres)
    df, total = apply_query(df, params, search_columns=["código", "identificador", "descripción"])
    rows = df.fill_null(0.0).to_dicts()
    return ListResponse(columns=cols, rows=rows, total=total)


def listar_actividades(p: QueryParams) -> ListResponse:
    return _listar_nodos("actividad", "actividades", p)


def listar_centros(p: QueryParams) -> ListResponse:
    return _listar_nodos("centro_de_coste", "centros de coste", p)


def listar_elementos(p: QueryParams) -> ListResponse:
    return _listar_nodos("elemento_de_coste", "elementos de coste", p)


# ----------------------------------------------------------------------
# Anomalías UC
# ----------------------------------------------------------------------

_COLS_ANOM: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID UC", format="text"),
    ColumnSpec(name="_origen", label="Origen", format="text"),
    ColumnSpec(name="campo", label="Campo afectado", format="text"),
    ColumnSpec(name="valor_inexistente", label="Identificador inexistente", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
]


def _identificadores_arbol(name: str) -> set[str]:
    arbol = _arbol_cached(name)
    if arbol is None:
        return set()
    return set(arbol._por_id.keys())


def listar_anomalias(params: QueryParams) -> ListResponse:
    df = _todas_uc()
    if df.is_empty():
        return ListResponse(columns=_COLS_ANOM, rows=[], total=0)

    ids_act = _identificadores_arbol("actividades")
    ids_cc = _identificadores_arbol("centros de coste")
    ids_ec = _identificadores_arbol("elementos de coste")

    anomalías: list[pl.DataFrame] = []
    for col, campo, ids_ok in (
        ("actividad", "actividad", ids_act),
        ("centro_de_coste", "centro_de_coste", ids_cc),
        ("elemento_de_coste", "elemento_de_coste", ids_ec),
    ):
        if not ids_ok or col not in df.columns:
            continue
        sub = df.filter(
            pl.col(col).is_not_null()
            & ~pl.col(col).is_in(list(ids_ok))
        )
        if sub.is_empty():
            continue
        sub = sub.select(
            pl.col("id"),
            pl.col("_origen"),
            pl.lit(campo).alias("campo"),
            pl.col(col).alias("valor_inexistente"),
            pl.col("importe"),
        )
        anomalías.append(sub)

    if not anomalías:
        return ListResponse(columns=_COLS_ANOM, rows=[], total=0)

    todo = pl.concat(anomalías, how="diagonal")
    todo, total = apply_query(
        todo, params,
        search_columns=["id", "_origen", "campo", "valor_inexistente"],
    )
    return ListResponse(columns=_COLS_ANOM, rows=todo.to_dicts(), total=total)
