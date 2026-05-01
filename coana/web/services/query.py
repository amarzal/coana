"""Filtrado, ordenación y paginación server-side sobre Polars.

Punto de entrada único para todos los routers que sirven listados:
acepta los mismos cinco parámetros (`q`, `column`, `sort_by`, `desc`,
`offset`, `limit`) y aplica la lógica equivalente a la del visor
Streamlit (búsqueda por substring insensible a tildes y mayúsculas).
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl
from fastapi import Query

from coana.util.normalize import col_sin_tildes, sin_tildes
from coana.web.schemas.common import ColumnStats

# Número de bins del histograma. 20 da resolución suficiente para un
# sparkline de ~80 px de ancho sin saturar la respuesta JSON.
_HIST_BINS = 20


@dataclass
class QueryParams:
    """Parámetros estandarizados para listados."""

    q: str | None = None
    column: str | None = None
    sort_by: str | None = None
    desc: bool = False
    offset: int = 0
    limit: int = 100


def query_dependency(
    q: str | None = Query(None, description="Substring a buscar (insensible a tildes y mayúsculas)"),
    column: str | None = Query(None, description="Columna sobre la que aplicar `q` (None = todas las columnas string)"),
    sort_by: str | None = Query(None, description="Columna por la que ordenar"),
    desc: bool = Query(False, description="Orden descendente"),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=10_000),
) -> QueryParams:
    """FastAPI dependency que recoge los parámetros estándar de un listado."""
    return QueryParams(q=q, column=column, sort_by=sort_by, desc=desc, offset=offset, limit=limit)


def _string_columns(df: pl.DataFrame) -> list[str]:
    return [c for c, dt in zip(df.columns, df.dtypes) if dt == pl.Utf8]


def _is_numeric(dtype: pl.DataType) -> bool:
    return dtype.is_numeric() and dtype != pl.Boolean


def compute_column_stats(df: pl.DataFrame) -> dict[str, ColumnStats]:
    """Calcula total e histograma para columnas numéricas del DataFrame.

    Pensado para correr sobre el DataFrame ya filtrado (no paginado) para
    que las cifras sean estables al cambiar de página. Si el DataFrame
    está vacío, devuelve ``{}``.
    """
    out: dict[str, ColumnStats] = {}
    if df.is_empty():
        return out
    for name, dtype in zip(df.columns, df.dtypes):
        if not _is_numeric(dtype):
            continue
        s = df.get_column(name).cast(pl.Float64)
        s_nn = s.drop_nulls().drop_nans()
        count = int(s_nn.len())
        if count == 0:
            out[name] = ColumnStats(total=0.0, count=0, min=None, max=None, bins=[])
            continue
        total = float(s_nn.sum())
        mn = float(s_nn.min())
        mx = float(s_nn.max())
        if mx == mn:
            bins = [count] + [0] * (_HIST_BINS - 1)
        else:
            width = (mx - mn) / _HIST_BINS
            # Índice de bin = floor((x - min) / width); el último valor
            # cae en el último bin (índice _HIST_BINS - 1).
            idx = ((s_nn - mn) / width).cast(pl.Int64)
            idx = idx.clip(0, _HIST_BINS - 1)
            counts_df = (
                pl.DataFrame({"b": idx})
                .group_by("b")
                .len()
                .sort("b")
            )
            bin_map = dict(zip(counts_df["b"].to_list(), counts_df["len"].to_list()))
            bins = [int(bin_map.get(i, 0)) for i in range(_HIST_BINS)]
        out[name] = ColumnStats(total=total, count=count, min=mn, max=mx, bins=bins)
    return out


def apply_query(
    df: pl.DataFrame,
    params: QueryParams,
    *,
    search_columns: list[str] | None = None,
) -> tuple[pl.DataFrame, int, dict[str, ColumnStats]]:
    """Aplica filtro+sort+paginación al DataFrame.

    Devuelve ``(df_paginado, total_tras_filtro, column_stats)``.

    Reglas:

    - Si ``params.q`` es no nulo, se filtra por substring insensible a
      tildes/mayúsculas. Si ``params.column`` está dado, solo en esa
      columna; si no, en ``search_columns`` (o todas las columnas Utf8 si
      es None).
    - Si ``params.sort_by`` está dado, se ordena (con ``desc`` y
      ``nulls_last``).
    - El ``total`` se calcula tras filtrar pero antes de paginar.
    - ``column_stats`` se calcula sobre el DataFrame filtrado (todas las
      filas que pasarían a la respuesta, no solo la página) para que el
      histograma y el total no varíen al paginar.
    - El slice usa ``offset``/``limit``.
    """
    if df.is_empty():
        return df, 0, {}

    if params.q:
        normal = sin_tildes(params.q)
        if params.column:
            cols = [params.column]
        else:
            cols = search_columns if search_columns is not None else _string_columns(df)
        if cols:
            mask = pl.lit(False)
            for c in cols:
                mask = mask | col_sin_tildes(c).str.contains(normal, literal=True)
            df = df.filter(mask)

    if params.sort_by and params.sort_by in df.columns:
        df = df.sort(params.sort_by, descending=params.desc, nulls_last=True)

    total = df.height
    stats = compute_column_stats(df)
    df = df.slice(params.offset, params.limit)
    return df, total, stats
