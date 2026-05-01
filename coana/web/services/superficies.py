"""Servicio del bloque Superficies.

Trabaja directamente con los Excel de ``data/entrada/superficies/``:
``ubicaciones.xlsx``, ``complejos.xlsx``, ``edificaciones.xlsx`` y
``zonas.xlsx``. No depende de que la fase 1 haya corrido.

La vista «Presencia centros» — más cara porque requiere las matrices que
calcula el procesamiento de inventario — se difiere a una fase posterior
(actualmente expone un endpoint que devuelve un placeholder).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.web.deps import DIR_ENTRADA, _mtime_ns
from coana.web.schemas.common import (
    ColumnSpec,
    Kpi,
    KpiPanel,
    ListResponse,
)
from coana.web.services.query import QueryParams, apply_query

DIR_SUP = DIR_ENTRADA / "superficies"
PATH_UBIC = DIR_SUP / "ubicaciones.xlsx"
PATH_COMPLEJOS = DIR_SUP / "complejos.xlsx"
PATH_EDIFICACIONES = DIR_SUP / "edificaciones.xlsx"
PATH_ZONAS = DIR_SUP / "zonas.xlsx"


@lru_cache(maxsize=8)
def _read_excel_cached(path: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    return read_excel(Path(path))


def _excel(path: Path) -> pl.DataFrame | None:
    if not path.exists():
        return None
    return _read_excel_cached(str(path), _mtime_ns(path))


# ----------------------------------------------------------------------
# Resumen
# ----------------------------------------------------------------------

def resumen() -> KpiPanel:
    ubic = _excel(PATH_UBIC)
    complejos = _excel(PATH_COMPLEJOS)
    edif = _excel(PATH_EDIFICACIONES)
    zonas = _excel(PATH_ZONAS)

    m2 = 0.0 if ubic is None else float(ubic["metros_cuadrados"].sum())
    n_ubic = 0 if ubic is None else ubic.height
    n_complejos = 0 if complejos is None else complejos.height
    n_edif = 0 if edif is None else edif.height
    n_zonas = 0 if zonas is None else zonas.height

    return KpiPanel(kpis=[
        Kpi(label="Superficie total", value=m2, format="m2"),
        Kpi(label="Complejos", value=n_complejos, format="int"),
        Kpi(label="Edificaciones", value=n_edif, format="int"),
        Kpi(label="Zonas", value=n_zonas, format="int"),
        Kpi(label="Ubicaciones", value=n_ubic, format="int"),
    ])


# ----------------------------------------------------------------------
# Totales por nivel
# ----------------------------------------------------------------------

_COLS_COMPLEJOS = [
    ColumnSpec(name="complejo", label="Complejo", format="text"),
    ColumnSpec(name="descripción", label="Descripción", format="text"),
    ColumnSpec(name="metros_cuadrados", label="m²", format="m2"),
    ColumnSpec(name="n_zonas", label="Zonas", format="int"),
    ColumnSpec(name="n_ubicaciones", label="Ubicaciones", format="int"),
]

_COLS_EDIFICACIONES = [
    ColumnSpec(name="complejo", label="Complejo", format="text"),
    ColumnSpec(name="edificación", label="Edificación", format="text"),
    ColumnSpec(name="descripción", label="Descripción", format="text"),
    ColumnSpec(name="metros_cuadrados", label="m²", format="m2"),
    ColumnSpec(name="n_zonas", label="Zonas", format="int"),
    ColumnSpec(name="n_ubicaciones", label="Ubicaciones", format="int"),
]

_COLS_ZONAS = [
    ColumnSpec(name="complejo", label="Complejo", format="text"),
    ColumnSpec(name="edificación", label="Edificación", format="text"),
    ColumnSpec(name="zona", label="Zona", format="text"),
    ColumnSpec(name="descripción", label="Descripción", format="text"),
    ColumnSpec(name="metros_cuadrados", label="m²", format="m2"),
    ColumnSpec(name="n_ubicaciones", label="Ubicaciones", format="int"),
]


def _totales_complejos() -> pl.DataFrame:
    ubic = _excel(PATH_UBIC)
    complejos = _excel(PATH_COMPLEJOS)
    if ubic is None or complejos is None:
        return pl.DataFrame()

    agreg = (
        ubic.group_by("área")
        .agg(
            pl.col("metros_cuadrados").sum(),
            pl.col("edificio").n_unique().alias("n_zonas"),
            pl.len().alias("n_ubicaciones"),
        )
        .rename({"área": "complejo"})
    )
    return complejos.join(agreg, on="complejo", how="left").with_columns(
        pl.col("metros_cuadrados").fill_null(0.0),
        pl.col("n_zonas").fill_null(0),
        pl.col("n_ubicaciones").fill_null(0),
    )


def _totales_edificaciones() -> pl.DataFrame:
    ubic = _excel(PATH_UBIC)
    edif = _excel(PATH_EDIFICACIONES)
    if ubic is None or edif is None:
        return pl.DataFrame()

    # Edificación = primer carácter de "edificio"
    ubic_ext = ubic.with_columns(
        pl.col("edificio").str.slice(0, 1).alias("edificación"),
    )
    agreg = (
        ubic_ext.group_by(["área", "edificación"])
        .agg(
            pl.col("metros_cuadrados").sum(),
            pl.col("edificio").n_unique().alias("n_zonas"),
            pl.len().alias("n_ubicaciones"),
        )
        .rename({"área": "complejo"})
    )
    return edif.join(agreg, on=["complejo", "edificación"], how="left").with_columns(
        pl.col("metros_cuadrados").fill_null(0.0),
        pl.col("n_zonas").fill_null(0),
        pl.col("n_ubicaciones").fill_null(0),
    )


def _totales_zonas() -> pl.DataFrame:
    ubic = _excel(PATH_UBIC)
    zonas = _excel(PATH_ZONAS)
    if ubic is None or zonas is None:
        return pl.DataFrame()

    # zona key: complejo + edificación + zona = "área" + "edificio" (donde
    # edificio tiene 2 caracteres edif+zona).
    ubic_ext = ubic.with_columns(
        pl.col("edificio").str.slice(0, 1).alias("edificación"),
        pl.col("edificio").str.slice(1).alias("zona"),
    )
    agreg = (
        ubic_ext.group_by(["área", "edificación", "zona"])
        .agg(
            pl.col("metros_cuadrados").sum(),
            pl.len().alias("n_ubicaciones"),
        )
        .rename({"área": "complejo"})
    )
    return zonas.join(
        agreg, on=["complejo", "edificación", "zona"], how="left",
    ).with_columns(
        pl.col("metros_cuadrados").fill_null(0.0),
        pl.col("n_ubicaciones").fill_null(0),
    )


def listar_complejos(params: QueryParams) -> ListResponse:
    df = _totales_complejos()
    if df.is_empty():
        return ListResponse(columns=_COLS_COMPLEJOS, rows=[], total=0)
    df = df.select([c.name for c in _COLS_COMPLEJOS])
    df, total = apply_query(df, params, search_columns=["complejo", "descripción"])
    return ListResponse(columns=_COLS_COMPLEJOS, rows=df.to_dicts(), total=total)


def listar_edificaciones(params: QueryParams) -> ListResponse:
    df = _totales_edificaciones()
    if df.is_empty():
        return ListResponse(columns=_COLS_EDIFICACIONES, rows=[], total=0)
    df = df.select([c.name for c in _COLS_EDIFICACIONES])
    df, total = apply_query(df, params, search_columns=["complejo", "edificación", "descripción"])
    return ListResponse(columns=_COLS_EDIFICACIONES, rows=df.to_dicts(), total=total)


def listar_zonas(params: QueryParams) -> ListResponse:
    df = _totales_zonas()
    if df.is_empty():
        return ListResponse(columns=_COLS_ZONAS, rows=[], total=0)
    df = df.select([c.name for c in _COLS_ZONAS])
    df, total = apply_query(df, params, search_columns=["complejo", "edificación", "zona", "descripción"])
    return ListResponse(columns=_COLS_ZONAS, rows=df.to_dicts(), total=total)
