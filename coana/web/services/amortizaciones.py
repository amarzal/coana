"""Servicio del bloque Amortizaciones.

Cada subvista expone un parquet de
``data/fase1/auxiliares/amortizaciones/`` o ``data/fase1/`` (las UC
generadas) con sus columnas específicas.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from coana.web.deps import DIR_AUX, DIR_FASE1, read_parquet
from coana.web.schemas.common import (
    ColumnSpec,
    FieldValue,
    Kpi,
    KpiPanel,
    ListResponse,
    RecordResponse,
)
from coana.web.services.query import QueryParams, apply_query

DIR_AMORT = DIR_AUX / "amortizaciones"

PATH_ENRIQUECIDO = DIR_AMORT / "inventario_enriquecido.parquet"
PATH_F_ESTADO = DIR_AMORT / "filtrados_estado.parquet"
PATH_F_CUENTA = DIR_AMORT / "filtrados_cuenta.parquet"
PATH_F_FECHA = DIR_AMORT / "filtrados_fecha.parquet"
PATH_SIN_CUENTA = DIR_AMORT / "sin_cuenta.parquet"
PATH_SIN_FECHA = DIR_AMORT / "sin_fecha_alta.parquet"
PATH_DET_CUENTAS = DIR_AMORT / "detalle_cuentas_filtradas.parquet"
PATH_SIN_UC = DIR_AMORT / "sin_uc.parquet"
PATH_UC = DIR_FASE1 / "uc amortizaciones.parquet"


def _safe_read(path: Path) -> pl.DataFrame | None:
    try:
        return read_parquet(path)
    except FileNotFoundError:
        return None


# ----------------------------------------------------------------------
# Esquemas de columnas comunes
# ----------------------------------------------------------------------

# Inventario enriquecido (10 cols)
_COLS_ENRIQ: list[ColumnSpec] = [
    ColumnSpec(name="id", label="id", format="id"),
    ColumnSpec(name="cuenta", label="Cuenta", format="text"),
    ColumnSpec(name="estado", label="Estado", format="text"),
    ColumnSpec(name="descripción", label="Descripción", format="text"),
    ColumnSpec(name="fecha_alta", label="Alta", format="date"),
    ColumnSpec(name="valor_inicial", label="Valor inicial", format="euro"),
    ColumnSpec(name="años_amortización", label="Años amort.", format="int"),
    ColumnSpec(name="días_en_año", label="Días en año", format="int"),
    ColumnSpec(name="importe", label="Importe año", format="euro"),
    ColumnSpec(name="id_ubicación", label="Ubicación", format="id"),
]

# Filtrados básicos (7 cols)
_COLS_FILTRADO: list[ColumnSpec] = [
    ColumnSpec(name="id", label="id", format="id"),
    ColumnSpec(name="cuenta", label="Cuenta", format="text"),
    ColumnSpec(name="estado", label="Estado", format="text"),
    ColumnSpec(name="descripción", label="Descripción", format="text"),
    ColumnSpec(name="fecha_alta", label="Alta", format="date"),
    ColumnSpec(name="valor_inicial", label="Valor inicial", format="euro"),
    ColumnSpec(name="id_ubicación", label="Ubicación", format="id"),
]

# Detalle de cuentas filtradas (3 cols)
_COLS_DET_CUENTAS: list[ColumnSpec] = [
    ColumnSpec(name="cuenta", label="Cuenta", format="text"),
    ColumnSpec(name="n", label="N filas", format="int"),
    ColumnSpec(name="valor_inicial", label="Valor inicial total", format="euro"),
]

# UC amortizaciones (8 cols)
_COLS_UC: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID UC", format="text"),
    ColumnSpec(name="elemento_de_coste", label="Elemento de coste", format="text"),
    ColumnSpec(name="centro_de_coste", label="Centro de coste", format="text"),
    ColumnSpec(name="actividad", label="Actividad", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="origen", label="Origen", format="text"),
    ColumnSpec(name="origen_id", label="Origen ID", format="text"),
    ColumnSpec(name="origen_porción", label="Porción", format="float"),
]


# ----------------------------------------------------------------------
# Resumen
# ----------------------------------------------------------------------

@dataclass
class _Stats:
    n: int = 0
    importe: float = 0.0


def _stats_parquet(path: Path, col_importe: str = "valor_inicial") -> _Stats:
    df = _safe_read(path)
    if df is None or df.is_empty():
        return _Stats()
    importe = float(df[col_importe].cast(pl.Float64).sum() or 0) if col_importe in df.columns else 0.0
    return _Stats(n=df.height, importe=importe)


def resumen() -> KpiPanel:
    enriq = _stats_parquet(PATH_ENRIQUECIDO, "importe")
    f_estado = _stats_parquet(PATH_F_ESTADO)
    f_cuenta = _stats_parquet(PATH_F_CUENTA)
    f_fecha = _stats_parquet(PATH_F_FECHA, "valor_inicial")
    sin_cuenta = _stats_parquet(PATH_SIN_CUENTA)
    sin_fecha = _stats_parquet(PATH_SIN_FECHA)
    sin_uc = _stats_parquet(PATH_SIN_UC, "importe")
    uc = _stats_parquet(PATH_UC, "importe")

    return KpiPanel(kpis=[
        Kpi(label="Registros enriquecidos", value=enriq.n, format="int"),
        Kpi(label="Importe amortización año", value=enriq.importe, format="euro"),
        Kpi(label="UC generadas", value=uc.n, format="int"),
        Kpi(label="Importe UC", value=uc.importe, format="euro"),
        Kpi(label="Filtrados (estado B)", value=f_estado.n, format="int"),
        Kpi(label="Filtrados (cuenta no válida)", value=f_cuenta.n, format="int"),
        Kpi(label="Filtrados (fecha)", value=f_fecha.n, format="int"),
        Kpi(label="Sin cuenta", value=sin_cuenta.n, format="int"),
        Kpi(label="Sin fecha de alta", value=sin_fecha.n, format="int"),
        Kpi(label="Sin centro de coste", value=sin_uc.n, format="int"),
    ])


# ----------------------------------------------------------------------
# Listados genéricos
# ----------------------------------------------------------------------

def _serialize(rows: list[dict]) -> list[dict]:
    return [
        {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()}
        for r in rows
    ]


def _listar(
    path: Path,
    cols: list[ColumnSpec],
    params: QueryParams,
    search: list[str] | None = None,
) -> ListResponse:
    df = _safe_read(path)
    if df is None:
        return ListResponse(columns=cols, rows=[], total=0)
    nombres = [c.name for c in cols if c.name in df.columns]
    df = df.select(nombres)
    df, total = apply_query(df, params, search_columns=search)
    return ListResponse(columns=cols, rows=_serialize(df.to_dicts()), total=total)


_SEARCH_INV = ["cuenta", "estado", "descripción"]


def listar_enriquecido(params: QueryParams) -> ListResponse:
    return _listar(PATH_ENRIQUECIDO, _COLS_ENRIQ, params, _SEARCH_INV)


def listar_filtrados_estado(params: QueryParams) -> ListResponse:
    return _listar(PATH_F_ESTADO, _COLS_FILTRADO, params, _SEARCH_INV)


def listar_filtrados_cuenta(params: QueryParams) -> ListResponse:
    return _listar(PATH_F_CUENTA, _COLS_FILTRADO, params, _SEARCH_INV)


def listar_filtrados_fecha(params: QueryParams) -> ListResponse:
    return _listar(PATH_F_FECHA, _COLS_FILTRADO, params, _SEARCH_INV)


def listar_sin_cuenta(params: QueryParams) -> ListResponse:
    return _listar(PATH_SIN_CUENTA, _COLS_FILTRADO, params, _SEARCH_INV)


def listar_sin_fecha(params: QueryParams) -> ListResponse:
    return _listar(PATH_SIN_FECHA, _COLS_FILTRADO, params, _SEARCH_INV)


def listar_detalle_cuentas(params: QueryParams) -> ListResponse:
    return _listar(PATH_DET_CUENTAS, _COLS_DET_CUENTAS, params, ["cuenta"])


def listar_sin_uc(params: QueryParams) -> ListResponse:
    return _listar(PATH_SIN_UC, _COLS_ENRIQ, params, _SEARCH_INV)


def listar_uc(params: QueryParams) -> ListResponse:
    return _listar(
        PATH_UC, _COLS_UC, params,
        search=["id", "elemento_de_coste", "centro_de_coste", "actividad",
                "origen", "origen_id"],
    )


# ----------------------------------------------------------------------
# Fichas individuales
# ----------------------------------------------------------------------

def _ficha_de(path: Path, cols: list[ColumnSpec], col_id: str, valor) -> RecordResponse | None:
    df = _safe_read(path)
    if df is None:
        return None
    fila = df.filter(pl.col(col_id) == valor)
    if fila.is_empty():
        return None
    row = fila.row(0, named=True)
    row = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()}
    main = [
        FieldValue(name=c.name, label=c.label, value=row.get(c.name), format=c.format)
        for c in cols if c.name in row
    ]
    return RecordResponse(main=main, sections=[])


def obtener_enriquecido(reg_id: int) -> RecordResponse | None:
    return _ficha_de(PATH_ENRIQUECIDO, _COLS_ENRIQ, "id", reg_id)


def obtener_uc(uc_id: str) -> RecordResponse | None:
    return _ficha_de(PATH_UC, _COLS_UC, "id", uc_id)
