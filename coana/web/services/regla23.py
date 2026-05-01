"""Servicio del bloque Regla 23.

Diez subvistas que exponen los parquets intermedios y las UC especiales
generadas durante el reparto de la masa retributiva indiferenciada del
PDI/PVI.
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
)
from coana.web.services.query import QueryParams, apply_query

DIR_NOMINAS = DIR_AUX / "nóminas"
PATH_PERSONAS = DIR_ENTRADA / "nóminas" / "personas.xlsx"

# Parquets de la regla 23
PATH_DED_DOCENTE = DIR_NOMINAS / "regla_23_dedicación_docente.parquet"
PATH_DED_TIT = DIR_NOMINAS / "regla_23_dedicación_titulaciones.parquet"
PATH_DED_EST = DIR_NOMINAS / "regla_23_dedicación_estudios.parquet"
PATH_HORAS = DIR_NOMINAS / "regla_23_horas_no_oficiales.parquet"
PATH_ESTRUCTURA = DIR_NOMINAS / "regla_23_estructura_estudios.parquet"
PATH_ATRASOS = DIR_NOMINAS / "regla_23_atrasos.parquet"
PATH_APART = DIR_NOMINAS / "regla_23_expedientes_apartados.parquet"
PATH_SIN_TIT = DIR_NOMINAS / "regla_23_asignaturas_sin_titulación.parquet"
PATH_ANOM_RES = DIR_NOMINAS / "regla_23_anomalías_resolución.parquet"
PATH_MULT_GRADO = DIR_NOMINAS / "regla_23_múltiples_con_grado.parquet"
PATH_DESPIDOS = DIR_NOMINAS / "uc_despidos.parquet"
PATH_INDEMS = DIR_NOMINAS / "uc_indemnizaciones_asistencias.parquet"
PATH_CARGOS = DIR_NOMINAS / "uc_cargos.parquet"


def _safe_read(path: Path) -> pl.DataFrame | None:
    try:
        return read_parquet(path)
    except FileNotFoundError:
        return None


# --- Personas (caché por mtime) ---------------------------------------

@lru_cache(maxsize=4)
def _personas_cached(path_str: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path_str)
    if not p.exists():
        return pl.DataFrame(schema={"per_id": pl.Int64, "persona": pl.Utf8})
    df = read_excel(p)
    return df.select(
        pl.col("per_id"),
        pl.concat_str(
            [pl.col("nombre"), pl.col("apellido1"), pl.col("apellido2")],
            separator=" ", ignore_nulls=True,
        ).alias("persona"),
    )


def _personas() -> pl.DataFrame:
    return _personas_cached(str(PATH_PERSONAS), _mtime_ns(PATH_PERSONAS))


def _enriq(df: pl.DataFrame) -> pl.DataFrame:
    if "per_id" not in df.columns:
        return df
    return df.join(_personas(), on="per_id", how="left")


def _serialize(rows: list[dict]) -> list[dict]:
    return [
        {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()}
        for r in rows
    ]


def _listar(
    path: Path, cols: list[ColumnSpec], params: QueryParams,
    search: list[str] | None = None,
    enriquecer_per_id: bool = True,
) -> ListResponse:
    df = _safe_read(path)
    if df is None:
        return ListResponse(columns=cols, rows=[], total=0)
    if enriquecer_per_id:
        df = _enriq(df)
    nombres = [c.name for c in cols if c.name in df.columns]
    df = df.select(nombres)
    df, total = apply_query(df, params, search_columns=search)
    return ListResponse(columns=cols, rows=_serialize(df.to_dicts()), total=total)


# ----------------------------------------------------------------------
# Resumen
# ----------------------------------------------------------------------

def resumen() -> KpiPanel:
    def _n_imp(path: Path, col: str = "importe") -> tuple[int, float]:
        df = _safe_read(path)
        if df is None or df.is_empty():
            return 0, 0.0
        n = df.height
        imp = float(df[col].sum() or 0) if col in df.columns else 0.0
        return n, imp

    n_ded = _safe_read(PATH_DED_DOCENTE)
    n_ded_n = 0 if n_ded is None else n_ded["expediente"].n_unique()
    cred_ded = 0.0 if n_ded is None else float(n_ded["créditos_impartidos"].sum() or 0)

    n_horas, _ = _n_imp(PATH_HORAS, "importe")
    n_atr, imp_atr = _n_imp(PATH_ATRASOS, "importe")
    n_apart = _safe_read(PATH_APART)
    n_apart_n = 0 if n_apart is None else n_apart.height
    n_desp, imp_desp = _n_imp(PATH_DESPIDOS, "importe")
    n_indem, imp_indem = _n_imp(PATH_INDEMS, "importe")
    n_cargos, imp_cargos = _n_imp(PATH_CARGOS, "importe")

    return KpiPanel(kpis=[
        Kpi(label="Expedientes con dedicación", value=n_ded_n, format="int"),
        Kpi(label="Créditos impartidos (total)", value=cred_ded, format="float"),
        Kpi(label="Horas no oficiales", value=n_horas, format="int"),
        Kpi(label="Bolsa de atrasos", value=imp_atr, format="euro"),
        Kpi(label="Expedientes apartados", value=n_apart_n, format="int"),
        Kpi(label="UC despidos", value=n_desp, format="int"),
        Kpi(label="Importe despidos", value=imp_desp, format="euro"),
        Kpi(label="UC indemnizaciones", value=n_indem, format="int"),
        Kpi(label="UC cargos en proyectos", value=n_cargos, format="int"),
        Kpi(label="Importe cargos", value=imp_cargos, format="euro"),
    ])


# ----------------------------------------------------------------------
# Dedicación docente (3 vistas: asignatura / titulación / estudio)
# ----------------------------------------------------------------------

_COLS_DED_ASIG: list[ColumnSpec] = [
    ColumnSpec(name="expediente", label="Expediente", format="id"),
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="asignatura", label="Asignatura", format="text"),
    ColumnSpec(name="créditos_impartidos", label="Créditos", format="float"),
]
_COLS_DED_TIT: list[ColumnSpec] = [
    ColumnSpec(name="expediente", label="Expediente", format="id"),
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="tipo", label="Tipo", format="text"),
    ColumnSpec(name="titulación", label="Titulación", format="text"),
    ColumnSpec(name="nombre_titulación", label="Nombre titulación", format="text"),
    ColumnSpec(name="créditos_impartidos", label="Créditos", format="float"),
]
_COLS_DED_EST: list[ColumnSpec] = [
    ColumnSpec(name="expediente", label="Expediente", format="id"),
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="tipo_estudio", label="Tipo", format="text"),
    ColumnSpec(name="código_estudio", label="Código", format="text"),
    ColumnSpec(name="nombre_estudio", label="Nombre estudio", format="text"),
    ColumnSpec(name="créditos_impartidos", label="Créditos", format="float"),
]


def listar_dedicacion_asignaturas(p: QueryParams) -> ListResponse:
    return _listar(PATH_DED_DOCENTE, _COLS_DED_ASIG, p, search=["persona", "asignatura"])


def listar_dedicacion_titulaciones(p: QueryParams) -> ListResponse:
    return _listar(PATH_DED_TIT, _COLS_DED_TIT, p,
                   search=["persona", "tipo", "titulación", "nombre_titulación"])


def listar_dedicacion_estudios(p: QueryParams) -> ListResponse:
    return _listar(PATH_DED_EST, _COLS_DED_EST, p,
                   search=["persona", "tipo_estudio", "código_estudio", "nombre_estudio"])


# ----------------------------------------------------------------------
# Docencia no oficial
# ----------------------------------------------------------------------

_COLS_HORAS: list[ColumnSpec] = [
    ColumnSpec(name="perid", label="per_id", format="id"),
    ColumnSpec(name="proyecto", label="Proyecto", format="text"),
    ColumnSpec(name="tipo_proyecto", label="Tipo proyecto", format="text"),
    ColumnSpec(name="nombre", label="Nombre", format="text"),
    ColumnSpec(name="motivo", label="Motivo", format="text"),
    ColumnSpec(name="unidad", label="Unidad", format="text"),
    ColumnSpec(name="total", label="Total", format="float"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="fecha", label="Fecha", format="date"),
]


def listar_horas_no_oficiales(p: QueryParams) -> ListResponse:
    return _listar(
        PATH_HORAS, _COLS_HORAS, p,
        search=["proyecto", "nombre", "motivo", "tipo_proyecto"],
        enriquecer_per_id=False,
    )


# ----------------------------------------------------------------------
# Estructura estudios
# ----------------------------------------------------------------------

_COLS_ESTRUCTURA: list[ColumnSpec] = [
    ColumnSpec(name="tipo", label="Tipo", format="text"),
    ColumnSpec(name="titulación", label="Titulación", format="text"),
    ColumnSpec(name="nombre_titulación", label="Nombre titulación", format="text"),
    ColumnSpec(name="estudio", label="Estudio", format="text"),
    ColumnSpec(name="nombre_estudio", label="Nombre estudio", format="text"),
    ColumnSpec(name="activa", label="Activa", format="bool"),
]


def listar_estructura(p: QueryParams) -> ListResponse:
    return _listar(
        PATH_ESTRUCTURA, _COLS_ESTRUCTURA, p,
        search=["tipo", "titulación", "nombre_titulación", "estudio", "nombre_estudio"],
        enriquecer_per_id=False,
    )


# ----------------------------------------------------------------------
# Bolsa de atrasos
# ----------------------------------------------------------------------

_COLS_ATRASOS: list[ColumnSpec] = [
    ColumnSpec(name="id", label="id", format="text"),
    ColumnSpec(name="expediente", label="Expediente", format="id"),
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="categoría", label="Categoría", format="text"),
    ColumnSpec(name="sector", label="Sector", format="text"),
    ColumnSpec(name="concepto_retributivo", label="Concepto", format="text"),
    ColumnSpec(name="proyecto", label="Proyecto", format="text"),
    ColumnSpec(name="centro", label="Centro", format="text"),
    ColumnSpec(name="aplicación", label="Aplicación", format="text"),
    ColumnSpec(name="fecha", label="Fecha", format="date"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="atrasos", label="Atrasos", format="text"),
]


def listar_atrasos(p: QueryParams) -> ListResponse:
    return _listar(
        PATH_ATRASOS, _COLS_ATRASOS, p,
        search=["persona", "categoría", "sector", "concepto_retributivo",
                "proyecto", "centro", "aplicación"],
    )


# ----------------------------------------------------------------------
# Expedientes apartados
# ----------------------------------------------------------------------

_COLS_APART: list[ColumnSpec] = [
    ColumnSpec(name="expediente", label="Expediente", format="id"),
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="sector", label="Sector", format="text"),
    ColumnSpec(name="tipo", label="Motivo", format="text"),
    ColumnSpec(name="importe_sin_atrasos", label="Sin atrasos", format="euro"),
    ColumnSpec(name="importe_atrasos", label="Atrasos", format="euro"),
]


def listar_apartados(p: QueryParams) -> ListResponse:
    return _listar(
        PATH_APART, _COLS_APART, p,
        search=["persona", "sector", "tipo"],
    )


# ----------------------------------------------------------------------
# UC: Despidos / Indemnizaciones / Cargos
# ----------------------------------------------------------------------

_COLS_UC_ESPECIAL: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID UC", format="text"),
    ColumnSpec(name="expediente", label="Expediente", format="id"),
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="elemento_de_coste", label="Elemento", format="text"),
    ColumnSpec(name="centro_de_coste", label="Centro", format="text"),
    ColumnSpec(name="actividad", label="Actividad", format="text"),
    ColumnSpec(name="proyecto", label="Proyecto", format="text"),
    ColumnSpec(name="tipo_proyecto", label="Tipo proyecto", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
]


def _listar_uc_especial(path: Path, p: QueryParams) -> ListResponse:
    return _listar(
        path, _COLS_UC_ESPECIAL, p,
        search=["id", "persona", "elemento_de_coste", "centro_de_coste",
                "actividad", "proyecto", "tipo_proyecto"],
    )


def listar_despidos(p: QueryParams) -> ListResponse:
    return _listar_uc_especial(PATH_DESPIDOS, p)


def listar_indemnizaciones(p: QueryParams) -> ListResponse:
    return _listar_uc_especial(PATH_INDEMS, p)


def listar_cargos(p: QueryParams) -> ListResponse:
    return _listar_uc_especial(PATH_CARGOS, p)


def _ficha_uc_especial(path: Path, uc_id: str) -> RecordResponse | None:
    df = _safe_read(path)
    if df is None:
        return None
    fila = df.filter(pl.col("id") == uc_id)
    if fila.is_empty():
        return None
    fila = _enriq(fila)
    row = fila.row(0, named=True)
    main = [
        FieldValue(name=c.name, label=c.label, value=row.get(c.name), format=c.format)
        for c in _COLS_UC_ESPECIAL if c.name in row
    ]
    return RecordResponse(main=main, sections=[])


def obtener_despido(uc_id: str) -> RecordResponse | None:
    return _ficha_uc_especial(PATH_DESPIDOS, uc_id)


def obtener_indemnizacion(uc_id: str) -> RecordResponse | None:
    return _ficha_uc_especial(PATH_INDEMS, uc_id)


def obtener_cargo(uc_id: str) -> RecordResponse | None:
    return _ficha_uc_especial(PATH_CARGOS, uc_id)


# ----------------------------------------------------------------------
# Anomalías
# ----------------------------------------------------------------------

_COLS_SIN_TIT: list[ColumnSpec] = [
    ColumnSpec(name="asignatura", label="Asignatura", format="text"),
    ColumnSpec(name="titulación", label="Titulación", format="text"),
    ColumnSpec(name="créditos_impartidos", label="Créditos", format="float"),
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
]


def listar_sin_titulacion(p: QueryParams) -> ListResponse:
    return _listar(PATH_SIN_TIT, _COLS_SIN_TIT, p, search=["asignatura", "titulación", "persona"])


_COLS_ANOM_RES: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="asignatura", label="Asignatura", format="text"),
    ColumnSpec(name="créditos_impartidos", label="Créditos", format="float"),
    ColumnSpec(name="motivo", label="Motivo", format="text"),
]


def listar_anomalias_resolucion(p: QueryParams) -> ListResponse:
    return _listar(
        PATH_ANOM_RES, _COLS_ANOM_RES, p,
        search=["persona", "asignatura", "motivo"],
    )


_COLS_MULT: list[ColumnSpec] = [
    ColumnSpec(name="asignatura", label="Asignatura", format="text"),
    ColumnSpec(name="titulación", label="Titulación", format="text"),
    ColumnSpec(name="tipo", label="Tipo", format="text"),
]


def listar_multiples_con_grado(p: QueryParams) -> ListResponse:
    return _listar(
        PATH_MULT_GRADO, _COLS_MULT, p,
        search=["asignatura", "titulación", "tipo"],
        enriquecer_per_id=False,
    )
