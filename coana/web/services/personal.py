"""Servicio del bloque Personal.

Subvistas:
- Resumen: KPIs por sector.
- Expedientes PDI/PTGAS/PVI/Otros: lista con per_id enriquecido a nombre.
- Multiexpediente: personas con expedientes en sectores distintos.
- Persona: agregado por per_id con sus UC y reparto de SS.
- Anomalías PDI: asignaturas sin titulación (si existe el parquet).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl
from pydantic import BaseModel, Field

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

DIR_NOMINAS = DIR_AUX / "nóminas"
PATH_PERSONAS = DIR_ENTRADA / "nóminas" / "personas.xlsx"

PATH_PDI = DIR_NOMINAS / "PDI.parquet"
PATH_PTGAS = DIR_NOMINAS / "PTGAS.parquet"
PATH_PVI = DIR_NOMINAS / "PVI.parquet"
PATH_OTROS = DIR_NOMINAS / "Otros.parquet"
PATH_MULTI = DIR_NOMINAS / "multiexpediente.parquet"
PATH_UC = DIR_NOMINAS / "persona_uc.parquet"
PATH_SS = DIR_NOMINAS / "persona_ss.parquet"
PATH_ANOM_PDI = DIR_NOMINAS / "regla_23_asignaturas_sin_titulación.parquet"
PATH_COSTES_SOC_CALC = DIR_NOMINAS / "costes_sociales_calculados.parquet"
PATH_NOMINAS_RAW = DIR_ENTRADA / "nóminas" / "nóminas y seguridad social.xlsx"

_SECTOR_PATHS = {
    "PDI": PATH_PDI,
    "PTGAS": PATH_PTGAS,
    "PVI": PATH_PVI,
    "Otros": PATH_OTROS,
}


def _safe_read(path: Path) -> pl.DataFrame | None:
    try:
        return read_parquet(path)
    except FileNotFoundError:
        return None


# Personas con caché ---------------------------------------------------

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


def _enriquecer_per_id(df: pl.DataFrame) -> pl.DataFrame:
    if "per_id" not in df.columns:
        return df
    return df.join(_personas(), on="per_id", how="left")


def _serialize(rows: list[dict]) -> list[dict]:
    return [
        {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()}
        for r in rows
    ]


# ----------------------------------------------------------------------
# Resumen
# ----------------------------------------------------------------------

def resumen() -> KpiPanel:
    kpis: list[Kpi] = []
    total_exp = 0
    total_imp = 0.0
    for sector, path in _SECTOR_PATHS.items():
        df = _safe_read(path)
        if df is None:
            continue
        n = df.height
        imp = float(df["importe"].sum() or 0)
        total_exp += n
        total_imp += imp
        kpis.append(Kpi(label=f"Expedientes {sector}", value=n, format="int"))
        kpis.append(Kpi(label=f"Importe {sector}", value=imp, format="euro"))
    multi = _safe_read(PATH_MULTI)
    n_multi = 0 if multi is None else multi.height
    kpis.append(Kpi(label="Total expedientes", value=total_exp, format="int"))
    kpis.append(Kpi(label="Importe total", value=total_imp, format="euro"))
    kpis.append(Kpi(label="Personas multiexpediente", value=n_multi, format="int"))
    return KpiPanel(kpis=kpis)


# ----------------------------------------------------------------------
# Expedientes por sector
# ----------------------------------------------------------------------

_COLS_EXP: list[ColumnSpec] = [
    ColumnSpec(name="expediente", label="Expediente", format="id"),
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="n_registros", label="N líneas nómina", format="int"),
]
_SEARCH_EXP = ["persona"]


def listar_sector(sector: str, params: QueryParams) -> ListResponse | None:
    path = _SECTOR_PATHS.get(sector)
    if path is None:
        return None
    df = _safe_read(path)
    if df is None:
        return ListResponse(columns=_COLS_EXP, rows=[], total=0)
    df = _enriquecer_per_id(df)
    df = df.select([c.name for c in _COLS_EXP if c.name in df.columns])
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_EXP)
    return ListResponse(columns=_COLS_EXP, rows=_serialize(df.to_dicts()), total=total, column_stats=stats)


@lru_cache(maxsize=2)
def _nominas_raw_cached(path_str: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path_str)
    if not p.exists():
        return pl.DataFrame()
    return read_excel(p)


def _nominas_raw() -> pl.DataFrame:
    return _nominas_raw_cached(str(PATH_NOMINAS_RAW), _mtime_ns(PATH_NOMINAS_RAW))


# Columnas de la tabla de líneas de nómina por expediente. El orden lo
# decide el frontend (ID + importes a la izquierda); aquí solo declaramos
# tipos para que se rendericen bien.
_COLS_LINEAS_NOMINA: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID", format="text"),
    ColumnSpec(name="tipo_coste", label="Tipo coste", format="text"),
    ColumnSpec(name="categoría", label="Categoría", format="text"),
    ColumnSpec(name="perceptor", label="Perceptor", format="id"),
    ColumnSpec(name="provisión", label="Provisión", format="text"),
    ColumnSpec(name="categoría_plaza", label="Cat. plaza", format="text"),
    ColumnSpec(name="sector_plaza", label="Sector plaza", format="text"),
    ColumnSpec(name="fecha", label="Fecha", format="date"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="atrasos", label="Atrasos", format="euro"),
    ColumnSpec(name="concepto_retributivo", label="Concepto", format="text"),
    ColumnSpec(name="proyecto", label="Proyecto", format="text"),
    ColumnSpec(name="subproyecto", label="Subproy.", format="text"),
    ColumnSpec(name="aplicación", label="Aplicación", format="text"),
    ColumnSpec(name="programa", label="Programa", format="text"),
    ColumnSpec(name="línea", label="Línea", format="text"),
    ColumnSpec(name="centro", label="Centro", format="text"),
    ColumnSpec(name="subcentro", label="Subcentro", format="text"),
    ColumnSpec(name="servicio", label="Servicio", format="id"),
    ColumnSpec(name="centro_plaza", label="Centro plaza", format="id"),
]
_SEARCH_LINEAS = [
    "id", "tipo_coste", "categoría", "categoría_plaza", "sector_plaza",
    "concepto_retributivo", "proyecto", "subproyecto", "aplicación",
    "programa", "línea", "centro", "subcentro",
]


# Proyectos «ordinarios» de PTGAS — vienen del clasificador de centros
# de coste de fase 1; se reusa la misma lista para reproducir la
# clasificación que hacía el visor Streamlit.
try:
    from coana.fase1.clasificador_centros_coste import (
        PROYECTOS_ORDINARIOS as _PROY_ORD,
    )
    _PROYECTOS_ORDINARIOS: list[str] = list(_PROY_ORD)
except Exception:
    _PROYECTOS_ORDINARIOS = []


@lru_cache(maxsize=2)
def _lineas_financiacion_cached(path_str: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path_str)
    if not p.exists():
        return pl.DataFrame()
    return read_excel(p)


def _enriquecer_tipo_linea(df: pl.DataFrame) -> pl.DataFrame:
    """Añade ``tipo_línea`` a partir de ``líneas de financiación.xlsx``."""
    if "línea" not in df.columns:
        return df
    p = DIR_ENTRADA / "presupuesto" / "líneas de financiación.xlsx"
    ref = _lineas_financiacion_cached(str(p), _mtime_ns(p))
    if ref.is_empty() or "línea" not in ref.columns or "tipo" not in ref.columns:
        return df
    return df.join(
        ref.select(pl.col("línea"), pl.col("tipo").alias("tipo_línea")),
        on="línea",
        how="left",
    )


def _grupos_lineas(df: pl.DataFrame, sector: str) -> list[tuple[str, pl.DataFrame]]:
    """Reparte las líneas de nómina de un expediente en grupos.

    La clasificación reproduce la del visor Streamlit:
    - costes sociales = aplicación que empieza por «12».
    - PTGAS: ordinarias / extra (proyecto en PROYECTOS_ORDINARIOS o no).
    - PVI: simplemente costes sociales / retribuciones.
    - PDI: costes sociales, retribuciones con financiación finalista
      (tipo_línea ≠ "00") y, dentro del resto, docencia / gestión /
      investigación / incentivos / regla 23 según concepto y proyecto.
    - Otros: igual que PVI.
    """
    if df.is_empty() or "aplicación" not in df.columns:
        return []
    es_cs = pl.col("aplicación").cast(pl.Utf8).str.starts_with("12")
    cs = df.filter(es_cs)
    no_cs = df.filter(~es_cs)
    grupos: list[tuple[str, pl.DataFrame]] = []

    if sector == "PTGAS" and "proyecto" in df.columns and _PROYECTOS_ORDINARIOS:
        es_ord = pl.col("proyecto").is_in(_PROYECTOS_ORDINARIOS)
        cr_48 = pl.col("concepto_retributivo").cast(pl.Utf8) == "48"
        ord_lineas = no_cs.filter(es_ord)
        # Orden pedido: ord (sin CR 48), ord CR 48, extra, costes sociales.
        grupos += [
            ("Retribuciones ordinarias (sin CR 48)", ord_lineas.filter(~cr_48)),
            ("Retribuciones ordinarias CR 48", ord_lineas.filter(cr_48)),
            ("Retribuciones extra", no_cs.filter(~es_ord)),
            ("Costes sociales", cs),
        ]
        return [(label, sub) for label, sub in grupos if not sub.is_empty()]

    if sector in ("PDI", "PVI"):
        from coana.fase1.nóminas.regla_23 import _PROYECTOS_GENERALES
        cr_str = pl.col("concepto_retributivo").cast(pl.Utf8)
        proy = pl.col("proyecto").cast(pl.Utf8)
        es_gen = proy.is_in(list(_PROYECTOS_GENERALES))

        ord_gen = no_cs.filter(es_gen)
        # Tratamientos especiales dentro de las ordinarias en proyecto
        # general, en líneas con CR específico. Los atrasos (CR 30/87)
        # ya no se apartan: entran en la bolsa de regla 23.
        despidos = ord_gen.filter(cr_str == "47")
        cargos_gen = ord_gen.filter(cr_str.is_in(["19", "64"]))
        # CR 48 sale a su propia pestaña independientemente del proyecto
        # (su tratamiento es uniforme).
        indemn = no_cs.filter(cr_str == "48")
        # Bolsa gorda regla 23: ordinarias generales que no son ninguna
        # de las anteriores. Incluye los atrasos (CR 30/87).
        regla23 = ord_gen.filter(~cr_str.is_in(["47", "48", "19", "64"]))
        # Retribuciones extra: proyecto específico, excluyendo CR 48 (que
        # ya se muestra en su pestaña).
        extra = no_cs.filter(~es_gen & (cr_str != "48"))
        grupos += [
            ("Retribuciones ordinarias para regla 23", regla23),
            ("Ordinarias despidos (CR 47)", despidos),
            ("Ordinarias indemnizaciones por asistencias (CR 48)", indemn),
            ("Ordinarias cargos (CR 19/64)", cargos_gen),
            ("Retribuciones extra", extra),
            ("Costes sociales", cs),
        ]
        return [(label, sub) for label, sub in grupos if not sub.is_empty()]

    # Otros sectores: solo costes sociales y retribuciones.
    grupos.append(("Costes sociales", cs))
    grupos.append(("Retribuciones", no_cs))
    return [(label, sub) for label, sub in grupos if not sub.is_empty()]


class GrupoLineas(BaseModel):
    label: str = Field(..., description="Etiqueta legible del grupo")
    n: int = Field(..., description="Número de líneas")
    importe: float = Field(..., description="Suma de importes del grupo")


class GruposLineasResponse(BaseModel):
    grupos: list[GrupoLineas]


UC_GENERADAS_LABEL = "UC generadas"
COSTES_CALCULADOS_LABEL = "Costes sociales calculados"
CARGOS_LABEL = "Cargos"


def _per_id_de_expediente(expediente: int) -> int | None:
    """Devuelve el `per_id` asociado al expediente (None si no se encuentra)."""
    for path in _SECTOR_PATHS.values():
        df = _safe_read(path)
        if df is None or "expediente" not in df.columns:
            continue
        sub = df.filter(pl.col("expediente") == expediente)
        if not sub.is_empty():
            v = sub.get_column("per_id").drop_nulls()
            if v.len():
                return int(v[0])
    return None


def _costes_calculados_de_persona(per_id: int) -> pl.DataFrame:
    """Fila de detalle del coste social calculado para una persona.

    Devuelve un DataFrame vacío si la persona no está en clases pasivas.
    """
    df = _safe_read(PATH_COSTES_SOC_CALC)
    if df is None or df.is_empty():
        return pl.DataFrame()
    return df.filter(pl.col("per_id") == per_id)


# --- Cargos de un expediente / persona --------------------------------

_AÑO_PERSONAL = 2025  # TODO: parametrizar

PATH_PERSONAS_CARGOS = DIR_ENTRADA / "nóminas" / "personas cargos.xlsx"
PATH_CARGOS_CAT = DIR_ENTRADA / "nóminas" / "cargos.xlsx"
PATH_CARGOS_RD = DIR_ENTRADA / "nóminas" / "cargos real decreto.xlsx"
PATH_EXP_RH_PERSONAL = DIR_ENTRADA / "nóminas" / "expedientes recursos humanos.xlsx"


@lru_cache(maxsize=2)
def _categoría_última_cached(_mt_nom: int, _mt_exp: int) -> dict[int, str]:
    """Categoría última por per_id (PDI/PVI) según CR 19/64 más reciente.

    Antes leía de `categoría_última_pdi_pvi.parquet`; ahora se calcula
    al vuelo desde las nóminas crudas y se cachea por mtime.
    """
    del _mt_nom, _mt_exp
    nom_path = DIR_ENTRADA / "nóminas" / "nóminas y seguridad social.xlsx"
    exp_path = DIR_ENTRADA / "nóminas" / "expedientes recursos humanos.xlsx"
    if not nom_path.exists() or not exp_path.exists():
        return {}
    nom = read_excel(nom_path)
    exp = read_excel(exp_path)
    cr = pl.col("concepto_retributivo").cast(pl.Utf8)
    df = (
        nom.join(exp.select("expediente", "per_id"), on="expediente", how="inner")
        .filter(cr.is_in(["19", "64"]))
        .sort("fecha", descending=True)
        .group_by("per_id")
        .agg(pl.col("categoría").first().alias("categoría"))
    )
    return {
        int(r["per_id"]): str(r["categoría"])
        for r in df.iter_rows(named=True) if r.get("categoría")
    }


@lru_cache(maxsize=2)
def _sector_principal_personal_cached(_mtime_pdi: int, _mtime_pvi: int) -> dict[int, str]:
    """per_id → sector con mayor importe en el año, considerando solo
    expedientes con actividad económica (los que llegaron a los parquets
    sectoriales `PDI/PTGAS/PVI/Otros.parquet` tras el filtro de 0 €).

    A diferencia de la prelación PTGAS > PVI > PDI usada en otros sitios,
    aquí asignamos a cada persona el sector donde más ha cobrado en el
    año — lo cual es lo coherente para deducir el elemento de coste de
    cargos académicos (típicamente PDI o PVI).
    """
    del _mtime_pdi, _mtime_pvi
    partes: list[pl.DataFrame] = []
    for sector, path in _SECTOR_PATHS.items():
        if not path.exists():
            continue
        df = pl.read_parquet(path)
        if "per_id" not in df.columns or "importe" not in df.columns:
            continue
        partes.append(
            df.group_by("per_id").agg(pl.col("importe").sum().alias("imp"))
            .with_columns(pl.lit(sector).alias("sector"))
        )
    if not partes:
        return {}
    df = pl.concat(partes, how="vertical")
    df = (
        df.sort(["per_id", "imp"], descending=[False, True])
        .group_by("per_id").first()
        .select("per_id", "sector")
    )
    return {int(r["per_id"]): str(r["sector"]) for r in df.iter_rows(named=True)}


def _elemento_de_coste_cargos(per_id: int) -> str | None:
    """ZZZ-XXX-cargos para una persona, según sector con más actividad
    en el año y categoría última en CR 19/64.
    """
    from coana.fase1.nóminas import _elemento_coste_pdi, _elemento_coste_pvi
    sectores = _sector_principal_personal_cached(
        _mtime_ns(PATH_PDI), _mtime_ns(PATH_PVI),
    )
    nom_path = DIR_ENTRADA / "nóminas" / "nóminas y seguridad social.xlsx"
    cats = _categoría_última_cached(
        _mtime_ns(nom_path), _mtime_ns(PATH_EXP_RH_PERSONAL),
    )
    sec = sectores.get(int(per_id))
    cat = cats.get(int(per_id))
    if not cat:
        return None
    if sec == "PDI":
        return _elemento_coste_pdi(cat, "19")
    if sec == "PVI":
        return _elemento_coste_pvi(cat, None, None, "19", None, None)
    return None


@lru_cache(maxsize=2)
def _personas_cargos_personal_cached(path: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path)
    if not p.exists():
        return pl.DataFrame()
    return read_excel(p)


@lru_cache(maxsize=2)
def _cargos_catálogo_cached(path: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path)
    if not p.exists():
        return pl.DataFrame()
    return read_excel(p)


@lru_cache(maxsize=2)
def _proyectos_cr19_64_por_persona_cached(path: str, mtime_ns: int) -> dict[int, list[str]]:
    """{per_id: [proyectos generales ordenados por importe desc]} para líneas
    con concepto retributivo 19 o 64 en el año analizado y proyecto general.
    Los cargos en proyecto específico ya están imputados a su proyecto y
    no se cuentan aquí."""
    from coana.fase1.nóminas.regla_23 import _PROYECTOS_GENERALES
    del mtime_ns
    p = Path(path)
    if not p.exists():
        return {}
    nóminas = read_excel(p)
    exp_rh_path = DIR_ENTRADA / "nóminas" / "expedientes recursos humanos.xlsx"
    if not exp_rh_path.exists():
        return {}
    exp = read_excel(exp_rh_path)
    cr = pl.col("concepto_retributivo").cast(pl.Utf8)
    proy = pl.col("proyecto").cast(pl.Utf8)
    agg = (
        nóminas.join(exp.select("expediente", "per_id"), on="expediente", how="inner")
        .filter(cr.is_in(["19", "64"]))
        .filter(proy.is_in(list(_PROYECTOS_GENERALES)))
        .filter(pl.col("fecha").dt.year() == _AÑO_PERSONAL)
        .group_by("per_id", "proyecto")
        .agg(pl.col("importe").sum().alias("imp"))
        .sort(["per_id", "imp"], descending=[False, True])
    )
    out: dict[int, list[str]] = {}
    for row in agg.iter_rows(named=True):
        out.setdefault(int(row["per_id"]), []).append(str(row["proyecto"]))
    return out


def _proyectos_cr19_64_persona(per_id: int) -> str:
    """Cadena con los proyectos donde la persona cobró CR 19/64 en el año
    analizado, separados por coma y ordenados por importe descendente."""
    p = DIR_ENTRADA / "nóminas" / "nóminas y seguridad social.xlsx"
    mapa = _proyectos_cr19_64_por_persona_cached(str(p), _mtime_ns(p))
    return ", ".join(mapa.get(int(per_id), []))


@lru_cache(maxsize=2)
def _cargos_rd_cached(path: str, mtime_ns: int) -> dict[int, float]:
    """{cargo_real_decreto: importe_mensual} del RD 1086/1989."""
    del mtime_ns
    p = Path(path)
    if not p.exists():
        return {}
    df = read_excel(p)
    return {
        int(r["cargo_real_decreto"]): float(r["importe_mensual"])
        for r in df.iter_rows(named=True)
    }


def _cargos_de_persona(per_id: int) -> pl.DataFrame:
    """Cargos de una persona en el año analizado con el importe imputado
    como UC anual.

    Reparte el total CR 19/64 percibido por la persona en el año entre
    los cargos ocupados, proporcionalmente al producto
    `días_en_año × importe_mensual_RD`, donde el importe RD viene de
    asimilar el cargo UJI a uno de los 8 tipos del RD 1086/1989
    (vía `cargo_asimilado` en `cargos.xlsx`).

    Anomalías marcadas con `_anomalía`:
    - Cargo sin `cargo_asimilado`: ignorado del reparto (peso 0).
    - Persona con cobro CR 19/64 > 0 pero sin cargos asimilados:
      la masa no imputable se reporta como anomalía global.
    """
    from datetime import date

    pc = _personas_cargos_personal_cached(
        str(PATH_PERSONAS_CARGOS), _mtime_ns(PATH_PERSONAS_CARGOS),
    )
    cat = _cargos_catálogo_cached(
        str(PATH_CARGOS_CAT), _mtime_ns(PATH_CARGOS_CAT),
    )
    rd = _cargos_rd_cached(str(PATH_CARGOS_RD), _mtime_ns(PATH_CARGOS_RD))
    if pc.is_empty():
        return pl.DataFrame()

    año = _AÑO_PERSONAL
    fin_año = date(año, 12, 31)
    inicio_año = date(año, 1, 1)

    sub = pc.filter(pl.col("per_id") == per_id)
    if sub.is_empty():
        return pl.DataFrame()
    activo = (
        pl.col("fecha_inicio").cast(pl.Date) <= pl.lit(fin_año)
    ) & (
        pl.col("fecha_fin").is_null()
        | (pl.col("fecha_fin").cast(pl.Date) >= pl.lit(inicio_año))
    )
    sub = sub.filter(activo).with_columns(pl.col("cargo").cast(pl.Utf8))
    if sub.is_empty():
        return pl.DataFrame()

    # Catálogo: añade nombre, cargo_asimilado, dedicación, actividad y centro.
    if not cat.is_empty():
        cat_min = cat.with_columns(pl.col("cargo").cast(pl.Utf8)).select(
            "cargo", "nombre", "cargo_asimilado",
            "dedicación", "actividad", "centro",
        )
        sub = sub.join(cat_min, on="cargo", how="left")

    rows = sub.to_dicts()

    def _días_en_año(r) -> int:
        """Días en 2025 efectivamente *cobrados* del cargo.

        Se usan `fecha_inicio_cobra` y `fecha_fin_cobra` (NO las fechas de
        nombramiento/cese). Si no hay periodo de cobro, el cargo no
        genera reparto (días = 0) aunque siga vigente — por ejemplo, la
        dirección de una cátedra no remunerada.
        """
        fi = r.get("fecha_inicio_cobra")
        ff = r.get("fecha_fin_cobra")
        if fi is None:
            return 0
        fi_d = fi.date() if hasattr(fi, "date") else fi
        ff_d = ff.date() if (ff is not None and hasattr(ff, "date")) else ff
        inicio_ef = max(fi_d, inicio_año)
        fin_ef = min(ff_d, fin_año) if ff_d is not None else fin_año
        if fin_ef < inicio_ef:
            return 0
        return (fin_ef - inicio_ef).days + 1

    # Total CR 19/64 anual para la persona.
    proyectos_lista = _proyectos_cr19_64_por_persona_cached(
        str(DIR_ENTRADA / "nóminas" / "nóminas y seguridad social.xlsx"),
        _mtime_ns(DIR_ENTRADA / "nóminas" / "nóminas y seguridad social.xlsx"),
    ).get(int(per_id), [])
    total_persona = _total_cr19_64_persona(per_id)

    # Calcula días e importe RD por fila; marca anomalías. Los cargos sin
    # periodo de cobro (días = 0) no se conservan: no generan UC y solo
    # ensucian la vista. La asimilación al RD se considera anomalía solo
    # si la persona cobra efectivamente por ese cargo.
    rows_remunerados: list[dict] = []
    pesos: list[float] = []
    for r in rows:
        días = _días_en_año(r)
        if días == 0:
            continue  # cargo no remunerado: descartado
        r["días_2025"] = días
        tipo_rd = r.get("cargo_asimilado")
        try:
            tipo_rd = int(tipo_rd) if tipo_rd is not None else None
        except (TypeError, ValueError):
            tipo_rd = None
        if tipo_rd is None or tipo_rd not in rd:
            r["importe_rd"] = None
            r["_anomalía"] = "sin asimilación a RD"
            pesos.append(0.0)
        else:
            r["importe_rd"] = rd[tipo_rd]
            r["_anomalía"] = ""
            pesos.append(días * rd[tipo_rd])
        rows_remunerados.append(r)
    rows = rows_remunerados
    if not rows:
        return pl.DataFrame()

    suma_pesos = sum(pesos)
    elemento = _elemento_de_coste_cargos(per_id)
    proyectos_str = ", ".join(proyectos_lista)

    for i, (r, w) in enumerate(zip(rows, pesos), start=1):
        r["id"] = f"C-{i:03d}"
        if suma_pesos > 0:
            r["peso"] = round(w, 2) if w > 0 else None
            r["importe_uc"] = round((w / suma_pesos) * total_persona, 2) if w > 0 else None
        else:
            r["peso"] = None
            r["importe_uc"] = None
        r["uc_elemento_de_coste"] = elemento or ""
        # CC y actividad: del propio catálogo de cargos (o vacío si patrón
        # pendiente de resolver — la lógica de patrones la añadiremos
        # cuando esté definida).
        r["uc_centro_de_coste"] = str(r.get("centro") or "")
        r["uc_actividad"] = str(r.get("actividad") or "")
        r["proyectos"] = proyectos_str

    return pl.DataFrame(rows).sort("importe_uc", descending=True, nulls_last=True)


def _total_cr19_64_persona(per_id: int) -> float:
    """Suma de importes de líneas con CR 19/64 en el año analizado y
    proyecto general (los cargos en proyecto específico ya generaron su
    UC en `generar_uc_cargos` y no se reparten aquí).
    """
    from coana.fase1.nóminas.regla_23 import _PROYECTOS_GENERALES
    p = DIR_ENTRADA / "nóminas" / "nóminas y seguridad social.xlsx"
    if not p.exists():
        return 0.0
    nóminas = read_excel(p)
    exp_rh_path = DIR_ENTRADA / "nóminas" / "expedientes recursos humanos.xlsx"
    if not exp_rh_path.exists():
        return 0.0
    exp = read_excel(exp_rh_path)
    cr = pl.col("concepto_retributivo").cast(pl.Utf8)
    proy = pl.col("proyecto").cast(pl.Utf8)
    total = (
        nóminas.join(exp.select("expediente", "per_id"), on="expediente", how="inner")
        .filter(cr.is_in(["19", "64"]))
        .filter(proy.is_in(list(_PROYECTOS_GENERALES)))
        .filter(pl.col("fecha").dt.year() == _AÑO_PERSONAL)
        .filter(pl.col("per_id") == per_id)
        .get_column("importe").sum()
    )
    return float(total or 0.0)


_COLS_CARGOS_EXP: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID", format="text"),
    ColumnSpec(name="cargo", label="Cargo", format="text"),
    ColumnSpec(name="nombre", label="Nombre", format="text"),
    ColumnSpec(name="cargo_asimilado", label="Tipo RD", format="id"),
    ColumnSpec(name="importe_rd", label="Importe RD/mes", format="euro"),
    ColumnSpec(name="dedicación", label="Dedicación", format="float"),
    ColumnSpec(name="fecha_inicio", label="Inicio", format="date"),
    ColumnSpec(name="fecha_fin", label="Fin", format="date"),
    ColumnSpec(name="días_2025", label="Días en año", format="int"),
    ColumnSpec(name="peso", label="Peso (días × RD)", format="euro"),
    ColumnSpec(name="importe_uc", label="Importe UC", format="euro"),
    ColumnSpec(name="uc_elemento_de_coste", label="UC · elemento", format="text"),
    ColumnSpec(name="uc_centro_de_coste", label="UC · centro", format="text"),
    ColumnSpec(name="uc_actividad", label="UC · actividad", format="text"),
    ColumnSpec(name="proyectos", label="Proyectos CR 19/64", format="text"),
    ColumnSpec(name="_anomalía", label="Anomalía", format="text"),
]


def grupos_lineas_nomina(sector: str, expediente: int) -> GruposLineasResponse:
    """Metadatos (label, n, importe) de los grupos de un expediente.

    Si la persona del expediente está en clases pasivas (PDI funcionario
    sin SS cotizada), la pestaña «Costes sociales» se sustituye por
    «Costes sociales calculados» con el detalle del cálculo. Las UC
    generadas se muestran en una tabla aparte debajo del bloque de
    pestañas (no aparecen como pestaña).
    """
    df = _nominas_raw()
    grupos_raw: list[tuple[str, int, float]] = []
    if not df.is_empty() and "expediente" in df.columns:
        sub = df.filter(pl.col("expediente") == expediente)
        for label, g in _grupos_lineas(sub, sector):
            imp = float(g["importe"].sum() or 0) if "importe" in g.columns else 0.0
            grupos_raw.append((label, g.height, imp))

    per_id = _per_id_de_expediente(expediente)
    calc_df: pl.DataFrame | None = None
    if per_id is not None:
        calc_df = _costes_calculados_de_persona(per_id)
        if calc_df.is_empty():
            calc_df = None

    out: list[GrupoLineas] = []
    for label, n, imp in grupos_raw:
        if label == "Costes sociales" and calc_df is not None:
            out.append(GrupoLineas(
                label=COSTES_CALCULADOS_LABEL,
                n=calc_df.height,
                importe=float(calc_df["importe_total"].sum() or 0),
            ))
        else:
            out.append(GrupoLineas(label=label, n=n, importe=imp))
    # Si no hay líneas de CS reales pero sí hay calculados, añadir la
    # pestaña al final.
    if calc_df is not None and not any(g.label == COSTES_CALCULADOS_LABEL for g in out):
        out.append(GrupoLineas(
            label=COSTES_CALCULADOS_LABEL,
            n=calc_df.height,
            importe=float(calc_df["importe_total"].sum() or 0),
        ))
    return GruposLineasResponse(grupos=out)


_UC_PATHS_POR_SECTOR = {
    "PDI": DIR_NOMINAS / "uc_pdi.parquet",
    "PTGAS": DIR_NOMINAS / "uc_ptgas.parquet",
    "PVI": DIR_NOMINAS / "uc_pvi.parquet",
}

# UC adicionales que pueden aplicar a un expediente con independencia
# del sector: despidos (CR 47), indemnizaciones por asistencia (CR 48),
# y cargos asociados a un proyecto específico (CR 19/64 cuando el
# proyecto no es general). Estas se generan en parquets separados; al
# consultarlas por expediente las consolidamos aquí.
_UC_PATHS_TRANSVERSALES = [
    DIR_NOMINAS / "uc_despidos.parquet",
    DIR_NOMINAS / "uc_indemnizaciones_asistencias.parquet",
    DIR_NOMINAS / "uc_cargos.parquet",
]

# UC de seguridad social: una fila por (per_id, actividad, CC) sin
# expediente concreto. Se asocian al expediente vía per_id.
PATH_PERSONA_UC = DIR_NOMINAS / "persona_uc.parquet"

# UC del reparto CR 19/64 (proyecto general) entre los cargos
# académicos de cada persona, incluyendo la parte extra (CR 68). Sin
# expediente: se asocia al expediente vía per_id.
PATH_CARGOS_UC = DIR_NOMINAS / "cargos_uc.parquet"


_COLS_UC_EXP: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID UC", format="text"),
    ColumnSpec(name="elemento_de_coste", label="Elemento", format="text"),
    ColumnSpec(name="centro_de_coste", label="Centro", format="text"),
    ColumnSpec(name="actividad", label="Actividad", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="origen", label="Origen", format="text"),
    ColumnSpec(name="origen_id", label="Origen ID", format="text"),
    ColumnSpec(name="origen_porción", label="Porción", format="float"),
]


def _uc_de_expediente(sector: str, expediente: int) -> pl.DataFrame:
    """Consolida las UC del expediente desde el parquet del sector, los
    parquets transversales (despidos, indemnizaciones por asistencia,
    cargos asociados a proyecto específico) y las UC de costes sociales
    (que son por per_id y se asocian vía la persona del expediente)."""
    partes: list[pl.DataFrame] = []
    rutas: list[Path] = []
    sector_path = _UC_PATHS_POR_SECTOR.get(sector)
    if sector_path is not None:
        rutas.append(sector_path)
    rutas.extend(_UC_PATHS_TRANSVERSALES)
    for path in rutas:
        df = _safe_read(path)
        if df is None or df.is_empty() or "expediente" not in df.columns:
            continue
        sub = df.filter(pl.col("expediente") == expediente).drop("expediente")
        if not sub.is_empty():
            partes.append(sub)

    # UC de SS y de cargos generales (CR 19/64 + extra CR 68): por
    # per_id, no por expediente.
    per_id = _per_id_de_expediente(expediente)
    if per_id is not None:
        ss_df = _safe_read(PATH_PERSONA_UC)
        if ss_df is not None and not ss_df.is_empty() and "tipo" in ss_df.columns:
            ss = ss_df.filter(
                (pl.col("tipo") == "coste social") & (pl.col("per_id") == per_id)
            )
            if not ss.is_empty():
                drop_cols = [c for c in ("expediente", "per_id", "tipo") if c in ss.columns]
                partes.append(ss.drop(drop_cols))

        cargos_df = _safe_read(PATH_CARGOS_UC)
        if cargos_df is not None and not cargos_df.is_empty():
            cu = cargos_df.filter(pl.col("per_id") == per_id)
            if not cu.is_empty():
                cu_norm = cu.select(
                    "id",
                    "elemento_de_coste",
                    "centro_de_coste",
                    "actividad",
                    pl.col("importe_uc").alias("importe"),
                    "origen",
                    "origen_id",
                    "origen_porción",
                )
                partes.append(cu_norm)

    if not partes:
        return pl.DataFrame()
    return pl.concat(partes, how="diagonal")


def listar_uc_expediente(
    sector: str, expediente: int, params: QueryParams,
) -> ListResponse:
    """UC generadas durante la fase 1 para un expediente concreto."""
    df = _uc_de_expediente(sector, expediente)
    if df.is_empty():
        return ListResponse(columns=_COLS_UC_EXP, rows=[], total=0)
    nombres = [c.name for c in _COLS_UC_EXP if c.name in df.columns]
    df = df.select(nombres)
    df, total, stats = apply_query(
        df, params,
        search_columns=[
            "id", "elemento_de_coste", "centro_de_coste", "actividad",
            "origen", "origen_id",
        ],
    )
    return ListResponse(
        columns=_COLS_UC_EXP,
        rows=_serialize(df.to_dicts()),
        total=total,
        column_stats=stats,
    )


_COLS_COSTES_SOC_CALC_EXP: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="total_retribuido", label="Total retribuido", format="euro"),
    ColumnSpec(name="base", label="Base cotización", format="euro"),
    ColumnSpec(name="contingencias_comunes", label="Cont. comunes", format="euro"),
    ColumnSpec(name="mei", label="MEI", format="euro"),
    ColumnSpec(name="formación_profesional", label="Form. prof.", format="euro"),
    ColumnSpec(name="cuota_solidaridad_tramo1", label="Solidaridad T1", format="euro"),
    ColumnSpec(name="cuota_solidaridad_tramo2", label="Solidaridad T2", format="euro"),
    ColumnSpec(name="cuota_solidaridad_tramo3", label="Solidaridad T3", format="euro"),
    ColumnSpec(name="cuota_solidaridad", label="Cuota solidaridad", format="euro"),
    ColumnSpec(name="importe_total", label="Importe total", format="euro"),
]


def listar_lineas_nomina(
    expediente: int,
    params: QueryParams,
    sector: str | None = None,
    grupo: str | None = None,
) -> ListResponse:
    """Líneas de nómina (importes y SS) de un expediente.

    Si ``sector`` y ``grupo`` están informados, devuelve solo las
    líneas pertenecientes a ese grupo (Costes sociales, Retribuciones
    ordinarias, etc.) según la clasificación del visor Streamlit.

    Caso especial: si ``grupo`` es «Costes sociales calculados»,
    devuelve la fila de detalle de cálculo de la persona (que es de
    la persona, no del expediente, pero se muestra como pestaña de éste
    para los PDI funcionarios en clases pasivas).
    """
    if grupo == COSTES_CALCULADOS_LABEL:
        per_id = _per_id_de_expediente(expediente)
        if per_id is None:
            return ListResponse(columns=_COLS_COSTES_SOC_CALC_EXP, rows=[], total=0)
        df = _costes_calculados_de_persona(per_id)
        if df.is_empty():
            return ListResponse(columns=_COLS_COSTES_SOC_CALC_EXP, rows=[], total=0)
        nombres = [c.name for c in _COLS_COSTES_SOC_CALC_EXP if c.name in df.columns]
        df = df.select(nombres)
        df, total, stats = apply_query(df, params)
        return ListResponse(
            columns=_COLS_COSTES_SOC_CALC_EXP,
            rows=_serialize(df.to_dicts()),
            total=total,
            column_stats=stats,
        )

    if grupo == CARGOS_LABEL:
        per_id = _per_id_de_expediente(expediente)
        if per_id is None:
            return ListResponse(columns=_COLS_CARGOS_EXP, rows=[], total=0)
        df = _cargos_de_persona(per_id)
        if df.is_empty():
            return ListResponse(columns=_COLS_CARGOS_EXP, rows=[], total=0)
        nombres = [c.name for c in _COLS_CARGOS_EXP if c.name in df.columns]
        df = df.select(nombres)
        df, total, stats = apply_query(
            df, params, search_columns=["nombre", "cargo"],
        )
        return ListResponse(
            columns=_COLS_CARGOS_EXP,
            rows=_serialize(df.to_dicts()),
            total=total,
            column_stats=stats,
        )

    df = _nominas_raw()
    if df.is_empty() or "expediente" not in df.columns:
        return ListResponse(columns=_COLS_LINEAS_NOMINA, rows=[], total=0)
    df = df.filter(pl.col("expediente") == expediente)
    if sector and grupo:
        partes = _grupos_lineas(df, sector)
        seleccion = next((g for label, g in partes if label == grupo), None)
        if seleccion is None:
            df = df.head(0)
        else:
            df = seleccion
    nombres = [c.name for c in _COLS_LINEAS_NOMINA if c.name in df.columns]
    df = df.select(nombres)
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_LINEAS)
    return ListResponse(
        columns=_COLS_LINEAS_NOMINA,
        rows=_serialize(df.to_dicts()),
        total=total,
        column_stats=stats,
    )


def obtener_expediente(sector: str, expediente: int) -> RecordResponse | None:
    path = _SECTOR_PATHS.get(sector)
    if path is None:
        return None
    df = _safe_read(path)
    if df is None:
        return None
    fila = df.filter(pl.col("expediente") == expediente)
    if fila.is_empty():
        return None
    fila = _enriquecer_per_id(fila)
    row = fila.row(0, named=True)
    main = [
        FieldValue(name="sector", label="Sector", value=sector, format="text"),
    ] + [
        FieldValue(name=c.name, label=c.label, value=row.get(c.name), format=c.format)
        for c in _COLS_EXP if c.name in row
    ]

    # Sección de puesto: categoría y categoría_plaza distintas
    # observadas en las líneas de nómina del expediente.
    sections: list[RecordSection] = []
    raw = _nominas_raw()
    if not raw.is_empty() and "expediente" in raw.columns:
        sub = raw.filter(pl.col("expediente") == expediente)
        if not sub.is_empty():
            from coana.web.services.lookups import (
                lookup_categoria, lookup_categoria_plaza,
            )

            def _join_with_lookup(valores: list[str], fn) -> str:
                """Devuelve "COD — Nombre, COD2 — Nombre2…" sin duplicados."""
                vistos: list[str] = []
                for v in valores:
                    if v in (None, "") or v in vistos:
                        continue
                    vistos.append(v)
                partes: list[str] = []
                for v in vistos:
                    nom = fn(v).get("nombre") or ""
                    partes.append(f"{v} — {nom}" if nom else str(v))
                return " · ".join(partes)

            campos_puesto: list[FieldValue] = []
            if "categoría_plaza" in sub.columns:
                vals = sub["categoría_plaza"].drop_nulls().unique().to_list()
                if vals:
                    campos_puesto.append(FieldValue(
                        name="categoría_plaza",
                        label="Categoría plaza",
                        value=_join_with_lookup(vals, lookup_categoria_plaza),
                        format="text",
                    ))
            if "categoría" in sub.columns:
                vals = sub["categoría"].drop_nulls().unique().to_list()
                if vals:
                    campos_puesto.append(FieldValue(
                        name="categoría",
                        label="Categoría RR.HH.",
                        value=_join_with_lookup(vals, lookup_categoria),
                        format="text",
                    ))
            if "sector_plaza" in sub.columns:
                vals = sub["sector_plaza"].drop_nulls().unique().to_list()
                if vals:
                    campos_puesto.append(FieldValue(
                        name="sector_plaza",
                        label="Sector plaza",
                        value=", ".join(str(v) for v in vals),
                        format="text",
                    ))
            if "centro_plaza" in sub.columns:
                vals = sub["centro_plaza"].drop_nulls().unique().to_list()
                if vals:
                    campos_puesto.append(FieldValue(
                        name="centro_plaza",
                        label="Centro plaza",
                        value=", ".join(str(v) for v in vals),
                        format="text",
                    ))
            if campos_puesto:
                sections.append(RecordSection(
                    label="Puesto", fields=campos_puesto,
                ))

    return RecordResponse(main=main, sections=sections)


# ----------------------------------------------------------------------
# Multiexpediente
# ----------------------------------------------------------------------

_COLS_MULTI: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="sectores_str", label="Sectores", format="text"),
    ColumnSpec(name="n_sectores", label="N sectores", format="int"),
    ColumnSpec(name="n_PDI", label="N PDI", format="int"),
    ColumnSpec(name="n_PTGAS", label="N PTGAS", format="int"),
    ColumnSpec(name="n_PVI", label="N PVI", format="int"),
]


_MESES_ES = ["ene", "feb", "mar", "abr", "may", "jun",
             "jul", "ago", "sep", "oct", "nov", "dic"]


def matriz_mensual(per_id: int, params: QueryParams) -> ListResponse:
    """Matriz expediente × mes con importes para una persona.

    Pensada para la vista Multiexpediente: una persona con varios
    expedientes en el año. Filas = expedientes; columnas = meses en
    los que hay nómina; celdas = suma de importes de la nómina ese mes.
    """
    # 1) Expedientes y sector de la persona.
    pares: list[tuple[int, str]] = []
    for sector, path in _SECTOR_PATHS.items():
        df = _safe_read(path)
        if df is None or "per_id" not in df.columns:
            continue
        sub = df.filter(pl.col("per_id") == per_id)
        for r in sub.iter_rows(named=True):
            pares.append((int(r["expediente"]), sector))
    if not pares:
        return ListResponse(columns=[], rows=[], total=0)
    expedientes = [p[0] for p in pares]
    sector_de = {p[0]: p[1] for p in pares}

    # 2) Líneas de nómina (importes y SS) de esos expedientes.
    raw = _nominas_raw()
    if raw.is_empty() or "fecha" not in raw.columns:
        return ListResponse(columns=[], rows=[], total=0)
    sub = raw.filter(pl.col("expediente").is_in(expedientes))
    if sub.is_empty():
        return ListResponse(columns=[], rows=[], total=0)

    # 3) Agregar por (expediente, año-mes).
    g = (
        sub.with_columns(pl.col("fecha").dt.strftime("%Y-%m").alias("yyyymm"))
        .group_by("expediente", "yyyymm")
        .agg(pl.col("importe").sum().alias("imp"))
    )

    # 4) Pivote a wide. Dejamos los huecos como null para que el
    # frontend los muestre en blanco (un 0,00 € visual confunde con
    # «hubo nómina y dio cero»).
    pivot = g.pivot(values="imp", index="expediente", on="yyyymm")

    yyyymm_cols = sorted(c for c in pivot.columns if c != "expediente")
    años = sorted({c[:4] for c in yyyymm_cols})
    multi_año = len(años) > 1

    def _label(yyyymm: str) -> str:
        # "2025-03" → "mar 2025" (o "mar" si solo hay un año).
        try:
            mes = int(yyyymm[5:7])
        except ValueError:
            return yyyymm
        nombre = _MESES_ES[mes - 1] if 1 <= mes <= 12 else yyyymm
        return f"{nombre} {yyyymm[:4]}" if multi_año else nombre

    pivot = pivot.with_columns(
        pl.col("expediente").cast(pl.Int64),
        pl.col("expediente").map_elements(
            lambda e: sector_de.get(int(e), ""), return_dtype=pl.Utf8,
        ).alias("sector"),
        pl.sum_horizontal(yyyymm_cols).alias("total"),
    )
    final_cols = ["sector", "expediente"] + yyyymm_cols + ["total"]
    pivot = pivot.select(final_cols).sort("total", descending=True)

    columns = (
        [
            ColumnSpec(name="sector", label="Sector", format="text"),
            ColumnSpec(name="expediente", label="Expediente", format="id"),
        ]
        + [ColumnSpec(name=m, label=_label(m), format="euro") for m in yyyymm_cols]
        + [ColumnSpec(name="total", label="Total", format="euro")]
    )
    df_q, total, stats = apply_query(pivot, params, search_columns=["sector"])
    return ListResponse(
        columns=columns,
        rows=df_q.to_dicts(),
        total=total,
        column_stats=stats,
    )


def listar_multiexpediente(params: QueryParams) -> ListResponse:
    df = _safe_read(PATH_MULTI)
    if df is None:
        return ListResponse(columns=_COLS_MULTI, rows=[], total=0)
    df = _enriquecer_per_id(df)
    if "sectores" in df.columns:
        df = df.with_columns(
            pl.col("sectores").list.join(", ").alias("sectores_str"),
        )
    df = df.select([c.name for c in _COLS_MULTI if c.name in df.columns])
    df, total, stats = apply_query(df, params, search_columns=["persona", "sectores_str"])
    return ListResponse(columns=_COLS_MULTI, rows=_serialize(df.to_dicts()), total=total, column_stats=stats)


# ----------------------------------------------------------------------
# Persona (vista agregada)
# ----------------------------------------------------------------------

_COLS_PERSONA: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="n_uc", label="N UC", format="int"),
    ColumnSpec(name="importe_total", label="Importe total", format="euro"),
    ColumnSpec(name="ss_total", label="SS total", format="euro"),
]


def listar_personas(params: QueryParams) -> ListResponse:
    uc = _safe_read(PATH_UC)
    ss = _safe_read(PATH_SS)
    if uc is None and ss is None:
        return ListResponse(columns=_COLS_PERSONA, rows=[], total=0)

    if uc is not None and "per_id" in uc.columns:
        agg_uc = uc.group_by("per_id").agg(
            pl.len().alias("n_uc"),
            pl.col("importe").sum().alias("importe_total"),
        )
    else:
        agg_uc = pl.DataFrame(schema={"per_id": pl.Int64, "n_uc": pl.UInt32, "importe_total": pl.Float64})

    if ss is not None and "per_id" in ss.columns:
        col_ss = "ss_proporcional" if "ss_proporcional" in ss.columns else "ss_total"
        agg_ss = ss.group_by("per_id").agg(pl.col(col_ss).sum().alias("ss_total"))
    else:
        agg_ss = pl.DataFrame(schema={"per_id": pl.Int64, "ss_total": pl.Float64})

    df = agg_uc.join(agg_ss, on="per_id", how="full", coalesce=True).with_columns(
        pl.col("n_uc").fill_null(0),
        pl.col("importe_total").fill_null(0.0),
        pl.col("ss_total").fill_null(0.0),
    )
    df = _enriquecer_per_id(df)
    df = df.select([c.name for c in _COLS_PERSONA if c.name in df.columns])
    df, total, stats = apply_query(df, params, search_columns=["persona"])
    return ListResponse(columns=_COLS_PERSONA, rows=_serialize(df.to_dicts()), total=total, column_stats=stats)


_COLS_UC_PERSONA: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID", format="text"),
    ColumnSpec(name="elemento_de_coste", label="Elemento", format="text"),
    ColumnSpec(name="centro_de_coste", label="Centro", format="text"),
    ColumnSpec(name="actividad", label="Actividad", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="origen", label="Origen", format="text"),
    ColumnSpec(name="tipo", label="Tipo", format="text"),
]


def obtener_persona(per_id: int) -> RecordResponse | None:
    """Devuelve datos básicos de la persona y sus secciones de UC y SS."""
    personas = _personas()
    fila = personas.filter(pl.col("per_id") == per_id)
    nombre = fila.row(0, named=True).get("persona") if not fila.is_empty() else None

    main: list[FieldValue] = [
        FieldValue(name="per_id", label="per_id", value=per_id, format="id"),
        FieldValue(name="persona", label="Persona", value=nombre, format="text"),
    ]

    sections: list[RecordSection] = []

    uc = _safe_read(PATH_UC)
    if uc is not None and "per_id" in uc.columns:
        uc_p = uc.filter(pl.col("per_id") == per_id)
        if not uc_p.is_empty():
            n = uc_p.height
            imp = float(uc_p["importe"].sum() or 0)
            sections.append(RecordSection(
                label="Resumen UC",
                fields=[
                    FieldValue(name="n_uc", label="N UC", value=n, format="int"),
                    FieldValue(name="importe", label="Importe total", value=imp, format="euro"),
                ],
            ))

    ss = _safe_read(PATH_SS)
    if ss is not None and "per_id" in ss.columns:
        ss_p = ss.filter(pl.col("per_id") == per_id)
        if not ss_p.is_empty():
            col_ss = "ss_proporcional" if "ss_proporcional" in ss.columns else "ss_total"
            n = ss_p.height
            imp = float(ss_p[col_ss].sum() or 0)
            sections.append(RecordSection(
                label="Reparto SS",
                fields=[
                    FieldValue(name="n_ss", label="Pares act/CC", value=n, format="int"),
                    FieldValue(name="ss_total", label="SS total", value=imp, format="euro"),
                ],
            ))

    return RecordResponse(main=main, sections=sections)


def listar_uc_persona(per_id: int, params: QueryParams) -> ListResponse:
    uc = _safe_read(PATH_UC)
    if uc is None or "per_id" not in uc.columns:
        return ListResponse(columns=_COLS_UC_PERSONA, rows=[], total=0)
    df = uc.filter(pl.col("per_id") == per_id)
    df = df.select([c.name for c in _COLS_UC_PERSONA if c.name in df.columns])
    df, total, stats = apply_query(
        df, params,
        search_columns=["id", "elemento_de_coste", "centro_de_coste", "actividad", "tipo"],
    )
    return ListResponse(columns=_COLS_UC_PERSONA, rows=_serialize(df.to_dicts()), total=total, column_stats=stats)


# ----------------------------------------------------------------------
# Anomalías PDI
# ----------------------------------------------------------------------

_COLS_ANOM: list[ColumnSpec] = [
    ColumnSpec(name="asignatura", label="Asignatura", format="text"),
    ColumnSpec(name="titulación", label="Titulación", format="text"),
    ColumnSpec(name="créditos_impartidos", label="Créditos", format="float"),
    ColumnSpec(name="per_id", label="per_id", format="id"),
]


def listar_anomalias_pdi(params: QueryParams) -> ListResponse:
    df = _safe_read(PATH_ANOM_PDI)
    if df is None:
        return ListResponse(columns=_COLS_ANOM, rows=[], total=0)
    nombres = [c.name for c in _COLS_ANOM if c.name in df.columns]
    df = df.select(nombres)
    df, total, stats = apply_query(df, params)
    return ListResponse(columns=_COLS_ANOM, rows=_serialize(df.to_dicts()), total=total, column_stats=stats)


# ----------------------------------------------------------------------
# Costes sociales calculados (PDI funcionario clases pasivas)
# ----------------------------------------------------------------------

_COLS_COSTES_SOC_CALC: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="total_retribuido", label="Total retribuido", format="euro"),
    ColumnSpec(name="base", label="Base cotización", format="euro"),
    ColumnSpec(name="contingencias_comunes", label="Cont. comunes", format="euro"),
    ColumnSpec(name="mei", label="MEI", format="euro"),
    ColumnSpec(name="formación_profesional", label="Form. prof.", format="euro"),
    ColumnSpec(name="cuota_solidaridad_tramo1", label="Solidaridad T1", format="euro"),
    ColumnSpec(name="cuota_solidaridad_tramo2", label="Solidaridad T2", format="euro"),
    ColumnSpec(name="cuota_solidaridad_tramo3", label="Solidaridad T3", format="euro"),
    ColumnSpec(name="cuota_solidaridad", label="Cuota solidaridad", format="euro"),
    ColumnSpec(name="importe_total", label="Importe total", format="euro"),
]


def listar_costes_sociales_calculados(params: QueryParams) -> ListResponse:
    """Detalle por persona del coste social calculado (clases pasivas PDI)."""
    df = _safe_read(PATH_COSTES_SOC_CALC)
    if df is None:
        return ListResponse(columns=_COLS_COSTES_SOC_CALC, rows=[], total=0)
    df = _enriquecer_per_id(df)
    nombres = [c.name for c in _COLS_COSTES_SOC_CALC if c.name in df.columns]
    df = df.select(nombres)
    df, total, stats = apply_query(df, params, search_columns=["persona"])
    return ListResponse(
        columns=_COLS_COSTES_SOC_CALC,
        rows=_serialize(df.to_dicts()),
        total=total,
        column_stats=stats,
    )
