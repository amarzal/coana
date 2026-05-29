"""Vista persona-360 para PDI y PVI.

Consolida en una sola página por persona del sector todo el flujo de
retribuciones y cotizaciones que se han imputado a la persona y todas
las UC que el sistema ha generado a partir de ese dinero. La métrica
clave es el ``Δ`` (delta de cuadre):

    Δ = (bruto cobrado + SS cotizada + SS calculada)
        − (Σ UC retributivas + Σ UC SS)

Que debe ser ≈ 0 para que el sistema esté correctamente balanceado.
Cualquier Δ significativamente distinto de 0 indica una vía
contabilizada en nómina que no termina convertida en UC (o, al
contrario, una UC que duplica un importe ya contabilizado por otra
vía).

El módulo se apoya en los parquets ya producidos por la fase 1 y no
toca ningún Excel de entrada salvo el de nóminas y el catálogo de
expedientes RR.HH. (ambos cacheados por `_safe_read`).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.util.configuración import cfg_int, cfg_tuple
from coana.web.deps import DIR_AUX, DIR_ENTRADA, DIR_FASE1, _mtime_ns, read_parquet
from coana.web.schemas.common import (
    ColumnSpec,
    Kpi,
    KpiPanel,
    ListResponse,
)
from coana.web.services.query import QueryParams, apply_query


# -------------------------------------------------------------------- paths
DIR_NOMINAS = DIR_AUX / "nóminas"
DIR_REGLA23 = DIR_FASE1 / "regla23"

PATH_PERSONAS = DIR_ENTRADA / "nóminas" / "personas.xlsx"
PATH_EXP = DIR_ENTRADA / "nóminas" / "expedientes recursos humanos.xlsx"
PATH_NOMINAS = DIR_ENTRADA / "nóminas" / "nóminas y seguridad social.xlsx"
# Nómina ya aplicada (tras filtros de atrasos no vinculados y descuento
# de extras de cargos al CR 68). Si existe, debe usarse en lugar de la
# original para que el cuadre refleje la misma masa que el orchestrator.
PATH_NOMINAS_APLICADA = DIR_NOMINAS / "nominas_aplicadas.parquet"

# UC retributivas y de SS
PATH_UC_PTGAS = DIR_NOMINAS / "uc_ptgas.parquet"
PATH_UC_PDI = DIR_NOMINAS / "uc_pdi.parquet"
PATH_UC_PVI = DIR_NOMINAS / "uc_pvi.parquet"
PATH_UC_DESPIDOS = DIR_NOMINAS / "uc_despidos.parquet"
PATH_UC_INDEM = DIR_NOMINAS / "uc_indemnizaciones_asistencias.parquet"
PATH_UC_CARGOS = DIR_NOMINAS / "uc_cargos.parquet"           # CR 19/64 en proy ESPECÍFICO
PATH_CARGOS_UC = DIR_NOMINAS / "cargos_uc.parquet"           # reparto cargos en proy GENERAL
PATH_PERSONA_SS = DIR_NOMINAS / "persona_ss.parquet"
PATH_PERSONA_UC = DIR_NOMINAS / "persona_uc.parquet"
PATH_UC_REPARTO_R23 = DIR_REGLA23 / "uc_reparto_regla_23.parquet"
PATH_SS_CALCULADOS = DIR_NOMINAS / "costes_sociales_calculados.parquet"
PATH_SEXENIOS = DIR_ENTRADA / "investigación" / "sexenios.xlsx"

# Constantes regla 23 (clases pasivas, asociados, proyectos generales)
_PROY_GEN_NÓMINA = set(cfg_tuple("proyectos_generales_nómina"))
_PROY_GEN_CARGOS = set(cfg_tuple("proyectos_generales_cargos"))
_CR_ESPECIALES = {"19", "64", "47", "48"}
_AÑO = cfg_int("año_analizado")


def _safe_read(path: Path) -> pl.DataFrame | None:
    try:
        return read_parquet(path)
    except FileNotFoundError:
        return None


# -------------------------------------------------------------------- helpers


@lru_cache(maxsize=4)
def _personas_cached(path_str: str, mtime: int) -> pl.DataFrame:
    del mtime
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


@lru_cache(maxsize=2)
def _sexenios_vivos_cached(mtime: int) -> frozenset[int]:
    """per_ids con sexenio vivo en el año analizado.

    Misma definición que usa el reparto de la regla 23: la persona tiene
    al menos un sexenio cuyo #campo("fecha_fin_sexenio") está dentro de
    los `sexenio_vivo_años` (6) años previos al fin del año.
    """
    del mtime
    from coana.fase1.regla23.reparto import _sexenios_vivos as _sv
    return frozenset(_sv(DIR_ENTRADA.parent, _AÑO))


def _sexenios_vivos() -> frozenset[int]:
    return _sexenios_vivos_cached(_mtime_ns(PATH_SEXENIOS))


# Sector canónico (modelo CoAna) → códigos en RR.HH.
_SECTOR_RAW = {"PDI": ("PDI",), "PVI": ("PI",), "PTGAS": ("PAS",)}


@lru_cache(maxsize=4)
def _expedientes_por_sector_cached(
    path_str: str, mtime: int, sector: str,
) -> pl.DataFrame:
    """Expedientes del sector canónico y `per_id` asociado.

    Traduce el sector canónico (#val("PDI"), #val("PVI"), #val("PTGAS"))
    al código bruto que aparece en #ruta("expedientes recursos humanos.xlsx")
    (#val("PI") para PVI, #val("PAS") para PTGAS).
    """
    del mtime
    p = Path(path_str)
    if not p.exists():
        return pl.DataFrame(schema={
            "expediente": pl.Int64, "per_id": pl.Int64, "sector": pl.Utf8,
        })
    exp = read_excel(p)
    códigos_raw = list(_SECTOR_RAW.get(sector, (sector,)))
    return exp.filter(pl.col("sector").is_in(códigos_raw)).select(
        "expediente", "per_id", "sector",
    )


def _expedientes_sector(sector: str) -> pl.DataFrame:
    return _expedientes_por_sector_cached(
        str(PATH_EXP), _mtime_ns(PATH_EXP), sector,
    )


@lru_cache(maxsize=4)
def _nóminas_año_cached(
    aplicada_str: str, original_str: str,
    aplicada_mtime: int, original_mtime: int, año: int,
) -> pl.DataFrame:
    del aplicada_mtime, original_mtime
    aplicada = Path(aplicada_str)
    if aplicada.exists():
        try:
            return read_parquet(aplicada).filter(pl.col("fecha").dt.year() == año)
        except FileNotFoundError:
            pass
    original = Path(original_str)
    if not original.exists():
        return pl.DataFrame()
    return read_excel(original).filter(pl.col("fecha").dt.year() == año)


def _nóminas_año() -> pl.DataFrame:
    return _nóminas_año_cached(
        str(PATH_NOMINAS_APLICADA), str(PATH_NOMINAS),
        _mtime_ns(PATH_NOMINAS_APLICADA), _mtime_ns(PATH_NOMINAS),
        _AÑO,
    )


# -------------------------------------------------------------------- cuadre


@lru_cache(maxsize=4)
def _per_ids_del_sector_cached(
    path_str: str, mtime: int, sector: str,
) -> tuple[int, ...]:
    del mtime
    df = _expedientes_por_sector_cached(path_str, _mtime_ns(Path(path_str)), sector)
    if df.is_empty():
        return ()
    return tuple(sorted(set(df["per_id"].to_list())))


PATH_EXTRAS_APLICADAS = DIR_NOMINAS / "cargos_extras_aplicadas.parquet"


@lru_cache(maxsize=2)
def _extras_aplicadas_dict_cached(path_str: str, mtime: int) -> dict[int, float]:
    del mtime
    p = Path(path_str)
    if not p.exists():
        return {}
    df = read_parquet(p)
    if df.is_empty() or "per_id" not in df.columns:
        return {}
    return {
        int(r["per_id"]): float(r.get("extra_aplicada") or 0.0)
        for r in df.iter_rows(named=True)
    }


def _extra_aplicada_per_id(per_id: int) -> float:
    return _extras_aplicadas_dict_cached(
        str(PATH_EXTRAS_APLICADAS), _mtime_ns(PATH_EXTRAS_APLICADAS),
    ).get(int(per_id), 0.0)


def _per_ids_sector(sector: str) -> set[int]:
    return set(_per_ids_del_sector_cached(str(PATH_EXP), _mtime_ns(PATH_EXP), sector))


def _nóminas_personas(per_ids: set[int]) -> pl.DataFrame:
    """Nóminas del año de TODAS las personas indicadas (todos sus
    expedientes, no solo los del sector primario)."""
    nom = _nóminas_año()
    if nom.is_empty() or not per_ids:
        return pl.DataFrame()
    exp = read_excel(PATH_EXP).select("expediente", "per_id", "sector")
    return nom.join(
        exp.filter(pl.col("per_id").is_in(list(per_ids))),
        on="expediente", how="inner",
    )


def _conceptos_clasificados(df: pl.DataFrame) -> pl.DataFrame:
    """Etiqueta cada línea de nómina con un `concepto` que identifica
    el flujo que la canaliza a UC. Depende del sector del expediente
    (la columna `sector` debe estar presente). Las etiquetas son:

    - ``ss``: aplicación 12* (cotización SS).
    - ``despidos``: CR 47 en proyecto general nóminas (PDI/PVI).
    - ``despidos-extra``: CR 47 en proyecto NO general (cae en extras).
    - ``indemnizaciones``: CR 48 (cualquier proyecto, cualquier sector).
    - ``cargos-reparto``: CR 19/64 en proyecto general de cargos
      (PDI/PVI).
    - ``cargos-extras``: CR 19/64 en proyecto NO general (PDI/PVI;
      genera uc_pdi/pvi línea a línea — duplicidad temporal documentada
      en el código).
    - ``masa-regla-23``: PDI/PVI con resto de CRs en proyecto general
      de nómina (entra a `uc_reparto_regla_23`).
    - ``retribuciones-extras``: PDI/PVI con resto de CRs en proyecto
      NO general; o PTGAS y otros sectores con CRs no especiales
      (cualquier proyecto, va a uc_ptgas/uc_pdi/uc_pvi línea a línea).
    """
    cr = pl.col("concepto_retributivo").cast(pl.Utf8)
    proy = pl.col("proyecto").cast(pl.Utf8)
    apli = pl.col("aplicación").cast(pl.Utf8)
    sector = pl.col("sector").cast(pl.Utf8)
    es_proy_gen_nom = proy.is_in(list(_PROY_GEN_NÓMINA))
    es_proy_gen_car = proy.is_in(list(_PROY_GEN_CARGOS))
    es_ss = apli.str.starts_with("12")
    es_pdi_pvi = sector.is_in(["PDI", "PI"])

    return df.with_columns(
        pl.when(es_ss).then(pl.lit("ss"))
        .when((cr == "48")).then(pl.lit("indemnizaciones"))
        .when((cr == "47") & es_proy_gen_nom & es_pdi_pvi).then(pl.lit("despidos"))
        .when((cr == "47")).then(pl.lit("despidos-extra"))
        .when(cr.is_in(["19", "64"]) & es_proy_gen_car & es_pdi_pvi).then(pl.lit("cargos-reparto"))
        .when(cr.is_in(["19", "64"]) & es_pdi_pvi).then(pl.lit("cargos-extras"))
        .when(es_proy_gen_nom & es_pdi_pvi).then(pl.lit("masa-regla-23"))
        .otherwise(pl.lit("retribuciones-extras"))
        .alias("concepto")
    )


_ORDEN_CONCEPTO = [
    "masa-regla-23", "cargos-reparto", "despidos", "indemnizaciones",
    "retribuciones-extras", "cargos-extras", "despidos-extra",
    "ss",
]

_CONCEPTO_A_UC = {
    "masa-regla-23": ["uc_reparto_regla_23"],
    "cargos-reparto": ["cargos_uc"],
    "despidos": ["uc_despidos"],
    "indemnizaciones": ["uc_indemnizaciones_asistencias"],
    "retribuciones-extras": ["uc_ptgas", "uc_pdi", "uc_pvi"],
    "cargos-extras": ["uc_cargos"],
    # «despidos-extra» (CR 47 en proyecto NO general) canalizan en
    # uc_pdi/uc_pvi como cualquier extra. Su importe ya está contado
    # en «retribuciones-extras», así que aquí no le asignamos UC para
    # evitar doble contabilización visual.
    "despidos-extra": [],
    "ss": ["persona_ss"],
}


# -------------------------------------------------------------------- master


def _agregar_por_persona(sector: str) -> pl.DataFrame:
    """Por per_id que tenga al menos un expediente del sector, devuelve
    cobrado/SS/calculada y UC sobre TODOS sus expedientes.

    El filtro por sector solo decide qué personas aparecen en la lista;
    las cifras consolidan la actividad completa de la persona (PDI + BEC
    + lo que sea). Las personas multi-sector aparecen en las listas de
    los sectores correspondientes con las mismas cifras de cuadre.
    """
    per_ids = _per_ids_sector(sector)
    nom = _nóminas_personas(per_ids)
    if nom.is_empty():
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "bruto": pl.Float64, "ss_cot": pl.Float64,
            "uc_retr": pl.Float64, "uc_ss": pl.Float64,
            "ss_calc": pl.Float64,
        })

    nom = _conceptos_clasificados(nom)
    cobrado = (
        nom.group_by("per_id")
        .agg(
            pl.col("importe").filter(pl.col("concepto") != "ss").sum().alias("bruto"),
            pl.col("importe").filter(pl.col("concepto") == "ss").sum().alias("ss_cot"),
        )
    )

    # La nómina aplicada ya tiene descontada la extra-paga del cargo
    # (CR 68) de aquellas personas con cargos académicos en proyecto
    # general. A efectos de visualización del bruto cobrado, devolvemos
    # esa extra para que se vea lo que la UJI paga realmente; la
    # contrapartida está en `cargos_uc` (UC retributivas) con el mismo
    # importe, así que el cuadre no se rompe.
    extras_df = _safe_read(PATH_EXTRAS_APLICADAS)
    if extras_df is not None and not extras_df.is_empty():
        extras_df = extras_df.select(
            pl.col("per_id").cast(pl.Int64),
            pl.col("extra_aplicada").alias("_extra"),
        )
        cobrado = (
            cobrado.join(extras_df, on="per_id", how="left")
            .with_columns(
                (pl.col("bruto") + pl.col("_extra").fill_null(0.0)).alias("bruto")
            )
            .drop("_extra")
        )

    # SS calculada (clases pasivas, PDI funcionario)
    ss_calc = _safe_read(PATH_SS_CALCULADOS)
    if ss_calc is not None and not ss_calc.is_empty():
        ss_calc = ss_calc.select(
            pl.col("per_id").cast(pl.Int64),
            pl.col("importe_total").alias("ss_calc"),
        )
        cobrado = cobrado.join(ss_calc, on="per_id", how="left")
    else:
        cobrado = cobrado.with_columns(pl.lit(0.0).alias("ss_calc"))
    cobrado = cobrado.with_columns(pl.col("ss_calc").fill_null(0.0))

    # UC retributivas (todas las fuentes excepto SS, todas las
    # personas indicadas).
    uc_retr = _uc_retributivas_por_persona(per_ids)
    cobrado = cobrado.join(uc_retr, on="per_id", how="left")
    cobrado = cobrado.with_columns(pl.col("uc_retr").fill_null(0.0))

    # UC SS (persona_ss).
    ss_df = _safe_read(PATH_PERSONA_SS)
    if ss_df is not None and not ss_df.is_empty():
        uc_ss = (
            ss_df.filter(~pl.col("ss_proporcional").is_nan())
            .group_by("per_id")
            .agg(pl.col("ss_proporcional").sum().alias("uc_ss"))
        )
        cobrado = cobrado.join(uc_ss, on="per_id", how="left")
    else:
        cobrado = cobrado.with_columns(pl.lit(0.0).alias("uc_ss"))
    cobrado = cobrado.with_columns(pl.col("uc_ss").fill_null(0.0))

    return cobrado


def _uc_retributivas_por_persona(per_ids: set[int]) -> pl.DataFrame:
    """Suma de importes de todas las UC retributivas para las personas
    indicadas, agregadas por per_id. Incluye TODOS sus expedientes (no
    solo los del sector).
    """
    pids = list(per_ids)
    # Mapa expediente → per_id para uc_pdi/pvi.
    exp_all = read_excel(PATH_EXP).filter(pl.col("per_id").is_in(pids))
    partes: list[pl.DataFrame] = []

    # uc_ptgas, uc_pdi y uc_pvi: por expediente → per_id.
    for path in (PATH_UC_PTGAS, PATH_UC_PDI, PATH_UC_PVI):
        df = _safe_read(path)
        if df is None or df.is_empty() or "expediente" not in df.columns:
            continue
        sub = df.join(
            exp_all.select("expediente", "per_id"), on="expediente", how="inner",
        )
        if sub.is_empty():
            continue
        partes.append(sub.select("per_id", "importe"))

    # uc_despidos, uc_indemnizaciones, uc_cargos: traen per_id.
    for path in (PATH_UC_DESPIDOS, PATH_UC_INDEM, PATH_UC_CARGOS):
        df = _safe_read(path)
        if df is None or df.is_empty() or "per_id" not in df.columns:
            continue
        sub = df.filter(pl.col("per_id").is_in(pids))
        if sub.is_empty():
            continue
        partes.append(sub.select("per_id", "importe"))

    # cargos_uc: per_id directo, importe en `importe_uc`.
    cargos = _safe_read(PATH_CARGOS_UC)
    if cargos is not None and not cargos.is_empty() and "per_id" in cargos.columns:
        sub = cargos.filter(pl.col("per_id").is_in(pids))
        if not sub.is_empty():
            partes.append(sub.select("per_id", pl.col("importe_uc").alias("importe")))

    # uc_reparto_regla_23: per_id directo.
    r23 = _safe_read(PATH_UC_REPARTO_R23)
    if r23 is not None and not r23.is_empty() and "per_id" in r23.columns:
        sub = r23.filter(pl.col("per_id").is_in(pids))
        if not sub.is_empty():
            partes.append(sub.select("per_id", "importe"))

    if not partes:
        return pl.DataFrame(schema={"per_id": pl.Int64, "uc_retr": pl.Float64})

    todas = pl.concat(partes, how="vertical_relaxed")
    return todas.group_by("per_id").agg(pl.col("importe").sum().alias("uc_retr"))


# -------------------------------------------------------------------- public API


_COLS_MASTER: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="sexenio_vivo", label="Sexenio vivo", format="bool"),
    ColumnSpec(name="bruto", label="Bruto cobrado", format="euro"),
    ColumnSpec(name="ss_cot", label="SS cotizada", format="euro"),
    ColumnSpec(name="ss_calc", label="SS calculada", format="euro"),
    ColumnSpec(name="uc_retr", label="UC retributivas", format="euro"),
    ColumnSpec(name="uc_ss", label="UC SS", format="euro"),
    ColumnSpec(name="delta", label="Δ cuadre", format="euro"),
]


def listar_personas_sector(sector: str, p: QueryParams) -> ListResponse:
    """Master de personas del sector con sus cifras de cuadre."""
    df = _agregar_por_persona(sector)
    if df.is_empty():
        return ListResponse(columns=_COLS_MASTER, rows=[], total=0)
    df = df.with_columns(
        ((pl.col("bruto") + pl.col("ss_cot") + pl.col("ss_calc"))
         - (pl.col("uc_retr") + pl.col("uc_ss"))).round(2).alias("delta")
    )
    df = df.join(_personas(), on="per_id", how="left").with_columns(
        pl.col("persona").fill_null("?")
    )
    sex = _sexenios_vivos()
    df = df.with_columns(
        pl.col("per_id").is_in(list(sex)).alias("sexenio_vivo")
    )
    df = df.select([c.name for c in _COLS_MASTER if c.name in df.columns])
    df, total, stats = apply_query(df, p, search_columns=["persona"])
    return ListResponse(
        columns=_COLS_MASTER,
        rows=[
            {k: (v.isoformat() if hasattr(v, "isoformat") else v)
             for k, v in r.items()}
            for r in df.to_dicts()
        ],
        total=total,
        column_stats=stats,
    )


_COLS_CUADRE: list[ColumnSpec] = [
    ColumnSpec(name="concepto", label="Concepto", format="text"),
    ColumnSpec(name="cobrado", label="Cobrado", format="euro"),
    ColumnSpec(name="uc", label="UC generadas", format="euro"),
    ColumnSpec(name="delta", label="Δ", format="euro"),
    ColumnSpec(name="vía_uc", label="Vía UC", format="text"),
]


def cuadre_persona(sector: str, per_id: int) -> ListResponse:
    """Tabla de cuadre detallada por concepto para una persona.

    El sector se usa solo para validar pertenencia; el cuadre se calcula
    sobre TODOS los expedientes de la persona (PDI + cualquier otro
    sector).
    """
    nom = _nóminas_personas({per_id})
    if nom.is_empty():
        return ListResponse(columns=_COLS_CUADRE, rows=[], total=0)
    nom = _conceptos_clasificados(nom)
    if nom.is_empty():
        return ListResponse(columns=_COLS_CUADRE, rows=[], total=0)

    cobrado_por_concepto = (
        nom.group_by("concepto")
        .agg(pl.col("importe").sum().alias("cobrado"))
    )
    cobrado_dict = {
        r["concepto"]: float(r["cobrado"] or 0.0)
        for r in cobrado_por_concepto.iter_rows(named=True)
    }
    # Reasignar la extra del CR 68 (paga adicional del cargo) desde
    # masa-regla-23 hacia cargos-reparto. Conceptualmente forma parte
    # del cargo aunque se cobre vía CR 68. La nómina aplicada ya tiene
    # ese importe descontado del CR 68, pero el orchestrator lo añade
    # al cargos_uc, así que para que cuadre lo presentamos como cobrado
    # del cargo aquí.
    extra_aplicada = _extra_aplicada_per_id(per_id)
    if extra_aplicada > 0:
        cobrado_dict["cargos-reparto"] = (
            cobrado_dict.get("cargos-reparto", 0.0) + extra_aplicada
        )

    # SS calculada (no aparece en nómina como tal): añadir como fila aparte.
    ss_calc = _safe_read(PATH_SS_CALCULADOS)
    ss_calc_val = 0.0
    if ss_calc is not None and not ss_calc.is_empty():
        f = ss_calc.filter(pl.col("per_id") == per_id)
        if not f.is_empty():
            ss_calc_val = float(f["importe_total"].sum() or 0.0)

    # UC por origen (vía).
    uc_por_concepto = _uc_persona_por_concepto(sector, per_id)

    filas = []
    for c in _ORDEN_CONCEPTO:
        cob = cobrado_dict.get(c, 0.0)
        if c == "ss":
            uc = uc_por_concepto.get("persona_ss", 0.0)
            cob = cob + ss_calc_val
            vía = "persona_ss.parquet (incluye SS cotizada y calculada)"
        else:
            uc = sum(uc_por_concepto.get(k, 0.0) for k in _CONCEPTO_A_UC[c])
            vía = " · ".join(_CONCEPTO_A_UC[c])
        if abs(cob) < 0.005 and abs(uc) < 0.005:
            continue
        filas.append({
            "concepto": c,
            "cobrado": round(cob, 2),
            "uc": round(uc, 2),
            "delta": round(cob - uc, 2),
            "vía_uc": vía,
        })

    # Fila TOTAL
    tot_cob = sum(r["cobrado"] for r in filas)
    tot_uc = sum(r["uc"] for r in filas)
    filas.append({
        "concepto": "TOTAL",
        "cobrado": round(tot_cob, 2),
        "uc": round(tot_uc, 2),
        "delta": round(tot_cob - tot_uc, 2),
        "vía_uc": "",
    })

    return ListResponse(columns=_COLS_CUADRE, rows=filas, total=len(filas))


# -------------------------------------------------------------------- nómina


_COLS_NOMINA: list[ColumnSpec] = [
    ColumnSpec(name="fecha", label="Mes", format="date"),
    ColumnSpec(name="expediente", label="Expediente", format="id"),
    ColumnSpec(name="tipo_coste", label="Tipo coste", format="text"),
    ColumnSpec(name="concepto_retributivo", label="Concepto retributivo", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="flujo", label="Flujo / destino", format="text"),
    ColumnSpec(name="proyecto", label="Proyecto", format="text"),
    ColumnSpec(name="aplicación", label="Aplicación", format="text"),
    ColumnSpec(name="servicio", label="Servicio", format="text"),
]
_SEARCH_NOMINA = [
    "concepto_retributivo", "flujo", "proyecto", "servicio", "tipo_coste", "aplicación",
]


def _catalogo_nombre(path: Path, key: str) -> pl.DataFrame:
    """Catálogo `key → nombre` con la columna nombre renombrada a
    `_n_<key>`. Vacío si el fichero no existe o no tiene `nombre`."""
    alias = f"_n_{key}"
    if not path.exists():
        return pl.DataFrame(schema={key: pl.Utf8, alias: pl.Utf8})
    d = read_excel(path)
    if key not in d.columns or "nombre" not in d.columns:
        return pl.DataFrame(schema={key: pl.Utf8, alias: pl.Utf8})
    return d.select(
        pl.col(key).cast(pl.Utf8),
        pl.col("nombre").cast(pl.Utf8).alias(alias),
    ).unique(subset=key)


def nomina_persona(sector: str, per_id: int, params: QueryParams) -> ListResponse:
    """Detalle línea a línea de la nómina del año de la persona.

    Incluye TODOS sus expedientes (no solo los del sector), con el nombre
    de cada concepto retributivo y tipo de coste, y el `flujo` que
    canaliza cada línea hacia su UC (la misma clasificación que usa el
    cuadre: masa-regla-23, ss, despidos, cargos-reparto, etc.).
    """
    nom = _nóminas_personas({int(per_id)})
    if nom is None or nom.is_empty():
        return ListResponse(columns=_COLS_NOMINA, rows=[], total=0)
    nom = _conceptos_clasificados(nom)

    cr_cat = _catalogo_nombre(
        DIR_ENTRADA / "nóminas" / "conceptos retributivos.xlsx", "concepto_retributivo")
    tc_cat = _catalogo_nombre(
        DIR_ENTRADA / "nóminas" / "tipos coste plantilla.xlsx", "tipo_coste")
    sv_cat = _catalogo_nombre(
        DIR_ENTRADA / "inventario" / "servicios.xlsx", "servicio")

    def _compose(code_col: str, name_col: str) -> pl.Expr:
        code = pl.col(code_col).cast(pl.Utf8)
        return (
            pl.when(pl.col(name_col).is_not_null() & (pl.col(name_col) != ""))
            .then(pl.concat_str([code, pl.lit(" — "), pl.col(name_col)]))
            .otherwise(code.fill_null(""))
        )

    df = (
        nom.with_columns(
            pl.col("concepto_retributivo").cast(pl.Utf8),
            pl.col("tipo_coste").cast(pl.Utf8),
            pl.col("servicio").cast(pl.Utf8),
        )
        .join(cr_cat, on="concepto_retributivo", how="left")
        .join(tc_cat, on="tipo_coste", how="left")
        .join(sv_cat, on="servicio", how="left")
        .with_columns(
            pl.col("fecha").dt.strftime("%Y-%m-%d").alias("fecha"),
            _compose("concepto_retributivo", "_n_concepto_retributivo").alias("concepto_retributivo"),
            _compose("tipo_coste", "_n_tipo_coste").alias("tipo_coste"),
            _compose("servicio", "_n_servicio").alias("servicio"),
            pl.col("proyecto").cast(pl.Utf8),
            pl.col("aplicación").cast(pl.Utf8),
            pl.col("concepto").alias("flujo"),
        )
    )
    df = df.select([c.name for c in _COLS_NOMINA if c.name in df.columns])
    if not params.sort_by:
        df = df.sort(["fecha", "flujo", "concepto_retributivo"])
    df, total, stats = apply_query(df, params, search_columns=_SEARCH_NOMINA)
    return ListResponse(
        columns=_COLS_NOMINA, rows=df.to_dicts(), total=total, column_stats=stats,
    )


def _uc_persona_por_concepto(sector: str, per_id: int) -> dict[str, float]:
    """Mapa {origen_uc: suma_importes} para una persona (todos los
    expedientes, no solo los del sector indicado).
    """
    exp_all = read_excel(PATH_EXP)
    exp_per = exp_all.filter(pl.col("per_id") == per_id)["expediente"].to_list()
    out: dict[str, float] = {}

    for tag, path in (
        ("uc_ptgas", PATH_UC_PTGAS),
        ("uc_pdi", PATH_UC_PDI),
        ("uc_pvi", PATH_UC_PVI),
    ):
        df = _safe_read(path)
        if df is None or df.is_empty() or "expediente" not in df.columns:
            continue
        v = float(df.filter(pl.col("expediente").is_in(exp_per))["importe"].sum() or 0.0)
        if v != 0:
            out[tag] = v

    for tag, path in (
        ("uc_despidos", PATH_UC_DESPIDOS),
        ("uc_indemnizaciones_asistencias", PATH_UC_INDEM),
        ("uc_cargos", PATH_UC_CARGOS),
    ):
        df = _safe_read(path)
        if df is None or df.is_empty() or "per_id" not in df.columns:
            continue
        v = float(df.filter(pl.col("per_id") == per_id)["importe"].sum() or 0.0)
        if v != 0:
            out[tag] = v

    df = _safe_read(PATH_CARGOS_UC)
    if df is not None and not df.is_empty() and "per_id" in df.columns:
        v = float(df.filter(pl.col("per_id") == per_id)["importe_uc"].sum() or 0.0)
        if v != 0:
            out["cargos_uc"] = v

    df = _safe_read(PATH_UC_REPARTO_R23)
    if df is not None and not df.is_empty() and "per_id" in df.columns:
        v = float(df.filter(pl.col("per_id") == per_id)["importe"].sum() or 0.0)
        if v != 0:
            out["uc_reparto_regla_23"] = v

    df = _safe_read(PATH_PERSONA_SS)
    if df is not None and not df.is_empty() and "per_id" in df.columns:
        sub = df.filter((pl.col("per_id") == per_id) & ~pl.col("ss_proporcional").is_nan())
        v = float(sub["ss_proporcional"].sum() or 0.0)
        if v != 0:
            out["persona_ss"] = v

    return out


# ------------------------------------------------------------------ KPIs


def resumen_persona(sector: str, per_id: int) -> KpiPanel:
    """KPIs para la cabecera del detalle de una persona."""
    nom = _nóminas_personas({per_id})
    if nom.is_empty():
        return KpiPanel(kpis=[Kpi(label="Sin nómina", value=0, format="int")])
    nom = _conceptos_clasificados(nom)
    bruto = float(nom.filter(pl.col("concepto") != "ss")["importe"].sum() or 0.0)
    bruto += _extra_aplicada_per_id(per_id)
    ss_cot = float(nom.filter(pl.col("concepto") == "ss")["importe"].sum() or 0.0)

    ss_calc_df = _safe_read(PATH_SS_CALCULADOS)
    ss_calc = 0.0
    if ss_calc_df is not None and not ss_calc_df.is_empty():
        f = ss_calc_df.filter(pl.col("per_id") == per_id)
        if not f.is_empty():
            ss_calc = float(f["importe_total"].sum() or 0.0)

    uc = _uc_persona_por_concepto(sector, per_id)
    uc_retr = sum(
        v for k, v in uc.items() if k not in ("persona_ss",)
    )
    uc_ss = uc.get("persona_ss", 0.0)
    delta = (bruto + ss_cot + ss_calc) - (uc_retr + uc_ss)

    return KpiPanel(kpis=[
        Kpi(label="Bruto cobrado", value=round(bruto, 2), format="euro"),
        Kpi(label="SS cotizada", value=round(ss_cot, 2), format="euro"),
        Kpi(label="SS calculada", value=round(ss_calc, 2), format="euro"),
        Kpi(label="UC retributivas", value=round(uc_retr, 2), format="euro"),
        Kpi(label="UC SS", value=round(uc_ss, 2), format="euro"),
        Kpi(
            label="Δ cuadre",
            value=round(delta, 2),
            format="euro",
            hint=(
                "Cuadrado al céntimo" if abs(delta) < 0.01
                else f"Descuadre de {delta:,.2f} €"
            ),
        ),
    ])


# ------------------------------------------------------------------ UC totales


_COLS_UC_PERSONA: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID", format="text"),
    ColumnSpec(name="origen", label="Origen", format="text"),
    ColumnSpec(name="elemento_de_coste", label="Elemento", format="text"),
    ColumnSpec(name="centro_de_coste", label="Centro", format="text"),
    ColumnSpec(name="actividad", label="Actividad", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
]


def listar_uc_persona_completa(
    sector: str, per_id: int, p: QueryParams | None = None,
) -> ListResponse:
    """Lista consolidada de TODAS las UC vinculadas a la persona
    (retributivas + SS), con el origen como columna para filtrar."""
    exp_all = read_excel(PATH_EXP)
    exp_per = exp_all.filter(pl.col("per_id") == per_id)["expediente"].to_list()
    partes: list[pl.DataFrame] = []

    def _cargar(tag: str, path: Path, *, via_expediente: bool, importe_col: str = "importe"):
        df = _safe_read(path)
        if df is None or df.is_empty():
            return
        if via_expediente:
            if "expediente" not in df.columns:
                return
            sub = df.filter(pl.col("expediente").is_in(exp_per))
        else:
            if "per_id" not in df.columns:
                return
            sub = df.filter(pl.col("per_id") == per_id)
        if sub.is_empty():
            return
        # Normaliza columnas mínimas.
        for c in ("id", "elemento_de_coste", "centro_de_coste", "actividad"):
            if c not in sub.columns:
                sub = sub.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))
        if importe_col != "importe":
            sub = sub.with_columns(pl.col(importe_col).alias("importe"))
        partes.append(sub.select(
            "id", "elemento_de_coste", "centro_de_coste", "actividad", "importe",
        ).with_columns(pl.lit(tag).alias("origen")))

    _cargar("uc_ptgas", PATH_UC_PTGAS, via_expediente=True)
    _cargar("uc_pdi", PATH_UC_PDI, via_expediente=True)
    _cargar("uc_pvi", PATH_UC_PVI, via_expediente=True)
    _cargar("uc_despidos", PATH_UC_DESPIDOS, via_expediente=False)
    _cargar("uc_indemnizaciones", PATH_UC_INDEM, via_expediente=False)
    _cargar("uc_cargos", PATH_UC_CARGOS, via_expediente=False)
    _cargar("cargos_uc", PATH_CARGOS_UC, via_expediente=False, importe_col="importe_uc")
    _cargar("uc_reparto_regla_23", PATH_UC_REPARTO_R23, via_expediente=False)

    # persona_ss → expandimos a UC sintéticas.
    ss_df = _safe_read(PATH_PERSONA_SS)
    if ss_df is not None and not ss_df.is_empty() and "per_id" in ss_df.columns:
        sub = ss_df.filter(
            (pl.col("per_id") == per_id) & ~pl.col("ss_proporcional").is_nan()
        )
        if not sub.is_empty():
            sub = sub.with_row_index("_n", offset=1).with_columns(
                (pl.lit("SS-") + pl.col("_n").cast(pl.Utf8).str.zfill(4)).alias("id"),
                pl.lit("seguridad-social").alias("elemento_de_coste"),
                pl.col("ss_proporcional").alias("importe"),
            )
            partes.append(sub.select(
                "id", "elemento_de_coste", "centro_de_coste", "actividad", "importe",
            ).with_columns(pl.lit("persona_ss").alias("origen")))

    if not partes:
        return ListResponse(columns=_COLS_UC_PERSONA, rows=[], total=0)

    todas = pl.concat(partes, how="vertical_relaxed").select(
        [c.name for c in _COLS_UC_PERSONA]
    ).sort("importe", descending=True)
    todas, total, stats = apply_query(
        todas, p or QueryParams(),
        search_columns=["origen", "elemento_de_coste", "centro_de_coste", "actividad"],
    )
    return ListResponse(
        columns=_COLS_UC_PERSONA,
        rows=[{k: v for k, v in r.items()} for r in todas.to_dicts()],
        total=total,
        column_stats=stats,
    )


__all__ = [
    "listar_personas_sector", "cuadre_persona", "resumen_persona",
    "listar_uc_persona_completa",
]
