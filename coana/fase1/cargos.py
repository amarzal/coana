"""Cargos académicos: reparto del cobro CR 19/64 entre cargos asimilados al RD.

Para cada persona PDI/PVI que ha percibido CR 19/64 en proyecto general en el
año analizado, se reparte el total entre los cargos que ostenta — siempre que
estén asimilados a uno de los 8 tipos del RD 1086/1989 y tengan periodo de
cobro solapado con el año — ponderando por (días cobrados × importe mensual
del RD asimilado).

Entrada:
- `data/entrada/nóminas/personas cargos.xlsx`
- `data/entrada/nóminas/cargos.xlsx`
- `data/entrada/nóminas/cargos real decreto.xlsx`
- `data/entrada/nóminas/nóminas y seguridad social.xlsx`
- `data/entrada/nóminas/expedientes recursos humanos.xlsx`

Salida:
- `data/fase1/auxiliares/nóminas/cargos_uc.parquet` — una fila por (per_id, cargo)
  remunerado en el año, con el importe imputado y la propuesta tentativa de UC.
"""

from datetime import date
from pathlib import Path

import polars as pl

from coana.util.excel_cache import read_excel


# TABLA-PROYECTOS-GENERALES (para CR 19/64): los 10 proyectos que, a
# efectos de los cargos académicos, se consideran «generales». Su CR 19/64
# entra al reparto por persona (en lugar de generar UC propia como sí
# hacen los proyectos específicos vía `generar_uc_cargos`).
# La constante análoga en código es `_PROYECTOS_GENERALES` de
# `coana.fase1.nóminas.regla_23`.
_PROYECTOS_GENERALES: tuple[str, ...] = (
    "07G011", "1I235", "22G010", "11G003",
    "1G019", "23G019", "02G041", "11G006", "1G046", "00000",
)


# Mapeo de categoría PDI → XXX del elemento de coste (replica
# coana.fase1.nóminas._PDI_CAT_XXX).
_PDI_CAT_XXX: dict[str, str] = {
    "CU": "cu",
    "TU": "tu", "TUI": "tu",
    "CEU": "ceu",
    "TEU": "teu",
    "AJ": "aj", "AJD": "aj", "AJDII": "aj",
    "PAA": "as", "PAL": "as",
    "PS": "ps",
    "PEME": "em",
    "PPL": "pl", "PPLV": "pl",
    "PVI": "pv",
    "PD": "pd",
    "PCD": "pcd",
    "PC": "pc",
}


def _dias_solape_2025_expr(año: int) -> pl.Expr:
    """Expresión polars para días naturales de solape del periodo de
    cobro `[fecha_inicio_cobra, fecha_fin_cobra or 31-dic-año]` con el año.

    Si `fecha_inicio_cobra` es nulo, el cargo NO se cobra → días = 0.
    """
    inicio_año = date(año, 1, 1)
    fin_año = date(año, 12, 31)
    fic = pl.col("fecha_inicio_cobra").cast(pl.Date)
    ffc = pl.col("fecha_fin_cobra").cast(pl.Date)
    inicio_ef = pl.when(fic > pl.lit(inicio_año)).then(fic).otherwise(pl.lit(inicio_año))
    fin_ef = (
        pl.when(ffc.is_null())
        .then(pl.lit(fin_año))
        .otherwise(
            pl.when(ffc < pl.lit(fin_año)).then(ffc).otherwise(pl.lit(fin_año))
        )
    )
    return (
        pl.when(fic.is_null())
        .then(0)
        .when(fin_ef >= inicio_ef)
        .then((fin_ef - inicio_ef).dt.total_days().cast(pl.Int64) + 1)
        .otherwise(0)
    )


def calcular_extras_cargos_por_persona(
    ruta_base: Path, año: int = 2025,
) -> dict[int, float]:
    """Estimación de la parte extra del cargo por persona en el año.

    Para cada cargo activo (con periodo de cobro solapado al año) y con
    asimilación a RD, la extra anual del cargo se estima como
    `2 × importe_rd × días / 365`. Sumamos por persona.

    Solo se considera a personas con cobro CR 19/64 > 0 en *proyecto
    general* en el año: la extra "camuflada" en el CR 68 solo aparece
    en ese caso. Los cargos pagados en proyectos específicos no tienen
    parte extra oculta — se cobran línea a línea — y por tanto la
    extra no debe restarse de su CR 68 ni añadirse a su UC individual.

    Esta cantidad se restará del CR 68 (paga adicional del complemento
    específico PDI) en el preprocesamiento de nóminas, y se añadirá al
    importe imputado al cargo en `cargos_uc.parquet`.
    """
    pc_path = Path(ruta_base) / "entrada" / "nóminas" / "personas cargos.xlsx"
    cat_path = Path(ruta_base) / "entrada" / "nóminas" / "cargos.xlsx"
    rd_path = Path(ruta_base) / "entrada" / "nóminas" / "cargos real decreto.xlsx"
    nom_path = Path(ruta_base) / "entrada" / "nóminas" / "nóminas y seguridad social.xlsx"
    exp_path = Path(ruta_base) / "entrada" / "nóminas" / "expedientes recursos humanos.xlsx"
    for p in (pc_path, cat_path, rd_path, nom_path, exp_path):
        if not p.exists():
            return {}
    pc = read_excel(pc_path)
    cat = read_excel(cat_path)
    rd = read_excel(rd_path)
    nom = read_excel(nom_path)
    exp = read_excel(exp_path)

    # Personas con CR 19/64 > 0 en proyecto general en el año: las únicas
    # que pueden tener extra "camuflada" en CR 68.
    cr = pl.col("concepto_retributivo").cast(pl.Utf8)
    proy = pl.col("proyecto").cast(pl.Utf8)
    personas_general = (
        nom.join(exp.select("expediente", "per_id"), on="expediente", how="inner")
        .filter(cr.is_in(["19", "64"]))
        .filter(proy.is_in(list(_PROYECTOS_GENERALES)))
        .filter(pl.col("fecha").dt.year() == año)
        .group_by("per_id")
        .agg(pl.col("importe").sum().alias("total"))
        .filter(pl.col("total") > 0)
        .get_column("per_id").to_list()
    )
    if not personas_general:
        return {}

    cat_min = cat.with_columns(pl.col("cargo").cast(pl.Utf8)).select(
        "cargo", "cargo_asimilado",
    )
    rd_min = rd.select(
        pl.col("cargo_real_decreto").alias("cargo_asimilado"),
        pl.col("importe_mensual").alias("importe_rd"),
    )
    df = (
        pc.filter(pl.col("per_id").is_in(personas_general))
        .with_columns(pl.col("cargo").cast(pl.Utf8))
        .join(cat_min, on="cargo", how="left")
        .filter(pl.col("cargo_asimilado").is_not_null())
        .join(rd_min, on="cargo_asimilado", how="left")
        .with_columns(_dias_solape_2025_expr(año).alias("días"))
        .filter(pl.col("días") > 0)
        .with_columns(
            (2 * pl.col("importe_rd") * pl.col("días") / 365.0).alias("extra")
        )
        .group_by("per_id")
        .agg(pl.col("extra").sum().alias("extra_total"))
    )
    return {int(r["per_id"]): float(r["extra_total"]) for r in df.iter_rows(named=True)}


def generar_cargos_uc(
    ruta_base: Path,
    dir_salida: Path,
    año: int = 2025,
    extras_aplicadas_por_persona: dict[int, float] | None = None,
) -> pl.DataFrame:
    """Genera `cargos_uc.parquet` con el reparto del cobro CR 19/64 en
    proyecto general entre los cargos asimilados al RD.
    """
    nom_path = Path(ruta_base) / "entrada" / "nóminas" / "nóminas y seguridad social.xlsx"
    exp_path = Path(ruta_base) / "entrada" / "nóminas" / "expedientes recursos humanos.xlsx"
    pc_path = Path(ruta_base) / "entrada" / "nóminas" / "personas cargos.xlsx"
    cat_path = Path(ruta_base) / "entrada" / "nóminas" / "cargos.xlsx"
    rd_path = Path(ruta_base) / "entrada" / "nóminas" / "cargos real decreto.xlsx"
    for p in (nom_path, exp_path, pc_path, cat_path, rd_path):
        if not p.exists():
            print(f"  ⚠ Cargos: falta {p}, se omite la generación")
            return pl.DataFrame()

    nom = read_excel(nom_path)
    exp = read_excel(exp_path)
    pc = read_excel(pc_path)
    cat = read_excel(cat_path)
    rd = read_excel(rd_path)

    # 1) Total CR 19/64 en proyecto general por persona en el año.
    cr = pl.col("concepto_retributivo").cast(pl.Utf8)
    proy = pl.col("proyecto").cast(pl.Utf8)
    totales = (
        nom.join(exp.select("expediente", "per_id"), on="expediente", how="inner")
        .filter(cr.is_in(["19", "64"]))
        .filter(proy.is_in(list(_PROYECTOS_GENERALES)))
        .filter(pl.col("fecha").dt.year() == año)
        .group_by("per_id")
        .agg(pl.col("importe").sum().alias("total_persona"))
        .filter(pl.col("total_persona") > 0)
    )
    if totales.is_empty():
        _persistir_vacío(dir_salida)
        return pl.DataFrame()

    # 2) Categoría última por persona (CR 19/64, máx fecha).
    cats_últimas = (
        nom.join(exp.select("expediente", "per_id"), on="expediente", how="inner")
        .filter(cr.is_in(["19", "64"]))
        .sort("fecha", descending=True)
        .group_by("per_id")
        .agg(pl.col("categoría").first().alias("categoría_última"))
    )

    # 3) Sector principal por persona (sector con mayor importe del año,
    # leyendo de los parquets sectoriales si están).
    sectores = _sector_principal_por_persona(ruta_base)

    # 4) Filas de personas cargos con periodo de cobro solapado al año,
    #    enriquecidas con cargo_asimilado, importe RD, centro y actividad.
    inicio_año = date(año, 1, 1)
    fin_año = date(año, 12, 31)
    cobra = (
        pl.col("fecha_inicio_cobra").is_not_null()
        & (pl.col("fecha_inicio_cobra").cast(pl.Date) <= fin_año)
        & (
            pl.col("fecha_fin_cobra").is_null()
            | (pl.col("fecha_fin_cobra").cast(pl.Date) >= inicio_año)
        )
    )
    cat_min = cat.with_columns(pl.col("cargo").cast(pl.Utf8)).select(
        "cargo",
        pl.col("nombre").alias("nombre_cargo"),
        "cargo_asimilado",
        pl.col("actividad").alias("act_cargo"),
        pl.col("centro").alias("cc_cargo"),
    )
    rd_min = rd.select(
        pl.col("cargo_real_decreto").alias("cargo_asimilado"),
        pl.col("importe_mensual").alias("importe_rd"),
    )
    activas = (
        pc.filter(cobra)
        .with_columns(pl.col("cargo").cast(pl.Utf8))
        .join(cat_min, on="cargo", how="left")
        .filter(pl.col("cargo_asimilado").is_not_null())
        .join(rd_min, on="cargo_asimilado", how="left")
    )
    if activas.is_empty():
        _persistir_vacío(dir_salida)
        return pl.DataFrame()

    # 5) Días naturales de solape del periodo de cobro con el año.
    activas = activas.with_columns(_dias_solape_2025_expr(año).alias("días"))
    activas = activas.filter(pl.col("días") > 0)
    if activas.is_empty():
        _persistir_vacío(dir_salida)
        return pl.DataFrame()

    # 6) Peso = días × importe_rd; reparto por suma de pesos por persona.
    activas = activas.with_columns(
        (pl.col("días") * pl.col("importe_rd")).alias("peso"),
        # Extra estimada por cargo: 2 mensualidades RD prorrateadas por días/365.
        (2.0 * pl.col("importe_rd") * pl.col("días") / 365.0).alias("extra_estimada"),
    )
    sumas = (
        activas.group_by("per_id")
        .agg(
            pl.col("peso").sum().alias("suma_pesos"),
            pl.col("extra_estimada").sum().alias("suma_extra_estimada"),
        )
    )

    # «Extra aplicada» por persona: si el preprocesamiento de nóminas pudo
    # restar toda la extra estimada del CR 68 disponible, la extra aplicada
    # coincide con la estimada. Si no, queda recortada (anomalía).
    if extras_aplicadas_por_persona is None:
        extras_aplicadas_por_persona = {}
    aplicadas_df = pl.DataFrame(
        {
            "per_id": list(extras_aplicadas_por_persona.keys()) or [],
            "extra_aplicada_total": list(extras_aplicadas_por_persona.values()) or [],
        },
        schema={"per_id": pl.Int64, "extra_aplicada_total": pl.Float64},
    )

    activas = (
        activas.join(sumas, on="per_id", how="left")
        .join(totales, on="per_id", how="inner")
        .join(aplicadas_df, on="per_id", how="left")
        .with_columns(
            pl.col("extra_aplicada_total").fill_null(
                pl.col("suma_extra_estimada"),
            ),
        )
        .with_columns(
            # Parte ordinaria: TOTAL_CR_19_64 repartido por peso (días×RD).
            (pl.col("total_persona") * pl.col("peso") / pl.col("suma_pesos"))
            .alias("importe_uc_ord"),
            # Parte extra del cargo: la extra aplicada en CR 68, prorrateada
            # entre los cargos por la misma proporción de la extra_estimada.
            (
                pl.col("extra_aplicada_total")
                * pl.col("extra_estimada")
                / pl.col("suma_extra_estimada")
            ).alias("importe_uc_extra"),
        )
        .with_columns(
            (pl.col("importe_uc_ord") + pl.col("importe_uc_extra"))
            .round(2)
            .alias("importe_uc"),
            pl.col("extra_estimada").round(2),
            pl.col("importe_uc_ord").round(2),
            pl.col("importe_uc_extra").round(2),
            (pl.col("suma_extra_estimada") - pl.col("extra_aplicada_total"))
            .round(2)
            .alias("extra_no_aplicada"),
        )
        .join(cats_últimas, on="per_id", how="left")
    )

    # 7) Elemento de coste: ZZZ-XXX-cargos.
    activas = activas.with_columns(
        pl.col("per_id").map_elements(
            lambda v: sectores.get(int(v), ""), return_dtype=pl.Utf8,
        ).alias("sector_principal"),
    )
    activas = activas.with_columns(
        pl.struct(["sector_principal", "categoría_última"]).map_elements(
            lambda s: _elemento_de_coste(s["sector_principal"], s["categoría_última"]),
            return_dtype=pl.Utf8,
        ).alias("elemento_de_coste"),
    )

    # 7.b) Resolver patrones (SERVICIO, TITULACIÓN, CENTROTITULACION) en
    # los campos `act_cargo` y `cc_cargo` que vienen de cargos.xlsx.
    mapping_serv = _cargar_mapping_servicio()
    mapping_tit = _cargar_mapping_titulaciones(ruta_base)

    def _resolver(row: dict) -> dict:
        act = row.get("act_cargo") or ""
        cc = row.get("cc_cargo") or ""
        if "SERVICIO" in act or "SERVICIO" in cc:
            srv = row.get("servicio")
            if srv is not None:
                try:
                    srv_key = str(int(srv))
                except (ValueError, TypeError):
                    srv_key = str(srv)
                mp = mapping_serv.get(srv_key)
                if mp is not None:
                    cc_resuelto = mp[0] or ""
                    if cc_resuelto:
                        act = act.replace("SERVICIO", cc_resuelto)
                        cc = cc.replace("SERVICIO", cc_resuelto)
        tit = row.get("titulación")
        if tit is not None and ("TITULACIÓN" in act or "TITULACIÓN" in cc or "CENTROTITULACION" in cc):
            try:
                tit_key = int(tit)
            except (ValueError, TypeError):
                tit_key = None
            mp_t = mapping_tit.get(tit_key) if tit_key is not None else None
            if mp_t is not None:
                act_t, cc_t = mp_t
                if "CENTROTITULACION" in cc and cc_t:
                    cc = cc.replace("CENTROTITULACION", cc_t)
                if act_t:
                    act = act.replace("TITULACIÓN", act_t)
                    cc = cc.replace("TITULACIÓN", act_t)
        return {"act_resuelta": act, "cc_resuelto": cc}

    activas = activas.with_columns(
        pl.struct(["act_cargo", "cc_cargo", "servicio", "titulación"]).map_elements(
            _resolver,
            return_dtype=pl.Struct({"act_resuelta": pl.Utf8, "cc_resuelto": pl.Utf8}),
        ).alias("_resuelto"),
    ).with_columns(
        pl.col("_resuelto").struct.field("act_resuelta").alias("act_cargo"),
        pl.col("_resuelto").struct.field("cc_resuelto").alias("cc_cargo"),
    ).drop("_resuelto")

    # 8) origen_id seriado.
    activas = activas.sort("per_id", "cargo")
    activas = activas.with_row_index("_idx").with_columns(
        (pl.lit("CARGO-") + (pl.col("_idx") + 1).cast(pl.Utf8).str.zfill(5))
        .alias("id"),
    ).drop("_idx")

    # Anomalía de patrón: si tras la resolución persiste algún token en
    # mayúsculas (SERVICIO, TITULACIÓN, CENTROTITULACION) en `act_cargo`
    # o `cc_cargo`, la UC queda con etiqueta inválida. Lo marcamos para
    # poder visualizar y depurar los casos.
    patron_pendiente = (
        pl.col("act_cargo").cast(pl.Utf8).str.contains(r"[A-ZÁÉÍÓÚÑ]{3,}").fill_null(False)
        | pl.col("cc_cargo").cast(pl.Utf8).str.contains(r"[A-ZÁÉÍÓÚÑ]{3,}").fill_null(False)
    )
    activas = activas.with_columns(
        pl.when(patron_pendiente)
        .then(pl.lit("patrón sin resolver (servicio/titulación faltante)"))
        .otherwise(pl.lit(""))
        .alias("_anomalía_patrón"),
    )

    out = activas.select(
        "id",
        "per_id",
        "cargo",
        "nombre_cargo",
        "cargo_asimilado",
        "importe_rd",
        "fecha_inicio_cobra",
        "fecha_fin_cobra",
        "días",
        "peso",
        "importe_uc_ord",
        "extra_estimada",
        "importe_uc_extra",
        "importe_uc",
        "extra_no_aplicada",
        pl.col("elemento_de_coste"),
        pl.col("cc_cargo").alias("centro_de_coste"),
        pl.col("act_cargo").alias("actividad"),
        "_anomalía_patrón",
        pl.col("categoría_última"),
        pl.col("sector_principal"),
        pl.lit("nómina").alias("origen"),
        pl.col("id").alias("origen_id"),
        pl.lit(1.0).alias("origen_porción"),
    )

    dir_salida.mkdir(parents=True, exist_ok=True)
    out.write_parquet(dir_salida / "cargos_uc.parquet")

    n_personas = out["per_id"].n_unique()
    n_uc = len(out)
    n_anom = out.filter(pl.col("_anomalía_patrón") != "").height
    total = float(out["importe_uc"].sum() or 0)
    print(
        f"  Cargos UC (reparto CR 19/64 proyecto general por persona): "
        f"{n_personas:,} personas, {n_uc:,} UC, {total:,.2f} €"
        + (f" ⚠ {n_anom:,} UC con patrón sin resolver" if n_anom else "")
    )
    return out


def _cargar_mapping_servicio() -> dict[str, tuple[str, str]]:
    """`{servicio_id: (cc, actividad)}` del clasificador de centros de coste.
    Sirve para resolver el patrón SERVICIO en las etiquetas de cargos.xlsx."""
    from coana.fase1.clasificador_centros_coste import _SERVICIO_CC
    return _SERVICIO_CC


def _cargar_mapping_titulaciones(
    ruta_base: Path,
) -> dict[int, tuple[str, str]]:
    """`{código_titulación: (actividad, centro)}` desde
    `data/entrada/docencia/titulaciones actividad centro.xlsx`.
    Sirve para resolver los patrones TITULACIÓN y CENTROTITULACION."""
    p = Path(ruta_base) / "entrada" / "docencia" / "titulaciones actividad centro.xlsx"
    if not p.exists():
        return {}
    df = read_excel(p)
    out: dict[int, tuple[str, str]] = {}
    for r in df.iter_rows(named=True):
        try:
            tid = int(r["titulación"])
        except (TypeError, ValueError, KeyError):
            continue
        act = str(r.get("actividad") or "").strip()
        cc = str(r.get("centro") or "").strip()
        out[tid] = (act, cc)
    return out


def _persistir_vacío(dir_salida: Path) -> None:
    dir_salida.mkdir(parents=True, exist_ok=True)
    p = dir_salida / "cargos_uc.parquet"
    if p.exists():
        p.unlink()


def _elemento_de_coste(sector: str | None, categoría: str | None) -> str:
    """`ZZZ-XXX-cargos` o cadena vacía si no se puede resolver."""
    if not categoría:
        return ""
    cat = str(categoría).strip()
    if sector == "PDI":
        xxx = _PDI_CAT_XXX.get(cat)
        return f"pdi-{xxx}-cargos" if xxx else ""
    if sector == "PVI":
        # Por defecto en PVI sin más contexto: `pid` (regla por defecto del
        # mapping de XXX para PVI cuando ninguna otra encaja).
        return "piyotper-pid-cargos"
    return ""


def _sector_principal_por_persona(ruta_base: Path) -> dict[int, str]:
    """Sector con mayor importe del año por per_id, leído de los parquets
    sectoriales generados por el preprocesamiento de nóminas."""
    dir_fase1 = Path(ruta_base).parent / "fase1" if not str(ruta_base).endswith("fase1") else Path(ruta_base)
    # ruta_base suele ser `data` → fase1 = `data/fase1`
    dir_aux = Path(ruta_base).parent.joinpath("fase1", "auxiliares", "nóminas")
    if str(ruta_base).rstrip("/").endswith("data"):
        dir_aux = Path(ruta_base) / "fase1" / "auxiliares" / "nóminas"
    partes: list[pl.DataFrame] = []
    for sector in ("PDI", "PTGAS", "PVI", "Otros"):
        p = dir_aux / f"{sector}.parquet"
        if not p.exists():
            continue
        df = pl.read_parquet(p)
        if "per_id" not in df.columns or "importe" not in df.columns:
            continue
        partes.append(
            df.group_by("per_id").agg(pl.col("importe").sum().alias("imp"))
            .with_columns(pl.lit(sector).alias("sector"))
        )
    if not partes:
        return {}
    df = pl.concat(partes, how="vertical").sort(
        ["per_id", "imp"], descending=[False, True],
    ).group_by("per_id").first().select("per_id", "sector")
    return {int(r["per_id"]): str(r["sector"]) for r in df.iter_rows(named=True)}
