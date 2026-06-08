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
# Origen: data/configuración.xlsx (clave `proyectos_generales_cargos`).
from coana.util.configuración import cfg_float as _cfg_float, cfg_tuple as _cfg_tuple
_PROYECTOS_GENERALES: tuple[str, ...] = _cfg_tuple("proyectos_generales_cargos")


# Overrides hiper-específicos para resolver el sentinel CENTROTITULACION
# de `cargos.xlsx` en filas concretas de `personas cargos.xlsx` cuando
# ni titulación ni servicio están informados. Última red de seguridad,
# fila a fila, antes de marcar como «patrón sin resolver».
#
# Clave: (per_id, cargo). Valor: centro de coste resultante.
_OVERRIDES_CENTRO_CARGO: dict[tuple[int, int], str] = {
    # Amparo López Merí, Vicedegana FCHS — sin titulación ni servicio
    # en personas cargos.xlsx; opera como vicedecana «de centro».
    (263437, 192): "fchs",
    # Cristina Giménez García, registrada como cargo 178 (Director
    # departament) pero en realidad Directora del Plan de Vida
    # Saludable adscrita al Vicerectorat d'Estudiantat i Vida
    # Saludable (servicio 4251 → centro `vevs`, actividad `dag-vevs`).
    # Su fila activa en personas cargos.xlsx no trae servicio.
    (14512, 178): "vevs",
    # María Ibáñez Martínez, cargo 489 (Subdirectora per a la
    # Coordinació de la Qualitat): adscrita a la Escola de Doctorat
    # (centro `ed`). Su fila no trae servicio ni titulación.
    (17322, 489): "ed",
}


# Overrides hiper-específicos para fijar la titulación en filas
# concretas de `personas cargos.xlsx` cuyo campo `titulación` esté en
# null. Se aplica antes del resolver de patrones, así que `TITULACIÓN`
# y `CENTROTITULACION` se resuelven después con normalidad vía
# `titulaciones actividad centro.xlsx`.
_OVERRIDES_TITULACION_CARGO: dict[tuple[int, int], int] = {
    # Marina Pavan, Coordinadora d'Intercanvi de Grau en Economia
    # (titulación 211 → grado-economía/fcje). La fila activa de su
    # cargo 198 no trae titulación informada.
    (310197, 198): 211,
    # Edgar Bresó (Coord. Pràctiques Externes Internacionals): Psicología.
    (64363, 389): 219,
    # Alba Puig (Coord. d'Intercanvi): Grau en ADE.
    (135911, 198): 210,
    # Francisco Trujillo (Tutor d'intercanvi): Grau en Relacions Laborals i RH.
    (141086, 200): 201,
    # Simone Alfarano (Coord. d'Intercanvi): Bachelor Int. Business Economics.
    (203280, 198): 257,
    # Gabriele Tedeschi (Coord. d'Intercanvi): Grau en ADE.
    (537722, 198): 210,
    # Anna Vera Raga (Tutor d'intercanvi): Grau en Dret.
    (866263, 200): 213,
}

# Número de pagas extras anuales del cargo «ocultas» en el CR 68 que
# hay que estimar para evitar duplicidad. Origen:
# data/configuración.xlsx (clave `pagas_extra_cargo`).
_PAGAS_EXTRA_CARGO: float = _cfg_float("pagas_extra_cargo")


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
    """Días naturales de solape del periodo de cobro con el año.

    Usa `fecha_inicio_cobra` / `fecha_fin_cobra` cuando están informadas
    y, en su defecto, cae a `fecha_inicio` / `fecha_fin` (periodo del
    cargo en sí). Esto permite reconocer cargos con fechas de cobro sin
    cumplimentar pero cuyo periodo del cargo sí solapa el ejercicio.
    """
    inicio_año = date(año, 1, 1)
    fin_año = date(año, 12, 31)
    fic = pl.coalesce(
        pl.col("fecha_inicio_cobra").cast(pl.Date),
        pl.col("fecha_inicio").cast(pl.Date),
    )
    ffc = pl.coalesce(
        pl.col("fecha_fin_cobra").cast(pl.Date),
        pl.col("fecha_fin").cast(pl.Date),
    )
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


def per_ids_con_cargo_asimilable_activo(
    ruta_base: Path, año: int = 2025,
) -> set[int]:
    """Conjunto de personas con al menos un cargo asimilado al RD vigente
    en el año (días naturales de solape > 0 según
    `_dias_solape_2025_expr`).

    Sirve para detectar cobros de CR 19/64 en proyecto general que NO
    corresponden a ningún cargo asimilable activo (atrasos / cobros
    residuales de cargos ya cerrados): esas líneas se filtran de la
    nómina en preprocesamiento.
    """
    pc_path = Path(ruta_base) / "entrada" / "nóminas" / "personas cargos.xlsx"
    cat_path = Path(ruta_base) / "entrada" / "nóminas" / "cargos.xlsx"
    if not (pc_path.exists() and cat_path.exists()):
        return set()
    pc = read_excel(pc_path)
    cat = read_excel(cat_path)
    cat_min = cat.with_columns(pl.col("cargo").cast(pl.Utf8)).select(
        "cargo", "cargo_asimilado",
    )
    df = (
        pc.with_columns(pl.col("cargo").cast(pl.Utf8))
        .join(cat_min, on="cargo", how="left")
        .filter(pl.col("cargo_asimilado").is_not_null())
        .with_columns(_dias_solape_2025_expr(año).alias("días"))
        .filter(pl.col("días") > 0)
    )
    return {int(v) for v in df["per_id"].drop_nulls().unique().to_list()}


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
            (pl.lit(_PAGAS_EXTRA_CARGO) * pl.col("importe_rd") * pl.col("días") / 365.0)
            .alias("extra")
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

    # Excluir los meses de absentismo (bonificación de SS): sus líneas
    # —incluido el CR 19/64— se desvían a la UC de absentismo, así que no
    # deben entrar en el reparto de cargos (evita el doble cómputo).
    meses_abs_path = Path(dir_salida) / "meses_absentismo.parquet"
    if meses_abs_path.exists():
        meses_abs = pl.read_parquet(meses_abs_path)
        if not meses_abs.is_empty():
            nom = (
                nom.join(exp.select("expediente", "per_id"), on="expediente", how="left")
                .with_columns(pl.col("fecha").dt.strftime("%Y-%m").alias("_mes"))
                .join(
                    meses_abs.rename({"mes": "_mes"}).with_columns(
                        pl.lit(True).alias("_abs")
                    ),
                    on=["per_id", "_mes"], how="left",
                )
                .filter(pl.col("_abs").is_null())
                .drop("per_id", "_mes", "_abs")
            )

    # Inyección de overrides cableados (per_id, cargo) → titulación.
    # Se aplica ANTES de la propagación para que tenga prioridad sobre
    # el histórico de la persona en ese cargo (puede que en años
    # anteriores la persona coordinara otra titulación distinta).
    if _OVERRIDES_TITULACION_CARGO:
        _ov_tit = _OVERRIDES_TITULACION_CARGO
        pc = pc.with_columns(
            pl.struct(["per_id", "cargo"]).map_elements(
                lambda s: _ov_tit.get(
                    (int(s["per_id"]), int(s["cargo"]))
                    if s["per_id"] is not None and s["cargo"] is not None
                    else None
                ),
                return_dtype=pl.Int64,
            ).alias("_ov_tit_v")
        ).with_columns(
            pl.coalesce("_ov_tit_v", "titulación").alias("titulación"),
        ).drop("_ov_tit_v")

    # Propagación entre filas del mismo (per_id, cargo) ANTES de filtrar
    # por periodo de cobro: rellena los nulos de `servicio`/`titulación`
    # con el valor histórico más frecuente del mismo cargo de la
    # persona. La moda es más robusta que «primero no nulo»: cuando hay
    # filas antiguas con un código distinto (típicamente porque la
    # titulación cambió de código tras un cambio de plan) y muchas
    # filas modernas con el código actual, gana el actual.
    pc = pc.with_columns(
        pl.col("servicio").fill_null(
            pl.col("servicio").drop_nulls().mode().first()
        ).over("per_id", "cargo"),
        pl.col("titulación").fill_null(
            pl.col("titulación").drop_nulls().mode().first()
        ).over("per_id", "cargo"),
    )

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
    # Fechas efectivas: `_cobra` con fallback al periodo del cargo en sí.
    fi_eff = pl.coalesce(
        pl.col("fecha_inicio_cobra").cast(pl.Date),
        pl.col("fecha_inicio").cast(pl.Date),
    )
    ff_eff = pl.coalesce(
        pl.col("fecha_fin_cobra").cast(pl.Date),
        pl.col("fecha_fin").cast(pl.Date),
    )
    cobra = (
        fi_eff.is_not_null()
        & (fi_eff <= fin_año)
        & (ff_eff.is_null() | (ff_eff >= inicio_año))
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
    # coincide con la estimada. Si no, queda recortada (anomalía). Modo
    # standalone (sin dict): se asume que se aplicó toda la estimada. En
    # flujo integrado las personas sin CR 68 disponible no aparecen en el
    # dict → su extra aplicada es 0 (de lo contrario `importe_uc_extra` se
    # imputaría sin contrapartida en el cobro, ya que el CE de los no
    # funcionarios va prorrateado en el CR 19/64 mensual).
    modo_standalone = extras_aplicadas_por_persona is None
    if extras_aplicadas_por_persona is None:
        extras_aplicadas_por_persona = {}
    aplicadas_df = pl.DataFrame(
        {
            "per_id": list(extras_aplicadas_por_persona.keys()) or [],
            "extra_aplicada_total": list(extras_aplicadas_por_persona.values()) or [],
        },
        schema={"per_id": pl.Int64, "extra_aplicada_total": pl.Float64},
    )

    fill_val = pl.col("suma_extra_estimada") if modo_standalone else pl.lit(0.0)
    activas = (
        activas.join(sumas, on="per_id", how="left")
        .join(totales, on="per_id", how="inner")
        .join(aplicadas_df, on="per_id", how="left")
        .with_columns(
            pl.col("extra_aplicada_total").fill_null(fill_val),
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
            .alias("importe_uc"),
            (pl.col("suma_extra_estimada") - pl.col("extra_aplicada_total"))
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
    # Overrides hiper-específicos (per_id, cargo) → centro_de_coste para
    # casos en que ni titulación ni servicio están informados en
    # `personas cargos.xlsx`. Última red de seguridad antes de marcar
    # como «patrón sin resolver».
    overrides_centro = _OVERRIDES_CENTRO_CARGO
    # `_OVERRIDES_TITULACION_CARGO` ya se aplicó arriba antes de la
    # propagación (con prioridad sobre el histórico).

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
        if tit is not None and (
            "TITULACIÓN" in act or "TITULACIÓN" in cc
            or "CENTROTITULACION" in act or "CENTROTITULACION" in cc
        ):
            try:
                tit_key = int(tit)
            except (ValueError, TypeError):
                tit_key = None
            mp_t = mapping_tit.get(tit_key) if tit_key is not None else None
            if mp_t is not None:
                act_t, cc_t = mp_t
                # CENTROTITULACION → centro de coste de la titulación.
                # Aplica tanto en `act` como en `cc` para soportar
                # patrones tipo `dag-CENTROTITULACION`.
                if cc_t:
                    if "CENTROTITULACION" in cc:
                        cc = cc.replace("CENTROTITULACION", cc_t)
                    if "CENTROTITULACION" in act:
                        act = act.replace("CENTROTITULACION", cc_t)
                # TITULACIÓN → actividad de la titulación.
                if act_t:
                    act = act.replace("TITULACIÓN", act_t)
                    cc = cc.replace("TITULACIÓN", act_t)
        # Fallback: si CENTROTITULACION o TITULACIÓN siguen presentes
        # (titulación nula o sin mapeo), intentar resolverlos con el
        # `servicio` de la fila (centro+actividad del servicio en
        # `servicios.xlsx`). Cubre cargos cuyo titular no coordina una
        # titulación concreta — vicedecanos «de centro», directores de
        # departamento, etc.
        if (
            "CENTROTITULACION" in act or "CENTROTITULACION" in cc
            or "TITULACIÓN" in act or "TITULACIÓN" in cc
        ):
            srv = row.get("servicio")
            if srv is not None:
                try:
                    srv_key = str(int(srv))
                except (ValueError, TypeError):
                    srv_key = str(srv)
                mp = mapping_serv.get(srv_key)
                if mp is not None:
                    cc_resuelto = mp[0] or ""
                    act_resuelta = mp[1] or ""
                    if cc_resuelto:
                        act = act.replace("CENTROTITULACION", cc_resuelto)
                        cc = cc.replace("CENTROTITULACION", cc_resuelto)
                    if act_resuelta:
                        act = act.replace("TITULACIÓN", act_resuelta)
                        cc = cc.replace("TITULACIÓN", act_resuelta)
        # Última red: override hiper-específico (per_id, cargo) cuando ni
        # titulación ni servicio están informados. Sustituye tanto
        # `CENTROTITULACION` como `SERVICIO` (ambos identifican el
        # centro) en los campos `act` y `cc`.
        if (
            "CENTROTITULACION" in act or "CENTROTITULACION" in cc
            or "SERVICIO" in act or "SERVICIO" in cc
        ):
            pid = row.get("per_id")
            cid = row.get("cargo")
            if pid is not None and cid is not None:
                try:
                    clave = (int(pid), int(cid))
                except (ValueError, TypeError):
                    clave = None
                cc_resuelto = overrides_centro.get(clave) if clave else None
                if cc_resuelto:
                    for token in ("CENTROTITULACION", "SERVICIO"):
                        act = act.replace(token, cc_resuelto)
                        cc = cc.replace(token, cc_resuelto)
        return {"act_resuelta": act, "cc_resuelto": cc}

    activas = activas.with_columns(
        pl.struct(
            ["act_cargo", "cc_cargo", "servicio", "titulación", "per_id", "cargo"]
        ).map_elements(
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
    from coana.fase1.clasificador_centros_coste import _servicio_cc
    return _servicio_cc()


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
