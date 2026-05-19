"""Reparto final regla 23: normalización a la jornada anual.

Toma ``dedicación_pdi.parquet`` (horas iniciales por per_id, actividad,
grupo, factor) y aplica las fases 5, 6 y 7 del modelo de la regla 23
para obtener las horas FINALES por (per_id, actividad, centro_de_coste)
que cubren exactamente la jornada anual T = 1642 h de cada PDI.

Algoritmo (por persona):

1. Horas iniciales efectivas por grupo:
   - HDO  = Σ horas × factor sobre grupo='docencia_oficial'
   - HDNO = Σ horas × factor sobre grupo='docencia_no_oficial'
   - HG   = Σ horas sobre grupo='gestión'
   - HI   = Σ horas sobre grupo='investigación'
   - HE   = 0 (no hay registro en la UJI)

2. Caso especial profesor asociado: toda la jornada T se imputa a las
   actividades docentes (oficial + no oficial) en proporción a sus
   horas iniciales efectivas. Sin gestión ni investigación.

3. Caso general:
   - La docencia se respeta tal cual: HDO_def = HDO, HDNO_def = HDNO.
   - HG está fija (ya calculada por el cargador `cargos.py` como
     `% × (T - HDO - HDNO)` o como cantidad horaria absoluta).
   - Pendientes_inv = T - HDO - HDNO - HG.
   - Si HI > Pendientes_inv > 0: se ESCALA investigación a Pendientes_inv
     proporcionalmente a las horas iniciales de cada actividad.
   - Si HI ≤ Pendientes_inv: HI_def = HI; HND = Pendientes_inv - HI.
   - Si Pendientes_inv ≤ 0 (docencia + gestión exceden T): HI_def = 0,
     HND = 0; el exceso se reporta como anomalía pero no se corrige
     (la docencia es intocable).

4. Reparto de HND (si > 0):
   - Sexenio "vivo" (último sexenio finaliza dentro de los 6 años
     previos al fin del año): HND → investigación.
   - Sin sexenio vivo: HND se reparte entre los tres grupos
     proporcionalmente al peso inicial (HDO+HDNO, HG, HI).

5. Repercusión a actividades concretas: las horas finales de cada
   grupo se reparten entre sus actividades concretas en proporción a
   las horas iniciales efectivas.

Salida: ``data/fase1/regla23/dedicación_pdi_normalizada.parquet`` con
columnas:

    per_id, actividad, centro_de_coste, grupo, origen,
    horas_iniciales, horas_finales, anomalía
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.util.configuración import cfg_int, cfg_set

# Estas dos constantes se leen de data/configuración.xlsx vía
# coana.util.configuración. Se exponen como módulo-level vars para no
# romper imports históricos.
JORNADA_ANUAL_PDI: float = float(cfg_int("jornada_anual_pdi"))

# Categorías de plaza que cuentan como profesor asociado (PAA/PAL).
# Origen: data/entrada/nóminas/categorías plazas.xlsx, filas con nombre
# que contiene "Associat/da" y sector PDI.
_CATEGORIAS_ASOCIADO: set[str] = cfg_set("categorías_asociado_plaza")


def aplicar_reparto_regla_23(
    ruta_base: Path,
    año: int = 2025,
    jornada: float = JORNADA_ANUAL_PDI,
) -> pl.DataFrame:
    """Aplica las fases 5-7 de la regla 23 sobre dedicación_pdi.parquet."""
    parquet_ded = ruta_base / "fase1" / "regla23" / "dedicación_pdi.parquet"
    if not parquet_ded.exists():
        return _esquema_vacío()
    df = pl.read_parquet(parquet_ded)
    if df.is_empty():
        return _esquema_vacío()

    df = df.with_columns(
        (pl.col("horas") * pl.col("factor")).alias("horas_efectivas"),
    )

    sexenios_vivos = _sexenios_vivos(ruta_base, año)
    asociados = _per_ids_asociados(ruta_base, año)

    # Agregaciones por (per_id, grupo): horas efectivas totales.
    por_grupo = (
        df.group_by("per_id", "grupo")
        .agg(pl.col("horas_efectivas").sum().alias("h_grupo"))
    )
    # Pivote para acceder cómodamente.
    pivot = por_grupo.pivot(
        values="h_grupo", index="per_id", on="grupo",
    ).with_columns([
        pl.col(c).fill_null(0.0) if c != "per_id" else pl.col(c)
        for c in por_grupo["grupo"].unique().to_list() + ["per_id"]
    ])

    # Garantizar las 4 columnas (docencia_oficial, docencia_no_oficial,
    # gestión, investigación) aunque no haya filas de algún grupo.
    for col in ("docencia_oficial", "docencia_no_oficial", "gestión", "investigación"):
        if col not in pivot.columns:
            pivot = pivot.with_columns(pl.lit(0.0).alias(col))

    pivot = pivot.with_columns(
        pl.col("per_id").is_in(list(asociados)).alias("es_asociado"),
        pl.col("per_id").is_in(list(sexenios_vivos)).alias("sexenio_vivo"),
    )

    # Calcular, por persona, horas finales por grupo y anomalía.
    # Vectorizamos con expresiones polars.
    HDO = pl.col("docencia_oficial")
    HDNO = pl.col("docencia_no_oficial")
    HG = pl.col("gestión")
    HI = pl.col("investigación")
    T = pl.lit(jornada)
    DOC = HDO + HDNO  # docencia total efectiva
    pendientes = T - DOC - HG
    exceso_doc_ges = (DOC + HG) > T

    # HI final (escalado si HI > pendientes; 0 si pendientes <= 0)
    hi_final = (
        pl.when(pendientes <= 0).then(0.0)
        .when(HI > pendientes).then(pendientes)
        .otherwise(HI)
    )
    hnd = (
        pl.when(pendientes <= 0).then(0.0)
        .when(HI > pendientes).then(0.0)
        .otherwise(pendientes - HI)
    )

    # Reparto HND. La docencia es intocable: NO se le suma HND. El HND
    # se reparte entre gestión e investigación proporcionalmente a sus
    # horas iniciales efectivas. Si la persona no tiene ni gestión ni
    # investigación iniciales (resto a 0), el HND va íntegramente a
    # investigación (toda persona del PDI investiga por defecto).
    es_sexenio = pl.col("sexenio_vivo")
    peso_resto = HG + HI
    hnd_a_ges = (
        pl.when(es_sexenio | (hnd <= 0)).then(0.0)
        .when(peso_resto <= 0).then(0.0)
        .otherwise(hnd * HG / peso_resto)
    )
    hnd_a_inv = (
        pl.when(hnd <= 0).then(0.0)
        .when(es_sexenio).then(hnd)
        .when(peso_resto <= 0).then(hnd)
        .otherwise(hnd * HI / peso_resto)
    )

    # Horas finales por grupo. La docencia se respeta tal cual.
    doc_final = DOC
    ges_final = HG + hnd_a_ges
    inv_final = hi_final + hnd_a_inv

    # Caso asociado: toda T a docencia (si tiene docencia inicial > 0).
    doc_final = pl.when(pl.col("es_asociado") & (DOC > 0)).then(T).otherwise(doc_final)
    ges_final = pl.when(pl.col("es_asociado")).then(0.0).otherwise(ges_final)
    inv_final = pl.when(pl.col("es_asociado")).then(0.0).otherwise(inv_final)

    pivot = pivot.with_columns(
        DOC.alias("h_doc_inicial"),
        HG.alias("h_ges_inicial"),
        HI.alias("h_inv_inicial"),
        doc_final.alias("h_doc_final"),
        ges_final.alias("h_ges_final"),
        inv_final.alias("h_inv_final"),
        exceso_doc_ges.alias("_anom_exceso"),
    )

    # Repercusión a actividades concretas. Para cada (per_id, grupo,
    # actividad, centro), su peso es horas_efectivas / horas_grupo_inicial.
    # Las horas finales del grupo se distribuyen con ese peso. Docencia
    # oficial y no oficial comparten total (mismo "h_doc_*").
    grupos_doc = ["docencia_oficial", "docencia_no_oficial"]
    detalle = df.join(
        pivot.select(
            "per_id",
            "h_doc_inicial", "h_doc_final",
            "h_ges_inicial", "h_ges_final",
            "h_inv_inicial", "h_inv_final",
            "es_asociado", "sexenio_vivo", "_anom_exceso",
        ),
        on="per_id", how="left",
    )

    detalle = detalle.with_columns(
        pl.when(pl.col("grupo").is_in(grupos_doc)).then(pl.col("h_doc_inicial"))
        .when(pl.col("grupo") == "gestión").then(pl.col("h_ges_inicial"))
        .when(pl.col("grupo") == "investigación").then(pl.col("h_inv_inicial"))
        .otherwise(pl.lit(0.0))
        .alias("_h_grupo_inicial"),

        pl.when(pl.col("grupo").is_in(grupos_doc)).then(pl.col("h_doc_final"))
        .when(pl.col("grupo") == "gestión").then(pl.col("h_ges_final"))
        .when(pl.col("grupo") == "investigación").then(pl.col("h_inv_final"))
        .otherwise(pl.lit(0.0))
        .alias("_h_grupo_final"),
    )

    detalle = detalle.with_columns(
        pl.when(pl.col("_h_grupo_inicial") > 0)
        .then(pl.col("horas_efectivas") * pl.col("_h_grupo_final") / pl.col("_h_grupo_inicial"))
        .otherwise(pl.lit(0.0))
        .alias("horas_finales"),
    )

    anomalía_expr = (
        pl.when(pl.col("anomalía").is_not_null())
        .then(pl.col("anomalía"))
        .when(pl.col("_anom_exceso") & ~pl.col("es_asociado"))
        .then(pl.lit("docencia + gestión exceden la jornada anual"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )

    salida = detalle.select(
        "per_id", "actividad", "centro_de_coste", "grupo", "origen",
        "origen_id",
        pl.col("horas_efectivas").alias("horas_iniciales"),
        pl.col("horas_finales"),
        "detalle",
        anomalía_expr.alias("anomalía"),
        "es_asociado", "sexenio_vivo",
    )

    # Personas con h_inv_final > 0 pero sin fila de investigación: emitir
    # fila sintética en la actividad umbrella `ai` y el centro de su
    # grupo principal (o `pendiente` si no se conoce).
    sintéticas = _filas_sintéticas_investigación(
        ruta_base, año, pivot, df,
    )
    if sintéticas.height > 0:
        salida = pl.concat([salida, sintéticas], how="vertical_relaxed")

    dir_out = ruta_base / "fase1" / "regla23"
    dir_out.mkdir(parents=True, exist_ok=True)
    salida.write_parquet(dir_out / "dedicación_pdi_normalizada.parquet")
    return salida


def _filas_sintéticas_investigación(
    ruta_base: Path,
    año: int,
    pivot: pl.DataFrame,
    df_inicial: pl.DataFrame,
) -> pl.DataFrame:
    """Genera filas para personas con h_inv_final > 0 sin fila inicial.

    Activitad: ``ai`` (umbrella). Centro: ``grupo-investigación-{id}`` si
    la persona pertenece a un grupo válido principal, ``pendiente`` si no.
    """
    # Personas que ya tienen alguna fila de investigación: NO se sintetiza.
    con_inv = (
        df_inicial.filter(pl.col("grupo") == "investigación")
        .select("per_id").unique()
    )
    candidatas = (
        pivot.filter(pl.col("h_inv_final") > 0)
        .select("per_id", "h_inv_final", "sexenio_vivo", "es_asociado")
        .join(con_inv, on="per_id", how="anti")
    )
    if candidatas.is_empty():
        return pl.DataFrame(schema=_columnas_salida())

    # Centro: grupo principal de la persona.
    centros = _centro_por_persona(ruta_base, año)
    candidatas = candidatas.join(centros, on="per_id", how="left")
    candidatas = candidatas.with_columns(
        pl.col("centro_de_coste").fill_null(pl.lit("pendiente"))
    )

    return candidatas.select(
        pl.col("per_id"),
        pl.lit("ai").alias("actividad"),
        pl.col("centro_de_coste"),
        pl.lit("investigación").alias("grupo"),
        pl.lit("reparto").alias("origen"),
        pl.lit(None, dtype=pl.Utf8).alias("origen_id"),
        pl.lit(0.0).alias("horas_iniciales"),
        pl.col("h_inv_final").alias("horas_finales"),
        pl.lit("HND repercutida a investigación (sin actividad concreta)").alias("detalle"),
        pl.lit(None, dtype=pl.Utf8).alias("anomalía"),
        pl.col("es_asociado"),
        pl.col("sexenio_vivo"),
    )


def _centro_por_persona(ruta_base: Path, año: int) -> pl.DataFrame:
    """Mapea per_id → centro_de_coste del grupo principal."""
    path = ruta_base / "entrada" / "investigación" / "investigadores en grupos.xlsx"
    mapeo_path = ruta_base / "entrada" / "investigación" / "grupos a institutos.xlsx"
    schema_vacío = pl.DataFrame(schema={
        "per_id": pl.Int64, "centro_de_coste": pl.Utf8,
    })
    if not path.exists():
        return schema_vacío
    inicio = date(año, 1, 1)
    fin = date(año, 12, 31)
    g = read_excel(path)
    g = g.with_columns(pl.col("fecha_baja").fill_null(fin).alias("_fin"))
    g = g.filter(
        (pl.col("fecha_alta") <= pl.lit(fin))
        & (pl.col("_fin") >= pl.lit(inicio))
    )
    if mapeo_path.exists():
        válidos = (
            read_excel(mapeo_path).select(pl.col("id_grupo").cast(pl.Utf8))
            .get_column("id_grupo").to_list()
        )
        g = g.filter(pl.col("id_grupo").cast(pl.Utf8).is_in(válidos))
    if g.is_empty():
        return schema_vacío
    # Tomar grupo principal por persona (principal == 'S' tiene prioridad).
    g = g.with_columns(
        (pl.col("principal") == "S").cast(pl.Int8).alias("_principal_int"),
    )
    g = g.sort(["per_id", "_principal_int"], descending=[False, True])
    primero = g.group_by("per_id", maintain_order=True).first()
    return primero.select(
        pl.col("per_id").cast(pl.Int64),
        (pl.lit("grupo-investigación-") + pl.col("id_grupo").cast(pl.Utf8))
        .alias("centro_de_coste"),
    )


def _columnas_salida() -> dict[str, pl.DataType]:
    return {
        "per_id": pl.Int64,
        "actividad": pl.Utf8,
        "centro_de_coste": pl.Utf8,
        "grupo": pl.Utf8,
        "origen": pl.Utf8,
        "origen_id": pl.Utf8,
        "horas_iniciales": pl.Float64,
        "horas_finales": pl.Float64,
        "detalle": pl.Utf8,
        "anomalía": pl.Utf8,
        "es_asociado": pl.Boolean,
        "sexenio_vivo": pl.Boolean,
    }


def _sexenios_vivos(ruta_base: Path, año: int) -> set[int]:
    """Per_ids con sexenio vivo: max(fecha_fin_sexenio) ≥ fin_año − 6 años."""
    path = ruta_base / "entrada" / "investigación" / "sexenios.xlsx"
    if not path.exists():
        return set()
    df = read_excel(path)
    if df.is_empty():
        return set()
    umbral = date(año - cfg_int("sexenio_vivo_años"), 12, 31)
    último = df.group_by("per_id").agg(
        pl.col("fecha_fin_sexenio").max().alias("último")
    )
    vivos = último.filter(pl.col("último") > pl.lit(umbral))
    return set(vivos["per_id"].to_list())


def _per_ids_asociados(ruta_base: Path, año: int) -> set[int]:
    """Per_ids del PDI con categoría de profesor asociado en el año.

    Determinado vía `nóminas y seguridad social.xlsx` (categoría_plaza
    × expediente), cruzando con `expedientes recursos humanos.xlsx`
    para obtener el per_id.
    """
    nom_path = ruta_base / "entrada" / "nóminas" / "nóminas y seguridad social.xlsx"
    exp_path = ruta_base / "entrada" / "nóminas" / "expedientes recursos humanos.xlsx"
    if not (nom_path.exists() and exp_path.exists()):
        return set()
    nom = read_excel(nom_path)
    exp = read_excel(exp_path)
    if nom.is_empty() or exp.is_empty():
        return set()
    nom = nom.with_columns(pl.col("categoría_plaza").cast(pl.Utf8).str.zfill(2))
    nom_año = nom.filter(pl.col("fecha").dt.year() == año)
    asociados = nom_año.filter(pl.col("categoría_plaza").is_in(list(_CATEGORIAS_ASOCIADO)))
    if asociados.is_empty():
        return set()
    join = asociados.select("expediente").unique().join(
        exp.select("expediente", "per_id"), on="expediente", how="inner",
    )
    return set(join["per_id"].to_list())


def _esquema_vacío() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "per_id": pl.Int64,
        "actividad": pl.Utf8,
        "centro_de_coste": pl.Utf8,
        "grupo": pl.Utf8,
        "origen": pl.Utf8,
        "origen_id": pl.Utf8,
        "horas_iniciales": pl.Float64,
        "horas_finales": pl.Float64,
        "detalle": pl.Utf8,
        "anomalía": pl.Utf8,
        "es_asociado": pl.Boolean,
        "sexenio_vivo": pl.Boolean,
    })
