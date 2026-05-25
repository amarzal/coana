"""Reparto final regla 23: normalización a la jornada anual.

Toma ``dedicación_pdi.parquet`` (horas iniciales por per_id, actividad,
grupo, factor) y aplica las fases 5, 6 y 7 del modelo de la regla 23
para obtener las horas FINALES por (per_id, actividad, centro_de_coste)
que cubren exactamente la jornada anual T = 1642 h de cada PDI.

Algoritmo (por persona) — reparto en cascada por prioridad estricta:

1. Horas iniciales efectivas por grupo:
   - HDO  = Σ horas × factor sobre grupo='docencia_oficial'
   - HDNO = Σ horas × factor sobre grupo='docencia_no_oficial'
   - HG   = Σ horas sobre grupo='gestión'
   - HI   = Σ horas sobre grupo='investigación'
   - DOC  = HDO + HDNO

2. Jornada a repartir: T = JORNADA_ANUAL_PDI × X_persona, con
   X_persona = 1 − fracción de reducción sindical. La fracción sindical
   se imputa aparte a la actividad `acción-sindical`; T es lo que queda.

3. Cascada de valores absolutos sobre T. Docencia y gestión son
   RÍGIDAS — se respetan tal cual si caben, se recortan si no, y nunca
   se inflan. La investigación absorbe el hueco que queda:
   - doc_final = min(DOC, T)
   - ges_final = min(HG, T − doc_final)
   - inv_final = T − doc_final − ges_final   (≥ 0)
   Así, investigación se contrae si su HI inicial supera el hueco
   disponible, y absorbe las horas no distribuidas si HI < hueco.
   La suma docencia + gestión + investigación es siempre exactamente T.

4. El `sexenio_vivo` es un dato informativo de la persona pero no
   afecta al reparto: con docencia y gestión rígidas, las horas no
   distribuidas solo pueden ir a investigación.

5. Caso especial profesor asociado: toda T a docencia, sin gestión ni
   investigación.

6. Repercusión a actividades concretas: las horas finales de cada
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

    # Factor X_persona por reducción sindical: la jornada de reparto
    # pasa de T a X_persona × T y la fracción (1 − X_persona) se imputa a
    # la actividad `acción-sindical`. Dos fuentes independientes y
    # complementarias (ver especificación, §«Reducciones sindicales»):
    #  - PTGAS — tipo 8 de `reducciones laborales.xlsx` (días y % de
    #    jornada). `_x_persona_por_reducción_sindical` está acotado a
    #    PDI/PVI; en la práctica devuelve {} porque ese fichero solo
    #    contiene PTGAS.
    #  - PDI — tipos 37-40 de `reducciones docentes.xlsx` (créditos
    #    traducidos a fracción de jornada).
    # Se combinan: X_persona = X_tipo8 × (1 − fracción_PDI).
    from coana.fase1.regla23.reducción_sindical_pdi import fracción_sindical_pdi
    x_tipo8 = _x_persona_por_reducción_sindical(ruta_base, año)
    fracción_pdi = fracción_sindical_pdi(ruta_base, año)
    x_persona: dict[int, float] = {}
    for _p in set(x_tipo8) | set(fracción_pdi):
        _x = x_tipo8.get(_p, 1.0) * (1.0 - fracción_pdi.get(_p, 0.0))
        if _x < 1.0:
            x_persona[_p] = max(0.0, _x)
    if x_persona:
        fx_df = pl.DataFrame(
            {"per_id": list(x_persona.keys()), "_x_persona": list(x_persona.values())},
            schema={"per_id": pl.Int64, "_x_persona": pl.Float64},
        )
        pivot = pivot.join(fx_df, on="per_id", how="left").with_columns(
            pl.col("_x_persona").fill_null(1.0)
        )
    else:
        pivot = pivot.with_columns(pl.lit(1.0).alias("_x_persona"))

    # ---- Fases 5-7: reparto en cascada de la jornada ----
    # T (jornada anual menos la dedicación sindical) se reparte por
    # prioridad estricta: docencia, luego gestión, luego investigación.
    HDO = pl.col("docencia_oficial")
    HDNO = pl.col("docencia_no_oficial")
    HG = pl.col("gestión")
    HI = pl.col("investigación")
    T = pl.lit(jornada) * pl.col("_x_persona")
    DOC = HDO + HDNO  # docencia total efectiva
    # Cascada de valores absolutos: docencia y, sobre lo que quede,
    # gestión. Ambas son rígidas — se respetan tal cual si caben y solo
    # se recortan si no caben en lo disponible. La investigación
    # absorbe el resto: si la inicial cabe en el hueco, queda igual y
    # las horas no distribuidas se le imputan; si la supera, se contrae
    # al hueco. El sexenio vivo no afecta al reparto porque docencia y
    # gestión no admiten horas no distribuidas (se conserva como dato
    # informativo, pero no entra en el cálculo).
    doc_base = pl.min_horizontal(DOC, T)
    ges_base = pl.min_horizontal(HG, T - doc_base)
    sobrante = T - doc_base - ges_base   # ≥ 0; hueco para investigación
    # Exceso: docencia + gestión iniciales no caben en T.
    exceso_doc_ges = (DOC + HG) > T

    doc_final = doc_base
    ges_final = ges_base
    inv_final = sobrante

    # Caso asociado: toda T a docencia (si tiene docencia inicial > 0),
    # sin gestión ni investigación.
    doc_final = pl.when(pl.col("es_asociado") & (DOC > 0)).then(T).otherwise(doc_final)
    ges_final = pl.when(pl.col("es_asociado")).then(0.0).otherwise(ges_final)
    inv_final = pl.when(pl.col("es_asociado")).then(0.0).otherwise(inv_final)

    # Liberación sindical del 100 % (X_persona = 0 ⇒ T = 0): la cascada
    # deja docencia, gestión e investigación en 0 por sí sola; se marca
    # como bandera para la anomalía.
    sin_jornada = pl.col("_x_persona") <= 0.0

    pivot = pivot.with_columns(
        DOC.alias("h_doc_inicial"),
        HG.alias("h_ges_inicial"),
        HI.alias("h_inv_inicial"),
        doc_final.alias("h_doc_final"),
        ges_final.alias("h_ges_final"),
        inv_final.alias("h_inv_final"),
        exceso_doc_ges.alias("_anom_exceso"),
        sin_jornada.alias("_sin_jornada"),
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
            "es_asociado", "sexenio_vivo", "_anom_exceso", "_sin_jornada",
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
        .when(pl.col("_sin_jornada") & pl.col("_anom_exceso"))
        .then(pl.lit(
            "liberación sindical del 100 %: docencia, gestión e "
            "investigación sin dedicación final"
        ))
        .when(pl.col("_anom_exceso") & ~pl.col("es_asociado"))
        .then(pl.lit("docencia + gestión superan la jornada disponible"))
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

    # Filas de dedicación a la representación sindical: la fracción
    # (1 − X_persona) de la jornada anual de cada representante. Se
    # emiten desde `x_persona` (no desde `pivot`) para no perder a quien
    # tenga reducción sindical pero ninguna otra dedicación registrada.
    sindicales = _filas_sindicales(x_persona, asociados, sexenios_vivos, jornada)
    if sindicales.height > 0:
        salida = pl.concat([salida, sindicales], how="vertical_relaxed")

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


def _filas_sindicales(
    x_persona: dict[int, float],
    asociados: set[int],
    sexenios: set[int],
    jornada: float,
) -> pl.DataFrame:
    """Una fila de dedicación `acción-sindical` por representante sindical.

    Las horas finales son `(1 − X_persona) × jornada`. El centro de
    coste y la actividad son los mismos que usa la reducción sindical
    del PTGAS (tipo 8), de modo que PDI y PTGAS confluyen en el mismo
    par (`acción-sindical`, `locales-sindicales`).
    """
    from coana.fase1.nóminas.reducciones_sindicales import (
        ACTIVIDAD_SINDICAL,
        CC_SINDICAL,
    )

    items = [(int(p), 1.0 - x) for p, x in x_persona.items() if x < 1.0]
    if not items:
        return pl.DataFrame(schema=_columnas_salida())
    per_ids = [p for p, _ in items]
    horas = [round(f * jornada, 4) for _, f in items]
    n = len(per_ids)
    return pl.DataFrame(
        {
            "per_id": per_ids,
            "actividad": [ACTIVIDAD_SINDICAL] * n,
            "centro_de_coste": [CC_SINDICAL] * n,
            "grupo": ["acción-sindical"] * n,
            "origen": ["reducción-sindical"] * n,
            "origen_id": [None] * n,
            "horas_iniciales": horas,
            "horas_finales": horas,
            "detalle": ["dedicación a representación sindical"] * n,
            "anomalía": [None] * n,
            "es_asociado": [p in asociados for p in per_ids],
            "sexenio_vivo": [p in sexenios for p in per_ids],
        },
        schema=_columnas_salida(),
    )


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


def _x_persona_por_reducción_sindical(
    ruta_base: Path, año: int,
) -> dict[int, float]:
    """X_persona = Σ(bruto_exp × X_exp) / Σ(bruto_exp) sobre los
    expedientes PDI/PVI de la persona. Solo devuelve personas con
    X_persona < 1 (al menos un expediente PDI/PVI con reducción)."""
    from coana.fase1.nóminas.reducciones_sindicales import (
        factor_x_por_expediente as _factor_x_sind,
    )

    factores_exp = _factor_x_sind(ruta_base, año=año)
    if not factores_exp:
        return {}
    exp_path = ruta_base / "entrada" / "nóminas" / "expedientes recursos humanos.xlsx"
    if not exp_path.exists():
        return {}
    from coana.util import read_excel

    expedientes = read_excel(exp_path)
    exp_pdi_pvi = (
        expedientes.filter(pl.col("sector").is_in(["PDI", "PI", "PVI"]))
        .select("expediente", "per_id")
    )
    # Bruto por expediente (suma de uc_pdi + uc_pvi + uc_reparto_regla_23).
    # Aproximación: sumamos los importes de los parquets PDI y PVI por
    # expediente. Si no existen, X_persona se calcula con peso uniforme.
    brutos: dict[int, float] = {}
    for f in ("PDI.parquet", "PVI.parquet"):
        p = ruta_base / "fase1" / "auxiliares" / "nóminas" / f
        if not p.exists():
            continue
        df_s = pl.read_parquet(p)
        if "expediente" not in df_s.columns or "importe" not in df_s.columns:
            continue
        for r in (
            df_s.group_by("expediente")
            .agg(pl.col("importe").sum().alias("imp"))
            .iter_rows(named=True)
        ):
            brutos[int(r["expediente"])] = float(r["imp"])

    # Calcular X_persona.
    out: dict[int, float] = {}
    for per_id, grupo in exp_pdi_pvi.group_by("per_id"):
        per_id_int = int(per_id[0])
        num = 0.0
        den = 0.0
        for r in grupo.iter_rows(named=True):
            exp = int(r["expediente"])
            x_exp = factores_exp.get(exp, 1.0)
            b = brutos.get(exp, 1.0)  # peso uniforme si falta
            num += b * x_exp
            den += b
        if den > 0:
            x_per = num / den
            if x_per < 1.0:
                out[per_id_int] = x_per
    return out


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
