"""Cargador cargos → dedicación del PDI a actividades de gestión.

Aplica la *estimación porcentual* del cuadro 9.7 de la regla 23: cada
cargo aporta un porcentaje de las horas no docentes de la persona,
prorrateado por los días de cobro en el año natural.

    horas_cargo = (días_cargo / 365) × pct_cuadro97 × horas_no_docentes_persona

Las horas no docentes se calculan a partir de la dedicación ya cargada
para esa persona (POD + tesis + docencia no oficial cuando exista):

    horas_no_docentes = max(jornada_anual − horas_docencia_efectiva, 0)

donde ``horas_docencia_efectiva`` ya incluye el factor ×2,5 de
impartición.

Se reutiliza ``cargos_uc.parquet`` (generado previamente en
``coana/fase1/cargos.py``) que ya trae cargo asimilado, días de solape
y actividad/centro resueltos.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

# Mapping cargo_asimilado (RD 1086/1989) → porcentaje del cuadro 9.7
# de la regla 23.
PCT_CUADRO_97: dict[int, float] = {
    1: 1.00,    # Rector/a
    2: 1.00,    # Vicerector/a
    3: 0.625,   # Degà/ana o director de centre
    4: 0.375,   # Director/a de Departament
    5: 0.375,   # Vicedegà o subdirector de centre
    6: 0.375,   # Director/a d'Institut Universitari
    7: 0.25,    # Secretari/ària de Departament
    8: 0.25,    # Coordinador/a de Curs d'Orientació (asimilado)
}

JORNADA_ANUAL_PDI = 1642.0


def cargar_cargos(
    ruta_base: Path,
    dedicación_previa: pl.DataFrame,
    jornada_anual: float = JORNADA_ANUAL_PDI,
) -> pl.DataFrame:
    """Genera filas de dedicación a gestión a partir de cargos_uc.parquet.

    ``dedicación_previa`` es la dedicación ya cargada por otras fuentes
    (POD, tesis, etc.) y se usa para calcular las horas no docentes por
    persona.
    """
    parquet_cargos = ruta_base / "fase1" / "auxiliares" / "nóminas" / "cargos_uc.parquet"
    if not parquet_cargos.exists():
        return _esquema_vacío()

    cargos = pl.read_parquet(parquet_cargos)
    if cargos.is_empty():
        return _esquema_vacío()

    # Porcentaje del cuadro 9.7 por cargo
    cargos = cargos.with_columns(
        pl.col("cargo_asimilado")
          .replace_strict(PCT_CUADRO_97, default=0.0, return_dtype=pl.Float64)
          .alias("pct_cuadro97")
    ).filter(pl.col("pct_cuadro97") > 0)

    # Horas docencia efectiva (con factor) por persona, a partir de las
    # fuentes ya cargadas en dedicación_pdi.
    if not dedicación_previa.is_empty():
        doc = (
            dedicación_previa
            .filter(pl.col("grupo").is_in(["docencia_oficial", "docencia_no_oficial"]))
            .group_by("per_id")
            .agg((pl.col("horas") * pl.col("factor")).sum().alias("h_doc_efectiva"))
        )
    else:
        doc = pl.DataFrame(
            schema={"per_id": pl.Int64, "h_doc_efectiva": pl.Float64}
        )

    cargos = cargos.join(doc, on="per_id", how="left").with_columns(
        pl.col("h_doc_efectiva").fill_null(0.0)
    )

    cargos = cargos.with_columns(
        pl.max_horizontal(
            pl.lit(jornada_anual) - pl.col("h_doc_efectiva"), pl.lit(0.0)
        ).alias("h_no_docentes")
    )

    cargos = cargos.with_columns(
        (
            (pl.col("días").cast(pl.Float64) / 365.0)
            * pl.col("pct_cuadro97")
            * pl.col("h_no_docentes")
        ).alias("horas")
    ).filter(pl.col("horas") > 0)

    anomalía_expr = (
        pl.when(pl.col("_anomalía_patrón").is_not_null())
        .then(pl.col("_anomalía_patrón"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )

    return cargos.select(
        pl.col("per_id").cast(pl.Int64),
        pl.col("actividad").fill_null("pendiente").alias("actividad"),
        pl.col("centro_de_coste").fill_null("pendiente").alias("centro_de_coste"),
        pl.col("horas").cast(pl.Float64),
        pl.lit("ep").alias("método"),
        pl.lit(1.0).alias("factor"),
        pl.lit("gestión").alias("grupo"),
        pl.lit("cargo").alias("origen"),
        pl.col("id").cast(pl.Utf8).alias("origen_id"),
        anomalía_expr.alias("anomalía"),
    )


def _esquema_vacío() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "per_id": pl.Int64,
        "actividad": pl.Utf8,
        "centro_de_coste": pl.Utf8,
        "horas": pl.Float64,
        "método": pl.Utf8,
        "factor": pl.Float64,
        "grupo": pl.Utf8,
        "origen": pl.Utf8,
        "origen_id": pl.Utf8,
        "anomalía": pl.Utf8,
    })
