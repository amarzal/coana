"""Cargador cargos → dedicación del PDI a actividades de gestión.

La dedicación de cada cargo se toma de ``cargos.xlsx``:

- Si ``dedicación_porcentual > 0``: ese porcentaje se aplica sobre las
  horas no docentes de la persona, prorrateado por los días de cobro
  en el año natural::

      horas_cargo = (días_cargo / 365) × pct × horas_no_docentes_persona

  El valor del xlsx tiene prioridad frente al cuadro 9.7 del modelo:
  permite afinar caso a caso (p. ej. un rector con actividad
  investigadora con < 100 %).

- Si ``dedicación_porcentual`` es nula o cero, se mira
  ``dedicación_horaria`` y, si es > 0, se interpreta como una cantidad
  anual absoluta de horas, prorrateada por los días de cobro::

      horas_cargo = (días_cargo / 365) × dedicación_horaria

- Si ambas son nulas o cero, el cargo no aporta horas (la fila no se
  emite).

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

from coana.util import read_excel

JORNADA_ANUAL_PDI = 1642.0


def cargar_cargos(
    ruta_base: Path,
    dedicación_previa: pl.DataFrame,
    jornada_anual: float = JORNADA_ANUAL_PDI,
) -> pl.DataFrame:
    """Genera filas de dedicación a gestión a partir de cargos_uc.parquet."""
    parquet_cargos = ruta_base / "fase1" / "auxiliares" / "nóminas" / "cargos_uc.parquet"
    cat_path = ruta_base / "entrada" / "nóminas" / "cargos.xlsx"
    if not parquet_cargos.exists() or not cat_path.exists():
        return _esquema_vacío()

    cargos = pl.read_parquet(parquet_cargos)
    if cargos.is_empty():
        return _esquema_vacío()

    # Dedicación del catálogo: porcentual (prioridad) y horaria (fallback).
    cat = read_excel(cat_path).select(
        pl.col("cargo").cast(pl.Utf8),
        pl.col("dedicación_porcentual").cast(pl.Float64),
        pl.col("dedicación_horaria").cast(pl.Float64),
    )
    cargos = cargos.with_columns(pl.col("cargo").cast(pl.Utf8)).join(
        cat, on="cargo", how="left",
    )

    pct = pl.col("dedicación_porcentual")
    hor = pl.col("dedicación_horaria")
    tiene_pct = pct.is_not_null() & (pct > 0)
    tiene_hor = hor.is_not_null() & (hor > 0)

    # Descartar cargos sin dedicación informada: ni porcentaje ni horas.
    cargos = cargos.filter(tiene_pct | tiene_hor)
    if cargos.is_empty():
        return _esquema_vacío()

    # Horas docencia efectiva (con factor) por persona.
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

    factor_año = pl.col("días").cast(pl.Float64) / 365.0
    horas_expr = (
        pl.when(tiene_pct)
        .then(factor_año * pct * pl.col("h_no_docentes"))
        .when(tiene_hor)
        .then(factor_año * hor)
        .otherwise(pl.lit(0.0))
    )
    cargos = cargos.with_columns(horas_expr.alias("horas")).filter(
        pl.col("horas") > 0
    )

    anomalía_expr = (
        pl.when(pl.col("_anomalía_patrón").is_not_null())
        .then(pl.col("_anomalía_patrón"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )

    # Detalle: refleja qué columna mandó (porcentual u horaria).
    detalle = pl.concat_str([
        pl.lit("Cargo "), pl.col("cargo").cast(pl.Utf8),
        pl.lit(" ("), pl.col("nombre_cargo").fill_null("?"), pl.lit(")"),
        pl.when(tiene_pct)
        .then(pl.concat_str([
            pl.lit(" · "), (pct * 100).round(1).cast(pl.Utf8),
            pl.lit(" % dedicación"),
        ]))
        .otherwise(pl.concat_str([
            pl.lit(" · "), hor.round(1).cast(pl.Utf8),
            pl.lit(" h/año dedicación"),
        ])),
        pl.lit(" · "), pl.col("días").cast(pl.Utf8), pl.lit(" días"),
    ])

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
        detalle.alias("detalle"),
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
        "detalle": pl.Utf8,
        "anomalía": pl.Utf8,
    })
