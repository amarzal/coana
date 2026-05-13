"""Cargador POD → dedicación del PDI a actividades docentes oficiales.

Convierte los créditos computables del POD a horas y los reparte por
actividad/centro a través de la tabla ``titulaciones actividad centro``.

Año natural 2025 = sem 2 del curso 2024-25 (100 %) + sem 1 del curso
2025-26 (100 %) + anuales del curso 2024-25 (50 %) + anuales del curso
2025-26 (50 %). Los semestres 1 del curso 2024-25 y 2 del curso 2025-26
no caen en el año natural y se descartan.

1 crédito ECTS equivale a 10 horas de impartición. El factor ×2,5 de la
regla 23 se almacena en la columna ``factor`` para auditar; las horas
brutas (sin multiplicar por 2,5) son las "horas registradas".
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from coana.util import read_excel


def cargar_pod(ruta_base: Path, año: int = 2025) -> pl.DataFrame:
    """Lee pod.xlsx y produce filas de dedicación a actividades docentes."""
    año_curso_anterior = año - 1  # curso 2024-25 → 2024

    pod = read_excel(ruta_base / "entrada" / "docencia" / "pod.xlsx")

    asig_grados = (
        read_excel(ruta_base / "entrada" / "docencia" / "asignaturas grados.xlsx")
        .select(
            pl.col("asignatura"),
            pl.col("grado").alias("titulación"),
        )
        .with_columns(pl.lit("grado").alias("tipo_titulación"))
    )
    asig_másteres = (
        read_excel(ruta_base / "entrada" / "docencia" / "asignaturas másteres.xlsx")
        .select(
            pl.col("asignatura"),
            pl.col("máster").alias("titulación"),
        )
        .with_columns(pl.lit("máster").alias("tipo_titulación"))
    )
    asignaturas = pl.concat([asig_grados, asig_másteres])

    tit_act_cc = (
        read_excel(ruta_base / "entrada" / "docencia" / "titulaciones actividad centro.xlsx")
        .select("titulación", "actividad", "centro")
    )

    # Peso del año natural por (curso, semestre)
    pod = pod.with_columns(
        pl.when(
            (pl.col("curso_académico") == año_curso_anterior)
            & (pl.col("semestre") == "2")
        )
        .then(1.0)
        .when(
            (pl.col("curso_académico") == año) & (pl.col("semestre") == "1")
        )
        .then(1.0)
        .when(
            pl.col("curso_académico").is_in([año_curso_anterior, año])
            & pl.col("semestre").is_in(["A", "1-2"])
        )
        .then(0.5)
        .otherwise(0.0)
        .alias("peso_año_natural")
    ).filter(pl.col("peso_año_natural") > 0)

    pod = pod.join(asignaturas, on="asignatura", how="left").join(
        tit_act_cc, on="titulación", how="left"
    )

    # horas brutas = créditos × 10 × peso (impartición sin factor 2,5)
    pod = pod.with_columns(
        (
            pl.col("créditos_computables").fill_null(0.0)
            * 10.0
            * pl.col("peso_año_natural")
        ).alias("horas")
    ).filter(pl.col("horas") > 0)

    # Agregar por (per_id, asignatura, curso, semestre, titulación) para colapsar
    # filas duplicadas por grupo/subgrupo. Esto da un origen_id único por
    # impartición de asignatura.
    pod = pod.group_by(
        "per_id",
        "asignatura",
        "curso_académico",
        "semestre",
        "titulación",
        "tipo_titulación",
        "actividad",
        "centro",
    ).agg(pl.col("horas").sum())

    anomalía = (
        pl.when(pl.col("actividad").is_null() | pl.col("centro").is_null())
        .then(pl.lit("titulación sin mapeo a actividad/centro"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )

    return pod.select(
        pl.col("per_id").cast(pl.Int64),
        pl.col("actividad").fill_null("pendiente").alias("actividad"),
        pl.col("centro").fill_null("pendiente").alias("centro_de_coste"),
        pl.col("horas").cast(pl.Float64),
        pl.lit("md").alias("método"),
        pl.lit(2.5).alias("factor"),
        pl.lit("docencia_oficial").alias("grupo"),
        pl.lit("POD").alias("origen"),
        pl.concat_str(
            [
                pl.col("asignatura"),
                pl.lit("/"),
                pl.col("curso_académico").cast(pl.Utf8),
                pl.lit("/"),
                pl.col("semestre"),
            ]
        ).alias("origen_id"),
        anomalía.alias("anomalía"),
    )
