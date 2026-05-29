"""Cargador POD → dedicación del PDI a actividades docentes oficiales.

Convierte los créditos computables del POD a horas y los reparte por
actividad/centro a través de la tabla ``titulaciones actividad centro``.

Año natural 2025 = sem 2 del curso 2024-25 (100 %) + sem 1 del curso
2025-26 (100 %) + anuales del curso 2024-25 (50 %) + anuales del curso
2025-26 (50 %). Los semestres 1 del curso 2024-25 y 2 del curso 2025-26
no caen en el año natural y se descartan.

Excepción (rescate de períodos «ocultos»): si una persona no tiene
NINGÚN crédito computable en el rango del año natural pero sí impartió
docencia en esos semestres fuera de rango, se rescatan esas filas a peso
completo (1,0). Así su coste se imputa a las titulaciones que impartió en
los períodos ocultos, en proporción a sus créditos, en lugar de caer a
(actividad=pendiente, centro=pendiente).

1 crédito ECTS equivale a 10 horas de impartición. El factor ×2,5 de la
regla 23 se almacena en la columna ``factor`` para auditar; las horas
brutas (sin multiplicar por 2,5) son las "horas registradas".
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.util.configuración import cfg_float, cfg_set

# Factor ×2,5 de la regla 23 sobre las horas de impartición de
# docencia. Origen: data/configuración.xlsx
# (clave `factor_impartición_docente`).
_FACTOR_DOCENTE: float = cfg_float("factor_impartición_docente")

# Másteres ficticios (sin alumnado matriculado): se descartan al cruzar
# el POD con `asignaturas másteres`. Cada una de sus asignaturas
# pertenece también a un máster real, por el que se captura su coste y
# dedicación, así que filtrarlos no pierde ninguna asignatura y evita el
# fan-out espurio del cruce. Origen: data/configuración.xlsx
# (clave `másteres_ficticios_pod`).
_MÁSTERES_FICTICIOS: set[str] = cfg_set("másteres_ficticios_pod")


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
        # Descartar los másteres ficticios antes del cruce: sus asignaturas
        # se capturan por su máster real (ver `_MÁSTERES_FICTICIOS`).
        .filter(~pl.col("máster").cast(pl.Utf8).is_in(_MÁSTERES_FICTICIOS))
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

    # Peso del año natural por (curso, semestre). El año natural está a
    # caballo de dos cursos: sem 2 del curso anterior (100 %), sem 1 del
    # curso actual (100 %) y la mitad de cada anual (50 %).
    peso_en_rango = (
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
    )
    # Períodos «ocultos»: la docencia de ambos cursos que cae FUERA del año
    # natural (sem 1 del curso anterior y sem 2 del curso actual).
    es_oculto = (
        ((pl.col("curso_académico") == año_curso_anterior) & (pl.col("semestre") == "1"))
        | ((pl.col("curso_académico") == año) & (pl.col("semestre") == "2"))
    )
    cred = pl.col("créditos_computables").cast(pl.Float64, strict=False).fill_null(0.0)
    pod = pod.with_columns(
        peso_en_rango.alias("_peso_en_rango"),
        es_oculto.alias("_oculto"),
        (cred * peso_en_rango).alias("_cred_en_rango"),
    )
    # Personas con alguna docencia dentro del rango del año natural.
    con_en_rango = pod.group_by("per_id").agg(
        (pl.col("_cred_en_rango").sum() > 0).alias("_tiene_en_rango")
    )
    pod = pod.join(con_en_rango, on="per_id", how="left")
    # Peso efectivo: el del rango; y SOLO para quien no tiene ninguna
    # docencia en el rango (0 créditos en-rango), se rescatan los períodos
    # ocultos a peso completo (1,0) para imputar su coste a las
    # titulaciones que impartió fuera del año natural.
    pod = pod.with_columns(
        pl.when(pl.col("_peso_en_rango") > 0)
        .then(pl.col("_peso_en_rango"))
        .when((~pl.col("_tiene_en_rango")) & pl.col("_oculto"))
        .then(1.0)
        .otherwise(0.0)
        .alias("peso_año_natural")
    ).filter(pl.col("peso_año_natural") > 0)

    pod = pod.join(asignaturas, on="asignatura", how="left").join(
        tit_act_cc, on="titulación", how="left"
    )

    # horas brutas = créditos × 10 × peso (impartición sin factor 2,5)
    pod = pod.with_columns(
        (cred * 10.0 * pl.col("peso_año_natural")).alias("horas")
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
        "_oculto",
    ).agg(pl.col("horas").sum())

    anomalía = (
        pl.when(pl.col("actividad").is_null() | pl.col("centro").is_null())
        .then(pl.lit("titulación sin mapeo a actividad/centro"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )

    detalle = pl.concat_str([
        pl.lit("Asignatura "), pl.col("asignatura"),
        pl.lit(" · curso "), pl.col("curso_académico").cast(pl.Utf8),
        pl.lit("/sem "), pl.col("semestre"),
        pl.lit(" · titulación "), pl.col("titulación").cast(pl.Utf8),
        pl.lit(" ("), pl.col("tipo_titulación"), pl.lit(")"),
        pl.lit(" · "), pl.col("horas").round(2).cast(pl.Utf8), pl.lit(" h registradas"),
        pl.when(pl.col("_oculto"))
          .then(pl.lit(" · rescatado de período fuera del año natural (sin docencia en rango)"))
          .otherwise(pl.lit("")),
    ])

    return pod.select(
        pl.col("per_id").cast(pl.Int64),
        pl.col("actividad").fill_null("pendiente").alias("actividad"),
        pl.col("centro").fill_null("pendiente").alias("centro_de_coste"),
        pl.col("horas").cast(pl.Float64),
        pl.lit("md").alias("método"),
        pl.lit(_FACTOR_DOCENTE).alias("factor"),
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
        detalle.alias("detalle"),
        anomalía.alias("anomalía"),
    )
