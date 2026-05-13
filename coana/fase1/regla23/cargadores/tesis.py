"""Cargador tesis → dedicación del PDI a dirección de tesis doctorales.

Cada tesis aporta 2 h/semana repartidas entre directores, tutor y
codirectores de la institución. Las horas se prorratean por los días
de solape con el año natural.

Mientras no dispongamos del programa de doctorado en ``tesis.xlsx`` el
centro de coste se marca como ``pendiente`` con anomalía
``"tesis sin programa de doctorado"`` y la actividad se asigna al
umbrella ``doctorado`` del árbol.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from coana.util import read_excel


def cargar_tesis(ruta_base: Path, año: int = 2025) -> pl.DataFrame:
    """Lee tesis.xlsx y produce filas de dedicación a tesis doctorales."""
    tesis = read_excel(ruta_base / "entrada" / "investigación" / "tesis.xlsx")

    inicio_año = date(año, 1, 1)
    fin_año = date(año, 12, 31)

    # Fin efectivo: lectura > fin_tiempo > fin del año (la dedicación termina
    # con la lectura, no con la última matrícula).
    tesis = tesis.with_columns(
        pl.coalesce(
            pl.col("fecha_lectura_tesis"),
            pl.col("fecha_fin_tiempo"),
            pl.lit(fin_año),
        ).alias("fecha_fin_efectiva")
    )

    tesis = tesis.with_columns(
        pl.max_horizontal(pl.col("fecha_inicio_tiempo"), pl.lit(inicio_año)).alias(
            "inicio_solape"
        ),
        pl.min_horizontal(pl.col("fecha_fin_efectiva"), pl.lit(fin_año)).alias(
            "fin_solape"
        ),
    )
    tesis = tesis.with_columns(
        (
            (pl.col("fin_solape") - pl.col("inicio_solape")).dt.total_days() + 1
        )
        .clip(lower_bound=0)
        .alias("días_solape")
    ).filter(pl.col("días_solape") > 0)

    # per_id_codirector2 viene como str en el xlsx; cast con strict=False
    # para tolerar "" como nulo.
    tesis = tesis.with_columns(
        pl.col("per_id_codirector2")
        .cast(pl.Int64, strict=False)
        .alias("per_id_codirector2")
    )

    # Unpivot a una fila por (tesis, rol) y luego deduplicar por (tesis, per_id):
    # si la misma persona aparece como director + tutor, cuenta una sola vez.
    df = (
        tesis.unpivot(
            index=["per_id_alumno", "días_solape"],
            on=[
                "per_id_director",
                "per_id_tutor",
                "per_id_codirector",
                "per_id_codirector2",
            ],
            variable_name="rol",
            value_name="per_id",
        )
        .filter(pl.col("per_id").is_not_null())
        .unique(subset=["per_id_alumno", "per_id"], keep="first")
    )

    # Número de personas únicas de la institución que dirigen cada tesis
    df = df.with_columns(
        pl.len().over("per_id_alumno").alias("n_dirección")
    ).filter(pl.col("n_dirección") > 0)

    # Horas/persona = 2 h/sem × (días_solape / 7) / N_único
    df = df.with_columns(
        (
            pl.lit(2.0)
            * pl.col("días_solape").cast(pl.Float64)
            / 7.0
            / pl.col("n_dirección").cast(pl.Float64)
        ).alias("horas_persona")
    )

    return df.select(
        pl.col("per_id").cast(pl.Int64),
        pl.lit("doctorado").alias("actividad"),
        pl.lit("pendiente").alias("centro_de_coste"),
        pl.col("horas_persona").cast(pl.Float64).alias("horas"),
        pl.lit("et").alias("método"),
        pl.lit(1.0).alias("factor"),
        pl.lit("investigación").alias("grupo"),
        pl.lit("tesis").alias("origen"),
        pl.col("per_id_alumno").cast(pl.Utf8).alias("origen_id"),
        pl.lit("tesis sin programa de doctorado").alias("anomalía"),
    )
