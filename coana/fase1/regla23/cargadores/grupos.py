"""Cargador grupos de investigación → coordinadores reciben 2 h/sem.

Según el cuadro 9.7 de la regla 23, la *coordinación o dirección de
grupos de investigación* aporta 2 h/semana al PDI coordinador, durante
los días que coordine el grupo en el año natural.

Solo se procesan filas con ``coordinador = 'S'`` en
``investigadores en grupos.xlsx``. Los demás miembros del grupo (y los
colaboradores) no reciben horas por aquí: las recibirán cuando carguemos
los proyectos concretos en los que participan.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from coana.util import read_excel


def cargar_grupos(ruta_base: Path, año: int = 2025) -> pl.DataFrame:
    """Genera filas de dedicación para coordinadores de grupo activos."""
    inv = read_excel(ruta_base / "entrada" / "investigación" / "investigadores en grupos.xlsx")
    coord = inv.filter(pl.col("coordinador") == "S")
    if coord.is_empty():
        return _esquema_vacío()

    grupos = read_excel(
        ruta_base / "entrada" / "investigación" / "grupos investigación.xlsx"
    ).select(
        pl.col("grupo").alias("id_grupo"),
        pl.col("nombre").alias("nombre_grupo"),
        pl.col("activo").alias("grupo_activo"),
    )
    coord = coord.join(grupos, on="id_grupo", how="left")

    # Deduplicar por (per_id, id_grupo): si una persona figura como
    # coordinadora en varias líneas del mismo grupo, sigue siendo una
    # única coordinación (2 h/sem, no 6 h/sem).
    coord = coord.sort("fecha_alta").unique(
        subset=["per_id", "id_grupo"], keep="first"
    )

    inicio_año = date(año, 1, 1)
    fin_año = date(año, 12, 31)

    coord = coord.with_columns(
        pl.col("fecha_baja").fill_null(fin_año).alias("fecha_baja_efectiva")
    )

    coord = coord.with_columns(
        pl.max_horizontal(pl.col("fecha_alta"), pl.lit(inicio_año)).alias(
            "inicio_solape"
        ),
        pl.min_horizontal(pl.col("fecha_baja_efectiva"), pl.lit(fin_año)).alias(
            "fin_solape"
        ),
    )
    coord = coord.with_columns(
        ((pl.col("fin_solape") - pl.col("inicio_solape")).dt.total_days() + 1)
        .clip(lower_bound=0)
        .alias("días_solape")
    ).filter(pl.col("días_solape") > 0)

    # 2 h/semana × días/7
    coord = coord.with_columns(
        (pl.lit(2.0) * pl.col("días_solape").cast(pl.Float64) / 7.0).alias("horas")
    ).filter(pl.col("horas") > 0)

    return coord.select(
        pl.col("per_id").cast(pl.Int64),
        pl.lit("pendiente").alias("actividad"),
        pl.lit("pendiente").alias("centro_de_coste"),
        pl.col("horas").cast(pl.Float64),
        pl.lit("et").alias("método"),
        pl.lit(1.0).alias("factor"),
        pl.lit("investigación").alias("grupo"),
        pl.lit("grupo").alias("origen"),
        pl.col("id_grupo").cast(pl.Utf8).alias("origen_id"),
        pl.lit("coordinación de grupo sin mapeo a actividad/centro").alias("anomalía"),
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
