"""Cargador tesis → dedicación del PDI a dirección y tutela de tesis.

Cada fila de ``tesis.xlsx`` es un *periodo de matrícula* (no una tesis
completa: una misma tesis, identificada por ``per_id_alumno``, puede
tener varios periodos a lo largo del tiempo). El criterio para
considerar un periodo activo en el año natural es:

1. Si ``fecha_lectura_tesis < 1/1/año`` → fuera (ya leída antes).
2. Si ``fecha_inicio_tiempo > 31/12/año`` o ``fecha_inicio_tesis >
   31/12/año`` → fuera (empieza después).
3. Descartar todas las filas con ``estado ∈ {B, BV, BM}`` (bajas).
4. Mantener las que tienen al menos un día del rango
   ``[fecha_inicio_tiempo, fecha_fin_tiempo]`` dentro del año natural.
5. El estado superviviente es ``C`` (tiempo completo) o ``P`` (parcial).

Para cada fila superviviente:

- Base anual: ``104 h`` si ``estado = C``, ``52 h`` si ``estado = P``
  (la dedicación parcial recibe la mitad de las horas).
- Horas de la tesis en el año: ``base × días_activos / 365``.
- Reparto: ``tutor`` recibe el 10 %, los miembros de la lista de
  directores (``per_id_director`` + ``per_id_codirector`` +
  ``per_id_codirector2``, sin nulos) se reparten el 90 % a partes
  iguales. Si la misma persona figura como tutor y como director,
  recibe ambas slices (no se deduplica).

El código de programa de doctorado de cada tesis (``estudio``, 90xxx)
se cruza con ``data/entrada/docencia/doctorados.xlsx`` para obtener el
nombre del programa, y con
``data/entrada/docencia/doctorados actividad centro.xlsx`` para obtener
la etiqueta de actividad y centro de coste. Si ese mapeo no existe
todavía, la fila se emite con ``actividad="doctorado"`` y
``centro="pendiente"`` con anomalía que incluye el código y nombre del
programa para facilitar el mapeo posterior.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.util.configuración import cfg_float

# Constantes leídas de data/configuración.xlsx (ver
# coana.util.configuración). Se exponen como módulo-level vars para
# documentar su nombre y mantener compatibilidad de imports.
BASE_HORAS_C: float = cfg_float("tesis_horas_tiempo_completo")
BASE_HORAS_P: float = cfg_float("tesis_horas_tiempo_parcial")
RATIO_TUTOR: float = cfg_float("tesis_pct_tutor")
RATIO_DIRECTORES: float = cfg_float("tesis_pct_directores")


def cargar_tesis(ruta_base: Path, año: int = 2025) -> pl.DataFrame:
    """Genera filas de dedicación a tesis doctorales en el año natural."""
    inicio_año = date(año, 1, 1)
    fin_año = date(año, 12, 31)

    t = read_excel(ruta_base / "entrada" / "investigación" / "tesis.xlsx")
    t = t.with_columns(
        pl.col("per_id_codirector2").cast(pl.Int64, strict=False),
    )

    # 1. lectura < año → fuera
    t = t.filter(
        pl.col("fecha_lectura_tesis").is_null()
        | (pl.col("fecha_lectura_tesis") >= pl.lit(inicio_año))
    )

    # 2. inicios > fin del año → fuera
    t = t.filter(
        (pl.col("fecha_inicio_tiempo") <= pl.lit(fin_año))
        & (pl.col("fecha_inicio_tesis").cast(pl.Date) <= pl.lit(fin_año))
    )

    # 3. descartar bajas
    t = t.filter(~pl.col("estado").is_in(["B", "BV", "BM"]))

    # 4. solape ≥ 1 día con el año natural
    t = t.with_columns(
        pl.max_horizontal(pl.col("fecha_inicio_tiempo"), pl.lit(inicio_año)).alias(
            "inicio_solape"
        ),
        pl.min_horizontal(
            pl.col("fecha_fin_tiempo").fill_null(fin_año), pl.lit(fin_año)
        ).alias("fin_solape"),
    )
    t = t.with_columns(
        ((pl.col("fin_solape") - pl.col("inicio_solape")).dt.total_days() + 1)
        .alias("días_activos")
    ).filter(pl.col("días_activos") > 0)

    if t.is_empty():
        return _esquema_vacío()

    # 5/6. Horas anuales por fila + reparto tutor/directores
    t = t.with_columns(
        pl.when(pl.col("estado") == "C")
          .then(pl.lit(BASE_HORAS_C))
          .otherwise(pl.lit(BASE_HORAS_P))
          .alias("base_anual"),
        pl.concat_list([
            pl.col("per_id_director"),
            pl.col("per_id_codirector"),
            pl.col("per_id_codirector2"),
        ]).list.drop_nulls().alias("directores"),
    )
    t = t.with_columns(
        (pl.col("base_anual") * pl.col("días_activos").cast(pl.Float64) / 365.0)
        .alias("horas_tesis"),
        pl.col("directores").list.len().alias("n_dir"),
    )

    # Enriquecer cada fila con el nombre del programa de doctorado y, si
    # existe, con la etiqueta de actividad y centro de coste para esa
    # combinación de estudio (90xxx).
    t = _enriquecer_estudio(ruta_base, t)

    # Filas para el tutor (cuando exista) y para cada director.
    filas_tutor = (
        t.filter(pl.col("per_id_tutor").is_not_null())
         .with_columns((pl.col("horas_tesis") * RATIO_TUTOR).alias("horas"))
         .select(
            pl.col("per_id_tutor").cast(pl.Int64).alias("per_id"),
            "per_id_alumno", "estudio", "nombre_doctorado",
            "actividad", "centro",
            "horas",
            pl.lit("tutor").alias("rol"),
        )
    )

    filas_dir = (
        t.filter(pl.col("n_dir") > 0)
         .with_columns(
            (pl.col("horas_tesis") * RATIO_DIRECTORES / pl.col("n_dir")).alias(
                "horas_por_director"
            ),
        )
         .explode("directores")
         .select(
            pl.col("directores").cast(pl.Int64).alias("per_id"),
            "per_id_alumno", "estudio", "nombre_doctorado",
            "actividad", "centro",
            pl.col("horas_por_director").alias("horas"),
            pl.lit("director").alias("rol"),
        )
    )

    filas = pl.concat([filas_tutor, filas_dir]).filter(pl.col("horas") > 0)

    anomalía_expr = (
        pl.when(pl.col("actividad").is_null() | pl.col("centro").is_null())
        .then(
            pl.concat_str([
                pl.lit("programa de doctorado "),
                pl.col("estudio").cast(pl.Utf8),
                pl.lit(" ("),
                pl.col("nombre_doctorado").fill_null("?"),
                pl.lit(") sin mapeo a actividad/centro"),
            ])
        )
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )

    detalle = pl.concat_str([
        pl.lit("Tesis alumno "), pl.col("per_id_alumno").cast(pl.Utf8),
        pl.lit(" · rol "), pl.col("rol"),
        pl.lit(" · programa "), pl.col("estudio").cast(pl.Utf8),
        pl.lit(" ("), pl.col("nombre_doctorado").fill_null("?"), pl.lit(")"),
        pl.lit(" · "), pl.col("horas").round(2).cast(pl.Utf8), pl.lit(" h asignadas"),
    ])

    return filas.select(
        pl.col("per_id"),
        pl.col("actividad").fill_null("doctorado").alias("actividad"),
        pl.col("centro").fill_null("pendiente").alias("centro_de_coste"),
        pl.col("horas").cast(pl.Float64),
        pl.lit("et").alias("método"),
        pl.lit(1.0).alias("factor"),
        pl.lit("investigación").alias("grupo"),
        pl.lit("tesis").alias("origen"),
        pl.concat_str(
            [pl.col("per_id_alumno").cast(pl.Utf8), pl.lit("/"), pl.col("rol")]
        ).alias("origen_id"),
        detalle.alias("detalle"),
        anomalía_expr.alias("anomalía"),
    )


def _enriquecer_estudio(ruta_base: Path, t: pl.DataFrame) -> pl.DataFrame:
    """Cruza ``estudio`` con doctorados.xlsx (nombre) y, si existe,
    con doctorados actividad centro.xlsx (mapeo a árbol)."""
    doc_path = ruta_base / "entrada" / "docencia" / "doctorados.xlsx"
    if doc_path.exists():
        doc = (
            read_excel(doc_path)
            .with_columns(pl.col("estudio").cast(pl.Utf8).str.strip_chars())
            .filter(pl.col("estudio") != "")
            .with_columns(pl.col("estudio").cast(pl.Int64))
            .group_by("estudio").agg(pl.col("nombre").first().alias("nombre_doctorado"))
        )
        t = t.join(doc, on="estudio", how="left")
    else:
        t = t.with_columns(pl.lit(None, dtype=pl.Utf8).alias("nombre_doctorado"))

    map_path = ruta_base / "entrada" / "docencia" / "doctorados actividad centro.xlsx"
    if map_path.exists():
        m = read_excel(map_path).select("estudio", "actividad", "centro")
        # Filtrar filas con actividad/centro vacíos para que el join deje null
        m = m.with_columns(
            pl.when(pl.col("actividad").cast(pl.Utf8).str.strip_chars() == "")
              .then(None).otherwise(pl.col("actividad")).alias("actividad"),
            pl.when(pl.col("centro").cast(pl.Utf8).str.strip_chars() == "")
              .then(None).otherwise(pl.col("centro")).alias("centro"),
        )
        t = t.join(m, on="estudio", how="left")
    else:
        t = t.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("actividad"),
            pl.lit(None, dtype=pl.Utf8).alias("centro"),
        )
    return t


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
