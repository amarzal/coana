"""Cargador grupos de investigación → coordinadores reciben 2 h/sem.

Según el cuadro 9.7 de la regla 23, la *coordinación o dirección de
grupos de investigación* aporta 2 h/semana al PDI coordinador, durante
los días que coordine el grupo en el año natural.

Solo se procesan filas con ``coordinador = 'S'`` en
``investigadores en grupos.xlsx``. Los demás miembros del grupo (y los
colaboradores) no reciben horas por aquí: las recibirán cuando carguemos
los proyectos concretos en los que participan.

Centro de coste: ``grupo-investigación-{id_grupo}`` (creado por
``coana/fase1/investigación.py``).

Actividad: ``dag-grupo-investigación-{id_grupo}``, insertada
dinámicamente en el árbol de actividades como hija de ``dag-{instituto}``
(p. ej. ``dag-iei`` para grupos del IEI) o de ``dag-inves`` para los
grupos no adscritos a institutos.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from coana.util import read_excel, Árbol

# Mapeo código de instituto en `grupos a institutos.xlsx` → etiqueta
# de actividad del padre en el árbol (`dag-{algo}`). Coincide con el
# mapeo de centros (`_INSTITUTO_A_ETIQUETA` en
# ``coana/fase1/investigación.py``) pero anteponiendo `dag-`.
_INSTITUTO_A_PADRE_ACT: dict[str, str] = {
    "INAM":   "dag-inam",
    "INIT":   "dag-init",
    "IIDL":   "dag-iidl",
    "IIEI":   "dag-iei",
    "IIFV":   "dag-ifv",
    "IIG":    "dag-iigeo",
    "IILP":   "dag-ii-lópez-piñero",
    "IMAC":   "dag-imac",
    "IUPA":   "dag-iupa",
    "IUDSP":  "dag-idsp",
    "IUDT":   "dag-iudt",
    "IUT":    "dag-iuturismo",
    "IUCE":   "dag-iuce",
    "IUTC":   "dag-iutc",
    "IULMA":  "dag-ilma",
    "IUEFG":  "dag-iuef",
    "INVES":  "dag-inves",
}


def cargar_grupos(
    ruta_base: Path,
    año: int = 2025,
    árbol_actividades: Árbol | None = None,
) -> pl.DataFrame:
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

    # Adscripción del grupo a un instituto (o INVES). Filtra a grupos
    # válidos (los del catálogo `grupos a institutos.xlsx`).
    map_path = ruta_base / "entrada" / "investigación" / "grupos a institutos.xlsx"
    if map_path.exists():
        mapeo = read_excel(map_path).select(
            pl.col("id_grupo").cast(pl.Utf8),
            pl.col("instituto").cast(pl.Utf8),
        )
        coord = coord.with_columns(pl.col("id_grupo").cast(pl.Utf8)).join(
            mapeo, on="id_grupo", how="inner",
        )
    else:
        coord = coord.with_columns(pl.lit("INVES").alias("instituto"))

    # Deduplicar por (per_id, id_grupo)
    coord = coord.sort("fecha_alta").unique(
        subset=["per_id", "id_grupo"], keep="first"
    )

    inicio_año = date(año, 1, 1)
    fin_año = date(año, 12, 31)

    coord = coord.with_columns(
        pl.col("fecha_baja").fill_null(fin_año).alias("fecha_baja_efectiva")
    )
    coord = coord.with_columns(
        pl.max_horizontal(pl.col("fecha_alta"), pl.lit(inicio_año)).alias("inicio_solape"),
        pl.min_horizontal(pl.col("fecha_baja_efectiva"), pl.lit(fin_año)).alias("fin_solape"),
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

    # Insertar nodos `dag-grupo-investigación-XXX` en el árbol de
    # actividades como hijos de `dag-{instituto}`.
    if árbol_actividades is not None:
        nombre_norm = (
            coord["nombre_grupo"].fill_null("?")
            .str.replace_all(r"\s+", " ").str.strip_chars()
        )
        for r in coord.with_columns(nombre_norm.alias("nombre_norm")).iter_rows(named=True):
            padre = _INSTITUTO_A_PADRE_ACT.get(r["instituto"])
            if padre is None:
                continue
            try:
                árbol_actividades.añadir_hijo(
                    padre,
                    f"Coord. grupo {r['id_grupo']} · {r['nombre_norm']}",
                    r["id_grupo"],
                    id_completo=f"dag-grupo-investigación-{r['id_grupo']}",
                )
            except (KeyError, ValueError):
                pass

    nombre_limpio = (
        pl.col("nombre_grupo").fill_null("?")
        .str.replace_all(r"\s+", " ").str.strip_chars()
    )
    detalle = pl.concat_str([
        pl.lit("Coord. grupo "), pl.col("id_grupo"),
        pl.lit(" ("), nombre_limpio, pl.lit(")"),
        pl.lit(" · instituto "), pl.col("instituto"),
        pl.lit(" · "), pl.col("días_solape").cast(pl.Utf8), pl.lit(" días activos"),
    ])

    return coord.select(
        pl.col("per_id").cast(pl.Int64),
        pl.concat_str([pl.lit("dag-grupo-investigación-"), pl.col("id_grupo")]).alias("actividad"),
        pl.concat_str([pl.lit("grupo-investigación-"), pl.col("id_grupo")]).alias("centro_de_coste"),
        pl.col("horas").cast(pl.Float64),
        pl.lit("et").alias("método"),
        pl.lit(1.0).alias("factor"),
        pl.lit("investigación").alias("grupo"),
        pl.lit("grupo").alias("origen"),
        pl.col("id_grupo").cast(pl.Utf8).alias("origen_id"),
        detalle.alias("detalle"),
        pl.lit(None, dtype=pl.Utf8).alias("anomalía"),
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
