"""Regla 23: dedicación del PDI a actividades.

Genera ``dedicación_pdi.parquet`` con una fila por
``(per_id, actividad, origen, origen_id)``: las horas que cada PDI dedica
a cada actividad, agregadas desde las distintas fuentes (POD, tesis,
cargos, proyectos…) que se vayan incorporando.

El esquema es el siguiente:

============== =====================================================
Columna        Significado
============== =====================================================
per_id         PDI al que se imputa la dedicación
actividad      Etiqueta del árbol de actividades (o ``"pendiente"``)
centro_de_coste Etiqueta del árbol de CC (o ``"pendiente"``)
horas          Horas registradas (sin factor ×2,5)
método         ``md`` / ``ep`` / ``et`` / ``pr`` (regla 23)
factor         2,5 para impartición de docencia, 1,0 para el resto
grupo          ``docencia_oficial`` / ``docencia_no_oficial`` /
               ``gestión`` / ``investigación`` / ``extensión``
origen         ``POD`` / ``tesis`` / ``cargo`` / ``proyecto`` / …
origen_id      Identificador en la tabla origen
anomalía       Texto descriptivo si hay dato pendiente o nulo
============== =====================================================

Cada cargador es una función pura que produce un DataFrame con este
esquema. El orquestador concatena todos los DataFrames y escribe el
parquet final.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from coana.fase1.regla23.cargadores.cargos import cargar_cargos
from coana.fase1.regla23.cargadores.grupos import cargar_grupos
from coana.fase1.regla23.cargadores.pod import cargar_pod
from coana.fase1.regla23.cargadores.proyectos import cargar_proyectos
from coana.fase1.regla23.cargadores.tesis import cargar_tesis

log = logging.getLogger(__name__)


SCHEMA: dict[str, pl.DataType] = {
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
}


def generar_dedicación_pdi(
    ruta_base: Path = Path("data"),
    año: int = 2025,
    árbol_actividades=None,
) -> pl.DataFrame:
    """Genera la tabla unificada de dedicación del PDI.

    Si se pasa ``árbol_actividades``, el cargador de proyectos podrá
    enriquecerlo con nodos `transf-60-XXX` para los contratos de
    artículo 60.
    """
    fuentes: list[pl.DataFrame] = []

    log.info("Cargando POD…")
    pod = cargar_pod(ruta_base, año=año)
    log.info("  POD: %s filas, %.1f h totales", f"{len(pod):,}", pod["horas"].sum())
    fuentes.append(pod)

    log.info("Cargando tesis…")
    tesis = cargar_tesis(ruta_base, año=año)
    log.info("  tesis: %s filas, %.1f h totales", f"{len(tesis):,}", tesis["horas"].sum())
    fuentes.append(tesis)

    log.info("Cargando coordinaciones de grupo…")
    grupos = cargar_grupos(ruta_base, año=año, árbol_actividades=árbol_actividades)
    log.info("  grupos: %s filas, %.1f h totales", f"{len(grupos):,}", grupos["horas"].sum())
    fuentes.append(grupos)

    log.info("Cargando proyectos y contratos de transferencia…")
    proyectos = cargar_proyectos(
        ruta_base, árbol_actividades=árbol_actividades, año=año,
    )
    log.info("  proyectos: %s filas, %.1f h totales", f"{len(proyectos):,}", proyectos["horas"].sum())
    fuentes.append(proyectos)

    # Cargos: necesita la dedicación previa para calcular horas no docentes
    log.info("Cargando cargos…")
    dedicación_previa = pl.concat(fuentes, how="vertical_relaxed")
    cargos = cargar_cargos(ruta_base, dedicación_previa=dedicación_previa)
    log.info("  cargos: %s filas, %.1f h totales", f"{len(cargos):,}", cargos["horas"].sum())
    fuentes.append(cargos)

    if not fuentes:
        return pl.DataFrame(schema=SCHEMA)

    dedicación = pl.concat(fuentes, how="vertical_relaxed").select(list(SCHEMA.keys()))

    dir_salida = ruta_base / "fase1" / "regla23"
    dir_salida.mkdir(parents=True, exist_ok=True)
    dedicación.write_parquet(dir_salida / "dedicación_pdi.parquet")
    log.info(
        "dedicación_pdi.parquet: %s filas, %.1f h, %s personas",
        f"{len(dedicación):,}",
        dedicación["horas"].sum(),
        dedicación["per_id"].n_unique(),
    )

    return dedicación
