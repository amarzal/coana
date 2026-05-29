"""Tabla 1 del reparto de actividades: destino de cada actividad dag.

Es un *artefacto de diseño* (no proviene de la base de datos corporativa):
lo definimos nosotros al especificar el sistema. Por eso vive como literal
en el código y se documenta en la especificación —no como hoja de cálculo.

Cada actividad dag se reparte a las actividades **hoja** de un destino,
que es *o bien* un CENTRO (sus actividades finalistas) *o bien* una
ACTIVIDAD (las hojas de ese subárbol) — exactamente uno de los dos. Reglas:

1. *Convención*: ``dag-X`` reparte a las hojas del centro ``X``.
2. *Servicios centrales*: si el centro ``X`` no tiene actividades
   finalistas (servicios, edificios, grupos/institutos sin actividad
   propia), reparte a las hojas de la actividad global ``principales``
   (toda la actividad finalista de la UJI).
3. *Excepciones explícitas* (``_EXCEPCIONES``): una de centro/actividad por
   entrada.
"""

from __future__ import annotations

from pathlib import Path

from coana.util import Árbol
from coana.reparto._servicios import SERVICIOS

# Nodo de actividad que agrupa todas las actividades finalistas.
GLOBAL_FINALISTA = "principales"

_PREFIJO_DAG = "dag-"
_PREFIJO_CONSERJERIA = "dag-conserjería-"
_PREFIJO_PS = "ps-"

# Destino explícito de cada actividad dag. Une la tabla completa de
# SERVICIOS de la UJI (artefacto de diseño en `_servicios.py`, reflejada
# en la spec) con un par de excepciones generales. Cada entrada apunta a
# UN destino: ("centro", X) reparte a las hojas finalistas del centro X;
# ("actividad", Y) reparte a las hojas de la actividad Y.
_EXCEPCIONES: dict[str, tuple[str, str]] = {
    **SERVICIOS,
    # Amortizaciones genéricas imputadas al nodo paraguas `dags`:
    "dags": ("actividad", GLOBAL_FINALISTA),
}


def _árbol_centros(ruta_base: Path) -> Árbol:
    final = Path(ruta_base) / "fase1" / "centros de coste.tree"
    if final.exists():
        return Árbol.from_file(final)
    return Árbol.from_file(
        Path(ruta_base) / "entrada" / "estructuras" / "centros de coste.tree"
    )


def resolver(
    actividad_dag: str, centros_con_finalistas: set[str],
) -> tuple[str, str]:
    """Destino de una actividad dag: ``(tipo, destino)`` con
    ``tipo ∈ {"centro", "actividad"}``.

    ``centros_con_finalistas`` es el conjunto de centros que tienen
    actividades finalistas (no-dag); decide la regla 2.
    """
    if actividad_dag in _EXCEPCIONES:
        tipo, dest = _EXCEPCIONES[actividad_dag]
        if tipo == "centro" and dest not in centros_con_finalistas:
            return ("actividad", GLOBAL_FINALISTA)
        return (tipo, dest)
    if actividad_dag.startswith(_PREFIJO_CONSERJERIA):
        cand = _PREFIJO_PS + actividad_dag[len(_PREFIJO_CONSERJERIA):]
        if cand in centros_con_finalistas:
            return ("centro", cand)
        return ("actividad", GLOBAL_FINALISTA)
    if actividad_dag.startswith(_PREFIJO_DAG):
        cand = actividad_dag[len(_PREFIJO_DAG):]
        if cand in centros_con_finalistas:
            return ("centro", cand)
    # Regla 2: servicio central sin base finalista → actividad global.
    return ("actividad", GLOBAL_FINALISTA)


def cargar_tabla(ruta_base: Path = Path("data")) -> dict[str, str]:
    """Compatibilidad con el chequeo de consistencia del motor actual:
    devuelve ``{actividad_dag: centro_esperado}`` para las entradas cuyo
    destino es un centro existente (convención + excepciones de centro).
    """
    centros = set(_árbol_centros(ruta_base)._por_id)
    out: dict[str, str] = {}
    for act, (tipo, dest) in _EXCEPCIONES.items():
        if tipo == "centro" and dest in centros:
            out[act] = dest
    return out
