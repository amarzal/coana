"""Tabla 1 del reparto de actividades dag: reglas (índice → destino).

*Artefacto de diseño* (no proviene de la base de datos corporativa): lo
definimos al especificar el sistema, por eso vive como literal aquí y se
documenta en la especificación.

Cada UC cuya **actividad** es dag (subárbol de `dags`, código `02.*`) se
reparte entre actividades **finalistas** (subárbol de `principales`, `01.*`),
conservando su elemento de coste. El destino se decide así (ver
`coana/reparto/reparto.py`):

1. *Tabla de reglas* (`REGLAS`, lista ORDENADA): la **primera** regla cuyo
   índice case `(centro, actividad)` de la UC manda.
2. *Defecto*: si ninguna regla casa y el centro de la UC tiene actividades
   finalistas propias, se reparte entre ellas (dentro del centro).
3. *Anomalía*: si ni regla ni base, la UC queda sin repartir (se conserva
   intacta) y se lista para revisión → material para diseñar nuevas reglas.

Una `ReglaDag` tiene:

- **índice** `(centro_índice, actividad_índice)`: captura las UC dag con
  `centro ∈ subárbol(centro_índice)` y `actividad ∈ subárbol(actividad_índice)`.
- **destino** `(centro_destino, actividades_destino)`: reparte a las finalistas
  hoja con `centro ∈ subárbol(centro_destino)` y
  `actividad ∈ ⋃ subárbol(actividades_destino)`, ponderando por su coste no-dag.

`UJI` es la raíz del árbol de centros (toda la universidad); `principales` la
raíz de las finalistas; `dags` la raíz de las dag.
"""

from __future__ import annotations

from dataclasses import dataclass


RAÍZ_CENTROS = "UJI"
RAÍZ_DAG = "dags"
RAÍZ_FINALISTAS = "principales"

# Centinela para `centro_destino`: «el mismo centro de la UC dag». La regla
# reparte entre las finalistas del propio centro (= comportamiento por
# defecto); si ese centro NO tiene actividad finalista propia (grupos e
# institutos sin actividad, sin UCs previas), la UC dag se *transforma* en una
# UC no-dag con las `actividades_destino` nombradas (a partes iguales),
# conservando su centro y recordando su procedencia en `marca_dag`.
MISMO_CENTRO = "·mismo·"


@dataclass(frozen=True)
class ReglaDag:
    centro_índice: str
    actividad_índice: str
    centro_destino: str
    actividades_destino: tuple[str, ...]


# Reglas para los centros SIN actividad finalista propia (servicios, apoyo,
# anexos, edificios…), cuya dag no puede repartirse «dentro del centro». Orden =
# prioridad (primera que casa). Semilla inicial: todo el dag de esas ramas se
# reparte a las finalistas de toda la UJI ponderando por coste. Se irá
# refinando analizando las anomalías (p. ej. acotar a `["ai","docencia"]`, o
# dirigir un edificio concreto a su facultad).
REGLAS: list[ReglaDag] = [
    # Extensión universitaria (paraninfo, llotja): su dag a actividades de cultura.
    ReglaDag("paraninfo", "dags", "UJI", ("cultura",)),
    ReglaDag("llotja-cànem", "dags", "UJI", ("cultura",)),
    # Unidad de divulgación científica: su dag (soporte a divulgación) se
    # transforma en la actividad finalista `divulgación-científica` del propio
    # centro (sin base previa → reparto a partes iguales sobre la nombrada).
    ReglaDag(
        "unidad-divulgación-científica", "dag-divulgación-científica",
        "unidad-divulgación-científica", ("divulgación-científica",),
    ),
    # Centros de soporte (servicios: gerencia, secretaría general, SGIT…).
    ReglaDag("soporte", "dags", "UJI", ("principales",)),
    # Centros de apoyo a docencia e investigación (bibliotecas, OPP…).
    ReglaDag("apoyo-docencia-investigación", "dags", "UJI", ("principales",)),
    # Centros anexos.
    ReglaDag("anexos", "dags", "UJI", ("principales",)),
    # Agrupaciones de costes (locales de fundaciones, etc.).
    ReglaDag("centros-agrupaciones-costes", "dags", "UJI", ("principales",)),
    # Centros intermedios de coste (edificios).
    ReglaDag("centros-intermedios-coste", "dags", "UJI", ("principales",)),
    # Centros de investigación (grupos e institutos): si el grupo tiene
    # actividad finalista propia, su dag se reparte entre ellas (mismo centro);
    # si no tiene ninguna (sin UCs previas), la dag se transforma en una UC de
    # `otras-ait-financiación-propia` en el propio grupo.
    ReglaDag("investigación", "dags", MISMO_CENTRO, ("otras-ait-financiación-propia",)),
]
