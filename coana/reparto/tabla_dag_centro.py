"""Tabla de reglas de reparto DAGs: reglas por *patrones de etiqueta*.

*Artefacto de diseño* (no proviene de la base de datos corporativa): lo
definimos al especificar el sistema, por eso vive como literal aquí y se
documenta en la especificación (§«Fase de reparto de actividades»).

Cada UC cuya **actividad** es dag (subárbol de `dags`) se reparte entre las
UC **finalistas** (subárbol de `principales`), conservando su elemento de
coste. El destino se decide con una *lista ordenada* de reglas: gana la
**primera** cuyo ORIGEN casa la UC (sin puntuar especificidad). El
atrapalotodo por defecto va el último.

Patrones (para actividad y para centro):

- ``"*"``            — cualquier nodo.
- ``"<etiqueta>"``   — exactamente ese nodo.
- ``"<etiqueta>.*"`` — ese nodo y todo su subárbol.

Se resuelven por *etiqueta → código → prefijo* (robusto al reordenado del
árbol, porque la regla se escribe con identificadores estables). El centro
**destino** admite además el comodín relacional ``MISMO`` (= el centro del
propio origen y su subárbol).

Una `ReglaDag` es ``ORIGEN(actividad, centro) → DESTINO(actividad, centro)``.
En cada lado, actividad y centro son *tuplas* de patrones; varios patrones
= *unión* de sus conjuntos (p. ej. destino actividad ``("docencia.*",
"ai.*")`` = todas las finalistas de docencia ∪ investigación).
"""

from __future__ import annotations

from dataclasses import dataclass


# Raíces (identificadores) de los subárboles dag y finalista.
RAÍZ_DAG = "dags"
RAÍZ_FINALISTAS = "principales"

# Comodín relacional para el centro DESTINO: el centro del origen y su
# subárbol. Permite expresar «repártelo dentro de su propio centro».
MISMO = "·mismo·"


@dataclass(frozen=True)
class ReglaDag:
    origen_actividad: tuple[str, ...]
    origen_centro: tuple[str, ...]
    destino_actividad: tuple[str, ...]
    destino_centro: tuple[str, ...]
    # Si el destino queda vacío (ninguna UC no-dag finalista casa), en vez
    # de marcar la UC como anomalía se *materializa*: se crea el destino
    # nombrado (las actividades de `destino_actividad`, sin `.*`) en el
    # centro destino, a partes iguales. Para grupos/institutos sin
    # actividad finalista propia, cuyo coste es su investigación propia.
    materializar: bool = False


def _regla(oa, oc, da, dc, materializar: bool = False) -> ReglaDag:
    """Constructor cómodo: acepta str o iterable de str en cada campo."""
    norm = lambda x: (x,) if isinstance(x, str) else tuple(x)
    return ReglaDag(norm(oa), norm(oc), norm(da), norm(dc), materializar)


# Lista ORDENADA: gana la primera cuyo ORIGEN casa. El defecto va el último.
REGLAS: list[ReglaDag] = [
    # --- Reglas por ACTIVIDAD (cada dag a su finalista homóloga) ---
    _regla("dag-deportes.*", "*", "deportes.*", "*"),
    _regla("dag-cultura.*", "*", "cultura.*", "*"),
    _regla("dag-cooperación.*", "*", "cooperación.*", "*"),
    _regla("dag-apoyo-estudiantes.*", "*", "apoyo-estudiantes.*", "*"),
    _regla("dag-divulgación-científica.*", "*", "divulgación-científica.*", MISMO,
           materializar=True),
    _regla("dag-escuela-doctorado.*", "*", "doctorado.*", "ed.*"),
    _regla("dag-biblioteca.*", "*", "docencia.* 20% + ai.* 80%", "*"),
    _regla("dag-apoyo-docencia-oficial.*", "*", "estudios-oficiales.*", "*"),
    _regla("dag-sgit.*", "*", "ai-financiación-propia.* + ait-financiación-externa.*", "*"),
    _regla("dag-estce.*", "*", "estudios-oficiales.*", "estce.*"),
    _regla("dag-fchs.*", "*", "estudios-oficiales.*", "fchs.*"),
    _regla("dag-fcje.*", "*", "estudios-oficiales.*", "fcje.*"),
    _regla("dag-fcs.*", "*", "estudios-oficiales.*", "fcs.*"),
    # Conserjerías de facultad (ANTES de dag-general-universidad, del que
    # cuelgan por código, para que no las capture esa regla general).
    _regla("dag-conserjería-estce", "*", "estudios-oficiales.*", "estce.*"),
    _regla("dag-conserjería-fcje", "*", "estudios-oficiales.*", "fcje.*"),
    _regla("dag-conserjería-fchs", "*", "estudios-oficiales.*", "fchs.*"),
    _regla("dag-conserjería-fcs", "*", "estudios-oficiales.*", "fcs.*"),
    _regla("dag-general-universidad.*", "*", "principales.*", "*"),
    # --- Reglas por CENTRO para la dag GENÉRICA (`dags`, amortizaciones) ---
    _regla("dags.*", "deportes.*", "deportes.*", "*"),
    _regla("dags.*", "ed.*", "doctorado.*", "*"),
    _regla("dags.*", "cooperación.*", "cooperación.*", "*"),
    _regla("dags.*", "fcje.*", "principales.*", "fcje.*"),
    # DEFECTO (último): cada dag entre las finalistas de su PROPIO centro.
    _regla("dags.*", "*", "principales.*", MISMO),
]
