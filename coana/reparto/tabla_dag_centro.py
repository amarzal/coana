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
    # de marcar la UC como anomalía se *materializa*: se crea una actividad
    # nombrada en el centro destino, a partes iguales. Para grupos/institutos
    # sin actividad finalista propia, cuyo coste es su investigación propia.
    materializar: bool = False
    # Actividad(es) a materializar cuando el destino queda vacío. Vacío =
    # usar las bases de `destino_actividad` (comportamiento histórico). Permite
    # *repartir* entre las principales reales del centro (`destino_actividad =
    # principales.*`) y, solo si no hay ninguna, *crear* una actividad de
    # reserva distinta (p. ej. `otras-ait-financiación-propia`).
    materializar_actividad: tuple[str, ...] = ()


def _regla(oa, oc, da, dc, materializar: bool = False,
           materializar_actividad=()) -> ReglaDag:
    """Constructor cómodo: acepta str o iterable de str en cada campo."""
    norm = lambda x: (x,) if isinstance(x, str) else tuple(x)
    return ReglaDag(norm(oa), norm(oc), norm(da), norm(dc),
                    materializar, norm(materializar_actividad))


# Lista ORDENADA: gana la primera cuyo ORIGEN casa. El defecto va el último.
REGLAS: list[ReglaDag] = [
    # --- Reglas por ACTIVIDAD (cada dag a su finalista homóloga) ---
    # Cosas de extensión
    _regla("dag-deportes.*", "*", "deportes.*", "*"),
    _regla("dag-cultura.*", "*", "cultura.*", "*"),
    _regla("dag-cooperación.*", "*", "cooperación.*", "*"),
    _regla("dag-divulgación-científica.*", "*", "divulgación-científica.*", MISMO,
           materializar=True),
    _regla("dag-apoyo-estudiantes.*", "*", "apoyo-estudiantes.*", "*"),
    _regla("dag-biblioteca.*", "*", "docencia.* 20% + ai.* 80%", "*"),
    _regla("dag-apoyo-docencia-oficial.*", "*", "estudios-oficiales.*", "*"),
    # Encargos de gestión y similares
    _regla("dag-apoyo-proyectos-internacionales.*", "*", "ai-internacional.*", "*"),
    # Estudios propios y microcredenciales
    _regla("dag-encargos-gestión-estudios-propios.*", "*",
           "másteres-formación-permanente.* + diplomas-especialización"
           " + diplomas-experto + cursos-formación-permanente", "*"),
    _regla("dag-encargos-gestión-microcredenciales.*", "*", "microcredenciales.*", "*"),
    # Actividades Europa / internacional
    _regla("dag-encargos-proyectos-investigación-europeos.*", "*", "ai-internacional.*", "*"),
    # Actividades de transferencia (incluye los encargos de gestión de
    # Espaitec, que cuelgan de este subárbol).
    _regla("dag-apoyo-transferencia-conocimiento.*", "*",
           "otras-ait-financiación-propia.* + transf.*", "*"),
    # Algunos centros especiales. El destino CENTRO con varios patrones va
    # como tupla (no como cadena «a + b»: el «+» solo lo parsea el destino
    # ACTIVIDAD para repartos porcentuales).
    _regla("dag-scic", "*", "principales.*", ("inam.*", "iupa.*", "iutc.*")),
    _regla("dag-sea", "*", "principales.*", (
        "grupo-investigación-311", "grupo-investigación-278", "grupo-investigación-206",
        "grupo-investigación-222", "grupo-investigación-326", "grupo-investigación-317",
        "grupo-investigación-207", "grupo-investigación-307",
    )),
    # Laboratorios singulares: su coste va a las actividades del ámbito al
    # que sirven (en cualquier centro).
    _regla("dag-labcom", "*", "ámbito-periodismo.*", "*"),
    _regla("dag-sala-disección", "*",
           "ámbito-medicina.* + cursos-formación-permanente-24G056.*", "*"),
    _regla("dag-escuela-doctorado.*", "*", "doctorado.*", "ed.*"),
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
    # Generales universidad
    _regla("dag-general-universidad.*", "*", "principales.*", "*"),
    # --- Reglas por CENTRO para la dag GENÉRICA (`dags`, amortizaciones) ---
    _regla("dags.*", "deportes.*", "deportes.*", "*"),
    _regla("dags.*", "ed.*", "principales.*", "*"),
    _regla("dags.*", "cooperación.*", "cooperación.*", "*"),
    _regla("dags.*", "fcje.*", "principales.*", "fcje.*"),
    _regla("dags.*", "fchs.*", "principales.*", "fchs.*"),
    _regla("dags.*", "estce.*", "principales.*", "estce.*"),
    _regla("dags.*", "fcs.*", "principales.*", "fcs.*"),
    _regla("dags.*", "paraninfo.*", "cultura.*", "*"),
    _regla("dags.*", "llotja-cànem.*", "cultura.*", "*"),
    _regla("dags.*", "soporte.*", "principales.*", "*"),
    _regla("dags.*", "apoyo-docencia-investigación.*", "principales.*", "*"),
    _regla("dags.*", "anexos.*", "principales.*", "*"),
    _regla("dags.*", "centros-agrupaciones-costes.*", "principales.*", "*"),
    _regla("dags.*", "centros-intermedios-coste.*", "principales.*", "*"),
    # Familias de centros que reparten dentro de sí mismas (·mismo·) entre sus
    # principales REALES y, si un centro no tiene ninguna, materializan una
    # actividad de reserva propia de la familia. Cubre investigación (institutos,
    # grupos, laboratorios y cátedras de investigación) y departamentos.
    _regla("dags.*", "investigación.*", "principales.*", MISMO,
           materializar=True, materializar_actividad="otras-ait-financiación-propia"),
    _regla("dags.*", "departamentos.*", "principales.*", MISMO,
           materializar=True, materializar_actividad="estudios-oficiales"),
    # DEFECTO (último): cada dag entre las finalistas de su PROPIO centro.
    _regla("dags.*", "*", "principales.*", MISMO),
]
