from dataclasses import dataclass

import coana.misc.typst as ty
from coana.configuración import Configuración
from coana.árbol import Árbol


@dataclass
class Estructuras:
    centros_de_coste_por_finalidad: Árbol
    centros_de_coste_por_comportamiento: Árbol
    elementos_de_coste: Árbol
    actividades: Árbol

    def __init__(self, cfg: Configuración) -> None:
        cc_finalidad = cfg.fichero("centros_de_coste_por_finalidad")
        cc_comportamiento = cfg.fichero("centros_de_coste_por_comportamiento")
        ec = cfg.fichero("elementos_de_coste")
        ac = cfg.fichero("actividades")
        self.centros_de_coste_por_finalidad = cc_finalidad.carga_árbol()
        self.centros_de_coste_por_comportamiento = cc_comportamiento.carga_árbol()
        self.elementos_de_coste = ec.carga_árbol()
        self.actividades = ac.carga_árbol()

        traza = cfg.traza
        traza("= Estructuras")
        for (título, estructura) in [
            ("Elementos de coste", self.elementos_de_coste),
            ("Centros de coste por finalidad", self.centros_de_coste_por_finalidad),
            ("Centros de coste por comportamiento", self.centros_de_coste_por_comportamiento),
            ("Actividades por finalidad", self.actividades),
        ]:
            traza(f"== {título}")
            traza("=== Árbol")
            traza(ty.árbol_a_tabla(título, estructura))
            traza("=== Índice")
            traza(ty.S(ty.árbol_a_tabla_índice(estructura)))
