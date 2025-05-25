from dataclasses import dataclass

import coana.misc.typst as ty
from coana.ficheros import Ficheros
from coana.misc.traza import Traza
from coana.misc.utils import Singleton
from coana.árbol import Árbol


@dataclass
class Estructuras(metaclass=Singleton):
    centros_de_coste_por_finalidad: Árbol
    centros_de_coste_por_comportamiento: Árbol
    elementos_de_coste: Árbol
    actividades: Árbol

    def __init__(self) -> None:
        ficheros = Ficheros()
        cc_finalidad = ficheros.fichero("centros_de_coste_por_finalidad")
        cc_comportamiento = ficheros.fichero("centros_de_coste_por_comportamiento")
        ec = ficheros.fichero("elementos_de_coste")
        ac = ficheros.fichero("actividades")
        self.centros_de_coste_por_finalidad = cc_finalidad.carga_árbol()
        self.centros_de_coste_por_comportamiento = cc_comportamiento.carga_árbol()
        self.elementos_de_coste = ec.carga_árbol()
        self.actividades = ac.carga_árbol()

    def traza(self) -> None:
        traza = Traza()
        traza("= Estructuras")

        traza("== Elementos de coste")
        traza("=== Árbol")
        traza(ty.árbol_a_tabla("Elementos de coste", self.elementos_de_coste))
        traza("=== Índice")
        traza(ty.S(ty.árbol_a_tabla_índice(self.elementos_de_coste)))

        traza("== Centros de coste por finalidad")
        traza("=== Árbol")
        traza(ty.árbol_a_tabla("Centros de coste por finalidad", self.centros_de_coste_por_finalidad))
        traza("=== Índice")
        traza(ty.S(ty.árbol_a_tabla_índice(self.centros_de_coste_por_finalidad)))

        traza("== Centros de coste por comportamiento")
        traza("=== Árbol")
        traza(ty.árbol_a_tabla("Centros de coste por comportamiento", self.centros_de_coste_por_comportamiento))
        traza("=== Índice")
        traza(ty.S(ty.árbol_a_tabla_índice(self.centros_de_coste_por_comportamiento)))

        traza("== Actividades por finalidad")
        traza("=== Árbol")
        traza(ty.árbol_a_tabla("Actividades por finalidad", self.actividades))
        traza("=== Índice")
        traza(ty.S(ty.árbol_a_tabla_índice(self.actividades)))
