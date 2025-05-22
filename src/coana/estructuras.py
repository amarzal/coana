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
        self.centros_de_coste_por_finalidad = Árbol.carga(ficheros.centros_de_coste_por_finalidad)
        self.centros_de_coste_por_comportamiento = Árbol.carga(ficheros.centros_de_coste_por_comportamiento)
        self.elementos_de_coste = Árbol.carga(ficheros.elementos_de_coste)
        self.actividades = Árbol.carga(ficheros.actividades)

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
