from dataclasses import dataclass

import polars as pl

from coana.configuración import Configuración
from coana.misc.utils import Singleton


@dataclass
class Maestros(metaclass=Singleton):
    _centros_id2nombre: dict[int, str]
    _centros_id2código: dict[int, str]
    _centros_código2id: dict[str, int]

    _subcentros: dict[int | str, dict[str, str]]

    _proyectos: dict[str, str]

    _subproyectos: dict[str, dict[str, str]]

    def __init__(self):
        ficheros = Ficheros()
        # Centros
        df = ficheros.fichero("maestro-centros").carga_dataframe()
        self._centros_id2nombre = {row["id"]: row["nombre"] for row in df.rows(named=True)}
        self._centros_id2código = {row["id"]: row["código"] for row in df.rows(named=True)}
        self._centros_código2id = {row["código"]: row["id"] for row in df.rows(named=True)}

        # Subcentros
        df = ficheros.fichero("maestro-subcentros").carga_dataframe()
        for row in df.rows(named=True):
            if row["id_centro"] not in self._subcentros:
                self._subcentros[row['id_centro']] = {}
            self._subcentros[row['código_subcentro']] = row['nombre']

        # Proyectos
        df = ficheros.fichero("maestro-proyectos").carga_dataframe()
        self._proyectos = {row["código"]: row["nombre"] for row in df.rows(named=True)}

        # Subproyectos
        df = ficheros.fichero("maestro-subproyectos").carga_dataframe()
        self._subproyectos = {(row["código_proyecto"], row["id_subproyecto"]): row["nombre"] for row in df.rows(named=True)}

        print(self._centros_id2código, self._centros_código2id)

    def centro(self, id: int | str, default: str = "") -> str:
        match id:
            case int():
                return self._centros_id2nombre.get(id, default)
            case str():
                return self.centro(self._centros_código2id.get(id, 0), default)

    def subcentro(self, código_centro: int | str, código_subcentro: str, default: str = "") -> str:
        if isinstance(código_centro, int):
            código_centro = self._centros_id2código.get(código_centro, "")
        subcentros = self._subcentros.get(código_centro, {})
        return subcentros.get(código_subcentro, default)

    def proyecto(self, id: str, default: str = "") -> str:
        return self._proyectos.get(id, default)

    def subproyecto(self, id: str, id_subproyecto: str, default: str = "") -> str:
        return self._subproyectos.get((id, id_subproyecto), default)
