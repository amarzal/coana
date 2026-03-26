"""Contexto de presupuesto: carga de todos los ficheros de referencia.

Reúne en una sola clase los DataFrames de los ficheros Excel de
presupuesto de gasto y los árboles de estructuras, dejando todo
listo para aplicar las reglas de generación de unidades de coste.
"""

import logging
from pathlib import Path

import polars as pl

from coana.util.arbol import Árbol
from coana.util.excel_cache import read_excel

log = logging.getLogger(__name__)


class ContextoPresupuesto:
    """Carga y almacena los datos de referencia del presupuesto de gasto."""

    def __init__(self, ruta_base: Path = Path("data")) -> None:
        presupuesto = Path(ruta_base) / "entrada" / "presupuesto"
        estructuras = Path(ruta_base) / "entrada" / "estructuras"

        # -- DataFrames --
        self.apuntes = self._cargar_excel(
            presupuesto / "apuntes presupuesto de gasto.xlsx"
        )
        self.centros = self._cargar_excel(presupuesto / "centros.xlsx")
        self.subcentros = self._cargar_excel(presupuesto / "subcentros.xlsx")
        self.proyectos = self._cargar_excel(presupuesto / "proyectos.xlsx")
        self.subproyectos = self._cargar_excel(presupuesto / "subproyectos.xlsx")
        self.tipos_de_proyecto = self._cargar_excel(
            presupuesto / "tipos de proyecto.xlsx"
        )
        self.líneas_de_financiación = self._cargar_excel(
            presupuesto / "líneas de financiación.xlsx"
        )
        self.tipos_de_línea = self._cargar_excel(
            presupuesto / "tipos de línea.xlsx"
        )
        self.programas = self._cargar_excel(
            presupuesto / "programas presupuestarios.xlsx"
        )
        self.aplicaciones = self._cargar_excel(
            presupuesto / "aplicaciones de gasto.xlsx"
        )
        self.capítulos = self._cargar_excel(
            presupuesto / "capítulos de gasto.xlsx"
        )
        self.artículos = self._cargar_excel(
            presupuesto / "artículos de gasto.xlsx"
        )
        self.conceptos = self._cargar_excel(
            presupuesto / "conceptos de gasto.xlsx"
        )
        # -- Árboles --
        self.elementos_de_coste = self._cargar_árbol(
            estructuras / "elementos de coste.tree"
        )
        self.actividades = self._cargar_árbol(
            estructuras / "actividades.tree"
        )
        self.centros_de_coste = self._cargar_árbol(
            estructuras / "centros de coste.tree"
        )

    # -- Métodos privados de carga --

    @staticmethod
    def _cargar_excel(ruta: Path) -> pl.DataFrame | None:
        try:
            return read_excel(ruta)
        except FileNotFoundError:
            log.warning("Fichero no encontrado: %s", ruta)
            return None

    @staticmethod
    def _cargar_árbol(ruta: Path) -> Árbol | None:
        try:
            return Árbol.from_file(ruta)
        except FileNotFoundError:
            log.warning("Fichero no encontrado: %s", ruta)
            return None
