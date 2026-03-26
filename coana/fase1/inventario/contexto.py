"""Contexto de inventario: carga de ficheros de referencia.

Reúne en una sola clase los DataFrames de los ficheros Excel del
inventario de bienes, dejando todo listo para el procesamiento
de amortizaciones.
"""

import logging
from pathlib import Path

import polars as pl

from coana.util.excel_cache import read_excel

log = logging.getLogger(__name__)


class ContextoInventario:
    """Carga y almacena los datos de referencia del inventario."""

    def __init__(self, ruta_base: Path = Path("data")) -> None:
        inv_dir = Path(ruta_base) / "entrada" / "inventario"
        superf_dir = Path(ruta_base) / "entrada" / "superficies"
        consumos_dir = Path(ruta_base) / "entrada" / "consumos"

        # Ficheros propios de inventario
        self.inventario = self._cargar_excel(inv_dir / "inventario.xlsx")
        self.años_amortización = self._cargar_excel(
            inv_dir / "años amortización por cuenta.xlsx"
        )
        self.corrector_superficie = self._cargar_excel(
            superf_dir / "corrector superficie.xlsx"
        )

        # Ficheros compartidos de superficies
        self.complejos = self._cargar_excel(superf_dir / "complejos.xlsx")
        self.edificaciones = self._cargar_excel(
            superf_dir / "edificaciones.xlsx"
        )
        self.zonas = self._cargar_excel(superf_dir / "zonas.xlsx")
        self.ubicaciones = self._cargar_excel(superf_dir / "ubicaciones.xlsx")
        self.tipos_ubicaciones = self._cargar_excel(
            superf_dir / "tipos de ubicación.xlsx"
        )
        self.ubicaciones_a_servicios = self._cargar_excel(
            inv_dir / "ubicaciones a servicios.xlsx"
        )
        self.servicios = self._cargar_excel(inv_dir / "servicios.xlsx")
        self.distribución_costes = self._cargar_excel(
            consumos_dir / "distribución OTOP.xlsx"
        )

    @staticmethod
    def _cargar_excel(ruta: Path) -> pl.DataFrame | None:
        try:
            return read_excel(ruta)
        except FileNotFoundError:
            log.warning("Fichero no encontrado: %s", ruta)
            return None
