"""Contexto de nóminas: carga de ficheros de referencia.

Reúne en una sola clase los DataFrames de los ficheros Excel del
directorio ``data/entrada/nóminas``.
"""

import logging
from pathlib import Path

import polars as pl

from coana.util.excel_cache import read_excel

log = logging.getLogger(__name__)


class ContextoNóminas:
    """Carga y almacena los datos de referencia de nóminas."""

    def __init__(self, ruta_base: Path = Path("data")) -> None:
        d = Path(ruta_base) / "entrada" / "nóminas"

        self.provisiones = self._cargar_excel(d / "provisiones.xlsx")
        self.categorías = self._cargar_excel(
            d / "categorías recursos humanos.xlsx"
        )
        self.expedientes = self._cargar_excel(
            d / "expedientes recursos humanos.xlsx"
        )
        self.perceptores = self._cargar_excel(d / "perceptores.xlsx")
        self.conceptos_retributivos = self._cargar_excel(
            d / "conceptos retributivos.xlsx"
        )
        self.nóminas = self._cargar_excel(
            d / "nóminas y seguridad social.xlsx"
        )

    @staticmethod
    def _cargar_excel(ruta: Path) -> pl.DataFrame | None:
        try:
            return read_excel(ruta)
        except FileNotFoundError:
            log.warning("Fichero no encontrado: %s", ruta)
            return None
