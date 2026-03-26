"""Contexto de docencia: carga de ficheros de referencia.

Reúne en una sola clase los DataFrames de los ficheros Excel del
directorio ``data/entrada/docencia``.
"""

import logging
from pathlib import Path

import polars as pl

from coana.util.excel_cache import read_excel

log = logging.getLogger(__name__)


class ContextoDocencia:
    """Carga y almacena los datos de referencia de docencia."""

    def __init__(self, ruta_base: Path = Path("data")) -> None:
        d = Path(ruta_base) / "entrada" / "docencia"

        self.asignaturas_grados = self._cargar_excel(
            d / "asignaturas grados.xlsx"
        )
        self.asignaturas_másteres = self._cargar_excel(
            d / "asignaturas másteres.xlsx"
        )
        self.estudios = self._cargar_excel(d / "estudios.xlsx")
        self.grados = self._cargar_excel(d / "grados.xlsx")
        self.másteres = self._cargar_excel(d / "másteres.xlsx")
        self.microcredenciales = self._cargar_excel(
            d / "microcredenciales.xlsx"
        )
        self.docencia = self._cargar_excel(d / "docencia.xlsx")

    @staticmethod
    def _cargar_excel(ruta: Path) -> pl.DataFrame | None:
        try:
            return read_excel(ruta)
        except FileNotFoundError:
            log.warning("Fichero no encontrado: %s", ruta)
            return None
