from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

import polars as pl
from loguru import logger

from coana.elemento_de_coste import ElementoDeCoste
from coana.elementos_de_coste import ElementosDeCoste
from coana.etiquetador import Etiquetador
from coana.ficheros import Ficheros
from coana.misc.euro import E
from coana.misc.utils import carga_excel_o_csv


@dataclass
class Apuntes:
    df: pl.DataFrame = field()

    col_identificador: ClassVar[str] = "ASE_ASIENTO"
    col_importe: ClassVar[str] = "CUANTIA"
    col_aplicación: ClassVar[str] = "APL_ID"
    col_tipo_linea: ClassVar[str] = "TIPO_LINEA"
    col_centro: ClassVar[str] = "NOMBRE_CENTRO"
    col_subcentro: ClassVar[str] = "SUBCENTRO"

    elemento_de_coste: ClassVar[str] = "ELEMENTO_DE_COSTE"
    centro_de_coste: ClassVar[str] = "CENTRO_DE_COSTE"

    @classmethod
    def carga(cls) -> "Apuntes":
        ficheros = Ficheros()
        logger.trace(f"Cargando apuntes de {ficheros.apuntes}")
        df = carga_excel_o_csv(ficheros.apuntes)
        df = df.with_columns(
            pl.col(Apuntes.col_importe).cast(pl.Float64),
            pl.col(Apuntes.col_aplicación).cast(pl.Utf8),
            pl.col(Apuntes.col_tipo_linea).cast(pl.Utf8),
            pl.col(Apuntes.col_centro).cast(pl.Utf8),
            pl.col(Apuntes.col_subcentro).cast(pl.Utf8),
        )
        logger.trace(f"Apuntes cargados: {df.shape[0]} filas")
        return cls(df)

    def guarda(self, fichero: Path) -> None:
        self.df.write_excel(fichero)

    def etiqueta(self, columna: str, etiquetador: Etiquetador) -> None:
        logger.trace(f"Etiquetando {columna} en apuntes")
        self.df = etiquetador("apuntes", columna, Apuntes.col_identificador, self.df)
        logger.trace(f"Etiquetada {columna} en apuntes")

    def a_elementos_de_coste(self) -> ElementosDeCoste:
        elementos_de_coste = []
        for row in self.df.iter_rows(named=True):
            ec = ElementoDeCoste(
                importe=E(row[Apuntes.col_importe]),
                etiqueta_elemento_de_coste=row[Apuntes.elemento_de_coste],
                etiqueta_centro_de_coste=row[Apuntes.centro_de_coste],
                etiqueta_actividad=None,
                traza=f"Apunte {row[Apuntes.col_identificador]}",
            )
            elementos_de_coste.append(ec)
        return ElementosDeCoste(elementos_de_coste)
