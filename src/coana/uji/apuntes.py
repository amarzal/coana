from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
from loguru import logger

from coana.elemento_de_coste import ElementoDeCoste
from coana.elementos_de_coste import ElementosDeCoste
from coana.etiquetador import Etiquetador
from coana.ficheros import Ficheros
from coana.misc.euro import E
from coana.árbol import Árbol


@dataclass
class Apuntes:
    df: pl.DataFrame = field()

    @classmethod
    def carga(cls) -> "Apuntes":
        fichero_apuntes = Ficheros().fichero("apuntes")
        logger.trace(f"Cargando apuntes de {fichero_apuntes.path}")
        df = fichero_apuntes.carga_dataframe()
        logger.trace(f"Apuntes cargados: {df.shape[0]} filas")
        return cls(df)

    def guarda(self, fichero: Path) -> None:
        self.df.write_excel(fichero)

    def etiqueta(self, etiqueta: str, etiquetador: Etiquetador, árbol: Árbol) -> None:
        logger.trace(f"Etiquetando {etiqueta} en apuntes")
        self.df = etiquetador("apuntes", etiqueta, 'id', self.df, árbol)
        logger.trace(f"Etiquetada {etiqueta} en apuntes")

    def a_elementos_de_coste(self) -> ElementosDeCoste:
        elementos_de_coste = []
        for row in self.df.iter_rows(named=True):
            ec = ElementoDeCoste(
                importe=E(row['importe']),
                etiqueta_elemento_de_coste=row['elemento_de_coste'],
                etiqueta_centro_de_coste=row['centro_de_coste'],
                etiqueta_actividad=None,
                traza=f"Apunte {row['id']}",
            )
            elementos_de_coste.append(ec)
        return ElementosDeCoste(elementos_de_coste)
