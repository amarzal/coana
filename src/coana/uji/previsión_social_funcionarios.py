from dataclasses import dataclass
from typing import ClassVar

import polars as pl

from coana.ficheros import Ficheros
from coana.misc.utils import carga_excel_o_csv


@dataclass
class PrevisionesSocialesFuncionarios:
    df: pl.DataFrame

    @classmethod
    def carga(cls) -> "PrevisionesSocialesFuncionarios":
        ficheros = Ficheros()
        df = carga_excel_o_csv(ficheros.previsión_social_funcionarios)

        print(df)
