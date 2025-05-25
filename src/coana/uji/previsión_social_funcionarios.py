from dataclasses import dataclass

import polars as pl

from coana.ficheros import Ficheros


@dataclass
class PrevisionesSocialesFuncionarios:
    df: pl.DataFrame

    @classmethod
    def carga(cls) -> "PrevisionesSocialesFuncionarios":
        ficheros = Ficheros()
        df = carga_excel_o_parquet(ficheros.previsión_social_funcionarios)

        print(df)
