from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import polars as pl
from loguru import logger

from coana.uji.nóminas import Nóminas


@dataclass
class PrevisiónSocialFuncionarios:
    df: pl.DataFrame

    @classmethod
    def calcula(cls, nóminas: Nóminas) -> "PrevisiónSocialFuncionarios":
        logger.trace("Procesando nóminas para cálculo de la previsión social de funcionarios")

        porcentaje_seguridad_social = 0.236 + 0.055 + 0.006 + 0.0058
        importe_máximo_anual_ss = 4909.50 * 12
        solo_pdi_funcs = nóminas.df.filter(pl.col("categoría_perceptor").is_in(["CU", "TU", "TEU", "CEU"]))

        personas = solo_pdi_funcs['per_id'].unique()
        ss_anual = {}

        # Para el Dataframe resultante
        filas = { key:[] for key in nóminas.df.columns }

        for persona in personas:
            df = nóminas.df.filter(pl.col("per_id") == persona)
            importe_anual_percibido = df['importe'].sum()
            apls = df['aplicación_presupuestaria'].unique()
            hay_pagos_a_ss = any(apli.startswith("12") for apli in apls)
            columnas_copiadas = [col for col in nóminas.df.columns if col not in ['id', 'importe', 'concepto_retributivo']]
            if not hay_pagos_a_ss:
                ss_anual = min(importe_máximo_anual_ss, float(importe_anual_percibido)) * porcentaje_seguridad_social
                for row in df.iter_rows(named=True):
                    fracción_ss = ss_anual * (float(row["importe"]) / float(importe_anual_percibido))
                    filas["id"].append(row["id"] + "/PSF")
                    filas["importe"].append(Decimal(f"{round(fracción_ss, 2):.2f}"))
                    filas["concepto_retributivo"].append("PSF")
                    for key in columnas_copiadas:
                        filas[key].append(row[key])

        df = pl.DataFrame(filas)
        df = df.with_columns(pl.col("importe").round(2).cast(pl.Decimal(scale=2)))
        return cls(df)

    def guarda(self, fichero: Path) -> None:
        self.df.write_excel(fichero)
