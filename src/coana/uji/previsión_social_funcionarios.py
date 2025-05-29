from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import polars as pl
from loguru import logger

from coana.misc.euro import E
from coana.misc.traza import Traza
from coana.misc.utils import num, porc
from coana.uji.nóminas import Nóminas


@dataclass
class PrevisiónSocialFuncionarios:
    df: pl.DataFrame

    @classmethod
    def calcula(cls, nóminas: Nóminas) -> "PrevisiónSocialFuncionarios":
        logger.trace("Procesando nóminas para cálculo de la previsión social de funcionarios")
        traza("= Previsión social de funcionarios")

        porcentaje_seguridad_social = 0.236 + 0.055 + 0.006 + 0.0058
        importe_máximo_anual_ss = 4720.50 * 12 # 4909.50 * 12
        solo_pdi_funcs = nóminas.df.filter(pl.col("categoría_perceptor").is_in(["CU", "TU", "TEU", "CEU"]))

        traza(f"""
            #align(center,
                table(columns: 2, align: (left, right), stroke: none,
                    table.hline(),
                    [Porcentaje de seguridad social], [{porc(porcentaje_seguridad_social, 1)} %],
                    [Base máxima seguridad social], [{E(importe_máximo_anual_ss)} euros],
                    [Tope seguridad social], [{E(importe_máximo_anual_ss * porcentaje_seguridad_social)} euros],
                    table.hline(),
                )
            )
            """)


        personas = solo_pdi_funcs['per_id'].unique()
        ss_anual = {}

        # Para el Dataframe resultante
        filas = { key:[] for key in nóminas.df.columns }

        for persona in personas:
            df = nóminas.df.filter(pl.col("per_id") == persona)
            importe_anual_percibido = df['importe'].sum()
            apls = df['aplicación'].unique()
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
        traza(f"""
            #align(center,
                table(columns: 3, align: (left, right, right), stroke: none,
                    table.header(
                        table.hline(),
                        [*Figura*],
                        [*Personas*],
                        [*Importe*],
                        table.hline(),
                    ),
                    [*CU*], [{num(len(df.filter(pl.col('categoría_perceptor') == 'CU')['per_id'].unique()))}], [{E(df.filter(pl.col('categoría_perceptor') == 'CU')['importe'].sum())} euros],
                    [*TU*], [{num(len(df.filter(pl.col('categoría_perceptor') == 'TU')['per_id'].unique()))}], [{E(df.filter(pl.col('categoría_perceptor') == 'TU')['importe'].sum())} euros],
                    [*TEU*], [{num(len(df.filter(pl.col('categoría_perceptor') == 'TEU')['per_id'].unique()))}], [{E(df.filter(pl.col('categoría_perceptor') == 'TEU')['importe'].sum())} euros],
                    [*CEU*], [{num(len(df.filter(pl.col('categoría_perceptor') == 'CEU')['per_id'].unique()))}], [{E(df.filter(pl.col('categoría_perceptor') == 'CEU')['importe'].sum())} euros],
                    table.hline(),
                    [*Total*], [{num(len(df['per_id'].unique()))}], [{E(df['importe'].sum())} euros],
                    table.hline(),
                )
            )
            """)
        return cls(df)

    def guarda(self, fichero: Path) -> None:
        self.df.write_excel(fichero)
