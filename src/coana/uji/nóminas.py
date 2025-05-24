from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

import polars as pl
from loguru import logger

from coana.etiquetador import Etiquetador
from coana.ficheros import Ficheros
from coana.misc.utils import carga_excel_o_csv

# ID	ID_COSTE	PER_ID	EXPEDIENTE	ACTIVIDAD	EXP_GRE	EXP_GRE_EJERCICIO	FECHA	CUANTIA	CODIGO_CONCEPTO_RETRIBUTIVO	NOMBRE_CONCEPTO_RETRIBUTIVO	TIPO	PROYECTO	NOMBRE_PROYECTO	SUBPROYECTO	NOMBRE_SUBPROYECTO	ID_APLICACION	NOMBRE_APLICACION	CAPITULO	PROGRAMA	ID_LINEA	NOMBRE_LINEA	ID_TIPO_LINEA	NOMBRE_TIPO_LINEA	CENTRO	DESCRIPCION_CENTRO	ID_SUBCENTRO	NOMBRE_SUBCENTRO	CONTRATO	TIPO_CONTRATO	TIPO_PERCEPTOR	ID_CATEGORIA_PER	COAN_CPER_ID	COAN_SS_ID	NOMBRE_CATEGORIA_PER	N_PLAZA	PLAZA_ID	ID_CENTRO_ESTRUCTURAL	NOMBRE_CENTRO_ESTRUCTURAL	ID_UBICACION	NOMBRE_UBICACION	COAN_CENTRO_ID	ID_AREA	NOMBRE_AREA	HORAS_PLAZA	HORAS_TRABAJO	CATEGORIA_PLAZA	DENOMINACION_PLAZA	GRUPO	NIVEL	ESPECIFICO

@dataclass
class Nóminas:
    df: pl.DataFrame

    col_identificador: ClassVar[str] = "ID"
    col_importe: ClassVar[str] = "CUANTIA"
    col_concepto_retributivo: ClassVar[str] = "CODIGO_CONCEPTO_RETRIBUTIVO"
    col_categoría_personal: ClassVar[str] = "ID_CATEGORIA_PER"

    elemento_de_coste: ClassVar[str] = "ELEMENTO_DE_COSTE"
    centro_de_coste: ClassVar[str] = "CENTRO_DE_COSTE"

    @classmethod
    def carga(cls) -> "Nóminas":
        ficheros = Ficheros()
        logger.trace(f"Cargando nóminas de {ficheros.nóminas}")
        df = carga_excel_o_csv(ficheros.nóminas)
        df = df.with_columns(
            pl.col(Nóminas.col_identificador).cast(pl.Utf8),
            #pl.col(Nóminas.col_importe).replace("", "0").cast(pl.Float64),
            pl.col(Nóminas.col_categoría_personal).cast(pl.Utf8),
        )
        logger.trace("Suprimiendo línea con cuantía == 0")
        df = df.filter(pl.col(Nóminas.col_importe) != 0)
        logger.trace(f"Nóminas cargadas: {df.shape[0]} filas")
        return cls(df)

    def guarda(self, fichero: Path) -> None:
        self.df.write_excel(fichero)

    def etiqueta(self, columna: str, etiquetador: Etiquetador) -> None:
        logger.trace(f"Etiquetando {columna} en nóminas")
        self.df = etiquetador("nóminas", columna, Nóminas.col_identificador, self.df)
        logger.trace(f"Etiquetada {columna} en nóminas")

    def análisis_de_previsión_social(self) -> None:
        logger.trace("Análisis de previsiones sociales")
