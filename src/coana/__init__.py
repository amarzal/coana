import sys
from calendar import c
from os import name
from pathlib import Path
from re import S, sub
from tkinter import E
from typing import Annotated

import polars as pl
import typer
from loguru import logger
from typer import Typer

from coana.estructuras import Estructuras
from coana.ficheros import Ficheros
from coana.misc.traza import Traza
from coana.uji.apuntes import Apuntes
from coana.uji.etiquetadores import EtiquetadoresUJI
from coana.uji.nóminas import Nóminas
from coana.uji.previsión_social_funcionarios import PrevisiónSocialFuncionarios

app = Typer(pretty_exceptions_show_locals=False)


logger.remove()
logger.add(
    sys.stderr,
    level="TRACE",
    format="{elapsed.seconds}.{elapsed.microseconds}|<blue>{level}</blue>|<cyan>{name}</cyan>:<green>{line}</green>| <bold>{message}</bold>",
    colorize=True,
)

@app.command()
def dev() -> None:
    datos = Path("../coana_data/2024")
    ficheros = Ficheros(datos)
    logger.trace(f"EJECUCIÓN DE DESARROLLO con {datos}")
    previsión_social_funcionarios = PrevisiónSocialFuncionarios.carga()
    # print(previsión_social_funcionarios.df)


    # df = nóminas.df.filter(pl.col("CUANTIA") != 0)
    # destino = ficheros.nóminas.with_name(ficheros.nóminas.stem + "_sin_ceros" + ficheros.nóminas.suffix)
    # df.write_excel(destino)



@app.command()
def uji(ruta_datos: Annotated[Path, typer.Argument(help="Ruta a los datos")]) -> None:
    traza = Traza()
    logger.trace(f"Cargando datos de {ruta_datos}")
    ficheros = Ficheros(ruta_datos)
    traza(ficheros.para_traza())

    estructuras = Estructuras()
    estructuras.traza()
    etiquetadores = EtiquetadoresUJI()

    # Carga y etiquetado de apuntes presupuestarios
    apuntes = Apuntes.carga()

    logger.trace("Etiquetando apuntes con elementos de coste")
    apuntes.etiqueta("elemento_de_coste", etiquetadores.elemento_de_coste_para_apuntes)

    logger.trace("Etiquetando apuntes con centros de coste")
    apuntes.etiqueta("centro_de_coste", etiquetadores.centro_de_coste_para_apuntes)
    # TODO: Etiquetar con actividades
    apuntes.guarda(ficheros.fichero("traza_apuntes_etiquetados").path)

    # Carga y etiquetado de nóminas
    nóminas = Nóminas.carga()

    nóminas.etiqueta("elemento_de_coste", etiquetadores.elemento_de_coste_para_nóminas)
    nóminas.etiqueta("centro_de_coste", etiquetadores.centro_de_coste_para_nóminas)

    nóminas.guarda(ficheros.fichero("traza_nóminas_etiquetadas").path)

    # Cálculo de previsiones sociales
    psf = PrevisiónSocialFuncionarios.carga()
    psf.guarda(ficheros.fichero("traza_previsiones_sociales_funcionarios_etiquetadas").path)

    traza.guarda()

    logger.trace("Fin")

@app.command()
def informe() -> None:
    raise NotImplementedError("No implementado")

def main() -> None:
    app()


if __name__ == "__main__":
    main()
