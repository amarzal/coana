import sys
from pathlib import Path
from typing import Annotated

import polars as pl
import typer
from loguru import logger
from typer import Typer

from coana.estructuras import Estructuras
from coana.etiquetador import Etiquetador
from coana.ficheros import Ficheros
from coana.misc.traza import Traza
from coana.uji.apuntes import Apuntes
from coana.uji.nóminas import Nóminas
from coana.uji.traductor_entrada_intermedio import TraductorEntradaIntermedio

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
    logger.trace(f"EJECUCIÓN DE DESARROLLO con {datos}")
    ficheros = Ficheros(datos)
    nóminas = Nóminas.carga()
    apuntes = Apuntes.carga()
    etiquetador = Etiquetador.carga("etiquetador_elemento_de_coste_para_apuntes")
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

    traductor = TraductorEntradaIntermedio()
    traductor.traduce()

    traza.guarda()

    logger.trace("Fin")

@app.command()
def informe() -> None:
    raise NotImplementedError("No implementado")

def main() -> None:
    app()


if __name__ == "__main__":
    main()
