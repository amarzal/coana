import sys
from pathlib import Path
from typing import Annotated

import typer
from loguru import logger
from typer import Typer

from coana.estructuras import Estructuras
from coana.ficheros import Ficheros
from coana.misc.traza import Traza
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
