import sys
from pathlib import Path
from typing import Annotated

import polars as pl
import typer
from loguru import logger
from typer import Typer

from coana.configuración import Configuración
from coana.uji.estudios import EstudiosOficiales
from coana.uji.uji import UJI

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
    cfg = Configuración(Path("../coana_data/2024"))
    eo = EstudiosOficiales.carga(cfg)
    eo.genera_actividades(cfg)


@app.command()
def uji(ruta_datos: Annotated[Path, typer.Argument(help="Ruta a los datos")]) -> None:
    cfg = Configuración(ruta_datos)
    uji = UJI(cfg)



@app.command()
def informe() -> None:
    raise NotImplementedError("No implementado")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
