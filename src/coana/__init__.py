import calendar
import datetime
import sys
from pathlib import Path
from pprint import pp
from typing import Annotated

import polars as pl
import typer
from dateutil.relativedelta import relativedelta
from loguru import logger
from typer import Typer

from coana.configuración import Configuración
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
    datos = Path("../coana_data/2024")
    ficheros = Ficheros(datos)
    logger.trace(f"EJECUCIÓN DE DESARROLLO con {datos}")

    inicio_año = datetime.date(2024, 1, 1)
    fin_año = datetime.date(2024, 12, 31)
    años_amortización = {}
    períodos_amortización = ficheros.fichero("períodos_amortización").carga_dataframe()
    fecha_frontera = {}
    for row in períodos_amortización.iter_rows(named=True):
        fecha_frontera[row["cuenta"]] = inicio_año - relativedelta(years=row["años"])
        años_amortización[row["cuenta"]] = row["años"]
    pp(fecha_frontera)
    inventario = ficheros.fichero("inventario").carga_dataframe()

    días_del_año_actual = 366 if calendar.isleap(inicio_año.year) else 365
    print(días_del_año_actual)

    rows = []
    for row in inventario.iter_rows(named=True):
        if row["recepción"] < fecha_frontera[row["cuenta"]]:
            continue
        if row['recepción'] > fin_año:
            continue
        if row["baja"] is not None:
            if row["baja"] < inicio_año:
                continue
            else:
                días_en_el_año = (row["baja"] - inicio_año).days
                print(min(días_en_el_año, días_del_año_actual))
        else:
            días_en_el_año = (fin_año - max(inicio_año, row['recepción'])).days
        if días_en_el_año == 0:
            continue
        fracción_del_año = días_en_el_año / días_del_año_actual
        vida_útil = años_amortización[row["cuenta"]]
        importe = row["importe"]
        importe_anual = float(importe) / vida_útil
        importe_anual_fracción = importe_anual * fracción_del_año
        row['importe'] = importe_anual_fracción
        rows.append(row)
    df = pl.DataFrame(
        {
            k: [row[k] for row in rows] for k in inventario.columns + ['importe']
        }
    )
    df = df.with_columns(
        pl.col('importe').round(2).cast(pl.Decimal(scale=2))
    )
    print(df)


@app.command()
def uji(ruta_datos: Annotated[Path, typer.Argument(help="Ruta a los datos")]) -> None:

    cfg = Configuración(ruta_datos)
    print(cfg)

    uji = UJI(cfg)

    # ficheros = Ficheros(ruta_datos)
    # ficheros.traza()
    # logger.trace(f"Cargado el manifesto.yaml de {ruta_datos}")

    # logger.trace(f"Cargando estructuras {ruta_datos}...")
    # estructuras = Estructuras()
    # estructuras.traza()
    # logger.trace(f"Cargadas las estructuras de {ruta_datos}")

    # logger.trace(f"Cargando etiquetadores {ruta_datos}...")
    # etiquetadores = EtiquetadoresUJI()

    # # Carga y etiquetado de apuntes presupuestarios
    # apuntes = Apuntes.carga()

    # logger.trace("Etiquetando apuntes con elementos de coste")
    # apuntes.etiqueta("elemento_de_coste", etiquetadores.elemento_de_coste_para_apuntes, estructuras.elementos_de_coste)

    # logger.trace("Etiquetando apuntes con centros de coste")
    # apuntes.etiqueta("centro_de_coste", etiquetadores.centro_de_coste_para_apuntes, estructuras.centros_de_coste_por_finalidad)
    # # TODO: Etiquetar con actividades
    # apuntes.guarda(ficheros.fichero("traza_apuntes_etiquetados").path)

    # # Carga y etiquetado de nóminas
    # nóminas = Nóminas.carga()

    # nóminas.etiqueta("elemento_de_coste", etiquetadores.elemento_de_coste_para_nóminas, estructuras.elementos_de_coste)
    # nóminas.etiqueta("centro_de_coste", etiquetadores.centro_de_coste_para_nóminas, estructuras.centros_de_coste_por_finalidad)

    # nóminas.guarda(ficheros.fichero("traza_nóminas_etiquetadas").path)

    # # Cálculo de previsiones sociales
    # psf = PrevisiónSocialFuncionarios.calcula(nóminas)
    # psf.guarda(ficheros.fichero("traza_previsiones_sociales_funcionarios_etiquetadas").path)

    # traza.guarda()

    # logger.trace("Fin")


@app.command()
def informe() -> None:
    raise NotImplementedError("No implementado")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
