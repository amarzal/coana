from pathlib import Path

import polars as pl


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def carga_excel_o_csv(fichero: Path) -> pl.DataFrame:
    if fichero.suffix == ".xlsx":
        return pl.read_excel(fichero, engine="openpyxl")
    elif fichero.suffix == ".csv":
        return pl.read_csv(fichero)
    elif fichero.suffix == ".parquet":
        return pl.read_parquet(fichero)
    else:
        raise ValueError(f"Formato de fichero no soportado: {fichero}")

def porcentaje(número: float, sobre: float, decimales: int = 2) -> str:
    return f'{número / sobre * 100:_.{decimales}f} %'.replace('.', ',').replace('_', '.')

def num(número: float | int, decimales: int = 0) -> str:
    match número:
        case float():
            return f'{número:_.{decimales}f}'.replace('.', ',').replace('_', '.')
        case int():
            return f'{número:_d}'.replace('.', ',').replace('_', '.')
        case _:
            raise ValueError(f"No se puede convertir {número} de tipo {type(número)} a un número")
