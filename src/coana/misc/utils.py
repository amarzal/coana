from decimal import Decimal
from typing import Any

import polars as pl

from coana.misc.euro import E


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def porcentaje(número: float | Decimal | E, sobre: float | Decimal | E, decimales: int = 2) -> str:
    return f'{float(número) / float(sobre) * 100:_.{decimales}f} %'.replace('.', ',').replace('_', '.')

def num(número: float | int, decimales: int = 0) -> str:
    match número:
        case float():
            return f'{número:_.{decimales}f}'.replace('.', ',').replace('_', '.')
        case int():
            return f'{número:_d}'.replace('.', ',').replace('_', '.')
        case _:
            raise ValueError(f"No se puede convertir {número} de tipo {type(número)} a un número")

def porc(número: float, sobre: float, decimales: int = 2) -> str:
    return f'{float(número) / float(sobre) * 100:_.{decimales}f} %'.replace('.', ',').replace('_', '.')


def añade_fila_a_dataframe(df: pl.DataFrame, fila: dict[str, Any]) -> pl.DataFrame:
    aux = {}
    for columna in df.columns:
        aux[columna] = fila.get(columna, None)
        aux[columna].append(fila[columna] if columna in fila else None)
    return pl.DataFrame(aux)
