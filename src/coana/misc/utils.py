import re
from decimal import Decimal
from typing import Any, Callable, Iterable

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


def human_sorted[T](it: Iterable[T], key: Callable[[T], str] = lambda x: str(x)) -> list[T]:
    """
    Sort a list in the way that humans expect.
    """

    def alphanum_key(s: str) -> list[int | str]:
        def tryint(s):
            try:
                return int(s)
            except ValueError:
                return s
        return [tryint(c) for c in re.split("([0-9]+)", s)]

    result = list(it)
    result.sort(key=lambda x: alphanum_key(key(x)))
    return result
