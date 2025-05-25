from decimal import Decimal
from pathlib import Path

import polars as pl


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def porcentaje(número: float | Decimal, sobre: float | Decimal, decimales: int = 2) -> str:
    return f'{float(número) / float(sobre) * 100:_.{decimales}f} %'.replace('.', ',').replace('_', '.')

def num(número: float | int, decimales: int = 0) -> str:
    match número:
        case float():
            return f'{número:_.{decimales}f}'.replace('.', ',').replace('_', '.')
        case int():
            return f'{número:_d}'.replace('.', ',').replace('_', '.')
        case _:
            raise ValueError(f"No se puede convertir {número} de tipo {type(número)} a un número")
