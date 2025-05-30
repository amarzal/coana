from dataclasses import dataclass, field

import polars as pl


@dataclass
class Subcentro:
    centro: str = field() # El padre
    código: str = field()
    nombre: str = field()


@dataclass
class Centro:
    código: str = field()
    nombre: str = field()
    subcentros: dict[str, Subcentro] = field(default_factory=dict)

class Centros(dict):
    def __init__(self, df: pl.DataFrame) -> None:
        for row in df.iter_rows(named=True):
            self[row["id"]] = Centro(
                código=row["código"],
                nombre=row["nombre"],
                subcentros={}
            )
