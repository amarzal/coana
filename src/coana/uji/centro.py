from dataclasses import dataclass

import polars as pl


@dataclass
class Subcentro:
    centro: str # El padre
    código: str
    nombre: str


@dataclass
class Centro:
    id: int
    código: str
    nombre: str
    subcentros: dict[str, Subcentro]

class Centros(dict):
    def __init__(self, df: pl.DataFrame) -> None:
        for row in df.iter_rows(named=True):
            self[row["id"]] = Centro(
                id=row["id"],
                código=row["código"],
                nombre=row["nombre"],
                subcentros={}
            )
