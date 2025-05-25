from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from typing import Iterator

from loguru import logger

type Ruta = tuple[int, ...]


@dataclass
class Árbol:
    ruta2identificador: dict[Ruta, str]
    ruta2descripción: dict[Ruta, str]
    id2ruta: dict[str, Ruta]
    hijos: dict[str, list[str]] = field(init=False)
    padre: dict[str, str | None] = field(init=False)
    fichero: Path | None = field(default=None)

    def __post_init__(self) -> None:
        self.hijos = {ident: [] for ident in self.id2ruta}
        self.padre = {}
        for ruta, ident in self.ruta2identificador.items():
            ruta_padre = ruta[:-1]
            if ruta_padre:
                padre = self.ruta2identificador[ruta_padre]
                self.hijos[padre].append(ident)
                self.padre[ident] = padre
            else:
                self.padre[ident] = None

    def como_texto_sangrado(self, indent_spaces: int = 4) -> str:
        t = StringIO()
        for ruta in sorted(self.ruta2identificador):
            indent = (len(ruta) - 1) * indent_spaces
            t.write(f"{' ' * indent}{self.ruta2identificador[ruta]} | {self.ruta2descripción[ruta]}\n")
        return t.getvalue()

    @classmethod
    def desde_texto_sangrado(cls, text: str, indent_spaces: int = 1) -> "Árbol":
        identificador: dict[Ruta, str] = {}
        descripción: dict[Ruta, str] = {}
        ruta: dict[str, Ruta] = {}

        ruta_actual = [0]
        for line in text.splitlines():
            indent = (len(line) - len(line.lstrip(" "))) // indent_spaces
            if indent == len(ruta_actual) - 1:
                ruta_actual[-1] += 1
            elif indent > len(ruta_actual) - 1:
                ruta_actual.append(1)
            else:
                ruta_actual = ruta_actual[: indent + 1]
                ruta_actual[-1] += 1
            if "|" not in line:
                raise ValueError(f"No se encuentra '|' en {line}")
            descr, ident = map(str.strip, line.split("|"))
            if ident == "":
                raise ValueError(f"Identificador vacío en la línea {line}")
            if ident != "SUPRIMIR":
                identificador[tuple(ruta_actual)] = ident
                descripción[tuple(ruta_actual)] = descr
                ruta[ident] = tuple(ruta_actual)
            elif ident in ruta:
                raise ValueError(f"Identificador duplicado en la línea {line}")
        return cls(identificador, descripción, ruta)

    def como_tripletas(self) -> list[tuple[Ruta, str, str]]:
        return sorted([
            (ruta, self.ruta2identificador[ruta], self.ruta2descripción[ruta])
            for ruta in self.ruta2identificador
        ])

    def __contains__(self, ident: str | Ruta | None) -> bool:
        return ident in self.ruta2identificador or ident in self.id2ruta

    def itera_etiquetas(self) -> Iterator[str]:
        yield from sorted(set(self.ruta2identificador.values()))

    def itera_etiquetas_en_postorden(self) -> Iterator[str]:
        def dfs(ident: str) -> Iterator[str]:
            for hijo in self.hijos[ident]:
                yield from dfs(hijo)
            yield ident
        for ident in self.itera_etiquetas():
            if self.padre[ident] is None:
                yield from dfs(ident)
