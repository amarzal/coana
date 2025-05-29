from dataclasses import dataclass


@dataclass
class Subproyecto:
    código: str
    nombre: str


@dataclass
class Proyecto:
    id: int
    código: str
    nombre: str
    tipo: str
    subproyectos: dict[str, Subproyecto]
