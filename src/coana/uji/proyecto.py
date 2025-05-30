from dataclasses import dataclass, field


@dataclass
class Subproyecto:
    código_proyecto: str = field()
    código: str = field()
    nombre: str = field()


@dataclass
class Proyecto:
    código: str = field()
    nombre: str = field()
    tipo: str = field()
    subtipo: str = field()
    subproyectos: dict[str, Subproyecto] = field(default_factory=dict)

@dataclass
class TipoProyecto:
    código: str = field()
    nombre: str = field()
