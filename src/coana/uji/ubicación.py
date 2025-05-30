from dataclasses import dataclass, field


@dataclass
class Ubicación:
    id: int = field()
    código: str = field()
    nombre: str = field()
