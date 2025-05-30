from dataclasses import dataclass, field


@dataclass
class LíneaDeFinanciación:
    código: str = field()
    nombre: str = field()
    tipo: str = field()

    def es_genérica(self) -> bool:
        return self.tipo == "00"


@dataclass
class TipoLínea:
    código: str = field()
    nombre: str = field()
