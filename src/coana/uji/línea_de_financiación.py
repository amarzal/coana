from dataclasses import dataclass


@dataclass
class LíneaDeFinanciación:
    código: str
    tipo: str

    def es_genérica(self) -> bool:
        return self.código in ["00000", "10000", "80001", "80002", "81000", "82000"]


@dataclass
class TipoLínea:
    código: str
    nombre: str
