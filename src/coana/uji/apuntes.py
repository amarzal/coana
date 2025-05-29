from dataclasses import dataclass, field
from datetime import date
from typing import Iterator

import polars as pl
from loguru import logger

from coana.configuración import Configuración
from coana.elemento_de_coste import ElementoDeCoste
from coana.elementos_de_coste import ElementosDeCoste
from coana.misc.euro import E


@dataclass(slots=True)
class Apunte:
    id: str
    importe: E
    proyecto: str
    subproyecto: str
    aplicación: str
    centro: str
    subcentro: str
    línea: str
    tipo_línea: str
    fecha: date
    elemento_de_coste: str | None = field(default=None)
    centro_de_coste: str | None = field(default=None)
    actividad: str | None = field(default=None)

@dataclass(slots=True)
class Apuntes:
    apuntes: dict[str, Apunte]

    @classmethod
    def carga(cls, configuración: Configuración) -> "Apuntes":
        fichero_apuntes = configuración.fichero("apuntes")
        logger.trace(f"Cargando apuntes de {fichero_apuntes.path}")
        df = fichero_apuntes.carga_dataframe()
        logger.trace(f"Apuntes en fichero: {df.shape[0]} registros")
        apuntes = {}
        for row in df.iter_rows(named=True):
            ident = row['id']
            if ident in apuntes:
                raise ValueError(f"Apunte duplicado: {ident}")
            apuntes[ident] = Apunte(
                id=ident,
                importe=E(row["importe"]),
                proyecto=row["proyecto"],
                subproyecto=row["subproyecto"],
                aplicación=row["aplicación"],
                centro=row["centro"],
                subcentro=row["subcentro"],
                línea=row["línea"],
                tipo_línea=row["tipo_línea"],
                fecha=row["fecha"],
                elemento_de_coste=row.get("elemento_de_coste", None),
                centro_de_coste=row.get("centro_de_coste", None),
                actividad=row.get("actividad", None),
            )
        logger.trace(f"Apuntes cargados: {len(apuntes)} registros")
        return cls(apuntes=apuntes)

    def dataframe(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "id": [apunte.id for apunte in self.apuntes.values()],
                "importe": [float(apunte.importe) for apunte in self.apuntes.values()],
                "proyecto": [apunte.proyecto for apunte in self.apuntes.values()],
                "subproyecto": [apunte.subproyecto for apunte in self.apuntes.values()],
                "aplicación": [apunte.aplicación for apunte in self.apuntes.values()],
                "centro": [apunte.centro for apunte in self.apuntes.values()],
                "subcentro": [apunte.subcentro for apunte in self.apuntes.values()],
                "línea": [apunte.línea for apunte in self.apuntes.values()],
                "tipo_línea": [apunte.tipo_línea for apunte in self.apuntes.values()],
                "elemento_de_coste": [apunte.elemento_de_coste for apunte in self.apuntes.values()],
                "centro_de_coste": [apunte.centro_de_coste for apunte in self.apuntes.values()],
                "actividad": [apunte.actividad for apunte in self.apuntes.values()],
            }
        )

    def __iter__(self) -> Iterator[Apunte]:
        return iter(self.apuntes.values())

    def __getitem__(self, id: str) -> Apunte:
        return self.apuntes[id]
