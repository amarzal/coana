from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Iterator

from loguru import logger

from coana.configuración import Configuración
from coana.misc.euro import E

# ID	ID_COSTE	PER_ID	EXPEDIENTE	ACTIVIDAD	EXP_GRE	EXP_GRE_EJERCICIO	FECHA	CUANTIA	CODIGO_CONCEPTO_RETRIBUTIVO	NOMBRE_CONCEPTO_RETRIBUTIVO	TIPO	PROYECTO	NOMBRE_PROYECTO	SUBPROYECTO	NOMBRE_SUBPROYECTO	ID_APLICACION	NOMBRE_APLICACION	CAPITULO	PROGRAMA	ID_LINEA	NOMBRE_LINEA	ID_TIPO_LINEA	NOMBRE_TIPO_LINEA	CENTRO	DESCRIPCION_CENTRO	ID_SUBCENTRO	NOMBRE_SUBCENTRO	CONTRATO	TIPO_CONTRATO	TIPO_PERCEPTOR	ID_CATEGORIA_PER	COAN_CPER_ID	COAN_SS_ID	NOMBRE_CATEGORIA_PER	N_PLAZA	PLAZA_ID	ID_CENTRO_ESTRUCTURAL	NOMBRE_CENTRO_ESTRUCTURAL	ID_UBICACION	NOMBRE_UBICACION	COAN_CENTRO_ID	ID_AREA	NOMBRE_AREA	HORAS_PLAZA	HORAS_TRABAJO	CATEGORIA_PLAZA	DENOMINACION_PLAZA	GRUPO	NIVEL	ESPECIFICO


@dataclass(slots=True)
class Nómina:
    id: str
    per_id: str
    fecha: date
    importe: E
    concepto_retributivo: str
    proyecto: str
    subproyecto: str
    aplicación: str
    programa_presupuestario: str
    línea: str
    tipo_línea: str
    centro: str
    subcentro: str
    categoría_perceptor: str
    ubicación: str
    horas_semanales: int
    elemento_de_coste: str | None = field(default=None)
    centro_de_coste: str | None = field(default=None)
    actividad: str | None = field(default=None)


@dataclass
class Nóminas:
    nóminas: dict[str, Nómina]

    @classmethod
    def carga(cls, cfg: Configuración | dict[str, Nómina]) -> "Nóminas":
        if isinstance(cfg, dict):
            return cls(nóminas=cfg)
        fichero_nóminas = cfg.fichero("nóminas")
        logger.trace(f" Cargando nóminas de {fichero_nóminas.ruta}")
        df = fichero_nóminas.carga_dataframe()
        nóminas = {}
        for row in df.iter_rows(named=True):
            if row["importe"] == 0:
                continue
            nóminas[row["id"]] = Nómina(
                id=row["id"],
                per_id=row["per_id"],
                fecha=row["fecha"],
                importe=E(row["importe"]),
                concepto_retributivo=row["concepto_retributivo"],
                proyecto=row["proyecto"],
                subproyecto=row["subproyecto"],
                aplicación=row["aplicación"],
                programa_presupuestario=row["programa_presupuestario"],
                línea=row["línea"],
                tipo_línea=row["tipo_línea"],
                centro=row["centro"],
                subcentro=row["subcentro"],
                categoría_perceptor=row["categoría_perceptor"],
                ubicación=row["ubicación"],
                horas_semanales=row["horas_semanales"],
                elemento_de_coste=row.get("elemento_de_coste", None),
                centro_de_coste=row.get("centro_de_coste", None),
                actividad=row.get("actividad", None),
            )
        logger.trace(f"Nóminas cargadas: {len(nóminas)} registros")
        return cls(nóminas=nóminas)

    def __iter__(self) -> Iterator[Nómina]:
        return iter(self.nóminas.values())
