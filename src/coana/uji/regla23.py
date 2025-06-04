from dataclasses import dataclass, field

from coana.misc import euro
from coana.misc.euro import E


@dataclass
class HorasRegistradasEnPOD:
    asignatura: str | None = field()
    titulación: str # Debería ser un código de actividad
    horas_oficiales: float

    @property
    def horas_dedicadas(self) -> float:
        return self.horas_oficiales * 2.5

@dataclass
class TrabajoFinalDirigido:
    titulación: str = field()
    número: float = field()

    @property
    def horas_dedicadas(self) -> float:
        return self.número * 2.5

@dataclass
class CoordinaciónDeMateria:
    asignatura: str = field()
    titulación: str = field()
    horas_registradas: float = field()

@dataclass
class CoordinaciónDeTFG:
    titulación: str = field()
    horas_registradas: float = field()

@dataclass
class DedicaciónPorCargo:
    cargo: str

    @property
    def dedicación(self) -> float: # 1 es 100%
        match self.cargo:
            case 'Rector':
                return 1
            case 'Vicerrector':
                return .5
            case 'Vicerrector adjunto':
                return .25
            case _:
                raise ValueError(f"Cargo desconocido {self.cargo}")


@dataclass
class CostesNoAsignadosPorPDI:
    per_id: str = field()
    importe_no_asignado: E = field(default_factory=lambda: euro.zero)

    # --- DOCENCIA OFICIAL
    horas_registradas_en_Grado: list[HorasRegistradasEnPOD] = field(default_factory=list)
    horas_registradas_en_Máster: list[HorasRegistradasEnPOD] = field(default_factory=list)
    tfg_dirigidos: list[TrabajoFinalDirigido] = field(default_factory=list)
    tfm_dirigidos: list[TrabajoFinalDirigido] = field(default_factory=list)
    coordinación_de_materia: dict[str, CoordinaciónDeMateria] = field(default_factory=dict)
    coordinación_de_prácticas: dict[str, float] = field(default_factory=dict)  # Titulación - 1: coordinador único/0.5 cocordinador
    coordinación_movilidad: dict[str, float] = field(default_factory=dict)
    # !!! Ver en el POD si estas son meras asignaturas
    prácticas_curriculares_dirigidas: dict[str, float] = field(default_factory=dict )  # Titulación - número de horas POD
    prácticas_extracurriculares_dirigidas: dict[str, float] = field(default_factory=dict)  # Titulación - número de horas POD
    coordinación_de_TFG: dict[str, CoordinaciónDeTFG] = field(default_factory=dict)

    # --- DOCENCIA NO OFICIAL
    # !!! Esto no estará en POD. ¿Tenemos la información?
    horas_en_Grado_propio: list[HorasRegistradasEnPOD] = field(default_factory=list)
    horas_en_Máster_propio: list[HorasRegistradasEnPOD] = field(default_factory=list)
    horas_en_Diplomas_especialización: list[HorasRegistradasEnPOD] = field(default_factory=list)
    horas_en_Diplomas_de_experto: list[HorasRegistradasEnPOD] = field(default_factory=list)
    horas_en_Microcredenciales: list[HorasRegistradasEnPOD] = field(default_factory=list)
    horas_en_universidad_para_mayores: list[HorasRegistradasEnPOD] = field(default_factory=list)
    dirección_en_Grado_propio: list[HorasRegistradasEnPOD] = field(default_factory=list)
    dirección_en_Máster_propio: list[HorasRegistradasEnPOD] = field(default_factory=list)
    dirección_en_Diplomas_especialización: list[HorasRegistradasEnPOD] = field(default_factory=list)
    dirección_en_Diplomas_de_experto: list[HorasRegistradasEnPOD] = field(default_factory=list)
    dirección_en_Microcredenciales: list[HorasRegistradasEnPOD] = field(default_factory=list)

    # !!! Gestión
