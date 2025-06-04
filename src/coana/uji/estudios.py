from dataclasses import dataclass, field
from re import S
from typing import ClassVar, Iterable

from coana.configuración import Configuración


@dataclass
class ÁmbitosDeConocimiento:
    ámbitos: dict[str, str] = field(default_factory=dict)
    singleton: ClassVar["ÁmbitosDeConocimiento | None"] = None

    @classmethod
    def carga(cls, cfg: Configuración) -> "ÁmbitosDeConocimiento":
        if cls.singleton is not None:
            return cls.singleton

        df = cfg.fichero("ámbitos-de-conocimiento").carga_dataframe()
        ámbitos = {}
        for row in df.iter_rows(named=True):
            ámbitos[row['código']] = row['nombre']
        ámbitos = dict(sorted(ámbitos.items(), key=lambda x: x[1]))
        if 'Interdisciplinar' in ámbitos:
            valor = ámbitos['Interdisciplinar']
            del ámbitos['Interdisciplinar']
            ámbitos['Interdisciplinar'] = valor # Ha de ser el último
        return cls(ámbitos=ámbitos)

    def __iter__(self) -> Iterable[tuple[str, str]]:
        return iter(self.ámbitos.items())


@dataclass
class EstudioOficial:
    código: int
    nombre: str
    tipo: str # Grado o Máster
    ámbito_de_conocimiento: str
    códigos_planes: list[int] = field(default_factory=list)


@dataclass
class PlanDeEstudioOficial:
    código: int
    nombre: int
    código_estudio: int


@dataclass
class EstudiosOficiales:
    ámbitos: ÁmbitosDeConocimiento = field()
    estudios: dict[int, EstudioOficial] = field()
    planes: dict[int, PlanDeEstudioOficial] = field()
    singleton: ClassVar["EstudiosOficiales | None"] = None

    @classmethod
    def carga(cls, cfg: Configuración) -> "EstudiosOficiales":
        if cls.singleton is not None:
            return cls.singleton

        ámbitos = ÁmbitosDeConocimiento.carga(cfg)
        estudios = {}
        planes = {}
        df = cfg.fichero("estudios-oficiales").carga_dataframe()
        for row in df.iter_rows(named=True):
            estudios[row["código"]] = EstudioOficial(código=row["código"], nombre=row["nombre"], tipo=row["tipo"], ámbito_de_conocimiento=row["ámbito_de_conocimiento"])
        return cls(ámbitos=ámbitos, estudios=estudios, planes=planes)

    def genera_actividades(self, cfg: Configuración) -> None:
        directorio = cfg.directorio("dir-actividades").ruta
        fichero = "AC_Estudios_Oficiales".lower() + ".tree"
        with open(directorio / fichero, "w") as f:
            for (código_ámbito, nombre_ámbito) in self.ámbitos:
                en_ámbito = [estudio for estudio in self.estudios.values() if estudio.ámbito_de_conocimiento == código_ámbito]
                if en_ámbito:
                    f.write(f"{nombre_ámbito} | AC_Ambito_{código_ámbito}\n")
                    grados = [estudio for estudio in en_ámbito if estudio.tipo == "Grado"]
                    másteres = [estudio for estudio in en_ámbito if estudio.tipo == "Máster"]
                    if grados:
                        f.write(f"    Grados | AC_Ambito_{código_ámbito}_Grados\n")
                        for grado in grados:
                            f.write(f"        {grado.nombre} | AC_Grado_{grado.código}\n")
                    if másteres:
                        f.write(f"    Másteres | AC_Ambito_{código_ámbito}_Masteres\n")
                        for máster in másteres:
                            f.write(f"        {máster.nombre} | AC_Master_{máster.código}\n")
