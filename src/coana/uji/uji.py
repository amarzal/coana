from dataclasses import dataclass, field, replace
from typing import Any

from loguru import logger

from coana.configuración import Configuración
from coana.estructuras import Estructuras
from coana.misc import euro
from coana.misc.typst import normaliza_texto
from coana.misc.utils import human_sorted, num
from coana.uji.amortizaciones import CostesPorAmortizaciones
from coana.uji.apuntes import Apuntes
from coana.uji.centro import Centro, Subcentro
from coana.uji.etiquetador import ReglasEtiquetador
from coana.uji.línea_de_financiación import LíneaDeFinanciación, TipoLínea
from coana.uji.nóminas import Nómina, Nóminas
from coana.uji.proyecto import Proyecto, Subproyecto, TipoProyecto
from coana.uji.ubicación import Ubicación


@dataclass
class UJI:
    cfg: Configuración = field()

    año: int = field(init=False)

    línea: dict[str, LíneaDeFinanciación] = field(default_factory=dict)
    tipo_de_línea: dict[str, TipoLínea] = field(default_factory=dict)
    proyecto: dict[str, Proyecto] = field(default_factory=dict)
    centro: dict[str, Centro] = field(default_factory=dict)
    ubicación: dict[str, Ubicación] = field(default_factory=dict)

    estructuras: Estructuras = field(init=False)

    def __init__(self, cfg: Configuración):
        logger.trace(f"Inicializando UJI {cfg.año} con ficheros de {cfg.raíz_datos}")
        self.cfg = cfg

        # Cargamos estructuras presupuestarias
        self.carga_centros_y_subcentros()
        self.carga_proyectos_subproyectos_y_tipos_de_proyecto()
        self.carga_líneas_y_tipos_de_línea()
        self.carga_ubicaciones()

        # Completa el árbol de actividades con proyectos de los que sabemos clasificar la actividad por su tipo
        self.crea_actividades_por_tipo_de_proyecto()

        self.estructuras = Estructuras(cfg)

        self.apuntes = Apuntes.carga(cfg)
        self._etiqueta("elemento_de_coste", "etiquetador_elemento_de_coste_para_apuntes", self.apuntes)
        self._etiqueta("centro_de_coste", "etiquetador_centro_de_coste_para_apuntes", self.apuntes)

        self.nóminas = Nóminas.carga(cfg)
        self._etiqueta("centro_de_coste", "etiquetador_centro_de_coste_para_nóminas", self.nóminas)
        self._etiqueta("elemento_de_coste", "etiquetador_elemento_de_coste_para_nóminas", self.nóminas)

        self.previsión_social_funcionarios = self.calcula_previsión_social_funcionarios()

        self.costes_por_amortizaciones = CostesPorAmortizaciones(cfg)

        self.cfg.traza.guarda()

    def _etiqueta(self, etiqueta: str, nombre_reglas: str, objetos: Any) -> None:
        reglas = ReglasEtiquetador(self.cfg, nombre_reglas)
        for objeto in objetos:
            for regla in reglas:
                if regla(objeto):
                    setattr(objeto, etiqueta, regla.etiqueta)
                    break

        self.cfg.traza(reglas.para_traza(etiqueta, objetos))

    def calcula_previsión_social_funcionarios(self) -> Nóminas:
        psf: dict[str, Nómina] = {}

        porcentaje_ss = self.cfg.previsión_social_funcionarios.porcentaje_previsión_social
        base_máxima_cotización = self.cfg.previsión_social_funcionarios.base_máxima_de_cotización
        tope_ss = base_máxima_cotización * porcentaje_ss

        por_per_id: dict[str, list[Nómina]] = {}
        for nómina in self.nóminas:
            if nómina.categoría_perceptor in ["CU", "TU", "TEU", "CEU"]:
                por_per_id.setdefault(nómina.per_id, []).append(nómina)
        for per_id in por_per_id:
            if any(nómina.aplicación.startswith("12") for nómina in por_per_id[per_id]):
                continue
            importe_total = sum((nómina.importe for nómina in por_per_id[per_id]))
            coste_ss = min(importe_total * porcentaje_ss, tope_ss)
            for nómina in por_per_id[per_id]:
                fracción = nómina.importe / importe_total
                psf[nómina.id] = replace(nómina, importe=coste_ss * fracción, aplicación="1200")

        traza = self.cfg.traza
        traza("= Previsión social de funcionarios\n")
        traza("""
            #align(center,
                table(
                    columns: 3,
                    align: (left, right, right),
                    stroke: none,
                    inset: (y: 0.2em),
                    table.header(
                         table.hline(),
                         [*Nómina*], [*Personas*], [*Importe*],
                         table.hline()
                    ),
        """)
        importe_por_categoría = {"CU": euro.zero, "TU": euro.zero, "TEU": euro.zero, "CEU": euro.zero}
        personas_por_categoría = {"CU": set(), "TU": set(), "TEU": set(), "CEU": set()}
        for previsión in psf:
            importe_por_categoría[psf[previsión].categoría_perceptor] += psf[previsión].importe
            personas_por_categoría[psf[previsión].categoría_perceptor].add(psf[previsión].per_id)
        for k, v in importe_por_categoría.items():
            traza(f"  [{k}], [{len(personas_por_categoría[k])}], [{v}],\n")
        traza("table.hline(),")
        traza(
            "[*Total*],"
            + f" [*{num(sum(len(x) for x in personas_por_categoría.values()))}*],"
            + f" [*{sum(importe_por_categoría.values())}*],"
        )
        traza("table.hline(),")
        traza("""
                )
            )
        """)

        return Nóminas(psf)

    def carga_centros_y_subcentros(self) -> None:
        df = self.cfg.fichero("centros").carga_dataframe()
        self.centro = {}
        for row in df.iter_rows(named=True):
            self.centro[row["código"]] = Centro(código=row["código"], nombre=row["nombre"])

        self.subcentro = {}
        df = self.cfg.fichero("subcentros").carga_dataframe()
        for row in df.iter_rows(named=True):
            código_centro = row["código_centro"]
            self.centro[código_centro].subcentros[row["código"]] = Subcentro(
                centro=código_centro, código=row["código"], nombre=row["nombre"]
            )

        traza = self.cfg.traza
        traza("= Centros y subcentros\n")
        traza("""
            #align(center,
                table(
                    columns: 3,
                    align: (left, left, left),
                    stroke: none,
                    inset: (y: 0.2em),
                    table.header(
                        table.hline(),
                        [*Centro*], [*Subcentro*], [*Nombre*],
                        table.hline()
                    ),
              """)
        for código, centro in sorted(self.centro.items(), key=lambda x: x[0]):
            traza("table.hline(stroke: .5pt),")
            traza(f"  [*{código}*], [], [*{centro.nombre}*],\n")
            for subcentro in human_sorted(centro.subcentros.values(), key=lambda x: x.código):
                traza(f"    [], [{subcentro.código}], [#h(2em){subcentro.nombre}],\n")
        traza("table.hline(),")
        traza("""
                )
            )
        """)

    def carga_proyectos_subproyectos_y_tipos_de_proyecto(self) -> None:
        df = self.cfg.fichero("proyectos").carga_dataframe()
        self.proyectos = {}
        for row in df.iter_rows(named=True):
            self.proyectos[row["código"]] = Proyecto(
                código=row["código"], nombre=row["nombre"], tipo=row["tipo"], subtipo=row["subtipo"]
            )

        self.subproyectos = {}
        df = self.cfg.fichero("subproyectos").carga_dataframe()
        for row in df.iter_rows(named=True):
            código_proyecto = row["código_proyecto"]
            nombre = row["nombre"].replace("//", "/")
            self.proyectos[código_proyecto].subproyectos[row["código"]] = Subproyecto(
                código_proyecto=código_proyecto, código=row["código"], nombre=nombre
            )

        self.tipo_de_proyecto = {}
        df = self.cfg.fichero("tipos-de-proyecto").carga_dataframe()
        for row in df.iter_rows(named=True):
            nombre = normaliza_texto(row["nombre"])
            self.tipo_de_proyecto[row["código"]] = TipoProyecto(código=row["código"], nombre=nombre)

        traza = self.cfg.traza
        traza("= Proyectos y subproyectos\n")
        traza("""
            #align(center,
                table(
                    columns: 4,
                    align: (left, left, left, left),
                    stroke: none,
                    inset: (y: 0.2em),
                    table.header(
                        table.hline(),
                        [*Proyecto*], [*Tipo*], [*Subproyecto*], [*Nombre*],
                        table.hline()
                    ),
              """)
        for código, proyecto in sorted(self.proyectos.items(), key=lambda x: x[0]):
            traza("table.hline(stroke: .5pt),")
            nombre = normaliza_texto(proyecto.nombre)
            traza(f"  [*{código}*], [*{proyecto.tipo}*], [], [*{nombre}*],")
            for subproyecto in human_sorted(proyecto.subproyectos.values(), key=lambda x: x.código):
                nombre = normaliza_texto(subproyecto.nombre)
                traza(f"    [], [], [{subproyecto.código}], [#h(2em){nombre}],")
        traza("table.hline(),")
        traza("""
                )
            )
        """)
        traza("== Tipos de proyecto\n")
        traza("""
            #align(center,
                table(
                    columns: 2,
                    align: (left, left),
                    stroke: none,
                    inset: (y: 0.2em),
                    table.header(
                        table.hline(),
                        [*Tipo*], [*Nombre*],
                        table.hline()
                    ),
              """)
        for código, tipo in sorted(self.tipo_de_proyecto.items(), key=lambda x: x[0]):
            traza(f"  [{código}], [{tipo.nombre}],")
        traza("""
                table.hline(),
                )
            )
        """)

    def carga_líneas_y_tipos_de_línea(self) -> None:
        df = self.cfg.fichero("líneas").carga_dataframe()
        self.línea = {}
        for row in df.iter_rows(named=True):
            self.línea[row["código"]] = LíneaDeFinanciación(
                código=row["código"], nombre=row["nombre"], tipo=row["tipo"]
            )

        self.tipo_de_línea = {}
        df = self.cfg.fichero("tipos-de-línea").carga_dataframe()
        for row in df.iter_rows(named=True):
            self.tipo_de_línea[row["código"]] = TipoLínea(código=row["código"], nombre=row["nombre"])

        traza = self.cfg.traza
        traza("= Líneas y tipos de línea\n")
        traza("== Líneas\n")
        traza("""
            #align(center,
                table(
                    columns: 3,
                    align: (left, left, left),
                    stroke: none,
                    inset: (y: 0.2em),
                    table.header(
                        table.hline(),
                        [*Línea*], [*Tipo*], [*Nombre*],
                        table.hline()
                    ),
              """)
        for código, línea in sorted(self.línea.items(), key=lambda x: x[0]):
            nombre = normaliza_texto(línea.nombre)
            traza(f"  [{código}], [{línea.tipo}], [{nombre}],")
        traza("""
                table.hline(),
                )
            )
        """)
        traza("== Tipos de línea\n")
        traza("""
            #align(center,
                table(
                    columns: 2,
                    align: (left, left),
                    stroke: none,
                    inset: (y: 0.2em),
                    table.header(
                        table.hline(),
                        [*Tipo*], [*Nombre*],
                        table.hline()
                    ),
              """)
        for código, tipo in sorted(self.tipo_de_línea.items(), key=lambda x: x[0]):
            nombre = normaliza_texto(tipo.nombre)
            traza(f"  [{código}], [{nombre}],")
        traza("""
                table.hline(),
                )
            )
        """)

    def carga_ubicaciones(self) -> None:
        df = self.cfg.fichero("ubicaciones").carga_dataframe()
        self.ubicación = {}
        for row in df.iter_rows(named=True):
            id = row["id"]
            nombre = normaliza_texto(row["nombre"])
            self.ubicación[id] = Ubicación(id=id, código=row["código"], nombre=nombre)

        traza = self.cfg.traza
        traza("= Ubicaciones\n")
        traza("""
            #align(center,
                table(
                    columns: 3,
                    align: (left, left, left),
                    stroke: none,
                    inset: (y: 0.2em),
                    table.header(
                        table.hline(),
                        [*Ubicación*], [*Código*], [*Nombre*],
                        table.hline()
                    ),
              """)
        for id, ubicación in sorted(self.ubicación.items(), key=lambda x: x[0]):
            nombre = normaliza_texto(ubicación.nombre)
            traza(f"  [{id}], [{ubicación.código}], [{nombre}],")
        traza("""
                table.hline(),
                )
            )
        """)

    def crea_actividades_por_tipo_de_proyecto(self):
        actividades = {
            "ac_transf.tree": (self.cfg.tipos_de_proyecto_convertibles_en_actividad.transferencia, None),
            "ac_inv_regional.tree": (self.cfg.tipos_de_proyecto_convertibles_en_actividad.investigación_regional, None),
            "ac_inv_nacional.tree": (self.cfg.tipos_de_proyecto_convertibles_en_actividad.investigación_nacional, None),
            "ac_inv_internacional.tree": (
                self.cfg.tipos_de_proyecto_convertibles_en_actividad.investigación_internacional,
                None,
            ),
            "ac_masteres_fp.tree": (self.cfg.tipos_de_proyecto_convertibles_en_actividad.formación_propia, "MasterFP"),
            "ac_diplomas_especializacion.tree": (
                self.cfg.tipos_de_proyecto_convertibles_en_actividad.formación_propia,
                "DiplomaEspecializacion",
            ),
            "ac_diplomas_experto.tree": (
                self.cfg.tipos_de_proyecto_convertibles_en_actividad.formación_propia,
                "DiplomaExperto",
            ),
            "ac_cursos.tree": (self.cfg.tipos_de_proyecto_convertibles_en_actividad.formación_propia, "CursoFP"),
            "ac_microcredenciales.tree": (
                self.cfg.tipos_de_proyecto_convertibles_en_actividad.formación_propia,
                "Microcredencial",
            ),
        }
        for actividad, (tipos, subtipo) in actividades.items():
            directorio = self.cfg.directorio("dir-actividades").ruta
            with open(directorio / actividad, "w") as f:
                for proyecto in self.proyectos.values():
                    if proyecto.tipo in tipos and (subtipo is None or proyecto.subtipo == subtipo):
                        código_actividad = "AC_" + proyecto.código
                        f.write(f"{proyecto.nombre} | {código_actividad}\n")
        return actividades
