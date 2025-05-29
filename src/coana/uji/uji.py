import re
from dataclasses import dataclass, field, replace
from re import I, S
from typing import Any

from loguru import logger

import coana.misc.typst as ty
from coana.configuración import Configuración
from coana.estructuras import Estructuras
from coana.misc import euro
from coana.misc.utils import num, porcentaje
from coana.uji.apuntes import Apuntes
from coana.uji.centro import Centro
from coana.uji.etiquetador import ReglasEtiquetador
from coana.uji.línea_de_financiación import LíneaDeFinanciación, TipoLínea
from coana.uji.nóminas import Nómina, Nóminas
from coana.uji.proyecto import Proyecto


@dataclass
class UJI:
    cfg: Configuración = field()

    año: int = field(init=False)

    estructuras: Estructuras = field(init=False)

    línea: dict[str, LíneaDeFinanciación] = field(default_factory=dict)
    tipo_de_línea: dict[str, TipoLínea] = field(default_factory=dict)
    proyecto: dict[str, Proyecto] = field(default_factory=dict)
    centro: dict[str, Centro] = field(default_factory=dict)

    def __init__(self, cfg: Configuración):
        logger.trace(f"Inicializando UJI {cfg.año} con ficheros de {cfg.raíz_datos}")
        self.cfg = cfg

        self.estructuras = Estructuras(cfg)

        self.apuntes = Apuntes.carga(cfg)
        self._etiqueta("elemento_de_coste", "etiquetador_elemento_de_coste_para_apuntes", self.apuntes)
        self._etiqueta("centro_de_coste", "etiquetador_centro_de_coste_para_apuntes", self.apuntes)

        self.nóminas = Nóminas.carga(cfg)
        self._etiqueta("centro_de_coste", "etiquetador_centro_de_coste_para_nóminas", self.nóminas)
        self._etiqueta("elemento_de_coste", "etiquetador_elemento_de_coste_para_nóminas", self.nóminas)

        self.previsión_social_funcionarios = self.calcula_previsión_social_funcionarios()

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

        porcentaje_ss = self.cfg.previsión_social_funcionarios["porcentaje_previsión_social"]
        base_máxima_cotización = self.cfg.previsión_social_funcionarios["base_máxima_cotización"]
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
        traza("== Nóminas\n")
        traza("""
            #align(center,
                table(
                    columns: 2,
                    align: (left, right),
                    stroke: none,
                    inset: (y: 0.2em),
                    table.header(
                         table.hline(),
                         [*Nómina*], [*Importe*],
                         table.hline()
                    ),
        """)
        importe_por_categoría = {'CU': euro.zero, 'TU': euro.zero, 'TEU': euro.zero, 'CEU': euro.zero}
        for previsión in psf:
            importe_por_categoría[psf[previsión].categoría_perceptor] += psf[previsión].importe
        for k, v in importe_por_categoría.items():
            traza(f"  [{k}], [{v}],\n")
        traza("table.hline(),")
        traza(f"[*Total*], [{sum(importe_por_categoría.values())}],")
        traza("table.hline(),")
        traza("""
                )
            )
        """)


        return Nóminas(psf)
