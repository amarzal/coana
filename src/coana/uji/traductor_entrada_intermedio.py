from dataclasses import dataclass

from loguru import logger

from coana.etiquetador import Etiquetador
from coana.ficheros import Ficheros
from coana.uji.apuntes import Apuntes
from coana.uji.nóminas import Nóminas
from coana.uji.previsión_social_funcionarios import PrevisionesSocialesFuncionarios


@dataclass
class TraductorEntradaIntermedio:
    etiquetador_elemento_de_coste_para_apuntes: Etiquetador
    etiquetador_centro_de_coste_para_apuntes: Etiquetador
    etiquetador_elemento_de_coste_para_nóminas: Etiquetador
    etiquetador_centro_de_coste_para_nóminas: Etiquetador

    def __init__(self) -> None:
        ficheros = Ficheros()
        df_elemento_de_coste_para_apuntes = ficheros.fichero("etiquetador_elemento_de_coste_para_apuntes").carga_dataframe()
        df_centro_de_coste_para_apuntes = ficheros.fichero("etiquetador_centro_de_coste_para_apuntes").carga_dataframe()
        df_elemento_de_coste_para_nóminas = ficheros.fichero("etiquetador_elemento_de_coste_para_nóminas").carga_dataframe()
        df_centro_de_coste_para_nóminas = ficheros.fichero("etiquetador_centro_de_coste_para_nóminas").carga_dataframe()
        self.etiquetador_elemento_de_coste_para_apuntes = Etiquetador(df_elemento_de_coste_para_apuntes)
        self.etiquetador_centro_de_coste_para_apuntes = Etiquetador(df_centro_de_coste_para_apuntes)
        self.etiquetador_elemento_de_coste_para_nóminas = Etiquetador(df_elemento_de_coste_para_nóminas)
        self.etiquetador_centro_de_coste_para_nóminas = Etiquetador(df_centro_de_coste_para_nóminas)

    def traduce(self) -> None:
        ficheros = Ficheros()

        apuntes = Apuntes.carga()

        logger.trace("Etiquetando apuntes con elementos de coste")
        apuntes.etiqueta("elemento_de_coste", self.etiquetador_elemento_de_coste_para_apuntes)

        logger.trace("Etiquetando apuntes con centros de coste")
        apuntes.etiqueta("centro_de_coste", self.etiquetador_centro_de_coste_para_apuntes)

        # TODO: Etiquetar con actividades

        apuntes.guarda(ficheros.fichero("traza_apuntes_etiquetados").path)

        nóminas = Nóminas.carga()
        nóminas.análisis_de_previsión_social()

        nóminas.etiqueta("elemento_de_coste", self.etiquetador_elemento_de_coste_para_nóminas)
        nóminas.etiqueta("centro_de_coste", self.etiquetador_centro_de_coste_para_nóminas)

        nóminas.guarda(ficheros.fichero("traza_nóminas_etiquetadas").path)
