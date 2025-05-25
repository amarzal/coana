from dataclasses import dataclass

from loguru import logger

from coana.etiquetador import Etiquetador
from coana.ficheros import Ficheros
from coana.uji.apuntes import Apuntes
from coana.uji.nóminas import Nóminas
from coana.uji.previsión_social_funcionarios import PrevisiónSocialFuncionarios


@dataclass
class EtiquetadoresUJI:
    elemento_de_coste_para_apuntes: Etiquetador
    centro_de_coste_para_apuntes: Etiquetador
    elemento_de_coste_para_nóminas: Etiquetador
    centro_de_coste_para_nóminas: Etiquetador

    def __init__(self) -> None:
        ficheros = Ficheros()
        df_elemento_de_coste_para_apuntes = ficheros.fichero("etiquetador_elemento_de_coste_para_apuntes").carga_dataframe()
        df_centro_de_coste_para_apuntes = ficheros.fichero("etiquetador_centro_de_coste_para_apuntes").carga_dataframe()
        df_elemento_de_coste_para_nóminas = ficheros.fichero("etiquetador_elemento_de_coste_para_nóminas").carga_dataframe()
        df_centro_de_coste_para_nóminas = ficheros.fichero("etiquetador_centro_de_coste_para_nóminas").carga_dataframe()
        self.elemento_de_coste_para_apuntes = Etiquetador(df_elemento_de_coste_para_apuntes)
        self.centro_de_coste_para_apuntes = Etiquetador(df_centro_de_coste_para_apuntes)
        self.elemento_de_coste_para_nóminas = Etiquetador(df_elemento_de_coste_para_nóminas)
        self.centro_de_coste_para_nóminas = Etiquetador(df_centro_de_coste_para_nóminas)
