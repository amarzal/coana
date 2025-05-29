from dataclasses import dataclass

from coana.configuración import Configuración
from coana.uji.etiquetador import ReglasEtiquetador


@dataclass
class ReglasEtiquetado:
    elemento_de_coste_para_apuntes: ReglasEtiquetador
    centro_de_coste_para_apuntes: ReglasEtiquetador
    elemento_de_coste_para_nóminas: ReglasEtiquetador
    centro_de_coste_para_nóminas: ReglasEtiquetador

    def __init__(self, cfg: Configuración ) -> None:
        df_elemento_de_coste_para_apuntes = cfg.fichero("etiquetador_elemento_de_coste_para_apuntes").carga_dataframe()
        df_centro_de_coste_para_apuntes = cfg.fichero("etiquetador_centro_de_coste_para_apuntes").carga_dataframe()
        df_elemento_de_coste_para_nóminas = cfg.fichero("etiquetador_elemento_de_coste_para_nóminas").carga_dataframe()
        df_centro_de_coste_para_nóminas = cfg.fichero("etiquetador_centro_de_coste_para_nóminas").carga_dataframe()
        self.elemento_de_coste_para_apuntes = ReglasEtiquetador(df_elemento_de_coste_para_apuntes)
        self.centro_de_coste_para_apuntes = ReglasEtiquetador(df_centro_de_coste_para_apuntes)
        self.elemento_de_coste_para_nóminas = ReglasEtiquetador(df_elemento_de_coste_para_nóminas)
        self.centro_de_coste_para_nóminas = ReglasEtiquetador(df_centro_de_coste_para_nóminas)
