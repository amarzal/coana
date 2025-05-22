from dataclasses import dataclass

from loguru import logger

from coana.etiquetador import Etiquetador
from coana.ficheros import Ficheros
from coana.uji.apuntes import Apuntes
from coana.uji.nóminas import Nóminas


@dataclass
class TraductorEntradaIntermedio:
    def traduce(self) -> None:
        ficheros = Ficheros()

        apuntes = Apuntes.carga()

        logger.trace("Etiquetando apuntes con elementos de coste")
        etq_ec_apuntes = Etiquetador.carga(ficheros.etiquetador_elemento_de_coste_para_apuntes)
        apuntes.etiqueta("ELEMENTO_DE_COSTE", etq_ec_apuntes)

        logger.trace("Etiquetando apuntes con centros de coste")
        etq_cc_apuntes = Etiquetador.carga(ficheros.etiquetador_centro_de_coste_para_apuntes)
        apuntes.etiqueta("CENTRO_DE_COSTE", etq_cc_apuntes)

        # TODO: Etiquetar con actividades

        apuntes.guarda(ficheros.traza_apuntes_etiquetados)

        nóminas = Nóminas.carga()
        etq_ec_nóminas = Etiquetador.carga(ficheros.etiquetador_elemento_de_coste_para_nóminas)
        nóminas.etiqueta("ELEMENTO_DE_COSTE", etq_ec_nóminas)

        etq_cc_nóminas = Etiquetador.carga(ficheros.etiquetador_centro_de_coste_para_nóminas)
        nóminas.etiqueta("CENTRO_DE_COSTE", etq_cc_nóminas)

        nóminas.guarda(ficheros.traza_nóminas_etiquetadas)
