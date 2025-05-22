from dataclasses import dataclass, field
from typing import Iterator

from loguru import logger
from openpyxl import Workbook

from coana.elemento_de_coste import ElementoDeCoste
from coana.ficheros import Ficheros


@dataclass
class ElementosDeCoste:
    elementos_de_coste: list[ElementoDeCoste] = field(default_factory=list)

    def __iter__(self) -> Iterator[ElementoDeCoste]:
        return iter(self.elementos_de_coste)

    def __len__(self) -> int:
        return len(self.elementos_de_coste)

    def añade(self, elemento_de_coste: ElementoDeCoste) -> None:
        self.elementos_de_coste.append(elemento_de_coste)

    def a_excel(self):
        ficheros = Ficheros()
        wb = Workbook()
        ws = wb.active
        assert ws is not None
        ws.title = "Elementos de coste"
        ws.append(["Elemento de coste", "Importe", "Centro de coste", "Actividad", "Traza", "Comentario"])
        for elemento_de_coste in self.elementos_de_coste:
            try:
                ws.append([
                    elemento_de_coste.etiqueta_elemento_de_coste,
                    elemento_de_coste.importe.céntimos / 100,
                    elemento_de_coste.etiqueta_centro_de_coste,
                    elemento_de_coste.etiqueta_actividad,
                    str(elemento_de_coste.traza),
                    elemento_de_coste.comentario,
                ])
            except Exception as e:
                logger.error(
                    f"Error al escribir el elemento de coste {elemento_de_coste}"
                    + " en el fichero {cfg.intermedio_elementos_de_coste}: {e}"
                )
                raise e
        wb.save(ficheros.intermedio_elementos_de_coste)
        logger.trace(f"Creado fichero {ficheros.intermedio_elementos_de_coste}")
