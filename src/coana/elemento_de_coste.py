from dataclasses import dataclass, field
from typing import Any

from coana.misc.euro import E


@dataclass
class ElementoDeCoste:
    importe: E
    etiqueta_elemento_de_coste: str | None
    etiqueta_centro_de_coste: str | None = field(default=None)
    etiqueta_actividad: str | None = field(default=None)
    traza: Any | None = field(default=None)
    comentario: str | None = field(default=None)

    def reparte(self, partes: list[float]) -> "list[ElementoDeCoste]":
        total_partes = sum(partes)
        if total_partes != 1:
            partes = [p / total_partes for p in partes]
        importe = self.importe
        todos_menos_último = [importe * p for p in partes[:-1]]
        último = importe - sum(todos_menos_último)
        ecs = []
        for parte, euros in zip(partes, (*todos_menos_último, último)):
            traza = (parte, self)
            ec = ElementoDeCoste(
                importe=euros,
                etiqueta_elemento_de_coste=self.etiqueta_elemento_de_coste,
                etiqueta_centro_de_coste=self.etiqueta_centro_de_coste,
                etiqueta_actividad=self.etiqueta_actividad,
                traza=traza,
            )
            ecs.append(ec)
        return ecs
