"""Unidad de coste de la contabilidad analítica."""

from enum import Enum

from pydantic import BaseModel

from coana.util.euro import Euro


class OrigenUC(str, Enum):
    """De dónde procede una unidad de coste."""

    PRESUPUESTO = "presupuesto"
    NÓMINA = "nómina"
    INVENTARIO = "inventario"
    UNIDAD = "unidad"
    ENERGÍA = "energía"
    AGUA = "agua"
    GAS = "gas"


class UnidadDeCoste(BaseModel):
    """Registro individual de una unidad de coste."""

    id: str
    elemento_de_coste: str
    centro_de_coste: str = ""
    actividad: str = ""
    importe: Euro

    origen: OrigenUC
    origen_id: str
    origen_porción: float = 1.0
