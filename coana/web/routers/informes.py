"""Endpoints para los informes de Fase 2.

Sirve los YAML de `data/informes/*.yaml` como JSON.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

DIR_INFORMES = Path("data/informes")


class InformeMeta(BaseModel):
    id: str
    título: str
    tipo: str   # "jerárquico" | "centros_d_i_p" | "matriz"


# Catálogo de informes disponibles. El `tipo` orienta al frontend
# sobre cómo renderizar la tabla.
_CATÁLOGO: list[InformeMeta] = [
    InformeMeta(id="cuadro_10_1", título="Cuadro 10.1 — Informe de elementos de coste", tipo="jerárquico"),
    InformeMeta(id="cuadro_10_3", título="Cuadro 10.3 — Informe general de ingresos por actividades", tipo="jerárquico"),
    InformeMeta(id="cuadro_10_4", título="Cuadro 10.4 — Informe de costes por centros de coste según su finalidad", tipo="jerárquico"),
    InformeMeta(id="cuadro_10_5", título="Cuadro 10.5 — Informe de costes primarios por centro de coste", tipo="centros_d_i_p"),
    InformeMeta(id="cuadro_10_7", título="Cuadro 10.7 — Composición del coste de las actividades finalistas", tipo="matriz"),
]


@router.get("", response_model=list[InformeMeta])
def listar_informes() -> list[InformeMeta]:
    """Lista los informes disponibles."""
    return [i for i in _CATÁLOGO if (DIR_INFORMES / f"{i.id}.yaml").exists()]


@router.get("/{cuadro_id}")
def obtener_informe(cuadro_id: str) -> dict[str, Any]:
    """Devuelve el YAML del cuadro como JSON. Incluye la metainformación
    del catálogo (`tipo`) para que el frontend sepa cómo renderizarlo."""
    meta = next((i for i in _CATÁLOGO if i.id == cuadro_id), None)
    if meta is None:
        raise HTTPException(status_code=404, detail=f"Cuadro {cuadro_id!r} no encontrado en el catálogo")
    path = DIR_INFORMES / f"{cuadro_id}.yaml"
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=(
                f"No existe {path}. Ejecuta «Generar informes» para "
                f"producir los artefactos de Fase 2."
            ),
        )
    with path.open(encoding="utf-8") as f:
        datos = yaml.safe_load(f)
    datos["_meta"] = meta.model_dump()
    return datos
