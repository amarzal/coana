"""Endpoint genérico de enriquecimiento por columna."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from coana.web.services import lookups as svc

router = APIRouter()


class EnrichRequest(BaseModel):
    row: dict[str, Any]


class EnrichResponse(BaseModel):
    enriquecimientos: dict[str, dict[str, str]]


@router.post("/enrich-row", response_model=EnrichResponse)
def enrich_row(req: EnrichRequest) -> EnrichResponse:
    """Aplica los lookups conocidos al row enviado.

    Devuelve un dict ``{columna: {campo_extra: valor}}`` con solo las
    columnas que tienen información adicional disponible (per_id →
    persona, grado → nombre, centro → nombre, etc.).
    """
    return EnrichResponse(enriquecimientos=svc.enrich_row(req.row))
