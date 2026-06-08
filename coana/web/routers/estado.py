"""Endpoint de estado de frescura del pipeline."""

from __future__ import annotations

from fastapi import APIRouter

from coana.web.services import estado as svc
from coana.web.services.estado import EstadoPipeline

router = APIRouter()


@router.get("/pipeline", response_model=EstadoPipeline)
def estado_pipeline() -> EstadoPipeline:
    """Frescura de las etapas derivadas (Reparto, Fase 2) vs la Fase 1."""
    return svc.estado_pipeline()
