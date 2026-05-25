"""Endpoints del bloque «Reducciones sindicales» (PDI + PTGAS)."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from coana.web.schemas.common import KpiPanel, ListResponse
from coana.web.services import reducciones_sindicales as svc
from coana.web.services.query import QueryParams, query_dependency

router = APIRouter()


@router.get("/_resumen", response_model=KpiPanel)
def resumen() -> KpiPanel:
    return svc.resumen()


@router.get("/pdi", response_model=ListResponse)
def listar_pdi(params: QueryParams = Depends(query_dependency)) -> ListResponse:
    """Reducción sindical del PDI (tipos 37-40, basada en créditos)."""
    return svc.listar_pdi(params)


@router.get("/ptgas", response_model=ListResponse)
def listar_ptgas(params: QueryParams = Depends(query_dependency)) -> ListResponse:
    """Reducción sindical del PTGAS (tipo 8, basada en días y % de jornada)."""
    return svc.listar_ptgas(params)
