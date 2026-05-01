"""Endpoints del bloque «Presupuesto»."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from coana.web.schemas.common import KpiPanel, ListResponse, RecordResponse
from coana.web.services import presupuesto as svc
from coana.web.services.query import QueryParams, query_dependency

router = APIRouter()


@router.get("/_resumen", response_model=KpiPanel)
def resumen() -> KpiPanel:
    """Panel de KPIs del bloque Presupuesto."""
    return svc.resumen()


@router.get("/uc", response_model=ListResponse)
def listar_uc(params: QueryParams = Depends(query_dependency)) -> ListResponse:
    """Listado paginado de unidades de coste de presupuesto."""
    return svc.listar_uc(params)


@router.get("/uc/{uc_id}", response_model=RecordResponse)
def obtener_uc(uc_id: str) -> RecordResponse:
    """Ficha de una UC concreta, con enriquecimientos."""
    rec = svc.obtener_uc(uc_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"UC {uc_id!r} no encontrada")
    return rec
