"""Endpoints del bloque «Superficies»."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from coana.web.schemas.common import KpiPanel, ListResponse
from coana.web.services import superficies as svc
from coana.web.services.query import QueryParams, query_dependency

router = APIRouter()


@router.get("/_resumen", response_model=KpiPanel)
def resumen() -> KpiPanel:
    return svc.resumen()


@router.get("/totales/complejos", response_model=ListResponse)
def listar_complejos(params: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_complejos(params)


@router.get("/totales/edificaciones", response_model=ListResponse)
def listar_edificaciones(params: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_edificaciones(params)


@router.get("/totales/zonas", response_model=ListResponse)
def listar_zonas(params: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_zonas(params)
