"""Endpoints del bloque «Reparto de actividades» (costes dag)."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from coana.web.schemas.common import KpiPanel, ListResponse
from coana.web.services import reparto as svc
from coana.web.services.query import QueryParams, query_dependency

router = APIRouter()


@router.get("/_resumen", response_model=KpiPanel)
def resumen() -> KpiPanel:
    return svc.resumen()


@router.get("/uc", response_model=ListResponse)
def uc(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_uc(p)


@router.get("/porcentajes", response_model=ListResponse)
def porcentajes(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_porcentajes(p)


@router.get("/anomalias", response_model=ListResponse)
def anomalias(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_anomalias(p)


@router.get("/dag", response_model=ListResponse)
def dag(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_dag(p)


@router.get("/dag/{marca_dag}", response_model=ListResponse)
def dag_detalle(
    marca_dag: str, p: QueryParams = Depends(query_dependency),
) -> ListResponse:
    return svc.detalle_dag(marca_dag, p)
