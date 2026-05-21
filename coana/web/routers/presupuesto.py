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


# ---- Sub-vistas adicionales -----------------------------------------

@router.get("/sin-uc", response_model=ListResponse)
def sin_uc(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_sin_uc(p)


@router.get("/sin-clasificar", response_model=ListResponse)
def sin_clasificar(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_sin_clasificar(p)


@router.get("/filtrados", response_model=ListResponse)
def filtrados(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_filtrados(p)


@router.get("/filtrados-por-motivo", response_model=ListResponse)
def filtrados_por_motivo(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_filtrados_por_motivo(p)


@router.get("/uc-suministros", response_model=ListResponse)
def uc_suministros(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_uc_suministros(p)


@router.get("/otop-resumen", response_model=ListResponse)
def otop_resumen(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    """Distribución OTOP: suma por (centro, elemento de coste)."""
    return svc.listar_otop_resumen(p)


@router.get("/otop-detalle", response_model=ListResponse)
def otop_detalle(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    """Distribución OTOP: detalle del centro top (asiento a asiento)."""
    return svc.listar_otop_detalle(p)


@router.get("/reglas/actividad", response_model=ListResponse)
def reglas_actividad(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_reglas_actividad(p)


@router.get("/reglas/cc", response_model=ListResponse)
def reglas_cc(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_reglas_cc(p)


@router.get("/reglas/ec", response_model=ListResponse)
def reglas_ec(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_reglas_ec(p)
