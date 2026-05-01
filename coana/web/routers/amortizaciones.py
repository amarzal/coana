"""Endpoints del bloque «Amortizaciones»."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from coana.web.schemas.common import KpiPanel, ListResponse, RecordResponse
from coana.web.services import amortizaciones as svc
from coana.web.services.query import QueryParams, query_dependency

router = APIRouter()


@router.get("/_resumen", response_model=KpiPanel)
def resumen() -> KpiPanel:
    return svc.resumen()


# ---- Listas ---------------------------------------------------------

@router.get("/enriquecido", response_model=ListResponse)
def enriquecido(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_enriquecido(p)


@router.get("/filtrados/estado", response_model=ListResponse)
def filtrados_estado(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_filtrados_estado(p)


@router.get("/filtrados/cuenta", response_model=ListResponse)
def filtrados_cuenta(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_filtrados_cuenta(p)


@router.get("/filtrados/fecha", response_model=ListResponse)
def filtrados_fecha(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_filtrados_fecha(p)


@router.get("/sin-cuenta", response_model=ListResponse)
def sin_cuenta(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_sin_cuenta(p)


@router.get("/sin-fecha-alta", response_model=ListResponse)
def sin_fecha_alta(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_sin_fecha(p)


@router.get("/detalle-cuentas", response_model=ListResponse)
def detalle_cuentas(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_detalle_cuentas(p)


@router.get("/sin-uc", response_model=ListResponse)
def sin_uc(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_sin_uc(p)


@router.get("/uc", response_model=ListResponse)
def uc(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_uc(p)


# ---- Fichas ---------------------------------------------------------

@router.get("/enriquecido/{reg_id}", response_model=RecordResponse)
def obtener_enriquecido(reg_id: int) -> RecordResponse:
    rec = svc.obtener_enriquecido(reg_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"id {reg_id} no encontrado")
    return rec


@router.get("/uc/{uc_id}", response_model=RecordResponse)
def obtener_uc(uc_id: str) -> RecordResponse:
    rec = svc.obtener_uc(uc_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"UC {uc_id!r} no encontrada")
    return rec
