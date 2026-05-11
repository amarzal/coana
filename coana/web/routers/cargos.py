"""Endpoints del bloque «Cargos académicos»."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from coana.web.schemas.common import KpiPanel, ListResponse, RecordResponse
from coana.web.services import cargos as svc
from coana.web.services.query import QueryParams, query_dependency

router = APIRouter()


@router.get("/_resumen", response_model=KpiPanel)
def resumen() -> KpiPanel:
    return svc.resumen()


@router.get("/categoria_pdi_pvi", response_model=ListResponse)
def listar_categoria(params: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_categoria(params)


@router.get("/categoria_pdi_pvi/{per_id}", response_model=RecordResponse)
def obtener_categoria(per_id: int) -> RecordResponse:
    rec = svc.obtener_categoria(per_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"per_id {per_id} no encontrado")
    return rec


@router.get("/departamentos", response_model=ListResponse)
def listar_departamentos(
    params: QueryParams = Depends(query_dependency),
) -> ListResponse:
    return svc.listar_departamentos(params)


@router.get("/departamentos/{idx}", response_model=RecordResponse)
def obtener_cargo(idx: int) -> RecordResponse:
    rec = svc.obtener_cargo(idx)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"índice {idx} fuera de rango")
    return rec


@router.get("/personas-cargos", response_model=ListResponse)
def listar_personas_cargos(
    params: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """Vista bruta de `personas cargos.xlsx`, enriquecida con el nombre."""
    return svc.listar_personas_cargos(params)


@router.get("/cargos", response_model=ListResponse)
def listar_cargos(
    params: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """Catálogo de cargos (`cargos.xlsx`)."""
    return svc.listar_cargos(params)


@router.get("/cargos/{cargo}/personas", response_model=ListResponse)
def listar_personas_de_cargo(
    cargo: str,
    params: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """Personas y expedientes que han ocupado el cargo en el año analizado."""
    return svc.listar_personas_de_cargo(cargo, params)
