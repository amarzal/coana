"""Endpoints del bloque «Personal»."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from coana.web.schemas.common import KpiPanel, ListResponse, RecordResponse
from coana.web.services import personal as svc
from coana.web.services.query import QueryParams, query_dependency

router = APIRouter()


@router.get("/_resumen", response_model=KpiPanel)
def resumen() -> KpiPanel:
    return svc.resumen()


# ---- Expedientes por sector -----------------------------------------

@router.get("/expedientes/{sector}", response_model=ListResponse)
def listar_sector(
    sector: str,
    params: QueryParams = Depends(query_dependency),
) -> ListResponse:
    res = svc.listar_sector(sector, params)
    if res is None:
        raise HTTPException(status_code=404, detail=f"sector inválido: {sector!r}")
    return res


@router.get("/expedientes/{sector}/{expediente}", response_model=RecordResponse)
def obtener_expediente(sector: str, expediente: int) -> RecordResponse:
    rec = svc.obtener_expediente(sector, expediente)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"expediente {expediente} no encontrado")
    return rec


# ---- Multiexpediente ------------------------------------------------

@router.get("/multiexpediente", response_model=ListResponse)
def multiexpediente(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_multiexpediente(p)


# ---- Persona --------------------------------------------------------

@router.get("/persona", response_model=ListResponse)
def listar_personas(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_personas(p)


@router.get("/persona/{per_id}", response_model=RecordResponse)
def obtener_persona(per_id: int) -> RecordResponse:
    rec = svc.obtener_persona(per_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"per_id {per_id} no encontrado")
    return rec


@router.get("/persona/{per_id}/uc", response_model=ListResponse)
def listar_uc_persona(
    per_id: int, p: QueryParams = Depends(query_dependency),
) -> ListResponse:
    return svc.listar_uc_persona(per_id, p)


# ---- Anomalías PDI --------------------------------------------------

@router.get("/anomalias-pdi", response_model=ListResponse)
def anomalias_pdi(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_anomalias_pdi(p)
