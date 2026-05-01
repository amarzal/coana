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


@router.get("/expedientes/{sector}/{expediente}/lineas", response_model=ListResponse)
def listar_lineas_expediente(
    sector: str,
    expediente: int,
    p: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """Líneas de la nómina (incluye SS) asociadas a un expediente."""
    del sector  # el filtro real es por expediente
    return svc.listar_lineas_nomina(expediente, p)


# ---- Multiexpediente ------------------------------------------------

@router.get("/multiexpediente", response_model=ListResponse)
def multiexpediente(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_multiexpediente(p)


@router.get("/multiexpediente/{per_id}/matriz", response_model=ListResponse)
def matriz_mensual(
    per_id: int,
    p: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """Matriz expediente × mes con importes para una persona."""
    return svc.matriz_mensual(per_id, p)


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
