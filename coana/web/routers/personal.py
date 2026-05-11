"""Endpoints del bloque «Personal»."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

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
    grupo: str | None = Query(None, description="Filtra por grupo (Costes sociales, Retribuciones ordinarias…)"),
    p: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """Líneas de la nómina (incluye SS) asociadas a un expediente.

    Si se pasa ``grupo``, devuelve solo las líneas de ese grupo según
    la clasificación por sector (PDI/PTGAS/PVI/Otros).
    """
    return svc.listar_lineas_nomina(expediente, p, sector=sector, grupo=grupo)


@router.get(
    "/expedientes/{sector}/{expediente}/grupos",
    response_model=svc.GruposLineasResponse,
)
def grupos_lineas_expediente(
    sector: str, expediente: int,
) -> svc.GruposLineasResponse:
    """Metadatos de los grupos en que se reparten las líneas del expediente."""
    return svc.grupos_lineas_nomina(sector, expediente)


@router.get(
    "/expedientes/{sector}/{expediente}/uc",
    response_model=ListResponse,
)
def listar_uc_expediente(
    sector: str,
    expediente: int,
    p: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """UC generadas durante la fase 1 para un expediente concreto."""
    return svc.listar_uc_expediente(sector, expediente, p)


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


# ---- Costes sociales calculados (clases pasivas PDI funcionario) ----

@router.get("/costes-sociales-calculados", response_model=ListResponse)
def costes_sociales_calculados(
    p: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """Detalle por persona del coste social calculado (clases pasivas)."""
    return svc.listar_costes_sociales_calculados(p)
