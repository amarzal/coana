"""Endpoints del bloque «Cargos académicos»."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from coana.web.schemas.common import KpiPanel, ListResponse
from coana.web.services import cargos as svc
from coana.web.services.query import QueryParams, query_dependency

router = APIRouter()


@router.get("/_resumen", response_model=KpiPanel)
def resumen() -> KpiPanel:
    return svc.resumen()


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


@router.get("/personas-remuneradas", response_model=ListResponse)
def listar_personas_con_cargos_remunerados(
    params: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """Personas con al menos un cargo remunerado activo en el año."""
    return svc.listar_personas_con_cargos_remunerados(params)


@router.get("/personas-remuneradas/{per_id}/cargos", response_model=ListResponse)
def listar_cargos_de_persona(
    per_id: int,
    params: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """Detalle de cargos remunerados (y anomalías) de una persona."""
    return svc.listar_cargos_de_persona(per_id, params)
