"""Endpoints de la vista persona-360 (PDI y PVI).

Master por sector con cuadre cobrado / cotizado / UC; detalle con
pestañas que combinan información de los servicios de personal y
regla 23.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from coana.web.schemas.common import KpiPanel, ListResponse
from coana.web.services import persona_360 as svc
from coana.web.services import regla23 as svc_r23
from coana.web.services.query import QueryParams, query_dependency

router = APIRouter()


_SECTORES = ("PDI", "PVI")


def _check_sector(sector: str) -> None:
    if sector not in _SECTORES:
        raise HTTPException(
            status_code=404, detail=f"Sector no soportado: {sector!r}"
        )


@router.get("/{sector}/personas", response_model=ListResponse)
def personas_sector(
    sector: str, p: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """Master por persona del sector con cuadre cobrado/UC."""
    _check_sector(sector)
    return svc.listar_personas_sector(sector, p)


@router.get("/{sector}/personas/{per_id}/resumen", response_model=KpiPanel)
def persona_resumen(sector: str, per_id: int) -> KpiPanel:
    _check_sector(sector)
    return svc.resumen_persona(sector, per_id)


@router.get("/{sector}/personas/{per_id}/cuadre", response_model=ListResponse)
def persona_cuadre(sector: str, per_id: int) -> ListResponse:
    """Tabla detallada de cuadre por concepto."""
    _check_sector(sector)
    return svc.cuadre_persona(sector, per_id)


@router.get("/{sector}/personas/{per_id}/uc", response_model=ListResponse)
def persona_uc(
    sector: str, per_id: int, p: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """Lista completa de UC vinculadas a la persona."""
    _check_sector(sector)
    return svc.listar_uc_persona_completa(sector, per_id, p)


# ---- Aliases hacia endpoints ya existentes en regla23 (reutilizados) ----

@router.get("/{sector}/personas/{per_id}/laboral", response_model=ListResponse)
def persona_laboral(sector: str, per_id: int) -> ListResponse:
    _check_sector(sector)
    return svc_r23.listar_relación_laboral_persona(per_id)


@router.get("/{sector}/personas/{per_id}/regla23/resumen", response_model=ListResponse)
def persona_r23_resumen(sector: str, per_id: int) -> ListResponse:
    _check_sector(sector)
    return svc_r23.listar_resumen_grupo_persona(per_id)


@router.get("/{sector}/personas/{per_id}/regla23/totales", response_model=ListResponse)
def persona_r23_totales(sector: str, per_id: int) -> ListResponse:
    _check_sector(sector)
    return svc_r23.listar_totales_actividad_centro(per_id)


@router.get("/{sector}/personas/{per_id}/regla23/detalle", response_model=ListResponse)
def persona_r23_detalle(
    sector: str, per_id: int, p: QueryParams = Depends(query_dependency),
) -> ListResponse:
    _check_sector(sector)
    return svc_r23.listar_dedicación_persona(per_id, p)
