"""Endpoints del bloque «Resultados Fase 1»."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from coana.web.schemas.common import KpiPanel, ListResponse, RecordResponse
from coana.web.services import entradas as svc_entradas
from coana.web.services import resultados as svc
from coana.web.services.query import QueryParams, query_dependency

router = APIRouter()


@router.get("/_resumen", response_model=KpiPanel)
def resumen() -> KpiPanel:
    return svc.resumen()


@router.get("/uc", response_model=ListResponse)
def todas_uc(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_todas(p)


@router.get("/uc/{origen}/{uc_id}", response_model=RecordResponse)
def obtener_uc(origen: str, uc_id: str) -> RecordResponse:
    """Ficha de una UC concreta + sección con el registro original (apunte
    presupuestario, bien inventariable, línea de nómina…)."""
    rec = svc.obtener_uc(origen, uc_id)
    if rec is None:
        raise HTTPException(
            status_code=404,
            detail=f"UC no encontrada: origen={origen!r}, id={uc_id!r}",
        )
    return rec


@router.get("/actividades", response_model=ListResponse)
def actividades(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_actividades(p)


@router.get("/centros-de-coste", response_model=ListResponse)
def centros(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_centros(p)


@router.get("/elementos-de-coste", response_model=ListResponse)
def elementos(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_elementos(p)


@router.get("/anomalias", response_model=ListResponse)
def anomalias(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_anomalias(p)


@router.get("/arbol/{nombre}", response_model=svc_entradas.NodoTree)
def arbol_final(nombre: str) -> svc_entradas.NodoTree:
    """Árbol final tras la fase 1: actividades, centros-de-coste, elementos-de-coste."""
    res = svc.cargar_arbol_final(nombre)
    if res is None:
        raise HTTPException(
            status_code=404,
            detail=f"Árbol no encontrado: {nombre!r}",
        )
    return res
