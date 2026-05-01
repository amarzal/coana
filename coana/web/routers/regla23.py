"""Endpoints del bloque «Regla 23»."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from coana.web.schemas.common import KpiPanel, ListResponse, RecordResponse
from coana.web.services import regla23 as svc
from coana.web.services.query import QueryParams, query_dependency

router = APIRouter()


@router.get("/_resumen", response_model=KpiPanel)
def resumen() -> KpiPanel:
    return svc.resumen()


# ---- Dedicación docente (3 vistas) ----------------------------------

@router.get("/dedicacion/asignaturas", response_model=ListResponse)
def dedicacion_asignaturas(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_dedicacion_asignaturas(p)


@router.get("/dedicacion/titulaciones", response_model=ListResponse)
def dedicacion_titulaciones(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_dedicacion_titulaciones(p)


@router.get("/dedicacion/estudios", response_model=ListResponse)
def dedicacion_estudios(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_dedicacion_estudios(p)


# ---- Otros listados -------------------------------------------------

@router.get("/horas-no-oficiales", response_model=ListResponse)
def horas(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_horas_no_oficiales(p)


@router.get("/estructura-estudios", response_model=ListResponse)
def estructura(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_estructura(p)


@router.get("/atrasos", response_model=ListResponse)
def atrasos(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_atrasos(p)


@router.get("/apartados", response_model=ListResponse)
def apartados(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_apartados(p)


# ---- UC especiales --------------------------------------------------

@router.get("/despidos", response_model=ListResponse)
def despidos(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_despidos(p)


@router.get("/despidos/{uc_id}", response_model=RecordResponse)
def obtener_despido(uc_id: str) -> RecordResponse:
    rec = svc.obtener_despido(uc_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"UC {uc_id!r} no encontrada")
    return rec


@router.get("/indemnizaciones", response_model=ListResponse)
def indemnizaciones(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_indemnizaciones(p)


@router.get("/indemnizaciones/{uc_id}", response_model=RecordResponse)
def obtener_indem(uc_id: str) -> RecordResponse:
    rec = svc.obtener_indemnizacion(uc_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"UC {uc_id!r} no encontrada")
    return rec


@router.get("/cargos", response_model=ListResponse)
def cargos(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_cargos(p)


@router.get("/cargos/{uc_id}", response_model=RecordResponse)
def obtener_cargo(uc_id: str) -> RecordResponse:
    rec = svc.obtener_cargo(uc_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"UC {uc_id!r} no encontrada")
    return rec


# ---- Anomalías ------------------------------------------------------

@router.get("/sin-titulacion", response_model=ListResponse)
def sin_titulacion(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_sin_titulacion(p)


@router.get("/anomalias/resolucion", response_model=ListResponse)
def anom_resolucion(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_anomalias_resolucion(p)


@router.get("/anomalias/multiples-grado", response_model=ListResponse)
def anom_multiples(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    return svc.listar_multiples_con_grado(p)
