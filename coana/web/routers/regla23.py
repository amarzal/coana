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


# ---- Dedicación PDI (regla 23 reescrita) ----------------------------

@router.get("/dedicacion-pdi/personas", response_model=ListResponse)
def dedicacion_pdi_personas(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    """Lista de personas con horas registradas (master)."""
    return svc.listar_personas_dedicación(p)


@router.get("/dedicacion-pdi/{per_id}", response_model=ListResponse)
def dedicacion_pdi_detalle(
    per_id: int,
    p: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """Detalle de actividades en las que una persona acumula horas."""
    return svc.listar_dedicación_persona(per_id, p)


@router.get("/dedicacion-pdi/{per_id}/resumen", response_model=ListResponse)
def dedicacion_pdi_resumen(per_id: int) -> ListResponse:
    """Resumen por grupo (docencia/investigación/gestión/extensión + HND)."""
    return svc.listar_resumen_grupo_persona(per_id)


@router.get("/dedicacion-pdi/{per_id}/laboral", response_model=ListResponse)
def dedicacion_pdi_laboral(per_id: int) -> ListResponse:
    """Histórico de relación laboral (categoría, sector, meses) observado en nómina."""
    return svc.listar_relación_laboral_persona(per_id)


@router.get("/dedicacion-pdi/{per_id}/totales", response_model=ListResponse)
def dedicacion_pdi_totales(per_id: int) -> ListResponse:
    """Totales por (actividad, centro de coste) tras el reparto."""
    return svc.listar_totales_actividad_centro(per_id)


@router.get("/dedicacion-pdi/{per_id}/uc-reparto", response_model=ListResponse)
def dedicacion_pdi_uc_reparto(
    per_id: int,
    p: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """UC por reparto de la masa regla 23 de la persona."""
    return svc.listar_uc_reparto_persona(per_id, p)
