"""Endpoints del bloque «Investigación»."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from coana.web.schemas.common import ListResponse, RecordResponse
from coana.web.services import investigación as svc
from coana.web.services.query import QueryParams, query_dependency

router = APIRouter()


@router.get("/grupos", response_model=ListResponse)
def grupos(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    """Listado de grupos de investigación activos en el año."""
    return svc.listar_grupos(p)


@router.get("/grupos/{id_grupo}/personas", response_model=ListResponse)
def personas_de_grupo(
    id_grupo: str, p: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """Personas activas de un grupo, ordenadas: interlocutor primero,
    luego coordinadores no interlocutores, luego resto."""
    return svc.listar_personas_de_grupo(id_grupo, p)


@router.get("/horas-kalendas", response_model=ListResponse)
def horas_kalendas(p: QueryParams = Depends(query_dependency)) -> ListResponse:
    """Suma de horas declaradas en Kalendas por (per_id, contrato)."""
    return svc.listar_horas_kalendas(p)


@router.get("/horas-kalendas/{clave}", response_model=RecordResponse)
def horas_kalendas_detalle(clave: str) -> RecordResponse:
    """Ficha con todo el detalle de una fila de Horas Kalendas. La clave es
    #val("per_id:contrato")."""
    try:
        per_s, contr_s = clave.split(":", 1)
        per_id, contrato = int(per_s), int(contr_s)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail=f"Clave no válida: {clave!r}")
    rec = svc.detalle_horas_kalendas(per_id, contrato)
    if rec is None:
        raise HTTPException(
            status_code=404,
            detail=f"Sin detalle para per_id={per_id}, contrato={contrato}",
        )
    return rec
