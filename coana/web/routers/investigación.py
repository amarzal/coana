"""Endpoints del bloque «Investigación»."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from coana.web.schemas.common import ListResponse
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
