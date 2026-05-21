"""Endpoints del bloque «Entradas»."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from coana.web.schemas.common import ListResponse
from coana.web.services import entradas as svc
from coana.web.services.query import QueryParams, query_dependency

router = APIRouter()


@router.get("/", response_model=svc.CatalogoEntradas)
def catalogo() -> svc.CatalogoEntradas:
    """Listado de subdirectorios y ficheros de data/entrada/."""
    return svc.listar_entradas()


@router.get("/xlsx", response_model=ListResponse)
def leer_xlsx(
    ruta: str = Query(..., description="Ruta relativa a data/entrada/"),
    params: QueryParams = Depends(query_dependency),
) -> ListResponse:
    """Devuelve un Excel paginado con filtro/sort comunes."""
    res = svc.listar_xlsx(ruta, params)
    if res is None:
        raise HTTPException(status_code=404, detail=f"Excel no encontrado: {ruta!r}")
    return res


@router.get("/tree", response_model=svc.NodoTree)
def leer_tree(
    ruta: str = Query(..., description="Ruta relativa a data/entrada/"),
) -> svc.NodoTree:
    """Devuelve un árbol .tree serializado como JSON jerárquico."""
    res = svc.cargar_tree(ruta)
    if res is None:
        raise HTTPException(status_code=404, detail=f"Árbol no encontrado: {ruta!r}")
    return res


@router.get("/ficha")
def ficha_fichero(
    ruta: str = Query(..., description="Ruta relativa a data/entrada/ (o nombre del fichero)"),
) -> dict:
    """Devuelve la ficha descriptiva del fichero (descripción + descripción
    por campo) extraída de la especificación.

    Si no hay ficha para ese fichero, devuelve `{descripción: "", campos: {}}`.
    """
    from coana.util.diccionario_ficheros import ficha_para
    from pathlib import Path

    nombre = Path(ruta).name
    ficha = ficha_para(nombre)
    return ficha or {"descripción": "", "campos": {}}
