"""Router del bloque Investigación."""

from fastapi import APIRouter, Depends

from coana.web.services import investigacion
from coana.web.services.query import QueryParams, query_dependency

router = APIRouter()


@router.get("/_resumen")
def get_resumen():
    """KPIs generales de dedicación a investigación."""
    return investigacion.resumen()


@router.get("/personas")
def listar_personas(params: QueryParams = Depends(query_dependency)):
    """Lista todas las personas con dedicación a investigación."""
    return investigacion.listar_personas_investigacion(params)


@router.get("/personas/{per_id}")
def obtener_persona(per_id: int):
    """Ficha de una persona con totales agregados."""
    result = investigacion.obtener_persona_investigacion(per_id)
    if result is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Persona no encontrada")
    return result


@router.get("/personas/{per_id}/detalle")
def listar_detalle(per_id: int, params: QueryParams = Depends(query_dependency)):
    """Detalle de registros de dedicación de una persona específica."""
    return investigacion.listar_detalle_persona(per_id, params)
