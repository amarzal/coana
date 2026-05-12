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


@router.get("/uc")
def listar_uc(params: QueryParams = Depends(query_dependency)):
    """Distribución porcentual por (per_id, actividad). Esta anualidad
    solo se calcula porcentaje, sin importe en euros (entrará al
    integrar la regla 23)."""
    return investigacion.listar_uc_investigacion(params)


@router.get("/uc/{per_id}/{actividad}/detalle")
def listar_uc_detalle(
    per_id: int, actividad: str,
    params: QueryParams = Depends(query_dependency),
):
    """Registros de detalle que contribuyen a una (per_id, actividad)
    concreta — para el drill-down de la pestaña UC."""
    return investigacion.listar_detalle_uc_actividad(per_id, actividad, params)
