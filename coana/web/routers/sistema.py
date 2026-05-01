"""Endpoints de sistema: salud, info, ejecución de fase 1 (futuro)."""

from __future__ import annotations

from importlib.metadata import version as _pkg_version
from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel

from coana.web.deps import DIR_ENTRADA, DIR_FASE1

router = APIRouter()


class Health(BaseModel):
    status: str
    version: str
    entrada_existe: bool
    fase1_existe: bool


@router.get("/health", response_model=Health)
def health() -> Health:
    """Ping simple: devuelve versión del paquete y existencia de directorios."""
    return Health(
        status="ok",
        version=_pkg_version("coana"),
        entrada_existe=DIR_ENTRADA.is_dir(),
        fase1_existe=DIR_FASE1.is_dir(),
    )
