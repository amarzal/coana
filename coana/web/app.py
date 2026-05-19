"""Punto de entrada de la app FastAPI."""

from __future__ import annotations

from importlib.metadata import version as _pkg_version
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from coana.web.routers import (
    amortizaciones,
    cargos,
    entradas,
    lookups,
    persona_360,
    personal,
    presupuesto,
    regla23,
    resultados,
    sistema,
    superficies,
)
from coana.web.routers import investigación as investigacion_router

app = FastAPI(
    title="CoAna — gemelo web",
    version=_pkg_version("coana"),
    description=(
        "API que sirve los datos del visor de CoAna a un frontend "
        "Vite/React. Convive con la versión Streamlit hasta paridad."
    ),
)

# CORS solo para desarrollo: el frontend Vite corre en :5173 y proxia
# /api a este backend. En producción ambos comparten origen.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(sistema.router, prefix="/api/sistema", tags=["sistema"])
app.include_router(presupuesto.router, prefix="/api/presupuesto", tags=["presupuesto"])
app.include_router(cargos.router, prefix="/api/cargos", tags=["cargos académicos"])
app.include_router(superficies.router, prefix="/api/superficies", tags=["superficies"])
app.include_router(entradas.router, prefix="/api/entradas", tags=["entradas"])
app.include_router(amortizaciones.router, prefix="/api/amortizaciones", tags=["amortizaciones"])
app.include_router(personal.router, prefix="/api/personal", tags=["personal"])
app.include_router(persona_360.router, prefix="/api/persona360", tags=["persona-360"])
app.include_router(regla23.router, prefix="/api/regla23", tags=["regla 23"])
app.include_router(investigacion_router.router, prefix="/api/investigacion", tags=["investigación"])
app.include_router(resultados.router, prefix="/api/resultados", tags=["resultados"])
app.include_router(lookups.router, prefix="/api/lookups", tags=["lookups"])


# Frontend estático: sirve coana/web/dist/ si existe (build de Vite).
# Para que un SPA con rutas client-side funcione (deep links del estilo
# /presupuesto/uc), montamos los assets bajo /assets y servimos index.html
# como _catch-all_ para cualquier otra ruta no-API.
_DIST = Path(__file__).parent / "dist"
if _DIST.is_dir():
    app.mount(
        "/assets",
        StaticFiles(directory=str(_DIST / "assets")),
        name="assets",
    )

    _INDEX = _DIST / "index.html"

    @app.get("/{full_path:path}", include_in_schema=False)
    def _spa_catch_all(full_path: str) -> FileResponse:
        # Reservar /api y /events; FastAPI solo llega aquí si ningún
        # router los ha capturado, pero por defensa devolvemos 404.
        if full_path.startswith(("api/", "events/", "openapi", "docs", "redoc")):
            raise HTTPException(status_code=404)
        # Si la ruta apunta a un fichero específico de dist (favicon, etc.),
        # servirlo directamente.
        candidate = _DIST / full_path
        if full_path and candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(_INDEX)
