"""Punto de entrada de la app FastAPI."""

from __future__ import annotations

from importlib.metadata import version as _pkg_version
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from coana.web.routers import sistema

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


# Frontend estático: sirve coana/web/dist/ si existe (build de Vite).
_DIST = Path(__file__).parent / "dist"
if _DIST.is_dir():
    app.mount("/", StaticFiles(directory=str(_DIST), html=True), name="frontend")
