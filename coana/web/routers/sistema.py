"""Endpoints de sistema: salud, info, ejecución de fase 1."""

from __future__ import annotations

import asyncio
import json
from importlib.metadata import version as _pkg_version

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from coana.web import streaming
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


# ----------------------------------------------------------------------
# Fase 1: lanzar, consultar estado, stream de log en vivo
# ----------------------------------------------------------------------

class JobInfo(BaseModel):
    id: str
    status: streaming.JobStatus
    started_at: float
    finished_at: float | None = None
    n_lines: int
    error: str | None = None


def _to_info(job: streaming.Job) -> JobInfo:
    return JobInfo(
        id=job.id,
        status=job.status,
        started_at=job.started_at,
        finished_at=job.finished_at,
        n_lines=len(job.lines),
        error=job.error,
    )


@router.post("/fase1/run", response_model=JobInfo)
def run_fase1() -> JobInfo:
    """Lanza una nueva ejecución de Fase 1 (en segundo plano)."""
    job = streaming.start_fase1()
    if job is None:
        # Ya hay una en curso; devolvemos su info en lugar de fallar.
        actual = streaming.get_current()
        if actual is not None:
            raise HTTPException(
                status_code=409,
                detail=f"Ya hay una Fase 1 en ejecución (job {actual.id})",
            )
        raise HTTPException(status_code=409, detail="No se pudo arrancar el job")
    return _to_info(job)


@router.get("/fase1/current", response_model=JobInfo | None)
def fase1_current() -> JobInfo | None:
    """Job en curso o último ejecutado, si lo hay."""
    job = streaming.get_current()
    return _to_info(job) if job is not None else None


@router.get("/fase1/{job_id}", response_model=JobInfo)
def fase1_status(job_id: str) -> JobInfo:
    job = streaming.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} no encontrado")
    return _to_info(job)


@router.get("/fase1/{job_id}/stream")
async def fase1_stream(job_id: str) -> StreamingResponse:
    """SSE con eventos `log` por cada línea, `done` o `error` al terminar.

    El cliente puede reabrir esta ruta y verá todas las líneas acumuladas
    desde el principio (la lista interna del job es persistente mientras
    la app esté viva).
    """
    job = streaming.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} no encontrado")

    async def gen():
        cursor = 0
        # Primer flush: enviar todo lo acumulado de inmediato.
        while True:
            j = streaming.get_job(job_id)
            if j is None:  # no debería pasar
                yield "event: error\ndata: {}\n\n"
                return
            # Líneas nuevas
            n = len(j.lines)
            while cursor < n:
                line = j.lines[cursor]
                cursor += 1
                yield f"event: log\ndata: {json.dumps(line, ensure_ascii=False)}\n\n"
            if j.status == "done":
                yield f"event: done\ndata: {json.dumps({'job_id': j.id}, ensure_ascii=False)}\n\n"
                return
            if j.status == "error":
                yield (
                    "event: error\n"
                    f"data: {json.dumps({'job_id': j.id, 'error': j.error}, ensure_ascii=False)}\n\n"
                )
                return
            await asyncio.sleep(0.2)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            # Evita buffering en proxies / nginx; clave para que las
            # líneas lleguen en tiempo real.
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
