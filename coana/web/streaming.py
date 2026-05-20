"""Job manager para la ejecución de Fase 1 con log en vivo.

La fase 1 lleva decenas de segundos y queremos verla progresar. El
patrón es:

1. ``POST /api/sistema/fase1/run`` arranca un job (uno único concurrente).
2. ``GET /api/sistema/fase1/{id}/stream`` abre un Server-Sent Events
   que emite las líneas de stdout que va produciendo la fase 1, más un
   evento final ``done`` o ``error``.
3. Si el navegador se cierra y se reabre, el listado de líneas
   acumuladas en el job permite reanudar el render.

El job corre en un hilo. La interacción con FastAPI (async) se hace
via lectura no-bloqueante de la lista compartida ``job.lines`` (la GIL
nos da las garantías necesarias para acceso simple a una lista).
"""

from __future__ import annotations

import contextlib
import io
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Literal


JobStatus = Literal["running", "done", "error"]


@dataclass
class Job:
    """Estado de una ejecución (Fase 1 o informes)."""

    id: str
    status: JobStatus = "running"
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    lines: list[str] = field(default_factory=list)
    error: str | None = None
    kind: str = "fase1"


class _LineWriter(io.TextIOBase):
    """File-like que parte el texto en líneas y lo añade a un Job."""

    def __init__(self, job: Job) -> None:
        super().__init__()
        self.job = job
        self._buffer = ""

    def writable(self) -> bool:  # type: ignore[override]
        return True

    def write(self, s: str) -> int:  # type: ignore[override]
        self._buffer += s
        while "\n" in self._buffer:
            line, _, rest = self._buffer.partition("\n")
            self._buffer = rest
            if line.strip():
                self.job.lines.append(line)
        return len(s)

    def flush(self) -> None:  # type: ignore[override]
        if self._buffer.strip():
            self.job.lines.append(self._buffer)
        self._buffer = ""


_LOCK = threading.Lock()
_JOBS: dict[str, Job] = {}
_CURRENT_ID: str | None = None  # solo un job concurrente


def get_job(job_id: str) -> Job | None:
    return _JOBS.get(job_id)


def get_current() -> Job | None:
    if _CURRENT_ID is None:
        return None
    return _JOBS.get(_CURRENT_ID)


def is_running() -> bool:
    job = get_current()
    return job is not None and job.status == "running"


def start_fase1() -> Job | None:
    """Arranca un nuevo job de Fase 1. Devuelve None si ya hay uno en curso."""
    global _CURRENT_ID
    with _LOCK:
        if is_running():
            return None
        job = Job(id=uuid.uuid4().hex[:8], kind="fase1")
        _JOBS[job.id] = job
        _CURRENT_ID = job.id
    threading.Thread(target=_run_job, args=(job,), daemon=True).start()
    return job


def start_informes() -> Job | None:
    """Arranca un nuevo job de generación de informes (Fase 2 + Typst →
    PDF → abrir en visor externo). Devuelve None si ya hay uno en curso."""
    global _CURRENT_ID
    with _LOCK:
        if is_running():
            return None
        job = Job(id=uuid.uuid4().hex[:8], kind="informes")
        _JOBS[job.id] = job
        _CURRENT_ID = job.id
    threading.Thread(target=_run_informes, args=(job,), daemon=True).start()
    return job


def _run_job(job: Job) -> None:
    try:
        # Importar tarde para no encarecer el arranque del visor.
        from coana.fase1 import ejecutar
        from coana.web.deps import clear_cache as clear_data_cache
        from coana.web.services.lookups import clear_cache as clear_lookups_cache

        job.lines.append(f"Lanzando cálculo de unidades de coste (job {job.id})…")
        writer = _LineWriter(job)
        with contextlib.redirect_stdout(writer):
            ejecutar()
        writer.flush()

        # Invalidar todas las cachés en memoria del backend para que las
        # vistas vean los parquets nuevos.
        clear_data_cache()
        clear_lookups_cache()
        _clear_module_caches()

        job.lines.append("Cálculo de unidades de coste completado con éxito.")
        job.status = "done"
    except Exception as exc:  # noqa: BLE001 - queremos cualquier error
        import traceback
        job.lines.append("ERROR durante el cálculo de unidades de coste:")
        job.lines.extend(traceback.format_exc().splitlines())
        job.error = str(exc)
        job.status = "error"
    finally:
        job.finished_at = time.time()


def _run_informes(job: Job) -> None:
    import subprocess
    import sys
    from pathlib import Path
    try:
        from coana.apps.gen_informes import generar as compilar_typst
        from coana.fase2 import ejecutar as ejecutar_fase2

        job.lines.append(f"Generando informes (job {job.id})…")
        writer = _LineWriter(job)
        with contextlib.redirect_stdout(writer):
            ejecutar_fase2()
            compilar_typst()
        writer.flush()

        pdf = Path("documentación/informes/informes.pdf").resolve()
        if not pdf.exists():
            raise FileNotFoundError(
                f"No se encontró {pdf} tras la compilación."
            )

        # Abrir en el visor de PDF del sistema.
        if sys.platform == "darwin":
            subprocess.Popen(["open", str(pdf)])
        elif sys.platform.startswith("linux"):
            subprocess.Popen(["xdg-open", str(pdf)])
        elif sys.platform == "win32":
            subprocess.Popen(["cmd", "/c", "start", "", str(pdf)])
        else:
            job.lines.append(
                f"PDF generado en {pdf}; no se pudo determinar visor en "
                f"plataforma {sys.platform}."
            )

        job.lines.append(f"Informes generados y abiertos: {pdf}")
        job.status = "done"
    except Exception as exc:  # noqa: BLE001
        import traceback
        job.lines.append("ERROR durante la generación de informes:")
        job.lines.extend(traceback.format_exc().splitlines())
        job.error = str(exc)
        job.status = "error"
    finally:
        job.finished_at = time.time()


def _clear_module_caches() -> None:
    """Vacía las lru_cache de servicios que cachean DataFrames por mtime."""
    # Cualquier módulo que tenga _read_excel_cached o similares.
    from coana.web.services import (
        amortizaciones,
        cargos,
        entradas,
        personal,
        presupuesto,
        regla23,
        resultados,
        superficies,
    )
    for mod in (amortizaciones, cargos, entradas, personal, presupuesto,
                regla23, resultados, superficies):
        for name in dir(mod):
            obj = getattr(mod, name)
            cache_clear = getattr(obj, "cache_clear", None)
            if callable(cache_clear):
                try:
                    cache_clear()
                except Exception:
                    pass
