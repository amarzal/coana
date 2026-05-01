"""Punto de entrada del comando ``visor``.

Arranca uvicorn en primer plano sirviendo el backend FastAPI y el
frontend ya compilado, y abre el navegador en la URL local. Diseñado
para ser un único proceso autocontenido, de modo que matar la orden
(Ctrl+C, cierre de terminal, ``kill``) baja todo. No spawnea
subprocesos, así que no quedan huérfanos en macOS, Linux ni Windows.

Para usos avanzados (modo dev con Vite aparte, ``--reload``, etc.) está
``coana web`` en :mod:`coana.cli`. Este launcher es deliberadamente
simple.
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
import webbrowser
from pathlib import Path

import uvicorn

DIST = Path(__file__).parent / "dist"
INDEX = DIST / "index.html"


def _open_browser_after(url: str, delay: float = 0.7) -> None:
    """Hilo daemon: espera ``delay`` y abre la URL en el navegador.

    Daemon → muere con el principal. ``webbrowser.open`` no retiene
    handles del proceso del navegador; el SO se encarga del resto.
    """
    def _run() -> None:
        time.sleep(delay)
        try:
            webbrowser.open(url)
        except Exception:
            # No bloqueamos el arranque del visor por un fallo abriendo navegador.
            pass

    threading.Thread(target=_run, daemon=True).start()


def _comprobar_dist() -> bool:
    """Devuelve True si el bundle del frontend está construido."""
    return INDEX.is_file()


def _mensaje_falta_dist() -> str:
    return (
        "No se encuentra el frontend compilado en\n"
        f"  {DIST}\n\n"
        "Para construirlo, desde la raíz del repo:\n\n"
        "  cd web/frontend\n"
        "  npm install      # solo la primera vez\n"
        "  npm run build\n"
        "  cd ../..\n"
        "  uv run visor\n"
    )


def run(
    host: str = "127.0.0.1",
    port: int = 8765,
    no_browser: bool = False,
) -> int:
    """Arranca el visor. Devuelve el código de salida.

    Bloquea hasta que uvicorn termina (Ctrl+C → ``KeyboardInterrupt``
    capturado internamente por uvicorn → cierre limpio).
    """
    if not _comprobar_dist():
        sys.stderr.write(_mensaje_falta_dist())
        return 1

    url = f"http://{host}:{port}"
    if not no_browser:
        _open_browser_after(url)

    print(f"Visor de CoAna en {url}  (Ctrl+C para detener)")

    try:
        uvicorn.run(
            "coana.web.app:app",
            host=host,
            port=port,
            log_level="info",
            timeout_graceful_shutdown=3,
        )
    except KeyboardInterrupt:
        # uvicorn lo gestiona, pero por si llega hasta aquí.
        pass
    return 0


def main() -> None:
    """Entry point del script ``visor`` (ver pyproject.toml)."""
    parser = argparse.ArgumentParser(
        prog="visor",
        description="Lanza el visor web de CoAna (FastAPI + frontend).",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Interfaz a escuchar")
    parser.add_argument("--port", type=int, default=8765, help="Puerto a escuchar")
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="No abrir el navegador automáticamente",
    )
    args = parser.parse_args()
    sys.exit(run(host=args.host, port=args.port, no_browser=args.no_browser))


if __name__ == "__main__":
    main()
