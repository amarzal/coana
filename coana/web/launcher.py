"""Punto de entrada del comando ``visor``.

Arranca uvicorn en primer plano sirviendo el backend FastAPI y el
frontend ya compilado, y abre el navegador en la URL local. Diseñado
para ser un único proceso autocontenido, de modo que matar la orden
(Ctrl+C, cierre de terminal, ``kill``) baja todo. No spawnea
subprocesos *durante* la ejecución; solo (opcionalmente) antes de
arrancar para reconstruir el frontend si está desactualizado.

Para usos avanzados (modo dev con Vite aparte, ``--reload``, etc.) está
``coana web`` en :mod:`coana.cli`.
"""

from __future__ import annotations

import argparse
import platform
import shutil
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path

import uvicorn

DIST = Path(__file__).parent / "dist"
INDEX = DIST / "index.html"

# Directorio del proyecto del frontend (web/frontend/) — solo existe en
# instalaciones desde código fuente. En un wheel instalado, no hay
# fuente y dist se sirve desde package_data.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_FRONTEND = _PROJECT_ROOT / "web" / "frontend"
_FRONTEND_SRC = _FRONTEND / "src"
_FRONTEND_NODE_MODULES = _FRONTEND / "node_modules"


# ----------------------------------------------------------------------
# Apertura de navegador
# ----------------------------------------------------------------------

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


# ----------------------------------------------------------------------
# Detección y reconstrucción del frontend
# ----------------------------------------------------------------------

def _hay_fuente_frontend() -> bool:
    """¿Estamos en una instalación de código fuente con web/frontend/?"""
    return _FRONTEND_SRC.is_dir() and (_FRONTEND / "package.json").is_file()


def _necesita_rebuild() -> bool:
    """Devuelve True si dist/ falta o está desactualizado vs los fuentes.

    Compara el mtime de ``index.html`` con el del fichero más reciente
    bajo ``src/`` y los ficheros de configuración del frontend. Si
    cualquier fuente es más nueva que el bundle, hay que reconstruir.
    """
    if not INDEX.is_file():
        return True
    if not _hay_fuente_frontend():
        # Instalación desde wheel: dist/ viene en el paquete; no podemos
        # comprobar y tampoco reconstruir aunque quisiéramos.
        return False

    dist_mtime = INDEX.stat().st_mtime

    # Cualquier fichero bajo src/ más nuevo que el bundle.
    for p in _FRONTEND_SRC.rglob("*"):
        if p.is_file() and p.stat().st_mtime > dist_mtime:
            return True

    # Ficheros de configuración que afectan al build.
    for nombre in ("package.json", "package-lock.json", "vite.config.ts",
                   "tsconfig.json", "tsconfig.app.json", "tsconfig.node.json",
                   "index.html"):
        f = _FRONTEND / nombre
        if f.is_file() and f.stat().st_mtime > dist_mtime:
            return True

    return False


def _resolver_npm() -> str | None:
    """Encuentra el ejecutable de npm cross-platform o None si no está."""
    return (
        shutil.which("npm")
        or (shutil.which("npm.cmd") if platform.system() == "Windows" else None)
    )


def _ejecutar_npm(args: list[str], npm: str) -> bool:
    """Ejecuta ``npm <args>`` en el directorio del frontend.

    En Windows, los entry points de npm son ficheros ``.cmd``; pasamos
    ``shell=True`` para que ``cmd.exe`` los interprete correctamente.
    """
    cmd_str = "npm " + " ".join(args)
    print(f"  → {cmd_str}")
    kwargs: dict = {"cwd": str(_FRONTEND)}
    if platform.system() == "Windows":
        kwargs["shell"] = True
        cmd = cmd_str
    else:
        cmd = [npm, *args]
    try:
        result = subprocess.run(cmd, **kwargs)
    except FileNotFoundError:
        return False
    return result.returncode == 0


def _intentar_rebuild() -> bool:
    """Reconstruye el frontend si npm está disponible. Devuelve True si éxito."""
    npm = _resolver_npm()
    if npm is None:
        sys.stderr.write(
            "El frontend está desactualizado pero no se encuentra `npm` en el PATH.\n"
            "Instala Node.js (https://nodejs.org) o reconstruye manualmente:\n\n"
            "  cd web/frontend\n"
            "  npm install      # solo la primera vez\n"
            "  npm run build\n\n"
            "O ejecuta `uv run visor --no-rebuild` para saltarte la comprobación.\n"
        )
        return False

    print("Reconstruyendo el frontend (puede tardar unos segundos)…")

    if not _FRONTEND_NODE_MODULES.is_dir():
        if not _ejecutar_npm(["install"], npm):
            sys.stderr.write("Falló `npm install`.\n")
            return False

    if not _ejecutar_npm(["run", "build"], npm):
        sys.stderr.write("Falló `npm run build`.\n")
        return False

    print("Frontend reconstruido.")
    return True


# ----------------------------------------------------------------------
# Arranque
# ----------------------------------------------------------------------

def run(
    host: str = "127.0.0.1",
    port: int = 8765,
    no_browser: bool = False,
    no_rebuild: bool = False,
) -> int:
    """Arranca el visor. Devuelve el código de salida.

    Bloquea hasta que uvicorn termina (Ctrl+C → ``KeyboardInterrupt``
    capturado internamente por uvicorn → cierre limpio).
    """
    if not no_rebuild and _necesita_rebuild():
        if not _intentar_rebuild():
            return 1

    if not INDEX.is_file():
        sys.stderr.write(
            "No se encuentra el frontend compilado en\n"
            f"  {DIST}\n\n"
            "Si tienes Node.js instalado, ejecuta sin `--no-rebuild`. "
            "Si no, constrúyelo manualmente:\n\n"
            "  cd web/frontend\n"
            "  npm install      # solo la primera vez\n"
            "  npm run build\n"
            "  cd ../..\n"
            "  uv run visor\n"
        )
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
    parser.add_argument(
        "--no-rebuild",
        action="store_true",
        help="No comprobar ni reconstruir el frontend antes de arrancar",
    )
    args = parser.parse_args()
    sys.exit(run(
        host=args.host, port=args.port,
        no_browser=args.no_browser, no_rebuild=args.no_rebuild,
    ))


if __name__ == "__main__":
    main()
