"""Interfaz CLI de CoAna (Typer)."""

import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path

import typer

app = typer.Typer(
    name="coana",
    help="CoAna – Contabilidad Analítica Universitaria",
)


@app.callback(invoke_without_command=True)
def callback(ctx: typer.Context):
    """CoAna – Contabilidad Analítica Universitaria."""
    if ctx.invoked_subcommand is None:
        app_path = Path(__file__).parent / "apps" / "visor_entradas.py"
        subprocess.run([sys.executable, "-m", "streamlit", "run", str(app_path)], check=True)


@app.command()
def version():
    """Muestra la versión de CoAna."""
    from importlib.metadata import version as pkg_version

    typer.echo(f"coana {pkg_version('coana')}")


@app.command()
def editor_tree(
    ruta_base: Path = typer.Option(Path("data"), help="Ruta base de datos"),
):
    """Abre el editor gráfico de ficheros .tree."""
    from coana.apps.editor_tree import EditorTree

    EditorTree(ruta_base).mainloop()


@app.command()
def web(
    host: str = typer.Option("127.0.0.1", help="Interfaz en la que escuchar"),
    port: int = typer.Option(8765, help="Puerto en el que escuchar"),
    reload: bool = typer.Option(False, help="Recarga automática del backend (uvicorn --reload)"),
    no_browser: bool = typer.Option(False, help="No abrir el navegador automáticamente"),
    dev: bool = typer.Option(
        False,
        help=(
            "Modo desarrollo: solo arranca el backend. El frontend Vite "
            "se lanza aparte con `cd web/frontend && npm run dev`."
        ),
    ),
):
    """Lanza el gemelo web (FastAPI + frontend compilado)."""
    import uvicorn

    if not no_browser and not dev:
        url = f"http://{host}:{port}"

        def _open_later() -> None:
            time.sleep(0.7)
            webbrowser.open(url)

        threading.Thread(target=_open_later, daemon=True).start()

    uvicorn.run(
        "coana.web.app:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info",
    )


def main():
    app()
