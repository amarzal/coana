"""Interfaz CLI de CoAna (Typer)."""

import subprocess
import sys
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


def main():
    app()
