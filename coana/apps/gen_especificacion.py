"""Compila la especificación Typst a PDF.

La especificación se escribe directamente en Typst en
``documentación/especificación.typ``.  Este script simplemente invoca
``typst compile`` para generar el PDF correspondiente.
"""

import subprocess
import sys
from pathlib import Path


def generar(
    ruta_typ: Path = Path("documentación/especificación.typ"),
) -> None:
    """Compila especificación.typ → especificación.pdf con typst."""
    if not ruta_typ.exists():
        print(f"No se encuentra {ruta_typ}", file=sys.stderr)
        sys.exit(1)

    try:
        subprocess.run(
            ["typst", "compile", str(ruta_typ)],
            check=True,
            timeout=60,
        )
        pdf = ruta_typ.with_suffix(".pdf")
        print(f"Compilado {pdf}")
    except FileNotFoundError:
        print("Error: typst no encontrado en el PATH", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"Error al compilar con typst: {e}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    generar()


if __name__ == "__main__":
    main()
