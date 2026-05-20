"""Compila el documento de informes Typst a PDF.

Ejecuta antes ``uv run coana informes`` para regenerar los datos.
"""

import subprocess
import sys
from pathlib import Path


def generar(
    ruta_typ: Path = Path("documentación/informes/informes.typ"),
) -> None:
    if not ruta_typ.exists():
        print(f"No se encuentra {ruta_typ}", file=sys.stderr)
        sys.exit(1)

    try:
        # `--root .` para que Typst pueda leer ficheros fuera del
        # directorio del .typ (en particular `data/informes/*.yaml`).
        subprocess.run(
            ["typst", "compile", "--root", ".", str(ruta_typ)],
            check=True, timeout=60,
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
