"""Fase 2 — generación de informes a partir de las UC.

Cada informe produce tres artefactos:
- ``data/informes/<id>.yaml`` con los datos estructurados.
- ``data/informes/<id>.xlsx`` con la tabla en formato Calibri.
- Inclusión en ``documentación/informes/informes.typ`` que se compila a PDF.
"""

from __future__ import annotations

from pathlib import Path


def ejecutar(ruta_base: Path = Path("data")) -> None:
    """Genera todos los informes de la Fase 2."""
    from coana.fase2.cuadro_10_1 import generar_cuadro_10_1
    from coana.fase2.cuadro_10_3 import generar_cuadro_10_3
    from coana.fase2.cuadro_10_4 import generar_cuadro_10_4
    from coana.fase2.cuadro_10_5 import generar_cuadro_10_5
    from coana.fase2.cuadro_10_7 import generar_cuadro_10_7

    dir_informes = ruta_base / "informes"
    dir_informes.mkdir(parents=True, exist_ok=True)

    print("Fase 2 — generación de informes")
    generar_cuadro_10_1(ruta_base, dir_informes)
    generar_cuadro_10_3(ruta_base, dir_informes)
    generar_cuadro_10_4(ruta_base, dir_informes)
    generar_cuadro_10_5(ruta_base, dir_informes)
    generar_cuadro_10_7(ruta_base, dir_informes)
    print(f"Informes generados en {dir_informes}")
