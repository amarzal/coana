"""Cuadro 10.3 — Informe general de ingresos por actividades.

La estructura jerárquica está fijada en `_PLANTILLA` siguiendo el PDF
oficial. Algunos niveles del PDF son dinámicos según los datos del año
(ámbitos de conocimiento, grados, másteres, programas de doctorado);
esos no aparecen aquí — se enumerarán al cargar los datos reales de
ingresos.

Mientras los ingresos no se procesen en la Fase 1, todos los importes
son 0 y el informe sirve solo de plantilla.
"""

from __future__ import annotations

from pathlib import Path

from coana.fase2._cuadro_jerarquico import (
    persistir_xlsx_jerárquico, persistir_yaml,
)

# (código_SUE, nombre_oficial, nivel)
_PLANTILLA: list[tuple[str, str, int]] = [
    ("01",             "Ingresos relacionados con actividades", 1),
    ("01.01",          "Ingresos por docencia", 2),
    ("01.01.01",       "Ingresos por estudios oficiales", 3),
    ("01.01.02",       "Ingresos por estudios propios de la universidad", 3),
    ("01.01.02.01",    "Grados propios", 4),
    ("01.01.02.02",    "Másteres de Formación Permanente", 4),
    ("01.01.02.03",    "Diplomas de especialización", 4),
    ("01.01.02.04",    "Diplomas de experto/a", 4),
    ("01.01.02.05",    "Cursos", 4),
    ("01.01.02.06",    "Microcredenciales", 4),
    ("01.01.02.07",    "Cursos de idiomas", 4),
    ("01.01.02.08",    "Cursos para extranjeros", 4),
    ("01.01.02.09",    "Acceso a enseñanzas oficiales", 4),
    ("01.01.02.10",    "Otras actividades de docencia", 4),
    ("01.01.02.11",    "Ingresos procedentes de centros adscritos", 4),
    ("01.02",          "Ingresos por actividades de investigación", 2),
    ("01.02.01",       "Ingresos por actividades de investigación y transferencia con financiación externa", 3),
    ("01.02.01.01",    "Ingresos por actividades financiadas por agentes externos en convocatorias competitivas", 4),
    ("01.02.01.01.01", "Ingresos por actividades con financiación de programas regionales", 5),
    ("01.02.01.01.02", "Ingresos por actividades con financiación de programas nacionales", 5),
    ("01.02.01.01.03", "Ingresos por actividades con financiación de programas internacionales", 5),
    ("01.02.01.01.04", "Otras actividades de investigación en convocatorias competitivas", 5),
    ("01.02.01.02",    "Ingresos por actividades de investigación aplicada y transferencia", 4),
    ("01.02.02",       "Ingresos por Doctorado", 3),
    ("01.03",          "Ingresos por actividades deportivas y de extensión universitaria", 2),
    ("01.04",          "Ingresos por servicios comunes de investigación", 2),
    ("01.05",          "Ingresos por publicaciones", 2),
    ("01.06",          "Ingresos por otras actividades", 2),
    ("01.07",          "TRUPI con ingresos calculados", 2),

    ("02",             "Ingresos relacionados con la organización en su conjunto", 1),
    ("02.01",          "Ingresos por uso de instalaciones universitarias no relacionadas con actividades", 2),
    ("02.02",          "Ingresos patrimoniales", 2),
    ("02.03",          "Tasas administrativas no relacionadas con actividades", 2),
    ("02.04",          "Otros ingresos no relacionados con actividades", 2),
]


def generar_cuadro_10_3(ruta_base: Path, dir_informes: Path) -> None:
    """Genera ``cuadro_10_3.yaml`` con la estructura. Importes a 0 mientras
    no se procesen los ingresos en Fase 1."""
    filas = [
        {
            "código": cód,
            "nombre": nombre,
            "nivel": nivel,
            "importe": 0.0,
            "pct_elemento": None,
            "pct_total": None,
        }
        for cód, nombre, nivel in _PLANTILLA
    ]
    datos = {
        "id": "cuadro_10_3",
        "título": "Informe de ingresos por actividades",
        "encabezado_concepto": "Elemento de ingreso por actividad",
        "estado": "pendiente — ingresos no procesados en Fase 1",
        "total": 0.0,
        "filas": filas,
    }
    persistir_yaml(dir_informes / "cuadro_10_3.yaml", datos)
    persistir_xlsx_jerárquico(dir_informes / "cuadro_10_3.xlsx", datos)
    print(f"  cuadro_10_3: plantilla con {len(filas)} filas (sin datos de ingresos).")
