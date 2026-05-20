"""Cuadro 10.1 — Informe de elementos de coste.

Plantilla SUE oficial. Para cada slug del árbol de elementos de coste
calcula su ``A+B`` (suma del subárbol) y los porcentajes asociados.
"""

from __future__ import annotations

from pathlib import Path

from coana.fase2._cuadro_jerarquico import generar_cuadro_jerárquico
from coana.fase2.calculo import cargar_árbol_elementos_de_coste, cargar_ucs

# (código_SUE, slug_árbol, nombre_oficial, nivel)
_PLANTILLA: list[tuple[str, str, str, int]] = [
    ("01",    "costes-personal",          "Costes de personal", 1),
    ("01.01", "salario-pdi",              "Sueldos y salarios del Personal Docente e Investigador (PDI)", 2),
    ("01.02", "salario-ptgas",            "Sueldos y salarios del Personal Técnico, de Gestión y de Administración y Servicios (PTGAS)", 2),
    ("01.03", "salario-piyotper",         "Costes del Personal Investigador laboral y de otro personal", 2),
    ("01.04", "ss",                       "Cotizaciones sociales a cargo del empleador", 2),
    ("01.05", "prevsoc-funcs",            "Previsión social de funcionarios (costes calculados)", 2),
    ("01.06", "indem",                    "Indemnizaciones", 2),
    ("01.07", "otcsoc",                   "Otros costes sociales", 2),
    ("01.08", "indemnizaciones-servicio", "Indemnizaciones por razón de servicio", 2),
    ("01.09", "otras-indemnizaciones",    "Otras indemnizaciones", 2),
    ("01.10", "transp-personal",          "Transporte de personal", 2),

    ("03",    "bienes-servicios",         "Coste de adquisición de bienes y servicios", 1),
    ("03.03", "material-laboratorio",     "Coste de material e instrumental de laboratorio y experimentación", 2),
    ("03.08", "material-oficina",         "Coste de material de oficina ordinario no inventariable", 2),
    ("03.10", "publicaciones",            "Coste de prensa, revistas, libros y otras publicaciones", 2),
    ("03.11", "bienes-investigación",     "Coste de adquisición de bienes asociados a proyectos de investigación", 2),
    ("03.12", "trabajos-otras-empresas",  "Trabajos realizados por otras empresas", 2),
    ("03.13", "otros-bienes-servicios",   "Otras adquisiciones de bienes y servicios", 2),

    ("04",    "servicios-exteriores",     "Coste de servicios exteriores", 1),
    ("04.02", "conservación",             "Reparaciones y conservación", 2),
    ("04.03", "servicios-profesionales",  "Servicios de profesionales independientes", 2),
    ("04.09", "suministros",              "Suministros", 2),
    ("04.10", "comunicaciones",           "Comunicaciones", 2),
    ("04.11", "limpieza-aseo",            "Limpieza y aseo", 2),
    ("04.12", "seguridad",                "Seguridad y vigilancia", 2),
    ("04.13", "costes-diversos",          "Costes diversos", 2),

    ("05",    "tributos",                 "Coste de tributos", 1),
    ("06",    "costes-financieros",       "Costes financieros", 1),

    ("07",    "amortizaciones",                       "Amortizaciones (costes calculados)", 1),
    ("07.01", "amortización-inmovilizado-material",   "Del inmovilizado material", 2),
    ("07.02", "amortización-inmovilizado-inmaterial", "Del inmovilizado inmaterial", 2),

    ("08",    "costes-oportunidad",                   "Costes de oportunidad (costes calculados)", 1),

    ("09",    "transferencias",                       "Transferencias", 1),
    ("09.01", "transferencias-alumnos",               "A alumnos/as", 2),
    ("09.02", "transferencias-organizaciones-grupo",  "A organizaciones del grupo", 2),
    ("09.03", "transferencias-otras-organizaciones",  "A otras organizaciones", 2),
]


def generar_cuadro_10_1(ruta_base: Path, dir_informes: Path) -> None:
    generar_cuadro_jerárquico(
        id_cuadro="cuadro_10_1",
        título="Informe de elementos de coste",
        encabezado_concepto="Elemento de coste",
        plantilla=_PLANTILLA,
        ucs=cargar_ucs(ruta_base),
        árbol=cargar_árbol_elementos_de_coste(ruta_base),
        columna_coordenada="elemento_de_coste",
        dir_informes=dir_informes,
    )
