"""Cuadro 10.4 — Informe de costes por centros de coste según su finalidad.

Plantilla SUE oficial sobre el árbol de centros de coste. Para cada
slug calcula su ``A+B`` (suma del subárbol).
"""

from __future__ import annotations

from pathlib import Path

from coana.fase2._cuadro_jerarquico import generar_cuadro_jerárquico
from coana.fase2.calculo import cargar_árbol_centros_de_coste, cargar_ucs

# (código_SUE, slug_árbol, nombre_oficial, nivel)
_PLANTILLA: list[tuple[str, str, str, int]] = [
    ("01",    "docencia",                       "Centros de docencia", 1),
    ("01.01", "facultades-escuelas",            "Facultades y escuelas", 2),
    ("01.02", "aulas-laboratorios-docentes",    "Aulas y laboratorios docentes", 2),
    ("01.03", "otros-docentes",                 "Otros centros docentes", 2),

    ("02",    "investigación",                  "Centros de investigación", 1),
    ("02.01", "institutos",                     "Institutos de investigación", 2),
    ("02.02", "centros-unidades-investigación", "Centros y unidades de investigación", 2),
    ("02.03", "laboratorios-investigación",     "Laboratorios de investigación", 2),
    ("02.04", "cátedras-investigación",         "Cátedras de investigación", 2),
    ("02.05", "otros-investigación",            "Otros centros de investigación", 2),

    ("03",    "docencia-investigación",                "Centros de docencia e investigación", 1),
    ("03.01", "departamentos",                         "Departamentos", 2),
    ("03.02", "ed",                                    "Escuela de doctorado", 2),
    ("03.03", "laboratorios-docencia-investigación",   "Laboratorios de docencia e investigación", 2),
    ("03.04", "otros-docentes-investigación",          "Otros centros de docencia e investigación", 2),

    ("04",    "apoyo-docencia-investigación",   "Centros de apoyo a docencia e investigación", 1),
    ("04.01", "bibliotecas",                    "Bibliotecas", 2),
    ("04.02", "otros-apoyo-docencia",           "Otras unidades de apoyo a la docencia", 2),
    ("04.03", "otros-apoyo-investigación",      "Otras unidades de apoyo a la investigación", 2),
    ("04.04", "otros-apoyo",                    "Otras unidades de apoyo", 2),

    ("05",    "extensión-universitaria-deportes",      "Centros de extensión universitaria y deportes", 1),
    ("05.01", "actividades-culturales",                "Centros de actividades culturales", 2),
    ("05.02", "deportes",                              "Centros de deportes", 2),
    ("05.03", "cooperación",                           "Cooperación", 2),
    ("05.04", "movilidad",                             "Movilidad", 2),
    ("05.05", "otros-extensión-universitaria-deportes", "Otros centros de extensión universitaria y deportes", 2),

    ("06",    "soporte",                        "Centros de soporte", 1),
    ("06.01", "área-rectorado",                 "Rectorado", 2),
    ("06.02", "vicerrectorados",                "Vicerrectorados", 2),
    ("06.03", "área-secretaria-general",        "Secretaría General", 2),
    ("06.04", "gerencia",                       "Gerencia", 2),
    ("06.05", "órganos-colegiados",             "Órganos colegiados", 2),
    ("06.06", "servicios-generales-centrales",  "Servicios generales y centrales", 2),
    ("06.07", "otros-servicios-soporte",        "Otros servicios de soporte", 2),

    ("07",    "anexos",                         "Centros anexos", 1),

    ("08",    "centros-agrupaciones-costes",    "Centros-Agrupaciones de costes", 1),
    ("08.01", "locales-vacios",                 "Locales vacíos", 2),
    ("08.02", "locales-sindicales",             "Locales sindicales", 2),
    ("08.03", "locales-fundaciones-y-otros",    "Locales de fundaciones y de otras entidades con personalidad jurídica propia", 2),
    ("08.04", "otros-agrupaciones",             "Otras agrupaciones de costes", 2),
]


_EXPANDIR_NIVEL_3: set[str] = {
    "facultades-escuelas",           # 01.01
    "otros-docentes",                # 01.03
    "institutos",                    # 02.01
    "departamentos",                 # 03.01
    "otros-apoyo-docencia",          # 04.02
    "otros-apoyo-investigación",     # 04.03
    "otros-apoyo",                   # 04.04
    "área-rectorado",                # 06.01
    "área-secretaria-general",       # 06.03
    "órganos-colegiados",            # 06.05
    "servicios-generales-centrales", # 06.06
    "otros-servicios-soporte",       # 06.07
}


def generar_cuadro_10_4(ruta_base: Path, dir_informes: Path) -> None:
    generar_cuadro_jerárquico(
        id_cuadro="cuadro_10_4",
        título="Informe de costes por centros de coste según su finalidad",
        encabezado_concepto="Tipo de centro",
        plantilla=_PLANTILLA,
        ucs=cargar_ucs(ruta_base),
        árbol=cargar_árbol_centros_de_coste(ruta_base),
        columna_coordenada="centro_de_coste",
        dir_informes=dir_informes,
        expandir_nivel_3=_EXPANDIR_NIVEL_3,
    )
