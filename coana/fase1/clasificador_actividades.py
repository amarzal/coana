"""Clasificador de actividades compartido.

Extrae la lógica de clasificación de actividades del traductor de
presupuesto para que pueda ser reutilizada por otros módulos de la Fase 1.
"""

import logging
from typing import Protocol

import polars as pl

log = logging.getLogger(__name__)

# ======================================================================
# Constantes para clasificación de actividades
# ======================================================================

# Tipos de proyecto considerados de investigación y transferencia
_TIPOS_IT = [
    "0000I", "000I", "06I", "A11I", "A1TI", "A83CA", "BECI", "CA",
    "COBEI", "COF", "CONI", "CONVI", "DIPI", "FGVI", "GVI", "IDI",
    "MCTFE", "MCTI", "MEC", "MECD", "MECI", "MIE", "MPEI", "MSI",
    "MSP", "MTAI", "MTD", "PCT", "PII", "PPSI", "UEI",
]

# R17 / 00G-00000: subcentro (vicerrectorado) → actividad
_SUBCENTRO_VICERRECT: dict[str, str] = {
    "VCL": "dag-vcls", "VEV": "dag-vevs", "VI": "dag-vi",
    "VIN": "dag-vis", "VTD": "dag-vitdc", "VA": "dag-voap",
    "VPE": "dag-vpee", "VRI": "dag-vri", "VRSPII": "dag-vrspii",
    "VEF": "dag-vefp",
    "VRS": "dag-vrspii",
    "R10": "dag-delegado",
}

# R19: subcentro → actividad (SLG/23G/20G, capítulo≠4)
_SUBCENTRO_R19: dict[str, str] = {
    "CP": "dag-otros-servicios-comunicación-publicaciones",
    "P3": "dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico",
    "OL": "dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico",
    "F3": "dag-otros-servicios-prevención-gestión-medioambiental",
    "I2": "dag-otros-servicios-ti",
    "F2": "dag-otros-servicios-obras-proyectos",
    "S9": "dag-otros-servicios-información-registro",
    "OC": "dag-otros-servicios-promoción-evaluación-calidad",
    "L2": "dag-otros-servicios-relaciones-internacionales",
    "GI": "dag-otros-servicios-ti",
    "DI": "dag-otros-servicios-atención-diversidad-apoyo-educativo",
    "R9": "dag-otros-servicios-promoción-fomento-igualdad",
}

# R20/R50: centro (departamento) → actividad
_CENTRO_DEPTOS: dict[str, str] = {
    "DADEM": "dag-daem", "DCAMN": "dag-dbbcn", "DCICO": "dag-dcc",
    "DDPRI": "dag-ddpri", "DDPUB": "dag-ddpub", "DDTSE": "dag-updtssee",
    "DEANG": "dag-dea", "DECIC": "dag-dicc", "DECON": "dag-deco",
    "DEDES": "dag-dede", "DEMEC": "dag-dmc", "DESID": "dag-desid",
    "DFICE": "dag-dfs", "DFICO": "dag-dfc", "DFISI": "dag-dfis",
    "DFISO": "dag-dfce", "DHIST": "dag-dhga", "DINFE": "dag-upi",
    "DIQUI": "dag-deq", "DLSIN": "dag-dlsi", "DMATE": "dag-dmat",
    "DMEDI": "dag-upm", "DPDID": "dag-dpdcsll", "DPSIB": "dag-dpbcp",
    "DPSIE": "dag-dpeesm", "DQFIA": "dag-dqfa", "DQUIO": "dag-dqio",
    "DTRAD": "dag-dtc",
}

# R21b: subcentro → actividad (EST/26G/20G, capítulo≠4)
_SUBCENTRO_R21: dict[str, str] = {
    "D2": "dag-sgde", "IH": "dag-oe", "D6": "dag-oipep",
    "O3": "dag-opp", "D8": "dag-uo",
}

# R47: subcentro → actividad (00G, proyecto=00000, capítulo≠4)
_SUBCENTRO_00G: dict[str, str] = {
    "R1": "dag-rectorado",
    "R5": "dag-síndico-agravios",
    "R10": "dag-delegado",
    "S1": "dag-secretaría-general",
    "S4": "dag-junta-electoral",
    "D7": "dag-consejo-estudiantes",
    "CP": "dag-otros-servicios-comunicación-publicaciones",
    "P3": "dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico",
    "OL": "dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico",
    "F3": "dag-otros-servicios-prevención-gestión-medioambiental",
    "GA": "dag-otros-servicios-ti",
    "I2": "dag-otros-servicios-ti",
    "F2": "dag-otros-servicios-obras-proyectos",
    "S9": "dag-otros-servicios-información-registro",
    "OC": "dag-otros-servicios-promoción-evaluación-calidad",
    "L2": "dag-otros-servicios-relaciones-internacionales",
    "GI": "dag-otros-servicios-ti",
    "DI": "dag-otros-servicios-atención-diversidad-apoyo-educativo",
    "C2": "dag-deportes",
    "C3": "dag-cultura",
    "DS": "dag-cooperación",
    "Y1": "dag-estce",
    "J1": "dag-fcje",
    "H1": "dag-fchs",
    "SA": "dag-fcs",
    "CC": "dag-cent",
    "LB": "dag-labcom",
    "D2": "dag-sgde",
    "D6": "dag-oipep",
    "O3": "dag-opp",
    "D8": "dag-uo",
    "I4": "dag-ocit",
    "SD": "dag-sala-disección",
    "ED": "dag-escuela-doctorado",
}

# R48: GEREN — prefijo de aplicación → actividad
_GEREN_PREFIJOS: dict[str, str] = {
    "21": "dag-org-gerencia-tributos",
    "221": "dag-org-gerencia-arrendamiento-bienes",
    "222": "dag-org-gerencia-reparación-conservación",
    "223": "dag-org-gerencia-suministros",
    "224": "dag-org-gerencia-transportes-comunicaciones",
    "225": "dag-org-gerencia-trabajos-realizados-otras-empresas",
    "226": "dag-org-gerencia-primas-seguros",
    "227": "dag-org-gerencia-material-oficina",
    "228": "dag-org-gerencia-gastos-diversos",
    "3": "dag-org-gerencia-gastos-financieros",
    "6711": "dag-org-gerencia",
    "23": "dag-org-gerencia-indemnizaciones-razón-servicio",
}

# 06G/9G077: subcentro vicerrectorado, prefijo de aplicación → actividad
_VICERRECT_PREFIJOS: dict[str, str] = {
    "21":   "dag-org-vicerrectorados-tributos",
    "221":  "dag-org-vicerrectorados-arrendamiento-bienes",
    "222":  "dag-org-vicerrectorados-reparación-conservación",
    "223":  "dag-org-vicerrectorados-suministros",
    "224":  "dag-org-vicerrectorados-transportes-comunicaciones",
    "225":  "dag-org-vicerrectorados-trabajos-realizados-otras-empresas",
    "226":  "dag-org-vicerrectorados-primas-seguros",
    "227":  "dag-org-vicerrectorados-material-oficina",
    "228":  "dag-org-vicerrectorados-gastos-diversos",
    "23":   "dag-org-vicerrectorados-indemnizaciones-razón-servicio",
    "3":    "dag-org-vicerrectorados-gastos-financieros",
    "6711": "dag-org-vicerrectorados-adquisiciones-bibliográficas",
}

# 8G022: prefijo de aplicación → sufijo de actividad (dag-{depto}-{sufijo})
_8G022_SUFIJOS: dict[str, str] = {
    "21":   "tributos",
    "221":  "arrendamiento-bienes",
    "222":  "reparación-conservación",
    "223":  "suministros",
    "224":  "transportes-comunicaciones",
    "225":  "trabajos-realizados-otras-empresas",
    "226":  "primas-seguros",
    "227":  "material-oficina",
    "228":  "gastos-diversos",
    "23":   "indemnizaciones-razón-servicio",
    "3":    "gastos-financieros",
    "6711": "adquisiciones-bibliográficas",
}

# 8G022: centro (departamento) → slug del departamento
_CENTRO_DEPTO_SLUG_8G022: dict[str, str] = {
    "DADEM": "daem",    "DCAMN": "dbbcn",   "DCICO": "dcc",
    "DDPRI": "ddpri",   "DDPUB": "ddpub",   "DDTSE": "updtssee",
    "DEANG": "dea",     "DECIC": "dicc",    "DECON": "deco",
    "DEDES": "dede",    "DEMEC": "dmc",     "DESID": "desid",
    "DFICE": "dfs",     "DFICO": "dfc",     "DFISI": "dfis",
    "DFISO": "dfce",    "DHIST": "dhga",    "DINFE": "upi",
    "DIQUI": "deq",     "DLSIN": "dlsi",    "DMATE": "dmat",
    "DMEDI": "upm",     "DPDID": "dpdcsll", "DPSIB": "dpbcp",
    "DPSIE": "dpeesm",  "DQFIA": "dqfa",    "DQUIO": "dqio",
    "DTRAD": "dtc",
}

# 1G010/9G082: subcentros en lista, prefijo de aplicación → actividad
_OTROS_SERVICIOS_PREFIJOS: dict[str, str] = {
    "21":   "dag-otros-servicios-tributos",
    "221":  "dag-otros-servicios-arrendamiento-bienes",
    "222":  "dag-otros-servicios-reparación-conservación",
    "223":  "dag-otros-servicios-suministros",
    "224":  "dag-otros-servicios-transportes-comunicaciones",
    "225":  "dag-otros-servicios-trabajos-realizados-otras-empresas",
    "226":  "dag-otros-servicios-primas-seguros",
    "227":  "dag-otros-servicios-material-oficina",
    "228":  "dag-otros-servicios-gastos-diversos",
    "23":   "dag-otros-servicios-indemnizaciones-razón-servicio",
    "3":    "dag-otros-servicios-gastos-financieros",
    "6711": "dag-otros-servicios-adquisiciones-bibliográficas",
}

_OTROS_SERVICIOS_SUBCENTROS: list[str] = [
    "CP", "P3", "OL", "F3", "GA", "I2", "F2", "S9", "OC", "L2", "GI", "DI", "R9",
]

# R48: SC001 — prefijo de aplicación → actividad
_SC001_PREFIJOS: dict[str, str] = {
    "221": "dag-sgc-tributos",
    "222": "dag-sgc-arrendamiento-bienes",
    "223": "dag-sgc-reparación-conservación",
    "224": "dag-sgc-suministros",
    "225": "dag-sgc-transportes-comunicaciones",
    "226": "dag-sgc-trabajos-realizados-otras-empresas",
    "227": "dag-sgc-primas-seguros",
    "228": "dag-sgc-material-oficina",
    "3": "dag-sgc-gastos-diversos",
    "6711": "dag-sgc-gastos-financieros",
    "23": "dag-org-gerencia",
    "21": "dag-org-gerencia-indemnizaciones-razón-servicio",
}

# R49: centro → actividad (00G, proyecto=00000, capítulo≠4)
_CENTRO_00G: dict[str, str] = {
    "CONSE": "dag-consejo-social",
    "IUDT": "dag-iudt",
    "IUEFG": "dag-iuef",
    "IMAC": "dag-imac",
    "INAM": "dag-inam",
    "INIT": "dag-init",
    "IUPA": "dag-iupa",
    "IUTC": "dag-iutc",
    "IUTUR": "dag-iuturismo",
    "IDL": "dag-iidl",
    "IEI": "dag-iei",
    "IFV": "dag-ifv",
    "IIG": "dag-iigeo",
    "IILP": "dag-ii-lópez-piñero",
    "I5": "dag-biblioteca",
    "CENT": "dag-cent",
    "IULMA": "dag-ilma",
    "IDSP": "dag-idsp",
    "SCIC": "dag-scic",
    "SEA": "dag-sea",
}

# Subcentros de vicerrectorado (spec §2-1-2-3)
_SUBCENTROS_VICERRECTORADO = [
    "VCL", "VEF", "VEV", "VI", "VIN", "VTD", "VA", "VPE", "VRI", "VRS",
]

# Másteres oficiales: proyecto → actividad (MO/MO08/MO09/MO10/MO12)
_MÁSTERES_OFICIALES: dict[str, str] = {
    "09G010": "dag-vefp",
    "07G067": "máster-márqueting",
    "07G069": "máster-rsc",
    "07G071": "máster-cooperación-desarrollo",
    "07G073": "máster-traducción-medicina",
    "07G075": "máster-mediación-familiar",
    "07G077": "máster-psicología-trabajo",
    "07G079": "máster-innovación-comunicación",
    "07G081": "máster-diseño-fabricación",
    "07G082": "máster-eficiencia-energética",
    "07G083": "máster-riesgos-laborales",
    "07G084": "máster-química-sostenible",
    "07G085": "máster-química-cromatográfica",
    "07G093": "máster-igualdad-género",
    "14G020": "máster-dirección-empresas",
    "14G109": "máster-ingeniería-industrial",
    "14G110": "máster-psicologia-sanitaria",
    "15G059": "máster-dirección-empresas",
    "15G061": "máster-cerebro",
    "22G138": "máster-enfermería-urgencias",
    "08G170": "máster-historia-mediterráneo",
    "08G175": "máster-comunicación-intercultural",
    "08G178": "máster-inglés-multilingüe",
    "09G046": "máster-gestión-financiera",
    "09G078": "máster-paz",
    "09G109": "máster-secundaria",
    "10G055": "máster-penal",
    "10G056": "máster-historia-arte",
    "10G057": "máster-inglés-comercio",
    "10G058": "máster-género",
    "10G120": "máster-traducción",
    "10G123": "máster-química-aplicada",
    "12G103": "máster-psicopedagogía",
    "12G104": "máster-salud-mental",
    "13G001": "máster-abogacía",
}

# Proyectos con tratamiento especial: proyecto → actividad
_PROYECTOS_ESPECIALES: dict[str, str] = {
    "25G032": "grado-medicina",
    "06G008": "acceso-enseñanzas-oficiales",
    "1G041": "acceso-enseñanzas-oficiales",
    "16G095": "universidad-mayores",
    "23I235": "ait-financiación-propia",
    "24I351": "ait-financiación-propia",
    "24I352": "ait-financiación-propia",
    "1I235": "cátedras-aulas-empresa",
    "22G023": "dag-biblioteca",
    "22G025": "dag-biblioteca",
    "22G013": "dag-otros-servicios-ti",
    "04G117": "dag-deportes",
    "23G011": "deportes",
    "18G048": "cultura",
    "8G015": "cultura",
    "08G023": "dag-rectorado",
    "23G012": "dag-otros-servicios-comunicación-publicaciones",
    "22G019": "dag-voap",
    "19G006": "dag-vevs",
    "22G020": "dag-vevs",
    "23G058": "dag-vri",
    "23G001": "dag-cultura",
    "19G007": "dag-vis",
    "16G071": "dag-gerencia",
    "25G134": "dag-consejo-estudiantes",
    "05G006": "dag-org-vicerrectorados-transportes-comunicaciones",
    "07G001": "dag-otros-servicios-relaciones-internacionales",
    "22G014": "dag-otros-servicios-comunicación-publicaciones",
    "22G015": "dag-otros-servicios-comunicación-publicaciones",
    "22G016": "dag-otros-servicios-comunicación-publicaciones",
    "22G017": "dag-otros-servicios-comunicación-publicaciones",
    "22G018": "dag-otros-servicios-comunicación-publicaciones",
    "24G107": "dag-otros-servicios-comunicación-publicaciones",
    "23G002": "dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico",
    "23G003": "dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico",
    "23G004": "dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico",
    "23G007": "dag-gerencia",
    "04G016": "dag-otros-servicios-ti",
    "04G007": "dag-otros-servicios-promoción-evaluación-calidad",
    "19G002": "dag-otros-servicios-relaciones-internacionales",
    "1G033": "dag-otros-servicios-ti",
    "19G012": "dag-vis",
    "25G080": "dag-otros-servicios-relaciones-internacionales",
    "25G058": "ámbito-filología",
    "25G059": "ámbito-industrial",
    "25G060": "ámbito-industrial",
    "25G061": "ámbito-industrial",
    "22G026": "dag-vi",
    "1G021": "dag-sgde",
    "07G011": "dag-vefp",
    "22G021": "dag-oipep",
    "24G112": "microcredenciales",
    "8G055": "acción-sindical",
}


# Proyectos que generan varias UC repartiendo el importe según porcentajes.
# Estructura: proyecto → { subproyecto → [(porcentaje_en_%, actividad), ...] }
# Cada apunte marcado se expande usando solo las filas cuyo SUBPROYECTO coincide
# con el valor del apunte.  Los porcentajes se normalizan internamente.
# Apuntes cuyo subproyecto no aparece en la tabla quedan sin clasificar.
_REPARTOS: dict[str, dict[str, list[tuple[float, str]]]] = {
    "23G010": {
        "03": [
            (10.0630, "dag-encargos-gestión-estudios-propios"),
            (19.0887, "máster-igualdad-género"),
            (36.6774, "dag-otros-servicios-promoción-fomento-igualdad"),
            (34.1709, "dag-otros-servicios-promoción-fomento-igualdad"),
        ],
        "02": [
            (38.8486, "dag-encargos-gestión-estudios-propios"),
            ( 9.5053, "dag-encargos-gestión"),
            ( 9.5053, "dag-encargos-gestión-estudios-propios"),
            ( 5.1648, "dag-encargos-gestión-proyectos-internacionales"),
            ( 5.6195, "dag-encargos-proyectos-investigación-europeos"),
            (14.9992, "dag-encargos-gestión-transferencia"),
            (12.4470, "dag-encargos-gestión-transferencia"),
            ( 3.9104, "dag-otros-servicios-relaciones-internacionales"),
        ],
        "01": [
            (46.6979, "dag-encargos-gestión-transferencia"),
            (10.6870, "dag-apoyo-estudiantes"),
            ( 9.7964, "máster-traducción-medicina"),
            (28.8112, "escola-estiu"),
            ( 4.0076, "dag-otros-servicios-comunicación-publicaciones"),
        ],
    },
}

# Proyectos Erasmus y similares → "ai-internacional" + PROYECTO
_ERASMUS_PROYECTOS: list[str] = [
    "22G097", "23G038", "23G057", "23G117",
    "23G143", "23G154", "23G155", "23G156", "23G157",
    "24G003", "24G011", "24G016", "24G041", "24G046", "24G084",
    "24G110", "24G126", "24G137",
    "25G033", "25G036", "25G037", "25G063", "25G064",
]

# Proyectos Erasmus de movilidad educativa → "dag-otros-servicios-relaciones-internacionales" + PROYECTO
_RELINT_PROYECTOS: list[str] = ["23G121", "23G131"]


# ======================================================================
# Protocolo para el contexto de enriquecimiento
# ======================================================================

class ContextoEnriquecimiento(Protocol):
    """Interfaz mínima que debe cumplir el contexto para enriquecer."""
    proyectos: pl.DataFrame | None
    líneas_de_financiación: pl.DataFrame | None


# ======================================================================
# Funciones públicas
# ======================================================================

def enriquecer_para_actividades(
    df: pl.DataFrame,
    ctx: ContextoEnriquecimiento,
) -> pl.DataFrame:
    """Añade las columnas de enriquecimiento necesarias para las reglas de actividad.

    Columnas añadidas:
    - ``_tipo_proyecto``: tipo del proyecto (desde ctx.proyectos)
    - ``_nombre_proyecto``: nombre del proyecto (desde ctx.proyectos)
    - ``_tipo_línea``: tipo de la línea de financiación (desde ctx.líneas_de_financiación)
    - ``_capítulo``: primer dígito de ``aplicación``
    """
    # Asegurar que las columnas clave son cadenas
    for col in ("centro", "subcentro", "programa", "línea",
                "proyecto", "aplicación"):
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))

    # tipo_proyecto, nombre_proyecto y centro_origen ← proyectos
    if ctx.proyectos is not None:
        cols_proj = [
            pl.col("proyecto").cast(pl.Utf8),
            pl.col("tipo").cast(pl.Utf8).str.strip_chars().alias("_tipo_proyecto"),
            pl.col("nombre").cast(pl.Utf8).alias("_nombre_proyecto"),
        ]
        if "centro_origen" in ctx.proyectos.columns:
            cols_proj.append(
                pl.col("centro_origen").cast(pl.Utf8).alias("_centro_origen")
            )
        df = df.join(
            ctx.proyectos.select(cols_proj),
            on="proyecto",
            how="left",
        )
        if "_centro_origen" not in df.columns:
            df = df.with_columns(
                pl.lit(None).cast(pl.Utf8).alias("_centro_origen")
            )
    else:
        df = df.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("_tipo_proyecto"),
            pl.lit(None).cast(pl.Utf8).alias("_nombre_proyecto"),
            pl.lit(None).cast(pl.Utf8).alias("_centro_origen"),
        )

    # tipo_línea ← líneas de financiación
    if ctx.líneas_de_financiación is not None:
        df = df.join(
            ctx.líneas_de_financiación.select(
                pl.col("línea").cast(pl.Utf8),
                pl.col("tipo").cast(pl.Utf8).alias("_tipo_línea"),
            ),
            on="línea",
            how="left",
        )
    else:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("_tipo_línea"))

    # capítulo = primer dígito de la aplicación
    df = df.with_columns(
        pl.col("aplicación").str.slice(0, 1).alias("_capítulo"),
    )

    return df


def clasificar_actividades(
    df: pl.DataFrame,
    árbol: object | None,
    obtener_descripciones: callable,
) -> tuple[pl.DataFrame, list[tuple[str, int, float]]]:
    """Clasifica actividades aplicando reglas estáticas, dinámicas y repartos.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame con columnas: ``proyecto``, ``aplicación``, ``programa``,
        ``centro``, ``subcentro``, ``línea``, ``subproyecto``, ``importe``,
        ``_tipo_proyecto``, ``_tipo_línea``, ``_capítulo``, ``_nombre_proyecto``.
    árbol : Árbol | None
        Árbol de actividades para crear nodos dinámicos.
    obtener_descripciones : callable
        Función ``(col: str, valores: list) -> dict[str, str]`` que obtiene
        descripciones de nodos para los valores de un campo.

    Returns
    -------
    tuple[pl.DataFrame, list[tuple[str, int, float]]]
        ``(df_con_actividad, conteo_reglas)`` donde ``conteo_reglas`` es una
        lista de tuplas ``(nombre_regla, n_filas, importe)``.
    """
    df, conteo = _reglas_actividad(df)
    df = _reglas_actividad_dinámicas(df, árbol, obtener_descripciones)
    df = _expandir_repartos(df)
    return df, conteo


# ======================================================================
# Reglas estáticas de actividad
# ======================================================================

def _reglas_actividad(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, list[tuple[str, int, float]]]:
    """Aplica reglas de actividad secuencialmente con conteo."""
    tp = pl.col("_tipo_proyecto")
    tl = pl.col("_tipo_línea")
    cap4 = pl.col("_capítulo") == "4"
    cap_ne_4 = pl.col("_capítulo") != "4"
    prog_541a = pl.col("programa") == "541-A"
    proy_00000 = pl.col("proyecto") == "00000"
    centro = pl.col("centro")
    subcentro = pl.col("subcentro")

    # Tipos de proyecto de investigación/transferencia (arts 60, cátedras…)
    tipos_arts = ["0000I", "A11I", "A1TI", "A83CA", "CA", "PCT", "IDI"]

    # Tipos de proyecto con posible co-financiación
    tipos_cofi = [
        "000I", "06I", "BECI", "COBEI", "COF", "CONI", "CONVI",
        "DIPI", "FGVI", "GVI", "MCTFE", "MCTI", "MEC", "MECD",
        "MECI", "MIE", "MIG", "MPEI", "MSI", "MSP", "MTAI",
        "MTD", "PII", "PPSI", "UEI", "UEGD",
    ]

    # Tipos para R17
    tipos_r17 = [
        "QMG", "EST", "MO", "MO08", "MO09", "MO10", "MO12", "21G", "20G",
    ]
    base_r17 = cap_ne_4 & tp.is_in(tipos_r17)

    # Base para R47-R49: 00G, proyecto=00000, capítulo≠4
    base_00g_00000 = cap_ne_4 & (tp == "00G") & proy_00000

    # (00G + proyectos) o 14G → departamento (mapping)
    base_r50 = cap_ne_4 & (
        (
            (tp == "00G")
            & pl.col("proyecto").is_in(
                ["00000", "24G103", "25G080", "25G109"]
            )
        )
        | (tp == "14G")
    )

    # =============================================================
    # Definir reglas como lista de (nombre, condición, resultado)
    # =============================================================

    reglas: list[tuple[str, pl.Expr, pl.Expr]] = [
        # CAPÍTULO 4
        ("[Becas] cap=4, no IT, proy=00000 → ayudas-genéricas-estudiantes",
         cap4 & ~tp.is_in(_TIPOS_IT) & proy_00000,
         pl.lit("ayudas-genéricas-estudiantes")),

        ("[Becas] cap=4, no IT, proy≠00000 → +=otras-ayudas-estudiantes",
         cap4 & ~tp.is_in(_TIPOS_IT) & (pl.col("proyecto") != "00000"),
         pl.lit("+=otras-ayudas-estudiantes")),

        ("[Becas] cap=4, IT, prog≠541-A → +=otras-ayudas-estudiantes",
         cap4 & tp.is_in(_TIPOS_IT) & ~prog_541a,
         pl.lit("+=otras-ayudas-estudiantes")),

        # INVESTIGACIÓN Y TRANSFERENCIA
        ("[Plan propio] arts, tl=00 → +=ait-plan-propio",
         tp.is_in(tipos_arts) & (tl == "00"),
         pl.lit("+=ait-plan-propio")),

        ("[Cátedras y aulas] arts, cátedra/aula empresa → +=cátedras-aulas-empresa",
         tp.is_in(tipos_arts)
         & pl.col("_nombre_proyecto").str.contains(
             r"(?i)c.tedra|aula empresa"
         ).fill_null(False),
         pl.lit("+=cátedras-aulas-empresa")),

        ("[Artículos 60] arts (resto) → +=transf-60",
         tp.is_in(tipos_arts),
         pl.lit("+=transf-60")),

        ("[Investigación regional] DIPI/FGVI/GVI, 541-A, tl≠00 → +=ai-regional",
         tp.is_in(["DIPI", "FGVI", "GVI"]) & prog_541a & (tl != "00"),
         pl.lit("+=ai-regional")),

        ("[Investigación nacional] cofi nacional, 541-A, tl≠00 → +=ai-nacional",
         tp.is_in([
             "06I", "COBEI", "MCTFE", "MCTI", "MEC", "MECD",
             "MECI", "MIE", "MIG", "MPEI", "MSI", "MSP",
             "MTAI", "MTD",
         ]) & prog_541a & (tl != "00"),
         pl.lit("+=ai-nacional")),

        ("[Investigación internacional] UEI/UEGD, 541-A, tl≠00 → +=ai-internacional",
         tp.is_in(["UEI", "UEGD"]) & prog_541a & (tl != "00"),
         pl.lit("+=ai-internacional")),

        ("[Otra investigación competitiva] BECI/CONI/CONVI, 541-A, tl≠00 → +=ai-otras-competitivas",
         tp.is_in(["BECI", "CONI", "CONVI"]) & prog_541a & (tl != "00"),
         pl.lit("+=ai-otras-competitivas")),

        ("[Co-financiación] cofi, 541-A, tl=00, PPSI → +=ppsi",
         tp.is_in(tipos_cofi) & prog_541a & (tl == "00")
         & pl.col("_nombre_proyecto").str.contains(
             r"(?i)PPSI"
         ).fill_null(False),
         pl.lit("+=ppsi")),

        ("[Co-financiación] cofi, 541-A, tl=00, UJI-/GACUJIMA → +=ait-plan-propio",
         tp.is_in(tipos_cofi) & prog_541a & (tl == "00")
         & pl.col("_nombre_proyecto").str.contains(
             r"(?i)UJI-|GACUJIMA"
         ).fill_null(False),
         pl.lit("+=ait-plan-propio")),

        ("[Co-financiación] cofi, 541-A, tl=00 (resto) → +=otras-ait-financiación-propia",
         tp.is_in(tipos_cofi) & prog_541a & (tl == "00"),
         pl.lit("+=otras-ait-financiación-propia")),

        ("[Co-financiación] cofi, 541-A, tl≠00 → +=ait-financiación-externa",
         tp.is_in(tipos_cofi) & prog_541a & (tl != "00"),
         pl.lit("+=ait-financiación-externa")),

        ("[Financiación propia] 000TR, 541-A, tl=00 → +=ait-financiación-propia",
         (tp == "000TR") & prog_541a & (tl == "00"),
         pl.lit("+=ait-financiación-propia")),

        ("[Doctorado] DOCT, 541-A → +=doctorado",
         (tp == "DOCT") & prog_541a,
         pl.lit("+=doctorado")),

        ("[Doctorado: acreditación] proy=19I005 → doctorado",
         pl.col("proyecto") == "19I005",
         pl.lit("doctorado")),

        ("[Internacional] 05G, línea≠00 → ai-internacional",
         (tp == "05G") & (pl.col("línea") != "00"),
         pl.lit("ai-internacional")),

        # DISTINTO DE INVESTIGACIÓN Y TRANSFERENCIA (capítulo ≠ 4)

        # Reglas de reparto: un apunte → varias UC (porcentajes en _REPARTOS)
        ("[Reparto] cap≠4, proy=23G010 → %%reparto:23G010",
         cap_ne_4 & (pl.col("proyecto") == "23G010"),
         pl.lit("%%reparto:23G010")),

        ("[Másteres oficiales] MO, proyecto en másteres → máster oficiales",
         cap_ne_4
         & tp.is_in(["MO", "MO08", "MO09", "MO10", "MO12"])
         & pl.col("proyecto").is_in(list(_MÁSTERES_OFICIALES.keys())),
         pl.col("proyecto").replace(
             list(_MÁSTERES_OFICIALES.keys()),
             list(_MÁSTERES_OFICIALES.values()),
         )),

        ("[Investigación nacional individual] proy=23I373 → +=ai-nacional",
         cap_ne_4 & (pl.col("proyecto") == "23I373"),
         pl.lit("+=ai-nacional")),

        ("[Investigación europea individual] proy=21I321 → +=ai-internacional",
         cap_ne_4 & (pl.col("proyecto") == "21I321"),
         pl.lit("+=ai-internacional")),

        ("[Cooperación singulares] proy=9I113/24G002 → +=cooperación",
         cap_ne_4 & pl.col("proyecto").is_in(["9I113", "24G002"]),
         pl.lit("+=cooperación")),

        ("[Proyectos con tratamiento especial] proyecto → actividad",
         cap_ne_4
         & pl.col("proyecto").is_in(list(_PROYECTOS_ESPECIALES.keys())),
         pl.col("proyecto").replace(
             list(_PROYECTOS_ESPECIALES.keys()),
             list(_PROYECTOS_ESPECIALES.values()),
         )),

        ("[Erasmus movilidad educativa] 23G121/23G131 → +=dag-otros-servicios-relaciones-internacionales",
         cap_ne_4 & pl.col("proyecto").is_in(_RELINT_PROYECTOS),
         pl.lit("+=dag-otros-servicios-relaciones-internacionales")),

        ("[Proyectos Erasmus y similares] proyecto en lista → +=ai-internacional",
         cap_ne_4 & pl.col("proyecto").is_in(_ERASMUS_PROYECTOS),
         pl.lit("+=ai-internacional")),

        ("[Delegado de la rectora] 26G/16G, sub=R10 → dag-delegado",
         cap_ne_4 & tp.is_in(["26G", "16G"]) & (subcentro == "R10"),
         pl.lit("dag-delegado")),

        ("R17, VEPF, sub=VEF/MA → dag-vefp",
         base_r17 & (centro == "VEPF") & subcentro.is_in(["VEF", "MA"]),
         pl.lit("dag-vefp")),

        ("R17, subcentro vicerrect → actividad",
         base_r17 & subcentro.is_in(list(_SUBCENTRO_VICERRECT.keys())),
         subcentro.replace(
             list(_SUBCENTRO_VICERRECT.keys()),
             list(_SUBCENTRO_VICERRECT.values()),
         )),

        ("EST, sub=D7 → dag-consejo-estudiantes",
         cap_ne_4 & (tp == "EST") & (subcentro == "D7"),
         pl.lit("dag-consejo-estudiantes")),

        ("[DAG Vicerrectorados] 00G/00000, sub=R9 → dag-otros-servicios-promoción-fomento-igualdad",
         base_00g_00000 & (subcentro == "R9"),
         pl.lit("dag-otros-servicios-promoción-fomento-igualdad")),

        ("[DAG Vicerrectorados] 00G/00000, sub=I5 → dag-biblioteca",
         base_00g_00000 & (subcentro == "I5"),
         pl.lit("dag-biblioteca")),

        ("[DAG Vicerrectorados] 00G/00000, subcentro vicerrect → actividad",
         base_00g_00000 & subcentro.is_in(list(_SUBCENTRO_VICERRECT.keys())),
         subcentro.replace(
             list(_SUBCENTRO_VICERRECT.keys()),
             list(_SUBCENTRO_VICERRECT.values()),
         )),

        ("SLG/23G/20G, subcentro → actividad",
         cap_ne_4 & tp.is_in(["SLG", "23G", "20G"])
         & subcentro.is_in(list(_SUBCENTRO_R19.keys())),
         subcentro.replace(
             list(_SUBCENTRO_R19.keys()),
             list(_SUBCENTRO_R19.values()),
         )),

        ("QMG, proy=24G006, centro depto → actividad",
         cap_ne_4 & (tp == "QMG")
         & (pl.col("proyecto") == "24G006")
         & centro.is_in(list(_CENTRO_DEPTOS.keys())),
         centro.replace(
             list(_CENTRO_DEPTOS.keys()),
             list(_CENTRO_DEPTOS.values()),
         )),

        ("EST/26G/20G, centro=CENT → dag-cent",
         cap_ne_4 & tp.is_in(["EST", "26G", "20G"]) & (centro == "CENT"),
         pl.lit("dag-cent")),

        ("EST/26G/20G, subcentro → actividad",
         cap_ne_4 & tp.is_in(["EST", "26G", "20G"])
         & subcentro.is_in(list(_SUBCENTRO_R21.keys())),
         subcentro.replace(
             list(_SUBCENTRO_R21.keys()),
             list(_SUBCENTRO_R21.values()),
         )),

        ("COOP → +=dag-cooperación",
         cap_ne_4 & (tp == "COOP"),
         pl.lit("+=dag-cooperación")),

        ("21G, proy≠00000, sub=DS → dag-cooperación",
         cap_ne_4 & (tp == "21G")
         & (pl.col("proyecto") != "00000") & (subcentro == "DS"),
         pl.lit("dag-cooperación")),

        ("[Innovación y emprendimiento (es EMP)] 20G, sub=EMP, tl=00 → ait-financiación-propia",
         cap_ne_4 & (tp == "20G") & (subcentro == "EMP") & (tl == "00"),
         pl.lit("ait-financiación-propia")),

        ("[Innovación y emprendimiento (es EMP)] 20G, sub=EMP, tl≠00 → ait-financiación-externa",
         cap_ne_4 & (tp == "20G") & (subcentro == "EMP") & (tl != "00"),
         pl.lit("ait-financiación-externa")),

        ("[Tesis Doctorales] 07G → dag-doctorado",
         cap_ne_4 & (tp == "07G"),
         pl.lit("dag-doctorado")),

        ("[Formación permanente] EPM → +=másteres-formación-permanente",
         cap_ne_4 & (tp == "EPM"),
         pl.lit("+=másteres-formación-permanente")),

        ("[Formación permanente] EPDE → +=diplomas-especialización",
         cap_ne_4 & (tp == "EPDE"),
         pl.lit("+=diplomas-especialización")),

        ("[Formación permanente] EPDEX → +=diplomas-experto",
         cap_ne_4 & (tp == "EPDEX"),
         pl.lit("+=diplomas-experto")),

        ("[Formación permanente] EPC → +=cursos-formación-permanente",
         cap_ne_4 & (tp == "EPC"),
         pl.lit("+=cursos-formación-permanente")),

        ("[Formación permanente] EPMI → +=microcredenciales",
         cap_ne_4 & (tp == "EPMI"),
         pl.lit("+=microcredenciales")),

        ("[Formación permanente] CUID → +=cursos-idiomas",
         cap_ne_4 & (tp == "CUID"),
         pl.lit("+=cursos-idiomas")),

        ("[Formación permanente] CUEX → +=cursos-extranjeros",
         cap_ne_4 & (tp == "CUEX"),
         pl.lit("+=cursos-extranjeros")),

        ("[Formación permanente] PAU → +=acceso-enseñanzas-oficiales",
         cap_ne_4 & (tp == "PAU"),
         pl.lit("+=acceso-enseñanzas-oficiales")),

        ("[Sin proyectos] OAD, sub=UMAJ → universidad-mayores",
         cap_ne_4 & (tp == "OAD") & (subcentro == "UMAJ"),
         pl.lit("universidad-mayores")),

        ("[Sin proyectos] OAD, sub≠UMAJ → otros-docencia-propia",
         cap_ne_4 & (tp == "OAD") & (subcentro != "UMAJ"),
         pl.lit("otros-docencia-propia")),

        ("[Sin proyectos] OAT, tl=00 → ait-financiación-propia",
         cap_ne_4 & (tp == "OAT") & (tl == "00"),
         pl.lit("ait-financiación-propia")),

        ("[Sin proyectos] OAT, tl≠00 → ait-financiación-externa",
         cap_ne_4 & (tp == "OAT") & (tl != "00"),
         pl.lit("ait-financiación-externa")),

        ("DEP → deportes",
         cap_ne_4 & (tp == "DEP"),
         pl.lit("deportes")),

        ("10G, proy=24G112 → microcredenciales",
         cap_ne_4 & (tp == "10G") & (pl.col("proyecto") == "24G112"),
         pl.lit("microcredenciales")),

        ("09G → +=otras-extensión-universitaria",
         cap_ne_4 & (tp == "09G"),
         pl.lit("+=otras-extensión-universitaria")),

        ("15G, proy=0G009 → universidad-mayores",
         cap_ne_4 & (tp == "15G") & (pl.col("proyecto") == "0G009"),
         pl.lit("universidad-mayores")),

        ("15G, proy=9G008 → otras-extensión-universitaria",
         cap_ne_4 & (tp == "15G") & (pl.col("proyecto") == "9G008"),
         pl.lit("otras-extensión-universitaria")),

        ("18G → cultura",
         cap_ne_4 & (tp == "18G"),
         pl.lit("cultura")),

        ("19G → deportes",
         cap_ne_4 & (tp == "19G"),
         pl.lit("deportes")),

        ("VARI, 541-A, tl=00 → ait-financiación-propia",
         cap_ne_4 & (tp == "VARI") & prog_541a & (tl == "00"),
         pl.lit("ait-financiación-propia")),

        ("21I321 → +=ai-internacional",
         cap_ne_4 & (tp == "21I321"),
         pl.lit("+=ai-internacional")),

        ("23I373 → +=ai-nacional",
         cap_ne_4 & (tp == "23I373"),
         pl.lit("+=ai-nacional")),

        ("DAGI, tl=00 → dag-institutos-centros-investigación",
         cap_ne_4 & (tp == "DAGI") & (tl == "00"),
         pl.lit("dag-institutos-centros-investigación")),

        ("UEG, proy=22G045 → máster-geoespacial",
         cap_ne_4 & (tp == "UEG") & (pl.col("proyecto") == "22G045"),
         pl.lit("máster-geoespacial")),

        ("UEG, proy=22G132 → máster-robótica-marina",
         cap_ne_4 & (tp == "UEG") & (pl.col("proyecto") == "22G132"),
         pl.lit("máster-robótica-marina")),

        ("UEG, proy=22G131 → +=ai-internacional",
         cap_ne_4 & (tp == "UEG") & (pl.col("proyecto") == "22G131"),
         pl.lit("+=ai-internacional")),

        ("00G/00000, subcentro → actividad",
         base_00g_00000 & subcentro.is_in(list(_SUBCENTRO_00G.keys())),
         subcentro.replace(
             list(_SUBCENTRO_00G.keys()),
             list(_SUBCENTRO_00G.values()),
         )),
    ]

    # GEREN: prefijo aplicación → actividad (prefijos largos primero)
    for prefijo, act in sorted(
        _GEREN_PREFIJOS.items(), key=lambda kv: -len(kv[0])
    ):
        if len(prefijo) == 4:
            match = pl.col("aplicación") == prefijo
        else:
            match = pl.col("aplicación").str.starts_with(prefijo)
        reglas.append((
            f"GEREN: {prefijo} → {act}",
            base_00g_00000 & (centro == "GEREN") & match,
            pl.lit(act),
        ))

    # SC001: prefijo aplicación → actividad
    for prefijo, act in sorted(
        _SC001_PREFIJOS.items(), key=lambda kv: -len(kv[0])
    ):
        if len(prefijo) == 4:
            match = pl.col("aplicación") == prefijo
        else:
            match = pl.col("aplicación").str.starts_with(prefijo)
        reglas.append((
            f"SC001: {prefijo} → {act}",
            base_00g_00000 & (centro == "SC001") & match,
            pl.lit(act),
        ))

    # 00G/00000, centro → actividad (mapping)
    reglas.append((
        "00G/00000, centro → actividad",
        base_00g_00000 & centro.is_in(list(_CENTRO_00G.keys())),
        centro.replace(
            list(_CENTRO_00G.keys()),
            list(_CENTRO_00G.values()),
        ),
    ))

    # (00G + proyectos) o 14G → departamento (mapping)
    reglas.append((
        "00G/14G, centro depto → actividad",
        base_r50 & centro.is_in(list(_CENTRO_DEPTOS.keys())),
        centro.replace(
            list(_CENTRO_DEPTOS.keys()),
            list(_CENTRO_DEPTOS.values()),
        ),
    ))

    # 06G, proyecto=9G077, subcentro vicerrectorado: prefijo → actividad
    base_vicerrect = (
        cap_ne_4 & (tp == "06G")
        & (pl.col("proyecto") == "9G077")
        & subcentro.is_in(_SUBCENTROS_VICERRECTORADO)
    )
    for prefijo, act in sorted(
        _VICERRECT_PREFIJOS.items(), key=lambda kv: -len(kv[0])
    ):
        match = (
            pl.col("aplicación") == prefijo
            if len(prefijo) == 4
            else pl.col("aplicación").str.starts_with(prefijo)
        )
        reglas.append((
            f"06G, proy=9G077, sub vicerrect: {prefijo} → {act}",
            base_vicerrect & match,
            pl.lit(act),
        ))

    # 1G010/9G082, subcentro en lista: prefijo → dag-otros-servicios-*
    base_otros_servicios = (
        cap_ne_4
        & pl.col("proyecto").is_in(["1G010", "9G082"])
        & subcentro.is_in(_OTROS_SERVICIOS_SUBCENTROS)
    )
    for prefijo, act in sorted(
        _OTROS_SERVICIOS_PREFIJOS.items(), key=lambda kv: -len(kv[0])
    ):
        match = (
            pl.col("aplicación") == prefijo
            if len(prefijo) == 4
            else pl.col("aplicación").str.starts_with(prefijo)
        )
        reglas.append((
            f"1G010/9G082, sub→otros-servicios: {prefijo} → {act}",
            base_otros_servicios & match,
            pl.lit(act),
        ))

    # 8G022, departamento: prefijo → dag-{depto}-{sufijo}
    base_8g022 = (
        cap_ne_4
        & (pl.col("proyecto") == "8G022")
        & centro.is_in(list(_CENTRO_DEPTO_SLUG_8G022.keys()))
    )
    for prefijo, sufijo in sorted(
        _8G022_SUFIJOS.items(), key=lambda kv: -len(kv[0])
    ):
        match = (
            pl.col("aplicación") == prefijo
            if len(prefijo) == 4
            else pl.col("aplicación").str.starts_with(prefijo)
        )
        reglas.append((
            f"8G022, depto: {prefijo} → dag-DEPTO-{sufijo}",
            base_8g022 & match,
            pl.concat_str([
                pl.lit("dag-"),
                centro.replace(
                    list(_CENTRO_DEPTO_SLUG_8G022.keys()),
                    list(_CENTRO_DEPTO_SLUG_8G022.values()),
                ),
                pl.lit(f"-{sufijo}"),
            ]),
        ))

    # =============================================================
    # Aplicar reglas secuencialmente con conteo e importe
    # =============================================================
    df = df.with_columns(
        pl.lit(None).cast(pl.Utf8).alias("_actividad"),
        pl.lit(None).cast(pl.Utf8).alias("_regla_actividad"),
    )
    conteo_reglas: list[tuple[str, int, float]] = []

    for nombre, condición, resultado in reglas:
        sin_act = pl.col("_actividad").is_null()
        mask = sin_act & condición
        filtrado = df.filter(mask)
        n = filtrado.height
        imp = float(filtrado.select(pl.col("importe").sum()).item()) if n > 0 else 0.0
        if n > 0:
            df = df.with_columns(
                pl.when(mask).then(resultado)
                .otherwise(pl.col("_actividad"))
                .alias("_actividad"),
                pl.when(mask).then(pl.lit(nombre))
                .otherwise(pl.col("_regla_actividad"))
                .alias("_regla_actividad"),
            )
        conteo_reglas.append((nombre, n, imp))

    return df, conteo_reglas


# ======================================================================
# Reglas dinámicas (operador +): crean hijos en el árbol
# ======================================================================

def _reglas_actividad_dinámicas(
    df: pl.DataFrame,
    árbol: object | None,
    obtener_descripciones: callable,
) -> pl.DataFrame:
    """Aplica reglas con '+' que crean nodos hijos en el árbol.

    En la cadena when/then, estas reglas producen un marcador
    ``"+=padre_id"`` en ``_actividad``.  Aquí se resuelve cada
    marcador concatenando ``padre_id + "-" + valor_campo`` y se
    crean los nodos correspondientes en el árbol de actividades.
    """
    if árbol is None:
        return df

    # Definición de reglas dinámicas: (marcador, col_sufijo)
    reglas: list[tuple[str, str]] = [
        ("+=otras-ayudas-estudiantes", "proyecto"),
        ("+=cooperación", "proyecto"),
        ("+=dag-cooperación", "proyecto"),
        ("+=otras-extensión-universitaria", "proyecto"),
        ("+=ait-plan-propio", "proyecto"),
        ("+=cátedras-aulas-empresa", "proyecto"),
        ("+=ai-regional", "proyecto"),
        ("+=ai-nacional", "proyecto"),
        ("+=ai-internacional", "proyecto"),
        ("+=ai-otras-competitivas", "proyecto"),
        ("+=ppsi", "proyecto"),
        ("+=otras-ait-financiación-propia", "proyecto"),
        ("+=ait-financiación-propia", "proyecto"),
        ("+=ait-financiación-externa", "proyecto"),
        ("+=transf-60", "proyecto"),
        ("+=doctorado", "proyecto"),
        ("+=dag-otros-servicios-relaciones-internacionales", "proyecto"),
        ("+=másteres-formación-permanente", "proyecto"),
        ("+=diplomas-especialización", "proyecto"),
        ("+=diplomas-experto", "proyecto"),
        ("+=cursos-formación-permanente", "proyecto"),
        ("+=microcredenciales", "proyecto"),
        ("+=cursos-idiomas", "proyecto"),
        ("+=cursos-extranjeros", "proyecto"),
        ("+=acceso-enseñanzas-oficiales", "proyecto"),
    ]

    for marcador, col_sufijo in reglas:
        padre_id = marcador.removeprefix("+=")
        es_marcador = pl.col("_actividad") == marcador
        candidatos = df.filter(es_marcador)

        if candidatos.is_empty():
            continue

        # Valores únicos del campo sufijo
        valores = candidatos.select(col_sufijo).unique().to_series().to_list()

        # Obtener descripciones para los nuevos nodos
        descripciones = obtener_descripciones(col_sufijo, valores)

        # Crear nodos hijos en el árbol
        for valor in valores:
            desc = descripciones.get(str(valor), str(valor))
            try:
                árbol.añadir_hijo(padre_id, desc, str(valor))
            except ValueError as e:
                log.warning("No se pudo crear nodo: %s", e)

        # Reemplazar marcador por padre_id + "-" + valor
        df = df.with_columns(
            pl.when(es_marcador)
            .then(pl.concat_str([
                pl.lit(f"{padre_id}-"),
                pl.col(col_sufijo).cast(pl.Utf8),
            ]))
            .otherwise(pl.col("_actividad"))
            .alias("_actividad")
        )

    return df


# ======================================================================
# Expansión de repartos (un apunte → varias UC)
# ======================================================================

def _expandir_repartos(df: pl.DataFrame) -> pl.DataFrame:
    """Expande apuntes con marcador %%reparto:{proy} en múltiples UC.

    Para cada apunte marcado busca en _REPARTOS[proyecto][subproyecto]
    la lista de (porcentaje, actividad) que le corresponde y genera una
    fila por entrada, distribuyendo el importe (normalizado a 1).

    Los apuntes cuyo subproyecto no aparece en la tabla quedan sin
    clasificar (_actividad = null).

    Añade la columna ``_origen_porción`` a todas las filas:
    - 1.0 para las filas no-reparto
    - la fracción correspondiente para las expandidas
    """
    _PREFIJO = "%%reparto:"
    mask_reparto = pl.col("_actividad").str.starts_with(_PREFIJO).fill_null(False)
    normales = df.filter(~mask_reparto).with_columns(
        pl.lit(1.0).alias("_origen_porción")
    )
    con_reparto = df.filter(mask_reparto)

    if con_reparto.is_empty():
        return normales

    # Extraer el identificador de proyecto del marcador
    con_reparto = con_reparto.with_columns(
        pl.col("_actividad").str.strip_prefix(_PREFIJO).alias("_proy_reparto"),
        pl.col("subproyecto").cast(pl.Utf8).alias("_subproy_str"),
    )

    expandidas: list[pl.DataFrame] = []
    sin_match: list[pl.DataFrame] = []

    for proyecto, por_subproy in _REPARTOS.items():
        filas_proy = con_reparto.filter(pl.col("_proy_reparto") == proyecto)
        if filas_proy.is_empty():
            continue

        subproyectos_conocidos = list(por_subproy.keys())

        for subproyecto, fracciones in por_subproy.items():
            filas = filas_proy.filter(pl.col("_subproy_str") == subproyecto)
            if filas.is_empty():
                continue
            suma = sum(pct for pct, _ in fracciones)
            for pct, actividad in fracciones:
                fracción = pct / suma
                expandidas.append(
                    filas.with_columns(
                        (pl.col("importe").cast(pl.Float64) * fracción).alias("importe"),
                        pl.lit(actividad).alias("_actividad"),
                        pl.lit(fracción).alias("_origen_porción"),
                    )
                )

        # Apuntes cuyo subproyecto no está en la tabla → sin clasificar
        no_match = filas_proy.filter(
            ~pl.col("_subproy_str").is_in(subproyectos_conocidos)
        )
        if not no_match.is_empty():
            log.warning(
                "Reparto %s: %d apunte(s) con subproyecto desconocido: %s",
                proyecto,
                no_match.height,
                no_match["_subproy_str"].unique().to_list(),
            )
            sin_match.append(
                no_match.with_columns(
                    pl.lit(None).cast(pl.Utf8).alias("_actividad"),
                    pl.lit(1.0).alias("_origen_porción"),
                )
            )

    cols_aux = ["_proy_reparto", "_subproy_str"]
    partes: list[pl.DataFrame] = [normales]
    partes.extend(e.drop(cols_aux) for e in expandidas)
    partes.extend(s.drop(cols_aux) for s in sin_match)
    return pl.concat(partes)
