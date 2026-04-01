"""Clasificador de centros de coste compartido.

Extrae la lógica de clasificación de centros de coste del traductor de
presupuesto para que pueda ser reutilizada por otros módulos de la Fase 1
(presupuesto, nóminas, suministros, etc.).
"""

import logging
from typing import Callable

import polars as pl

from coana.util.arbol import Árbol

log = logging.getLogger(__name__)

# ======================================================================
# Constantes para clasificación de centros de coste
# ======================================================================

# Cátedras: CENTRO=INVES + proyecto en lista → "cátedras-investigación-{proyecto}"
_CC_CÁTEDRAS_PROYECTOS: list[str] = [
    "1I235", "12I327", "13I037", "15I116", "15I129", "16I028", "18I352",
    "19I055", "20I035", "21I159", "21I221", "21I242", "21I616", "21I633",
    "22I070", "22I242", "22I248", "22I618",
    "23G030", "23G044", "23G051", "23G069", "23G070", "23G071", "23G144",
    "24G012", "24G015", "24G019", "24G022", "24G025", "24G026", "24G028",
    "24G034", "24G035", "24I137", "24I256", "24I308", "24I557",
    "25I016", "25I030", "25I042", "25I130", "25I254",
    "09G013", "22G011",
]

# Pares (centro, subcentro) específicos — se comprueban primero
_CC_ESPECÍFICO: dict[tuple[str, str], str] = {
    ("VI", "ED"): "ed",
}

# Subcentro → centro de coste  (centro = % = cualquier valor)
_CC_SUBCENTRO: dict[str, str] = {
    "CP": "scp",
    "P3": "slt",
    "OL": "ol",
    "F3": "oppsm",
    "GA": "uadti",
    "I2": "uiic",
    "F2": "otop",
    "S9": "oir",
    "OC": "opaq",
    "L2": "ori",
    "GI": "oiati",
    "DI": "udd",
    "R9": "ui",
    "D2": "sgde",
    "IH": "oe",
    "D6": "oipep",
    "O3": "opp",
    "D8": "uo",
    "R1": "rectorado",
    "R5": "síndico-agravios",
    "R10": "delegado",
    "S1": "secretaría-general",
    "S4": "junta-electoral",
    "D7": "consejo-estudiantes",
    "C2": "deportes",
    "C3": "sasc",
    "DS": "ocds",
    "Y1": "estce",
    "J1": "fcje",
    "H1": "fchs",
    "SA": "fcs",
    "CC": "cent",
    "LB": "labcom",
    "I4": "ocit",
    "SD": "sala-disección",
    "ED": "ed",
}

# Centro → centro de coste  (subcentro = % = cualquier valor)
_CC_GENÉRICO: dict[str, str] = {
    "CENT": "cent",
    "CONSE": "consejo-social",
    "DADEM": "daem",
    "DCAMN": "dbbcn",
    "DCICO": "dcc",
    "DDPRI": "ddpri",
    "DDPUB": "ddpub",
    "DDTSE": "updtssee",
    "DEANG": "dea",
    "DECIC": "dicc",
    "DECON": "deco",
    "DEDES": "dede",
    "DEMEC": "dmc",
    "DESID": "desid",
    "DFICE": "dfs",
    "DFICO": "dfc",
    "DFISI": "dfis",
    "DFISO": "dfce",
    "DHIST": "dhga",
    "DINFE": "upi",
    "DIQUI": "deq",
    "DLSIN": "dlsi",
    "DMATE": "dmat",
    "DMEDI": "upm",
    "DPDID": "dpdcsll",
    "DPSIB": "dpbcp",
    "DPSIE": "dpeesm",
    "DQFIA": "dqfa",
    "DQUIO": "dqio",
    "DTRAD": "dtc",
    "ECTEC": "estce",
    "FCCHS": "fchs",
    "FCCJE": "fcje",
    "FCCS": "fcs",
    "GEREN": "gerencia",
    "IDL": "iidl",
    "IDSP": "idsp",
    "IEI": "iei",
    "IFV": "ifv",
    "IIG": "iigeo",
    "IILP": "ii-lópez-piñero",
    "IMAC": "imac",
    "INAM": "inam",
    "INIT": "init",
    "IUDT": "iudt",
    "IUEFG": "iuef",
    "IULMA": "ilma",
    "IUPA": "iupa",
    "IUTC": "iutc",
    "IUTUR": "iuturismo",
    "LABCOM": "labcom",
    "REC": "rectorado",
    "SCIC": "scic",
    "SEA": "sea",
    "SECRE": "secretaría-general",
    "UMAJ": "universidad-mayores",
    "VCLS": "vcls",
    "VEFP": "vefp",
    "VEVS": "vevs",
    "VI": "vi",
    "VINS": "vis",
    "VITDC": "vitdc",
    "VOAP": "voap",
    "VPEE": "vpee",
    "VRI": "vri",
    "VRSPII": "vrspii",
    "INVES": "otros-investigación",
}

# Aplicaciones de suministros centrales que se distribuyen por centro de coste
_APLICACIONES_SUMINISTROS_DISTRIBUIDOS: set[str] = {
    "2251", "2252", "2222", "2223", "2225",
}

# ======================================================================
# Mapeo servicio → centro de coste (para nóminas PTGAS y presupuesto)
# ======================================================================

# Mapeo servicio → (centro_de_coste, actividad) para retribuciones ordinarias PTGAS.
_SERVICIO_CC: dict[str, tuple[str, str]] = {
    "523": ("asesoría-jurídica", "dag-asesoría-jurídica"),
    "660": ("bibliotecas", "dag-biblioteca"),
    "640": ("cent", "dag-cent"),
    "263": ("consejo-social", "dag-consejo-social"),
    "2984": ("consejo-estudiantes", "dag-consejo-estudiantes"),
    "1862": ("cátedras-investigación-1I201", "otras-ait-financiación-propia-1I201"),
    "1662": ("cátedras-investigación-1I235", ""),
    "4267": ("delegado", "dag-delegado"),
    "101": ("daem", "dag-daem"),
    "93": ("deco", "dag-deco"),
    "3466": ("dea", "dag-dede"),
    "2103": ("dmc", "dag-dmc"),
    "81": ("deq", "dag-deq"),
    "2102": ("desid", "dag-desid"),
    "1442": ("dicc", "dag-dicc"),
    "1882": ("dea", "dag-dea"),
    "104": ("dhga", "dag-dhga"),
    "4207": ("dbbcn", "dag-dbbcn"),
    "2502": ("dcc", "dag-dcc"),
    "90": ("ddpub", "dag-ddpub"),
    "1883": ("dfce", "dag-dfce"),
    "2503": ("dfs", "dag-dfs"),
    "102": ("dfc", "dag-dfc"),
    "2283": ("dfis", "dag-dfis"),
    "1443": ("dlsi", "dag-dlsi"),
    "92": ("dmat", "dag-dmat"),
    "3465": ("dpdcsll", "dag-dpdcsll"),
    "97": ("dpbcp", "dag-dpbcp"),
    "96": ("dpeesm", "dag-dpeesm"),
    "2284": ("dqfa", "dag-dqfa"),
    "98": ("dqio", "dag-dqio"),
    "99": ("dtc", "dag-dtc"),
    "4": ("estce", "dag-estce"),
    "3165": ("ed", "dag-escuela-doctorado"),
    "2": ("fchs", "dag-fchs"),
    "3": ("fcje", "dag-fcje"),
    "2922": ("fcs", "dag-fcs"),
    "3405": ("rectorado", "dag-rectorado"),
    "261": ("gerencia", "dag-gerencia"),
    "4907": ("inspección-servicios", "dag-inspección-servicios"),
    "3145": ("iidl", "dag-iidl"),
    "3285": ("inam", "dag-inam"),
    "2603": ("init", "dag-init"),
    "2022": ("iupa", "dag-iupa"),
    "264": ("iutc", "dag-iutc"),
    "1982": ("labcom", "dag-labcom"),
    "4168": ("ol", "dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico"),
    "364": ("otop", "dag-otros-servicios-obras-proyectos"),
    "3408": ("oe", "dag-oe"),
    "3406": ("oir", "dag-otros-servicios-información-registro"),
    "3425": ("oiati", "dag-otros-servicios-ti"),
    "2883": ("oipep", "dag-oipep"),
    "1723": ("ocds", "cooperación"),
    "242": ("ocit", "dag-ocit"),
    "3847": ("opp", "dag-opp"),
    "4567": ("oppsm", "dag-otros-servicios-prevención-gestión-medioambiental"),
    "2882": ("ori", "dag-otros-servicios-relaciones-internacionales"),
    "1722": ("opaq", "dag-otros-servicios-promoción-evaluación-calidad"),
    "311": ("secretaría-general", "dag-secretaría-general"),
    "720": ("scic", "dag-scic"),
    "251": ("sasc", "cultura"),
    "760": ("se", "deportes"),
    "3004": ("sea", "dag-sea"),
    "1530": ("sic", "dag-sic"),
    "366": ("scp", "dag-otros-servicios-comunicación-publicaciones"),
    "1544": ("scag", "dag-scag"),
    "1529": ("sci", "dag-sci"),
    "1543": ("sge", "dag-sge"),
    "361": ("sgde", "dag-sgde"),
    "4887": ("sgit", "dag-sgit"),
    "350": ("slt", "dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico"),
    "362": ("srh", "dag-srh"),
    "2942": ("upi", "dag-upi"),
    "95": ("updtssee", "dag-updtssee"),
    "2943": ("upm", "dag-upm"),
    "3427": ("uadti", "dag-otros-servicios-ti"),
    "4167": ("gencisub", "dag-gencisub"),
    "2822": ("ui", "dag-otros-servicios-promoción-fomento-igualdad"),
    "218": ("uiic", "dag-otros-servicios-ti"),
    "4487": ("uo", "dag-uo"),
    "4687": ("udpea", "otras-extensión-universitaria-refinamiento"),
    "4488": ("udd", "dag-otros-servicios-atención-diversidad-apoyo-educativo"),
    "4489": ("ufie", "dag-ufie"),
    "344": ("sgit", "dag-sgit"),
    "3409": ("sgit", "dag-sgit"),
    "3445": ("sgit", "dag-sgit"),
    "345": ("sgit", "dag-sgit"),
    "347": ("sgit", "dag-sgit"),
    "346": ("sgit", "dag-sgit"),
    "348": ("sgit", "dag-sgit"),
    "349": ("sgit", "dag-sgit"),
    "2263": ("sgit", "dag-sgit"),
    "4647": ("sgit", "dag-sgit"),
    "2342": ("universidad-mayores", "universidad-mayores"),
    "4251": ("vevs", "dag-vevs"),
    "4252": ("vefp", "dag-vefp"),
    "4248": ("vis", "dag-vis"),
    "4250": ("vitdc", "dag-vitdc"),
    "4247": ("vi", "dag-vi"),
    "2224": ("voap", "dag-voap"),
    "4253": ("vcls", "dag-vcls"),
    "4255": ("vpee", "dag-vpee"),
    "4254": ("vri", "dag-vri"),
    "4249": ("vrspii", "dag-vrspii"),
}

# Excepción servicio 368: centro_plaza → (centro_de_coste, actividad).
_CENTRO_PLAZA_CC: dict[str, tuple[str, str]] = {
    "2": ("ps-fchs", "dag-conserjería-fchs"),
    "3": ("ps-fcje", "dag-conserjería-fcje"),
    "4": ("ps-estce", "dag-conserjería-estce"),
    "212": ("ps-rectorado", "dag-conserjería-rectorado"),
    "263": ("ps-escuela-doctorado-consejo-social", "dag-conserjería-consejo-social"),
    "2402": ("ps-parque-tecnológico", "dag-conserjería-parque-tecnológico"),
    "2922": ("ps-fcs", "dag-conserjería-fcs"),
}

# Proyectos ordinarios: si el proyecto está en esta lista, se aplica
# la regla de servicio para asignar centro de coste en presupuesto.
_PROYECTOS_ORDINARIOS: list[str] = [
    "1G019", "23G019", "02G041", "11G006", "1G046", "00000",
]


# ======================================================================
# Helpers internos
# ======================================================================


def _df_específico(mapping: dict[tuple[str, str], str], col_valor: str) -> pl.DataFrame:
    """Crea un DataFrame con columnas (centro, subcentro, col_valor)."""
    rows = [(c, s, v) for (c, s), v in mapping.items()]
    return pl.DataFrame(rows, schema=["centro", "subcentro", col_valor], orient="row")


def _df_genérico(mapping: dict[str, str], col_valor: str) -> pl.DataFrame:
    """Crea un DataFrame con columnas (centro, col_valor)."""
    rows = list(mapping.items())
    return pl.DataFrame(rows, schema=["centro", col_valor], orient="row")


# ======================================================================
# Función principal de clasificación
# ======================================================================


def clasificar_centros_coste(
    df: pl.DataFrame,
    árbol_cc: Árbol | None,
    distribución_costes: pl.DataFrame | None,
    obtener_descripciones: Callable[[str, list], dict[str, str]],
) -> tuple[pl.DataFrame, list[tuple[str, int, float]]]:
    """Asigna centro de coste a cada fila del DataFrame.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame con al menos las columnas: centro, subcentro, proyecto,
        aplicación, importe.  Opcionalmente: servicio, centro_plaza.
    árbol_cc : Árbol | None
        Árbol de centros de coste (se le añaden nodos de cátedras dinámicamente).
    distribución_costes : pl.DataFrame | None
        Tabla con columnas (centro, porcentaje) para distribución OTOP de
        suministros centrales (SC001 + aplicación específica).
    obtener_descripciones : Callable[[str, list], dict[str, str]]
        Función que, dado un nombre de columna y una lista de valores,
        devuelve un dict {valor: descripción}.

    Returns
    -------
    tuple[pl.DataFrame, list[tuple[str, int, float]]]
        (df_con_cc, conteo_cc)
        - df_con_cc: DataFrame original con columnas ``_centro_de_coste``
          y ``_regla_cc`` añadidas.
        - conteo_cc: lista de tuplas (nombre_regla, n_filas, importe)
          con las estadísticas de asignación.
    """
    # Sentinel para marcar filas que se distribuirán después
    _SENTINEL_DISTRIBUIR = "__DISTRIBUIR__"

    # -1. Suministros distribuidos: SC001 + aplicaciones → se expanden después
    df = df.with_columns(
        pl.when(
            (pl.col("centro") == "SC001")
            & pl.col("aplicación").is_in(
                list(_APLICACIONES_SUMINISTROS_DISTRIBUIDOS)
            )
        )
        .then(pl.lit(_SENTINEL_DISTRIBUIR))
        .otherwise(pl.lit(None))
        .cast(pl.Utf8)
        .alias("_cc_sum")
    )

    # 0. Cátedras: centro=INVES + proyecto en lista
    df = df.with_columns(
        pl.when(
            (pl.col("centro") == "INVES")
            & pl.col("proyecto").is_in(_CC_CÁTEDRAS_PROYECTOS)
        )
        .then(pl.concat_str([
            pl.lit("cátedras-investigación-"),
            pl.col("proyecto"),
        ]))
        .otherwise(pl.lit(None))
        .cast(pl.Utf8)
        .alias("_cc_cát")
    )

    # 1. Pares específicos
    df_esp = _df_específico(_CC_ESPECÍFICO, "_cc_esp")
    df = df.join(df_esp, on=["centro", "subcentro"], how="left")

    # 2. Por servicio: solo cuando servicio no es null y proyecto es ordinario
    tiene_servicio = "servicio" in df.columns
    if tiene_servicio:
        # Construir DataFrame de mapeo servicio → centro de coste
        srv_rows = [(k, v[0]) for k, v in _SERVICIO_CC.items()]
        df_srv = pl.DataFrame(
            srv_rows, schema=["servicio", "_cc_srv"], orient="row",
        )
        tiene_centro_plaza = "centro_plaza" in df.columns
        # Excepción servicio 368: mapeo por centro_plaza
        if tiene_centro_plaza:
            cp_rows = [(k, v[0]) for k, v in _CENTRO_PLAZA_CC.items()]
            df_cp = pl.DataFrame(
                cp_rows, schema=["centro_plaza", "_cc_cp"], orient="row",
            )
            df = df.join(df_cp, on="centro_plaza", how="left")
        # Join con mapeo de servicio
        df = df.join(df_srv, on="servicio", how="left")
        # La regla de servicio solo aplica a proyectos ordinarios
        es_ordinario = pl.col("proyecto").cast(pl.Utf8).is_in(_PROYECTOS_ORDINARIOS)
        srv_no_null = pl.col("servicio").is_not_null()
        if tiene_centro_plaza:
            # Servicio 368 usa centro_plaza; el resto usa servicio
            es_368 = pl.col("servicio").cast(pl.Utf8) == "368"
            df = df.with_columns(
                pl.when(srv_no_null & es_ordinario & es_368)
                .then(pl.col("_cc_cp"))
                .when(srv_no_null & es_ordinario & ~es_368)
                .then(pl.col("_cc_srv"))
                .otherwise(pl.lit(None))
                .cast(pl.Utf8)
                .alias("_cc_servicio")
            ).drop("_cc_srv", "_cc_cp")
        else:
            df = df.with_columns(
                pl.when(srv_no_null & es_ordinario)
                .then(pl.col("_cc_srv"))
                .otherwise(pl.lit(None))
                .cast(pl.Utf8)
                .alias("_cc_servicio")
            ).drop("_cc_srv")
    else:
        df = df.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("_cc_servicio")
        )

    # 3. Subcentro genérico
    df_sub = pl.DataFrame(
        list(_CC_SUBCENTRO.items()),
        schema=["subcentro", "_cc_sub"],
        orient="row",
    )
    df = df.join(df_sub, on="subcentro", how="left")

    # 4. Centro genérico
    df_gen = _df_genérico(_CC_GENÉRICO, "_cc_gen")
    df = df.join(df_gen, on="centro", how="left")

    # Crear nodos hijos en el árbol de centros de coste para cátedras
    if árbol_cc is not None:
        cátedras_df = df.filter(pl.col("_cc_cát").is_not_null())
        if not cátedras_df.is_empty():
            valores = (
                cátedras_df
                .select(pl.col("proyecto").cast(pl.Utf8))
                .unique()
                .to_series()
                .to_list()
            )
            descripciones = obtener_descripciones("proyecto", valores)
            for valor in valores:
                if str(valor) == "1I235":
                    desc = "CATEDRAS UNESCO"
                else:
                    desc = descripciones.get(str(valor), str(valor))
                try:
                    árbol_cc.añadir_hijo(
                        "cátedras-investigación", desc, str(valor)
                    )
                except ValueError as e:
                    log.warning("No se pudo crear nodo CC: %s", e)

    # Conteo por nivel de regla (antes de coalescer)
    summ = pl.col("_cc_sum")
    cát = pl.col("_cc_cát")
    esp = pl.col("_cc_esp")
    srv = pl.col("_cc_servicio")
    sub = pl.col("_cc_sub")
    gen = pl.col("_cc_gen")

    def _cc_stats(mask_expr: pl.Expr) -> tuple[int, float]:
        filtrado = df.filter(mask_expr)
        n = filtrado.height
        imp = float(filtrado.select(pl.col("importe").sum()).item()) if n > 0 else 0.0
        return n, imp

    n_sum, i_sum = _cc_stats(summ.is_not_null())
    no_sum = summ.is_null()
    n_cát, i_cát = _cc_stats(no_sum & cát.is_not_null())
    n_esp, i_esp = _cc_stats(no_sum & cát.is_null() & esp.is_not_null())
    n_srv, i_srv = _cc_stats(
        no_sum & cát.is_null() & esp.is_null() & srv.is_not_null()
    )
    n_sub, i_sub = _cc_stats(
        no_sum & cát.is_null() & esp.is_null() & srv.is_null() & sub.is_not_null()
    )
    n_gen, i_gen = _cc_stats(
        no_sum & cát.is_null() & esp.is_null() & srv.is_null() & sub.is_null() & gen.is_not_null()
    )
    n_sin, i_sin = _cc_stats(
        no_sum & cát.is_null() & esp.is_null() & srv.is_null() & sub.is_null() & gen.is_null()
    )

    conteo_cc: list[tuple[str, int, float]] = [
        ("[Suministros distribuidos] SC001 + aplicación", n_sum, i_sum),
        ("[Cátedras y aulas de empresa] INVES + proyecto", n_cát, i_cát),
        ("Par específico (centro/subcentro)", n_esp, i_esp),
        ("Por servicio (proyecto ordinario)", n_srv, i_srv),
        ("Subcentro (%/subcentro)", n_sub, i_sub),
        ("Centro genérico (centro/%)", n_gen, i_gen),
        ("Sin asignar", n_sin, i_sin),
    ]

    # Regla CC aplicada a cada fila
    df = df.with_columns(
        pl.when(summ.is_not_null()).then(pl.lit("[Suministros distribuidos] SC001 + aplicación"))
        .when(cát.is_not_null()).then(pl.lit("[Cátedras y aulas de empresa] INVES + proyecto"))
        .when(esp.is_not_null()).then(pl.lit("Par específico (centro/subcentro)"))
        .when(srv.is_not_null()).then(pl.lit("Por servicio (proyecto ordinario)"))
        .when(sub.is_not_null()).then(pl.lit("Subcentro (%/subcentro)"))
        .when(gen.is_not_null()).then(pl.lit("Centro genérico (centro/%)"))
        .otherwise(pl.lit("Sin asignar"))
        .alias("_regla_cc")
    )

    # Coalescer: suministros > cátedras > específico > servicio > subcentro > genérico
    df = df.with_columns(
        pl.coalesce("_cc_sum", "_cc_cát", "_cc_esp", "_cc_servicio", "_cc_sub", "_cc_gen")
        .alias("_centro_de_coste")
    ).drop("_cc_sum", "_cc_cát", "_cc_esp", "_cc_servicio", "_cc_sub", "_cc_gen")

    return df, conteo_cc
