"""Generación de unidades de coste a partir de amortizaciones.

Para cada registro del inventario enriquecido, genera una UC con:
- elemento de coste según la CUENTA (tabla de 53 entradas)
- centro de coste vía id_ubicación → servicio → centro
- actividad = "dags"
"""

from dataclasses import dataclass

import polars as pl

from coana.fase1.inventario.contexto import ContextoInventario
from coana.fase1.inventario.procesamiento import ResultadoInventario


# ======================================================================
# Tabla CUENTA → elemento de coste (de la especificación 3-2-3)
# ======================================================================

_CUENTA_A_ELEMENTO: dict[str, str] = {
    "2020": "amortización-construcciones",
    "2021": "amortización-construcciones",
    "2022": "amortización-construcciones",
    "2023": "amortización-construcciones",
    "2024": "amortización-construcciones",
    "2025": "amortización-construcciones",
    "2026": "amortización-construcciones",
    "2027": "amortización-construcciones",
    "2030": "amortización-maquinaria",
    "2031": "amortización-instalaciones",
    "2032": "amortización-transporte",
    "2033": "amortización-utillaje",
    "2034": "amortización-utillaje",
    "2035": "amortización-utillaje",
    "2040": "amortización-transporte",
    "2050": "amortización-mobiliario",
    "2051": "amortización-otro-inmovilizado-material",
    "2052": "amortización-mobiliario",
    "2056": "amortización-mobiliario",
    "2059": "amortización-mobiliario",
    "2060": "amortización-equipos-informáticos",
    "2110": "amortización-construcciones",
    "2111": "amortización-construcciones",
    "2112": "amortización-construcciones",
    "2113": "amortización-construcciones",
    "2114": "amortización-construcciones",
    "2115": "amortización-construcciones",
    "2117": "amortización-construcciones",
    "2140": "amortización-maquinaria",
    "2141": "amortización-instalaciones",
    "2142": "amortización-transporte",
    "2143": "amortización-utillaje",
    "2144": "amortización-utillaje",
    "2145": "amortización-utillaje",
    "2150": "amortización-aplicaciones-informáticas",
    "2160": "amortización-mobiliario",
    "2161": "amortización-otro-inmovilizado-material",
    "2162": "amortización-mobiliario",
    "2165": "amortización-mobiliario",
    "2169": "amortización-mobiliario",
    "2170": "amortización-equipos-informáticos",
}


@dataclass
class EstadísticaAmortización:
    """Estadísticas de la generación de UC de amortizaciones para la traza."""

    n_registros: int
    n_con_centro: int
    n_sin_centro: int
    importe_con_centro: float
    importe_sin_centro: float
    n_con_múltiples_centros: int
    n_cuenta_sin_elemento: int
    # Distribución por elemento de coste
    por_elemento: pl.DataFrame  # (elemento_de_coste, n, importe)
    # Top centros por importe
    por_centro: pl.DataFrame  # (centro_de_coste, n, importe)


def generar_uc_amortizaciones(
    resultado_inv: ResultadoInventario,
    ctx_inv: ContextoInventario,
) -> tuple[pl.DataFrame, pl.DataFrame, EstadísticaAmortización]:
    """Genera unidades de coste a partir del inventario enriquecido.

    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame, EstadísticaAmortización]
        (uc_amortizaciones, sin_uc_amortizaciones, estadísticas)
    """
    inv = resultado_inv.inventario_enriquecido

    # -- 1. Asignar elemento de coste por cuenta --
    mapa_elem = pl.DataFrame({
        "cuenta": list(_CUENTA_A_ELEMENTO.keys()),
        "elemento_de_coste": list(_CUENTA_A_ELEMENTO.values()),
    })

    inv_con_elem = inv.join(mapa_elem, on="cuenta", how="left")

    n_sin_elem = inv_con_elem.filter(
        pl.col("elemento_de_coste").is_null()
    ).height
    if n_sin_elem > 0:
        cuentas_faltantes = (
            inv_con_elem.filter(pl.col("elemento_de_coste").is_null())
            .select("cuenta")
            .unique()
            .to_series()
            .to_list()
        )
        print(f"  AVISO: {n_sin_elem} registros con cuenta sin mapeo de elemento: {cuentas_faltantes}")

    # -- 2. Asignar centro de coste vía ubicación → servicio → centro --
    assert ctx_inv.ubicaciones_a_servicios is not None
    assert ctx_inv.servicios is not None

    # Mapa servicio → centro (solo vivos con centro asignado)
    mapa_centro = (
        ctx_inv.servicios.filter(
            (pl.col("vivo") == 1) & pl.col("centro").is_not_null()
        )
        .select("servicio", "centro")
    )

    # id_ubicación → servicios → centros
    ubic_centros = (
        ctx_inv.ubicaciones_a_servicios
        .join(mapa_centro, on="servicio", how="inner")
        .select(
            pl.col("ubicación").alias("id_ubicación"),
            "centro",
        )
    )

    # Contar servicios por ubicación para distribución proporcional
    n_centros_por_ubic = (
        ubic_centros.group_by("id_ubicación")
        .agg(pl.len().alias("n_centros"))
    )

    # Join inventario con centros
    inv_con_centro = (
        inv_con_elem
        .join(ubic_centros, on="id_ubicación", how="left")
        .join(n_centros_por_ubic, on="id_ubicación", how="left")
        .with_columns(
            pl.col("n_centros").fill_null(0),
        )
    )

    # Separar con y sin centro
    sin_centro_0 = inv_con_centro.filter(pl.col("centro").is_null())
    con_centro = inv_con_centro.filter(pl.col("centro").is_not_null())

    # Regla de centro por DESCRIPCIÓN para registros sin id_ubicación.
    # La comparación es case-insensitive y sin acentos.
    _QUITAR_ACENTOS = str.maketrans(
        "áéíóúàèìòùäëïöüâêîôûñ",
        "aeiouaeiouaeiouaeioun",
    )

    def _normalizar(s: str | None) -> str:
        if s is None:
            return ""
        return s.lower().translate(_QUITAR_ACENTOS)

    sin_centro_0 = sin_centro_0.with_columns(
        pl.col("descripción").cast(pl.Utf8).map_elements(
            _normalizar, return_dtype=pl.Utf8
        ).alias("_desc_norm"),
    )

    # Casos especiales: split a múltiples centros.
    rescatados_partes: list[pl.DataFrame] = []

    # "rectorado" Y "biblioteca" → 50% a cada centro.
    es_rect_bib = (
        sin_centro_0["_desc_norm"].str.contains("rectorado", literal=True)
        & sin_centro_0["_desc_norm"].str.contains("biblioteca", literal=True)
    )
    rect_bib = sin_centro_0.filter(es_rect_bib).drop("_desc_norm")
    resto_sin = sin_centro_0.filter(~es_rect_bib)
    if not rect_bib.is_empty():
        for cc in ("rectorado", "bibliotecas"):
            rescatados_partes.append(
                rect_bib.with_columns(
                    pl.lit(cc).alias("centro"),
                    pl.lit(2).cast(pl.UInt32).alias("n_centros"),
                )
            )

    # "mat" o "inf" → 33% dmat, 34% dlsi, 33% dicc.
    es_mat_inf = (
        resto_sin["_desc_norm"].str.contains("mat", literal=True)
        | resto_sin["_desc_norm"].str.contains("inf", literal=True)
    )
    mat_inf = resto_sin.filter(es_mat_inf).drop("_desc_norm")
    resto_sin = resto_sin.filter(~es_mat_inf)
    if not mat_inf.is_empty():
        for cc in ("dmat", "dlsi", "dicc"):
            rescatados_partes.append(
                mat_inf.with_columns(
                    pl.lit(cc).alias("centro"),
                    pl.lit(3).cast(pl.UInt32).alias("n_centros"),
                )
            )

    # Reglas simples para el resto
    resto_sin = resto_sin.with_columns(
        pl.when(
            pl.col("_desc_norm").str.contains("humanas", literal=True)
            | pl.col("_desc_norm").str.contains("fchs", literal=True)
        )
        .then(pl.lit("fchs"))
        .when(
            pl.col("_desc_norm").str.contains("jurid", literal=True)
            | pl.col("_desc_norm").str.contains("jj", literal=True)
            | pl.col("_desc_norm").str.contains("jco", literal=True)
            | pl.col("_desc_norm").str.contains("jc", literal=True)
        )
        .then(pl.lit("fcje"))
        .when(pl.col("_desc_norm").str.contains("salud", literal=True))
        .then(pl.lit("fcs"))
        .when(
            pl.col("_desc_norm").str.contains("tecnologia", literal=True)
            | pl.col("_desc_norm").str.contains("ciencias experimentales", literal=True)
            | pl.col("_desc_norm").str.contains("cientifico", literal=True)
            | pl.col("_desc_norm").str.contains("tec", literal=True)
            | pl.col("_desc_norm").str.contains("talleres", literal=True)
            | pl.col("_desc_norm").str.contains("estce", literal=True)
            | pl.col("_desc_norm").str.contains("tc", literal=True)
        )
        .then(pl.lit("estce"))
        .when(
            pl.col("_desc_norm").str.contains("rectorat", literal=True)
            | pl.col("_desc_norm").str.contains("rectorado", literal=True)
        )
        .then(pl.lit("rectorado"))
        .when(pl.col("_desc_norm").str.contains("itc", literal=True))
        .then(pl.lit("iutc"))
        .when(
            pl.col("_desc_norm").str.contains("lonja", literal=True)
            | pl.col("_desc_norm").str.contains("llotja", literal=True)
        )
        .then(pl.lit("llotja-cànem"))
        .when(pl.col("_desc_norm").str.contains("biblioteca", literal=True))
        .then(pl.lit("bibliotecas"))
        .when(pl.col("_desc_norm").str.contains("consell", literal=True))
        .then(pl.lit("consejo-social"))
        .when(
            pl.col("_desc_norm").str.contains("modulo ti", literal=True)
            | pl.col("_desc_norm").str.contains("estyce", literal=True)
        )
        .then(pl.lit("estce"))
        .when(pl.col("_desc_norm").str.contains("scic", literal=True))
        .then(pl.lit("scic"))
        .when(pl.col("_desc_norm").str.contains("deport", literal=True))
        .then(pl.lit("se"))
        .when(pl.col("_desc_norm").str.contains("paran", literal=True))
        .then(pl.lit("paraninfo"))
        .when(
            pl.col("_desc_norm").str.contains("nb", literal=True)
            | pl.col("_desc_norm").str.contains("investigacio ii", literal=True)
        )
        .then(pl.lit("edificio-investigación-2"))
        .when(pl.col("_desc_norm").str.contains("piscina", literal=True))
        .then(pl.lit("se"))
        .when(pl.col("_desc_norm").str.contains("central del parc", literal=True))
        .then(pl.lit("edificio-espaitec-2"))
        .when(pl.col("_desc_norm").str.contains("animal", literal=True))
        .then(pl.lit("sea"))
        .when(
            (
                pl.col("_desc_norm").str.contains("docente", literal=True)
                | pl.col("_desc_norm").str.contains("direccion obra", literal=True)
            )
            & (pl.col("fecha_alta").dt.year() == 1998)
        )
        .then(pl.lit("estce"))
        .when(pl.col("_desc_norm").str.contains("residencia", literal=True))
        .then(pl.lit("residencia-universitaria"))
        .otherwise(pl.col("centro"))
        .alias("centro"),
    ).drop("_desc_norm")

    rescatados_simple = resto_sin.filter(pl.col("centro").is_not_null()).with_columns(
        pl.lit(1).cast(pl.UInt32).alias("n_centros"),
    )
    sin_centro = resto_sin.filter(pl.col("centro").is_null())

    all_rescatados = rescatados_partes + (
        [rescatados_simple] if not rescatados_simple.is_empty() else []
    )
    if all_rescatados:
        con_centro = pl.concat([con_centro] + all_rescatados, how="diagonal")

    # Distribuir proporcionalmente si múltiples centros
    con_centro = con_centro.with_columns(
        (pl.col("importe") / pl.col("n_centros")).alias("importe_uc"),
        (1.0 / pl.col("n_centros")).alias("origen_porción"),
    )

    n_múltiples = con_centro.filter(pl.col("n_centros") > 1).select("id").n_unique()

    # -- 3. Construir DataFrame de UCs --
    if not con_centro.is_empty():
        uc = (
            con_centro
            .with_row_index("_seq")
            .with_columns(
                pl.concat_str([
                    pl.lit("A-"),
                    (pl.col("_seq") + 1).cast(pl.Utf8).str.zfill(5),
                ]).alias("uc_id"),
            )
            .select(
                pl.col("uc_id").alias("id"),
                pl.col("elemento_de_coste").fill_null("").alias("elemento_de_coste"),
                pl.col("centro").alias("centro_de_coste"),
                pl.lit("dags").alias("actividad"),
                pl.col("importe_uc").alias("importe"),
                pl.lit("inventario").alias("origen"),
                pl.col("id").cast(pl.Utf8).alias("origen_id"),
                pl.col("origen_porción"),
            )
        )
    else:
        uc = pl.DataFrame(
            schema={
                "id": pl.Utf8,
                "elemento_de_coste": pl.Utf8,
                "centro_de_coste": pl.Utf8,
                "actividad": pl.Utf8,
                "importe": pl.Float64,
                "origen": pl.Utf8,
                "origen_id": pl.Utf8,
                "origen_porción": pl.Float64,
            }
        )

    # -- 4. DataFrame de registros sin centro --
    if not sin_centro.is_empty():
        sin_uc = sin_centro.select(
            [c for c in inv.columns]
        )
    else:
        sin_uc = pl.DataFrame(schema={c: inv.dtypes[i] for i, c in enumerate(inv.columns)})

    # -- 5. Estadísticas --
    importe_con = float(uc["importe"].sum()) if not uc.is_empty() else 0.0
    importe_sin = float(sin_centro["importe"].sum()) if not sin_centro.is_empty() else 0.0

    if not uc.is_empty():
        por_elem = (
            uc.group_by("elemento_de_coste")
            .agg(
                pl.len().alias("n"),
                pl.col("importe").sum().alias("importe"),
            )
            .sort("importe", descending=True)
        )
        por_centro = (
            uc.group_by("centro_de_coste")
            .agg(
                pl.len().alias("n"),
                pl.col("importe").sum().alias("importe"),
            )
            .sort("importe", descending=True)
        )
    else:
        por_elem = pl.DataFrame(schema={"elemento_de_coste": pl.Utf8, "n": pl.UInt32, "importe": pl.Float64})
        por_centro = pl.DataFrame(schema={"centro_de_coste": pl.Utf8, "n": pl.UInt32, "importe": pl.Float64})

    estadísticas = EstadísticaAmortización(
        n_registros=len(inv),
        n_con_centro=len(con_centro),
        n_sin_centro=len(sin_centro),
        importe_con_centro=importe_con,
        importe_sin_centro=importe_sin,
        n_con_múltiples_centros=n_múltiples,
        n_cuenta_sin_elemento=n_sin_elem,
        por_elemento=por_elem,
        por_centro=por_centro,
    )

    print(
        f"  Amortizaciones: {len(inv)} registros → "
        f"{len(uc)} UC ({importe_con:,.2f} €), "
        f"{len(sin_centro)} sin centro ({importe_sin:,.2f} €)"
    )
    if n_múltiples > 0:
        print(f"    Registros con múltiples centros: {n_múltiples}")

    return uc, sin_uc, estadísticas
