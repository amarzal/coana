"""Procesamiento y filtro previo de amortizaciones.

Implementa los pasos descritos en la especificación
``2-2-2 procesamiento y filtro previo.md``:

0. Filtro por estado (solo estado = "A").
1. Filtro por cuenta contable (solo prefijos válidos).
2. Filtro por fecha (período de amortización fuera del año contable).
3. Enriquecimiento del inventario (cálculo de importes de amortización).
4. Filtro por falta de información (registros sin cuenta).
5. Cálculo de m² por zona, edificación y complejo.
6. Matrices de presencia de cada centro de coste en zonas, edificaciones, complejos y UJI.

La jerarquía espacial es: complejo → edificación → zona.
En ``ubicaciones.xlsx`` el complejo se llama ``área`` y la combinación
edificación+zona se llama ``edificio`` (dos caracteres).
"""

from dataclasses import dataclass
from datetime import date

import polars as pl

from coana.fase1.inventario.contexto import ContextoInventario

# Columnas de ubicaciones que identifican una zona.
# (``área`` = complejo, ``edificio`` = edificación_zona)
_ZONA = ["área", "edificio"]


@dataclass
class ResultadoInventario:
    """Resultado del preprocesamiento de inventario."""

    inventario_enriquecido: pl.DataFrame
    metros_por_zona: pl.DataFrame
    metros_por_edificación: pl.DataFrame
    metros_por_complejo: pl.DataFrame
    # Matrices de presencia de centros de coste
    presencia_zona: pl.DataFrame        # (área, edificio, centro, m2, pct)
    presencia_edificación: pl.DataFrame  # (área, edificación, centro, m2, pct)
    presencia_complejo: pl.DataFrame     # (área, centro, m2, pct)
    presencia_uji: pl.DataFrame          # (centro, m2, pct)
    # Estadísticas de filtro y enriquecimiento
    n_registros_original: int
    valor_inicial_total: float
    n_filtrados_estado: int
    valor_inicial_filtrados_estado: float
    n_tras_filtro_estado: int
    valor_inicial_tras_filtro_estado: float
    n_sin_cuenta: int
    valor_inicial_sin_cuenta: float
    n_filtrados_cuenta: int
    valor_inicial_filtrados_cuenta: float
    detalle_cuentas_filtradas: pl.DataFrame  # (cuenta, n, valor_inicial)
    n_tras_filtro_cuenta: int
    valor_inicial_tras_filtro_cuenta: float
    n_sin_fecha_alta: int
    valor_inicial_sin_fecha_alta: float
    n_filtrados_fecha: int
    valor_inicial_filtrados_fecha: float
    n_registros_tras_filtro: int
    valor_inicial_tras_filtro: float
    importe_total: float
    # Metros y centros de coste
    n_zonas_sin_asignación: int
    n_zonas_con_asignación: int
    n_centros_con_presencia: int
    m2_asignados: float
    m2_sin_asignar: float
    m2_total_campus: float
    # Distribución de costes OTOP por centro
    distribución_costes: pl.DataFrame  # (centro, porcentaje)
    detalle_distribución: pl.DataFrame  # (centro, prefijo, comentario, pct_distribución, pct_presencia, contribución)
    prefijos_duplicados: list[tuple[str, int, float]]  # (prefijo, n_filas, pct_total)
    prefijos_sin_match: list[tuple[str, float, str]]   # (prefijo, pct, comentario)
    # DataFrames intermedios para el visor
    filtrados_estado_df: pl.DataFrame
    sin_cuenta_df: pl.DataFrame
    filtrados_cuenta_df: pl.DataFrame
    sin_fecha_alta_df: pl.DataFrame
    filtrados_fecha_df: pl.DataFrame


# Prefijos de 3 dígitos válidos para cuentas contables de inventario.
_PREFIJOS_CUENTA = ("202", "203", "204", "205", "206", "211", "214", "215", "216", "217", "218")


# ======================================================================
# Filtros y enriquecimiento del inventario
# ======================================================================

@dataclass
class _InfoFiltros:
    """Resultados intermedios del filtrado y enriquecimiento."""

    df: pl.DataFrame
    n_filtrados_estado: int
    vi_filtrados_estado: float
    n_tras_filtro_estado: int
    vi_tras_filtro_estado: float
    n_sin_cuenta: int
    vi_sin_cuenta: float
    n_filtrados_cuenta: int
    vi_filtrados_cuenta: float
    detalle_cuentas_filtradas: pl.DataFrame  # (cuenta, n, valor_inicial)
    n_tras_filtro_cuenta: int
    vi_tras_filtro_cuenta: float
    n_sin_fecha_alta: int
    vi_sin_fecha_alta: float
    n_filtrados_fecha: int
    vi_filtrados_fecha: float
    # DataFrames intermedios para el visor
    filtrados_estado_df: pl.DataFrame
    sin_cuenta_df: pl.DataFrame
    filtrados_cuenta_df: pl.DataFrame
    sin_fecha_alta_df: pl.DataFrame
    filtrados_fecha_df: pl.DataFrame


def _filtrar_y_enriquecer(
    inventario: pl.DataFrame,
    años_amort: pl.DataFrame,
    año: int,
) -> _InfoFiltros:
    """Filtra y enriquece el inventario con importes de amortización para *año*.

    Orden de procesamiento (siguiendo la especificación):
    0. Filtro por estado (solo estado = "A").
    1. Filtro por cuenta contable (solo prefijos válidos).
    2. Filtro por fecha (sin fecha_alta + importe/días tras enriquecimiento).
    3. Enriquecimiento (join con años_amortización + cálculo de importe).
    4. Filtro por falta de información (registros sin cuenta).
    """
    inicio_año = date(año, 1, 1)
    inicio_siguiente = date(año + 1, 1, 1)
    días_del_año = (inicio_siguiente - inicio_año).days

    # -- 0. Filtro por estado (descartar solo "B" = baja) --
    no_activos = inventario.filter(pl.col("estado") == "B")
    activos = inventario.filter(pl.col("estado") != "B")

    # -- Separar registros sin cuenta (se reportan en paso 4) --
    sin_cuenta = activos.filter(
        pl.col("cuenta").is_null() | (pl.col("cuenta") == "")
    )
    con_cuenta = activos.filter(
        pl.col("cuenta").is_not_null() & (pl.col("cuenta") != "")
    )

    # -- 1. Filtro por cuenta contable (prefijos válidos) --
    condición_prefijo = pl.lit(False)
    for prefijo in _PREFIJOS_CUENTA:
        condición_prefijo = condición_prefijo | pl.col("cuenta").str.starts_with(prefijo)

    con_prefijo_válido = con_cuenta.filter(condición_prefijo)
    sin_prefijo_válido = con_cuenta.filter(~condición_prefijo)

    detalle_filtradas = (
        sin_prefijo_válido.group_by("cuenta")
        .agg(
            pl.len().alias("n"),
            pl.col("valor_inicial").sum(),
        )
        .sort("valor_inicial", descending=True)
    )

    # -- 2a. Filtro por fecha: registros sin fecha de alta --
    sin_fecha_alta = con_prefijo_válido.filter(
        pl.col("fecha_alta").is_null()
    )
    con_fecha_alta = con_prefijo_válido.filter(
        pl.col("fecha_alta").is_not_null()
    )

    # -- 3. Enriquecimiento (join + cálculo de importe) --
    df = con_fecha_alta.join(
        años_amort.select("cuenta", "años_amortización"),
        on="cuenta",
        how="inner",
    )

    # Caso especial: cuenta "2060" → 6 años si fecha_alta > 2018-12-31
    df = df.with_columns(
        pl.when(
            (pl.col("cuenta") == "2060")
            & (pl.col("fecha_alta").cast(pl.Date) > date(2018, 12, 31))
        )
        .then(pl.lit(6))
        .otherwise(pl.col("años_amortización"))
        .alias("años_amortización")
    )

    df = df.with_columns(
        pl.col("fecha_alta").cast(pl.Date).alias("_fecha_alta"),
    ).with_columns(
        pl.col("_fecha_alta")
        .dt.offset_by(pl.col("años_amortización").cast(pl.String) + "y")
        .alias("_fecha_fin"),
    )

    df = df.with_columns(
        pl.max_horizontal(pl.col("_fecha_alta"), pl.lit(inicio_año))
        .alias("_inicio_overlap"),
        pl.min_horizontal(pl.col("_fecha_fin"), pl.lit(inicio_siguiente))
        .alias("_fin_overlap"),
    ).with_columns(
        (pl.col("_fin_overlap") - pl.col("_inicio_overlap"))
        .dt.total_days()
        .clip(lower_bound=0)
        .alias("días_en_año"),
    )

    df = df.with_columns(
        (
            pl.col("valor_inicial")
            / pl.col("años_amortización")
            * pl.col("días_en_año")
            / días_del_año
        ).alias("importe"),
    )

    # -- 2b. Filtro por fecha: importe y días --
    filtrados_fecha = df.filter(~((pl.col("importe") > 0) & (pl.col("días_en_año") > 0)))
    df = df.filter((pl.col("importe") > 0) & (pl.col("días_en_año") > 0))

    return _InfoFiltros(
        df=df.drop([c for c in df.columns if c.startswith("_")]),
        n_filtrados_estado=len(no_activos),
        vi_filtrados_estado=float(no_activos["valor_inicial"].sum()),
        n_tras_filtro_estado=len(activos),
        vi_tras_filtro_estado=float(activos["valor_inicial"].sum()),
        n_sin_cuenta=len(sin_cuenta),
        vi_sin_cuenta=float(sin_cuenta["valor_inicial"].sum()),
        n_filtrados_cuenta=len(sin_prefijo_válido),
        vi_filtrados_cuenta=float(sin_prefijo_válido["valor_inicial"].sum()),
        detalle_cuentas_filtradas=detalle_filtradas,
        n_tras_filtro_cuenta=len(con_prefijo_válido),
        vi_tras_filtro_cuenta=float(con_prefijo_válido["valor_inicial"].sum()),
        n_sin_fecha_alta=len(sin_fecha_alta),
        vi_sin_fecha_alta=float(sin_fecha_alta["valor_inicial"].sum()),
        n_filtrados_fecha=len(filtrados_fecha),
        vi_filtrados_fecha=float(filtrados_fecha["valor_inicial"].sum()),
        filtrados_estado_df=no_activos,
        sin_cuenta_df=sin_cuenta,
        filtrados_cuenta_df=sin_prefijo_válido,
        sin_fecha_alta_df=sin_fecha_alta,
        filtrados_fecha_df=filtrados_fecha,
    )


# ======================================================================
# Paso 2: m² por zona, edificación y complejo
# ======================================================================

def _metros_por_zona(
    ubicaciones: pl.DataFrame,
    zonas: pl.DataFrame,
) -> pl.DataFrame:
    """Suma de m² de ubicaciones agrupadas por zona (complejo+edificación+zona)."""
    metros = ubicaciones.group_by(_ZONA).agg(
        pl.col("metros_cuadrados").sum(),
    )
    zonas_join = zonas.with_columns(
        (pl.col("edificación") + pl.col("zona")).alias("_edif_zona"),
    ).select(
        pl.col("complejo").alias("área"),
        pl.col("_edif_zona").alias("edificio"),
        "descripción",
    )
    return (
        metros.join(zonas_join, on=_ZONA, how="left")
        .filter(pl.col("metros_cuadrados") > 0)
        .sort("metros_cuadrados", descending=True)
    )


def _metros_por_edificación(
    ubicaciones: pl.DataFrame,
    edificaciones: pl.DataFrame,
) -> pl.DataFrame:
    """Suma de m² agrupadas por edificación (complejo + 1er carácter de edificio)."""
    df = ubicaciones.with_columns(
        pl.col("edificio").str.slice(0, 1).alias("_edificación"),
    )
    metros = df.group_by(["área", "_edificación"]).agg(
        pl.col("metros_cuadrados").sum(),
    )
    edif_join = edificaciones.select(
        pl.col("complejo").alias("área"),
        pl.col("edificación").alias("_edificación"),
        "descripción",
    )
    return (
        metros.join(edif_join, on=["área", "_edificación"], how="left")
        .filter(pl.col("metros_cuadrados") > 0)
        .sort("metros_cuadrados", descending=True)
    )


def _metros_por_complejo(
    ubicaciones: pl.DataFrame,
    complejos: pl.DataFrame,
) -> pl.DataFrame:
    """Suma de m² agrupados por complejo a partir de las ubicaciones."""
    return (
        ubicaciones.group_by("área")
        .agg(pl.col("metros_cuadrados").sum())
        .filter(pl.col("metros_cuadrados") > 0)
        .join(
            complejos.rename({"complejo": "área"}),
            on="área",
            how="left",
        )
        .sort("metros_cuadrados", descending=True)
    )


# ======================================================================
# Paso 3: Matrices de presencia centro × zona/edificación/complejo/UJI
# ======================================================================

@dataclass
class _InfoMatrices:
    """Matrices de presencia de centros de coste a 4 niveles."""

    presencia_zona: pl.DataFrame        # (área, edificio, centro, m2, pct)
    presencia_edificación: pl.DataFrame  # (área, edificación, centro, m2, pct)
    presencia_complejo: pl.DataFrame     # (área, centro, m2, pct)
    presencia_uji: pl.DataFrame          # (centro, m2, pct)
    n_zonas_sin_asignación: int
    n_zonas_con_asignación: int
    n_centros_con_presencia: int


def _matrices_centro(
    ubicaciones: pl.DataFrame,
    ubicaciones_a_servicios: pl.DataFrame,
    servicios: pl.DataFrame,
) -> _InfoMatrices:
    """Construye matrices de presencia centro×zona/edificación/complejo/UJI.

    Traduce servicios a centros de coste (solo vivos con centro asignado),
    agrega m² por centro, redistribuye zonas sin asignar y calcula
    porcentajes a los 4 niveles espaciales.
    """
    # -- Mapa servicio → centro (solo servicios vivos con centro asignado) --
    mapa_centro = (
        servicios.filter(
            (pl.col("vivo") == 1) & pl.col("centro").is_not_null()
        )
        .select("servicio", "centro")
    )

    # -- Asignación directa de m² a centros --
    n_serv_por_ubic = (
        ubicaciones_a_servicios.group_by("ubicación")
        .agg(pl.len().alias("n_servicios"))
    )

    ubic_centro = (
        ubicaciones.join(
            ubicaciones_a_servicios,
            left_on="id_ubicación",
            right_on="ubicación",
            how="inner",
        )
        .join(n_serv_por_ubic, left_on="id_ubicación", right_on="ubicación")
        .join(mapa_centro, on="servicio", how="inner")
        .with_columns(
            (pl.col("metros_cuadrados") / pl.col("n_servicios")).alias("m2_porción"),
        )
    )

    directo = ubic_centro.group_by(_ZONA + ["centro"]).agg(
        pl.col("m2_porción").sum().alias("m2_directo"),
    )

    # -- m² total y asignado por zona --
    total_por_zona = ubicaciones.group_by(_ZONA).agg(
        pl.col("metros_cuadrados").sum().alias("m2_total"),
    )

    ubic_ids_con_servicio = ubicaciones_a_servicios.select("ubicación").unique()
    m2_asignado = (
        ubicaciones.filter(
            pl.col("id_ubicación").is_in(ubic_ids_con_servicio["ubicación"])
        )
        .group_by(_ZONA)
        .agg(pl.col("metros_cuadrados").sum().alias("m2_asignado"))
    )

    m2_zona = (
        total_por_zona.join(m2_asignado, on=_ZONA, how="left")
        .with_columns(pl.col("m2_asignado").fill_null(0))
        .with_columns(
            (pl.col("m2_total") - pl.col("m2_asignado")).alias("m2_sin_asignar"),
        )
    )

    # -- Redistribución intra-zona (espacios comunes) --
    zonas_con_centros_df = directo.select(_ZONA).unique()

    total_directo_por_zona = directo.group_by(_ZONA).agg(
        pl.col("m2_directo").sum().alias("m2_directo_total"),
    )

    redistr_local = (
        directo.join(total_directo_por_zona, on=_ZONA)
        .join(m2_zona.select(_ZONA + ["m2_sin_asignar"]), on=_ZONA)
        .with_columns(
            pl.when(pl.col("m2_directo_total") > 0)
            .then(
                pl.col("m2_directo")
                / pl.col("m2_directo_total")
                * pl.col("m2_sin_asignar")
            )
            .otherwise(0.0)
            .alias("m2_redistribuido"),
        )
        .with_columns(
            (pl.col("m2_directo") + pl.col("m2_redistribuido")).alias(
                "metros_cuadrados"
            ),
        )
        .select(_ZONA + ["centro", "metros_cuadrados"])
    )

    # -- Redistribución de zonas sin ningún centro --
    zonas_sin = m2_zona.join(
        zonas_con_centros_df, on=_ZONA, how="anti",
    )

    n_zonas_sin = len(zonas_sin)
    n_zonas_con = len(zonas_con_centros_df)

    if not zonas_sin.is_empty() and not directo.is_empty():
        total_global = directo.select(pl.col("m2_directo").sum()).item()
        presencia_global = (
            directo.group_by("centro")
            .agg(pl.col("m2_directo").sum().alias("m2_global"))
            .with_columns(
                (pl.col("m2_global") / total_global).alias("proporción"),
            )
        )

        redistr_global = (
            zonas_sin.select(_ZONA + ["m2_total"])
            .join(
                presencia_global.select("centro", "proporción"),
                how="cross",
            )
            .with_columns(
                (pl.col("m2_total") * pl.col("proporción")).alias("metros_cuadrados"),
            )
            .select(_ZONA + ["centro", "metros_cuadrados"])
        )

        centro_zona = pl.concat([redistr_local, redistr_global])
    else:
        centro_zona = redistr_local

    # Limpiar NaN y filas con 0 m²
    centro_zona = centro_zona.with_columns(
        pl.col("metros_cuadrados").fill_nan(0.0),
    ).filter(pl.col("metros_cuadrados") > 0)

    n_centros = centro_zona.select("centro").unique().height

    # -- Construir las 4 matrices con m² y % --

    # 1. Presencia por zona: (área, edificio, centro, m2, pct)
    total_zona = centro_zona.group_by(_ZONA).agg(
        pl.col("metros_cuadrados").sum().alias("m2_zona"),
    )
    presencia_zona = (
        centro_zona.join(total_zona, on=_ZONA)
        .with_columns(
            (pl.col("metros_cuadrados") / pl.col("m2_zona") * 100).alias("pct"),
        )
        .rename({"metros_cuadrados": "m2"})
        .select("área", "edificio", "centro", "m2", "pct")
        .sort("área", "edificio", "centro")
    )

    # 2. Presencia por edificación: (área, edificación, centro, m2, pct)
    centro_edif = (
        centro_zona.with_columns(
            pl.col("edificio").str.slice(0, 1).alias("edificación"),
        )
        .group_by(["área", "edificación", "centro"])
        .agg(pl.col("metros_cuadrados").sum().alias("m2"))
    )
    total_edif = centro_edif.group_by(["área", "edificación"]).agg(
        pl.col("m2").sum().alias("m2_edif"),
    )
    presencia_edificación = (
        centro_edif.join(total_edif, on=["área", "edificación"])
        .with_columns(
            (pl.col("m2") / pl.col("m2_edif") * 100).alias("pct"),
        )
        .select("área", "edificación", "centro", "m2", "pct")
        .sort("área", "edificación", "centro")
    )

    # 3. Presencia por complejo: (área, centro, m2, pct)
    centro_complejo = (
        centro_zona.group_by(["área", "centro"])
        .agg(pl.col("metros_cuadrados").sum().alias("m2"))
    )
    total_complejo = centro_complejo.group_by("área").agg(
        pl.col("m2").sum().alias("m2_complejo"),
    )
    presencia_complejo = (
        centro_complejo.join(total_complejo, on="área")
        .with_columns(
            (pl.col("m2") / pl.col("m2_complejo") * 100).alias("pct"),
        )
        .select("área", "centro", "m2", "pct")
        .sort("área", "centro")
    )

    # 4. Presencia en UJI: (centro, m2, pct)
    centro_uji = (
        centro_zona.group_by("centro")
        .agg(pl.col("metros_cuadrados").sum().alias("m2"))
    )
    total_uji = centro_uji.select(pl.col("m2").sum()).item()
    presencia_uji = (
        centro_uji.with_columns(
            (pl.col("m2") / total_uji * 100).alias("pct"),
        )
        .sort("m2", descending=True)
    )

    return _InfoMatrices(
        presencia_zona=presencia_zona,
        presencia_edificación=presencia_edificación,
        presencia_complejo=presencia_complejo,
        presencia_uji=presencia_uji,
        n_zonas_sin_asignación=n_zonas_sin,
        n_zonas_con_asignación=n_zonas_con,
        n_centros_con_presencia=n_centros,
    )


# ======================================================================
# Paso 4: Distribución de costes OTOP por centro
# ======================================================================

@dataclass
class _InfoDistribución:
    """Resultado del cálculo de distribución de costes OTOP."""

    por_centro: pl.DataFrame   # (centro, porcentaje) — total por centro
    detalle: pl.DataFrame      # (centro, prefijo, comentario, pct_distribución, pct_presencia, contribución)
    prefijos_duplicados: list[tuple[str, int, float]]  # (prefijo, n_filas, pct_total)
    prefijos_sin_match: list[tuple[str, float, str]]   # (prefijo, pct, comentario)


def _distribución_costes(
    distribución: pl.DataFrame,
    matrices: _InfoMatrices,
) -> _InfoDistribución:
    """Calcula el porcentaje de costes OTOP que corresponde a cada centro.

    Para cada fila del fichero de distribución (prefijo, porcentaje), se
    determina si el prefijo identifica una zona (≥3 chars), edificación
    (2 chars) o complejo (1 char), y se reparte el porcentaje entre los
    centros con presencia en ese nivel según su presencia relativa.

    Si un prefijo aparece más de una vez, se agregan sus porcentajes
    antes de procesar (según la especificación).
    """
    # Agregar prefijos duplicados
    agregado = (
        distribución.group_by("prefijo")
        .agg(
            pl.col("porcentaje").sum(),
            pl.col("comentario").first(),
            pl.len().alias("n_filas"),
        )
    )
    prefijos_duplicados: list[tuple[str, int, float]] = [
        (str(row["prefijo"]), row["n_filas"], float(row["porcentaje"]))
        for row in agregado.filter(pl.col("n_filas") > 1).iter_rows(named=True)
    ]

    filas_detalle: list[dict] = []
    prefijos_sin_match: list[tuple[str, float, str]] = []

    for row in agregado.iter_rows(named=True):
        prefijo = str(row["prefijo"]).strip()
        pct_dist = float(row["porcentaje"])
        comentario = str(row.get("comentario") or "")

        if len(prefijo) >= 3:
            # Zona: área = 1er char, edificio = resto
            área = prefijo[0]
            edificio = prefijo[1:]
            centros_en = matrices.presencia_zona.filter(
                (pl.col("área") == área) & (pl.col("edificio") == edificio)
            )
        elif len(prefijo) == 2:
            # Edificación: área = 1er char, edificación = 2º char
            área = prefijo[0]
            edificación = prefijo[1]
            centros_en = matrices.presencia_edificación.filter(
                (pl.col("área") == área) & (pl.col("edificación") == edificación)
            )
        elif len(prefijo) == 1:
            # Complejo
            centros_en = matrices.presencia_complejo.filter(
                pl.col("área") == prefijo
            )
        else:
            continue

        if centros_en.is_empty():
            prefijos_sin_match.append((prefijo, pct_dist, comentario))
            continue

        for row_c in centros_en.iter_rows(named=True):
            pct_presencia = row_c["pct"]  # ya en % (0-100)
            contribución = pct_dist * pct_presencia / 100
            filas_detalle.append({
                "centro": row_c["centro"],
                "prefijo": prefijo,
                "comentario": comentario,
                "pct_distribución": pct_dist,
                "pct_presencia": pct_presencia,
                "contribución": contribución,
            })

    if filas_detalle:
        detalle = pl.DataFrame(filas_detalle)
        por_centro = (
            detalle.group_by("centro")
            .agg(pl.col("contribución").sum().alias("porcentaje"))
            .sort("porcentaje", descending=True)
        )

        # Redistribuir porcentaje no asignado (prefijos sin match)
        total_asignado = por_centro["porcentaje"].sum()
        pct_sin_asignar = 1.0 - total_asignado
        if pct_sin_asignar > 1e-9:
            por_centro = por_centro.with_columns(
                (
                    pl.col("porcentaje")
                    + pct_sin_asignar * pl.col("porcentaje") / total_asignado
                ).alias("porcentaje"),
            ).sort("porcentaje", descending=True)
    else:
        detalle = pl.DataFrame(
            schema={"centro": pl.Utf8, "prefijo": pl.Utf8, "comentario": pl.Utf8,
                    "pct_distribución": pl.Float64, "pct_presencia": pl.Float64,
                    "contribución": pl.Float64}
        )
        por_centro = pl.DataFrame(schema={"centro": pl.Utf8, "porcentaje": pl.Float64})

    return _InfoDistribución(
        por_centro=por_centro,
        detalle=detalle,
        prefijos_duplicados=prefijos_duplicados,
        prefijos_sin_match=prefijos_sin_match,
    )


# ======================================================================
# Función principal
# ======================================================================

def procesar_inventario(
    ctx: ContextoInventario,
    año: int,
) -> ResultadoInventario:
    """Ejecuta el preprocesamiento completo del inventario para *año*."""
    assert ctx.inventario is not None, "Falta inventario.xlsx"
    assert ctx.años_amortización is not None, "Falta años amortización por cuenta.xlsx"
    assert ctx.ubicaciones is not None, "Falta ubicaciones.xlsx"
    assert ctx.zonas is not None, "Falta zonas.xlsx"
    assert ctx.complejos is not None, "Falta complejos.xlsx"
    assert ctx.edificaciones is not None, "Falta edificaciones.xlsx"
    assert ctx.ubicaciones_a_servicios is not None, "Falta ubicaciones a servicios.xlsx"
    assert ctx.servicios is not None, "Falta servicios.xlsx"

    info = _filtrar_y_enriquecer(ctx.inventario, ctx.años_amortización, año)

    metros_zona = _metros_por_zona(ctx.ubicaciones, ctx.zonas)
    metros_edif = _metros_por_edificación(ctx.ubicaciones, ctx.edificaciones)
    metros_complejo = _metros_por_complejo(ctx.ubicaciones, ctx.complejos)

    matrices = _matrices_centro(
        ctx.ubicaciones, ctx.ubicaciones_a_servicios, ctx.servicios
    )

    # m² asignados / sin asignar (totales campus)
    ubic_ids_con_servicio = ctx.ubicaciones_a_servicios.select("ubicación").unique()
    m2_total_campus = float(
        ctx.ubicaciones.select(pl.col("metros_cuadrados").sum()).item()
    )
    m2_asignados = float(
        ctx.ubicaciones.filter(
            pl.col("id_ubicación").is_in(ubic_ids_con_servicio["ubicación"])
        )
        .select(pl.col("metros_cuadrados").sum())
        .item()
    )

    # Distribución de costes OTOP
    if ctx.distribución_costes is not None:
        dist = _distribución_costes(ctx.distribución_costes, matrices)
    else:
        dist = _InfoDistribución(
            por_centro=pl.DataFrame(schema={"centro": pl.Utf8, "porcentaje": pl.Float64}),
            detalle=pl.DataFrame(
                schema={"centro": pl.Utf8, "prefijo": pl.Utf8, "comentario": pl.Utf8,
                        "pct_distribución": pl.Float64, "pct_presencia": pl.Float64,
                        "contribución": pl.Float64}
            ),
            prefijos_duplicados=[],
            prefijos_sin_match=[],
        )

    return ResultadoInventario(
        inventario_enriquecido=info.df,
        metros_por_zona=metros_zona,
        metros_por_edificación=metros_edif,
        metros_por_complejo=metros_complejo,
        presencia_zona=matrices.presencia_zona,
        presencia_edificación=matrices.presencia_edificación,
        presencia_complejo=matrices.presencia_complejo,
        presencia_uji=matrices.presencia_uji,
        n_registros_original=len(ctx.inventario),
        valor_inicial_total=float(ctx.inventario["valor_inicial"].sum()),
        n_filtrados_estado=info.n_filtrados_estado,
        valor_inicial_filtrados_estado=info.vi_filtrados_estado,
        n_tras_filtro_estado=info.n_tras_filtro_estado,
        valor_inicial_tras_filtro_estado=info.vi_tras_filtro_estado,
        n_sin_cuenta=info.n_sin_cuenta,
        valor_inicial_sin_cuenta=info.vi_sin_cuenta,
        n_filtrados_cuenta=info.n_filtrados_cuenta,
        valor_inicial_filtrados_cuenta=info.vi_filtrados_cuenta,
        detalle_cuentas_filtradas=info.detalle_cuentas_filtradas,
        n_tras_filtro_cuenta=info.n_tras_filtro_cuenta,
        valor_inicial_tras_filtro_cuenta=info.vi_tras_filtro_cuenta,
        n_sin_fecha_alta=info.n_sin_fecha_alta,
        valor_inicial_sin_fecha_alta=info.vi_sin_fecha_alta,
        n_filtrados_fecha=info.n_filtrados_fecha,
        valor_inicial_filtrados_fecha=info.vi_filtrados_fecha,
        n_registros_tras_filtro=len(info.df),
        valor_inicial_tras_filtro=float(info.df["valor_inicial"].sum()),
        importe_total=float(info.df["importe"].sum()),
        n_zonas_sin_asignación=matrices.n_zonas_sin_asignación,
        n_zonas_con_asignación=matrices.n_zonas_con_asignación,
        n_centros_con_presencia=matrices.n_centros_con_presencia,
        m2_asignados=m2_asignados,
        m2_sin_asignar=m2_total_campus - m2_asignados,
        m2_total_campus=m2_total_campus,
        distribución_costes=dist.por_centro,
        detalle_distribución=dist.detalle,
        prefijos_duplicados=dist.prefijos_duplicados,
        prefijos_sin_match=dist.prefijos_sin_match,
        filtrados_estado_df=info.filtrados_estado_df,
        sin_cuenta_df=info.sin_cuenta_df,
        filtrados_cuenta_df=info.filtrados_cuenta_df,
        sin_fecha_alta_df=info.sin_fecha_alta_df,
        filtrados_fecha_df=info.filtrados_fecha_df,
    )
