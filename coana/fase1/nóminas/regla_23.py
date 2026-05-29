"""Construcción de los diccionarios de actividades reales para la regla 23.

Para cada expediente de PDI/PVI se construyen diccionarios heterogéneos
(créditos docentes, PFG, tesis, proyectos…) que luego se traducirán a
horas y porcentajes de distribución de masa salarial. De momento se
implementa la dedicación docente en tres claves: asignatura, titulación
y estudio.

Modelo:
- Una *titulación* es un grado o máster concreto (una edición del plan).
- Un *estudio* agrupa titulaciones (diferentes ediciones de un mismo plan).
- Las titulaciones pueden tener `estudio` null (grados antiguos,
  másteres no oficiales). En el diccionario de estudios esas titulaciones
  se pasan directamente como estudio sintético.
"""

from pathlib import Path

import polars as pl

from coana.util.excel_cache import read_excel
from coana.util.configuración import cfg_set

# Másteres ficticios (sin alumnado): se descartan del catálogo de
# titulaciones igual que en el cargador POD, para no inflar el número de
# titulaciones por asignatura. Origen: data/configuración.xlsx.
_MÁSTERES_FICTICIOS: set[str] = cfg_set("másteres_ficticios_pod")


def _expedientes_pdi_pvi(expedientes: pl.DataFrame) -> pl.DataFrame:
    return expedientes.filter(
        pl.col("sector").is_in(["PDI", "PI"])
    ).select("expediente", "per_id")


def _cargar_pod(ruta_base: Path) -> pl.DataFrame | None:
    """Carga ``pod.xlsx`` descartando asignaturas con 0 créditos impartidos
    y 0 créditos computables."""
    pod_path = Path(ruta_base) / "entrada" / "docencia" / "pod.xlsx"
    if not pod_path.exists():
        return None
    pod = read_excel(pod_path)
    # El POD enriquecido puede traer los créditos como texto (celdas vacías
    # → inferencia a string). Forzamos numérico para los cálculos y filtros.
    pod = pod.with_columns(
        pl.col("créditos_impartidos").cast(pl.Float64, strict=False).fill_null(0.0),
        pl.col("créditos_computables").cast(pl.Float64, strict=False).fill_null(0.0),
    )
    return pod.filter(
        ~((pl.col("créditos_impartidos") == 0) & (pl.col("créditos_computables") == 0))
    )


def _asignatura_titulaciones(ruta_base: Path) -> pl.DataFrame:
    """Mapping asignatura → titulación.

    Columnas: ``asignatura, tipo, titulación, nombre_titulación,
    estudio, nombre_estudio, oficial``. ``estudio`` y ``nombre_estudio``
    pueden ser null para titulaciones sin estudio. ``oficial`` es 'S'/'N'
    para másteres y siempre 'S' para grados (todos se consideran oficiales).
    """
    d = Path(ruta_base) / "entrada" / "docencia"
    ag = read_excel(d / "asignaturas grados.xlsx").select("asignatura", "grado")
    am = (
        read_excel(d / "asignaturas másteres.xlsx")
        .select("asignatura", "máster")
        # Descartar los másteres ficticios, igual que el cargador POD.
        .filter(~pl.col("máster").cast(pl.Utf8).is_in(_MÁSTERES_FICTICIOS))
    )
    gr = read_excel(d / "grados.xlsx").select(
        pl.col("grado").alias("titulación"),
        pl.col("nombre").alias("nombre_titulación"),
        "estudio",
    )
    mr = read_excel(d / "másteres.xlsx").select(
        pl.col("máster").alias("titulación"),
        pl.col("nombre").alias("nombre_titulación"),
        "estudio",
        "oficial",
    )
    # `estudios.xlsx` tiene códigos `estudio` repetidos (varias ediciones
    # con el mismo código). Sin deduplicar, el join de abajo multiplica
    # las filas de cada asignatura y falsea el recuento de titulaciones.
    est = read_excel(d / "estudios.xlsx").select(
        "estudio", pl.col("nombre").alias("nombre_estudio"),
    ).unique(subset="estudio", keep="first")

    COLS = ["asignatura", "tipo", "titulación", "nombre_titulación",
            "estudio", "nombre_estudio", "oficial"]
    grado_tit = (
        ag.join(gr, left_on="grado", right_on="titulación", how="inner")
        .with_columns(
            pl.lit("grado").alias("tipo"),
            pl.col("grado").alias("titulación"),
            pl.lit("S").alias("oficial"),
        )
        .drop("grado")
        .join(est, on="estudio", how="left")
        .select(COLS)
    )
    máster_tit = (
        am.join(mr, left_on="máster", right_on="titulación", how="inner")
        .with_columns(
            pl.lit("máster").alias("tipo"),
            pl.col("máster").alias("titulación"),
        )
        .drop("máster")
        .join(est, on="estudio", how="left")
        .select(COLS)
    )
    # Garantía: una sola fila por (asignatura, titulación), por si alguna
    # otra tabla de referencia introdujera duplicados.
    return pl.concat([grado_tit, máster_tit], how="vertical").unique(
        subset=["asignatura", "titulación"], keep="first",
    )


def _cargar_pod_másteres(ruta_base: Path) -> pl.DataFrame | None:
    """Carga ``pod másteres.xlsx`` (resolución de asignaturas con varias titulaciones)."""
    p = Path(ruta_base) / "entrada" / "docencia" / "pod másteres.xlsx"
    if not p.exists():
        return None
    return read_excel(p)


def _resolver_titulación_efectiva(
    pod: pl.DataFrame,
    ruta_base: Path,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Determina para cada (per_id, asignatura) su titulación efectiva.

    Regla:
    - Asignatura con exactamente 1 titulación → ésa.
    - Asignatura con >1 titulación → todas deben ser másteres no
      oficiales; se busca el máster concreto en
      ``pod_másteres_no_oficiales.xlsx`` para (per_id, asignatura).
      Si alguna no es no oficial o no se encuentra la entrada, se marca
      como anomalía.

    Devuelve ``(pod_resuelto, anomalías, múltiples_con_grado)``:
    - ``pod_resuelto``: pod con titulación efectiva resuelta.
    - ``anomalías``: filas sin titulación efectiva resuelta (con columna
      ``motivo``).
    - ``múltiples_con_grado``: asignaturas con >1 titulación que incluyen
      algún grado u otra titulación distinta de máster (contra la regla).
      Columnas: ``asignatura, tipo, titulación, nombre_titulación, oficial``.
    """
    asig_tit = _asignatura_titulaciones(ruta_base)
    n_tit_por_asig = (
        asig_tit.group_by("asignatura")
        .agg(pl.col("titulación").n_unique().alias("_n_tit"))
    )
    pod_n = pod.join(n_tit_por_asig, on="asignatura", how="left")

    # Asignaturas con 1 sola titulación: join directo
    simples = (
        pod_n.filter(pl.col("_n_tit") == 1)
        .join(asig_tit, on="asignatura", how="inner")
    )

    # Sin titulación en catálogo: anomalía (asignatura verdadera huérfana)
    sin_cat = pod_n.filter(pl.col("_n_tit").is_null())

    # Asignaturas con múltiples titulaciones: resolver por pod_másteres_no_oficiales
    múltiples = pod_n.filter(pl.col("_n_tit") > 1)

    anomalías: list[pl.DataFrame] = []
    if not sin_cat.is_empty():
        anomalías.append(
            sin_cat.select("per_id", "asignatura", "créditos_impartidos")
            .with_columns(pl.lit("sin titulación en catálogo").alias("motivo"))
        )

    resueltas_múltiples = pl.DataFrame()
    múltiples_con_grado = pl.DataFrame()
    if not múltiples.is_empty():
        # Detectar asignaturas con alguna titulación que NO es máster
        # (regla: todas deben ser másteres).
        asig_tit_múltiples = asig_tit.join(
            múltiples.select("asignatura").unique(),
            on="asignatura", how="inner",
        )
        no_master_por_asig = (
            asig_tit_múltiples
            .group_by("asignatura")
            .agg((pl.col("tipo") != "máster").any().alias("tiene_no_master"))
        )
        asig_con_grado = no_master_por_asig.filter(
            pl.col("tiene_no_master")
        ).select("asignatura")
        if not asig_con_grado.is_empty():
            múltiples_con_grado = asig_tit_múltiples.join(
                asig_con_grado, on="asignatura", how="inner",
            ).select(
                "asignatura", "tipo", "titulación", "nombre_titulación", "oficial",
            )

        # Resolver con pod másteres: máster específico por (per_id, asignatura)
        pnmo = _cargar_pod_másteres(ruta_base)
        if pnmo is not None:
            resolución = pnmo.select("per_id", "asignatura", "máster").unique()
            # Unimos la titulación concreta asignada
            mr_info = asig_tit.filter(pl.col("tipo") == "máster").select(
                "titulación", "nombre_titulación", "estudio", "nombre_estudio", "oficial",
            ).unique()
            resolución = resolución.join(
                mr_info.rename({"titulación": "máster"}),
                on="máster",
                how="left",
            ).with_columns(
                pl.lit("máster").alias("tipo"),
                pl.col("máster").alias("titulación"),
            ).drop("máster")

            resueltas_múltiples = múltiples.join(
                resolución, on=["per_id", "asignatura"], how="left",
            )
            sin_resolver = resueltas_múltiples.filter(pl.col("titulación").is_null())
            if not sin_resolver.is_empty():
                anomalías.append(
                    sin_resolver.select("per_id", "asignatura", "créditos_impartidos")
                    .with_columns(pl.lit("máster múltiple no resuelto").alias("motivo"))
                )
            resueltas_múltiples = resueltas_múltiples.filter(
                pl.col("titulación").is_not_null()
            )
        else:
            anomalías.append(
                múltiples.select("per_id", "asignatura", "créditos_impartidos")
                .with_columns(pl.lit("falta pod másteres").alias("motivo"))
            )

    partes = [simples.drop("_n_tit")]
    if not resueltas_múltiples.is_empty():
        partes.append(resueltas_múltiples.drop("_n_tit"))
    pod_resuelto = pl.concat(partes, how="diagonal") if partes else pl.DataFrame()

    anomalías_df = pl.concat(anomalías, how="diagonal") if anomalías else pl.DataFrame()
    return pod_resuelto, anomalías_df, múltiples_con_grado


def generar_dedicación_docente(
    expedientes: pl.DataFrame,
    ruta_base: Path,
    dir_salida: Path,
) -> pl.DataFrame:
    """Diccionario {asignatura: créditos impartidos} por expediente PDI/PVI.

    Guarda ``regla_23_dedicación_docente.parquet`` con columnas
    `expediente, per_id, asignatura, créditos_impartidos`.
    """
    pod = _cargar_pod(ruta_base)
    if pod is None:
        print("    ⚠ pod.xlsx no encontrado")
        return pl.DataFrame()

    exp = _expedientes_pdi_pvi(expedientes)
    if exp.is_empty():
        return pl.DataFrame()

    # pod tiene varias filas por (per_id, asignatura) — una por grupo/subgrupo.
    # El diccionario agrega por asignatura (créditos totales del profesor).
    ded = (
        exp.join(
            pod.select("per_id", "asignatura", "créditos_impartidos"),
            on="per_id",
            how="inner",
        )
        .group_by("expediente", "per_id", "asignatura")
        .agg(pl.col("créditos_impartidos").sum())
    )
    if ded.is_empty():
        return pl.DataFrame()

    dir_salida.mkdir(parents=True, exist_ok=True)
    ded.write_parquet(dir_salida / "regla_23_dedicación_docente.parquet")

    # Validación: para cada profesor, la suma del diccionario (por persona,
    # deduplicada tras multi-expediente) coincide con el total de pod.xlsx.
    tot_dicc = (
        ded.select("per_id", "asignatura", "créditos_impartidos").unique()
        .group_by("per_id")
        .agg(pl.col("créditos_impartidos").sum().alias("_suma_dicc"))
    )
    per_ids_en_dicc = ded["per_id"].unique().implode()
    tot_pod = (
        pod.filter(pl.col("per_id").is_in(per_ids_en_dicc))
        .group_by("per_id")
        .agg(pl.col("créditos_impartidos").sum().alias("_suma_pod"))
    )
    check = tot_dicc.join(tot_pod, on="per_id", how="left").with_columns(
        pl.col("_suma_pod").fill_null(0.0),
    )
    discrepancias = check.filter(
        (pl.col("_suma_dicc") - pl.col("_suma_pod")).abs() > 0.01
    )
    if not discrepancias.is_empty():
        print(
            f"    ⚠ {len(discrepancias):,} per_id con discrepancia "
            "entre diccionario docente y pod.xlsx"
        )

    n_exp = ded["expediente"].n_unique()
    tot_cred = float(ded["créditos_impartidos"].sum())
    print(
        f"  Regla 23 — dedicación docente: {n_exp:,} expedientes, "
        f"{len(ded):,} filas, {tot_cred:,.2f} créditos impartidos"
    )
    return ded


def generar_pod_resuelto(
    expedientes: pl.DataFrame,
    ruta_base: Path,
    dir_salida: Path,
) -> pl.DataFrame:
    """Persiste ``regla_23_pod_resuelto.parquet``: pod de expedientes
    PDI/PVI con la titulación efectiva resuelta y las anomalías separadas."""
    pod = _cargar_pod(ruta_base)
    if pod is None:
        return pl.DataFrame()

    exp = _expedientes_pdi_pvi(expedientes)
    if exp.is_empty():
        return pl.DataFrame()

    pod_pdi_pvi = pod.join(
        exp.select("per_id").unique(), on="per_id", how="inner",
    )
    if pod_pdi_pvi.is_empty():
        return pl.DataFrame()

    resuelto, anomalías, múlt_con_grado = _resolver_titulación_efectiva(
        pod_pdi_pvi, ruta_base,
    )

    anom_path = dir_salida / "regla_23_anomalías_resolución.parquet"
    if not anomalías.is_empty():
        anomalías.write_parquet(anom_path)
        n_anom = len(anomalías)
        c_anom = float(anomalías["créditos_impartidos"].sum())
        print(
            f"  Regla 23 — anomalías resolución: {n_anom:,} filas de pod sin "
            f"titulación efectiva ({c_anom:,.2f} créditos)"
        )
    elif anom_path.exists():
        anom_path.unlink()

    grado_path = dir_salida / "regla_23_múltiples_con_grado.parquet"
    if not múlt_con_grado.is_empty():
        múlt_con_grado.write_parquet(grado_path)
        n_asig = múlt_con_grado["asignatura"].n_unique()
        print(
            f"  Regla 23 — múltiples con grado: {n_asig:,} asignaturas con "
            "varias titulaciones y alguna no es máster"
        )
    elif grado_path.exists():
        grado_path.unlink()

    if resuelto.is_empty():
        return pl.DataFrame()

    ded = resuelto.join(
        exp.select("expediente", "per_id"),
        on="per_id",
        how="inner",
    )
    ded.write_parquet(dir_salida / "regla_23_pod_resuelto.parquet")
    return ded


def generar_dedicación_titulaciones(
    ded: pl.DataFrame,
    dir_salida: Path,
) -> pl.DataFrame:
    """Diccionario {(tipo, titulación, nombre): créditos} por expediente.

    Parte de ``ded`` (pod con titulación resuelta) generado por
    ``generar_pod_resuelto``.
    """
    if ded.is_empty():
        return pl.DataFrame()

    agrup = (
        ded.group_by("expediente", "per_id", "tipo", "titulación", "nombre_titulación")
        .agg(pl.col("créditos_impartidos").sum().round(2))
        .sort("expediente", "créditos_impartidos", descending=[False, True])
    )
    agrup.write_parquet(dir_salida / "regla_23_dedicación_titulaciones.parquet")

    n_exp = agrup["expediente"].n_unique()
    n_tit = agrup.select(pl.struct("tipo", "titulación")).unique().height
    tot_cred = float(agrup["créditos_impartidos"].sum())
    print(
        f"  Regla 23 — dedicación titulaciones: {n_exp:,} expedientes, "
        f"{n_tit:,} titulaciones distintas, {tot_cred:,.2f} créditos"
    )
    return agrup


def generar_dedicación_estudios(
    ded: pl.DataFrame,
    dir_salida: Path,
) -> pl.DataFrame:
    """Diccionario {(tipo, código, nombre): créditos} por expediente.

    Si la titulación tiene estudio, la UC se agrega a ese estudio. Si
    no, la titulación se pasa directamente como estudio sintético
    (``tipo_estudio`` = 'estudio propio').
    """
    if ded.is_empty():
        return pl.DataFrame()

    ded = ded.with_columns(
        pl.when(pl.col("estudio").is_not_null())
          .then(pl.lit("estudio"))
          .otherwise(pl.lit("estudio propio"))
          .alias("tipo_estudio"),
        pl.when(pl.col("estudio").is_not_null())
          .then(pl.col("estudio"))
          .otherwise(pl.col("titulación"))
          .alias("código_estudio"),
        pl.when(pl.col("nombre_estudio").is_not_null())
          .then(pl.col("nombre_estudio"))
          .otherwise(pl.col("nombre_titulación"))
          .alias("nombre_estudio_resuelto"),
    ).drop("nombre_estudio").rename({"nombre_estudio_resuelto": "nombre_estudio"})

    agrup = (
        ded.group_by(
            "expediente", "per_id",
            "tipo_estudio", "código_estudio", "nombre_estudio",
        )
        .agg(pl.col("créditos_impartidos").sum().round(2))
        .sort("expediente", "créditos_impartidos", descending=[False, True])
    )
    agrup.write_parquet(dir_salida / "regla_23_dedicación_estudios.parquet")

    n_exp = agrup["expediente"].n_unique()
    n_est = agrup.select(pl.struct("tipo_estudio", "código_estudio")).unique().height
    tot_cred = float(agrup["créditos_impartidos"].sum())
    print(
        f"  Regla 23 — dedicación estudios: {n_exp:,} expedientes, "
        f"{n_est:,} estudios distintos, {tot_cred:,.2f} créditos"
    )
    return agrup


def generar_asignaturas_sin_titulación(
    expedientes: pl.DataFrame,
    ruta_base: Path,
    dir_salida: Path,
) -> pl.DataFrame:
    """Asignaturas del PDI/PVI que no aparecen en ninguna tabla de asignaturas.

    No incluye asignaturas con titulación pero sin estudio (ésas van al
    diccionario de estudios como estudio sintético).
    Guarda ``regla_23_asignaturas_sin_titulación.parquet`` con columnas
    `asignatura, per_id, créditos_impartidos`.
    """
    pod = _cargar_pod(ruta_base)
    if pod is None:
        return pl.DataFrame()

    exp = _expedientes_pdi_pvi(expedientes)
    if exp.is_empty():
        return pl.DataFrame()

    ded = exp.join(
        pod.select("per_id", "asignatura", "créditos_impartidos"),
        on="per_id",
        how="inner",
    )
    if ded.is_empty():
        return pl.DataFrame()

    asignaturas_con_tit = (
        _asignatura_titulaciones(ruta_base)
        .select("asignatura")
        .unique()
    )
    sin_tit = (
        ded.join(asignaturas_con_tit, on="asignatura", how="anti")
        .select("asignatura", "per_id", "créditos_impartidos")
    )
    if sin_tit.is_empty():
        # Limpia parquet previo si no hay anomalías
        p = dir_salida / "regla_23_asignaturas_sin_titulación.parquet"
        if p.exists():
            p.unlink()
        return pl.DataFrame()

    sin_tit.write_parquet(dir_salida / "regla_23_asignaturas_sin_titulación.parquet")
    n_asig = sin_tit["asignatura"].n_unique()
    tot_cred = float(sin_tit["créditos_impartidos"].sum())
    print(
        f"  Regla 23 — asignaturas sin titulación: {n_asig:,} asignaturas, "
        f"{tot_cred:,.2f} créditos"
    )
    return sin_tit


def generar_estructura_estudios_titulaciones(
    expedientes: pl.DataFrame,
    ruta_base: Path,
    dir_salida: Path,
) -> pl.DataFrame:
    """Mapping estudio ↔ titulaciones para la página estructural de la app.

    Columnas: ``tipo, titulación, nombre_titulación, estudio, nombre_estudio, activa``.
    ``estudio`` y ``nombre_estudio`` son null para titulaciones huérfanas.
    ``activa`` = True si alguna asignatura de la titulación tiene créditos
    impartidos por algún expediente PDI/PVI en el año.
    Se incluyen todas las titulaciones, tengan o no asignaturas ligadas.
    """
    d = Path(ruta_base) / "entrada" / "docencia"
    gr = read_excel(d / "grados.xlsx").select(
        pl.col("grado").alias("titulación"),
        pl.col("nombre").alias("nombre_titulación"),
        "estudio",
    ).with_columns(pl.lit("grado").alias("tipo"))
    mr = read_excel(d / "másteres.xlsx").select(
        pl.col("máster").alias("titulación"),
        pl.col("nombre").alias("nombre_titulación"),
        "estudio",
    ).with_columns(pl.lit("máster").alias("tipo"))
    est = read_excel(d / "estudios.xlsx").select(
        "estudio", pl.col("nombre").alias("nombre_estudio"),
    )

    tit = (
        pl.concat([gr, mr], how="vertical_relaxed")
        .join(est, on="estudio", how="left")
        .select("tipo", "titulación", "nombre_titulación", "estudio", "nombre_estudio")
    )

    # Marcar titulaciones activas: tienen asignaturas con créditos impartidos
    # por algún expediente PDI/PVI en el año.
    activas = pl.DataFrame(schema={"tipo": pl.Utf8, "titulación": pl.Int64})
    pod = _cargar_pod(ruta_base)
    exp = _expedientes_pdi_pvi(expedientes)
    if pod is not None and not exp.is_empty():
        ded = exp.join(pod.select("per_id", "asignatura"), on="per_id", how="inner")
        if not ded.is_empty():
            activas = (
                ded.select("asignatura").unique()
                .join(
                    _asignatura_titulaciones(ruta_base).select("asignatura", "tipo", "titulación"),
                    on="asignatura",
                    how="inner",
                )
                .select("tipo", "titulación")
                .unique()
                .with_columns(pl.lit(True).alias("activa"))
            )

    tit = (
        tit.join(activas, on=["tipo", "titulación"], how="left")
        .with_columns(pl.col("activa").fill_null(False))
    )
    tit.write_parquet(dir_salida / "regla_23_estructura_estudios.parquet")

    n_act = tit.filter(pl.col("activa")).height
    n_inact = tit.filter(~pl.col("activa")).height
    print(
        f"  Regla 23 — estructura: {len(tit):,} titulaciones "
        f"({n_act:,} activas, {n_inact:,} sin créditos este año)"
    )
    return tit


# Conceptos retributivos que son atrasos (se reparten luego como bolsa común,
# no via regla 23 porque dependen de actividad de años anteriores).
_CONCEPTOS_ATRASOS = ("30", "87")

# Concepto retributivo de despidos (indemnización por finalización de contrato).
_CONCEPTO_DESPIDO = "47"
# Indemnizaciones por asistencia a tribunales y similares.
_CONCEPTO_INDEMN_ASIST = "48"
# Cargos asociables a proyecto específico: 19 (cargos académicos docentes),
# 64 (retribución mérito individual proyectos UE).
_CONCEPTOS_CARGOS = ("19", "64")
# TABLA-PROYECTOS-GENERALES (ver spec): proyectos que NO se consideran
# "proyecto específico" a efectos de la regla de cargos académicos. Las filas
# con estos proyectos y concepto retributivo 19 o 64 NO generan UC de cargos
# y siguen la vía de retribuciones normales. Origen:
# data/configuración.xlsx (clave `proyectos_generales_cargos`).
from coana.util.configuración import cfg_tuple as _cfg_tuple
_PROYECTOS_GENERALES = _cfg_tuple("proyectos_generales_cargos")


def _clasificar_y_construir_uc(
    rows: pl.DataFrame,
    id_prefix: str,
    origen_tag: str,
    nombre_salida: str,
    dir_salida: Path,
    rows_fijos: pl.DataFrame | None = None,
    ctx_enriquecimiento=None,
    árbol_actividades=None,
    árbol_cc=None,
    distribución_costes=None,
    obtener_descripciones=None,
) -> pl.DataFrame:
    """Núcleo común para UC definidas: aplica clasificadores a ``rows``
    (que debe tener columna ``_ec``), une con ``rows_fijos`` (que ya
    tiene ``_cc`` y ``_act``), agrupa y persiste.
    """
    from coana.fase1.clasificador_actividades import (
        clasificar_actividades,
        enriquecer_para_actividades,
    )
    from coana.fase1.clasificador_centros_coste import clasificar_centros_coste

    if not rows.is_empty():
        _desc_fn = obtener_descripciones or (lambda c, v: {})
        if ctx_enriquecimiento is not None:
            rows = enriquecer_para_actividades(rows, ctx_enriquecimiento)
        if árbol_cc is not None:
            rows, _ = clasificar_centros_coste(
                rows, árbol_cc, distribución_costes, _desc_fn,
            )
            rows = rows.rename({"_centro_de_coste": "_cc"})
        else:
            rows = rows.with_columns(pl.lit(None).cast(pl.Utf8).alias("_cc"))
        if árbol_actividades is not None:
            rows, _ = clasificar_actividades(rows, árbol_actividades, _desc_fn)
            rows = rows.rename({"_actividad": "_act"})
        else:
            rows = rows.with_columns(pl.lit(None).cast(pl.Utf8).alias("_act"))

    partes: list[pl.DataFrame] = []
    if rows_fijos is not None and not rows_fijos.is_empty():
        partes.append(rows_fijos)
    if not rows.is_empty():
        partes.append(rows)
    if not partes:
        p = dir_salida / f"{nombre_salida}.parquet"
        if p.exists():
            p.unlink()
        return pl.DataFrame()
    todos = pl.concat(partes, how="diagonal_relaxed")

    # Enriquecer con tipo_proyecto para mostrar en la UC
    if ctx_enriquecimiento is not None and getattr(ctx_enriquecimiento, "proyectos", None) is not None:
        proy_ref = ctx_enriquecimiento.proyectos.select(
            pl.col("proyecto").cast(pl.Utf8),
            pl.col("tipo").cast(pl.Utf8).str.strip_chars().alias("_tipo_proy"),
        )
        todos = todos.with_columns(pl.col("proyecto").cast(pl.Utf8)).join(
            proy_ref, on="proyecto", how="left",
        )
    else:
        todos = todos.with_columns(pl.lit(None).cast(pl.Utf8).alias("_tipo_proy"))

    sin_res = todos.filter(pl.col("_cc").is_null() | pl.col("_act").is_null())
    if not sin_res.is_empty():
        n_sr = len(sin_res)
        imp_sr = float(sin_res["importe"].sum())
        print(
            f"    ⚠ {n_sr:,} {origen_tag.lower()}(s) sin CC/actividad "
            f"resuelta ({imp_sr:,.2f} €)"
        )
    todos = todos.filter(
        pl.col("_cc").is_not_null() & pl.col("_act").is_not_null()
    )
    if todos.is_empty():
        p = dir_salida / f"{nombre_salida}.parquet"
        if p.exists():
            p.unlink()
        return pl.DataFrame()

    agrup = (
        todos.group_by("expediente", "per_id", "_ec", "_cc", "_act")
        .agg(
            pl.col("importe").sum(),
            pl.col("proyecto").cast(pl.Utf8).unique().sort()
              .str.join(", ").alias("_proyectos"),
            pl.col("_tipo_proy").unique().sort()
              .str.join(", ").alias("_tipos_proy"),
        )
    )

    _id = [0]

    def _nid() -> str:
        _id[0] += 1
        return f"{id_prefix}-{_id[0]:05d}"

    filas = []
    for row in agrup.iter_rows(named=True):
        filas.append({
            "id": _nid(),
            "expediente": row["expediente"],
            "per_id": row["per_id"],
            "elemento_de_coste": row["_ec"],
            "centro_de_coste": row["_cc"],
            "actividad": row["_act"],
            "proyecto": row["_proyectos"],
            "tipo_proyecto": row["_tipos_proy"],
            "importe": row["importe"],
            "origen": "nómina",
            "origen_id": (
                f"{origen_tag}-exp-{row['expediente']}-ec-{row['_ec']}-"
                f"cc-{row['_cc']}-act-{row['_act']}"
            ),
            "origen_porción": 1.0,
        })
    uc = pl.DataFrame(filas)
    uc.write_parquet(dir_salida / f"{nombre_salida}.parquet")
    return uc


def _filtrar_pdi_pvi_por_conceptos(
    nóminas: pl.DataFrame,
    expedientes: pl.DataFrame,
    conceptos: tuple[str, ...] | list[str],
) -> pl.DataFrame:
    """Filas PDI/PVI no-SS con concepto_retributivo en la lista dada."""
    exp_pp = expedientes.filter(
        pl.col("sector").is_in(["PDI", "PI"])
    ).select("expediente", "per_id", "sector")
    if exp_pp.is_empty():
        return pl.DataFrame()
    n = nóminas.join(exp_pp, on="expediente", how="inner")
    es_ss = pl.col("aplicación").cast(pl.Utf8).str.starts_with("12")
    return n.filter(
        ~es_ss
        & pl.col("concepto_retributivo").cast(pl.Utf8).is_in(list(conceptos))
    )


def _añadir_elemento_coste(rows: pl.DataFrame) -> pl.DataFrame:
    """Calcula ``_ec`` por fila usando _elemento_coste_pdi/pvi."""
    from coana.fase1.nóminas import _elemento_coste_pdi, _elemento_coste_pvi
    ecs: list[str | None] = []
    for row in rows.iter_rows(named=True):
        if row["sector"] == "PDI":
            e = _elemento_coste_pdi(row["categoría"], row["concepto_retributivo"])
        else:
            e = _elemento_coste_pvi(
                row["categoría"], row["perceptor"],
                row["provisión"], row["concepto_retributivo"],
                row.get("categoría_plaza"), row.get("sector_plaza"),
            )
        ecs.append(e)
    return rows.with_columns(pl.Series("_ec", ecs))


def generar_uc_despidos(
    nóminas: pl.DataFrame,
    expedientes: pl.DataFrame,
    dir_salida: Path,
    ctx_enriquecimiento=None,
    árbol_actividades=None,
    árbol_cc=None,
    distribución_costes=None,
    obtener_descripciones=None,
) -> pl.DataFrame:
    """Genera UC de despidos PDI/PVI (concepto_retributivo 47).

    Solo procesa los despidos imputados a *proyectos generales* (los
    diez de TABLA-PROYECTOS-GENERALES). Los despidos en proyecto
    específico son coste del propio proyecto y se procesan por el flujo
    normal de retribuciones extras.

    Reglas para los despidos en proyecto general:
    - Si ``proyecto`` = "23G019" (fondo de contingencia para despidos):
      actividad = ``otras-ait-financiación-propia``, centro = ``vi``.
    - En otro caso (resto de proyectos generales): CC y actividad vía
      los clasificadores compartidos.
    """
    despidos = _filtrar_pdi_pvi_por_conceptos(
        nóminas, expedientes, (_CONCEPTO_DESPIDO,),
    )
    if not despidos.is_empty():
        despidos = despidos.filter(
            pl.col("proyecto").cast(pl.Utf8).is_in(list(_PROYECTOS_GENERALES))
        )
    if despidos.is_empty():
        p = dir_salida / "uc_despidos.parquet"
        if p.exists():
            p.unlink()
        return pl.DataFrame()

    despidos = _añadir_elemento_coste(despidos)
    errores = despidos.filter(pl.col("_ec").is_null())
    if not errores.is_empty():
        print(
            f"    ⚠ {len(errores):,} despidos sin elemento de coste "
            f"({float(errores['importe'].sum()):,.2f} €)"
        )
    despidos = despidos.filter(pl.col("_ec").is_not_null())
    if despidos.is_empty():
        return pl.DataFrame()

    es_23g = pl.col("proyecto").cast(pl.Utf8) == "23G019"
    fijos = despidos.filter(es_23g).with_columns(
        pl.lit("vi").alias("_cc"),
        pl.lit("otras-ait-financiación-propia").alias("_act"),
    )
    resto = despidos.filter(~es_23g)

    uc = _clasificar_y_construir_uc(
        rows=resto,
        rows_fijos=fijos,
        id_prefix="DE",
        origen_tag="DESPIDO",
        nombre_salida="uc_despidos",
        dir_salida=dir_salida,
        ctx_enriquecimiento=ctx_enriquecimiento,
        árbol_actividades=árbol_actividades,
        árbol_cc=árbol_cc,
        distribución_costes=distribución_costes,
        obtener_descripciones=obtener_descripciones,
    )
    if not uc.is_empty():
        print(
            f"  UC despidos (PDI/PVI): {len(uc):,} UC, "
            f"{float(uc['importe'].sum()):,.2f} €"
        )
    return uc


def generar_uc_indemnizaciones_asistencias(
    nóminas: pl.DataFrame,
    expedientes: pl.DataFrame,
    dir_salida: Path,
    ctx_enriquecimiento=None,
    árbol_actividades=None,
    árbol_cc=None,
    distribución_costes=None,
    obtener_descripciones=None,
) -> pl.DataFrame:
    """Genera UC de indemnizaciones por asistencia (concepto 48).

    Cubre los tres sectores (PTGAS, PDI, PVI). Reglas (spec, sección
    «Reglas para el tratamiento de los costes del PTGAS · Segundo.1»
    y equivalente para PDI/PVI):

    - Elemento de coste: fijo, ``otras-indemnizaciones``.
    - Actividad: fija, ``dag-sgc-indemnizaciones-asistencias``.
    - Centro de coste: el del Servicio en la tabla ``_SERVICIO_CC``.
      Caso especial servicio ``368``: se usa ``centro_plaza`` con la
      tabla ``_CENTRO_PLAZA_CC`` (igual que en Segundo.2).
    """
    from coana.fase1.clasificador_centros_coste import (
        _CENTRO_PLAZA_CC,
        _servicio_cc,
    )

    exp_min = expedientes.select("expediente", "per_id", "sector")
    es_ss = pl.col("aplicación").cast(pl.Utf8).str.starts_with("12")
    rows = (
        nóminas.join(exp_min, on="expediente", how="inner")
        .filter(
            ~es_ss
            & (pl.col("concepto_retributivo").cast(pl.Utf8) == _CONCEPTO_INDEMN_ASIST)
        )
    )
    if rows.is_empty():
        p = dir_salida / "uc_indemnizaciones_asistencias.parquet"
        if p.exists():
            p.unlink()
        return pl.DataFrame()

    srv_to_cc = {k: v[0] for k, v in _servicio_cc().items()}
    cp_to_cc = {k: v[0] for k, v in _CENTRO_PLAZA_CC.items()}

    cols = rows.columns
    cast_srv = (
        pl.col("servicio").cast(pl.Utf8)
        if "servicio" in cols
        else pl.lit(None, dtype=pl.Utf8)
    )
    cast_cp = (
        pl.col("centro_plaza").cast(pl.Utf8)
        if "centro_plaza" in cols
        else pl.lit(None, dtype=pl.Utf8)
    )

    rows = rows.with_columns(
        pl.lit("otras-indemnizaciones").alias("_ec"),
        pl.lit("dag-sgc-indemnizaciones-asistencias").alias("_act"),
        pl.when(cast_srv == "368")
        .then(cast_cp.replace_strict(cp_to_cc, default=None, return_dtype=pl.Utf8))
        .otherwise(cast_srv.replace_strict(srv_to_cc, default=None, return_dtype=pl.Utf8))
        .alias("_cc"),
    )

    sin_cc = rows.filter(pl.col("_cc").is_null())
    if not sin_cc.is_empty():
        n = len(sin_cc)
        imp = float(sin_cc["importe"].sum())
        print(
            f"    ⚠ {n:,} línea(s) CR 48 sin centro resoluble por servicio "
            f"({imp:,.2f} €)"
        )

    uc = _clasificar_y_construir_uc(
        rows=pl.DataFrame(),
        rows_fijos=rows,
        id_prefix="IA",
        origen_tag="INDEMN-ASIST",
        nombre_salida="uc_indemnizaciones_asistencias",
        dir_salida=dir_salida,
        ctx_enriquecimiento=ctx_enriquecimiento,
        árbol_actividades=árbol_actividades,
        árbol_cc=árbol_cc,
        distribución_costes=distribución_costes,
        obtener_descripciones=obtener_descripciones,
    )
    if not uc.is_empty():
        print(
            f"  UC indemnizaciones asistencias (PTGAS/PDI/PVI): {len(uc):,} UC, "
            f"{float(uc['importe'].sum()):,.2f} €"
        )
    return uc


def generar_uc_cargos(
    nóminas: pl.DataFrame,
    expedientes: pl.DataFrame,
    dir_salida: Path,
    ctx_enriquecimiento=None,
    árbol_actividades=None,
    árbol_cc=None,
    distribución_costes=None,
    obtener_descripciones=None,
) -> pl.DataFrame:
    """Genera UC de cargos PDI/PVI asociables a proyecto (conceptos 19, 64).

    El elemento de coste se calcula por sector+categoría (ZZZ-XXX-YYY).
    El CC y la actividad se determinan con los clasificadores compartidos.
    """
    rows = _filtrar_pdi_pvi_por_conceptos(
        nóminas, expedientes, _CONCEPTOS_CARGOS,
    )
    if not rows.is_empty():
        rows = rows.filter(
            ~pl.col("proyecto").cast(pl.Utf8).is_in(list(_PROYECTOS_GENERALES))
        )
    if rows.is_empty():
        p = dir_salida / "uc_cargos.parquet"
        if p.exists():
            p.unlink()
        return pl.DataFrame()

    rows = _añadir_elemento_coste(rows)
    errores = rows.filter(pl.col("_ec").is_null())
    if not errores.is_empty():
        print(
            f"    ⚠ {len(errores):,} cargos sin elemento de coste "
            f"({float(errores['importe'].sum()):,.2f} €)"
        )
    rows = rows.filter(pl.col("_ec").is_not_null())
    if rows.is_empty():
        return pl.DataFrame()

    uc = _clasificar_y_construir_uc(
        rows=rows,
        rows_fijos=None,
        id_prefix="CA",
        origen_tag="CARGO",
        nombre_salida="uc_cargos",
        dir_salida=dir_salida,
        ctx_enriquecimiento=ctx_enriquecimiento,
        árbol_actividades=árbol_actividades,
        árbol_cc=árbol_cc,
        distribución_costes=distribución_costes,
        obtener_descripciones=obtener_descripciones,
    )
    if not uc.is_empty():
        print(
            f"  UC cargos (PDI/PVI): {len(uc):,} UC, "
            f"{float(uc['importe'].sum()):,.2f} €"
        )
    return uc


def generar_atrasos_y_apartados(
    nóminas: pl.DataFrame,
    expedientes: pl.DataFrame,
    dir_salida: Path,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """No-op: los atrasos (CR 30/87) ya no se separan.

    Se integran en la masa de regla 23 y se reparten con la misma
    distribución promedio que el resto. Esta función se mantiene como
    stub por compatibilidad con el orquestador y limpia los parquets
    antiguos si existen.
    """
    for nombre in ("regla_23_atrasos.parquet", "regla_23_expedientes_apartados.parquet"):
        p = dir_salida / nombre
        if p.exists():
            p.unlink()
    return pl.DataFrame(), pl.DataFrame()
