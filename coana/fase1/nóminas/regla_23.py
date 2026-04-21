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
    am = read_excel(d / "asignaturas másteres.xlsx").select("asignatura", "máster")
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
    est = read_excel(d / "estudios.xlsx").select(
        "estudio", pl.col("nombre").alias("nombre_estudio"),
    )

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
    return pl.concat([grado_tit, máster_tit], how="vertical")


def _cargar_pod_másteres_no_oficiales(ruta_base: Path) -> pl.DataFrame | None:
    """Carga ``pod_másteres_no_oficiales.xlsx``."""
    p = Path(ruta_base) / "entrada" / "docencia" / "pod_másteres_no_oficiales.xlsx"
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

    Devuelve ``(pod_resuelto, anomalías, múltiples_oficiales)``:
    - ``pod_resuelto``: pod con titulación efectiva resuelta.
    - ``anomalías``: filas sin titulación efectiva resuelta (con columna
      ``motivo``).
    - ``múltiples_oficiales``: asignaturas con >1 titulación que incluyen
      algún máster oficial (contra la regla). Columnas: ``asignatura,
      titulación, nombre_titulación, oficial``.
    """
    asig_tit = _asignatura_titulaciones(ruta_base)
    n_tit_por_asig = (
        asig_tit.group_by("asignatura")
        .agg(pl.len().alias("_n_tit"))
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
    múltiples_oficiales = pl.DataFrame()
    if not múltiples.is_empty():
        # Detectar asignaturas con alguna titulación oficial (no deberían)
        asig_tit_múltiples = asig_tit.join(
            múltiples.select("asignatura").unique(),
            on="asignatura", how="inner",
        )
        ofi_por_asig = (
            asig_tit_múltiples
            .group_by("asignatura")
            .agg((pl.col("oficial") == "S").any().alias("tiene_oficial"))
        )
        asig_con_ofi = ofi_por_asig.filter(pl.col("tiene_oficial")).select("asignatura")
        if not asig_con_ofi.is_empty():
            múltiples_oficiales = asig_tit_múltiples.join(
                asig_con_ofi, on="asignatura", how="inner",
            ).select(
                "asignatura", "tipo", "titulación", "nombre_titulación", "oficial",
            )

        # Resolver con pod_másteres_no_oficiales: máster específico por (per_id, asignatura)
        pnmo = _cargar_pod_másteres_no_oficiales(ruta_base)
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
            # Marcar como anomalía las que no se resolvieron
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
            # No hay tabla de resolución: todas las múltiples son anomalía
            anomalías.append(
                múltiples.select("per_id", "asignatura", "créditos_impartidos")
                .with_columns(pl.lit("falta pod_másteres_no_oficiales").alias("motivo"))
            )

    partes = [simples.drop("_n_tit")]
    if not resueltas_múltiples.is_empty():
        partes.append(resueltas_múltiples.drop("_n_tit"))
    pod_resuelto = pl.concat(partes, how="diagonal") if partes else pl.DataFrame()

    anomalías_df = pl.concat(anomalías, how="diagonal") if anomalías else pl.DataFrame()
    return pod_resuelto, anomalías_df, múltiples_oficiales


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

    resuelto, anomalías, múlt_ofi = _resolver_titulación_efectiva(
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

    ofi_path = dir_salida / "regla_23_múltiples_oficiales.parquet"
    if not múlt_ofi.is_empty():
        múlt_ofi.write_parquet(ofi_path)
        n_asig = múlt_ofi["asignatura"].n_unique()
        print(
            f"  Regla 23 — múltiples con oficial: {n_asig:,} asignaturas con "
            "varias titulaciones y al menos un máster oficial"
        )
    elif ofi_path.exists():
        ofi_path.unlink()

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
