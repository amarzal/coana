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
    estudio, nombre_estudio``. ``estudio`` y ``nombre_estudio`` pueden
    ser null para titulaciones sin estudio (grados antiguos, másteres
    no oficiales).
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
    )
    est = read_excel(d / "estudios.xlsx").select(
        "estudio", pl.col("nombre").alias("nombre_estudio"),
    )

    grado_tit = (
        ag.join(gr, left_on="grado", right_on="titulación", how="inner")
        .with_columns(
            pl.lit("grado").alias("tipo"),
            pl.col("grado").alias("titulación"),
        )
        .drop("grado")
        .join(est, on="estudio", how="left")
    )
    máster_tit = (
        am.join(mr, left_on="máster", right_on="titulación", how="inner")
        .with_columns(
            pl.lit("máster").alias("tipo"),
            pl.col("máster").alias("titulación"),
        )
        .drop("máster")
        .join(est, on="estudio", how="left")
    )
    return pl.concat([grado_tit, máster_tit], how="vertical_relaxed")


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

    ded = exp.join(
        pod.select("per_id", "asignatura", "créditos_impartidos"),
        on="per_id",
        how="inner",
    )
    if ded.is_empty():
        return pl.DataFrame()

    dir_salida.mkdir(parents=True, exist_ok=True)
    ded.write_parquet(dir_salida / "regla_23_dedicación_docente.parquet")

    n_exp = ded["expediente"].n_unique()
    tot_cred = float(ded["créditos_impartidos"].sum())
    print(
        f"  Regla 23 — dedicación docente: {n_exp:,} expedientes, "
        f"{len(ded):,} filas, {tot_cred:,.2f} créditos impartidos"
    )
    return ded


def generar_dedicación_titulaciones(
    expedientes: pl.DataFrame,
    ruta_base: Path,
    dir_salida: Path,
) -> pl.DataFrame:
    """Diccionario {(tipo, titulación, nombre): créditos} por expediente PDI/PVI.

    Si una asignatura aparece en varias titulaciones (común en másteres),
    sus créditos se reparten a partes iguales entre ellas.
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

    asig_tit = _asignatura_titulaciones(ruta_base).select(
        "asignatura", "tipo", "titulación", "nombre_titulación",
    )
    n_tit = (
        asig_tit.group_by("asignatura")
        .agg(pl.len().alias("_n_tit"))
    )
    ded = (
        ded.join(asig_tit, on="asignatura", how="inner")
        .join(n_tit, on="asignatura", how="left")
        .with_columns(
            (pl.col("créditos_impartidos") / pl.col("_n_tit"))
            .alias("créditos_impartidos")
        )
    )

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
    expedientes: pl.DataFrame,
    ruta_base: Path,
    dir_salida: Path,
) -> pl.DataFrame:
    """Diccionario {(tipo, código, nombre): créditos} por expediente PDI/PVI.

    Si la titulación tiene estudio, la UC se agrega a ese estudio. Si
    no, la titulación se pasa directamente como estudio sintético
    (``tipo`` = 'grado' o 'máster', ``código`` = código de la titulación).
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

    asig_tit = _asignatura_titulaciones(ruta_base)
    # Clave de estudio: real si hay estudio, sintética («estudio propio») si no.
    asig_est = asig_tit.with_columns(
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
          .alias("nombre_estudio"),
    ).select("asignatura", "tipo_estudio", "código_estudio", "nombre_estudio").unique()

    n_est = (
        asig_est.group_by("asignatura")
        .agg(pl.len().alias("_n_est"))
    )
    ded = (
        ded.join(asig_est, on="asignatura", how="inner")
        .join(n_est, on="asignatura", how="left")
        .with_columns(
            (pl.col("créditos_impartidos") / pl.col("_n_est"))
            .alias("créditos_impartidos")
        )
    )

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
