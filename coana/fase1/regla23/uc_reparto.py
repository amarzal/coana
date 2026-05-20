"""Generación de unidades de coste por reparto de la masa regla 23.

La «masa regla 23» es el subconjunto de las nóminas PDI/PVI con:

- aplicación que NO empieza por #val("12") (no es seguridad social)
- proyecto presupuestario que SÍ está en
  #campo("TABLA-PROYECTOS-GENERALES-NÓMINA")
- concepto retributivo NO en #val("19"), #val("64"), #val("47"),
  #val("48") (esos conceptos tienen tratamiento propio: cargos,
  despidos, indemnizaciones).

Esa masa no se imputa a una actividad concreta línea a línea: se
reparte por persona entre las actividades y centros de coste en
proporción a las horas finales calculadas por la regla 23
(`dedicación_pdi_normalizada.parquet`).

Para cada (per_id, elemento_de_coste) calculamos su importe total y lo
distribuimos entre las parejas (actividad, centro) de la persona con
peso `horas_finales / sum(horas_finales)`. Cada combinación
(per_id, ec, actividad, centro) genera una unidad de coste.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from coana.util import read_excel


def generar_uc_reparto_regla_23(
    ruta_base: Path,
    año: int = 2025,
) -> pl.DataFrame:
    """Genera y persiste las UC por reparto de la masa regla 23."""
    from coana.fase1.nóminas import _elemento_coste_pdi, _elemento_coste_pvi
    from coana.fase1.nóminas.regla_23 import _PROYECTOS_GENERALES

    # Preferir la nómina aplicada (tras descuento del CR 68 por extras
    # de cargos y filtro de atrasos no vinculados) si está disponible.
    # En su ausencia (p. ej. ejecución parcial), caer al Excel original.
    nom_aplicada_path = (
        ruta_base / "fase1" / "auxiliares" / "nóminas" / "nominas_aplicadas.parquet"
    )
    nom_path = ruta_base / "entrada" / "nóminas" / "nóminas y seguridad social.xlsx"
    exp_path = ruta_base / "entrada" / "nóminas" / "expedientes recursos humanos.xlsx"
    norm_path = ruta_base / "fase1" / "regla23" / "dedicación_pdi_normalizada.parquet"
    if not (exp_path.exists() and norm_path.exists()):
        return _esquema_vacío()

    if nom_aplicada_path.exists():
        nom = pl.read_parquet(nom_aplicada_path)
    elif nom_path.exists():
        nom = read_excel(nom_path)
    else:
        return _esquema_vacío()
    exp = read_excel(exp_path)
    norm = pl.read_parquet(norm_path)
    if nom.is_empty() or exp.is_empty() or norm.is_empty():
        return _esquema_vacío()

    # Filtro de la masa regla 23.
    cr = pl.col("concepto_retributivo").cast(pl.Utf8)
    proy = pl.col("proyecto").cast(pl.Utf8)
    apli = pl.col("aplicación").cast(pl.Utf8)
    masa = (
        nom.filter(~apli.str.starts_with("12"))
        .filter(proy.is_in(list(_PROYECTOS_GENERALES)))
        .filter(~cr.is_in(["19", "64", "47", "48"]))
        .filter(pl.col("fecha").dt.year() == año)
    )
    if masa.is_empty():
        return _esquema_vacío()

    # Solo PDI y PVI (el reparto regla 23 aplica a estos sectores). En
    # `expedientes recursos humanos.xlsx` el PVI viene codificado como
    # `PI`; lo normalizamos para que el cargador lo trate como PVI.
    exp_pdi_pvi = exp.filter(pl.col("sector").is_in(["PDI", "PVI", "PI"]))
    exp_pdi_pvi = exp_pdi_pvi.with_columns(
        pl.col("sector").replace({"PI": "PVI"})
    )
    masa = masa.join(
        exp_pdi_pvi.select("expediente", "per_id", "sector"),
        on="expediente", how="inner",
    )
    if masa.is_empty():
        return _esquema_vacío()

    # Calcular elemento de coste fila a fila (depende del sector).
    ecs: list[str | None] = []
    for r in masa.iter_rows(named=True):
        if r["sector"] == "PDI":
            ec = _elemento_coste_pdi(r.get("categoría"), r.get("concepto_retributivo"))
        else:  # PVI
            ec = _elemento_coste_pvi(
                r.get("categoría"), r.get("perceptor"), r.get("provisión"),
                r.get("concepto_retributivo"),
                r.get("categoría_plaza"), r.get("sector_plaza"),
            )
        ecs.append(ec)
    masa = masa.with_columns(pl.Series("_ec", ecs))

    sin_ec = masa.filter(pl.col("_ec").is_null())
    if not sin_ec.is_empty():
        imp_err = float(sin_ec["importe"].sum())
        print(
            f"    ⚠ {len(sin_ec):,} registros de masa regla 23 sin elemento "
            f"de coste ({imp_err:,.2f} €) — se descartan."
        )
    masa = masa.filter(pl.col("_ec").is_not_null())
    if masa.is_empty():
        return _esquema_vacío()

    # Importe por (per_id, expediente, elemento_de_coste). Conservamos
    # `expediente` para que la UC resultante pueda atribuirse a un
    # expediente concreto (no a un «principal» elegido a posteriori).
    # Cuando una persona tiene varios expedientes PDI/PVI, cada uno
    # se queda con SU cacho de masa regla 23 prorrateado por sus
    # propias líneas; los pesos por (actividad, centro_de_coste) son
    # los de la persona (la regla 23 calcula horas a nivel per_id, no
    # por expediente).
    masa_pp = (
        masa.group_by("per_id", "expediente", "_ec")
        .agg(pl.col("importe").sum().alias("importe_ec"))
    )

    # Pesos por (per_id, actividad, centro_de_coste).
    pesos = (
        norm.filter(pl.col("horas_finales") > 0)
        .group_by("per_id", "actividad", "centro_de_coste")
        .agg(pl.col("horas_finales").sum().alias("h"))
    )
    totales = pesos.group_by("per_id").agg(pl.col("h").sum().alias("h_total"))
    pesos = pesos.join(totales, on="per_id", how="left").with_columns(
        (pl.col("h") / pl.col("h_total")).alias("peso")
    ).filter(pl.col("h_total") > 0)

    # Personas con masa pero sin dedicación → fallback a (pendiente,
    # pendiente) con peso 1 (toda la masa de la persona a esa única
    # fila) para no perder importe del coste analítico.
    sin_ded = masa_pp.join(
        totales.select("per_id"), on="per_id", how="anti",
    )
    if not sin_ded.is_empty():
        imp_huérf = float(sin_ded["importe_ec"].sum())
        n_personas = sin_ded["per_id"].n_unique()
        print(
            f"    ⚠ {n_personas:,} personas con masa regla 23 sin "
            f"dedicación calculada ({imp_huérf:,.2f} €) — repartido a "
            f"(actividad=pendiente, centro=pendiente)."
        )
        sin_ded_uc = sin_ded.select(
            "per_id", "expediente", "_ec", "importe_ec",
        ).with_columns(
            pl.lit("pendiente").alias("actividad"),
            pl.lit("pendiente").alias("centro_de_coste"),
            pl.lit(1.0).alias("peso"),
        )
    else:
        sin_ded_uc = None

    # Producto cartesiano (per_id × expediente × ec) × (per_id ×
    # actividad × centro). El expediente se conserva en la UC final.
    uc = masa_pp.join(pesos, on="per_id", how="inner").with_columns(
        (pl.col("importe_ec") * pl.col("peso")).round(2).alias("importe"),
    )
    if sin_ded_uc is not None:
        sin_ded_uc = sin_ded_uc.with_columns(
            (pl.col("importe_ec") * pl.col("peso")).round(2).alias("importe"),
        )
        cols_comunes = [
            "per_id", "expediente", "_ec", "importe_ec",
            "actividad", "centro_de_coste", "peso", "importe",
        ]
        uc = pl.concat([
            uc.select(cols_comunes), sin_ded_uc.select(cols_comunes),
        ], how="vertical_relaxed")

    # Esquema UC estándar.
    uc = uc.with_columns(
        (pl.lit("R23-") + pl.col("per_id").cast(pl.Utf8) + pl.lit("-")
         + pl.col("expediente").cast(pl.Utf8) + pl.lit("-")
         + pl.col("_ec") + pl.lit("-") + pl.col("actividad")
         + pl.lit("-") + pl.col("centro_de_coste")).alias("origen_id"),
    )
    uc_out = uc.with_row_index("_n", offset=1).select(
        (pl.lit("R23-") + pl.col("_n").cast(pl.Utf8).str.zfill(6)).alias("id"),
        pl.col("_ec").alias("elemento_de_coste"),
        pl.col("centro_de_coste"),
        pl.col("actividad"),
        pl.col("importe"),
        pl.lit("regla_23").alias("origen"),
        pl.col("origen_id"),
        pl.col("peso").alias("origen_porción"),
        pl.col("per_id"),
        pl.col("expediente"),
    )

    dir_out = ruta_base / "fase1" / "regla23"
    dir_out.mkdir(parents=True, exist_ok=True)
    uc_out.write_parquet(dir_out / "uc_reparto_regla_23.parquet")
    return uc_out


def _esquema_vacío() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "id": pl.Utf8,
        "elemento_de_coste": pl.Utf8,
        "centro_de_coste": pl.Utf8,
        "actividad": pl.Utf8,
        "importe": pl.Float64,
        "origen": pl.Utf8,
        "origen_id": pl.Utf8,
        "origen_porción": pl.Float64,
        "per_id": pl.Int64,
        "expediente": pl.Int64,
    })
