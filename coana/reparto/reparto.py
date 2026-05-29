"""Cálculo del reparto de actividades (costes dag → actividades no-dag).

Toma el conjunto consolidado de UC de la fase 1 y, para cada UC cuya
actividad empieza por ``dag-``, reparte su importe entre las actividades
no-dag de su mismo centro de coste, en proporción al coste no-dag de cada
una en ese centro. La UC dag original se sustituye por sus fragmentos.

Salidas (en ``data/fase1/reparto/``):
- ``uc_post_reparto.parquet``: UC no-dag (intactas) + fragmentos + UC dag
  anómalas (intactas), todas con la columna nueva ``marca_dag``.
- ``porcentajes_centro.parquet``: tabla de % no-dag por (centro, actividad).
- ``anomalias.parquet``: UC dag que no se han repartido, con su motivo.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.reparto.tabla_dag_centro import cargar_tabla

_PREFIJO_DAG = "dag-"
# `dags` es el nodo paraguas (lo usan p.ej. las amortizaciones como
# actividad genérica). NO es una actividad finalista: se trata como dag y
# se reparte; nunca es destino del reparto.
_ACT_DAGS = "dags"
_ORIGEN = "reparto-dag"


def _es_dag() -> pl.Expr:
    act = pl.col("actividad").cast(pl.Utf8).fill_null("")
    return (act == _ACT_DAGS) | act.str.starts_with(_PREFIJO_DAG)

_MOTIVO_SIN_BASE = "centro_sin_base_no_dag"
_MOTIVO_INCONSISTENTE = "centro_distinto_de_tabla1"

_COLS_UC = [
    "id", "elemento_de_coste", "centro_de_coste", "actividad", "importe",
    "origen", "origen_id", "origen_porción", "marca_dag",
]


def _cargar_uc_fase1(ruta_base: Path) -> pl.DataFrame:
    ruta = Path(ruta_base) / "fase1" / "unidades de coste.xlsx"
    if not ruta.exists():
        raise FileNotFoundError(
            f"No existe {ruta}. Ejecuta antes la fase 1 (Cálculo de unidades de coste)."
        )
    return read_excel(ruta)


def calcular_reparto(
    uc: pl.DataFrame, tabla1: dict[str, str],
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Devuelve ``(post, porcentajes, anomalías)``. Función pura."""
    es_dag = _es_dag()
    no_dag = uc.filter(~es_dag)
    dag = uc.filter(es_dag)

    # --- Tabla de % no-dag por centro ---
    porcentajes = (
        no_dag.group_by("centro_de_coste", "actividad")
        .agg(pl.col("importe").sum().alias("importe_actividad"))
        .with_columns(
            pl.col("importe_actividad").sum().over("centro_de_coste").alias("total_no_dag_centro"),
        )
        .with_columns(
            (pl.col("importe_actividad") / pl.col("total_no_dag_centro")).alias("porcentaje"),
        )
        .with_columns(
            pl.concat_str([pl.col("centro_de_coste"), pl.lit("·"), pl.col("actividad")]).alias("clave"),
        )
        .sort("centro_de_coste", "importe_actividad", descending=[False, True])
    )

    centros_con_base = set(porcentajes["centro_de_coste"].to_list())

    # --- Anomalías ---
    if dag.is_empty():
        dag = dag.with_columns(pl.lit(None, dtype=pl.Utf8).alias("_centro_esperado"))
    else:
        esperado = pl.col("actividad").cast(pl.Utf8).replace_strict(
            tabla1, default=None, return_dtype=pl.Utf8,
        )
        dag = dag.with_columns(esperado.alias("_centro_esperado"))

    # (b) centro inconsistente con la Tabla 1 (esperado conocido y distinto).
    es_inconsistente = (
        pl.col("_centro_esperado").is_not_null()
        & (pl.col("_centro_esperado") != pl.col("centro_de_coste"))
    )
    # (a) centro sin base no-dag (entre las NO inconsistentes).
    es_sin_base = ~es_inconsistente & ~pl.col("centro_de_coste").is_in(list(centros_con_base))

    anom_b = dag.filter(es_inconsistente)
    anom_a = dag.filter(es_sin_base)
    válidas = dag.filter(~es_inconsistente & ~es_sin_base)

    anomalías = pl.concat([
        anom_b.select(
            "id", "centro_de_coste", "actividad", "importe",
            pl.lit(_MOTIVO_INCONSISTENTE).alias("motivo"),
            pl.col("_centro_esperado").alias("centro_esperado"),
        ),
        anom_a.select(
            "id", "centro_de_coste", "actividad", "importe",
            pl.lit(_MOTIVO_SIN_BASE).alias("motivo"),
            pl.lit(None, dtype=pl.Utf8).alias("centro_esperado"),
        ),
    ], how="vertical")

    # --- Fragmentos de las dag válidas ---
    dest = porcentajes.select(
        "centro_de_coste",
        pl.col("actividad").alias("_act_dest"),
        "porcentaje",
    )
    fragmentos = (
        válidas.join(dest, on="centro_de_coste", how="inner")
        .with_columns((pl.col("importe") * pl.col("porcentaje")).alias("_importe_frag"))
        .select(
            pl.concat_str([pl.lit("RDAG-"), pl.col("id"), pl.lit("·"), pl.col("_act_dest")]).alias("id"),
            pl.col("elemento_de_coste"),
            pl.col("centro_de_coste"),
            pl.col("_act_dest").alias("actividad"),
            pl.col("_importe_frag").alias("importe"),
            pl.lit(_ORIGEN).alias("origen"),
            pl.col("id").alias("origen_id"),
            pl.col("porcentaje").alias("origen_porción"),
            pl.col("actividad").alias("marca_dag"),
        )
    )

    # --- Conjunto post-reparto ---
    no_dag_out = no_dag.select(
        *[c for c in _COLS_UC if c != "marca_dag"],
        pl.lit(None, dtype=pl.Utf8).alias("marca_dag"),
    )
    # UC dag anómalas: se conservan intactas (sin repartir), marca_dag None.
    anómalas_uc = pl.concat([anom_b, anom_a], how="vertical").select(
        *[c for c in _COLS_UC if c != "marca_dag"],
        pl.lit(None, dtype=pl.Utf8).alias("marca_dag"),
    )
    post = pl.concat([no_dag_out, fragmentos, anómalas_uc], how="diagonal").select(_COLS_UC)

    return post, porcentajes, anomalías


def ejecutar(ruta_base: Path = Path("data")) -> None:
    """Orquestador de la fase de reparto de actividades."""
    ruta_base = Path(ruta_base)
    print("Repartiendo costes dag entre actividades no-dag…")
    uc = _cargar_uc_fase1(ruta_base)
    tabla1 = cargar_tabla(ruta_base)
    if not tabla1:
        print("    ⚠ Tabla dag→centro vacía o ausente; sin chequeo de consistencia.")

    total_entrada = float(uc["importe"].sum())
    dag_in = uc.filter(_es_dag())
    n_dag = dag_in.height
    imp_dag = float(dag_in["importe"].sum())

    post, porcentajes, anomalías = calcular_reparto(uc, tabla1)

    dir_out = ruta_base / "fase1" / "reparto"
    dir_out.mkdir(parents=True, exist_ok=True)
    post.write_parquet(dir_out / "uc_post_reparto.parquet")
    porcentajes.write_parquet(dir_out / "porcentajes_centro.parquet")
    anomalías.write_parquet(dir_out / "anomalias.parquet")

    frag = post.filter(pl.col("origen") == _ORIGEN)
    n_frag = frag.height
    imp_frag = float(frag["importe"].sum())
    n_anom = anomalías.height
    imp_anom = float(anomalías["importe"].sum()) if not anomalías.is_empty() else 0.0
    total_post = float(post["importe"].sum())

    # Cifras de cabecera para el visor (evita recomputarlas en cada request).
    pl.DataFrame([{
        "n_uc_entrada": uc.height,
        "imp_entrada": total_entrada,
        "n_uc_dag": n_dag,
        "imp_dag": imp_dag,
        "n_fragmentos": n_frag,
        "imp_fragmentos": imp_frag,
        "n_anomalias": n_anom,
        "imp_anomalias": imp_anom,
        "n_uc_post": post.height,
        "imp_post": total_post,
    }]).write_parquet(dir_out / "resumen.parquet")

    print(f"  UC de entrada: {uc.height:,} ({total_entrada:,.2f} €)")
    print(f"  UC dag: {n_dag:,} ({imp_dag:,.2f} €)")
    print(f"  Fragmentos generados: {n_frag:,} ({imp_frag:,.2f} €)")
    if n_anom:
        print(f"  ⚠ UC dag NO repartidas (anomalías): {n_anom:,} ({imp_anom:,.2f} €)")
        for motivo, sub in anomalías.group_by("motivo"):
            print(f"      · {motivo[0]}: {sub.height:,} ({float(sub['importe'].sum()):,.2f} €)")
    print(f"  UC post-reparto: {post.height:,} ({total_post:,.2f} €)")
    dif = total_post - total_entrada
    marca = "✓" if abs(dif) < 0.01 else "⚠"
    print(f"  Conservación del importe: diferencia {dif:,.4f} €  {marca}")
    print(f"  Escrito: {dir_out}")
