"""Cálculo del reparto de actividades (costes dag → actividades finalistas).

Toma el conjunto consolidado de UC de la fase 1 y reparte cada UC cuya
**actividad** es dag (subárbol de `dags`, código `02.*`) entre actividades
**finalistas** (subárbol de `principales`, `01.*`), conservando su elemento de
coste. El destino se decide por la *tabla de reglas* (`tabla_dag_centro.REGLAS`,
índice (centro, actividad) → destino (centro, actividades)); si ninguna regla
casa, por *defecto* se reparte entre las finalistas del propio centro; si no hay
base, la UC queda como *anomalía* (sin repartir).

El peso de cada finalista hoja del destino es su coste no-dag directo sobre el
total del destino. Para acotar el nº de fragmentos, las UC dag se agregan por
(centro, actividad, elemento de coste) antes de repartir.

Salidas (en ``data/fase1/reparto/``):
- ``uc_post_reparto.parquet``: UC no-dag (intactas) + fragmentos + UC dag
  anómalas (intactas), con la columna ``marca_dag`` (la actividad dag de origen).
- ``porcentajes_centro.parquet``: % no-dag finalista por (centro, actividad).
- ``anomalias.parquet``: UC dag no repartidas, con su motivo.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from coana.util import read_excel, Árbol
from coana.reparto.tabla_dag_centro import (
    MISMO_CENTRO, RAÍZ_DAG, RAÍZ_FINALISTAS, REGLAS, ReglaDag,
)

_LEAVES_SCHEMA = {
    "_destino_id": pl.Utf8, "_c_leaf": pl.Utf8,
    "_a_leaf": pl.Utf8, "_peso": pl.Float64,
}

_ORIGEN = "reparto-dag"
_MOTIVO_SIN_REGLA_NI_BASE = "sin_regla_ni_base"
_MOTIVO_DESTINO_SIN_BASE = "destino_de_regla_sin_base"

_COLS_UC = [
    "id", "elemento_de_coste", "centro_de_coste", "actividad", "importe",
    "origen", "origen_id", "origen_porción", "marca_dag",
]


# ----------------------------------------------------------------------
# Árboles y subárboles
# ----------------------------------------------------------------------

def _árbol(ruta_base: Path, nombre: str) -> Árbol:
    final = Path(ruta_base) / "fase1" / f"{nombre}.tree"
    if final.exists():
        return Árbol.from_file(final)
    return Árbol.from_file(
        Path(ruta_base) / "entrada" / "estructuras" / f"{nombre}.tree"
    )


def _descendientes(árbol: Árbol, slug: str) -> set[str]:
    """Identificadores del nodo `slug` y todo su subárbol (por prefijo de
    código). La raíz (código vacío, p. ej. `UJI`) devuelve todo el árbol.
    Conjunto vacío si el slug no existe."""
    nodo = árbol._por_id.get(slug)
    if nodo is None:
        return set()
    cod = nodo.código
    if not cod:  # raíz: todo el árbol
        return {ident for ident in árbol._por_id if ident}
    pref = cod + "."
    return {
        ident for ident, n in árbol._por_id.items()
        if ident and (n.código == cod or n.código.startswith(pref))
    }


def _cargar_uc_fase1(ruta_base: Path) -> pl.DataFrame:
    ruta = Path(ruta_base) / "fase1" / "unidades de coste.xlsx"
    if not ruta.exists():
        raise FileNotFoundError(
            f"No existe {ruta}. Ejecuta antes la fase 1 (Cálculo de unidades de coste)."
        )
    return read_excel(ruta)


# ----------------------------------------------------------------------
# Reparto
# ----------------------------------------------------------------------

def calcular_reparto(
    uc: pl.DataFrame,
    reglas: list[ReglaDag],
    árbol_centros: Árbol,
    árbol_actividades: Árbol,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Devuelve ``(post, porcentajes, anomalías)``. Función pura."""
    finalistas = _descendientes(árbol_actividades, RAÍZ_FINALISTAS)
    dags = _descendientes(árbol_actividades, RAÍZ_DAG)

    # `fill_null("")` para que las UC con actividad nula cuenten como no-dag
    # (si no, `is_in` devuelve null y se perderían en ambos filtros).
    es_dag = pl.col("actividad").cast(pl.Utf8).fill_null("").is_in(list(dags))
    no_dag = uc.filter(~es_dag)
    dag = uc.filter(es_dag)

    # --- Base del reparto: coste no-dag finalista por (centro, actividad) ---
    base = (
        no_dag.filter(pl.col("actividad").cast(pl.Utf8).is_in(list(finalistas)))
        .group_by("centro_de_coste", "actividad")
        .agg(pl.col("importe").sum().alias("_coste"))
        .filter(pl.col("_coste") > 0)
    )
    centros_con_base = set(base["centro_de_coste"].to_list())

    porcentajes = (
        base.with_columns(
            pl.col("_coste").alias("importe_actividad"),
            pl.col("_coste").sum().over("centro_de_coste").alias("total_no_dag_centro"),
        )
        .with_columns(
            (pl.col("importe_actividad") / pl.col("total_no_dag_centro")).alias("porcentaje"),
            pl.concat_str([pl.col("centro_de_coste"), pl.lit("·"), pl.col("actividad")]).alias("clave"),
        )
        .select(
            "centro_de_coste", "actividad", "importe_actividad",
            "total_no_dag_centro", "porcentaje", "clave",
        )
        .sort("centro_de_coste", "importe_actividad", descending=[False, True])
    )

    if dag.is_empty():
        post = no_dag.select(
            *[c for c in _COLS_UC if c != "marca_dag"],
            pl.lit(None, dtype=pl.Utf8).alias("marca_dag"),
        ).select(_COLS_UC)
        anom = pl.DataFrame(schema={
            "id": pl.Utf8, "centro_de_coste": pl.Utf8, "actividad": pl.Utf8,
            "importe": pl.Float64, "motivo": pl.Utf8, "centro_esperado": pl.Utf8,
        })
        return post, porcentajes, anom

    # --- Resolver el destino de cada par (centro, actividad) dag ---
    reglas_exp = [
        (
            _descendientes(árbol_centros, r.centro_índice),
            _descendientes(árbol_actividades, r.actividad_índice),
            i,
        )
        for i, r in enumerate(reglas)
    ]
    pares = dag.select("centro_de_coste", "actividad").unique().rows()
    finalistas_lista = list(finalistas)

    def _ponderar(t: pl.DataFrame, destino_id: str) -> pl.DataFrame:
        """Leaves ponderadas por coste (peso = coste / Σ coste)."""
        total = float(t["_coste"].sum() or 0.0)
        return t.select(
            pl.lit(destino_id).alias("_destino_id"),
            pl.col("centro_de_coste").alias("_c_leaf"),
            pl.col("actividad").alias("_a_leaf"),
            (pl.col("_coste") / total).alias("_peso"),
        )

    def _iguales(centros: list[str], acts: list[str], destino_id: str) -> pl.DataFrame:
        """Leaves a partes iguales sobre (centros × actividades finalistas
        nombradas). Materializa los destinos aunque no tengan UCs previas."""
        combos = [(c, a) for c in centros for a in acts if a in finalistas]
        if not combos:
            return pl.DataFrame(schema=_LEAVES_SCHEMA)
        peso = 1.0 / len(combos)
        return pl.DataFrame({
            "_destino_id": [destino_id] * len(combos),
            "_c_leaf": [c for c, _ in combos],
            "_a_leaf": [a for _, a in combos],
            "_peso": [peso] * len(combos),
        }, schema=_LEAVES_SCHEMA)

    def _leaves_regla(i: int) -> pl.DataFrame:
        r = reglas[i]
        cset = _descendientes(árbol_centros, r.centro_destino)
        aset: set[str] = set()
        for a in r.actividades_destino:
            aset |= _descendientes(árbol_actividades, a)
        t = base.filter(
            pl.col("centro_de_coste").is_in(list(cset))
            & pl.col("actividad").is_in(list(aset & finalistas))
        )
        if not t.is_empty() and float(t["_coste"].sum() or 0.0) > 0:
            return _ponderar(t, f"R{i}")
        # Base cero: a partes iguales entre las actividades nombradas, en el
        # centro destino de la regla.
        return _iguales([r.centro_destino], list(r.actividades_destino), f"R{i}")

    def _leaves_mismo(i: int, c: str) -> pl.DataFrame:
        """Destino «mismo centro»: reparte entre TODAS las finalistas del propio
        centro `c` (= comportamiento por defecto); si `c` no tiene base
        finalista, transforma a las actividades nombradas (a partes iguales)."""
        did = f"R{i}:{c}"
        t = base.filter(
            (pl.col("centro_de_coste") == c)
            & pl.col("actividad").is_in(finalistas_lista)
        )
        if not t.is_empty() and float(t["_coste"].sum() or 0.0) > 0:
            return _ponderar(t, did)
        return _iguales([c], list(reglas[i].actividades_destino), did)

    leaves_cache: dict[str, pl.DataFrame] = {}

    asignación: list[dict] = []          # (centro, actividad) -> _destino_id
    anom_pares: set[tuple[str, str]] = set()
    motivo_par: dict[tuple[str, str], str] = {}

    for c, a in pares:
        destino_id = None
        for cset, aset, i in reglas_exp:
            if c in cset and a in aset:
                if reglas[i].centro_destino == MISMO_CENTRO:
                    did = f"R{i}:{c}"
                    if did not in leaves_cache:
                        leaves_cache[did] = _leaves_mismo(i, c)
                else:
                    did = f"R{i}"
                    if did not in leaves_cache:
                        leaves_cache[did] = _leaves_regla(i)
                if leaves_cache[did].is_empty():
                    anom_pares.add((c, a))
                    motivo_par[(c, a)] = _MOTIVO_DESTINO_SIN_BASE
                else:
                    destino_id = did
                break
        else:
            # ninguna regla casó
            if c in centros_con_base:
                destino_id = f"C:{c}"
            else:
                anom_pares.add((c, a))
                motivo_par[(c, a)] = _MOTIVO_SIN_REGLA_NI_BASE
        if destino_id is not None:
            asignación.append({"centro_de_coste": c, "actividad": a, "_destino_id": destino_id})

    # --- Tabla de leaves destino (reglas + defecto) ---
    leaves_parts = [df for df in leaves_cache.values() if not df.is_empty()]
    centros_defecto = sorted({
        r["centro_de_coste"] for r in asignación if r["_destino_id"].startswith("C:")
    })
    if centros_defecto:
        leaves_parts.append(
            porcentajes.filter(pl.col("centro_de_coste").is_in(centros_defecto)).select(
                (pl.lit("C:") + pl.col("centro_de_coste")).alias("_destino_id"),
                pl.col("centro_de_coste").alias("_c_leaf"),
                pl.col("actividad").alias("_a_leaf"),
                pl.col("porcentaje").alias("_peso"),
            )
        )
    leaves = (
        pl.concat(leaves_parts, how="vertical")
        if leaves_parts else
        pl.DataFrame(schema={
            "_destino_id": pl.Utf8, "_c_leaf": pl.Utf8,
            "_a_leaf": pl.Utf8, "_peso": pl.Float64,
        })
    )

    # --- Fragmentos: dag agregado por (centro, actividad, EC) × leaves ---
    dag_g = (
        dag.group_by("centro_de_coste", "actividad", "elemento_de_coste")
        .agg(pl.col("importe").sum().alias("importe"))
    )
    if asignación:
        mapa = pl.DataFrame(asignación)
        dag_distrib = dag_g.join(mapa, on=["centro_de_coste", "actividad"], how="inner")
        fragmentos = (
            dag_distrib.join(leaves, on="_destino_id", how="inner")
            .with_columns(
                (pl.col("importe") * pl.col("_peso")).alias("_imp"),
                pl.concat_str([
                    pl.lit("RDAG-"), pl.col("centro_de_coste"), pl.lit("-"),
                    pl.col("actividad"), pl.lit("-"), pl.col("elemento_de_coste"),
                ]).alias("_oid"),
                pl.col("actividad").alias("_marca"),
            )
            .select(
                pl.col("elemento_de_coste"),
                pl.col("_c_leaf").alias("centro_de_coste"),
                pl.col("_a_leaf").alias("actividad"),
                pl.col("_imp").alias("importe"),
                pl.lit(_ORIGEN).alias("origen"),
                pl.col("_oid").alias("origen_id"),
                pl.col("_peso").alias("origen_porción"),
                pl.col("_marca").alias("marca_dag"),
            )
            .with_row_index("_n", offset=1)
            .with_columns(
                pl.concat_str([pl.lit("RDAG-"), pl.col("_n").cast(pl.Utf8).str.zfill(8)]).alias("id"),
            )
            .drop("_n")
            .select(_COLS_UC)
        )
    else:
        fragmentos = pl.DataFrame(schema={c: dag.schema.get(c, pl.Utf8) for c in _COLS_UC})

    # --- UC dag anómalas (sin repartir, intactas) ---
    if anom_pares:
        clave_anom = pl.concat_str([pl.col("centro_de_coste"), pl.lit("§"), pl.col("actividad")])
        set_anom = {f"{c}§{a}" for c, a in anom_pares}
        dag_anom = dag.filter(clave_anom.is_in(list(set_anom)))
    else:
        dag_anom = dag.head(0)

    anómalas_uc = dag_anom.select(
        *[c for c in _COLS_UC if c != "marca_dag"],
        pl.lit(None, dtype=pl.Utf8).alias("marca_dag"),
    )

    # --- Anomalías (informe) ---
    if not dag_anom.is_empty():
        motivo_expr = pl.concat_str([pl.col("centro_de_coste"), pl.lit("§"), pl.col("actividad")])
        mapa_motivo = pl.DataFrame({
            "_k": [f"{c}§{a}" for (c, a) in motivo_par],
            "motivo": [motivo_par[(c, a)] for (c, a) in motivo_par],
        })
        anomalías = (
            dag_anom.with_columns(motivo_expr.alias("_k"))
            .join(mapa_motivo, on="_k", how="left")
            .select(
                "id", "centro_de_coste", "actividad", "importe",
                pl.col("motivo").fill_null(_MOTIVO_SIN_REGLA_NI_BASE),
                pl.lit(None, dtype=pl.Utf8).alias("centro_esperado"),
            )
        )
    else:
        anomalías = pl.DataFrame(schema={
            "id": pl.Utf8, "centro_de_coste": pl.Utf8, "actividad": pl.Utf8,
            "importe": pl.Float64, "motivo": pl.Utf8, "centro_esperado": pl.Utf8,
        })

    # --- Conjunto post-reparto ---
    no_dag_out = no_dag.select(
        *[c for c in _COLS_UC if c != "marca_dag"],
        pl.lit(None, dtype=pl.Utf8).alias("marca_dag"),
    )
    post = pl.concat(
        [no_dag_out, fragmentos, anómalas_uc], how="diagonal",
    ).select(_COLS_UC)

    return post, porcentajes, anomalías


def ejecutar(ruta_base: Path = Path("data")) -> None:
    """Orquestador de la fase de reparto de actividades."""
    ruta_base = Path(ruta_base)
    print("Repartiendo costes dag entre actividades finalistas…")
    uc = _cargar_uc_fase1(ruta_base)
    árbol_centros = _árbol(ruta_base, "centros de coste")
    árbol_actividades = _árbol(ruta_base, "actividades")

    total_entrada = float(uc["importe"].sum())
    dags = _descendientes(árbol_actividades, RAÍZ_DAG)
    dag_in = uc.filter(pl.col("actividad").cast(pl.Utf8).fill_null("").is_in(list(dags)))
    n_dag = dag_in.height
    imp_dag = float(dag_in["importe"].sum())

    post, porcentajes, anomalías = calcular_reparto(
        uc, REGLAS, árbol_centros, árbol_actividades,
    )

    dir_out = ruta_base / "fase1" / "reparto"
    dir_out.mkdir(parents=True, exist_ok=True)
    post.write_parquet(dir_out / "uc_post_reparto.parquet")
    porcentajes.write_parquet(dir_out / "porcentajes_centro.parquet")
    anomalías.write_parquet(dir_out / "anomalias.parquet")

    frag = post.filter(pl.col("origen") == _ORIGEN)
    n_frag = frag.height
    imp_frag = float(frag["importe"].sum()) if not frag.is_empty() else 0.0
    n_anom = anomalías.height
    imp_anom = float(anomalías["importe"].sum()) if not anomalías.is_empty() else 0.0
    total_post = float(post["importe"].sum())

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
