"""Cálculo del reparto de actividades (costes dag → actividades finalistas).

Toma el conjunto consolidado de UC de la fase 1 y reparte cada UC cuya
**actividad** es dag (subárbol de `dags`) entre actividades **finalistas**
(subárbol de `principales`), conservando su elemento de coste.

El destino se decide con la *tabla de reglas por patrones* de
`tabla_dag_centro.REGLAS`: una lista ordenada de
``ORIGEN(actividad, centro) → DESTINO(actividad, centro)``; gana la
**primera** regla cuyo ORIGEN casa la UC dag. La UC origen se reparte entre
las UC no-dag finalistas que casan el DESTINO, **proporcional a su importe**.
El comodín ``·mismo·`` del centro destino = el centro del origen y su
subárbol. Si la regla que casa deja el destino *vacío*, la UC queda como
*anomalía* (sin repartir, conservada intacta).

Cada fragmento conserva el *elemento de coste del origen*, toma el centro y
la actividad del destino, y se anota con ``marca_dag`` (la actividad dag de
procedencia). Para acotar el nº de fragmentos, las UC dag se agregan por
(centro, actividad, elemento de coste) antes de repartir.

Salidas (en ``data/fase1/reparto/``):
- ``uc_post_reparto.parquet``: UC no-dag (intactas) + fragmentos + UC dag
  anómalas (intactas), con la columna ``marca_dag``.
- ``porcentajes_centro.parquet``: peso no-dag finalista por (centro, actividad).
- ``anomalias.parquet``: UC dag no repartidas, con su motivo.
"""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from coana.util import read_excel, Árbol
from coana.reparto.tabla_dag_centro import (
    MISMO, RAÍZ_DAG, RAÍZ_FINALISTAS, REGLAS, ReglaDag,
)

_ORIGEN = "reparto-dag"
_MOTIVO_DESTINO_VACÍO = "destino_vacío"
_MOTIVO_SIN_REGLA = "sin_regla"

_COLS_UC = [
    "id", "elemento_de_coste", "centro_de_coste", "actividad", "importe",
    "origen", "origen_id", "origen_porción", "marca_dag",
]

_LEAVES_SCHEMA = {
    "_destino_id": pl.Utf8, "_c_leaf": pl.Utf8,
    "_a_leaf": pl.Utf8, "_peso": pl.Float64,
}


# ----------------------------------------------------------------------
# Árboles, subárboles y patrones
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
    código). La raíz (código vacío) devuelve todo el árbol. Conjunto vacío
    si el slug no existe."""
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


def _ids_patrón(patrón: str, árbol: Árbol) -> set[str]:
    """Identificadores que casa un patrón: ``*`` (todos), ``etiqueta``
    (exacto) o ``etiqueta.*`` (nodo + subárbol)."""
    if patrón == "*":
        return {ident for ident in árbol._por_id if ident}
    if patrón.endswith(".*"):
        return _descendientes(árbol, patrón[:-2])
    return {patrón} if patrón in árbol._por_id else set()


def _ids_patrones(patrones: tuple[str, ...], árbol: Árbol) -> set[str]:
    """Unión de los conjuntos de varios patrones."""
    s: set[str] = set()
    for p in patrones:
        s |= _ids_patrón(p, árbol)
    return s


def _ident_base(patrón: str) -> str | None:
    """Identificador base de un patrón (sin `.*`); None para `*`."""
    if patrón == "*":
        return None
    return patrón[:-2] if patrón.endswith(".*") else patrón


def _idents_base(patrones: tuple[str, ...], árbol: Árbol) -> list[str]:
    """Identificadores base de los patrones que existen en el árbol
    (para materializar el destino nombrado)."""
    out: list[str] = []
    for p in patrones:
        b = _ident_base(p)
        if b is not None and b in árbol._por_id and b not in out:
            out.append(b)
    return out


def _parse_grupos(patrones: tuple[str, ...]) -> list[tuple[str, float | None]]:
    """Parsea los patrones de actividad DESTINO en grupos (patrón, fracción).

    Notación de reparto porcentual por grupo: ``"docencia.* 20% + ai.* 80%"``
    (el 20 % de la masa al grupo docencia, el 80 % al grupo ai). El ``+`` y
    el ``%`` pueden venir en una sola cadena o en elementos separados de la
    tupla. Sin porcentaje → fracción ``None`` (todo el conjunto pondera solo
    por importe). Devuelve ``[(patrón, fracción|None), …]``."""
    out: list[tuple[str, float | None]] = []
    for elem in patrones:
        for sub in elem.split("+"):
            sub = sub.strip()
            if not sub:
                continue
            m = re.match(r"^(.*?)\s+(\d+(?:\.\d+)?)\s*%$", sub)
            if m:
                out.append((m.group(1).strip(), float(m.group(2)) / 100.0))
            else:
                out.append((sub, None))
    return out


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

    es_dag = pl.col("actividad").cast(pl.Utf8).fill_null("").is_in(list(dags))
    no_dag = uc.filter(~es_dag)
    dag = uc.filter(es_dag)

    # --- Base del reparto: importe no-dag finalista por (centro, actividad) ---
    base = (
        no_dag.filter(pl.col("actividad").cast(pl.Utf8).is_in(list(finalistas)))
        .group_by("centro_de_coste", "actividad")
        .agg(pl.col("importe").sum().alias("_coste"))
        .filter(pl.col("_coste") > 0)
    )

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

    # --- Precomputar los conjuntos de cada regla ---
    reglas_exp = []
    for r in reglas:
        origen_act = _ids_patrones(r.origen_actividad, árbol_actividades) & dags
        origen_cen = _ids_patrones(r.origen_centro, árbol_centros)
        # Grupos de actividad destino: (conjunto de ids finalistas, fracción).
        grupos_parsed = _parse_grupos(r.destino_actividad)
        destino_grupos = [
            (_ids_patrón(p, árbol_actividades) & finalistas, frac)
            for p, frac in grupos_parsed
        ]
        cen_fijos = tuple(p for p in r.destino_centro if p != MISMO)
        destino_cen_fijo = _ids_patrones(cen_fijos, árbol_centros) if cen_fijos else set()
        tiene_mismo = MISMO in r.destino_centro
        # Para materializar: actividad(es) de reserva explícitas si se dieron;
        # si no, las bases nombradas del propio destino (comportamiento previo).
        mat_pat = r.materializar_actividad or tuple(p for p, _ in grupos_parsed)
        act_mat = _idents_base(mat_pat, árbol_actividades)
        cen_mat_fijo = _idents_base(cen_fijos, árbol_centros)
        reglas_exp.append((
            origen_act, origen_cen, destino_grupos, destino_cen_fijo, tiene_mismo,
            r.materializar, act_mat, cen_mat_fijo,
        ))

    pares = dag.select("centro_de_coste", "actividad").unique().rows()

    def _materializar(centros: list[str], acts: list[str], did: str) -> pl.DataFrame:
        """Destino nombrado a partes iguales (crea (centro, act) aunque no
        tengan UCs previas)."""
        combos = [(c, a) for c in centros for a in acts]
        if not combos:
            return pl.DataFrame(schema=_LEAVES_SCHEMA)
        peso = 1.0 / len(combos)
        return pl.DataFrame({
            "_destino_id": [did] * len(combos),
            "_c_leaf": [c for c, _ in combos],
            "_a_leaf": [a for _, a in combos],
            "_peso": [peso] * len(combos),
        }, schema=_LEAVES_SCHEMA)

    def _leaves_grupo(centro_set: set[str], act_set: set[str], did: str, factor: float) -> pl.DataFrame:
        """Destinos de UN grupo, ponderados por importe y escalados por
        `factor` (la fracción de la masa que le toca al grupo). Vacío si no
        hay ninguna UC no-dag con coste que case."""
        t = base.filter(
            pl.col("centro_de_coste").is_in(list(centro_set))
            & pl.col("actividad").is_in(list(act_set))
        )
        total = float(t["_coste"].sum() or 0.0)
        if t.is_empty() or total <= 0:
            return pl.DataFrame(schema=_LEAVES_SCHEMA)
        return t.select(
            pl.lit(did).alias("_destino_id"),
            pl.col("centro_de_coste").alias("_c_leaf"),
            pl.col("actividad").alias("_a_leaf"),
            (factor * pl.col("_coste") / total).alias("_peso"),
        )

    def _leaves(centro_set: set[str], grupos: list, did: str) -> pl.DataFrame:
        """Destinos de una regla. Si algún grupo lleva fracción (porcentaje),
        cada grupo recibe su fracción de la masa y, dentro, pondera por
        importe; si un grupo con fracción queda vacío, el destino es
        *incompleto* → se devuelve vacío (anomalía, para no romper la
        conservación). Sin fracciones, todos los grupos forman un único
        conjunto ponderado solo por importe."""
        pesado = any(frac is not None for _, frac in grupos)
        if not pesado:
            act_all: set[str] = set()
            for act_set, _ in grupos:
                act_all |= act_set
            return _leaves_grupo(centro_set, act_all, did, 1.0)
        partes: list[pl.DataFrame] = []
        for act_set, frac in grupos:
            f = frac or 0.0
            if f <= 0:
                continue
            lv = _leaves_grupo(centro_set, act_set, did, f)
            if lv.is_empty():
                return pl.DataFrame(schema=_LEAVES_SCHEMA)  # grupo vacío → anomalía
            partes.append(lv)
        if not partes:
            return pl.DataFrame(schema=_LEAVES_SCHEMA)
        return pl.concat(partes, how="vertical")

    leaves_cache: dict[str, pl.DataFrame] = {}
    asignación: list[dict] = []
    anom_pares: set[tuple[str, str]] = set()
    motivo_par: dict[tuple[str, str], str] = {}

    for c, a in pares:
        destino_id = None
        for i, (oa, oc, grupos, dcf, mismo, materializar, act_mat, cen_mat_fijo) in enumerate(reglas_exp):
            if a not in oa or c not in oc:
                continue
            # Primera regla que casa el ORIGEN.
            if mismo:
                centro_set = dcf | _descendientes(árbol_centros, c)
                did = f"R{i}:{c}"
            else:
                centro_set = dcf
                did = f"R{i}"
            if did not in leaves_cache:
                lv = _leaves(centro_set, grupos, did)
                if lv.is_empty() and materializar:
                    # Materializar el destino nombrado a partes iguales.
                    centros_mat = ([c] if mismo else []) + cen_mat_fijo
                    lv = _materializar(centros_mat, act_mat, did)
                leaves_cache[did] = lv
            if leaves_cache[did].is_empty():
                anom_pares.add((c, a))
                motivo_par[(c, a)] = _MOTIVO_DESTINO_VACÍO
            else:
                destino_id = did
            break
        else:
            # No casó ninguna regla (centro fuera del árbol, etc.).
            anom_pares.add((c, a))
            motivo_par[(c, a)] = _MOTIVO_SIN_REGLA
        if destino_id is not None:
            asignación.append({"centro_de_coste": c, "actividad": a, "_destino_id": destino_id})

    leaves_parts = [df for df in leaves_cache.values() if not df.is_empty()]
    leaves = (
        pl.concat(leaves_parts, how="vertical")
        if leaves_parts else pl.DataFrame(schema=_LEAVES_SCHEMA)
    )

    # --- Fragmentos: dag agregado por (centro, actividad, EC) × destinos ---
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
                pl.col("motivo").fill_null(_MOTIVO_DESTINO_VACÍO),
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
