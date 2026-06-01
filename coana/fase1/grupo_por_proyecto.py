"""Resolver proyecto presupuestario → grupo de investigación del IP.

Sirve para imputar el gasto de presupuesto de los proyectos de
investigación al centro de coste del **grupo de investigación** del
investigador principal (IP), en lugar del departamento. La especificación
pedía una tabla propia proyecto→IP→grupo; mientras no exista, se deriva
encadenando los ficheros de investigación:

    proyecto → contrato      (proyectos en contratos investigación.xlsx)
            → IP, principal   (investigadores en contratos.xlsx, `principal`='S')
            → grupo del IP     (investigadores en grupos.xlsx, activo en el año
                                y donde el IP es `principal`/`coordinador`,
                                validado contra grupos a institutos.xlsx)

Desempate: el conjunto de grupos «principal/coordinador» de los IP del
proyecto debe ser exactamente **uno**; si es 0 ó >1, no se resuelve (el
gasto se queda en el departamento).

Override: ``data/entrada/investigación/proyectos a grupos.xlsx`` (columnas
``proyecto``, ``id_grupo``) sustituye lo derivado — fila con ``id_grupo``
vacío elimina la asignación.
"""

from __future__ import annotations

from datetime import date
from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.util import read_excel


def _grupos_válidos(dir_inv: Path) -> set[str]:
    """id_grupo que son grupos de investigación de verdad (no institutos)."""
    mapeo = dir_inv / "grupos a institutos.xlsx"
    if not mapeo.exists():
        return set()
    return set(
        read_excel(mapeo)
        .select(pl.col("id_grupo").cast(pl.Utf8))
        .get_column("id_grupo")
        .to_list()
    )


def _aplicar_override(
    dir_inv: Path, derivado: dict[str, str], válidos: set[str],
) -> dict[str, str]:
    ov_path = dir_inv / "proyectos a grupos.xlsx"
    if not ov_path.exists():
        return derivado
    ov = read_excel(ov_path)
    if ov.is_empty() or "proyecto" not in ov.columns or "id_grupo" not in ov.columns:
        return derivado
    out = dict(derivado)
    filas = ov.select(
        pl.col("proyecto").cast(pl.Utf8),
        pl.col("id_grupo").cast(pl.Utf8),
    ).iter_rows(named=True)
    for r in filas:
        proy, gid = r["proyecto"], r["id_grupo"]
        if proy is None:
            continue
        if gid is None or gid == "":
            out.pop(proy, None)            # override que elimina la asignación
        elif not válidos or gid in válidos:
            out[proy] = gid                # override que fija/corrige el grupo
    return out


@lru_cache(maxsize=8)
def grupo_por_proyecto(
    ruta_base: Path = Path("data"), año: int = 2025,
) -> dict[str, str]:
    """``{proyecto: id_grupo}`` para los proyectos de investigación cuyo IP
    tiene un grupo de investigación inequívoco. Solo proyectos resueltos
    (los demás caen al departamento por la cascada del clasificador)."""
    dir_inv = Path(ruta_base) / "entrada" / "investigación"
    pc_path = dir_inv / "proyectos en contratos investigación.xlsx"
    ic_path = dir_inv / "investigadores en contratos.xlsx"
    ig_path = dir_inv / "investigadores en grupos.xlsx"
    válidos = _grupos_válidos(dir_inv)
    if not (pc_path.exists() and ic_path.exists() and ig_path.exists()):
        return _aplicar_override(dir_inv, {}, válidos)

    inicio, fin = date(año, 1, 1), date(año, 12, 31)

    # proyecto ↔ contrato
    pc = read_excel(pc_path).select(
        pl.col("proyecto").cast(pl.Utf8), pl.col("contrato"),
    )
    # contrato → IP (principal)
    ic = (
        read_excel(ic_path)
        .filter(pl.col("principal").cast(pl.Utf8) == "S")
        .select("contrato", pl.col("per_id").cast(pl.Int64))
    )
    proyecto_ip = (
        pc.join(ic, on="contrato", how="inner")
        .select("proyecto", "per_id").unique()
    )
    if proyecto_ip.is_empty():
        return _aplicar_override(dir_inv, {}, válidos)

    # per_id → grupo donde es principal/coordinador, activo en el año, válido
    g = read_excel(ig_path)
    g = g.with_columns(pl.col("fecha_baja").fill_null(fin).alias("_fin"))
    g = g.filter(
        (pl.col("fecha_alta") <= pl.lit(fin)) & (pl.col("_fin") >= pl.lit(inicio))
    )
    if válidos:
        g = g.filter(pl.col("id_grupo").cast(pl.Utf8).is_in(list(válidos)))
    g = g.filter(
        (pl.col("principal").cast(pl.Utf8) == "S")
        | (pl.col("coordinador").cast(pl.Utf8) == "S")
    )
    g = g.select(
        pl.col("per_id").cast(pl.Int64), pl.col("id_grupo").cast(pl.Utf8),
    ).unique()

    # proyecto → conjunto de grupos; se resuelve si es exactamente uno.
    pg = proyecto_ip.join(g, on="per_id", how="inner").select(
        "proyecto", "id_grupo",
    ).unique()
    agg = pg.group_by("proyecto").agg(
        pl.col("id_grupo").alias("grupos"),
    )
    derivado: dict[str, str] = {}
    for r in agg.iter_rows(named=True):
        grupos = r["grupos"]
        if grupos is not None and len(grupos) == 1:
            derivado[str(r["proyecto"])] = str(grupos[0])

    return _aplicar_override(dir_inv, derivado, válidos)
