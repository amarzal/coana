"""Cálculo común a todos los informes.

Para cada nodo X de un árbol calculamos:

- ``A(X)``: suma de importes de UC con coordenada exactamente igual a X.
- ``B(X)``: suma de A(Y) sobre todos los descendientes Y de X.
- ``A+B(X)``: suma directa + heredada-abajo = "coste propio del subárbol".

Esta función es suficiente para los informes que listan nodos de un
único árbol (no requieren el reparto C entre coordenadas distintas).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from coana.util import Árbol


def descendientes_inclusivo(árbol: Árbol, identificador: str) -> list[str]:
    """Lista de identificadores que cuelgan del nodo dado, incluyéndolo."""
    nodo = árbol._nodo(identificador)
    salida: list[str] = []

    def _rec(n) -> None:
        if n.identificador:
            salida.append(n.identificador)
        for h in n.hijos:
            _rec(h)

    _rec(nodo)
    return salida


def importes_por_nodo(
    ucs: pl.DataFrame,
    árbol: Árbol,
    columna_coordenada: str,
) -> dict[str, float]:
    """Devuelve, para cada nodo del árbol, su ``A(X)+B(X)``.

    ``columna_coordenada`` es el nombre de la columna en ``ucs`` que
    contiene el identificador del nodo (p. ej. ``elemento_de_coste``,
    ``centro_de_coste`` o ``actividad``).
    """
    # Suma directa por identificador (A).
    directos: dict[str, float] = {}
    if not ucs.is_empty():
        agg = (
            ucs.group_by(columna_coordenada)
            .agg(pl.col("importe").sum().alias("imp"))
        )
        for r in agg.iter_rows(named=True):
            ident = r[columna_coordenada]
            if ident is not None:
                directos[str(ident)] = float(r["imp"] or 0.0)

    # Agregar a cada nodo la suma de su subárbol (A + B).
    salida: dict[str, float] = {}
    for ident in árbol._por_id:
        if not ident:
            continue
        descs = descendientes_inclusivo(árbol, ident)
        salida[ident] = sum(directos.get(d, 0.0) for d in descs)
    return salida


def total_general(ucs: pl.DataFrame) -> float:
    """Suma total de importes (todas las UC)."""
    if ucs.is_empty():
        return 0.0
    return float(ucs["importe"].sum() or 0.0)


def cargar_ucs(ruta_base: Path) -> pl.DataFrame:
    """Carga las UC generadas por la Fase 1."""
    p = ruta_base / "fase1" / "unidades de coste.xlsx"
    if not p.exists():
        raise FileNotFoundError(f"No existe {p}. Ejecuta antes la Fase 1.")
    return pl.read_excel(p)


def _cargar_árbol(ruta_base: Path, nombre: str) -> Árbol:
    """Carga el árbol *enriquecido* generado por Fase 1 si existe; en
    su ausencia cae al árbol de entrada. La Fase 1 inserta nodos
    (grupos de investigación, edificios, etc.) que solo aparecen en
    los árboles finales y son necesarios para que las UC encajen
    bajo nodos conocidos del cuadro."""
    final = ruta_base / "fase1" / nombre
    if final.exists():
        return Árbol.from_file(final)
    return Árbol.from_file(ruta_base / "entrada" / "estructuras" / nombre)


def cargar_árbol_elementos_de_coste(ruta_base: Path) -> Árbol:
    return _cargar_árbol(ruta_base, "elementos de coste.tree")


def cargar_árbol_centros_de_coste(ruta_base: Path) -> Árbol:
    return _cargar_árbol(ruta_base, "centros de coste.tree")


def cargar_árbol_actividades(ruta_base: Path) -> Árbol:
    return _cargar_árbol(ruta_base, "actividades.tree")
