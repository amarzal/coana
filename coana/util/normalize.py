"""Normalización de cadenas para búsqueda insensible a tildes y mayúsculas.

Reglas comunes al visor Streamlit y al backend FastAPI: ambos deben usar
estas funciones para que el filtrado se comporte igual en los dos.
"""

from __future__ import annotations

import unicodedata

import polars as pl


def sin_tildes(s: str) -> str:
    """Devuelve ``s`` en minúsculas, sin marcas diacríticas (NFD + Mn).

    Equivalente a:

    >>> sin_tildes("José Mª Núñez")
    'jose ma nunez'
    """
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    ).lower()


def col_sin_tildes(col: str) -> pl.Expr:
    """Expresión Polars: columna a minúsculas y sin marcas diacríticas.

    Casa con :func:`sin_tildes` aplicada a una cadena de Python para que
    un filtro `q` normalizado encuentre filas cuyas columnas contengan
    `q` con cualquier combinación de tildes y mayúsculas.
    """
    return (
        pl.col(col).cast(pl.Utf8)
        .str.to_lowercase()
        .str.normalize("NFD")
        .str.replace_all(r"[\u0300-\u036f]", "")
    )
