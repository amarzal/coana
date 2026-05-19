"""Acceso a las constantes anuales y de política definidas en
``data/configuración.xlsx``.

El Excel tiene tres columnas: ``nombre``, ``valor``, ``descripción``.
Cada constante se identifica por su ``nombre`` y se accede vía las
funciones :func:`cfg_int`, :func:`cfg_float`, :func:`cfg_str` o
:func:`cfg_set`, que castean al tipo deseado. El fichero se cachea en
memoria; modificarlo requiere reiniciar el proceso.

El objetivo de centralizar estas constantes aquí es que, al cambiar de
ejercicio analizado, baste con actualizar este Excel (sin tocar
código) para que los nuevos tipos de cotización, base máxima, jornada
anual y tablas de categorías afecten a todo el pipeline.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

import polars as pl


_RUTA_POR_DEFECTO = Path(__file__).resolve().parents[2] / "data" / "configuración.xlsx"


@cache
def _config_dict(ruta: Path = _RUTA_POR_DEFECTO) -> dict[str, str]:
    """Lee el Excel y devuelve ``{nombre: valor}`` con todo como str."""
    from coana.util.excel_cache import read_excel
    df = read_excel(ruta)
    if df.is_empty():
        return {}
    df = df.with_columns(pl.col("valor").cast(pl.Utf8))
    return {
        str(row["nombre"]).strip(): row["valor"].strip()
        for row in df.iter_rows(named=True)
        if row.get("nombre") is not None and row.get("valor") is not None
    }


def _get(nombre: str) -> str:
    d = _config_dict()
    if nombre not in d:
        raise KeyError(
            f"Constante {nombre!r} no encontrada en data/configuración.xlsx. "
            f"Disponibles: {sorted(d)}"
        )
    return d[nombre]


def cfg_int(nombre: str) -> int:
    """Valor entero de la constante ``nombre``."""
    return int(_get(nombre))


def cfg_float(nombre: str) -> float:
    """Valor en coma flotante de la constante ``nombre``."""
    return float(_get(nombre))


def cfg_str(nombre: str) -> str:
    """Valor textual de la constante ``nombre``."""
    return _get(nombre)


def cfg_set(nombre: str) -> set[str]:
    """Lista separada por comas → ``set`` de cadenas (sin espacios)."""
    raw = _get(nombre).replace(" ", "")
    return set(raw.split(",")) if raw else set()


def cfg_tuple(nombre: str) -> tuple[str, ...]:
    """Lista separada por comas → ``tuple`` ordenado (sin espacios)."""
    raw = _get(nombre).replace(" ", "")
    return tuple(raw.split(",")) if raw else ()


__all__ = ["cfg_int", "cfg_float", "cfg_str", "cfg_set", "cfg_tuple"]
