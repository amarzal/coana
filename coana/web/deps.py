"""Rutas, caché por mtime y dependencias compartidas del backend."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

def _base_data_dir() -> Path:
    """Directorio de datos.

    Por defecto ``./data`` relativo al CWD. Si la variable de entorno
    ``COANA_DATA_DIR`` está definida, se usa ese valor. Así el visor
    funciona sin importar desde qué subdirectorio se haya lanzado.
    """
    import os
    env = os.environ.get("COANA_DATA_DIR")
    if env:
        return Path(env)
    # Si no hay env, intenta encontrar la raíz del proyecto buscando
    # `data/` desde el CWD hacia arriba; cae al CWD si no la encuentra.
    cwd = Path.cwd()
    for d in (cwd, *cwd.parents):
        if (d / "data" / "entrada").is_dir():
            return d / "data"
    return cwd / "data"


DIR_BASE = _base_data_dir()
DIR_ENTRADA = DIR_BASE / "entrada"
DIR_FASE1 = DIR_BASE / "fase1"
DIR_AUX = DIR_FASE1 / "auxiliares"


def _mtime_ns(path: Path) -> int:
    """Devuelve el mtime en ns o 0 si el fichero no existe."""
    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return 0


@lru_cache(maxsize=128)
def _read_parquet_cached(path_str: str, mtime_ns: int) -> pl.DataFrame:
    """Caché interna; clave incluye mtime_ns para invalidar al cambiar el fichero."""
    del mtime_ns  # solo participa en la clave
    return pl.read_parquet(path_str)


def read_parquet(path: Path) -> pl.DataFrame:
    """Lee un parquet con caché invalidada por mtime.

    Parameters
    ----------
    path: ruta absoluta o relativa al parquet a leer.

    Returns
    -------
    pl.DataFrame
        DataFrame leído. Lanza ``FileNotFoundError`` si no existe.
    """
    if not path.exists():
        raise FileNotFoundError(path)
    return _read_parquet_cached(str(path), _mtime_ns(path))


def clear_cache() -> None:
    """Vacía las cachés en memoria (tras ejecutar la fase 1, por ejemplo)."""
    _read_parquet_cached.cache_clear()
