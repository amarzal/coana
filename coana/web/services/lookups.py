"""Enriquecimiento de fichas: para un valor de campo, busca info de referencia.

Al servir una ficha de registro, queremos resolver `per_id` → nombre
completo de la persona, `centro` → nombre del centro presupuestario,
`aplicación` → nombre y jerarquía, etc. Aquí centralizamos esos
lookups: cada uno carga la tabla de referencia con
``coana.util.read_excel`` (que ya gestiona caché parquet por mtime) y
devuelve un dict de campos enriquecidos.

Las funciones son tolerantes: si el fichero no existe o el valor no se
encuentra, devuelven dict vacío en lugar de fallar.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.web.deps import DIR_ENTRADA


def _safe_read(path: Path) -> pl.DataFrame | None:
    try:
        return read_excel(path)
    except FileNotFoundError:
        return None


@lru_cache(maxsize=64)
def _personas() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "nóminas" / "personas.xlsx")


@lru_cache(maxsize=64)
def _grados() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "docencia" / "grados.xlsx")


@lru_cache(maxsize=64)
def _masteres() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "docencia" / "másteres.xlsx")


@lru_cache(maxsize=64)
def _estudios() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "docencia" / "estudios.xlsx")


@lru_cache(maxsize=64)
def _subcentros() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "presupuesto" / "subcentros.xlsx")


@lru_cache(maxsize=64)
def _centros() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "presupuesto" / "centros.xlsx")


@lru_cache(maxsize=64)
def _proyectos() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "presupuesto" / "proyectos.xlsx")


@lru_cache(maxsize=64)
def _aplicaciones() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "presupuesto" / "aplicaciones de gasto.xlsx")


@lru_cache(maxsize=64)
def _programas() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "presupuesto" / "programas presupuestarios.xlsx")


def lookup_persona(per_id: int | str | None) -> dict[str, str]:
    if per_id is None:
        return {}
    df = _personas()
    if df is None:
        return {}
    try:
        per_id_int = int(per_id)
    except (TypeError, ValueError):
        return {}
    fila = df.filter(pl.col("per_id") == per_id_int)
    if fila.is_empty():
        return {}
    row = fila.row(0, named=True)
    nombre = " ".join(
        s for s in (row.get("nombre"), row.get("apellido1"), row.get("apellido2"))
        if s
    )
    return {"persona": nombre, "tipo": row.get("tipo") or ""}


def _lookup_simple(df: pl.DataFrame | None, key_col: str, key: str | None) -> dict[str, str]:
    if df is None or key is None:
        return {}
    fila = df.filter(pl.col(key_col).cast(pl.Utf8) == str(key))
    if fila.is_empty():
        return {}
    row = fila.row(0, named=True)
    return {k: (str(v) if v is not None else "") for k, v in row.items() if k != key_col}


def lookup_centro(centro: str | None) -> dict[str, str]:
    return _lookup_simple(_centros(), "centro", centro)


def lookup_proyecto(proyecto: str | None) -> dict[str, str]:
    return _lookup_simple(_proyectos(), "proyecto", proyecto)


def lookup_aplicacion(aplicacion: str | None) -> dict[str, str]:
    return _lookup_simple(_aplicaciones(), "aplicación", aplicacion)


def lookup_programa(programa: str | None) -> dict[str, str]:
    return _lookup_simple(_programas(), "programa", programa)


def _resolver_estudio(codigo: str) -> str:
    """Devuelve "<código> — <nombre>" si existe; si no, solo el código."""
    df = _estudios()
    if df is None:
        return codigo
    fila = df.filter(pl.col("estudio").cast(pl.Utf8) == codigo)
    if fila.is_empty():
        return codigo
    nombre = fila.row(0, named=True).get("nombre") or ""
    return f"{codigo} — {nombre}" if nombre else codigo


def lookup_grado(valor) -> dict[str, str]:
    if valor is None:
        return {}
    df = _grados()
    if df is None:
        return {}
    fila = df.filter(pl.col("grado").cast(pl.Utf8) == str(valor))
    if fila.is_empty():
        return {}
    row = fila.row(0, named=True)
    out: dict[str, str] = {}
    for k, v in row.items():
        if k == "grado":
            continue
        s = str(v) if v is not None else ""
        # Encadena el lookup: estudio dentro de grado se resuelve a su nombre.
        if k == "estudio" and s:
            s = _resolver_estudio(s)
        out[k] = s
    return out


def lookup_master(valor) -> dict[str, str]:
    if valor is None:
        return {}
    df = _masteres()
    if df is None:
        return {}
    fila = df.filter(pl.col("máster").cast(pl.Utf8) == str(valor))
    if fila.is_empty():
        return {}
    row = fila.row(0, named=True)
    out: dict[str, str] = {}
    for k, v in row.items():
        if k == "máster":
            continue
        s = str(v) if v is not None else ""
        if k == "estudio" and s:
            s = _resolver_estudio(s)
        out[k] = s
    return out


def lookup_estudio(valor) -> dict[str, str]:
    if valor is None:
        return {}
    df = _estudios()
    if df is None:
        return {}
    fila = df.filter(pl.col("estudio").cast(pl.Utf8) == str(valor))
    if fila.is_empty():
        return {}
    row = fila.row(0, named=True)
    return {k: (str(v) if v is not None else "") for k, v in row.items() if k != "estudio"}


def lookup_subcentro(valor) -> dict[str, str]:
    return _lookup_simple(_subcentros(), "subcentro", valor)


# ----------------------------------------------------------------------
# Mapeo nombre de columna → función de lookup
# ----------------------------------------------------------------------

_LOOKUP_BY_COL = {
    "per_id": lambda v: lookup_persona(v),
    "perid": lambda v: lookup_persona(v),
    "centro": lookup_centro,
    "subcentro": lookup_subcentro,
    "proyecto": lookup_proyecto,
    "aplicación": lookup_aplicacion,
    "programa": lookup_programa,
    "grado": lookup_grado,
    "máster": lookup_master,
    "estudio": lookup_estudio,
}


def enrich_row(row: dict) -> dict[str, dict[str, str]]:
    """Aplica los lookups conocidos a cada campo del row.

    Devuelve un dict ``{nombre_columna: {campo_extra: valor}}``. Solo
    incluye columnas que tienen lookup definido y para las que el valor
    aporta información (dict no vacío).
    """
    result: dict[str, dict[str, str]] = {}
    for col, fn in _LOOKUP_BY_COL.items():
        if col in row and row[col] not in (None, ""):
            data = fn(row[col])
            if data:
                result[col] = data
    return result


def clear_cache() -> None:
    """Vacía las cachés de tablas de referencia (tras editar entradas)."""
    for fn in (_personas, _centros, _proyectos, _aplicaciones, _programas,
               _grados, _masteres, _estudios, _subcentros):
        fn.cache_clear()
