"""Servicio del bloque Entradas: escaneo dinámico de ``data/entrada/``.

Expone:
- listado de subdirectorios y ficheros (.xlsx, .tree).
- vista paginada/filtrada de un .xlsx concreto.
- vista del árbol de un .tree.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl
from pydantic import BaseModel

from coana.util import Árbol, NodoÁrbol, read_excel
from coana.web.deps import DIR_ENTRADA, _mtime_ns
from coana.web.schemas.common import ColumnFormat, ColumnSpec, ListResponse
from coana.web.services.query import QueryParams, apply_query


# ----------------------------------------------------------------------
# Listado dinámico
# ----------------------------------------------------------------------

class FicheroEntrada(BaseModel):
    nombre: str          # nombre con extensión, ej. "centros.xlsx"
    stem: str            # sin extensión, ej. "centros"
    extension: str       # ".xlsx" o ".tree"
    ruta_relativa: str   # "presupuesto/centros.xlsx"
    tamaño_bytes: int


class GrupoEntradas(BaseModel):
    subdirectorio: str          # p.ej. "presupuesto"
    ficheros: list[FicheroEntrada]


class CatalogoEntradas(BaseModel):
    grupos: list[GrupoEntradas]


def listar_entradas() -> CatalogoEntradas:
    if not DIR_ENTRADA.is_dir():
        return CatalogoEntradas(grupos=[])

    grupos: list[GrupoEntradas] = []
    for sub in sorted(DIR_ENTRADA.iterdir()):
        if not sub.is_dir() or sub.name.startswith("_") or sub.name.startswith("."):
            continue
        ficheros: list[FicheroEntrada] = []
        for f in sorted(sub.iterdir()):
            if not f.is_file() or f.suffix not in (".xlsx", ".tree"):
                continue
            if f.name.startswith("~$"):  # ficheros temporales de Excel abiertos
                continue
            ficheros.append(FicheroEntrada(
                nombre=f.name,
                stem=f.stem,
                extension=f.suffix,
                ruta_relativa=str(f.relative_to(DIR_ENTRADA)),
                tamaño_bytes=f.stat().st_size,
            ))
        if ficheros:
            grupos.append(GrupoEntradas(subdirectorio=sub.name, ficheros=ficheros))

    return CatalogoEntradas(grupos=grupos)


# ----------------------------------------------------------------------
# Visor de .xlsx
# ----------------------------------------------------------------------

def _resolver_ruta(ruta_relativa: str) -> Path | None:
    """Resuelve una ruta relativa contra DIR_ENTRADA, evitando traversal."""
    p = (DIR_ENTRADA / ruta_relativa).resolve()
    try:
        p.relative_to(DIR_ENTRADA.resolve())
    except ValueError:
        return None
    if not p.is_file():
        return None
    return p


def _tipo_a_format(dtype: pl.DataType) -> ColumnFormat:
    if dtype == pl.Boolean:
        return "bool"
    if dtype.is_integer():
        # En el visor genérico de un .xlsx tratamos los enteros como
        # identificadores (sin separador de miles). Las cantidades
        # agregadas las definen los servicios concretos, no este
        # visor genérico.
        return "id"
    if dtype.is_float():
        return "float"
    if dtype in (pl.Date, pl.Datetime):
        return "date"
    return "text"


@lru_cache(maxsize=32)
def _excel_cached(path_str: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    return read_excel(Path(path_str))


def listar_xlsx(ruta_relativa: str, params: QueryParams) -> ListResponse | None:
    p = _resolver_ruta(ruta_relativa)
    if p is None or p.suffix != ".xlsx":
        return None
    df = _excel_cached(str(p), _mtime_ns(p))

    columnas = [
        ColumnSpec(name=c, label=c, format=_tipo_a_format(df.schema[c]))
        for c in df.columns
    ]
    df_q, total = apply_query(df, params)
    rows = []
    for r in df_q.to_dicts():
        rows.append({
            k: (v.isoformat() if hasattr(v, "isoformat") else v)
            for k, v in r.items()
        })
    return ListResponse(columns=columnas, rows=rows, total=total)


# ----------------------------------------------------------------------
# Visor de .tree
# ----------------------------------------------------------------------

class NodoTree(BaseModel):
    código: str
    descripción: str
    identificador: str
    hijos: list["NodoTree"] = []


def _serializar_nodo(nodo: NodoÁrbol) -> NodoTree:
    return NodoTree(
        código=nodo.código,
        descripción=nodo.descripción,
        identificador=nodo.identificador,
        hijos=[_serializar_nodo(h) for h in nodo.hijos],
    )


@lru_cache(maxsize=8)
def _arbol_cached(path_str: str, mtime_ns: int) -> Árbol:
    del mtime_ns
    return Árbol.from_file(Path(path_str))


def cargar_tree(ruta_relativa: str) -> NodoTree | None:
    p = _resolver_ruta(ruta_relativa)
    if p is None or p.suffix != ".tree":
        return None
    arbol = _arbol_cached(str(p), _mtime_ns(p))
    raíz = arbol.raíz
    return _serializar_nodo(raíz)
