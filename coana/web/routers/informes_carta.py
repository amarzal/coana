"""Informes a la carta: agrupación jerárquica de UC por CC/ACT/EC.

El usuario selecciona uno o más slugs de cada uno de los tres árboles
(centros de coste, actividades, elementos de coste), elige el orden
de los tres niveles (p. ej. CC → ACT → EC) y obtiene un listado
jerárquico con conteo de UC e importe acumulado por nodo.

Selección por subárbol: cada slug seleccionado incluye implícitamente
todos sus descendientes en el árbol correspondiente. Lista vacía =
«todos los slugs».

Configuraciones guardadas: `data/informes/carta_configs/{nombre}.yaml`.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import polars as pl
import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from coana.fase2.calculo import (
    cargar_árbol_actividades, cargar_árbol_centros_de_coste,
    cargar_árbol_elementos_de_coste, cargar_ucs, descendientes_inclusivo,
)
from coana.util import Árbol

router = APIRouter()


_RUTA_BASE = Path("data")
_DIR_CONFIGS = _RUTA_BASE / "informes" / "carta_configs"


class Filtro(BaseModel):
    centros_de_coste: list[str] = Field(default_factory=list)
    actividades: list[str] = Field(default_factory=list)
    elementos_de_coste: list[str] = Field(default_factory=list)
    # Permutación de las tres dimensiones; valores: "cc", "act", "ec".
    orden: list[str] = Field(default_factory=lambda: ["cc", "act", "ec"])


class Opciones(BaseModel):
    centros_de_coste: list[dict]
    actividades: list[dict]
    elementos_de_coste: list[dict]


def _opciones_de(árbol: Árbol) -> list[dict]:
    """Lista plana de nodos del árbol, ordenados por código."""
    out: list[dict] = []
    for ident, nodo in árbol._por_id.items():
        if not ident or nodo is árbol.raíz:
            continue
        out.append({
            "slug": ident,
            "código": nodo.código,
            "descripción": nodo.descripción,
        })
    out.sort(key=lambda x: x["código"])
    return out


@router.get("/opciones", response_model=Opciones)
def opciones() -> Opciones:
    """Devuelve los slugs disponibles en cada árbol para los selectores."""
    return Opciones(
        centros_de_coste=_opciones_de(cargar_árbol_centros_de_coste(_RUTA_BASE)),
        actividades=_opciones_de(cargar_árbol_actividades(_RUTA_BASE)),
        elementos_de_coste=_opciones_de(cargar_árbol_elementos_de_coste(_RUTA_BASE)),
    )


def _expandir(slugs: list[str], árbol: Árbol) -> set[str]:
    """Conjunto de slugs efectivos: para cada `s` en `slugs`, los
    descendientes inclusivos en `árbol`. Lista vacía = no filtrar."""
    if not slugs:
        return set()
    s: set[str] = set()
    for slug in slugs:
        try:
            s |= set(descendientes_inclusivo(árbol, slug))
        except KeyError:
            pass
    return s


class NodoJerarquía(BaseModel):
    nivel: int
    eje: str       # "cc" | "act" | "ec"
    slug: str
    código: str
    descripción: str
    n_ucs: int
    importe: float
    hijos: list["NodoJerarquía"] = Field(default_factory=list)


NodoJerarquía.model_rebuild()


class Resultado(BaseModel):
    orden: list[str]
    n_ucs: int
    importe: float
    raíces: list[NodoJerarquía]


_EJES_COLS = {
    "cc": "centro_de_coste",
    "act": "actividad",
    "ec": "elemento_de_coste",
}
_EJES_ÁRBOL_FN = {
    "cc": cargar_árbol_centros_de_coste,
    "act": cargar_árbol_actividades,
    "ec": cargar_árbol_elementos_de_coste,
}


@router.post("/consulta", response_model=Resultado)
def consulta(filtro: Filtro) -> Resultado:
    """Aplica los filtros y devuelve el desglose jerárquico solicitado."""
    if sorted(filtro.orden) != ["act", "cc", "ec"]:
        raise HTTPException(
            status_code=400,
            detail=f"`orden` debe ser permutación de ['cc','act','ec']: {filtro.orden!r}",
        )
    ucs = cargar_ucs(_RUTA_BASE)

    # Pre-cargar los árboles que necesitemos.
    árboles: dict[str, Árbol] = {
        "cc": cargar_árbol_centros_de_coste(_RUTA_BASE),
        "act": cargar_árbol_actividades(_RUTA_BASE),
        "ec": cargar_árbol_elementos_de_coste(_RUTA_BASE),
    }
    seleccionados: dict[str, set[str]] = {
        "cc": _expandir(filtro.centros_de_coste, árboles["cc"]),
        "act": _expandir(filtro.actividades, árboles["act"]),
        "ec": _expandir(filtro.elementos_de_coste, árboles["ec"]),
    }
    raíces_eje: dict[str, list[str]] = {
        "cc": list(filtro.centros_de_coste),
        "act": list(filtro.actividades),
        "ec": list(filtro.elementos_de_coste),
    }

    # Aplicar filtros «slug ∈ subárbol de algún seleccionado».
    sub = ucs
    for eje in ("cc", "act", "ec"):
        sel = seleccionados[eje]
        if sel:
            sub = sub.filter(pl.col(_EJES_COLS[eje]).is_in(list(sel)))

    # Si no hay UC tras el filtro, devolvemos respuesta vacía.
    if sub.is_empty():
        return Resultado(orden=filtro.orden, n_ucs=0, importe=0.0, raíces=[])

    total_n = int(sub.height)
    total_imp = float(sub["importe"].sum() or 0.0)

    # Construcción del árbol jerárquico. Para cada nivel, agrupamos por
    # los slugs raíz seleccionados (o por la raíz del propio árbol si
    # no se filtró nada). Cada UC se atribuye al primer slug raíz cuyo
    # subárbol la contenga.
    def _meta(eje: str, slug: str) -> tuple[str, str]:
        try:
            nodo = árboles[eje]._nodo(slug)
            return (nodo.código, nodo.descripción)
        except KeyError:
            return ("?", slug)

    def _slugs_grupo(eje: str, df: pl.DataFrame) -> list[tuple[str, set[str]]]:
        """Slugs que sirven de raíz para el agrupamiento de este eje.
        Si el usuario seleccionó slugs en este eje, se usan tal cual
        (cada uno con su subárbol). Si no, se enumeran los slugs
        distintos presentes en `df`."""
        if raíces_eje[eje]:
            return [
                (s, set(descendientes_inclusivo(árboles[eje], s)))
                for s in raíces_eje[eje]
            ]
        col = _EJES_COLS[eje]
        slugs = sorted(
            s for s in df[col].drop_nulls().unique().to_list() if s
        )
        return [(s, {s}) for s in slugs]

    def _construir(
        df: pl.DataFrame, nivel: int, ejes_restantes: list[str],
    ) -> list[NodoJerarquía]:
        if not ejes_restantes:
            return []
        eje = ejes_restantes[0]
        col = _EJES_COLS[eje]
        nodos_out: list[NodoJerarquía] = []
        for slug_raíz, descs in _slugs_grupo(eje, df):
            grupo = df.filter(pl.col(col).is_in(list(descs)))
            if grupo.is_empty():
                continue
            cod, desc = _meta(eje, slug_raíz)
            hijos = _construir(grupo, nivel + 1, ejes_restantes[1:])
            nodos_out.append(NodoJerarquía(
                nivel=nivel,
                eje=eje,
                slug=slug_raíz,
                código=cod,
                descripción=desc,
                n_ucs=int(grupo.height),
                importe=round(float(grupo["importe"].sum() or 0.0), 2),
                hijos=hijos,
            ))
        # Ordenar por código.
        nodos_out.sort(key=lambda n: n.código)
        return nodos_out

    raíces = _construir(sub, nivel=1, ejes_restantes=list(filtro.orden))
    return Resultado(
        orden=filtro.orden,
        n_ucs=total_n,
        importe=round(total_imp, 2),
        raíces=raíces,
    )


class PeticiónUcs(BaseModel):
    centros_de_coste: list[str] = Field(default_factory=list)
    actividades: list[str] = Field(default_factory=list)
    elementos_de_coste: list[str] = Field(default_factory=list)
    limit: int = 5000


@router.post("/uc")
def uc_de_combinación(p: PeticiónUcs) -> dict[str, Any]:
    """Lista de UC que cuelgan de los slugs indicados (expansión por
    subárbol). Devuelve hasta `limit` filas."""
    ucs = cargar_ucs(_RUTA_BASE)
    sels = {
        "cc": _expandir(p.centros_de_coste, cargar_árbol_centros_de_coste(_RUTA_BASE)),
        "act": _expandir(p.actividades, cargar_árbol_actividades(_RUTA_BASE)),
        "ec": _expandir(p.elementos_de_coste, cargar_árbol_elementos_de_coste(_RUTA_BASE)),
    }
    sub = ucs
    for eje, col in _EJES_COLS.items():
        if sels[eje]:
            sub = sub.filter(pl.col(col).is_in(list(sels[eje])))
    n_total = int(sub.height)
    if "importe" in sub.columns:
        sub = sub.sort(pl.col("importe").abs(), descending=True)
    filas = sub.head(p.limit).to_dicts()
    return {
        "n_total": n_total,
        "n_devueltas": len(filas),
        "filas": filas,
    }


# ----------------------------------------------------------------------
# Configuraciones guardadas: CRUD.
# ----------------------------------------------------------------------

_NOMBRE_OK = re.compile(r"^[\wáéíóúñÁÉÍÓÚÑ.\-\s]{1,80}$")


def _path_config(nombre: str) -> Path:
    if not _NOMBRE_OK.match(nombre):
        raise HTTPException(
            status_code=400,
            detail="Nombre inválido: usa letras, dígitos, espacios, '.' o '-'.",
        )
    return _DIR_CONFIGS / f"{nombre}.yaml"


class ConfigMeta(BaseModel):
    nombre: str


@router.get("/configs", response_model=list[ConfigMeta])
def listar_configs() -> list[ConfigMeta]:
    if not _DIR_CONFIGS.exists():
        return []
    out = []
    for p in sorted(_DIR_CONFIGS.glob("*.yaml")):
        out.append(ConfigMeta(nombre=p.stem))
    return out


@router.get("/configs/{nombre}", response_model=Filtro)
def cargar_config(nombre: str) -> Filtro:
    p = _path_config(nombre)
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"Config {nombre!r} no existe")
    with p.open(encoding="utf-8") as f:
        datos = yaml.safe_load(f) or {}
    return Filtro(**datos)


@router.put("/configs/{nombre}", response_model=ConfigMeta)
def guardar_config(nombre: str, filtro: Filtro) -> ConfigMeta:
    p = _path_config(nombre)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        yaml.safe_dump(filtro.model_dump(), f, allow_unicode=True, sort_keys=False)
    return ConfigMeta(nombre=nombre)


@router.delete("/configs/{nombre}")
def borrar_config(nombre: str) -> dict[str, str]:
    p = _path_config(nombre)
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"Config {nombre!r} no existe")
    p.unlink()
    return {"borrada": nombre}
