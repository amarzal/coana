"""Servicio del bloque «Investigación».

Vistas:
- Listado de grupos de investigación activos en el año.
- Detalle de cada grupo: personas con rol (interlocutor / coordinador
  no interlocutor / miembro) ordenadas para destacar primero al
  interlocutor.
"""

from __future__ import annotations

from datetime import date
from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.web.deps import DIR_ENTRADA, _mtime_ns
from coana.web.schemas.common import ColumnSpec, ListResponse
from coana.web.services.query import QueryParams, apply_query

AÑO = 2025
PATH_GRUPOS = DIR_ENTRADA / "investigación" / "grupos investigación.xlsx"
PATH_INVESTIGADORES = DIR_ENTRADA / "investigación" / "investigadores en grupos.xlsx"
PATH_PERSONAS = DIR_ENTRADA / "nóminas" / "personas.xlsx"


# ----------------------------------------------------------------------
# Cargas con caché por mtime
# ----------------------------------------------------------------------

@lru_cache(maxsize=4)
def _grupos_cached(path: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path)
    if not p.exists():
        return pl.DataFrame()
    df = read_excel(p)
    return df.select(
        pl.col("grupo").cast(pl.Utf8).alias("id_grupo"),
        pl.col("nombre").alias("nombre_grupo"),
        pl.col("activo"),
    )


@lru_cache(maxsize=4)
def _personas_cached(path: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path)
    if not p.exists():
        return pl.DataFrame(schema={"per_id": pl.Int64, "persona": pl.Utf8})
    df = read_excel(p)
    return df.select(
        pl.col("per_id"),
        pl.concat_str(
            [pl.col("nombre"), pl.col("apellido1"), pl.col("apellido2")],
            separator=" ", ignore_nulls=True,
        ).alias("persona"),
    )


@lru_cache(maxsize=4)
def _investigadores_activos_cached(
    path: str, mtime_ns: int, año: int,
) -> pl.DataFrame:
    """Filas de investigadores con al menos un día de solape con el año."""
    del mtime_ns
    p = Path(path)
    if not p.exists():
        return pl.DataFrame()
    df = read_excel(p)
    inicio = date(año, 1, 1)
    fin = date(año, 12, 31)
    df = df.with_columns(
        pl.col("fecha_baja").fill_null(fin).alias("_fin"),
    ).with_columns(
        pl.max_horizontal(pl.col("fecha_alta"), pl.lit(inicio)).alias("_ini_sol"),
        pl.min_horizontal(pl.col("_fin"), pl.lit(fin)).alias("_fin_sol"),
    )
    df = df.with_columns(
        ((pl.col("_fin_sol") - pl.col("_ini_sol")).dt.total_days() + 1)
        .clip(lower_bound=0)
        .alias("días_activos")
    ).filter(pl.col("días_activos") > 0)
    return df.select(
        pl.col("per_id").cast(pl.Int64),
        pl.col("id_grupo").cast(pl.Utf8),
        pl.col("coordinador").cast(pl.Utf8),
        pl.col("interlocutor").cast(pl.Utf8),
        pl.col("principal").cast(pl.Utf8),
        pl.col("línea").cast(pl.Int64),
        pl.col("días_activos"),
    )


def _grupos() -> pl.DataFrame:
    return _grupos_cached(str(PATH_GRUPOS), _mtime_ns(PATH_GRUPOS))


def _personas() -> pl.DataFrame:
    return _personas_cached(str(PATH_PERSONAS), _mtime_ns(PATH_PERSONAS))


def _investigadores() -> pl.DataFrame:
    return _investigadores_activos_cached(
        str(PATH_INVESTIGADORES), _mtime_ns(PATH_INVESTIGADORES), AÑO,
    )


# ----------------------------------------------------------------------
# Master: lista de grupos
# ----------------------------------------------------------------------

_COLS_GRUPOS: list[ColumnSpec] = [
    ColumnSpec(name="id_grupo", label="id", format="id"),
    ColumnSpec(name="nombre_grupo", label="Nombre", format="text"),
    ColumnSpec(name="activo", label="Activo", format="text"),
    ColumnSpec(name="n_personas", label="Nº personas", format="int"),
    ColumnSpec(name="n_coordinadores", label="Coord.", format="int"),
    ColumnSpec(name="n_interlocutores", label="Interl.", format="int"),
]


def listar_grupos(p: QueryParams) -> ListResponse:
    grupos = _grupos()
    if grupos.is_empty():
        return ListResponse(columns=_COLS_GRUPOS, rows=[], total=0)

    # Excluir institutos de investigación (nombre que empieza por INSTITUT).
    # Los institutos no son grupos en sentido estricto y tienen su propio
    # tratamiento.
    grupos = grupos.filter(
        ~pl.col("nombre_grupo").str.to_uppercase().str.starts_with("INSTITUT")
    )

    inv = _investigadores()
    if not inv.is_empty():
        únicos = inv.unique(subset=["id_grupo", "per_id"])
        counts = únicos.group_by("id_grupo").agg(
            pl.len().alias("n_personas"),
            (pl.col("coordinador") == "S").sum().alias("n_coordinadores"),
            (pl.col("interlocutor") == "S").sum().alias("n_interlocutores"),
        )
        grupos = grupos.join(counts, on="id_grupo", how="left").with_columns(
            pl.col("n_personas").fill_null(0).cast(pl.Int64),
            pl.col("n_coordinadores").fill_null(0).cast(pl.Int64),
            pl.col("n_interlocutores").fill_null(0).cast(pl.Int64),
        )
    else:
        grupos = grupos.with_columns(
            pl.lit(0).alias("n_personas"),
            pl.lit(0).alias("n_coordinadores"),
            pl.lit(0).alias("n_interlocutores"),
        )

    # Excluir grupos sin personas activas en el año.
    grupos = grupos.filter(pl.col("n_personas") > 0)

    df = grupos.select([c.name for c in _COLS_GRUPOS])
    df, total, stats = apply_query(
        df, p, search_columns=["id_grupo", "nombre_grupo"],
    )
    return ListResponse(
        columns=_COLS_GRUPOS, rows=df.to_dicts(),
        total=total, column_stats=stats,
    )


# ----------------------------------------------------------------------
# Detail: personas de un grupo, ordenadas por rol
# ----------------------------------------------------------------------

_COLS_PERSONAS_GRUPO: list[ColumnSpec] = [
    ColumnSpec(name="rol", label="Rol", format="text"),
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="principal", label="Principal", format="text"),
    ColumnSpec(name="línea", label="Línea", format="int"),
    ColumnSpec(name="días_activos", label="Días activos", format="int"),
]


def listar_personas_de_grupo(id_grupo: str, p: QueryParams) -> ListResponse:
    inv = _investigadores()
    if inv.is_empty():
        return ListResponse(columns=_COLS_PERSONAS_GRUPO, rows=[], total=0)
    sub = inv.filter(pl.col("id_grupo") == id_grupo)
    if sub.is_empty():
        return ListResponse(columns=_COLS_PERSONAS_GRUPO, rows=[], total=0)
    # Una fila por (per_id, línea); enriquecer con nombre.
    sub = sub.join(_personas(), on="per_id", how="left")
    # Rol consolidado por persona: interlocutor > coordinador > miembro.
    # (Se calcula sobre TODAS las líneas del per_id en este grupo.)
    rol_persona = (
        sub.group_by("per_id").agg(
            (pl.col("interlocutor") == "S").any().alias("_es_int"),
            (pl.col("coordinador") == "S").any().alias("_es_coord"),
        )
        .with_columns(
            pl.when(pl.col("_es_int")).then(pl.lit("interlocutor"))
            .when(pl.col("_es_coord")).then(pl.lit("coordinador"))
            .otherwise(pl.lit("miembro"))
            .alias("rol")
        )
        .select("per_id", "rol")
    )
    # Una fila por persona (agregando líneas)
    agg = (
        sub.group_by("per_id", "persona").agg(
            pl.col("principal").max().alias("principal"),
            pl.col("línea").min().alias("línea"),
            pl.col("días_activos").max().alias("días_activos"),
        )
        .join(rol_persona, on="per_id", how="left")
    )
    # Orden: interlocutor (0), coordinador (1), miembro (2), luego por persona.
    orden = {"interlocutor": 0, "coordinador": 1, "miembro": 2}
    agg = agg.with_columns(
        pl.col("rol").replace_strict(orden, default=3, return_dtype=pl.Int64).alias("_ord")
    ).sort(["_ord", "persona"]).drop("_ord")

    df = agg.select([c.name for c in _COLS_PERSONAS_GRUPO if c.name in agg.columns])
    df, total, stats = apply_query(
        df, p, search_columns=["persona", "rol"],
    )
    return ListResponse(
        columns=_COLS_PERSONAS_GRUPO, rows=df.to_dicts(),
        total=total, column_stats=stats,
    )
