"""Servicio del bloque Investigación.

Subvistas:
- Resumen: KPIs de horas totales
- Personas: lista con horas totales por tipo (grupos, tesis, proyectos)
- Detalle persona: registros individuales de origen
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.web.deps import DIR_AUX, _mtime_ns, read_parquet
from coana.web.schemas.common import (
    ColumnSpec,
    FieldValue,
    Kpi,
    KpiPanel,
    ListResponse,
    RecordResponse,
    RecordSection,
)
from coana.web.services.query import QueryParams, apply_query

DIR_INV = DIR_AUX / "investigación"
PATH_RESUMEN = DIR_INV / "resumen_investigacion.parquet"
PATH_DETALLE = DIR_INV / "detalle_investigacion.parquet"


def _safe_read(path: Path) -> pl.DataFrame | None:
    try:
        return read_parquet(path)
    except FileNotFoundError:
        return None


@lru_cache(maxsize=2)
def _resumen_cached(path_str: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path_str)
    if not p.exists():
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "horas_totales": pl.Float64,
            "semanas_grupos": pl.Int32,
            "semanas_tesis": pl.Int32,
            "semanas_proyectos": pl.Int32,
            "horas_grupos": pl.Float64,
            "horas_tesis": pl.Float64,
            "horas_proyectos": pl.Float64,
        })
    return read_parquet(p)


def _resumen() -> pl.DataFrame:
    return _resumen_cached(str(PATH_RESUMEN), _mtime_ns(PATH_RESUMEN))


@lru_cache(maxsize=2)
def _detalle_cached(path_str: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path_str)
    if not p.exists():
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "tipo": pl.Utf8,
            "identificador": pl.Utf8,
            "descripción": pl.Utf8,
            "semanas": pl.Int32,
            "horas": pl.Float64,
            "origen": pl.Utf8,
        })
    return read_parquet(p)


def _detalle() -> pl.DataFrame:
    return _detalle_cached(str(PATH_DETALLE), _mtime_ns(PATH_DETALLE))


def _enriquecer_per_id(df: pl.DataFrame) -> pl.DataFrame:
    """Añade el nombre de la persona a partir de per_id."""
    if "per_id" not in df.columns:
        return df
    
    # Importar desde personal para reusar la función de personas
    from coana.web.services.personal import _personas
    personas = _personas()
    if personas.is_empty():
        return df
    
    return df.join(personas, on="per_id", how="left")


def _serialize(rows: list[dict]) -> list[dict]:
    return [
        {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()}
        for r in rows
    ]


# ----------------------------------------------------------------------
# Resumen
# ----------------------------------------------------------------------

def resumen() -> KpiPanel:
    """KPIs generales de dedicación a investigación."""
    df = _resumen()
    
    if df.is_empty():
        return KpiPanel(kpis=[
            Kpi(label="Personas con dedicación", value=0, format="int"),
            Kpi(label="Total horas", value=0.0, format="float"),
        ])
    
    n_personas = df.height
    total_horas = float(df["horas_totales"].sum() or 0) if "horas_totales" in df.columns else 0.0
    promedio = total_horas / n_personas if n_personas > 0 else 0.0
    
    horas_grupos = float(df["horas_grupos"].sum() or 0) if "horas_grupos" in df.columns else 0.0
    horas_tesis = float(df["horas_tesis"].sum() or 0) if "horas_tesis" in df.columns else 0.0
    horas_proyectos = float(df["horas_proyectos"].sum() or 0) if "horas_proyectos" in df.columns else 0.0
    
    return KpiPanel(kpis=[
        Kpi(label="Personas con dedicación", value=n_personas, format="int"),
        Kpi(label="Total horas", value=total_horas, format="float"),
        Kpi(label="Promedio horas/persona", value=promedio, format="float"),
        Kpi(label="Horas coordinación grupos", value=horas_grupos, format="float"),
        Kpi(label="Horas tesis", value=horas_tesis, format="float"),
        Kpi(label="Horas proyectos", value=horas_proyectos, format="float"),
    ])


# ----------------------------------------------------------------------
# Lista de personas con dedicación
# ----------------------------------------------------------------------

_COLS_PERSONAS: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="horas_totales", label="Horas totales", format="float"),
    ColumnSpec(name="horas_grupos", label="H. Grupos", format="float"),
    ColumnSpec(name="horas_tesis", label="H. Tesis", format="float"),
    ColumnSpec(name="horas_proyectos", label="H. Proyectos", format="float"),
    ColumnSpec(name="semanas_grupos", label="Sem. Grupos", format="int"),
    ColumnSpec(name="semanas_tesis", label="Sem. Tesis", format="int"),
    ColumnSpec(name="semanas_proyectos", label="Sem. Proyectos", format="int"),
]


def listar_personas_investigacion(params: QueryParams) -> ListResponse:
    """Lista todas las personas con dedicación a investigación."""
    df = _resumen()
    
    if df.is_empty():
        return ListResponse(columns=_COLS_PERSONAS, rows=[], total=0)
    
    # Enriquecer con nombre
    df = _enriquecer_per_id(df)
    
    # Seleccionar columnas
    cols = [c.name for c in _COLS_PERSONAS if c.name in df.columns]
    df = df.select(cols)
    
    # Aplicar query
    df, total, stats = apply_query(df, params, search_columns=["persona"])
    
    return ListResponse(
        columns=_COLS_PERSONAS,
        rows=_serialize(df.to_dicts()),
        total=total,
        column_stats=stats,
    )


# ----------------------------------------------------------------------
# Detalle de una persona
# ----------------------------------------------------------------------

_COLS_DETALLE: list[ColumnSpec] = [
    ColumnSpec(name="tipo", label="Tipo", format="text"),
    ColumnSpec(name="identificador", label="Identificador", format="text"),
    ColumnSpec(name="descripción", label="Descripción", format="text"),
    ColumnSpec(name="semanas", label="Semanas", format="int"),
    ColumnSpec(name="horas", label="Horas", format="float"),
    ColumnSpec(name="origen", label="Origen", format="text"),
]


def obtener_persona_investigacion(per_id: int) -> RecordResponse | None:
    """Devuelve datos agregados de una persona."""
    df = _resumen()
    
    if df.is_empty() or "per_id" not in df.columns:
        return None
    
    fila = df.filter(pl.col("per_id") == per_id)
    if fila.is_empty():
        return None
    
    # Enriquecer con nombre
    fila = _enriquecer_per_id(fila)
    row = fila.row(0, named=True)
    
    main: list[FieldValue] = [
        FieldValue(name="per_id", label="per_id", value=per_id, format="id"),
    ]
    
    if "persona" in row:
        main.append(FieldValue(name="persona", label="Persona", value=row["persona"], format="text"))
    
    main.extend([
        FieldValue(name="horas_totales", label="Horas totales", value=row.get("horas_totales"), format="float"),
        FieldValue(name="horas_grupos", label="Horas grupos", value=row.get("horas_grupos"), format="float"),
        FieldValue(name="horas_tesis", label="Horas tesis", value=row.get("horas_tesis"), format="float"),
        FieldValue(name="horas_proyectos", label="Horas proyectos", value=row.get("horas_proyectos"), format="float"),
    ])
    
    sections: list[RecordSection] = []
    
    # Sección con semanas
    semanas_fields = [
        FieldValue(name="semanas_grupos", label="Semanas grupos", value=row.get("semanas_grupos"), format="int"),
        FieldValue(name="semanas_tesis", label="Semanas tesis", value=row.get("semanas_tesis"), format="int"),
        FieldValue(name="semanas_proyectos", label="Semanas proyectos", value=row.get("semanas_proyectos"), format="int"),
    ]
    sections.append(RecordSection(label="Semanas de dedicación", fields=semanas_fields))
    
    return RecordResponse(main=main, sections=sections)


def listar_detalle_persona(per_id: int, params: QueryParams) -> ListResponse:
    """Lista los registros de detalle de una persona específica."""
    df = _detalle()
    
    if df.is_empty() or "per_id" not in df.columns:
        return ListResponse(columns=_COLS_DETALLE, rows=[], total=0)
    
    # Filtrar por persona
    df = df.filter(pl.col("per_id") == per_id)
    
    # Seleccionar columnas
    cols = [c.name for c in _COLS_DETALLE if c.name in df.columns]
    df = df.select(cols)
    
    # Aplicar query
    df, total, stats = apply_query(
        df, params,
        search_columns=["tipo", "identificador", "descripción", "origen"],
    )
    
    return ListResponse(
        columns=_COLS_DETALLE,
        rows=_serialize(df.to_dicts()),
        total=total,
        column_stats=stats,
    )
