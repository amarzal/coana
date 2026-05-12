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
PATH_UC = DIR_INV / "uc_investigacion.parquet"


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
    ColumnSpec(name="fecha_inicio", label="Fecha inicio", format="date"),
    ColumnSpec(name="fecha_fin", label="Fecha fin", format="date"),
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


# ----------------------------------------------------------------------
# UC investigación (distribución porcentual por persona/actividad)
# ----------------------------------------------------------------------

@lru_cache(maxsize=2)
def _uc_cached(path_str: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path_str)
    if not p.exists():
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "actividad": pl.Utf8,
            "horas": pl.Float64,
            "horas_totales": pl.Float64,
            "porcentaje": pl.Float64,
        })
    return read_parquet(p)


def _uc() -> pl.DataFrame:
    return _uc_cached(str(PATH_UC), _mtime_ns(PATH_UC))


_COLS_UC: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="actividad", label="Actividad", format="text"),
    ColumnSpec(name="horas", label="Horas", format="float"),
    ColumnSpec(name="horas_totales", label="Horas totales persona", format="float"),
    ColumnSpec(name="porcentaje", label="% sobre total persona", format="float"),
]


def listar_uc_investigacion(params: QueryParams) -> ListResponse:
    """Lista los pares (per_id, actividad) con horas y porcentaje
    sobre el total de la persona."""
    df = _uc()
    if df.is_empty():
        return ListResponse(columns=_COLS_UC, rows=[], total=0)

    df = _enriquecer_per_id(df)
    cols = [c.name for c in _COLS_UC if c.name in df.columns]
    df = df.select(cols)
    df, total, stats = apply_query(
        df, params, search_columns=["persona", "actividad"],
    )
    return ListResponse(
        columns=_COLS_UC,
        rows=_serialize(df.to_dicts()),
        total=total,
        column_stats=stats,
    )


_COLS_UC_DETALLE: list[ColumnSpec] = [
    ColumnSpec(name="tipo", label="Tipo", format="text"),
    ColumnSpec(name="identificador", label="Identificador", format="text"),
    ColumnSpec(name="descripción", label="Descripción", format="text"),
    ColumnSpec(name="semanas", label="Semanas", format="int"),
    ColumnSpec(name="horas", label="Horas", format="float"),
    ColumnSpec(name="origen", label="Origen", format="text"),
]


def listar_detalle_uc_actividad(
    per_id: int, actividad: str, params: QueryParams,
) -> ListResponse:
    """Lista los registros de detalle que contribuyen a una actividad
    concreta de una persona — para el modal de drill-down de UC.

    Reproduce la lógica de resolución de actividad usada por
    :func:`coana.fase1.investigacion.generar_distribución_investigación`
    (tesis y grupos vía etiqueta fija; proyectos vía join con
    `proyectos.xlsx`). De ese modo el filtrado por actividad coincide
    exactamente con lo que se ha imputado en `uc_investigacion.parquet`.
    """
    df = _detalle()
    if df.is_empty() or "per_id" not in df.columns:
        return ListResponse(columns=_COLS_UC_DETALLE, rows=[], total=0)

    df = df.filter(pl.col("per_id") == per_id)
    if df.is_empty():
        return ListResponse(columns=_COLS_UC_DETALLE, rows=[], total=0)

    # Reutilizamos el resolver del módulo fase1 para que las
    # actividades coincidan al 100% con uc_investigacion.parquet.
    from coana.fase1.investigacion import (
        _actividades_por_proyecto,
        _doctorados_por_alumno,
        _PREFIJO_FALLBACK_INV,
    )
    from coana.web.deps import DIR_ENTRADA

    ruta_base = DIR_ENTRADA.parent
    actividades_proy = _actividades_por_proyecto(ruta_base)
    doctorados = _doctorados_por_alumno(ruta_base)

    es_proy = pl.col("tipo") == "proyectos"
    es_tesis = pl.col("tipo") == "tesis"

    if actividades_proy is not None:
        df = df.with_columns(
            pl.when(es_proy)
            .then(pl.col("identificador").cast(pl.Utf8))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("_proy"),
        ).join(
            actividades_proy.select(
                pl.col("proyecto").alias("_proy"),
                pl.col("actividad").alias("_actividad_proy"),
            ),
            on="_proy",
            how="left",
        )
    else:
        df = df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("_actividad_proy"),
        )

    if doctorados is not None:
        df = df.with_columns(
            pl.when(es_tesis)
            .then(pl.col("identificador").cast(pl.Int64, strict=False))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("_alumno"),
        ).join(
            doctorados.select(
                pl.col("per_id_alumno").alias("_alumno"),
                pl.col("actividad_doctorado").alias("_actividad_doctorado"),
            ),
            on="_alumno",
            how="left",
        )
    else:
        df = df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("_actividad_doctorado"),
        )

    actividad_expr = (
        pl.when(pl.col("tipo") == "grupos")
        .then(pl.lit("dag-inves"))
        .when(es_tesis)
        .then(
            pl.coalesce([
                pl.col("_actividad_doctorado"),
                pl.lit("doctorado-sin-programa"),
            ])
        )
        .when(es_proy)
        .then(
            pl.coalesce([
                pl.col("_actividad_proy"),
                pl.concat_str([
                    pl.lit(f"{_PREFIJO_FALLBACK_INV}-"),
                    pl.col("identificador").cast(pl.Utf8),
                ]),
            ])
        )
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )

    df = (
        df.with_columns(actividad_expr.alias("_actividad"))
        .filter(pl.col("_actividad") == actividad)
        .drop(
            ["_actividad", "_actividad_proy", "_proy",
             "_actividad_doctorado", "_alumno"],
            strict=False,
        )
    )

    cols = [c.name for c in _COLS_UC_DETALLE if c.name in df.columns]
    df = df.select(cols)
    df, total, stats = apply_query(
        df, params,
        search_columns=["tipo", "identificador", "descripción", "origen"],
    )
    return ListResponse(
        columns=_COLS_UC_DETALLE,
        rows=_serialize(df.to_dicts()),
        total=total,
        column_stats=stats,
    )
