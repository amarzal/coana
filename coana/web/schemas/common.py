"""Schemas Pydantic compartidos por todos los routers."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

ColumnFormat = Literal["text", "id", "int", "float", "euro", "m2", "date", "bool"]
"""Cómo debe renderizar el frontend cada columna."""


class ColumnSpec(BaseModel):
    """Descripción de una columna de una tabla servida al frontend."""

    name: str = Field(..., description="Nombre interno de la columna")
    label: str = Field(..., description="Etiqueta humana mostrada en cabecera")
    format: ColumnFormat = "text"
    sortable: bool = True


class ColumnStats(BaseModel):
    """Resumen estadístico de una columna numérica del dataset filtrado.

    Calculado sobre todas las filas que pasan el filtro (no solo la página
    actual) para que el total y el histograma sean estables al paginar.
    Solo se calcula para columnas numéricas (Float/Int).
    """

    total: float = Field(..., description="Suma de valores no nulos")
    count: int = Field(..., description="Número de valores no nulos")
    min: float | None = None
    max: float | None = None
    bins: list[int] = Field(
        default_factory=list,
        description="Conteo por bin (20 bins de igual anchura entre min y max)",
    )


class ListResponse(BaseModel):
    """Respuesta común para listados con filtro/sort/paginación.

    El frontend usa ``columns`` para conocer formato y orden visual de las
    columnas; ``rows`` es una lista de diccionarios con valores ya
    serializables a JSON; ``total`` es el número total de filas tras aplicar
    el filtro pero antes de paginar; ``column_stats`` aporta total e
    histograma para columnas numéricas.
    """

    columns: list[ColumnSpec]
    rows: list[dict[str, Any]]
    total: int
    column_stats: dict[str, ColumnStats] = Field(default_factory=dict)


class Kpi(BaseModel):
    """Métrica agregada que se muestra en el panel KPI de cabecera."""

    label: str
    value: float | int | str | None
    format: ColumnFormat = "text"
    hint: str | None = None


class KpiPanel(BaseModel):
    kpis: list[Kpi]


class FieldValue(BaseModel):
    """Un par (campo, valor) ya formateado para la ficha de registro."""

    name: str
    label: str
    value: Any
    format: ColumnFormat = "text"


class RecordSection(BaseModel):
    """Una sección de la ficha (puede haber varias para enriquecimientos)."""

    label: str
    fields: list[FieldValue]


class RecordResponse(BaseModel):
    """Ficha completa de un registro: campos directos + secciones enriquecidas.

    El primer bloque (``main``) son los campos del propio registro; los
    enriquecimientos (lookup de personas, titulaciones, etc.) van en
    ``sections``.
    """

    main: list[FieldValue]
    sections: list[RecordSection] = []
