"""Carga de las horas declaradas en Kalendas.

El fichero ``horas kalendas.xlsx`` contiene una fila por validación de
horas (#campo("per_id"), #campo("fecha_validación"),
#campo("horas_declaradas"), #campo("contrato"),
#campo("tipo_actividad")). Aquí se agregan en un diccionario anidado:

    {per_id: {contrato: Σ horas_declaradas}}

es decir, por cada investigador, un diccionario que asocia a cada
contrato la suma de sus horas declaradas en ese contrato.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from coana.util import read_excel

# Solo cuentan las horas imputadas a proyectos de investigación; las demás
# actividades (docencia, vacaciones, bajas, otras actividades I+D…) se
# descartan antes de agregar.
TIPO_PROYECTO_INVESTIGACIÓN = "Proyecto de investigacion"


def _ruta(ruta_base: Path) -> Path:
    return Path(ruta_base) / "entrada" / "investigación" / "horas kalendas.xlsx"


def cargar_horas_kalendas(ruta_base: Path = Path("data")) -> dict[int, dict[int, float]]:
    """Diccionario ``{per_id: {contrato: Σ horas_declaradas}}``.

    Solo se consideran las filas con
    ``tipo_actividad == "Proyecto de investigacion"``. Vacío si el fichero
    no existe.
    """
    ruta = _ruta(ruta_base)
    if not ruta.exists():
        return {}
    df = read_excel(ruta)
    if df.is_empty() or "per_id" not in df.columns:
        return {}
    df = df.filter(pl.col("tipo_actividad") == TIPO_PROYECTO_INVESTIGACIÓN)
    agg = (
        df.group_by("per_id", "contrato")
        .agg(pl.col("horas_declaradas").cast(pl.Float64).sum().alias("horas"))
    )
    out: dict[int, dict[int, float]] = {}
    for r in agg.iter_rows(named=True):
        out.setdefault(int(r["per_id"]), {})[int(r["contrato"])] = float(r["horas"] or 0.0)
    return out
