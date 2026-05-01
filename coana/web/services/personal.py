"""Servicio del bloque Personal.

Subvistas:
- Resumen: KPIs por sector.
- Expedientes PDI/PTGAS/PVI/Otros: lista con per_id enriquecido a nombre.
- Multiexpediente: personas con expedientes en sectores distintos.
- Persona: agregado por per_id con sus UC y reparto de SS.
- Anomalías PDI: asignaturas sin titulación (si existe el parquet).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.web.deps import DIR_AUX, DIR_ENTRADA, _mtime_ns, read_parquet
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

DIR_NOMINAS = DIR_AUX / "nóminas"
PATH_PERSONAS = DIR_ENTRADA / "nóminas" / "personas.xlsx"

PATH_PDI = DIR_NOMINAS / "PDI.parquet"
PATH_PTGAS = DIR_NOMINAS / "PTGAS.parquet"
PATH_PVI = DIR_NOMINAS / "PVI.parquet"
PATH_OTROS = DIR_NOMINAS / "Otros.parquet"
PATH_MULTI = DIR_NOMINAS / "multiexpediente.parquet"
PATH_UC = DIR_NOMINAS / "persona_uc.parquet"
PATH_SS = DIR_NOMINAS / "persona_ss.parquet"
PATH_ANOM_PDI = DIR_NOMINAS / "regla_23_asignaturas_sin_titulación.parquet"

_SECTOR_PATHS = {
    "PDI": PATH_PDI,
    "PTGAS": PATH_PTGAS,
    "PVI": PATH_PVI,
    "Otros": PATH_OTROS,
}


def _safe_read(path: Path) -> pl.DataFrame | None:
    try:
        return read_parquet(path)
    except FileNotFoundError:
        return None


# Personas con caché ---------------------------------------------------

@lru_cache(maxsize=4)
def _personas_cached(path_str: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path_str)
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


def _personas() -> pl.DataFrame:
    return _personas_cached(str(PATH_PERSONAS), _mtime_ns(PATH_PERSONAS))


def _enriquecer_per_id(df: pl.DataFrame) -> pl.DataFrame:
    if "per_id" not in df.columns:
        return df
    return df.join(_personas(), on="per_id", how="left")


def _serialize(rows: list[dict]) -> list[dict]:
    return [
        {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()}
        for r in rows
    ]


# ----------------------------------------------------------------------
# Resumen
# ----------------------------------------------------------------------

def resumen() -> KpiPanel:
    kpis: list[Kpi] = []
    total_exp = 0
    total_imp = 0.0
    for sector, path in _SECTOR_PATHS.items():
        df = _safe_read(path)
        if df is None:
            continue
        n = df.height
        imp = float(df["importe"].sum() or 0)
        total_exp += n
        total_imp += imp
        kpis.append(Kpi(label=f"Expedientes {sector}", value=n, format="int"))
        kpis.append(Kpi(label=f"Importe {sector}", value=imp, format="euro"))
    multi = _safe_read(PATH_MULTI)
    n_multi = 0 if multi is None else multi.height
    kpis.append(Kpi(label="Total expedientes", value=total_exp, format="int"))
    kpis.append(Kpi(label="Importe total", value=total_imp, format="euro"))
    kpis.append(Kpi(label="Personas multiexpediente", value=n_multi, format="int"))
    return KpiPanel(kpis=kpis)


# ----------------------------------------------------------------------
# Expedientes por sector
# ----------------------------------------------------------------------

_COLS_EXP: list[ColumnSpec] = [
    ColumnSpec(name="expediente", label="Expediente", format="id"),
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="n_registros", label="N líneas nómina", format="int"),
]
_SEARCH_EXP = ["persona"]


def listar_sector(sector: str, params: QueryParams) -> ListResponse | None:
    path = _SECTOR_PATHS.get(sector)
    if path is None:
        return None
    df = _safe_read(path)
    if df is None:
        return ListResponse(columns=_COLS_EXP, rows=[], total=0)
    df = _enriquecer_per_id(df)
    df = df.select([c.name for c in _COLS_EXP if c.name in df.columns])
    df, total = apply_query(df, params, search_columns=_SEARCH_EXP)
    return ListResponse(columns=_COLS_EXP, rows=_serialize(df.to_dicts()), total=total)


def obtener_expediente(sector: str, expediente: int) -> RecordResponse | None:
    path = _SECTOR_PATHS.get(sector)
    if path is None:
        return None
    df = _safe_read(path)
    if df is None:
        return None
    fila = df.filter(pl.col("expediente") == expediente)
    if fila.is_empty():
        return None
    fila = _enriquecer_per_id(fila)
    row = fila.row(0, named=True)
    main = [
        FieldValue(name=c.name, label=c.label, value=row.get(c.name), format=c.format)
        for c in _COLS_EXP if c.name in row
    ]
    return RecordResponse(main=main, sections=[])


# ----------------------------------------------------------------------
# Multiexpediente
# ----------------------------------------------------------------------

_COLS_MULTI: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="sectores_str", label="Sectores", format="text"),
    ColumnSpec(name="n_sectores", label="N sectores", format="int"),
    ColumnSpec(name="n_PDI", label="N PDI", format="int"),
    ColumnSpec(name="n_PTGAS", label="N PTGAS", format="int"),
    ColumnSpec(name="n_PVI", label="N PVI", format="int"),
]


def listar_multiexpediente(params: QueryParams) -> ListResponse:
    df = _safe_read(PATH_MULTI)
    if df is None:
        return ListResponse(columns=_COLS_MULTI, rows=[], total=0)
    df = _enriquecer_per_id(df)
    if "sectores" in df.columns:
        df = df.with_columns(
            pl.col("sectores").list.join(", ").alias("sectores_str"),
        )
    df = df.select([c.name for c in _COLS_MULTI if c.name in df.columns])
    df, total = apply_query(df, params, search_columns=["persona", "sectores_str"])
    return ListResponse(columns=_COLS_MULTI, rows=_serialize(df.to_dicts()), total=total)


# ----------------------------------------------------------------------
# Persona (vista agregada)
# ----------------------------------------------------------------------

_COLS_PERSONA: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="n_uc", label="N UC", format="int"),
    ColumnSpec(name="importe_total", label="Importe total", format="euro"),
    ColumnSpec(name="ss_total", label="SS total", format="euro"),
]


def listar_personas(params: QueryParams) -> ListResponse:
    uc = _safe_read(PATH_UC)
    ss = _safe_read(PATH_SS)
    if uc is None and ss is None:
        return ListResponse(columns=_COLS_PERSONA, rows=[], total=0)

    if uc is not None and "per_id" in uc.columns:
        agg_uc = uc.group_by("per_id").agg(
            pl.len().alias("n_uc"),
            pl.col("importe").sum().alias("importe_total"),
        )
    else:
        agg_uc = pl.DataFrame(schema={"per_id": pl.Int64, "n_uc": pl.UInt32, "importe_total": pl.Float64})

    if ss is not None and "per_id" in ss.columns:
        col_ss = "ss_proporcional" if "ss_proporcional" in ss.columns else "ss_total"
        agg_ss = ss.group_by("per_id").agg(pl.col(col_ss).sum().alias("ss_total"))
    else:
        agg_ss = pl.DataFrame(schema={"per_id": pl.Int64, "ss_total": pl.Float64})

    df = agg_uc.join(agg_ss, on="per_id", how="full", coalesce=True).with_columns(
        pl.col("n_uc").fill_null(0),
        pl.col("importe_total").fill_null(0.0),
        pl.col("ss_total").fill_null(0.0),
    )
    df = _enriquecer_per_id(df)
    df = df.select([c.name for c in _COLS_PERSONA if c.name in df.columns])
    df, total = apply_query(df, params, search_columns=["persona"])
    return ListResponse(columns=_COLS_PERSONA, rows=_serialize(df.to_dicts()), total=total)


_COLS_UC_PERSONA: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID", format="text"),
    ColumnSpec(name="elemento_de_coste", label="Elemento", format="text"),
    ColumnSpec(name="centro_de_coste", label="Centro", format="text"),
    ColumnSpec(name="actividad", label="Actividad", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
    ColumnSpec(name="origen", label="Origen", format="text"),
    ColumnSpec(name="tipo", label="Tipo", format="text"),
]


def obtener_persona(per_id: int) -> RecordResponse | None:
    """Devuelve datos básicos de la persona y sus secciones de UC y SS."""
    personas = _personas()
    fila = personas.filter(pl.col("per_id") == per_id)
    nombre = fila.row(0, named=True).get("persona") if not fila.is_empty() else None

    main: list[FieldValue] = [
        FieldValue(name="per_id", label="per_id", value=per_id, format="id"),
        FieldValue(name="persona", label="Persona", value=nombre, format="text"),
    ]

    sections: list[RecordSection] = []

    uc = _safe_read(PATH_UC)
    if uc is not None and "per_id" in uc.columns:
        uc_p = uc.filter(pl.col("per_id") == per_id)
        if not uc_p.is_empty():
            n = uc_p.height
            imp = float(uc_p["importe"].sum() or 0)
            sections.append(RecordSection(
                label="Resumen UC",
                fields=[
                    FieldValue(name="n_uc", label="N UC", value=n, format="int"),
                    FieldValue(name="importe", label="Importe total", value=imp, format="euro"),
                ],
            ))

    ss = _safe_read(PATH_SS)
    if ss is not None and "per_id" in ss.columns:
        ss_p = ss.filter(pl.col("per_id") == per_id)
        if not ss_p.is_empty():
            col_ss = "ss_proporcional" if "ss_proporcional" in ss.columns else "ss_total"
            n = ss_p.height
            imp = float(ss_p[col_ss].sum() or 0)
            sections.append(RecordSection(
                label="Reparto SS",
                fields=[
                    FieldValue(name="n_ss", label="Pares act/CC", value=n, format="int"),
                    FieldValue(name="ss_total", label="SS total", value=imp, format="euro"),
                ],
            ))

    return RecordResponse(main=main, sections=sections)


def listar_uc_persona(per_id: int, params: QueryParams) -> ListResponse:
    uc = _safe_read(PATH_UC)
    if uc is None or "per_id" not in uc.columns:
        return ListResponse(columns=_COLS_UC_PERSONA, rows=[], total=0)
    df = uc.filter(pl.col("per_id") == per_id)
    df = df.select([c.name for c in _COLS_UC_PERSONA if c.name in df.columns])
    df, total = apply_query(
        df, params,
        search_columns=["id", "elemento_de_coste", "centro_de_coste", "actividad", "tipo"],
    )
    return ListResponse(columns=_COLS_UC_PERSONA, rows=_serialize(df.to_dicts()), total=total)


# ----------------------------------------------------------------------
# Anomalías PDI
# ----------------------------------------------------------------------

_COLS_ANOM: list[ColumnSpec] = [
    ColumnSpec(name="asignatura", label="Asignatura", format="text"),
    ColumnSpec(name="titulación", label="Titulación", format="text"),
    ColumnSpec(name="créditos_impartidos", label="Créditos", format="float"),
    ColumnSpec(name="per_id", label="per_id", format="id"),
]


def listar_anomalias_pdi(params: QueryParams) -> ListResponse:
    df = _safe_read(PATH_ANOM_PDI)
    if df is None:
        return ListResponse(columns=_COLS_ANOM, rows=[], total=0)
    nombres = [c.name for c in _COLS_ANOM if c.name in df.columns]
    df = df.select(nombres)
    df, total = apply_query(df, params)
    return ListResponse(columns=_COLS_ANOM, rows=_serialize(df.to_dicts()), total=total)
