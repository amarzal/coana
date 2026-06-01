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
from coana.web.schemas.common import (
    ColumnSpec,
    FieldValue,
    ListResponse,
    RecordResponse,
    RecordSection,
)
from coana.web.services.query import QueryParams, apply_query

AÑO = 2025
PATH_GRUPOS = DIR_ENTRADA / "investigación" / "grupos investigación.xlsx"
PATH_INVESTIGADORES = DIR_ENTRADA / "investigación" / "investigadores en grupos.xlsx"
PATH_PERSONAS = DIR_ENTRADA / "nóminas" / "personas.xlsx"
PATH_KALENDAS = DIR_ENTRADA / "investigación" / "horas kalendas.xlsx"


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


# ----------------------------------------------------------------------
# Horas declaradas en Kalendas (suma por per_id × contrato)
# ----------------------------------------------------------------------

@lru_cache(maxsize=4)
def _kalendas_cached(path: str, mtime_ns: int) -> pl.DataFrame:
    """Suma de horas_declaradas por (per_id, contrato)."""
    del mtime_ns
    p = Path(path)
    if not p.exists():
        return pl.DataFrame(schema={
            "per_id": pl.Int64, "contrato": pl.Int64, "horas_declaradas": pl.Float64,
        })
    df = read_excel(p)
    if df.is_empty():
        return pl.DataFrame(schema={
            "per_id": pl.Int64, "contrato": pl.Int64, "horas_declaradas": pl.Float64,
        })
    from coana.fase1.kalendas import TIPO_PROYECTO_INVESTIGACIÓN
    df = df.filter(pl.col("tipo_actividad") == TIPO_PROYECTO_INVESTIGACIÓN)
    return (
        df.group_by("per_id", "contrato")
        .agg(pl.col("horas_declaradas").cast(pl.Float64).sum().alias("horas_declaradas"))
    )


def _kalendas() -> pl.DataFrame:
    return _kalendas_cached(str(PATH_KALENDAS), _mtime_ns(PATH_KALENDAS))


_COLS_KALENDAS: list[ColumnSpec] = [
    ColumnSpec(name="per_id", label="per_id", format="id"),
    ColumnSpec(name="persona", label="Persona", format="text"),
    ColumnSpec(name="contrato", label="Contrato", format="id"),
    ColumnSpec(name="horas_declaradas", label="Horas declaradas", format="float"),
]
_SEARCH_KALENDAS = ["persona"]


def listar_horas_kalendas(p: QueryParams) -> ListResponse:
    """Suma de horas declaradas en Kalendas por (per_id, contrato),
    enriquecida con el nombre de la persona."""
    df = _kalendas()
    if df.is_empty():
        return ListResponse(columns=_COLS_KALENDAS, rows=[], total=0)
    df = df.join(_personas(), on="per_id", how="left")
    df = df.select([c.name for c in _COLS_KALENDAS if c.name in df.columns])
    if not p.sort_by:
        df = df.sort(["per_id", "contrato"])
    df, total, stats = apply_query(df, p, search_columns=_SEARCH_KALENDAS)
    return ListResponse(
        columns=_COLS_KALENDAS, rows=df.to_dicts(), total=total, column_stats=stats,
    )


# ----------------------------------------------------------------------
# Ficha de detalle de una fila (per_id × contrato) de Horas Kalendas
# ----------------------------------------------------------------------

PATH_INV_CONTRATOS = DIR_ENTRADA / "investigación" / "investigadores en contratos.xlsx"
PATH_PROY_CONTRATOS = DIR_ENTRADA / "investigación" / "proyectos en contratos investigación.xlsx"
PATH_ANEXOS = DIR_ENTRADA / "investigación" / "anexos proyectos.xlsx"
PATH_CONTR_DEPTOS = DIR_ENTRADA / "investigación" / "contratos a departamentos.xlsx"


@lru_cache(maxsize=2)
def _kalendas_raw_cached(path: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path)
    return read_excel(p) if p.exists() else pl.DataFrame()


def _persona_nombre(per_id: int) -> str:
    per = _personas()
    if per.is_empty():
        return ""
    f = per.filter(pl.col("per_id") == per_id)
    return "" if f.is_empty() else (f.row(0, named=True).get("persona") or "")


def _fecha(v) -> str:
    return v.isoformat() if hasattr(v, "isoformat") else (str(v) if v is not None else "")


def detalle_horas_kalendas(per_id: int, contrato: int) -> RecordResponse | None:
    """Ficha con todo el detalle de una fila (per_id, contrato): horas por
    tipo de actividad, validaciones de proyecto, y los datos del contrato
    (proyecto presupuestario, anexo/financiación, departamento) y de la
    participación del investigador."""
    raw = _kalendas_raw_cached(str(PATH_KALENDAS), _mtime_ns(PATH_KALENDAS))
    if raw.is_empty():
        return None
    filas = raw.filter((pl.col("per_id") == per_id) & (pl.col("contrato") == contrato))
    if filas.is_empty():
        return None

    from coana.fase1.kalendas import TIPO_PROYECTO_INVESTIGACIÓN
    proy = filas.filter(pl.col("tipo_actividad") == TIPO_PROYECTO_INVESTIGACIÓN)
    horas_proy = float(proy["horas_declaradas"].sum() or 0.0)

    main = [
        FieldValue(name="per_id", label="per_id", value=per_id, format="id"),
        FieldValue(name="persona", label="Persona", value=_persona_nombre(per_id), format="text"),
        FieldValue(name="contrato", label="Contrato", value=contrato, format="id"),
        FieldValue(name="horas_proyecto", label="Horas a proyecto", value=round(horas_proy, 2), format="float"),
        FieldValue(name="n_validaciones", label="Nº validaciones (proyecto)", value=proy.height, format="int"),
        FieldValue(name="horas_total", label="Horas declaradas (todas)", value=round(float(filas["horas_declaradas"].sum() or 0.0), 2), format="float"),
    ]
    sections: list[RecordSection] = []

    # Desglose por tipo de actividad.
    por_tipo = (
        filas.group_by("tipo_actividad")
        .agg(pl.col("horas_declaradas").sum().alias("h"))
        .sort("h", descending=True)
    )
    sections.append(RecordSection(
        label="Horas por tipo de actividad",
        fields=[
            FieldValue(name=f"tipo_{i}", label=str(r["tipo_actividad"]),
                       value=round(float(r["h"] or 0.0), 2), format="float")
            for i, r in enumerate(por_tipo.iter_rows(named=True))
        ],
    ))

    # Validaciones de proyecto (fecha → horas), acotadas.
    if not proy.is_empty():
        vp = proy.sort("fecha_validación")
        cap = 60
        campos = [
            FieldValue(name=f"val_{i}", label=_fecha(r["fecha_validación"]),
                       value=round(float(r["horas_declaradas"] or 0.0), 2), format="float")
            for i, r in enumerate(vp.head(cap).iter_rows(named=True))
        ]
        if vp.height > cap:
            campos.append(FieldValue(name="val_más", label="…",
                          value=f"+{vp.height - cap} validaciones más", format="text"))
        sections.append(RecordSection(label="Validaciones a proyecto de investigación", fields=campos))

    # Datos del contrato: proyecto presupuestario, vigencia, importe.
    pc = _kalendas_raw_cached(str(PATH_PROY_CONTRATOS), _mtime_ns(PATH_PROY_CONTRATOS))
    if not pc.is_empty() and "contrato" in pc.columns:
        sub = pc.filter(pl.col("contrato") == contrato)
        if not sub.is_empty():
            con_imp = sub.filter(pl.col("importe_concedido").cast(pl.Float64, strict=False) > 0)
            principal = (con_imp.sort("línea") if not con_imp.is_empty() else sub.sort("línea")).row(0, named=True)
            campos = [
                FieldValue(name="proyecto", label="Proyecto presupuestario", value=str(principal.get("proyecto") or ""), format="text"),
                FieldValue(name="subproyecto", label="Subproyecto", value=str(principal.get("subproyecto") or ""), format="text"),
                FieldValue(name="n_lineas", label="Nº líneas", value=sub.height, format="int"),
                FieldValue(name="vigencia_ini", label="Vigencia desde", value=_fecha(sub["fecha_inicio"].min()), format="date"),
                FieldValue(name="vigencia_fin", label="Vigencia hasta", value=_fecha(sub["fecha_fin"].max()), format="date"),
                FieldValue(name="importe", label="Importe concedido (Σ líneas)",
                           value=round(float(sub["importe_concedido"].cast(pl.Float64, strict=False).sum() or 0.0), 2), format="euro"),
            ]
            sections.append(RecordSection(label="Contrato (SGIT)", fields=campos))

    # Anexo / tipo de financiación.
    an = _kalendas_raw_cached(str(PATH_ANEXOS), _mtime_ns(PATH_ANEXOS))
    if not an.is_empty() and "contrato" in an.columns:
        sub = an.filter(pl.col("contrato") == contrato)
        if not sub.is_empty():
            r = sub.row(0, named=True)
            tipo = "".join(str(r.get(k) or "") for k in ("tipo_anexo", "subtipo_anexo", "microtipo_anexo"))
            sections.append(RecordSection(label="Financiación (anexo)", fields=[
                FieldValue(name="tipo_anexo", label="Tipo (tipo+subtipo+micro)", value=tipo, format="text"),
                FieldValue(name="codex", label="Código externo", value=str(r.get("codex") or ""), format="text"),
                FieldValue(name="ejercicio", label="Ejercicio convocatoria", value=str(r.get("ejercicio_convocatoria") or ""), format="text"),
            ]))

    # Adscripción a departamento/unidad.
    cd = _kalendas_raw_cached(str(PATH_CONTR_DEPTOS), _mtime_ns(PATH_CONTR_DEPTOS))
    if not cd.is_empty() and "contrato" in cd.columns:
        sub = cd.filter(pl.col("contrato") == contrato)
        if not sub.is_empty():
            r = sub.row(0, named=True)
            sections.append(RecordSection(label="Adscripción", fields=[
                FieldValue(name="id_dep", label="Departamento (id)", value=str(r.get("id_dep") or ""), format="text"),
                FieldValue(name="unidad", label="Unidad", value=str(r.get("nombre") or ""), format="text"),
                FieldValue(name="tuest_id", label="Tipo unidad", value=str(r.get("tuest_id") or ""), format="text"),
            ]))

    # Participación del investigador en el contrato.
    ic = _kalendas_raw_cached(str(PATH_INV_CONTRATOS), _mtime_ns(PATH_INV_CONTRATOS))
    if not ic.is_empty() and {"per_id", "contrato"} <= set(ic.columns):
        sub = ic.filter((pl.col("per_id") == per_id) & (pl.col("contrato") == contrato))
        if not sub.is_empty():
            r = sub.row(0, named=True)
            sections.append(RecordSection(label="Participación del investigador", fields=[
                FieldValue(name="principal", label="Investigador principal", value=str(r.get("principal") or ""), format="text"),
                FieldValue(name="interlocutor", label="Interlocutor", value=str(r.get("interlocutor") or ""), format="text"),
                FieldValue(name="horas_semana", label="Horas/semana contratadas",
                           value=(round(float(r["horas_contratadas_semana"]), 2) if r.get("horas_contratadas_semana") is not None else ""), format="text"),
                FieldValue(name="desde", label="Desde", value=_fecha(r.get("fecha_inicio_solicitud")), format="date"),
                FieldValue(name="hasta", label="Hasta", value=_fecha(r.get("fecha_fin_solicitud")), format="date"),
            ]))

    return RecordResponse(main=main, sections=sections)
