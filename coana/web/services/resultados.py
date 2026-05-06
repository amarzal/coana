"""Servicio del bloque «Resultados Fase 1».

Visión consolidada de todas las UC generadas, agregaciones por nodo de
los árboles finales (actividades, centros de coste, elementos de coste)
y detección de anomalías de integridad referencial.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.util import Árbol, read_excel
from coana.web.deps import DIR_AUX, DIR_ENTRADA, DIR_FASE1, _mtime_ns, read_parquet
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

# Las fuentes de UC: (etiqueta para columna `_origen`, ruta del parquet).
_FUENTES_UC: list[tuple[str, Path]] = [
    ("presupuesto",     DIR_FASE1 / "uc presupuesto.parquet"),
    ("amortizaciones",  DIR_FASE1 / "uc amortizaciones.parquet"),
    ("suministros",     DIR_FASE1 / "uc suministros.parquet"),
    ("nóminas-PTGAS",   DIR_NOMINAS / "uc_ptgas.parquet"),
    ("nóminas-PVI",     DIR_NOMINAS / "uc_pvi.parquet"),
    ("nóminas-PDI",     DIR_NOMINAS / "uc_pdi.parquet"),
    ("despidos",        DIR_NOMINAS / "uc_despidos.parquet"),
    ("indemnizaciones", DIR_NOMINAS / "uc_indemnizaciones_asistencias.parquet"),
    ("cargos",          DIR_NOMINAS / "uc_cargos.parquet"),
    ("seguridad-social", DIR_NOMINAS / "persona_ss.parquet"),
]


def _safe_read(path: Path) -> pl.DataFrame | None:
    try:
        return read_parquet(path)
    except FileNotFoundError:
        return None


def _normalize(df: pl.DataFrame, fuente: str) -> pl.DataFrame:
    """Asegura que el DF tiene las columnas mínimas para el listado consolidado."""
    cols_min = ["id", "elemento_de_coste", "centro_de_coste", "actividad", "importe"]
    out = df
    if "id" not in out.columns:
        # persona_ss no tiene id; lo construimos
        if fuente == "seguridad-social":
            out = out.with_row_index("_idx").with_columns(
                pl.format("SS-{}", pl.col("_idx").cast(pl.String)).alias("id"),
            ).drop("_idx")
        else:
            out = out.with_row_index("id").with_columns(pl.col("id").cast(pl.String))
    if "importe" not in out.columns:
        # persona_ss usa ss_proporcional como importe efectivo
        if "ss_proporcional" in out.columns:
            out = out.with_columns(pl.col("ss_proporcional").alias("importe"))
        else:
            out = out.with_columns(pl.lit(0.0).alias("importe"))
    for c in ("elemento_de_coste", "centro_de_coste", "actividad"):
        if c not in out.columns:
            out = out.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))
    return out.select(cols_min).with_columns(pl.lit(fuente).alias("_origen"))


# Caché de la concatenación: clave = tupla con los mtime_ns de todos los parquets.
def _key_mtimes() -> tuple[int, ...]:
    return tuple(_mtime_ns(p) for _, p in _FUENTES_UC)


@lru_cache(maxsize=4)
def _todas_uc_cached(key: tuple[int, ...]) -> pl.DataFrame:
    del key
    partes: list[pl.DataFrame] = []
    for fuente, path in _FUENTES_UC:
        df = _safe_read(path)
        if df is None or df.is_empty():
            continue
        partes.append(_normalize(df, fuente))
    if not partes:
        return pl.DataFrame(schema={
            "id": pl.Utf8, "elemento_de_coste": pl.Utf8,
            "centro_de_coste": pl.Utf8, "actividad": pl.Utf8,
            "importe": pl.Float64, "_origen": pl.Utf8,
        })
    return pl.concat(partes, how="diagonal")


def _todas_uc() -> pl.DataFrame:
    return _todas_uc_cached(_key_mtimes())


# ----------------------------------------------------------------------
# Resumen
# ----------------------------------------------------------------------

def resumen() -> KpiPanel:
    df = _todas_uc()
    n_total = df.height
    imp_total = float(df["importe"].sum() or 0)
    kpis: list[Kpi] = [
        Kpi(label="UC totales", value=n_total, format="int"),
        Kpi(label="Importe total", value=imp_total, format="euro"),
    ]
    # Desglose por origen
    if not df.is_empty():
        agreg = (
            df.group_by("_origen")
            .agg(pl.len().alias("n"), pl.col("importe").sum().alias("imp"))
            .sort("imp", descending=True)
        )
        for row in agreg.iter_rows(named=True):
            kpis.append(Kpi(
                label=f"  {row['_origen']}", value=row["n"], format="int",
                hint=f"importe = {row['imp']:,.2f} €",
            ))
    return KpiPanel(kpis=kpis)


# ----------------------------------------------------------------------
# Listado consolidado (Todas las UC)
# ----------------------------------------------------------------------

_COLS_TODAS: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID", format="text"),
    ColumnSpec(name="_origen", label="Origen", format="text"),
    ColumnSpec(name="elemento_de_coste", label="Elemento", format="text"),
    ColumnSpec(name="centro_de_coste", label="Centro", format="text"),
    ColumnSpec(name="actividad", label="Actividad", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
]


_FUENTE_PATH = dict(_FUENTES_UC)


def _format_de(name: str, value) -> str:
    """Heurística simple para asignar formato de campo a una columna del UC."""
    if name in ("importe", "ss_total", "ss_proporcional", "importe_uc"):
        return "euro"
    if name == "pct" or name == "origen_porción":
        return "float"
    if hasattr(value, "isoformat"):
        return "date"
    if name in ("per_id", "expediente", "asiento") or name.startswith("id_"):
        return "id"
    if isinstance(value, bool):
        return "bool"
    return "text"


def _row_a_fields(row: dict) -> list[FieldValue]:
    out: list[FieldValue] = []
    for k, v in row.items():
        fmt = _format_de(k, v)
        val = v.isoformat() if hasattr(v, "isoformat") else v
        out.append(FieldValue(name=k, label=k, value=val, format=fmt))
    return out


@lru_cache(maxsize=2)
def _apuntes_presupuesto_cached(path_str: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path_str)
    if not p.exists():
        return pl.DataFrame()
    return read_excel(p)


def _apuntes_presupuesto() -> pl.DataFrame:
    p = DIR_ENTRADA / "presupuesto" / "apuntes presupuesto de gasto.xlsx"
    return _apuntes_presupuesto_cached(str(p), _mtime_ns(p))


@lru_cache(maxsize=2)
def _inventario_cached(path_str: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path_str)
    if not p.exists():
        return pl.DataFrame()
    return read_excel(p)


def _inventario() -> pl.DataFrame:
    p = DIR_ENTRADA / "inventario" / "inventario.xlsx"
    return _inventario_cached(str(p), _mtime_ns(p))


@lru_cache(maxsize=2)
def _nominas_raw_cached(path_str: str, mtime_ns: int) -> pl.DataFrame:
    del mtime_ns
    p = Path(path_str)
    if not p.exists():
        return pl.DataFrame()
    return read_excel(p)


def _nominas_raw() -> pl.DataFrame:
    p = DIR_ENTRADA / "nóminas" / "nóminas y seguridad social.xlsx"
    return _nominas_raw_cached(str(p), _mtime_ns(p))


def _seccion_origen_presupuesto(origen_id: str) -> list[RecordSection]:
    df = _apuntes_presupuesto()
    if df.is_empty() or "registro" not in df.columns:
        return []
    # Las UC de presupuesto guardan la base del registro (sin "/AÑO");
    # los apuntes lo serializan como "12964/2025". Aceptamos ambas formas.
    sub = df.filter(
        (pl.col("registro") == origen_id)
        | pl.col("registro").str.starts_with(f"{origen_id}/")
    )
    if sub.is_empty():
        return []
    rows = sub.head(20).to_dicts()
    secciones: list[RecordSection] = []
    label = "Apunte presupuestario" if len(rows) == 1 else f"Apuntes presupuestarios ({len(rows)})"
    for i, r in enumerate(rows, 1):
        sub_label = label if len(rows) == 1 else f"{label} #{i}"
        secciones.append(RecordSection(label=sub_label, fields=_row_a_fields(r)))
    return secciones


def _seccion_origen_inventario(origen_id: str) -> list[RecordSection]:
    df = _inventario()
    if df.is_empty() or "id" not in df.columns:
        return []
    try:
        oid = int(origen_id)
    except (TypeError, ValueError):
        return []
    sub = df.filter(pl.col("id") == oid)
    if sub.is_empty():
        return []
    return [RecordSection(label="Bien inventariable", fields=_row_a_fields(sub.row(0, named=True)))]


def _seccion_origen_nomina(origen_id: str) -> list[RecordSection]:
    df = _nominas_raw()
    if df.is_empty() or "id" not in df.columns:
        return []
    sub = df.filter(pl.col("id").cast(pl.Utf8) == str(origen_id))
    if sub.is_empty():
        return []
    return [RecordSection(label="Línea de nómina", fields=_row_a_fields(sub.row(0, named=True)))]


def obtener_uc(origen: str, uc_id: str) -> RecordResponse | None:
    """Ficha completa de una UC + sección con su registro de origen."""
    path = _FUENTE_PATH.get(origen)
    if path is None:
        return None
    df = _safe_read(path)
    if df is None or df.is_empty():
        return None

    # Caso especial: persona_ss no tiene id natural; durante la
    # consolidación lo construimos como "SS-{idx}".
    if origen == "seguridad-social":
        if not uc_id.startswith("SS-"):
            return None
        try:
            idx = int(uc_id[3:])
        except ValueError:
            return None
        if idx < 0 or idx >= df.height:
            return None
        fila = df.slice(idx, 1)
    else:
        if "id" not in df.columns:
            return None
        fila = df.filter(pl.col("id").cast(pl.Utf8) == str(uc_id))

    if fila.is_empty():
        return None
    row = fila.row(0, named=True)
    main = [FieldValue(name="_origen", label="Origen", value=origen, format="text")]
    main += _row_a_fields(row)

    # Resolver el registro de origen.
    sections: list[RecordSection] = []
    origen_id = row.get("origen_id")
    if origen_id not in (None, ""):
        oid = str(origen_id)
        if origen in ("presupuesto", "suministros"):
            sections += _seccion_origen_presupuesto(oid)
        elif origen == "amortizaciones":
            sections += _seccion_origen_inventario(oid)
        elif origen.startswith("nóminas-") or origen in ("despidos", "indemnizaciones", "cargos"):
            sections += _seccion_origen_nomina(oid)

    return RecordResponse(main=main, sections=sections)


def listar_todas(params: QueryParams) -> ListResponse:
    df = _todas_uc()
    df = df.select([c.name for c in _COLS_TODAS if c.name in df.columns])
    df, total, stats = apply_query(
        df, params,
        search_columns=["id", "_origen", "elemento_de_coste", "centro_de_coste", "actividad"],
    )
    return ListResponse(columns=_COLS_TODAS, rows=df.to_dicts(), total=total, column_stats=stats)


# ----------------------------------------------------------------------
# Agregación por nodo de árbol
# ----------------------------------------------------------------------

@lru_cache(maxsize=8)
def _arbol(path_str: str, mtime_ns: int) -> Árbol | None:
    del mtime_ns
    p = Path(path_str)
    if not p.exists():
        return None
    return Árbol.from_file(p)


# Mapping para endpoints `/arbol/{nombre}` → fichero en data/fase1/.
_ARBOL_NOMBRES = {
    "actividades": "actividades",
    "centros-de-coste": "centros de coste",
    "elementos-de-coste": "elementos de coste",
}


def cargar_arbol_final(slug: str):
    """Devuelve un árbol .tree final como NodoTree serializable.

    Marca como ``nuevo=True`` los nodos cuyo identificador no aparecía
    en el árbol original (`data/entrada/estructuras/{slug}.tree`); el
    frontend los destaca en otro color.
    """
    fichero = _ARBOL_NOMBRES.get(slug)
    if fichero is None:
        return None
    p_final = DIR_FASE1 / f"{fichero}.tree"
    arbol = _arbol(str(p_final), _mtime_ns(p_final))
    if arbol is None:
        return None

    p_orig = DIR_ENTRADA / "estructuras" / f"{fichero}.tree"
    orig = _arbol(str(p_orig), _mtime_ns(p_orig))
    ids_orig: set[str] = set(orig._por_id.keys()) if orig is not None else set()
    ids_final: set[str] = set(arbol._por_id.keys())
    nuevos = ids_final - ids_orig

    from coana.web.services.entradas import _serializar_nodo
    return _serializar_nodo(arbol.raíz, identificadores_nuevos=nuevos)


def _arbol_cached(name: str) -> Árbol | None:
    p = DIR_FASE1 / f"{name}.tree"
    return _arbol(str(p), _mtime_ns(p))


def _por_nodo(col_uc: str, árbol_name: str) -> pl.DataFrame:
    """Devuelve, para cada nodo del árbol, importe desglosado por origen + total."""
    arbol = _arbol_cached(árbol_name)
    df = _todas_uc()

    # Desglose UC × origen → tabla pivote por nodo.
    if df.is_empty() or col_uc not in df.columns:
        nodos = []
        if arbol is not None:
            for ident, nodo in arbol._por_id.items():
                if nodo.código:  # excluir raíz
                    nodos.append({
                        "código": nodo.código,
                        "identificador": ident,
                        "descripción": nodo.descripción,
                        "total": 0.0,
                    })
        return pl.DataFrame(nodos) if nodos else pl.DataFrame(
            schema={"código": pl.Utf8, "identificador": pl.Utf8, "descripción": pl.Utf8, "total": pl.Float64})

    pivot = (
        df.filter(pl.col(col_uc).is_not_null())
        .group_by(col_uc, "_origen")
        .agg(pl.col("importe").sum().alias("imp"))
        .pivot(values="imp", index=col_uc, on="_origen")
        .with_columns(
            pl.sum_horizontal(pl.exclude(col_uc)).alias("total"),
        )
    )

    # Construir filas por nodo (todos los del árbol, también los con 0).
    if arbol is None:
        return pivot.rename({col_uc: "identificador"})

    filas = []
    for ident, nodo in arbol._por_id.items():
        if not nodo.código:
            continue
        filas.append({
            "código": nodo.código,
            "identificador": ident,
            "descripción": nodo.descripción,
        })
    base = pl.DataFrame(filas)

    return base.join(
        pivot.rename({col_uc: "identificador"}),
        on="identificador", how="left",
    ).with_columns(pl.col("total").fill_null(0.0))


def _columns_for(df: pl.DataFrame) -> list[ColumnSpec]:
    cols: list[ColumnSpec] = [
        ColumnSpec(name="código", label="Código", format="text"),
        ColumnSpec(name="descripción", label="Descripción", format="text"),
        ColumnSpec(name="identificador", label="Identificador", format="text"),
        ColumnSpec(name="total", label="Total", format="euro"),
    ]
    # añadir cada origen presente como columna euro a continuación
    for c in df.columns:
        if c in ("código", "identificador", "descripción", "total"):
            continue
        cols.append(ColumnSpec(name=c, label=c, format="euro"))
    return cols


def _listar_nodos(col_uc: str, árbol_name: str, params: QueryParams) -> ListResponse:
    df = _por_nodo(col_uc, árbol_name)
    cols = _columns_for(df)
    nombres = [c.name for c in cols if c.name in df.columns]
    df = df.select(nombres)
    df, total, stats = apply_query(df, params, search_columns=["código", "identificador", "descripción"])
    rows = df.fill_null(0.0).to_dicts()
    return ListResponse(columns=cols, rows=rows, total=total, column_stats=stats)


def listar_actividades(p: QueryParams) -> ListResponse:
    return _listar_nodos("actividad", "actividades", p)


def listar_centros(p: QueryParams) -> ListResponse:
    return _listar_nodos("centro_de_coste", "centros de coste", p)


def listar_elementos(p: QueryParams) -> ListResponse:
    return _listar_nodos("elemento_de_coste", "elementos de coste", p)


# ----------------------------------------------------------------------
# Anomalías UC
# ----------------------------------------------------------------------

_COLS_ANOM: list[ColumnSpec] = [
    ColumnSpec(name="id", label="ID UC", format="text"),
    ColumnSpec(name="_origen", label="Origen", format="text"),
    ColumnSpec(name="campo", label="Campo afectado", format="text"),
    ColumnSpec(name="valor_inexistente", label="Identificador inexistente", format="text"),
    ColumnSpec(name="importe", label="Importe", format="euro"),
]


def _identificadores_arbol(name: str) -> set[str]:
    arbol = _arbol_cached(name)
    if arbol is None:
        return set()
    return set(arbol._por_id.keys())


def listar_anomalias(params: QueryParams) -> ListResponse:
    df = _todas_uc()
    if df.is_empty():
        return ListResponse(columns=_COLS_ANOM, rows=[], total=0)

    ids_act = _identificadores_arbol("actividades")
    ids_cc = _identificadores_arbol("centros de coste")
    ids_ec = _identificadores_arbol("elementos de coste")

    anomalías: list[pl.DataFrame] = []
    for col, campo, ids_ok in (
        ("actividad", "actividad", ids_act),
        ("centro_de_coste", "centro_de_coste", ids_cc),
        ("elemento_de_coste", "elemento_de_coste", ids_ec),
    ):
        if not ids_ok or col not in df.columns:
            continue
        sub = df.filter(
            pl.col(col).is_not_null()
            & ~pl.col(col).is_in(list(ids_ok))
        )
        if sub.is_empty():
            continue
        sub = sub.select(
            pl.col("id"),
            pl.col("_origen"),
            pl.lit(campo).alias("campo"),
            pl.col(col).alias("valor_inexistente"),
            pl.col("importe"),
        )
        anomalías.append(sub)

    if not anomalías:
        return ListResponse(columns=_COLS_ANOM, rows=[], total=0)

    todo = pl.concat(anomalías, how="diagonal")
    todo, total, stats = apply_query(
        todo, params,
        search_columns=["id", "_origen", "campo", "valor_inexistente"],
    )
    return ListResponse(columns=_COLS_ANOM, rows=todo.to_dicts(), total=total, column_stats=stats)


_COLS_ANOM_UNICOS: list[ColumnSpec] = [
    ColumnSpec(name="campo", label="Campo afectado", format="text"),
    ColumnSpec(name="valor_inexistente", label="Identificador inexistente", format="text"),
    ColumnSpec(name="n_uc", label="N UC afectadas", format="int"),
    ColumnSpec(name="importe", label="Importe total", format="euro"),
]


def listar_anomalias_unicos(params: QueryParams) -> ListResponse:
    """Identificadores inexistentes únicos: agrega las anomalías por
    (campo, valor_inexistente) y devuelve el número de UC afectadas y
    el importe acumulado. Útil para tener la lista de "qué hay que
    arreglar en los árboles" sin repetir filas."""
    df = _todas_uc()
    if df.is_empty():
        return ListResponse(columns=_COLS_ANOM_UNICOS, rows=[], total=0)

    ids_act = _identificadores_arbol("actividades")
    ids_cc = _identificadores_arbol("centros de coste")
    ids_ec = _identificadores_arbol("elementos de coste")

    anomalías: list[pl.DataFrame] = []
    for col, campo, ids_ok in (
        ("actividad", "actividad", ids_act),
        ("centro_de_coste", "centro_de_coste", ids_cc),
        ("elemento_de_coste", "elemento_de_coste", ids_ec),
    ):
        if not ids_ok or col not in df.columns:
            continue
        sub = df.filter(
            pl.col(col).is_not_null() & ~pl.col(col).is_in(list(ids_ok))
        )
        if sub.is_empty():
            continue
        anomalías.append(
            sub.select(
                pl.lit(campo).alias("campo"),
                pl.col(col).alias("valor_inexistente"),
                pl.col("importe"),
            )
        )

    if not anomalías:
        return ListResponse(columns=_COLS_ANOM_UNICOS, rows=[], total=0)

    todo = pl.concat(anomalías, how="diagonal")
    agg = (
        todo.group_by("campo", "valor_inexistente")
        .agg(
            pl.len().alias("n_uc"),
            pl.col("importe").sum().alias("importe"),
        )
        .sort(["campo", "valor_inexistente"])
    )
    agg, total, stats = apply_query(
        agg, params, search_columns=["campo", "valor_inexistente"],
    )
    return ListResponse(
        columns=_COLS_ANOM_UNICOS,
        rows=agg.to_dicts(),
        total=total,
        column_stats=stats,
    )
