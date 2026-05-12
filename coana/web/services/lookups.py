"""Enriquecimiento de fichas: para un valor de campo, busca info de referencia.

Al servir una ficha de registro, queremos resolver `per_id` → nombre
completo de la persona, `centro` → nombre del centro presupuestario,
`aplicación` → nombre y jerarquía, etc. Aquí centralizamos esos
lookups: cada uno carga la tabla de referencia con
``coana.util.read_excel`` (que ya gestiona caché parquet por mtime) y
devuelve un dict de campos enriquecidos.

Las funciones son tolerantes: si el fichero no existe o el valor no se
encuentra, devuelven dict vacío en lugar de fallar.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import polars as pl

from coana.util import Árbol, read_excel
from coana.web.deps import DIR_ENTRADA, DIR_FASE1, _mtime_ns


def _safe_read(path: Path) -> pl.DataFrame | None:
    try:
        return read_excel(path)
    except FileNotFoundError:
        return None


@lru_cache(maxsize=64)
def _personas() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "nóminas" / "personas.xlsx")


@lru_cache(maxsize=64)
def _grados() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "docencia" / "grados.xlsx")


@lru_cache(maxsize=64)
def _masteres() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "docencia" / "másteres.xlsx")


@lru_cache(maxsize=64)
def _asignaturas_grados() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "docencia" / "asignaturas grados.xlsx")


@lru_cache(maxsize=64)
def _asignaturas_masteres() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "docencia" / "asignaturas másteres.xlsx")


@lru_cache(maxsize=64)
def _estudios() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "docencia" / "estudios.xlsx")


@lru_cache(maxsize=64)
def _subcentros() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "presupuesto" / "subcentros.xlsx")


@lru_cache(maxsize=64)
def _centros() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "presupuesto" / "centros.xlsx")


@lru_cache(maxsize=64)
def _proyectos() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "presupuesto" / "proyectos.xlsx")


@lru_cache(maxsize=64)
def _aplicaciones() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "presupuesto" / "aplicaciones de gasto.xlsx")


@lru_cache(maxsize=64)
def _programas() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "presupuesto" / "programas presupuestarios.xlsx")


@lru_cache(maxsize=64)
def _ubicaciones() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "superficies" / "ubicaciones.xlsx")


@lru_cache(maxsize=64)
def _servicios() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "inventario" / "servicios.xlsx")


@lru_cache(maxsize=64)
def _cuentas() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "inventario" / "años amortización por cuenta.xlsx")


@lru_cache(maxsize=64)
def _categorias_rh() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "nóminas" / "categorías recursos humanos.xlsx")


@lru_cache(maxsize=64)
def _categorias_plaza() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "nóminas" / "categorías plazas.xlsx")


@lru_cache(maxsize=64)
def _conceptos_retributivos() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "nóminas" / "conceptos retributivos.xlsx")


@lru_cache(maxsize=64)
def _tipos_coste() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "nóminas" / "tipos coste plantilla.xlsx")


@lru_cache(maxsize=64)
def _perceptores() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "nóminas" / "perceptores.xlsx")


@lru_cache(maxsize=64)
def _cargos() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "nóminas" / "cargos.xlsx")


@lru_cache(maxsize=64)
def _tipos_cargo() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "nóminas" / "tipos cargo.xlsx")


def lookup_persona(per_id: int | str | None) -> dict[str, str]:
    if per_id is None:
        return {}
    df = _personas()
    if df is None:
        return {}
    try:
        per_id_int = int(per_id)
    except (TypeError, ValueError):
        return {}
    fila = df.filter(pl.col("per_id") == per_id_int)
    if fila.is_empty():
        return {}
    row = fila.row(0, named=True)
    nombre = " ".join(
        s for s in (row.get("nombre"), row.get("apellido1"), row.get("apellido2"))
        if s
    )
    return {"persona": nombre, "tipo": row.get("tipo") or ""}


def _lookup_simple(df: pl.DataFrame | None, key_col: str, key: str | None) -> dict[str, str]:
    if df is None or key is None:
        return {}
    fila = df.filter(pl.col(key_col).cast(pl.Utf8) == str(key))
    if fila.is_empty():
        return {}
    row = fila.row(0, named=True)
    return {k: (str(v) if v is not None else "") for k, v in row.items() if k != key_col}


def lookup_centro(centro: str | None) -> dict[str, str]:
    return _lookup_simple(_centros(), "centro", centro)


def lookup_proyecto(proyecto: str | None) -> dict[str, str]:
    return _lookup_simple(_proyectos(), "proyecto", proyecto)


def lookup_aplicacion(aplicacion: str | None) -> dict[str, str]:
    return _lookup_simple(_aplicaciones(), "aplicación", aplicacion)


def lookup_programa(programa: str | None) -> dict[str, str]:
    return _lookup_simple(_programas(), "programa", programa)


def _resolver_estudio(codigo: str) -> str:
    """Devuelve "<código> — <nombre>" si existe; si no, solo el código."""
    df = _estudios()
    if df is None:
        return codigo
    fila = df.filter(pl.col("estudio").cast(pl.Utf8) == codigo)
    if fila.is_empty():
        return codigo
    nombre = fila.row(0, named=True).get("nombre") or ""
    return f"{codigo} — {nombre}" if nombre else codigo


def lookup_grado(valor) -> dict[str, str]:
    if valor is None:
        return {}
    df = _grados()
    if df is None:
        return {}
    fila = df.filter(pl.col("grado").cast(pl.Utf8) == str(valor))
    if fila.is_empty():
        return {}
    row = fila.row(0, named=True)
    out: dict[str, str] = {}
    for k, v in row.items():
        if k == "grado":
            continue
        s = str(v) if v is not None else ""
        # Encadena el lookup: estudio dentro de grado se resuelve a su nombre.
        if k == "estudio" and s:
            s = _resolver_estudio(s)
        out[k] = s
    return out


def lookup_master(valor) -> dict[str, str]:
    if valor is None:
        return {}
    df = _masteres()
    if df is None:
        return {}
    fila = df.filter(pl.col("máster").cast(pl.Utf8) == str(valor))
    if fila.is_empty():
        return {}
    row = fila.row(0, named=True)
    out: dict[str, str] = {}
    for k, v in row.items():
        if k == "máster":
            continue
        s = str(v) if v is not None else ""
        if k == "estudio" and s:
            s = _resolver_estudio(s)
        out[k] = s
    return out


def lookup_estudio(valor) -> dict[str, str]:
    if valor is None:
        return {}
    df = _estudios()
    if df is None:
        return {}
    fila = df.filter(pl.col("estudio").cast(pl.Utf8) == str(valor))
    if fila.is_empty():
        return {}
    row = fila.row(0, named=True)
    return {k: (str(v) if v is not None else "") for k, v in row.items() if k != "estudio"}


def lookup_subcentro(valor) -> dict[str, str]:
    return _lookup_simple(_subcentros(), "subcentro", valor)


def lookup_servicio(valor) -> dict[str, str]:
    return _lookup_simple(_servicios(), "servicio", valor)


def lookup_cuenta(valor) -> dict[str, str]:
    return _lookup_simple(_cuentas(), "cuenta", valor)


def lookup_categoria(valor) -> dict[str, str]:
    """Categoría: prueba primero RR.HH., y si no hay, plazas."""
    res = _lookup_simple(_categorias_rh(), "categoría", valor)
    if res:
        return res
    return _lookup_simple(_categorias_plaza(), "categoría", valor)


def lookup_categoria_plaza(valor) -> dict[str, str]:
    return _lookup_simple(_categorias_plaza(), "categoría", valor)


def lookup_concepto_retributivo(valor) -> dict[str, str]:
    return _lookup_simple(_conceptos_retributivos(), "concepto_retributivo", valor)


def lookup_tipo_coste(valor) -> dict[str, str]:
    return _lookup_simple(_tipos_coste(), "tipo_coste", valor)


def lookup_perceptor(valor) -> dict[str, str]:
    return _lookup_simple(_perceptores(), "perceptor", valor)


def lookup_cargo(valor) -> dict[str, str]:
    """Cargo: enlaza también con tipo_cargo cuando lo tenga."""
    base = _lookup_simple(_cargos(), "cargo", valor)
    tc = base.get("tipo_cargo")
    if tc:
        nombre = _lookup_simple(_tipos_cargo(), "tipo_cargo", tc).get("nombre")
        if nombre:
            base["tipo_cargo"] = f"{tc} — {nombre}"
    return base


def lookup_tipo_cargo(valor) -> dict[str, str]:
    return _lookup_simple(_tipos_cargo(), "tipo_cargo", valor)


@lru_cache(maxsize=2)
def _expedientes_rh() -> pl.DataFrame | None:
    return _safe_read(DIR_ENTRADA / "nóminas" / "expedientes recursos humanos.xlsx")


@lru_cache(maxsize=2)
def _expediente_resumen_año() -> pl.DataFrame:
    """Pre-agrega importe total y nº de líneas por expediente en el año."""
    p = DIR_ENTRADA / "nóminas" / "nóminas y seguridad social.xlsx"
    if not p.exists():
        return pl.DataFrame()
    df = read_excel(p)
    if "fecha" not in df.columns:
        return pl.DataFrame()
    año = 2026 if False else 2025  # TODO: parametrizar año analizado
    return (
        df.filter(pl.col("fecha").dt.year() == año)
        .group_by("expediente")
        .agg(
            pl.col("importe").sum().alias("importe"),
            pl.len().alias("n"),
        )
    )


_SECTOR_NOMBRE = {
    "PDI": "PDI",
    "PI": "PVI",
    "PAS": "PTGAS",
    "BEC": "Becario",
}


def lookup_expediente(valor) -> dict[str, str]:
    if valor in (None, ""):
        return {}
    try:
        exp = int(valor)
    except (ValueError, TypeError):
        return {}
    rh = _expedientes_rh()
    out: dict[str, str] = {}
    if rh is not None and "expediente" in rh.columns:
        fila = rh.filter(pl.col("expediente") == exp)
        if not fila.is_empty():
            r = fila.row(0, named=True)
            sector = str(r.get("sector") or "")
            out["sector"] = _SECTOR_NOMBRE.get(sector, sector)
    resumen = _expediente_resumen_año()
    if resumen is not None and not resumen.is_empty():
        sub = resumen.filter(pl.col("expediente") == exp)
        if not sub.is_empty():
            r = sub.row(0, named=True)
            imp = float(r.get("importe") or 0)
            n = int(r.get("n") or 0)
            out["importe 2025"] = (
                f"{imp:,.2f} €".replace(",", "·").replace(".", ",").replace("·", ".")
            )
            out["líneas 2025"] = str(n)
    return out


def lookup_ubicacion(valor) -> dict[str, str]:
    """Lookup por id_ubicación: devuelve área/edificio/planta/etc."""
    if valor is None:
        return {}
    df = _ubicaciones()
    if df is None:
        return {}
    try:
        v_int = int(valor)
    except (TypeError, ValueError):
        return {}
    fila = df.filter(pl.col("id_ubicación") == v_int)
    if fila.is_empty():
        return {}
    row = fila.row(0, named=True)
    return {
        k: (str(v) if v is not None else "")
        for k, v in row.items()
        if k != "id_ubicación"
    }


def lookup_asignatura(valor) -> dict[str, str]:
    """Asignatura: busca en grados; si no, en másteres."""
    if valor is None:
        return {}
    code = str(valor)

    # Grados
    df = _asignaturas_grados()
    if df is not None:
        fila = df.filter(pl.col("asignatura").cast(pl.Utf8) == code)
        if not fila.is_empty():
            row = fila.row(0, named=True)
            nombre = str(row.get("nombre") or "")
            grado = row.get("grado")
            grado_nombre = ""
            if grado is not None and grado != "":
                gd = _grados()
                if gd is not None:
                    g = gd.filter(pl.col("grado").cast(pl.Utf8) == str(grado))
                    if not g.is_empty():
                        grado_nombre = str(g.row(0, named=True).get("nombre") or "")
            out = {"nombre": nombre}
            if grado is not None and grado != "":
                out["grado"] = (
                    f"{grado} — {grado_nombre}" if grado_nombre else str(grado)
                )
            return out

    # Másteres
    df = _asignaturas_masteres()
    if df is not None:
        fila = df.filter(pl.col("asignatura").cast(pl.Utf8) == code)
        if not fila.is_empty():
            row = fila.row(0, named=True)
            nombre = str(row.get("nombre") or "")
            master = row.get("máster")
            master_nombre = ""
            if master is not None and master != "":
                md = _masteres()
                if md is not None:
                    m = md.filter(pl.col("máster").cast(pl.Utf8) == str(master))
                    if not m.is_empty():
                        master_nombre = str(m.row(0, named=True).get("nombre") or "")
            out = {"nombre": nombre}
            if master is not None and master != "":
                out["máster"] = (
                    f"{master} — {master_nombre}" if master_nombre else str(master)
                )
            return out

    return {}


# ----------------------------------------------------------------------
# Árboles finales (elemento de coste, centro de coste, actividad).
# Se leen de data/fase1/*.tree con caché por mtime.
# ----------------------------------------------------------------------

@lru_cache(maxsize=8)
def _arbol_cached(path_str: str, mtime_ns: int) -> Árbol | None:
    del mtime_ns
    p = Path(path_str)
    if not p.exists():
        return None
    return Árbol.from_file(p)


def _arbol_fase1(name: str) -> Árbol | None:
    p = DIR_FASE1 / f"{name}.tree"
    return _arbol_cached(str(p), _mtime_ns(p))


def _lookup_arbol(name: str, identificador) -> dict[str, str]:
    if identificador in (None, ""):
        return {}
    arbol = _arbol_fase1(name)
    if arbol is None:
        return {}
    nodo = arbol._por_id.get(str(identificador))
    if nodo is None:
        return {}
    out: dict[str, str] = {"código": str(nodo.código)}
    if nodo.descripción:
        out["descripción"] = str(nodo.descripción)
    return out


def lookup_actividad(valor) -> dict[str, str]:
    return _lookup_arbol("actividades", valor)


def lookup_centro_de_coste(valor) -> dict[str, str]:
    return _lookup_arbol("centros de coste", valor)


def lookup_elemento_de_coste(valor) -> dict[str, str]:
    return _lookup_arbol("elementos de coste", valor)


# ----------------------------------------------------------------------
# Mapeo nombre de columna → función de lookup
# ----------------------------------------------------------------------

_LOOKUP_BY_COL = {
    "per_id": lambda v: lookup_persona(v),
    "perid": lambda v: lookup_persona(v),
    "centro": lookup_centro,
    "centro_plaza": lookup_centro,
    "subcentro": lookup_subcentro,
    "proyecto": lookup_proyecto,
    "aplicación": lookup_aplicacion,
    "programa": lookup_programa,
    "grado": lookup_grado,
    "máster": lookup_master,
    "estudio": lookup_estudio,
    "asignatura": lookup_asignatura,
    "titulación": lambda v: lookup_grado(v) or lookup_master(v),
    "id_ubicación": lookup_ubicacion,
    "servicio": lookup_servicio,
    "cuenta": lookup_cuenta,
    "categoría": lookup_categoria,
    "categoría_plaza": lookup_categoria_plaza,
    "concepto_retributivo": lookup_concepto_retributivo,
    "tipo_coste": lookup_tipo_coste,
    "perceptor": lookup_perceptor,
    "cargo": lookup_cargo,
    "tipo_cargo": lookup_tipo_cargo,
    "actividad": lookup_actividad,
    "centro_de_coste": lookup_centro_de_coste,
    "elemento_de_coste": lookup_elemento_de_coste,
    "expediente": lookup_expediente,
}


# Actividad mensual de cargos académicos (CR 19/64, excluye atrasos
# 30/87): para una persona en un año dado, importe percibido por mes.
# Se precalcula al primer uso y se cachea por mtime del xlsx.

_MESES_ABREV = [
    "Ene", "Feb", "Mar", "Abr", "May", "Jun",
    "Jul", "Ago", "Sep", "Oct", "Nov", "Dic",
]
_PATH_NOMINAS_RAW = DIR_ENTRADA / "nóminas" / "nóminas y seguridad social.xlsx"
_AÑO_ANALIZADO = 2025  # TODO: parametrizar


@lru_cache(maxsize=2)
def _actividad_cargos_por_persona_mes(path_str: str, mtime_ns: int) -> dict:
    """Pre-agrega importes por (per_id, mes) de CR 19/64 en el año analizado
    y proyecto general (los CR 19/64 en proyecto específico ya generaron su
    UC y no se computan aquí).

    Devuelve un dict {per_id: {mes(1..12): importe_total}}.
    """
    from coana.fase1.nóminas.regla_23 import _PROYECTOS_GENERALES
    del mtime_ns
    p = Path(path_str)
    if not p.exists():
        return {}
    nóminas = read_excel(p)
    exp_path = DIR_ENTRADA / "nóminas" / "expedientes recursos humanos.xlsx"
    if not exp_path.exists():
        return {}
    expedientes = read_excel(exp_path)
    cr = pl.col("concepto_retributivo").cast(pl.Utf8)
    proy = pl.col("proyecto").cast(pl.Utf8)
    df = (
        nóminas.join(
            expedientes.select("expediente", "per_id"), on="expediente", how="inner",
        )
        .filter(cr.is_in(["19", "64"]))
        .filter(proy.is_in(list(_PROYECTOS_GENERALES)))
        .filter(pl.col("fecha").dt.year() == _AÑO_ANALIZADO)
        .with_columns(pl.col("fecha").dt.month().alias("mes"))
        .group_by("per_id", "mes")
        .agg(pl.col("importe").sum().alias("importe"))
    )
    out: dict[int, dict[int, float]] = {}
    for row in df.iter_rows(named=True):
        out.setdefault(int(row["per_id"]), {})[int(row["mes"])] = float(row["importe"])
    return out


def _actividad_cargo(per_id) -> dict[str, str]:
    """Devuelve un dict {Ene: «N,NN €», …} solo con meses con importe > 0."""
    if per_id in (None, ""):
        return {}
    try:
        pid = int(per_id)
    except (ValueError, TypeError):
        return {}
    mapa = _actividad_cargos_por_persona_mes(
        str(_PATH_NOMINAS_RAW), _mtime_ns(_PATH_NOMINAS_RAW),
    )
    meses = mapa.get(pid, {})
    if not meses:
        return {}
    return {
        _MESES_ABREV[m - 1]: f"{imp:,.2f} €".replace(",", "·").replace(".", ",").replace("·", ".")
        for m, imp in sorted(meses.items())
        if imp and imp > 0
    }


def enrich_row(row: dict) -> dict[str, dict[str, str]]:
    """Aplica los lookups conocidos a cada campo del row.

    Devuelve un dict ``{nombre_columna: {campo_extra: valor}}``. Solo
    incluye columnas que tienen lookup definido y para las que el valor
    aporta información (dict no vacío).
    """
    result: dict[str, dict[str, str]] = {}
    for col, fn in _LOOKUP_BY_COL.items():
        if col in row and row[col] not in (None, ""):
            data = fn(row[col])
            if data:
                result[col] = data
    # Enriquecimiento sintético: actividad mensual del cargo en el año
    # analizado (sumando líneas con CR 19/64 del per_id, excluye atrasos
    # CR 30/87). Solo aplica si la fila tiene per_id y cargo.
    if (
        "per_id" in row and row["per_id"] not in (None, "")
        and "cargo" in row and row["cargo"] not in (None, "")
    ):
        meses = _actividad_cargo(row["per_id"])
        if meses:
            previo = result.get("cargo", {})
            result["cargo"] = {**previo, **{f"{_AÑO_ANALIZADO} {k}": v for k, v in meses.items()}}
    return result


def clear_cache() -> None:
    """Vacía las cachés de tablas de referencia (tras editar entradas)."""
    for fn in (_personas, _centros, _proyectos, _aplicaciones, _programas,
               _grados, _masteres, _estudios, _subcentros, _ubicaciones,
               _asignaturas_grados, _asignaturas_masteres,
               _servicios, _cuentas, _categorias_rh, _categorias_plaza,
               _conceptos_retributivos, _tipos_coste, _perceptores,
               _cargos, _tipos_cargo, _arbol_cached):
        fn.cache_clear()
