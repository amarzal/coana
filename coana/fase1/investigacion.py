"""Módulo de cálculo de dedicación a investigación.

Procesa información de:
- Coordinación de grupos de investigación
- Dirección/codirección/tutoría de tesis doctorales
- Participación en proyectos y contratos de investigación

Genera parquets con resumen por persona y detalle de registros.

Notas de implementación
-----------------------
Versión vectorizada con polars puro. Todas las funciones evitan
``iter_rows()`` y procesan los DataFrames mediante joins y expresiones
diferidas. En ``calcular_horas_proyectos`` esto baja el coste de
O(filas_investigadores × filas_kalendas) a O(filas) lineal.

Cambios menores respecto a la versión anterior (validados con la spec
de ``documentación/especificación.typ``):
- Kalendas: se acepta la columna real ``horas_declaradas`` (además del
  alias ``horas`` por compatibilidad).
- Proyectos: el lookup de fechas se hace por ``contrato`` (clave común
  con ``investigadores en contratos.xlsx``), no por ``proyecto``.
- Grupos: se conserva ``id_grupo`` directamente (la entrada no trae
  nombre del grupo, se deja a None).
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import polars as pl

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Esquemas vacíos (se devuelven cuando faltan los Excel o columnas clave).
# Mantenerlos centralizados evita repetir 8 veces el mismo dict.
# ---------------------------------------------------------------------------

_SCHEMA_GRUPOS: dict[str, type] = {
    "per_id": pl.Int64,
    "grupo_id": pl.Utf8,
    "grupo_nombre": pl.Utf8,
    "semanas": pl.Int32,
    "horas": pl.Float64,
    "tipo": pl.Utf8,
    "fecha_inicio": pl.Datetime,
    "fecha_fin": pl.Datetime,
}

_SCHEMA_TESIS: dict[str, type] = {
    "per_id": pl.Int64,
    "alumno_per_id": pl.Int64,
    "rol": pl.Utf8,
    "semanas": pl.Int32,
    "horas": pl.Float64,
    "tipo": pl.Utf8,
    "fecha_inicio": pl.Datetime,
    "fecha_fin": pl.Datetime,
}

_SCHEMA_PROYECTOS: dict[str, type] = {
    "per_id": pl.Int64,
    "contrato": pl.Utf8,
    "anexo": pl.Utf8,
    "semanas": pl.Int32,
    "horas": pl.Float64,
    "origen": pl.Utf8,
    "tipo": pl.Utf8,
    "fecha_inicio": pl.Datetime,
    "fecha_fin": pl.Datetime,
}


def _empty(schema: dict[str, type]) -> pl.DataFrame:
    return pl.DataFrame(schema=schema)


# ---------------------------------------------------------------------------
# Cálculo de semanas dentro del año (expresión polars reutilizable).
# ---------------------------------------------------------------------------

def _semanas_en_año(
    fecha_inicio: pl.Expr, fecha_fin: pl.Expr, año: int,
) -> pl.Expr:
    """Número de semanas (entero, redondeo hacia arriba) que el rango
    ``[fecha_inicio, fecha_fin]`` interseca con el año dado.

    Si cualquiera de las dos fechas es null, devuelve 0 (no hay
    evidencia de vigencia). Esto evita el comportamiento de
    ``max_horizontal``/``min_horizontal`` de polars, que descarta
    nulls y termina asumiendo el año entero cuando se desconocen las
    fechas.
    """
    inicio_año = pl.lit(datetime(año, 1, 1))
    fin_año = pl.lit(datetime(año, 12, 31))
    inicio_ef = pl.max_horizontal(fecha_inicio, inicio_año)
    fin_ef = pl.min_horizontal(fecha_fin, fin_año)
    dias = (fin_ef - inicio_ef).dt.total_days() + 1
    rango_valido = (
        fecha_inicio.is_not_null()
        & fecha_fin.is_not_null()
        & (dias > 0)
    )
    dias_validos = pl.when(rango_valido).then(dias).otherwise(0)
    return (dias_validos / 7.0).ceil().cast(pl.Int32)


# ---------------------------------------------------------------------------
# 1. Coordinación de grupos de investigación
# ---------------------------------------------------------------------------

def calcular_horas_grupos_investigacion(
    ruta_base: Path, año: int = 2025,
) -> pl.DataFrame:
    """Calcula horas de dedicación por coordinación de grupos de
    investigación. Solo cuentan coordinadores (``coordinador = 'S'``)
    cuya vigencia interseque con el año. 2 horas por semana vigente.
    """
    path = ruta_base / "entrada" / "investigación" / "investigadores en grupos.xlsx"
    if not path.exists():
        log.warning("No existe %s", path)
        return _empty(_SCHEMA_GRUPOS)

    from coana.util import read_excel
    df = read_excel(path)

    cols_requeridas = {"coordinador", "fecha_alta", "fecha_baja", "per_id"}
    faltan = cols_requeridas - set(df.columns)
    if faltan:
        log.warning("Faltan columnas en grupos: %s", faltan)
        return _empty(_SCHEMA_GRUPOS)

    # Detectar columna del grupo (lo más común: id_grupo).
    col_grupo: str | None = next(
        (c for c in ("id_grupo", "grupo") if c in df.columns), None,
    )
    col_nombre: str | None = next(
        (c for c in ("nombre_grupo", "nombre") if c in df.columns), None,
    )

    # Filtrar coordinadores, rellenar fecha_baja nula con fin del año y
    # calcular semanas vigentes en una sola pasada.
    fin_año = pl.lit(datetime(año, 12, 31))
    df = (
        df.filter(pl.col("coordinador") == "S")
        .with_columns(
            pl.col("fecha_baja").fill_null(fin_año).alias("fecha_baja"),
        )
        .with_columns(
            _semanas_en_año(
                pl.col("fecha_alta"), pl.col("fecha_baja"), año,
            ).alias("semanas"),
        )
        .filter(pl.col("semanas") > 0)
    )

    # Deduplicar por (per_id, grupo) — una persona puede aparecer
    # varias veces como coordinadora del mismo grupo a través de
    # distintas líneas.
    subset = ["per_id"] + ([col_grupo] if col_grupo else [])
    df = df.unique(subset=subset, keep="first")

    return (
        df.with_columns(
            (pl.col("semanas") * 2.0).alias("horas"),
            pl.lit("grupos").alias("tipo"),
            (
                pl.col(col_grupo).cast(pl.Utf8)
                if col_grupo
                else pl.lit(None, dtype=pl.Utf8)
            ).alias("grupo_id"),
            (
                pl.col(col_nombre).cast(pl.Utf8)
                if col_nombre
                else pl.lit(None, dtype=pl.Utf8)
            ).alias("grupo_nombre"),
            pl.col("fecha_alta").cast(pl.Datetime).alias("fecha_inicio"),
            pl.col("fecha_baja").cast(pl.Datetime).alias("fecha_fin"),
        )
        .select([
            "per_id", "grupo_id", "grupo_nombre",
            "semanas", "horas", "tipo", "fecha_inicio", "fecha_fin",
        ])
    )


# ---------------------------------------------------------------------------
# 2. Tesis doctorales (dirección, codirección, tutoría)
# ---------------------------------------------------------------------------

_ROLES_TESIS = [
    ("per_id_director", "director"),
    ("per_id_codirector", "codirector"),
    ("per_id_codirector2", "codirector2"),
    ("per_id_tutor", "tutor"),
]


def calcular_horas_tesis(ruta_base: Path, año: int = 2025) -> pl.DataFrame:
    """Calcula horas de dedicación por dirección/codirección/tutoría de
    tesis doctorales. 2 h/semana para el tutor; 2 h/semana para los
    directores y codirectores repartido entre ellos.

    Implementación vectorizada: en vez de un bucle Python por rol se
    hace un único ``unpivot`` que pasa las cuatro columnas de per_id a
    formato largo, y se calcula todo de una sola pasada.
    """
    path = ruta_base / "entrada" / "investigación" / "tesis.xlsx"
    if not path.exists():
        log.warning("No existe %s", path)
        return _empty(_SCHEMA_TESIS)

    from coana.util import read_excel
    df = read_excel(path)

    if "fecha_inicio_tiempo" not in df.columns or "fecha_fin_tiempo" not in df.columns:
        log.warning("Faltan fechas en tesis.xlsx")
        return _empty(_SCHEMA_TESIS)

    # Eliminar tesis dadas de baja.
    if "estado" in df.columns:
        df = df.filter(pl.col("estado") != "B")
    if df.is_empty():
        return _empty(_SCHEMA_TESIS)

    # Roles realmente presentes (per_id_codirector2 puede venir como
    # String — la casteamos a Int64 igual que las demás).
    roles_presentes = [(c, r) for c, r in _ROLES_TESIS if c in df.columns]
    if not roles_presentes:
        log.warning("No hay columnas de per_id en tesis.xlsx")
        return _empty(_SCHEMA_TESIS)

    cols_per_id = [c for c, _ in roles_presentes]

    # Cast Int64 a todas las columnas per_id en una sola llamada.
    df = df.with_columns(
        [pl.col(c).cast(pl.Int64, strict=False) for c in cols_per_id],
    )

    # Número de directores/codirectores no nulos por fila (los tutores
    # no se cuentan: las 2 h se reparten solo entre director(es)).
    cols_dir = [c for c, r in roles_presentes if r != "tutor"]
    n_directores_expr = (
        pl.sum_horizontal([pl.col(c).is_not_null().cast(pl.Int32) for c in cols_dir])
        if cols_dir else pl.lit(1, dtype=pl.Int32)
    )

    fin_año = pl.lit(datetime(año, 12, 31))
    df = df.with_columns(
        pl.col("fecha_fin_tiempo").fill_null(fin_año).alias("fecha_fin_efectiva"),
        n_directores_expr.alias("n_directores"),
    )

    # Añadir alumno_per_id si existe; si no, columna de nulos.
    if "per_id_alumno" not in df.columns:
        df = df.with_columns(
            pl.lit(None, dtype=pl.Int64).alias("per_id_alumno"),
        )

    # Pasar las columnas de per_id de roles de wide → long.
    largo = (
        df.select([
            "per_id_alumno",
            "fecha_inicio_tiempo",
            "fecha_fin_efectiva",
            "n_directores",
            *cols_per_id,
        ])
        .unpivot(
            on=cols_per_id,
            index=["per_id_alumno", "fecha_inicio_tiempo",
                   "fecha_fin_efectiva", "n_directores"],
            variable_name="_col_rol",
            value_name="per_id",
        )
        .filter(pl.col("per_id").is_not_null())
    )
    if largo.is_empty():
        return _empty(_SCHEMA_TESIS)

    # Mapear nombre de columna → rol legible.
    mapa_rol = {c: r for c, r in roles_presentes}
    largo = largo.with_columns(
        pl.col("_col_rol").replace(mapa_rol).alias("rol"),
    )

    # Semanas vigentes en el año + horas (2 h/semana, repartidas para
    # director/codirector; 2 h/semana enteras para el tutor).
    semanas = _semanas_en_año(
        pl.col("fecha_inicio_tiempo"),
        pl.col("fecha_fin_efectiva"),
        año,
    )
    horas = pl.when(pl.col("rol") == "tutor").then(
        pl.col("semanas") * 2.0,
    ).otherwise(
        pl.col("semanas") * 2.0 / pl.col("n_directores"),
    )

    return (
        largo
        .with_columns(semanas.alias("semanas"))
        .filter(pl.col("semanas") > 0)
        .with_columns(
            horas.alias("horas"),
            pl.lit("tesis").alias("tipo"),
            pl.col("per_id_alumno").alias("alumno_per_id"),
            pl.col("fecha_inicio_tiempo").cast(pl.Datetime).alias("fecha_inicio"),
            pl.col("fecha_fin_efectiva").cast(pl.Datetime).alias("fecha_fin"),
        )
        .select([
            "per_id", "alumno_per_id", "rol",
            "semanas", "horas", "tipo", "fecha_inicio", "fecha_fin",
        ])
    )


# ---------------------------------------------------------------------------
# 3. Participación en proyectos y contratos de investigación
# ---------------------------------------------------------------------------

# Tabla por defecto de horas/semana para los tipos de anexo (según la
# spec ``documentación/especificación.typ``, sección «Información de
# dedicación a investigación según las horas en el proyecto o
# contrato»). Se aplica cuando ``Tipos Anexo.xlsx`` no proporciona
# explícitamente una columna de horas. Cada clave es el "anexo" tal
# como aparece en los datos (concatenación tipo+subtipo o
# tipo+subtipo+microtipo).
_HORAS_POR_ANEXO_DEFECTO: dict[str, float] = {
    "2PI": 10.0, "2PE": 10.0, "2PN": 10.0, "2PA": 10.0, "2PV": 10.0,
    "2PU": 6.0,
    "1CI": 6.0, "1CV": 6.0,
    "1CE": 2.0,
    "1CA": 8.0, "1CS": 8.0, "1CT": 8.0,
}
_HORAS_DEFECTO = 6.0


def _kalendas_agregado(
    ruta_base: Path, año: int,
) -> pl.DataFrame | None:
    """Devuelve un DataFrame con (per_id, contrato, horas_kalendas)
    sumando todos los registros de Kalendas validados en el año.
    Retorna None si no existe el fichero o le faltan columnas clave.
    """
    path = ruta_base / "entrada" / "investigación" / "horas kalendas.xlsx"
    if not path.exists():
        return None

    from coana.util import read_excel
    df = read_excel(path)
    if df.is_empty():
        return None

    # La columna real es ``horas_declaradas`` (la versión antigua
    # buscaba ``horas`` y por eso nunca sumaba nada).
    col_horas = next(
        (c for c in ("horas_declaradas", "horas") if c in df.columns),
        None,
    )
    if col_horas is None or "contrato" not in df.columns or "per_id" not in df.columns:
        return None

    if "fecha_validación" in df.columns:
        df = df.filter(pl.col("fecha_validación").dt.year() == año)

    return (
        df.group_by("per_id", "contrato")
        .agg(pl.col(col_horas).sum().alias("horas_kalendas"))
        .with_columns(pl.col("contrato").cast(pl.Utf8))
    )


def _proyectos_fechas(ruta_base: Path) -> pl.DataFrame | None:
    """Devuelve (contrato, fecha_inicio_proy, fecha_fin_proy) para
    poder enriquecer cada (per_id, contrato) que no traiga fechas
    propias. Si el mismo contrato aparece varias veces (una por línea),
    cogemos la primera ocurrencia — basta con que el rango sea
    representativo del proyecto.
    """
    path = ruta_base / "entrada" / "investigación" / "proyectos en contratos investigación.xlsx"
    if not path.exists():
        return None

    from coana.util import read_excel
    df = read_excel(path)
    if df.is_empty() or "contrato" not in df.columns:
        return None
    if "fecha_inicio" not in df.columns or "fecha_fin" not in df.columns:
        return None

    return (
        df.select(
            pl.col("contrato").cast(pl.Utf8),
            pl.col("fecha_inicio").alias("fecha_inicio_proy"),
            pl.col("fecha_fin").alias("fecha_fin_proy"),
        )
        .group_by("contrato")
        .agg(
            pl.col("fecha_inicio_proy").first(),
            pl.col("fecha_fin_proy").first(),
        )
    )


def _anexos_por_contrato(ruta_base: Path) -> pl.DataFrame | None:
    """Devuelve (contrato, anexo, tipo_anexo) a partir de
    ``anexos proyectos.xlsx``.
    """
    path = ruta_base / "entrada" / "investigación" / "anexos proyectos.xlsx"
    if not path.exists():
        return None
    from coana.util import read_excel
    df = read_excel(path)
    if df.is_empty() or "contrato" not in df.columns:
        return None
    partes = []
    if "tipo_anexo" in df.columns:
        partes.append(pl.col("tipo_anexo").cast(pl.Utf8))
    if "subtipo_anexo" in df.columns:
        partes.append(pl.col("subtipo_anexo").cast(pl.Utf8).fill_null(""))
    if "microtipo_anexo" in df.columns:
        partes.append(pl.col("microtipo_anexo").cast(pl.Utf8).fill_null(""))
    if not partes:
        return None
    tipo_expr = (
        pl.col("tipo_anexo").cast(pl.Int64, strict=False)
        if "tipo_anexo" in df.columns
        else pl.lit(None, dtype=pl.Int64)
    )
    return (
        df.select(
            pl.col("contrato").cast(pl.Utf8),
            pl.concat_str(partes).str.strip_chars().alias("anexo"),
            tipo_expr.alias("tipo_anexo"),
        )
        .group_by("contrato")
        .agg(pl.col("anexo").first(), pl.col("tipo_anexo").first())
    )


def _tipos_anexo_horas(ruta_base: Path) -> pl.DataFrame | None:
    """Devuelve (anexo, horas_semana_anexo) a partir de Tipos Anexo.xlsx.

    Si el fichero tiene una columna numérica explícita con horas se usa.
    En caso contrario (que es lo habitual con el formato actual) se
    aplica la tabla por defecto declarada en la spec.
    """
    path = ruta_base / "entrada" / "investigación" / "Tipos Anexo.xlsx"
    if not path.exists():
        return None

    from coana.util import read_excel
    df = read_excel(path)
    if df.is_empty():
        return None

    # Construir la clave "anexo" como concatenación tipo+subtipo (y
    # microtipo cuando existe) — coincide con el formato esperado por
    # los datos de investigadores.
    partes = []
    if "tipo" in df.columns:
        partes.append(pl.col("tipo").cast(pl.Utf8))
    if "subtipo" in df.columns:
        partes.append(pl.col("subtipo").cast(pl.Utf8).fill_null(""))
    if "microtipo" in df.columns:
        partes.append(pl.col("microtipo").cast(pl.Utf8).fill_null(""))
    if not partes:
        return None

    anexo_expr = pl.concat_str(partes).str.strip_chars()

    # Si la tabla incluye una columna numérica con horas/semana, úsala.
    col_horas = next(
        (c for c in df.columns
         if "hora" in c.lower() and df.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)),
        None,
    )
    if col_horas:
        return df.select(
            anexo_expr.alias("anexo"),
            pl.col(col_horas).cast(pl.Float64).alias("horas_semana_anexo"),
        )

    # Caso habitual: aplicamos la tabla por defecto a la clave anexo.
    return df.select(
        anexo_expr.alias("anexo"),
        anexo_expr.replace_strict(
            _HORAS_POR_ANEXO_DEFECTO,
            default=_HORAS_DEFECTO,
            return_dtype=pl.Float64,
        ).alias("horas_semana_anexo"),
    )


_SCHEMA_SIN_FECHAS: dict[str, type] = {
    "per_id": pl.Int64,
    "contrato": pl.Utf8,
    "anexo": pl.Utf8,
    "tipo_anexo": pl.Int64,
    "horas_contratadas_semana": pl.Float64,
    "principal": pl.Utf8,
    "interlocutor": pl.Utf8,
    "tiene_kalendas": pl.Boolean,
    "razon": pl.Utf8,
}


def calcular_horas_proyectos(
    ruta_base: Path, año: int = 2025,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Calcula horas de dedicación por participación en contratos /
    proyectos de investigación.

    Jerarquía de horas/semana (en orden de prelación):

    1. Si la persona tiene horas registradas y validadas en Kalendas
       para el contrato durante el año, esas son las horas (origen:
       ``"Kalendas"``). Sustituye totalmente al cálculo por semanas.
    2. ``horas_contratadas_semana`` del propio fichero
       ``investigadores en contratos.xlsx`` cuando viene informada
       (origen: ``"Horas contratadas"``). Es la información explícita
       del contrato — tiene prioridad sobre la tabla.
    3. Tabla ``Tipos Anexo.xlsx`` por el anexo del registro
       (origen: ``"Tabla tipos anexo"``).
    4. Valor por defecto de 6 h/semana (origen: ``"Estimación"``).

    Las fechas de vigencia se resuelven con la prelación
    ``fecha_*_solicitud_alternativa`` → ``fecha_*_solicitud`` →
    fechas del propio proyecto (``proyectos en contratos
    investigación.xlsx``). Si tras esta cadena la fecha de inicio
    sigue siendo null, el registro se considera **sin fechas
    resolubles** y NO se imputan horas: se devuelve por separado en
    el segundo DataFrame para trazabilidad (origen: parquet
    ``proyectos_sin_fechas.parquet`` que escribe el orquestador).

    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame]
        ``(horas, sin_fechas)``. ``horas`` es la salida principal con
        las imputaciones; ``sin_fechas`` lista los registros sin
        vigencia resoluble en el año (incluyendo si tienen Kalendas o
        ``horas_contratadas_semana`` para que se vean en la app).
    """
    path_inv = ruta_base / "entrada" / "investigación" / "investigadores en contratos.xlsx"
    if not path_inv.exists():
        log.warning("No existe %s", path_inv)
        return _empty(_SCHEMA_PROYECTOS), _empty(_SCHEMA_SIN_FECHAS)

    from coana.util import read_excel
    df = read_excel(path_inv)
    if df.is_empty() or "per_id" not in df.columns or "contrato" not in df.columns:
        return _empty(_SCHEMA_PROYECTOS), _empty(_SCHEMA_SIN_FECHAS)

    # Normalizar contrato a String (clave de unión con Kalendas y
    # con proyectos).
    df = df.with_columns(pl.col("contrato").cast(pl.Utf8))

    # ── 1. Pre-agregar Kalendas y unir ────────────────────────────────
    kal = _kalendas_agregado(ruta_base, año)
    if kal is not None:
        df = df.join(kal, on=["per_id", "contrato"], how="left")
    else:
        df = df.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("horas_kalendas"),
        )

    # Asegurar columna horas_contratadas_semana presente (aunque venga
    # totalmente vacía en algún caso).
    if "horas_contratadas_semana" not in df.columns:
        df = df.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("horas_contratadas_semana"),
        )
    else:
        df = df.with_columns(
            pl.col("horas_contratadas_semana").cast(pl.Float64),
        )

    # ── 2. Resolver fechas de participación ──────────────────────────
    # Prelación: alternativa > solicitud > fechas del proyecto.
    proy_fechas = _proyectos_fechas(ruta_base)
    if proy_fechas is not None:
        df = df.join(proy_fechas, on="contrato", how="left")
    else:
        df = df.with_columns(
            pl.lit(None, dtype=pl.Date).alias("fecha_inicio_proy"),
            pl.lit(None, dtype=pl.Date).alias("fecha_fin_proy"),
        )

    def _coalesce_inicio() -> pl.Expr:
        candidatos: list[pl.Expr] = []
        for c in ("fecha_inicio_solicitud_alternativa",
                  "fecha_inicio_solicitud", "fecha_inicio_proy"):
            if c in df.columns:
                candidatos.append(pl.col(c))
        return pl.coalesce(candidatos) if candidatos else pl.lit(None)

    def _coalesce_fin() -> pl.Expr:
        candidatos: list[pl.Expr] = []
        for c in ("fecha_fin_solicitud_alternativa",
                  "fecha_fin_solicitud", "fecha_fin_proy"):
            if c in df.columns:
                candidatos.append(pl.col(c))
        candidatos.append(pl.lit(datetime(año, 12, 31)))
        return pl.coalesce(candidatos)

    df = df.with_columns(
        _coalesce_inicio().alias("fecha_inicio_efectiva"),
        _coalesce_fin().alias("fecha_fin_efectiva"),
    )

    # ── 3. Calcular semanas vigentes en el año (vectorizado) ─────────
    df = df.with_columns(
        _semanas_en_año(
            pl.col("fecha_inicio_efectiva"),
            pl.col("fecha_fin_efectiva"),
            año,
        ).alias("semanas"),
    )

    # ── 4. Anexo del contrato y horas/semana asociadas ───────────────
    anexos = _anexos_por_contrato(ruta_base)
    if anexos is not None:
        df = df.join(anexos, on="contrato", how="left")
    else:
        df = df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("anexo"),
            pl.lit(None, dtype=pl.Int64).alias("tipo_anexo"),
        )

    tipos = _tipos_anexo_horas(ruta_base)
    if tipos is not None:
        df = df.join(tipos, on="anexo", how="left")
    else:
        df = df.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("horas_semana_anexo"),
        )

    # ── 5. Separar registros descartables y guardarlos para traza ────
    # Razones (mutuamente excluyentes en una fila):
    #   a) Sin fecha_inicio resoluble y sin Kalendas.
    #   b) Anexo de tipo 3 o 4 sin horas explícitas (ni Kalendas ni
    #      horas_contratadas_semana).
    tiene_kal = pl.col("horas_kalendas").fill_null(0.0) > 0
    tiene_horas_contratadas = pl.col("horas_contratadas_semana").is_not_null()
    es_tipo_no_inv = pl.col("tipo_anexo").is_in([3, 4])

    sin_fechas_mask = pl.col("fecha_inicio_efectiva").is_null() & ~tiene_kal
    tipo_descartado_mask = (
        es_tipo_no_inv & ~tiene_kal & ~tiene_horas_contratadas
    )
    descartado_mask = sin_fechas_mask | tipo_descartado_mask

    razon_expr = (
        pl.when(sin_fechas_mask)
        .then(pl.lit("Sin fecha_inicio resoluble (ni solicitud, ni alternativa, ni proyecto)"))
        .when(tipo_descartado_mask)
        .then(pl.lit("Anexo tipo 3/4 (no investigación) sin horas explícitas"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )

    sin_fechas = df.filter(descartado_mask).select(
        pl.col("per_id"),
        pl.col("contrato"),
        pl.col("anexo"),
        pl.col("tipo_anexo"),
        pl.col("horas_contratadas_semana"),
        pl.col("principal") if "principal" in df.columns else pl.lit(None).alias("principal"),
        pl.col("interlocutor") if "interlocutor" in df.columns else pl.lit(None).alias("interlocutor"),
        tiene_kal.alias("tiene_kalendas"),
        razon_expr.alias("razon"),
    )
    df = df.filter(~descartado_mask)

    # Prelación de horas/semana: horas_contratadas_semana > tabla
    # anexo > defecto 6 h/sem.
    horas_semana_eff = pl.coalesce([
        pl.col("horas_contratadas_semana"),
        pl.col("horas_semana_anexo"),
        pl.lit(_HORAS_DEFECTO, dtype=pl.Float64),
    ])
    horas_no_decl_expr = pl.col("semanas").cast(pl.Float64) * horas_semana_eff

    # ── 6. Resolver horas y origen ───────────────────────────────────
    df = df.with_columns(
        pl.when(pl.col("horas_kalendas").fill_null(0.0) > 0)
        .then(pl.col("horas_kalendas"))
        .otherwise(horas_no_decl_expr)
        .alias("horas"),
        pl.when(pl.col("horas_kalendas").fill_null(0.0) > 0)
        .then(pl.lit("Kalendas"))
        .when(pl.col("horas_contratadas_semana").is_not_null())
        .then(pl.lit("Horas contratadas"))
        .when(pl.col("horas_semana_anexo").is_not_null())
        .then(pl.lit("Tabla tipos anexo"))
        .otherwise(pl.lit("Estimación"))
        .alias("origen"),
    )

    # ── 7. Filtrar registros sin contenido y formatear salida ────────
    df = df.filter(
        (pl.col("horas").fill_null(0.0) > 0)
        | (pl.col("horas_kalendas").fill_null(0.0) > 0),
    )

    if df.is_empty():
        return _empty(_SCHEMA_PROYECTOS), sin_fechas

    horas = df.select(
        pl.col("per_id"),
        pl.col("contrato"),
        pl.col("anexo"),
        # Las filas con horas de Kalendas no tienen "semanas" en
        # sentido estricto: se deja null para no inducir a error.
        pl.when(pl.col("origen") == "Kalendas")
        .then(pl.lit(None, dtype=pl.Int32))
        .otherwise(pl.col("semanas"))
        .alias("semanas"),
        pl.col("horas"),
        pl.col("origen"),
        pl.lit("proyectos").alias("tipo"),
        pl.when(pl.col("origen") == "Kalendas")
        .then(pl.lit(None, dtype=pl.Datetime))
        .otherwise(pl.col("fecha_inicio_efectiva").cast(pl.Datetime))
        .alias("fecha_inicio"),
        pl.when(pl.col("origen") == "Kalendas")
        .then(pl.lit(None, dtype=pl.Datetime))
        .otherwise(pl.col("fecha_fin_efectiva").cast(pl.Datetime))
        .alias("fecha_fin"),
    )
    return horas, sin_fechas


# ---------------------------------------------------------------------------
# Consolidación
# ---------------------------------------------------------------------------

def consolidar_dedicacion_investigacion(
    ruta_base: Path, año: int = 2025,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Calcula y consolida las tres fuentes de dedicación a
    investigación (grupos, tesis, proyectos) devolviendo tres
    DataFrames: ``resumen`` (por persona), ``detalle`` (registros
    individuales con horas imputadas) y ``sin_fechas`` (registros
    de proyectos descartados por no tener fecha resoluble en el
    año, para trazabilidad).
    """
    log.info("Calculando horas de grupos de investigación...")
    grupos = calcular_horas_grupos_investigacion(ruta_base, año)

    log.info("Calculando horas de tesis...")
    tesis = calcular_horas_tesis(ruta_base, año)

    log.info("Calculando horas de proyectos...")
    proyectos, proyectos_sin_fechas = calcular_horas_proyectos(ruta_base, año)

    partes_detalle: list[pl.DataFrame] = []

    if not grupos.is_empty():
        partes_detalle.append(grupos.select(
            pl.col("per_id"),
            pl.lit("grupos").alias("tipo"),
            pl.col("grupo_id").alias("identificador"),
            pl.col("grupo_nombre").alias("descripción"),
            pl.col("semanas"),
            pl.col("horas"),
            pl.lit("Coordinación grupo").alias("origen"),
            pl.col("fecha_inicio").cast(pl.Datetime),
            pl.col("fecha_fin").cast(pl.Datetime),
        ))

    if not tesis.is_empty():
        partes_detalle.append(tesis.select(
            pl.col("per_id"),
            pl.lit("tesis").alias("tipo"),
            pl.col("alumno_per_id").cast(pl.Utf8).alias("identificador"),
            pl.col("rol").alias("descripción"),
            pl.col("semanas"),
            pl.col("horas"),
            pl.lit("Dirección/tutoría tesis").alias("origen"),
            pl.col("fecha_inicio").cast(pl.Datetime),
            pl.col("fecha_fin").cast(pl.Datetime),
        ))

    if not proyectos.is_empty():
        partes_detalle.append(proyectos.select(
            pl.col("per_id"),
            pl.lit("proyectos").alias("tipo"),
            pl.col("contrato").alias("identificador"),
            pl.col("anexo").alias("descripción"),
            pl.col("semanas"),
            pl.col("horas"),
            pl.col("origen"),
            pl.col("fecha_inicio").cast(pl.Datetime),
            pl.col("fecha_fin").cast(pl.Datetime),
        ))

    if partes_detalle:
        detalle = pl.concat(partes_detalle, how="diagonal")
    else:
        detalle = pl.DataFrame(schema={
            "per_id": pl.Int64,
            "tipo": pl.Utf8,
            "identificador": pl.Utf8,
            "descripción": pl.Utf8,
            "semanas": pl.Int32,
            "horas": pl.Float64,
            "origen": pl.Utf8,
            "fecha_inicio": pl.Datetime,
            "fecha_fin": pl.Datetime,
        })

    if not detalle.is_empty():
        es_grupos = pl.col("tipo") == "grupos"
        es_tesis = pl.col("tipo") == "tesis"
        es_proy = pl.col("tipo") == "proyectos"
        resumen = detalle.group_by("per_id").agg(
            pl.col("horas").sum().alias("horas_totales"),
            pl.when(es_grupos).then(pl.col("semanas")).otherwise(0).sum().alias("semanas_grupos"),
            pl.when(es_tesis).then(pl.col("semanas")).otherwise(0).sum().alias("semanas_tesis"),
            pl.when(es_proy).then(pl.col("semanas")).otherwise(0).sum().alias("semanas_proyectos"),
            pl.when(es_grupos).then(pl.col("horas")).otherwise(0.0).sum().alias("horas_grupos"),
            pl.when(es_tesis).then(pl.col("horas")).otherwise(0.0).sum().alias("horas_tesis"),
            pl.when(es_proy).then(pl.col("horas")).otherwise(0.0).sum().alias("horas_proyectos"),
        )
    else:
        resumen = pl.DataFrame(schema={
            "per_id": pl.Int64,
            "horas_totales": pl.Float64,
            "semanas_grupos": pl.Int32,
            "semanas_tesis": pl.Int32,
            "semanas_proyectos": pl.Int32,
            "horas_grupos": pl.Float64,
            "horas_tesis": pl.Float64,
            "horas_proyectos": pl.Float64,
        })

    log.info("Procesadas %d personas con dedicación a investigación", resumen.height)
    log.info("Total registros de detalle: %d", detalle.height)
    log.info("Proyectos descartados por falta de fechas: %d", proyectos_sin_fechas.height)
    return resumen, detalle, proyectos_sin_fechas
