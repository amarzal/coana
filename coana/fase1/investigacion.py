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
    "proyecto": pl.Utf8,
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


# ---------------------------------------------------------------------------
# Flag de la regla cross-project de no-superposición con Kalendas.
#
# Por defecto, los días en los que la persona tiene actividad Kalendas
# validada (en un contrato que también aparece en `investigadores en
# contratos.xlsx`) NO se imputan a otros contratos no-Kalendas de la
# misma persona durante ese intervalo (ver spec §«Regla cross-project
# de no-superposición con Kalendas»).
#
# Si se quiere desactivar el descuento y permitir que las horas de
# proyectos no-Kalendas también computen aunque haya Kalendas ese mes,
# basta con poner `BLOQUEO_KALENDAS_HABILITADO = False`. La rama
# Kalendas seguirá usando sus horas declaradas igualmente; solo
# cambia el cálculo de semanas en la rama no-Kalendas.
# ---------------------------------------------------------------------------
BLOQUEO_KALENDAS_HABILITADO: bool = True


def _kalendas_agregado(
    ruta_base: Path, año: int,
) -> pl.DataFrame | None:
    """Devuelve un DataFrame con (per_id, contrato, horas_kalendas,
    semanas_kalendas) sumando todos los registros de Kalendas
    validados en el año.

    ``semanas_kalendas`` cuenta las **semanas ISO distintas** del año
    en las que la persona tiene horas_declaradas > 0 para ese
    contrato (spec §«Cálculo de horas», punto 1: las horas
    corresponden a la suma del periodo y las semanas se determinan a
    partir de las semanas con registros).

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
        df = df.filter(
            pl.col("fecha_validación").is_not_null()
            & (pl.col("fecha_validación").dt.year() == año)
        )

    if "fecha_validación" not in df.columns:
        # Sin fechas no podemos contar semanas; devolvemos solo horas.
        return (
            df.group_by("per_id", "contrato")
            .agg(
                pl.col(col_horas).sum().alias("horas_kalendas"),
                pl.lit(None, dtype=pl.Int32).alias("semanas_kalendas"),
            )
            .with_columns(pl.col("contrato").cast(pl.Utf8))
        )

    return (
        df.with_columns(
            pl.col("fecha_validación").dt.week().alias("_iso_week"),
        )
        .group_by("per_id", "contrato")
        .agg(
            pl.col(col_horas).sum().alias("horas_kalendas"),
            # Semanas ISO distintas con horas > 0
            pl.col("_iso_week")
            .filter(pl.col(col_horas) > 0)
            .n_unique()
            .cast(pl.Int32)
            .alias("semanas_kalendas"),
        )
        .with_columns(pl.col("contrato").cast(pl.Utf8))
    )


def _ocupacion_kalendas(
    ruta_base: Path,
    año: int,
    contratos_válidos: pl.DataFrame | None = None,
) -> pl.DataFrame | None:
    """Devuelve ``(per_id, oc_inicio, oc_fin)``: intervalos del año en
    los que cada persona tiene actividad Kalendas validada en contratos
    que forman parte del cálculo actual (``investigadores en contratos``).

    Si se pasa *contratos_válidos* (con columnas ``per_id``, ``contrato``),
    solo se consideran los registros Kalendas cuyo ``(per_id, contrato)``
    aparezca en ese DataFrame. Así, contratos Kalendas ajenos al fichero
    de investigadores no bloquean la imputación de otros proyectos.

    Regla por mes:
    - Si el día máximo de ``fecha_validación`` en el mes es ≤ 7
      (actividad solo en la primera semana), se ocupan únicamente los
      días 1-7.
    - Si es > 7 (la actividad se extiende más allá de la primera
      semana), se ocupa el mes entero.
    """
    path = ruta_base / "entrada" / "investigación" / "horas kalendas.xlsx"
    if not path.exists():
        return None
    from coana.util import read_excel
    df = read_excel(path)
    if (
        df.is_empty()
        or "per_id" not in df.columns
        or "fecha_validación" not in df.columns
        or "contrato" not in df.columns
    ):
        return None
    df = df.filter(
        pl.col("fecha_validación").is_not_null()
        & (pl.col("fecha_validación").dt.year() == año)
    )
    if df.is_empty():
        return None

    if contratos_válidos is not None:
        df = df.with_columns(pl.col("contrato").cast(pl.Utf8))
        df = df.join(
            contratos_válidos.select("per_id", "contrato").unique(),
            on=["per_id", "contrato"],
            how="inner",
        )
        if df.is_empty():
            return None

    agg = (
        df.with_columns(
            pl.col("fecha_validación").dt.month().alias("_mes"),
            pl.col("fecha_validación").dt.day().alias("_día"),
        )
        .group_by("per_id", "_mes")
        .agg(pl.col("_día").max().alias("_max_día"))
    )
    return agg.with_columns(
        pl.datetime(año, pl.col("_mes"), 1).alias("oc_inicio"),
        pl.when(pl.col("_max_día") <= 7)
        .then(pl.datetime(año, pl.col("_mes"), 7))
        .otherwise(pl.datetime(año, pl.col("_mes"), 1).dt.month_end())
        .alias("oc_fin"),
    ).select("per_id", "oc_inicio", "oc_fin")


def _proyectos_fechas(ruta_base: Path) -> pl.DataFrame | None:
    """Devuelve (contrato, proyecto, fecha_inicio_proy, fecha_fin_proy)
    para enriquecer cada (per_id, contrato) que no traiga fechas
    propias. Si el mismo contrato aparece varias veces (una por línea),
    cogemos la primera ocurrencia.
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

    proyecto_expr = (
        pl.col("proyecto").cast(pl.Utf8)
        if "proyecto" in df.columns
        else pl.lit(None, dtype=pl.Utf8).alias("proyecto")
    )
    return (
        df.select(
            pl.col("contrato").cast(pl.Utf8),
            proyecto_expr.alias("proyecto"),
            pl.col("fecha_inicio").alias("fecha_inicio_proy"),
            pl.col("fecha_fin").alias("fecha_fin_proy"),
        )
        .group_by("contrato")
        .agg(
            pl.col("proyecto").first(),
            pl.col("fecha_inicio_proy").first(),
            pl.col("fecha_fin_proy").first(),
        )
    )


def _anexos_por_contrato(ruta_base: Path) -> pl.DataFrame | None:
    """Devuelve (contrato, anexo, tipo_anexo, codex) a partir de
    ``anexos proyectos.xlsx``. ``codex`` es el código externo del
    contrato (campo ``codex`` del Excel) — útil para la descripción
    en la app.
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
    codex_expr = (
        pl.col("codex").cast(pl.Utf8).str.strip_chars()
        if "codex" in df.columns
        else pl.lit(None, dtype=pl.Utf8)
    )
    return (
        df.select(
            pl.col("contrato").cast(pl.Utf8),
            pl.concat_str(partes).str.strip_chars().alias("anexo"),
            tipo_expr.alias("tipo_anexo"),
            codex_expr.alias("codex"),
        )
        .group_by("contrato")
        .agg(
            pl.col("anexo").first(),
            pl.col("tipo_anexo").first(),
            pl.col("codex").first(),
        )
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
            pl.lit(None, dtype=pl.Utf8).alias("proyecto"),
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

    # ── 3. Calcular semanas vigentes en el año, descontando los meses
    #       (o primera semana) en los que la persona ya tiene actividad
    #       Kalendas en cualquier contrato — regla de no-superposición
    #       cross-project. La rama Kalendas usa sus horas declaradas y
    #       el valor de ``semanas`` se anula para esos registros en el
    #       paso de salida, así que el descuento aquí solo afecta de
    #       facto a los proyectos no-Kalendas. ───────────────────────
    inicio_año_lit = pl.lit(datetime(año, 1, 1))
    fin_año_lit = pl.lit(datetime(año, 12, 31))
    inicio_clip = pl.max_horizontal(
        pl.col("fecha_inicio_efectiva"), inicio_año_lit
    )
    fin_clip = pl.min_horizontal(
        pl.col("fecha_fin_efectiva"), fin_año_lit
    )
    dias_raw = (fin_clip - inicio_clip).dt.total_days() + 1
    rango_valido = (
        pl.col("fecha_inicio_efectiva").is_not_null()
        & pl.col("fecha_fin_efectiva").is_not_null()
        & (dias_raw > 0)
    )
    df = df.with_columns(
        pl.when(rango_valido).then(dias_raw).otherwise(0).alias("_dias_brutos"),
    )

    # Regla cross-project: descontar días ocupados por Kalendas a la
    # rama no-Kalendas (ver flag BLOQUEO_KALENDAS_HABILITADO arriba en
    # el módulo). Si el flag está a False, equivale a ``_ocupacion_*``
    # = vacío y no se descuenta nada.
    ocupacion = (
        _ocupacion_kalendas(
            ruta_base, año,
            contratos_válidos=df.select("per_id", "contrato"),
        )
        if BLOQUEO_KALENDAS_HABILITADO
        else None
    )
    if ocupacion is not None and not ocupacion.is_empty():
        df = df.with_row_index("_row_id")
        overlap = (
            df.select(
                "_row_id", "per_id",
                "fecha_inicio_efectiva", "fecha_fin_efectiva",
            )
            .join(ocupacion, on="per_id", how="inner")
        )
        ov_inicio = pl.max_horizontal(
            pl.col("fecha_inicio_efectiva"), pl.col("oc_inicio")
        )
        ov_fin = pl.min_horizontal(
            pl.col("fecha_fin_efectiva"), pl.col("oc_fin")
        )
        overlap = overlap.with_columns(
            pl.when(ov_fin >= ov_inicio)
            .then((ov_fin - ov_inicio).dt.total_days() + 1)
            .otherwise(0)
            .alias("_ov_days"),
        )
        ocup_agg = overlap.group_by("_row_id").agg(
            pl.col("_ov_days").sum().alias("_dias_kalendas_ocup")
        )
        df = (
            df.join(ocup_agg, on="_row_id", how="left")
            .with_columns(pl.col("_dias_kalendas_ocup").fill_null(0))
            .drop("_row_id")
        )
    else:
        df = df.with_columns(
            pl.lit(0, dtype=pl.Int64).alias("_dias_kalendas_ocup")
        )

    dias_efectivos = pl.max_horizontal(
        pl.lit(0, dtype=pl.Int64),
        pl.col("_dias_brutos") - pl.col("_dias_kalendas_ocup"),
    )
    df = df.with_columns(
        (dias_efectivos / 7.0).ceil().cast(pl.Int32).alias("semanas"),
    ).drop("_dias_brutos", "_dias_kalendas_ocup")

    # ── 4. Anexo del contrato y horas/semana asociadas ───────────────
    anexos = _anexos_por_contrato(ruta_base)
    if anexos is not None:
        df = df.join(anexos, on="contrato", how="left")
    else:
        df = df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("anexo"),
            pl.lit(None, dtype=pl.Int64).alias("tipo_anexo"),
            pl.lit(None, dtype=pl.Utf8).alias("codex"),
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

    # En la rama Kalendas las "semanas" representan las semanas ISO en
    # las que la persona declaró horas en ese contrato (ver
    # `_kalendas_agregado`). En la rama no-Kalendas son las semanas
    # vigentes en el año tras descontar la ocupación cross-project.
    semanas_finales = (
        pl.when(pl.col("origen") == "Kalendas")
        .then(pl.col("semanas_kalendas").cast(pl.Int32))
        .otherwise(pl.col("semanas"))
    )

    # Las fechas de la rama Kalendas no son las de la solicitud sino
    # las del rango de declaración. Como las semanas ISO ya dan la
    # información temporal real, dejamos las fechas a null en
    # Kalendas para evitar inducir a error en el detalle.
    horas = df.select(
        pl.col("per_id"),
        pl.col("contrato"),
        pl.col("proyecto") if "proyecto" in df.columns
            else pl.lit(None, dtype=pl.Utf8).alias("proyecto"),
        pl.col("codex") if "codex" in df.columns
            else pl.lit(None, dtype=pl.Utf8).alias("codex"),
        pl.col("anexo"),
        semanas_finales.alias("semanas"),
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
        proyecto_id = (
            pl.col("proyecto")
            if "proyecto" in proyectos.columns
            else pl.col("contrato")
        )
        # Descripción: "contrato (codex) · anexo" — codex entre
        # paréntesis se omite si es null/vacío.
        contrato_str = pl.col("contrato").cast(pl.Utf8)
        if "codex" in proyectos.columns:
            con_codex = pl.concat_str(
                [contrato_str, pl.lit(" ("), pl.col("codex"), pl.lit(")")],
            )
            contrato_y_codex = pl.when(
                pl.col("codex").is_not_null()
                & (pl.col("codex").str.len_chars() > 0)
            ).then(con_codex).otherwise(contrato_str)
        else:
            contrato_y_codex = contrato_str
        anexo_str = pl.col("anexo").fill_null(pl.lit(""))
        desc_expr = pl.concat_str(
            [contrato_y_codex, anexo_str],
            separator=" · ",
        ).str.strip_chars()
        partes_detalle.append(proyectos.select(
            pl.col("per_id"),
            pl.lit("proyectos").alias("tipo"),
            proyecto_id.alias("identificador"),
            desc_expr.alias("descripción"),
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


# ---------------------------------------------------------------------------
# Generación de UC a partir de la dedicación a investigación
# ---------------------------------------------------------------------------

_SCHEMA_UC_INVESTIGACION: dict[str, type] = {
    "per_id": pl.Int64,
    "actividad": pl.Utf8,
    "horas": pl.Float64,
    "horas_totales": pl.Float64,
    "porcentaje": pl.Float64,
}


# ---------------------------------------------------------------------------
# Mapeo tipo_proyecto → prefijo de actividad. Coincide con las reglas
# del traductor de presupuesto (§«Costes en proyectos de Investigación
# y transferencia») cuando el proyecto cumple programa=541-A y
# tipo_línea≠00 (caso por defecto de los proyectos de investigación).
# Los tipos «artículos 60» (0000I/A11I/A1TI/A83CA/CA/PCT/IDI) se
# tratan aparte porque su prefijo depende del nombre del proyecto
# (cátedra/aula empresa vs. resto).
# ---------------------------------------------------------------------------
_TIPO_PROYECTO_PREFIJO_AI: dict[str, str] = {
    # Investigación internacional
    "UEI": "ai-internacional",
    "UEGD": "ai-internacional",
    # Investigación nacional
    "06I": "ai-nacional",
    "COBEI": "ai-nacional",
    "MCTFE": "ai-nacional",
    "MCTI": "ai-nacional",
    "MEC": "ai-nacional",
    "MECD": "ai-nacional",
    "MECI": "ai-nacional",
    "MIE": "ai-nacional",
    "MIG": "ai-nacional",
    "MPEI": "ai-nacional",
    "MSI": "ai-nacional",
    "MSP": "ai-nacional",
    "MTAI": "ai-nacional",
    "MTD": "ai-nacional",
    # Investigación regional
    "DIPI": "ai-regional",
    "FGVI": "ai-regional",
    "GVI": "ai-regional",
    # Otras competitivas
    "BECI": "ai-otras-competitivas",
    "CONI": "ai-otras-competitivas",
    "CONVI": "ai-otras-competitivas",
    # Plan propio
    "000TR": "ait-financiación-propia",
    # Co-financiación: por simplicidad caen en financiación externa
    # (la spec distingue propia/externa con `_tipo_línea`, que aquí
    # no tenemos a mano; lo más común para contratos de investigación
    # es externa, ya que tienen una línea de financiación finalista).
    "000I": "ait-financiación-externa",
    "PII": "ait-financiación-externa",
    "COF": "ait-financiación-externa",
    "MEC": "ai-nacional",  # ya cubierto arriba, dejado por claridad
}

# Tipos de "artículos 60" que generan transf-60 o cátedras-aulas-empresa
# según el nombre del proyecto contenga «cátedra» / «aula empresa».
_TIPOS_ARTS60 = {"0000I", "A11I", "A1TI", "A83CA", "CA", "PCT", "IDI"}

# Fallback cuando no podemos resolver categoría — se usa el nodo
# genérico de ayudas de investigación y transferencia.
_PREFIJO_FALLBACK_INV = "ait"


# Overrides para mapear `estudio` (de estudios.xlsx) al identificador
# del nodo del árbol de actividades cuando no se puede deducir por
# nombre (idioma distinto entre el dato y la spec del árbol, o el
# nodo no existe todavía y se usa un genérico de la Escuela de
# Doctorado). Las claves son los códigos de `estudios.xlsx`.
_DOCTORADO_OVERRIDES: dict[int, str] = {
    # Catalán: "Programa de Doctorat en Diseño, Gestión..." — el
    # nodo del árbol está en español como "Diseño, Gestión y
    # Evaluación de Políticas Públicas de Bienestar Social" → doc-dgpeb.
    90167: "doc-dgpeb",
    # "Estudios Interdisciplinarios" (estudios.xlsx) vs
    # "Estudios Interdisciplinares" (árbol) → mismo programa.
    90171: "doc-eig",
    # Doctorado Europeo Conjunto Marie Sklodowska-Curie: no hay nodo
    # específico en el árbol; cae en la Escuela de Doctorado.
    90184: "dag-escuela-doctorado",
    90191: "dag-escuela-doctorado",
}


def _normaliza_nombre(s: str) -> str:
    """Normaliza un nombre para comparación: minúsculas, sin
    acentos ni espacios extra."""
    import unicodedata
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.lower().strip()


def _doctorado_id_por_nombre_arbol(ruta_base: Path) -> dict[str, str]:
    """Lee `actividades.tree` y construye {nombre_normalizado_programa:
    identificador_nodo} para los hijos del nodo ``doctorado``.

    Devuelve dict vacío si el árbol no existe o no se puede parsear.
    """
    import re
    path = ruta_base / "entrada" / "estructuras" / "actividades.tree"
    if not path.exists():
        return {}
    try:
        from coana.util.arbol import Árbol
        arbol = Árbol.from_file(path)
        # Comprobar que existe el nodo doctorado
        if "doctorado" not in arbol._por_id:
            return {}
        mapping: dict[str, str] = {}
        for hijo in arbol.hijos("doctorado"):
            # Esperamos descripciones del tipo
            # "Programa de doctorado en Historia del Arte"
            m = re.match(
                r"(?i)Programa de [Dd]octorad[oa]?t? en (.+?)\s*$",
                hijo.descripción,
            )
            if m:
                mapping[_normaliza_nombre(m.group(1))] = hijo.identificador
        return mapping
    except Exception as e:
        log.warning("No se pudo leer árbol de actividades: %s", e)
        return {}


def _doctorados_por_alumno(ruta_base: Path) -> pl.DataFrame | None:
    """Devuelve (per_id_alumno, actividad_doctorado, nombre_doctorado)
    cruzando ``tesis.xlsx`` con ``docencia/estudios.xlsx`` y con el
    árbol de actividades.

    Para cada tesis:
    1. Si `estudio` está en :data:`_DOCTORADO_OVERRIDES` → se usa el
       identificador del nodo configurado manualmente.
    2. Si el nombre del estudio matchea (normalizado) con la
       descripción de algún hijo del nodo `doctorado` del árbol → se
       usa el identificador de ese hijo (p. ej. ``doc-ha``).
    3. Si no, fallback a ``doctorado-{estudio}`` con warning.
    """
    import re
    path_tesis = ruta_base / "entrada" / "investigación" / "tesis.xlsx"
    if not path_tesis.exists():
        return None

    from coana.util import read_excel
    df = read_excel(path_tesis)
    if (
        df.is_empty()
        or "per_id_alumno" not in df.columns
        or "estudio" not in df.columns
    ):
        return None

    base = df.select(
        pl.col("per_id_alumno").cast(pl.Int64),
        pl.col("estudio").cast(pl.Int64, strict=False),
    ).unique(subset=["per_id_alumno"], keep="first")

    # Enriquecer con el nombre del programa.
    path_estudios = ruta_base / "entrada" / "docencia" / "estudios.xlsx"
    if path_estudios.exists():
        estudios = read_excel(path_estudios)
        if (
            not estudios.is_empty()
            and "estudio" in estudios.columns
            and "nombre" in estudios.columns
        ):
            base = base.join(
                estudios.select(
                    pl.col("estudio").cast(pl.Int64, strict=False),
                    pl.col("nombre").cast(pl.Utf8).alias("nombre_doctorado"),
                ),
                on="estudio",
                how="left",
            )
        else:
            base = base.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias("nombre_doctorado"),
            )
    else:
        base = base.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("nombre_doctorado"),
        )

    # Mapping nombre_normalizado → identificador del nodo del árbol.
    nombre_a_id = _doctorado_id_por_nombre_arbol(ruta_base)

    # Resolver el identificador por fila aplicando la jerarquía:
    # override → árbol → fallback.
    def _resolver(estudio: int | None, nombre: str | None) -> str | None:
        if estudio is None:
            return None
        if estudio in _DOCTORADO_OVERRIDES:
            return _DOCTORADO_OVERRIDES[estudio]
        if nombre and nombre_a_id:
            m = re.match(
                r"(?i)Programa de [Dd]octorad[oa]?t? en (.+?)\s*$",
                nombre,
            )
            if m:
                clave = _normaliza_nombre(m.group(1))
                ident = nombre_a_id.get(clave)
                if ident:
                    return ident
        # Fallback: doctorado-{estudio} (avisa para que se añada al
        # override o se complete el árbol).
        log.warning(
            "Tesis con estudio=%s (%s) sin nodo en árbol de doctorado; "
            "usando fallback doctorado-%s",
            estudio, nombre, estudio,
        )
        return f"doctorado-{estudio}"

    return base.with_columns(
        pl.struct(["estudio", "nombre_doctorado"])
        .map_elements(
            lambda s: _resolver(s["estudio"], s["nombre_doctorado"]),
            return_dtype=pl.Utf8,
        )
        .alias("actividad_doctorado"),
    ).select(
        "per_id_alumno", "actividad_doctorado", "nombre_doctorado",
    )


def _actividades_por_proyecto(ruta_base: Path) -> pl.DataFrame | None:
    """Devuelve (proyecto, tipo_proyecto, actividad) — la actividad
    se construye como `{prefijo}-{proyecto}` aplicando las reglas
    simplificadas del traductor de presupuesto sobre `tipo_proyecto`.

    Si el tipo no se reconoce, se asigna ``ait-{proyecto}`` como
    fallback (nodo de ayudas de investigación y transferencia
    genérico).
    """
    path = ruta_base / "entrada" / "presupuesto" / "proyectos.xlsx"
    if not path.exists():
        log.warning("No existe %s; no se resolverán actividades de proyectos", path)
        return None

    from coana.util import read_excel
    df = read_excel(path)
    if df.is_empty() or "proyecto" not in df.columns or "tipo" not in df.columns:
        return None

    es_arts60 = pl.col("tipo").is_in(list(_TIPOS_ARTS60))
    es_cátedra = (
        pl.col("nombre").cast(pl.Utf8)
        .str.contains(r"(?i)c.tedra|aula empresa")
        .fill_null(False)
        if "nombre" in df.columns else pl.lit(False)
    )

    prefijo = (
        pl.when(es_arts60 & es_cátedra)
        .then(pl.lit("cátedras-aulas-empresa"))
        .when(es_arts60)
        .then(pl.lit("transf-60"))
        .otherwise(
            pl.col("tipo").replace_strict(
                _TIPO_PROYECTO_PREFIJO_AI,
                default=_PREFIJO_FALLBACK_INV,
                return_dtype=pl.Utf8,
            )
        )
    )

    actividad = pl.concat_str([prefijo, pl.lit("-"), pl.col("proyecto").cast(pl.Utf8)])

    return df.select(
        pl.col("proyecto").cast(pl.Utf8),
        pl.col("tipo").alias("tipo_proyecto"),
        actividad.alias("actividad"),
    )


def generar_distribución_investigación(
    detalle: pl.DataFrame | None = None,
    ruta_base: Path | None = None,
    año: int = 2025,
) -> pl.DataFrame:
    """Distribución porcentual de horas de investigación por
    (per_id, actividad).

    Por cada persona se resuelve la actividad de cada registro de
    detalle (proyectos, tesis, grupos) y se acumulan las horas; el
    porcentaje es ``horas_actividad / horas_totales_persona``.

    Parameters
    ----------
    detalle : DataFrame opcional con la salida de
        :func:`consolidar_dedicacion_investigacion`. Si se pasa, no
        se lee de disco. Útil para el orquestador, que ya lo tiene
        en memoria.
    ruta_base : alternativa a ``detalle``: se lee
        ``fase1/auxiliares/investigación/detalle_investigacion.parquet``
        de la ruta dada.
    año : sin uso aquí; se acepta por simetría con el resto del
        módulo.

    Returns
    -------
    DataFrame con (per_id, actividad, horas, horas_totales,
    porcentaje). Los porcentajes de una persona suman ~100%, salvo
    redondeo y registros sin actividad resuelta (que se omiten de la
    suma de actividad pero permanecen en ``horas_totales`` para que
    el porcentaje refleje la dedicación real).

    Notas
    -----
    - Para tesis se usa una actividad placeholder hasta que se
      mapee el programa de doctorado (TODO en la spec).
    - Para coordinación de grupos se usa ``dag-inves`` (transitorio,
      hasta que el árbol de actividades contemple nodos por grupo).
    - Para proyectos se aplica :func:`_resolver_actividad_proyecto`
      (placeholder; TODO integrar reglas de proyectos de
      transferencia/investigación).
    - En esta anualidad solo se calculan porcentajes — ``importe``
      en euros entrará al integrar regla 23 y datos retributivos.
    """
    del año  # parámetro reservado para futura simetría con otras fns

    if detalle is None:
        if ruta_base is None:
            raise ValueError(
                "Hay que pasar `detalle` o `ruta_base` para localizar "
                "detalle_investigacion.parquet",
            )
        path = (
            ruta_base / "fase1" / "auxiliares" / "investigación"
            / "detalle_investigacion.parquet"
        )
        if not path.exists():
            log.warning(
                "No existe %s; no se genera UC de investigación", path,
            )
            return _empty(_SCHEMA_UC_INVESTIGACION)
        detalle = pl.read_parquet(path)

    if detalle.is_empty():
        return _empty(_SCHEMA_UC_INVESTIGACION)

    # Resolver la actividad de cada registro de tipo "proyectos"
    # mediante join con la tabla `proyectos.xlsx`. Para tesis se
    # cruza per_id_alumno con `tesis.xlsx` → `estudios.xlsx` y se
    # genera ``doctorado-{estudio}``. Grupos sigue usando el nodo
    # transitorio (TODO en la spec).
    if ruta_base is not None:
        actividades_proy = _actividades_por_proyecto(ruta_base)
        doctorados = _doctorados_por_alumno(ruta_base)
    else:
        actividades_proy = None
        doctorados = None

    detalle_con_act = detalle.clone()
    if actividades_proy is not None:
        es_proy = pl.col("tipo") == "proyectos"
        detalle_con_act = detalle_con_act.with_columns(
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
        detalle_con_act = detalle_con_act.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("_actividad_proy"),
        )

    if doctorados is not None:
        es_tesis = pl.col("tipo") == "tesis"
        detalle_con_act = detalle_con_act.with_columns(
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
        detalle_con_act = detalle_con_act.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("_actividad_doctorado"),
        )

    actividad_expr = (
        pl.when(pl.col("tipo") == "grupos")
        .then(pl.lit("dag-inves"))  # transitorio (TODO de la spec)
        .when(pl.col("tipo") == "tesis")
        .then(
            pl.coalesce([
                pl.col("_actividad_doctorado"),
                # Fallback: tesis sin `estudio` en tesis.xlsx.
                pl.lit("doctorado-sin-programa"),
            ])
        )
        .when(pl.col("tipo") == "proyectos")
        .then(
            pl.coalesce([
                pl.col("_actividad_proy"),
                # Proyecto no presente en proyectos.xlsx → fallback
                # genérico ait-{proyecto}.
                pl.concat_str([
                    pl.lit(f"{_PREFIJO_FALLBACK_INV}-"),
                    pl.col("identificador").cast(pl.Utf8),
                ]),
            ])
        )
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )

    detalle_con_act = detalle_con_act.with_columns(
        actividad_expr.alias("actividad"),
    ).drop(
        ["_actividad_proy", "_proy", "_actividad_doctorado", "_alumno"],
        strict=False,
    )

    # Total de horas por persona — incluye TODOS los registros (los
    # que no resuelven actividad también suman al denominador, para
    # que los porcentajes no se inflen artificialmente).
    totales = detalle_con_act.group_by("per_id").agg(
        pl.col("horas").sum().alias("horas_totales"),
    )

    # Suma de horas por (per_id, actividad), omitiendo registros sin
    # actividad resuelta.
    con_actividad = detalle_con_act.filter(pl.col("actividad").is_not_null())
    if con_actividad.is_empty():
        return _empty(_SCHEMA_UC_INVESTIGACION)

    uc = (
        con_actividad.group_by("per_id", "actividad")
        .agg(pl.col("horas").sum().alias("horas"))
        .join(totales, on="per_id", how="left")
        .with_columns(
            (
                pl.col("horas") / pl.col("horas_totales") * 100.0
            ).alias("porcentaje"),
        )
        .select(["per_id", "actividad", "horas", "horas_totales", "porcentaje"])
        .sort("per_id", "porcentaje", descending=[False, True])
    )

    log.info(
        "UC de investigación: %d pares (per_id, actividad) sobre "
        "%d personas",
        uc.height, uc["per_id"].n_unique(),
    )
    return uc
