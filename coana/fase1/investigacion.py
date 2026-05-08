"""Módulo de cálculo de dedicación a investigación.

Procesa información de:
- Coordinación de grupos de investigación
- Dirección/codirección/tutoría de tesis doctorales
- Participación en proyectos y contratos de investigación

Genera parquets con resumen por persona y detalle de registros.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import polars as pl

log = logging.getLogger(__name__)


def _semanas_en_año(fecha_inicio: pl.Expr, fecha_fin: pl.Expr, año: int) -> pl.Expr:
    """Calcula número de semanas de vigencia dentro del año especificado.
    
    Args:
        fecha_inicio: Expresión de Polars con fecha de inicio
        fecha_fin: Expresión de Polars con fecha de fin
        año: Año de referencia (ej: 2025)
    
    Returns:
        Expresión que calcula las semanas de intersección con el año
    """
    inicio_año = pl.lit(datetime(año, 1, 1))
    fin_año = pl.lit(datetime(año, 12, 31))
    
    # Intersección del rango [fecha_inicio, fecha_fin] con el año
    inicio_efectivo = pl.max_horizontal(fecha_inicio, inicio_año)
    fin_efectivo = pl.min_horizontal(fecha_fin, fin_año)
    
    # Días de diferencia (si fin < inicio, será negativo → lo convertimos a 0)
    dias = (fin_efectivo - inicio_efectivo).dt.total_days() + 1
    dias_validos = pl.when(dias > 0).then(dias).otherwise(0)
    
    # Convertir a semanas (redondeo hacia arriba)
    return (dias_validos / 7.0).ceil().cast(pl.Int32)


def calcular_horas_grupos_investigacion(ruta_base: Path, año: int = 2025) -> pl.DataFrame:
    """Calcula horas de investigación por coordinación de grupos.
    
    Lee `investigadores en grupos.xlsx`, filtra coordinadores vigentes en el año,
    y calcula 2 horas/semana.
    
    Returns:
        DataFrame con columnas: per_id, grupo_id, grupo_nombre, semanas, horas, tipo
    """
    path = ruta_base / "entrada" / "investigación" / "investigadores en grupos.xlsx"
    if not path.exists():
        log.warning(f"No existe {path}")
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "grupo_id": pl.Utf8,
            "grupo_nombre": pl.Utf8,
            "semanas": pl.Int32,
            "horas": pl.Float64,
            "tipo": pl.Utf8,
        })
    
    from coana.util import read_excel
    df = read_excel(path)
    
    # Eliminar duplicados por per_id y grupo
    # (una persona puede aparecer varias veces como coordinadora del mismo grupo)
    grupo_cols = []
    if "grupo" in df.columns:
        grupo_cols.append("grupo")
    if "per_id" in df.columns:
        if grupo_cols:
            df = df.unique(subset=["per_id"] + grupo_cols, keep="first")
    
    # Filtrar coordinadores
    if "coordinador" not in df.columns:
        log.warning("No hay columna 'coordinador' en investigadores en grupos")
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "grupo_id": pl.Utf8,
            "grupo_nombre": pl.Utf8,
            "semanas": pl.Int32,
            "horas": pl.Float64,
            "tipo": pl.Utf8,
        })
    
    df = df.filter(pl.col("coordinador") == "S")
    
    # Calcular semanas vigentes en el año
    if "fecha_alta" not in df.columns or "fecha_baja" not in df.columns:
        log.warning("Faltan columnas de fechas en investigadores en grupos")
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "grupo_id": pl.Utf8,
            "grupo_nombre": pl.Utf8,
            "semanas": pl.Int32,
            "horas": pl.Float64,
            "tipo": pl.Utf8,
        })
    
    # Rellenar fecha_baja nula con fin del año
    df = df.with_columns(
        pl.col("fecha_baja").fill_null(datetime(año, 12, 31))
    )
    
    df = df.with_columns(
        _semanas_en_año(pl.col("fecha_alta"), pl.col("fecha_baja"), año).alias("semanas")
    )
    
    # Filtrar solo los que tienen semanas > 0
    df = df.filter(pl.col("semanas") > 0)
    
    # Calcular horas: 2 por semana
    df = df.with_columns(
        (pl.col("semanas") * 2.0).alias("horas"),
        pl.lit("grupos").alias("tipo"),
    )
    
    # Renombrar columnas de fechas para consistencia
    df = df.rename({"fecha_alta": "fecha_inicio", "fecha_baja": "fecha_fin"})
    
    # Seleccionar columnas relevantes
    cols = ["per_id", "semanas", "horas", "tipo", "fecha_inicio", "fecha_fin"]
    
    # Añadir información del grupo si existe
    if "grupo" in df.columns:
        df = df.rename({"grupo": "grupo_id"})
        cols.insert(1, "grupo_id")
    else:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("grupo_id"))
        cols.insert(1, "grupo_id")
    
    if "nombre_grupo" in df.columns:
        df = df.rename({"nombre_grupo": "grupo_nombre"})
        cols.insert(2, "grupo_nombre")
    elif "nombre" in df.columns:
        df = df.rename({"nombre": "grupo_nombre"})
        cols.insert(2, "grupo_nombre")
    else:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("grupo_nombre"))
        cols.insert(2, "grupo_nombre")
    
    return df.select(cols)


def calcular_horas_tesis(ruta_base: Path, año: int = 2025) -> pl.DataFrame:
    """Calcula horas de investigación por dirección/tutoría de tesis.
    
    Lee `tesis.xlsx`, busca per_id en director/codirector/codirector2/tutor,
    y calcula horas según rol.
    
    Returns:
        DataFrame con columnas: per_id, alumno_per_id, rol, semanas, horas, tipo
    """
    path = ruta_base / "entrada" / "investigación" / "tesis.xlsx"
    if not path.exists():
        log.warning(f"No existe {path}")
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "alumno_per_id": pl.Int64,
            "rol": pl.Utf8,
            "semanas": pl.Int32,
            "horas": pl.Float64,
            "tipo": pl.Utf8,
        })
    
    from coana.util import read_excel
    df = read_excel(path)
    
    # Filtrar tesis dadas de baja (estado == "B")
    if "estado" in df.columns:
        df = df.filter(pl.col("estado") != "B")
    
    # Forzar tipo Int64 en todas las columnas per_id para evitar problemas de esquema
    per_id_cols = [col for col in df.columns if "per_id" in col.lower()]
    for col in per_id_cols:
        df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))
    
    # Necesitamos las fechas
    if "fecha_inicio_tiempo" not in df.columns:
        log.warning("No hay columna 'fecha_inicio_tiempo'")
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "alumno_per_id": pl.Int64,
            "rol": pl.Utf8,
            "semanas": pl.Int32,
            "horas": pl.Float64,
            "tipo": pl.Utf8,
        })
    
    # Usamos fecha_fin_tiempo como fecha de fin
    if "fecha_fin_tiempo" not in df.columns:
        log.warning("No hay columna 'fecha_fin_tiempo' en tesis")
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "alumno_per_id": pl.Int64,
            "rol": pl.Utf8,
            "semanas": pl.Int32,
            "horas": pl.Float64,
            "tipo": pl.Utf8,
        })
    
    # Rellenar fecha_fin_tiempo nula con fin de año
    df = df.with_columns(
        pl.col("fecha_fin_tiempo").fill_null(datetime(año, 12, 31)).alias("fecha_fin_efectiva")
    )
    
    # Identificar las columnas de per_id relacionados
    roles = []
    if "per_id_director" in df.columns:
        roles.append(("per_id_director", "director"))
    if "per_id_codirector" in df.columns:
        roles.append(("per_id_codirector", "codirector"))
    if "per_id_codirector2" in df.columns:
        roles.append(("per_id_codirector2", "codirector2"))
    if "per_id_tutor" in df.columns:
        roles.append(("per_id_tutor", "tutor"))
    
    if not roles:
        log.warning("No hay columnas de per_id de participantes en tesis")
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "alumno_per_id": pl.Int64,
            "rol": pl.Utf8,
            "semanas": pl.Int32,
            "horas": pl.Float64,
            "tipo": pl.Utf8,
        })
    
    # Calcular número de directores/codirectores por tesis
    directores_cols = [col for col, rol in roles if rol != "tutor"]
    if directores_cols:
        df = df.with_columns(
            pl.sum_horizontal([
                pl.when(pl.col(c).is_not_null()).then(1).otherwise(0)
                for c in directores_cols
            ]).alias("n_directores")
        )
    else:
        df = df.with_columns(pl.lit(1).alias("n_directores"))
    
    # Crear un registro por cada rol
    partes = []
    for col_per_id, rol in roles:
        sub = df.filter(pl.col(col_per_id).is_not_null()).select([
            pl.col(col_per_id).alias("per_id"),
            pl.col("per_id_alumno").alias("alumno_per_id") if "per_id_alumno" in df.columns 
                else pl.lit(None).cast(pl.Int64).alias("alumno_per_id"),
            pl.lit(rol).alias("rol"),
            pl.col("fecha_inicio_tiempo"),
            pl.col("fecha_fin_efectiva"),
            pl.col("n_directores"),
        ])
        
        # Calcular semanas
        sub = sub.with_columns(
            _semanas_en_año(
                pl.col("fecha_inicio_tiempo"),
                pl.col("fecha_fin_efectiva"),
                año
            ).alias("semanas")
        )
        
        # Calcular horas según rol
        if rol == "tutor":
            # Tutor: 2 horas/semana
            sub = sub.with_columns(
                (pl.col("semanas") * 2.0).alias("horas")
            )
        else:
            # Director/codirector: 2 horas/semana repartidas entre participantes
            sub = sub.with_columns(
                (pl.col("semanas") * 2.0 / pl.col("n_directores")).alias("horas")
            )
        
        partes.append(sub)
    
    if not partes:
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "alumno_per_id": pl.Int64,
            "rol": pl.Utf8,
            "semanas": pl.Int32,
            "horas": pl.Float64,
            "tipo": pl.Utf8,
        })
    
    resultado = pl.concat(partes, how="diagonal")
    
    # Filtrar solo registros con semanas > 0
    resultado = resultado.filter(pl.col("semanas") > 0)
    
    # Añadir tipo y renombrar fechas para consistencia
    resultado = resultado.with_columns(pl.lit("tesis").alias("tipo"))
    resultado = resultado.rename({
        "fecha_inicio_tiempo": "fecha_inicio",
        "fecha_fin_efectiva": "fecha_fin"
    })
    
    return resultado.select(["per_id", "alumno_per_id", "rol", "semanas", "horas", "tipo", "fecha_inicio", "fecha_fin"])


def calcular_horas_proyectos(ruta_base: Path, año: int = 2025) -> pl.DataFrame:
    """Calcula horas de investigación por participación en proyectos.
    
    Jerarquía:
    1. Horas registradas en Kalendas
    2. Valores no declarados (con tabla de horas por tipo de anexo)
    
    Returns:
        DataFrame con columnas: per_id, contrato, anexo, semanas, horas, origen, tipo
    """
    path_inv = ruta_base / "entrada" / "investigación" / "investigadores en contratos.xlsx"
    if not path_inv.exists():
        log.warning(f"No existe {path_inv}")
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "contrato": pl.Utf8,
            "anexo": pl.Utf8,
            "semanas": pl.Int32,
            "horas": pl.Float64,
            "origen": pl.Utf8,
            "tipo": pl.Utf8,
        })
    
    from coana.util import read_excel
    df_inv = read_excel(path_inv)
    
    # Convertir columnas de identificadores de proyecto/contrato a String
    if "contrato" in df_inv.columns:
        df_inv = df_inv.with_columns(pl.col("contrato").cast(pl.Utf8))
    if "proyecto" in df_inv.columns:
        df_inv = df_inv.with_columns(pl.col("proyecto").cast(pl.Utf8))
    
    # 1. Cargar horas de Kalendas
    path_kalendas = ruta_base / "entrada" / "investigación" / "horas kalendas.xlsx"
    horas_kalendas = None
    if path_kalendas.exists():
        horas_kalendas = read_excel(path_kalendas)
        # Convertir columnas de proyecto/contrato a String
        if "contrato" in horas_kalendas.columns:
            horas_kalendas = horas_kalendas.with_columns(pl.col("contrato").cast(pl.Utf8))
        if "proyecto" in horas_kalendas.columns:
            horas_kalendas = horas_kalendas.with_columns(pl.col("proyecto").cast(pl.Utf8))
        # Filtrar por año 2025 en fecha_validación
        if "fecha_validación" in horas_kalendas.columns:
            horas_kalendas = horas_kalendas.filter(
                pl.col("fecha_validación").dt.year() == año
            )
    
    # 2. Cargar tipos de anexo
    path_tipos = ruta_base / "entrada" / "investigación" / "Tipos Anexo.xlsx"
    tipos_anexo = None
    if path_tipos.exists():
        tipos_anexo = read_excel(path_tipos)
    
    # 3. Cargar info de proyectos
    path_proy = ruta_base / "entrada" / "investigación" / "proyectos en contratos investigación.xlsx"
    proyectos = None
    if path_proy.exists():
        proyectos = read_excel(path_proy)
        # Convertir columnas de proyecto/contrato a String
        if "contrato" in proyectos.columns:
            proyectos = proyectos.with_columns(pl.col("contrato").cast(pl.Utf8))
        if "proyecto" in proyectos.columns:
            proyectos = proyectos.with_columns(pl.col("proyecto").cast(pl.Utf8))
    
    resultados = []
    
    # Procesar cada investigador-contrato
    for row in df_inv.iter_rows(named=True):
        per_id = row.get("per_id")
        contrato = row.get("contrato") or row.get("proyecto")
        
        if per_id is None or contrato is None:
            continue
        
        # Determinar el anexo (tipo de proyecto)
        anexo = None
        if "anexo" in row:
            anexo = row["anexo"]
        elif "tipo" in row:
            anexo = row["tipo"]
        elif "subtipo" in row:
            anexo = row["subtipo"]
        
        # Opción 1: Horas de Kalendas
        horas_from_kalendas = 0.0
        if horas_kalendas is not None and "per_id" in horas_kalendas.columns:
            # Buscar registros de este investigador en este proyecto en año 2025
            col_proy = "proyecto" if "proyecto" in horas_kalendas.columns else "contrato"
            if col_proy in horas_kalendas.columns:
                registros = horas_kalendas.filter(
                    (pl.col("per_id") == per_id) & 
                    (pl.col(col_proy) == contrato)
                )
                if not registros.is_empty() and "horas" in registros.columns:
                    horas_from_kalendas = float(registros["horas"].sum() or 0)
        
        if horas_from_kalendas > 0:
            # Usamos horas de Kalendas (sin fechas disponibles)
            resultados.append({
                "per_id": per_id,
                "contrato": contrato,
                "anexo": anexo,
                "semanas": None,  # No aplicable
                "horas": horas_from_kalendas,
                "origen": "Kalendas",
                "tipo": "proyectos",
                "fecha_inicio": None,
                "fecha_fin": None,
            })
            continue
        
        # Opción 2: Valores no declarados
        # Determinar fechas de participación
        fecha_inicio = None
        fecha_fin = None
        
        # Prioridad: fecha_inicio_solicitud_alternativa
        if "fecha_inicio_solicitud_alternativa" in row and row["fecha_inicio_solicitud_alternativa"] is not None:
            fecha_inicio = row["fecha_inicio_solicitud_alternativa"]
            if "fecha_fin_solicitud_alternativa" in row:
                fecha_fin = row["fecha_fin_solicitud_alternativa"]
        elif "fecha_inicio_solicitud" in row and row["fecha_inicio_solicitud"] is not None:
            fecha_inicio = row["fecha_inicio_solicitud"]
            if "fecha_fin_solicitud" in row:
                fecha_fin = row["fecha_fin_solicitud"]
        else:
            # Usar fechas del proyecto
            if proyectos is not None and "proyecto" in proyectos.columns:
                col_proy_ref = "proyecto" if "proyecto" in proyectos.columns else "contrato"
                proy_info = proyectos.filter(pl.col(col_proy_ref) == contrato)
                if not proy_info.is_empty():
                    if "fecha_inicio" in proy_info.columns:
                        fecha_inicio = proy_info.row(0, named=True).get("fecha_inicio")
                    if "fecha_fin" in proy_info.columns:
                        fecha_fin = proy_info.row(0, named=True).get("fecha_fin")
        
        if fecha_inicio is None:
            continue
        
        if fecha_fin is None:
            fecha_fin = datetime(año, 12, 31)
        
        # Calcular semanas
        if isinstance(fecha_inicio, datetime) and isinstance(fecha_fin, datetime):
            inicio_año = datetime(año, 1, 1)
            fin_año = datetime(año, 12, 31)
            inicio_efectivo = max(fecha_inicio, inicio_año)
            fin_efectivo = min(fecha_fin, fin_año)
            
            if fin_efectivo >= inicio_efectivo:
                dias = (fin_efectivo - inicio_efectivo).days + 1
                semanas = int((dias / 7.0) + 0.5)  # Redondeo
            else:
                semanas = 0
        else:
            semanas = 0
        
        if semanas == 0:
            continue
        
        # Determinar horas por semana según tipo de anexo
        horas_semana = None
        if "horas" in row and row["horas"] is not None:
            # Hay horas explícitas en el registro
            horas_total = float(row["horas"])
            horas_semana = horas_total / semanas if semanas > 0 else 0
        elif tipos_anexo is not None and anexo is not None:
            # Buscar en la tabla de tipos
            # La primera columna es la concatenación de tipo, subtipo y microtipo
            # Necesitamos buscar por el anexo
            tipo_row = tipos_anexo.filter(
                pl.col(tipos_anexo.columns[0]).cast(pl.Utf8).str.strip_chars() == str(anexo).strip()
            )
            if not tipo_row.is_empty() and len(tipo_row.columns) >= 3:
                # La tercera columna tiene las horas/semana
                horas_semana = tipo_row.row(0)[2]
        
        if horas_semana is None:
            # Valor por defecto si no se encuentra
            horas_semana = 6.0
        
        horas_total = semanas * horas_semana
        
        resultados.append({
            "per_id": per_id,
            "contrato": contrato,
            "anexo": anexo,
            "semanas": semanas,
            "horas": horas_total,
            "origen": "Tabla tipos anexo" if tipos_anexo is not None else "Estimación",
            "tipo": "proyectos",
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
        })
    
    if not resultados:
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "contrato": pl.Utf8,
            "anexo": pl.Utf8,
            "semanas": pl.Int32,
            "horas": pl.Float64,
            "origen": pl.Utf8,
            "tipo": pl.Utf8,
        })
    
    return pl.DataFrame(resultados)


def consolidar_dedicacion_investigacion(
    ruta_base: Path, 
    año: int = 2025
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Consolida las tres fuentes de dedicación a investigación.
    
    Returns:
        Tuple con:
        - resumen: DataFrame con totales por persona
        - detalle: DataFrame con todos los registros individuales
    """
    log.info("Calculando horas de grupos de investigación...")
    grupos = calcular_horas_grupos_investigacion(ruta_base, año)
    
    log.info("Calculando horas de tesis...")
    tesis = calcular_horas_tesis(ruta_base, año)
    
    log.info("Calculando horas de proyectos...")
    proyectos = calcular_horas_proyectos(ruta_base, año)
    
    # Crear detalle consolidado
    partes_detalle = []
    
    if not grupos.is_empty():
        det_grupos = grupos.select([
            pl.col("per_id"),
            pl.lit("grupos").alias("tipo"),
            pl.col("grupo_id").alias("identificador"),
            pl.col("grupo_nombre").alias("descripción"),
            pl.col("semanas"),
            pl.col("horas"),
            pl.lit("Coordinación grupo").alias("origen"),
            pl.col("fecha_inicio").cast(pl.Datetime),
            pl.col("fecha_fin").cast(pl.Datetime),
        ])
        partes_detalle.append(det_grupos)
    
    if not tesis.is_empty():
        det_tesis = tesis.select([
            pl.col("per_id"),
            pl.lit("tesis").alias("tipo"),
            pl.col("alumno_per_id").cast(pl.Utf8).alias("identificador"),
            pl.col("rol").alias("descripción"),
            pl.col("semanas"),
            pl.col("horas"),
            pl.lit("Dirección/tutoría tesis").alias("origen"),
            pl.col("fecha_inicio").cast(pl.Datetime),
            pl.col("fecha_fin").cast(pl.Datetime),
        ])
        partes_detalle.append(det_tesis)
    
    if not proyectos.is_empty():
        det_proy = proyectos.select([
            pl.col("per_id"),
            pl.lit("proyectos").alias("tipo"),
            pl.col("contrato").alias("identificador"),
            pl.col("anexo").alias("descripción"),
            pl.col("semanas"),
            pl.col("horas"),
            pl.col("origen"),
            pl.col("fecha_inicio").cast(pl.Datetime),
            pl.col("fecha_fin").cast(pl.Datetime),
        ])
        partes_detalle.append(det_proy)
    
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
    
    # Crear resumen por persona
    if not detalle.is_empty():
        resumen = detalle.group_by("per_id").agg([
            pl.col("horas").sum().alias("horas_totales"),
            pl.when(pl.col("tipo") == "grupos").then(pl.col("semanas")).otherwise(0).sum().alias("semanas_grupos"),
            pl.when(pl.col("tipo") == "tesis").then(pl.col("semanas")).otherwise(0).sum().alias("semanas_tesis"),
            pl.when(pl.col("tipo") == "proyectos").then(pl.col("semanas")).otherwise(0).sum().alias("semanas_proyectos"),
            pl.when(pl.col("tipo") == "grupos").then(pl.col("horas")).otherwise(0).sum().alias("horas_grupos"),
            pl.when(pl.col("tipo") == "tesis").then(pl.col("horas")).otherwise(0).sum().alias("horas_tesis"),
            pl.when(pl.col("tipo") == "proyectos").then(pl.col("horas")).otherwise(0).sum().alias("horas_proyectos"),
        ])
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
    
    log.info(f"Procesadas {resumen.height} personas con dedicación a investigación")
    log.info(f"Total registros de detalle: {detalle.height}")
    
    return resumen, detalle
