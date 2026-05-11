"""Preprocesamiento de nóminas: agrupación por expediente y sector."""

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from coana.fase1.clasificador_actividades import (
    clasificar_actividades,
    enriquecer_para_actividades,
)
from coana.fase1.clasificador_centros_coste import (
    _SERVICIO_CC,
    _CENTRO_PLAZA_CC,
    PROYECTOS_ORDINARIOS,
    clasificar_centros_coste,
)
from coana.fase1.nóminas.contexto import ContextoNóminas
from coana.fase1.nóminas.regla_23 import (
    generar_asignaturas_sin_titulación,
    generar_atrasos_y_apartados,
    generar_dedicación_docente,
    generar_dedicación_estudios,
    generar_dedicación_titulaciones,
    generar_estructura_estudios_titulaciones,
    generar_horas_no_oficiales,
    generar_pod_resuelto,
    generar_uc_cargos,
    generar_uc_despidos,
    generar_uc_indemnizaciones_asistencias,
)

# Mapeo de sectores codificados a nombres usados en el modelo.
_MAPEO_SECTOR = {"PAS": "PTGAS", "PI": "PVI"}
# Sectores reconocidos (tras mapeo).
_SECTORES_CONOCIDOS = {"PDI", "PTGAS", "PVI"}

# Prelación de sectores para asignar UC a expedientes cuando hay varios.
_PRELACIÓN_SECTOR = ["PTGAS", "PVI", "PDI", "Otros"]



@dataclass
class ResultadoNóminas:
    """Estadísticas del preprocesamiento de nóminas."""

    expedientes_por_sector: dict[str, int]
    importe_por_sector: dict[str, float]
    # UC generadas a partir de retribuciones ordinarias PTGAS.
    uc_ptgas: pl.DataFrame = field(default_factory=pl.DataFrame)
    # UC generadas a partir de retribuciones PVI.
    uc_pvi: pl.DataFrame = field(default_factory=pl.DataFrame)
    # UC generadas a partir de retribuciones PDI.
    uc_pdi: pl.DataFrame = field(default_factory=pl.DataFrame)
    # UC generadas a partir de despidos PDI/PVI (concepto 47).
    uc_despidos: pl.DataFrame = field(default_factory=pl.DataFrame)
    # UC generadas a partir de indemnizaciones por asistencias (concepto 48).
    uc_indemnizaciones_asistencias: pl.DataFrame = field(default_factory=pl.DataFrame)
    # UC generadas a partir de cargos PDI/PVI (conceptos 19, 64).
    uc_cargos: pl.DataFrame = field(default_factory=pl.DataFrame)


def _mapear_sector(expedientes: pl.DataFrame) -> pl.DataFrame:
    """Devuelve expedientes con columna 'sector_mapeado' (PTGAS/PVI/PDI/Otros)."""
    return expedientes.with_columns(
        pl.col("sector")
        .replace(_MAPEO_SECTOR)
        .fill_null("Otros")
        .alias("sector_mapeado"),
    ).with_columns(
        pl.when(pl.col("sector_mapeado").is_in(_SECTORES_CONOCIDOS))
        .then(pl.col("sector_mapeado"))
        .otherwise(pl.lit("Otros"))
        .alias("sector_mapeado"),
    )


# Mapeo categoría → XXX del elemento de coste PTGAS (ptgas-XXX-YYY).
# La excepción de FC + per_id 65214 → "dir" se trata en código.
# LF = Laboral Fijo, LT = Laboral Temporal, LE = Laboral Eventual
# (los dos últimos son no-fijos y comparten subtipo «labtemp» en el
# árbol de elementos de coste).
_PTGAS_CAT_XXX: dict[str, str] = {
    "FC": "func", "FI": "func", "E": "ev",
    "LF": "labfijo", "LT": "labtemp", "LE": "labtemp",
}

# Mapeo categoría → XXX del elemento de coste PDI (pdi-XXX-YYY).
# Sin default: una categoría no reconocida es error.
_PDI_CAT_XXX: dict[str, str] = {
    "CU": "cu",
    "TU": "tu", "TUI": "tu",
    "CEU": "ceu",
    "TEU": "teu",
    "AJ": "aj", "AJD": "aj", "AJDII": "aj",
    "PAA": "as", "PAL": "as",
    "PS": "ps",
    "PEME": "em",
    "PPL": "pl", "PPLV": "pl",
    "PVI": "pv",
    "PD": "pd",
    "PCD": "pcd",
    "PC": "pc",
}

# Mapeo concepto_retributivo → YYY del elemento de coste PTGAS.
# Refleja la tabla de la especificación (§ «Tabla para determinar parte del
# elemento de coste a partir del concepto retributivo»).
_PTGAS_CR_YYY: dict[str, str] = {
    "01": "sueldo",     "03": "trienios",   "04": "paga-extra", "05": "esp",
    "06": "esp",        "10": "dst",        "12": "dst",        "13": "otvars",
    "15": "esp",        "17": "otfij",      "18": "esp",        "19": "cargos",
    "20": "quin",       "24": "dst",        "25": "otvars",     "26": "sexinv",
    "30": "cargos",     "32": "prod",       "34": "otfij",      "35": "otvars",
    "43": "otvars",     "44": "trienios",   "47": "otvars",
    # CR 48 (indemnización por asistencias) NO está en esta tabla:
    # se imputa al elemento de coste fijo `otras-indemnizaciones` para
    # los tres sectores (PTGAS + PDI + PVI), vía
    # `generar_uc_indemnizaciones_asistencias`.
    "53": "otvars",
    "55": "otvars",     "56": "esp",        "57": "otfij",      "59": "dst",
    "62": "otvars",     "64": "otvars",     "67": "otvars",     "68": "esp",
    "70": "otvars",     "71": "esp",        "72": "trienios",   "75": "cprof",
    "76": "cprof",      "77": "sextransf",  "78": "otvars",     "80": "otvars",
    "82": "sueldo",     "83": "otvars",     "86": "quin",       "87": "otvars",
    "90": "otvars",     "98": "trienios",   "99": "quin",
}

# per_id de excepción: FC + este per_id → "dir" en vez de "func".
_PTGAS_PER_ID_DIR = 65214


def _elemento_coste_ptgas(categoría: str, concepto_retributivo: str, per_id: int | None) -> str | None:
    """Calcula el elemento de coste ptgas-XXX-YYY. Devuelve None si hay error."""
    cat = str(categoría).strip()
    cr = str(concepto_retributivo).strip()

    # XXX
    if cat == "FC" and per_id == _PTGAS_PER_ID_DIR:
        xxx = "dir"
    elif cat in _PTGAS_CAT_XXX:
        xxx = _PTGAS_CAT_XXX[cat]
    else:
        return None  # error

    # YYY
    yyy = _PTGAS_CR_YYY.get(cr)
    if yyy is None:
        return None  # error

    return f"ptgas-{xxx}-{yyy}"


def _xxx_pvi(
    categoría,
    perceptor,
    provisión,
    categoría_plaza=None,
    sector_plaza=None,
) -> str:
    """Calcula el XXX del elemento de coste PVI (piyotper-XXX-YYY).

    Reglas first-match-wins; la última absorbe el resto → `pid`.
    """
    def _s(v):
        return str(v).strip() if v is not None else ""
    per = _s(perceptor)
    cat = _s(categoría)
    prov = _s(provisión)
    cp = _s(categoría_plaza)
    sp = _s(sector_plaza)
    if prov == "P4":
        return "act"
    if per == "35":
        return "act"
    if cp in ("41J", "41S") and sp == "PI":
        return "act"
    if cat == "PREDO":
        return "pif"
    if prov == "PD":
        return "pif"
    if prov == "P2":
        return "idi"
    return "pid"


def _elemento_coste_pvi(
    categoría,
    perceptor,
    provisión,
    concepto_retributivo: str,
    categoría_plaza=None,
    sector_plaza=None,
) -> str | None:
    """Calcula el elemento de coste piyotper-XXX-YYY. Devuelve None si error."""
    cr = str(concepto_retributivo).strip()
    xxx = _xxx_pvi(categoría, perceptor, provisión, categoría_plaza, sector_plaza)
    yyy = _PTGAS_CR_YYY.get(cr)
    if yyy is None:
        return None
    # Excepción: el personal con contrato de actividades científico-técnicas
    # (XXX = "act") no tiene rama «cprof» (carrera profesional) en el árbol.
    # Los conceptos retributivos 75/76 que normalmente irían a `cprof` se
    # canalizan a `otvars` para esa categoría.
    if xxx == "act" and yyy == "cprof":
        yyy = "otvars"
    return f"piyotper-{xxx}-{yyy}"


def _elemento_coste_pdi(categoría: str, concepto_retributivo: str) -> str | None:
    """Calcula el elemento de coste pdi-XXX-YYY. Devuelve None si error."""
    cat = str(categoría).strip() if categoría is not None else ""
    cr = str(concepto_retributivo).strip()
    xxx = _PDI_CAT_XXX.get(cat)
    if xxx is None:
        return None
    yyy = _PTGAS_CR_YYY.get(cr)
    if yyy is None:
        return None
    return f"pdi-{xxx}-{yyy}"


# Acumulador global de etiquetas de elemento de coste que no existen en el
# árbol. Lo poblamos durante `preprocesar_nóminas` y, al final, si tiene
# contenido, se lanza un único `ValueError` con TODAS las etiquetas faltantes
# (en lugar de detener en la primera, que obligaría a iterar varias veces).
_etiquetas_ec_faltantes: dict[str, tuple[float, set[str]]] = {}


def _reset_etiquetas_ec_faltantes() -> None:
    _etiquetas_ec_faltantes.clear()


def _validar_etiquetas_ec(
    etiquetas_con_importe: dict[str, float],
    árbol_ec,
    contexto: str = "",
) -> None:
    """Registra etiquetas que no existen en el árbol de EC.

    No detiene el proceso aquí: las acumula en `_etiquetas_ec_faltantes`
    para que al final de `preprocesar_nóminas` se reporten todas juntas.
    Implementa el requisito de la spec: «cuando generas una etiqueta
    `ZZZ-XXX-YYY` has de comprobar que existe en el árbol de elementos
    de coste. Si no existe… error que sí detenga el proceso».
    """
    if árbol_ec is None:
        return
    for etq, imp in etiquetas_con_importe.items():
        if etq in árbol_ec._por_id:
            continue
        previo_imp, contextos = _etiquetas_ec_faltantes.get(etq, (0.0, set()))
        _etiquetas_ec_faltantes[etq] = (
            previo_imp + float(imp),
            contextos | ({contexto} if contexto else set()),
        )


def _detener_si_etiquetas_ec_faltantes() -> None:
    if not _etiquetas_ec_faltantes:
        return
    lineas = []
    for etq, (imp, contextos) in sorted(
        _etiquetas_ec_faltantes.items(), key=lambda x: -x[1][0]
    ):
        ctx = f"  [{', '.join(sorted(contextos))}]" if contextos else ""
        lineas.append(f"  · {etq}  ({imp:,.2f} €){ctx}")
    raise ValueError(
        "Etiquetas de elemento de coste no existentes en el árbol "
        "(añádelas a data/entrada/estructuras/elementos de coste.tree o "
        "ajusta las reglas):\n" + "\n".join(lineas)
    )


def _generar_uc_ptgas(
    nóminas_filtradas: pl.DataFrame,
    expedientes: pl.DataFrame,
    ctx_enriquecimiento=None,
    árbol_actividades=None,
    árbol_cc=None,
    árbol_ec=None,
    distribución_costes=None,
    obtener_descripciones=None,
) -> pl.DataFrame:
    """Genera UC a partir de retribuciones del PTGAS.

    El centro de coste de cada fila se obtiene con el clasificador de CC
    compartido (mismo usado en presupuesto). La actividad:
    - En ordinarias (proyecto en `PROYECTOS_ORDINARIOS`): se toma de la
      tabla servicio→actividad (`_SERVICIO_CC`, `_CENTRO_PLAZA_CC`).
    - En extras: del clasificador de actividades.
    """
    exp_ptgas = _mapear_sector(expedientes)
    exp_ptgas = exp_ptgas.filter(pl.col("sector_mapeado") == "PTGAS")
    if exp_ptgas.is_empty():
        return pl.DataFrame()

    registros = nóminas_filtradas.join(
        exp_ptgas.select("expediente", "per_id"),
        on="expediente",
        how="inner",
    )

    es_ss = pl.col("aplicación").cast(pl.Utf8).str.starts_with("12")
    es_cr_48 = pl.col("concepto_retributivo").cast(pl.Utf8) == "48"
    registros = registros.filter(~es_ss & ~es_cr_48)
    if registros.is_empty():
        return pl.DataFrame()

    # Elemento de coste por fila
    ecs = [
        _elemento_coste_ptgas(
            row["categoría"], row["concepto_retributivo"], row["per_id"],
        )
        for row in registros.select("categoría", "concepto_retributivo", "per_id").iter_rows(named=True)
    ]
    registros = registros.with_columns(pl.Series("_ec", ecs))
    errores = registros.filter(pl.col("_ec").is_null())
    if not errores.is_empty():
        n_err = len(errores)
        imp_err = float(errores["importe"].sum())
        print(f"    ⚠ {n_err:,} registros PTGAS sin elemento de coste ({imp_err:,.2f} €)")
    registros = registros.filter(pl.col("_ec").is_not_null())

    # Spec: «cuando generas una etiqueta `ZZZ-XXX-YYY` has de comprobar
    # que existe en el árbol de elementos de coste. Si no existe… error
    # que sí detenga el proceso».
    importes_por_etq = dict(
        registros.group_by("_ec")
        .agg(pl.col("importe").sum().alias("imp"))
        .iter_rows()
    )
    _validar_etiquetas_ec(importes_por_etq, árbol_ec, contexto="PTGAS")

    _desc_fn = obtener_descripciones or (lambda col, vals: {})

    # Enriquecimiento y clasificación compartida
    if ctx_enriquecimiento is not None:
        registros = enriquecer_para_actividades(registros, ctx_enriquecimiento)
    if árbol_cc is not None:
        registros, _ = clasificar_centros_coste(
            registros, árbol_cc, distribución_costes, _desc_fn,
        )
    else:
        registros = registros.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("_centro_de_coste")
        )
    if árbol_actividades is not None:
        registros, _ = clasificar_actividades(
            registros, árbol_actividades, _desc_fn,
        )
    elif "_actividad" not in registros.columns:
        registros = registros.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("_actividad")
        )

    es_ord = pl.col("proyecto").cast(pl.Utf8).is_in(PROYECTOS_ORDINARIOS)
    ordinarias = registros.filter(es_ord)
    extras = registros.filter(~es_ord)

    uc_partes: list[pl.DataFrame] = []
    _id_counter = [0]

    def _next_id() -> str:
        _id_counter[0] += 1
        return f"N-{_id_counter[0]:05d}"

    # Retribuciones ordinarias
    # La actividad en ordinarias se toma de la tabla servicio→actividad
    # (o centro_plaza para servicio 368).  El CC viene del clasificador.
    if not ordinarias.is_empty():
        srv = pl.col("servicio").cast(pl.Utf8)
        resto = ordinarias.filter(srv != "368")
        srv_368 = ordinarias.filter(srv == "368")

        if not resto.is_empty():
            agrup = (
                resto.group_by("expediente", "_ec", "servicio", "_centro_de_coste")
                .agg(pl.col("importe").sum())
            )
            filas = []
            for row in agrup.iter_rows(named=True):
                srv_key = str(row["servicio"])
                if row["_centro_de_coste"] is None:
                    continue
                mapping = _SERVICIO_CC.get(srv_key)
                act = mapping[1] if mapping and mapping[1] else "dag-general-universidad"
                filas.append({
                    "id": _next_id(),
                    "expediente": row["expediente"],
                    "elemento_de_coste": row["_ec"],
                    "centro_de_coste": row["_centro_de_coste"],
                    "actividad": act,
                    "importe": row["importe"],
                    "origen": "nómina",
                    "origen_id": f"PTGAS-exp-{row['expediente']}-srv-{srv_key}",
                    "origen_porción": 1.0,
                })
            if filas:
                uc_partes.append(pl.DataFrame(filas))

        if not srv_368.is_empty() and "centro_plaza" in srv_368.columns:
            agrup_368 = (
                srv_368.group_by("expediente", "_ec", "centro_plaza", "_centro_de_coste")
                .agg(pl.col("importe").sum())
            )
            filas_368 = []
            for row in agrup_368.iter_rows(named=True):
                cp_key = str(row["centro_plaza"])
                if row["_centro_de_coste"] is None:
                    continue
                mapping = _CENTRO_PLAZA_CC.get(cp_key)
                act = mapping[1] if mapping and mapping[1] else "dag-general-universidad"
                filas_368.append({
                    "id": _next_id(),
                    "expediente": row["expediente"],
                    "elemento_de_coste": row["_ec"],
                    "centro_de_coste": row["_centro_de_coste"],
                    "actividad": act,
                    "importe": row["importe"],
                    "origen": "nómina",
                    "origen_id": f"PTGAS-exp-{row['expediente']}-srv-368-cp-{cp_key}",
                    "origen_porción": 1.0,
                })
            if filas_368:
                uc_partes.append(pl.DataFrame(filas_368))

    # Retribuciones extra: CC y actividad del clasificador
    if not extras.is_empty():
        con_todo = extras.filter(
            pl.col("_centro_de_coste").is_not_null()
            & pl.col("_actividad").is_not_null()
        )
        filas_extra = []
        for row in con_todo.iter_rows(named=True):
            filas_extra.append({
                "id": _next_id(),
                "expediente": row["expediente"],
                "elemento_de_coste": row["_ec"],
                "centro_de_coste": row["_centro_de_coste"],
                "actividad": row["_actividad"],
                "importe": row["importe"],
                "origen": "nómina",
                "origen_id": f"PTGAS-extra-exp-{row['expediente']}-{row['proyecto']}",
                "origen_porción": row.get("_origen_porción", 1.0),
            })
        if filas_extra:
            uc_partes.append(pl.DataFrame(filas_extra))
            print(f"    UC PTGAS extra: {len(filas_extra):,} UC")

    if not uc_partes:
        return pl.DataFrame()

    # Solo retributivas: la SS de PTGAS se reparte por persona en
    # `_generar_reparto_ss_persona`, junto con la de PDI/PVI.
    return pl.concat(uc_partes)


def _generar_uc_pdi_pvi_extras(
    nóminas_filtradas: pl.DataFrame,
    expedientes: pl.DataFrame,
    *,
    sector_canónico: str,
    elemento_coste_fn,
    id_prefix: str,
    origen_tag: str,
    ctx_enriquecimiento=None,
    árbol_actividades=None,
    árbol_cc=None,
    árbol_ec=None,
    distribución_costes=None,
    obtener_descripciones=None,
) -> pl.DataFrame:
    """Genera UC «retribuciones extra» de PDI o PVI.

    «Extra» = proyecto NO en TABLA-PROYECTOS-GENERALES (los registros
    en proyecto general se apartan: van por la regla 23 o por las
    funciones específicas — atrasos, despidos, indemnizaciones,
    cargos, etc.). Se excluyen también CR 47/48 (van por sus
    funciones propias) y CR 19/64 con proyecto NO general (van por
    `generar_uc_cargos`). Los registros con CR 19/64 en proyecto
    general siguen aquí como UC retributivas mientras no esté
    operativo el cálculo de UC por departamento (Fase B).

    Las UC obtenidas reciben CC y actividad concretos vía los
    clasificadores compartidos.
    """
    exp_filtrados = _mapear_sector(expedientes)
    exp_filtrados = exp_filtrados.filter(pl.col("sector_mapeado") == sector_canónico)
    if exp_filtrados.is_empty():
        return pl.DataFrame()

    registros = nóminas_filtradas.join(
        exp_filtrados.select("expediente", "per_id"),
        on="expediente",
        how="inner",
    )

    from coana.fase1.nóminas.regla_23 import _PROYECTOS_GENERALES

    cr = pl.col("concepto_retributivo").cast(pl.Utf8)
    es_proy_gen = pl.col("proyecto").cast(pl.Utf8).is_in(list(_PROYECTOS_GENERALES))
    es_ss = pl.col("aplicación").cast(pl.Utf8).str.starts_with("12")
    es_uc_definida = (
        cr.is_in(["47", "48"])
        | (cr.is_in(["19", "64"]) & ~es_proy_gen)
    )

    # «Extras» = proyecto NO general. La masa en proyecto general queda
    # apartada para regla 23 (no genera UC retributivas individuales),
    # excepto los CR 19/64 que de momento se mantienen aquí como UC
    # retributivas (TODO: descartar cuando estén las UC calculadas por
    # departamento — Fase B).
    es_extra = ~es_proy_gen | cr.is_in(["19", "64"])
    extras = registros.filter(~es_ss & ~es_uc_definida & es_extra)

    if extras.is_empty():
        # Reportar masa apartada para regla 23
        masa_regla_23 = (
            registros.filter(~es_ss & es_proy_gen & ~cr.is_in(["19", "64", "47", "48"]))
        )
        if not masa_regla_23.is_empty():
            print(
                f"    {sector_canónico}: {len(masa_regla_23):,} registros en "
                f"proyecto general apartados para regla 23 "
                f"({float(masa_regla_23['importe'].sum()):,.2f} €)"
            )
        return pl.DataFrame()

    # Reportar masa apartada para regla 23 (proyecto general, conceptos
    # no especiales): de momento no genera UC; se reparte cuando esté
    # implementada la regla 23.
    masa_regla_23 = (
        registros.filter(~es_ss & es_proy_gen & ~cr.is_in(["19", "64", "47", "48", "30", "87"]))
    )
    if not masa_regla_23.is_empty():
        print(
            f"    {sector_canónico}: {len(masa_regla_23):,} registros en "
            f"proyecto general apartados para regla 23 "
            f"({float(masa_regla_23['importe'].sum()):,.2f} €)"
        )

    ecs = [elemento_coste_fn(row) for row in extras.iter_rows(named=True)]
    extras = extras.with_columns(pl.Series("_ec", ecs))

    errores = extras.filter(pl.col("_ec").is_null())
    if not errores.is_empty():
        n_err = len(errores)
        imp_err = float(errores["importe"].sum())
        print(
            f"    ⚠ {n_err:,} registros {sector_canónico} sin elemento de coste "
            f"({imp_err:,.2f} €)"
        )
    extras = extras.filter(pl.col("_ec").is_not_null())
    if extras.is_empty():
        return pl.DataFrame()

    # Validación contra el árbol de elementos de coste (spec)
    importes_por_etq = dict(
        extras.group_by("_ec")
        .agg(pl.col("importe").sum().alias("imp"))
        .iter_rows()
    )
    _validar_etiquetas_ec(
        importes_por_etq, árbol_ec, contexto=f"extras {sector_canónico}",
    )

    # Clasificación de CC y actividad
    _desc_fn = obtener_descripciones or (lambda c, v: {})
    if ctx_enriquecimiento is not None:
        extras = enriquecer_para_actividades(extras, ctx_enriquecimiento)
    if árbol_cc is not None:
        extras, _ = clasificar_centros_coste(
            extras, árbol_cc, distribución_costes, _desc_fn,
        )
    else:
        extras = extras.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("_centro_de_coste")
        )
    if árbol_actividades is not None:
        extras, _ = clasificar_actividades(
            extras, árbol_actividades, _desc_fn,
        )
    elif "_actividad" not in extras.columns:
        extras = extras.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("_actividad")
        )

    sin_res = extras.filter(
        pl.col("_centro_de_coste").is_null() | pl.col("_actividad").is_null()
    )
    if not sin_res.is_empty():
        print(
            f"    ⚠ {len(sin_res):,} extras {sector_canónico} sin CC/actividad "
            f"resueltos ({float(sin_res['importe'].sum()):,.2f} €)"
        )
    extras = extras.filter(
        pl.col("_centro_de_coste").is_not_null() & pl.col("_actividad").is_not_null()
    )
    if extras.is_empty():
        return pl.DataFrame()

    agrup = (
        extras.group_by(
            "expediente", "proyecto", "_ec", "_centro_de_coste", "_actividad",
        )
        .agg(pl.col("importe").sum())
    )

    _id_counter = [0]

    def _next_id() -> str:
        _id_counter[0] += 1
        return f"{id_prefix}-{_id_counter[0]:05d}"

    filas = []
    for row in agrup.iter_rows(named=True):
        filas.append({
            "id": _next_id(),
            "expediente": row["expediente"],
            "elemento_de_coste": row["_ec"],
            "centro_de_coste": row["_centro_de_coste"],
            "actividad": row["_actividad"],
            "importe": row["importe"],
            "origen": "nómina",
            "origen_id": (
                f"{origen_tag}-exp-{row['expediente']}-proy-{row['proyecto']}"
                f"-ec-{row['_ec']}-cc-{row['_centro_de_coste']}-act-{row['_actividad']}"
            ),
            "origen_porción": 1.0,
        })

    if not filas:
        return pl.DataFrame()

    return pl.DataFrame(filas)


def _generar_uc_pdi(
    nóminas_filtradas: pl.DataFrame,
    expedientes: pl.DataFrame,
    ctx_enriquecimiento=None,
    árbol_actividades=None,
    árbol_cc=None,
    árbol_ec=None,
    distribución_costes=None,
    obtener_descripciones=None,
) -> pl.DataFrame:
    """UC retributivas extra del PDI (proyecto NO general)."""
    return _generar_uc_pdi_pvi_extras(
        nóminas_filtradas, expedientes,
        sector_canónico="PDI",
        elemento_coste_fn=lambda row: _elemento_coste_pdi(
            row["categoría"], row["concepto_retributivo"],
        ),
        id_prefix="D",
        origen_tag="PDI",
        ctx_enriquecimiento=ctx_enriquecimiento,
        árbol_actividades=árbol_actividades,
        árbol_cc=árbol_cc,
        árbol_ec=árbol_ec,
        distribución_costes=distribución_costes,
        obtener_descripciones=obtener_descripciones,
    )


def _generar_uc_pvi(
    nóminas_filtradas: pl.DataFrame,
    expedientes: pl.DataFrame,
    ctx_enriquecimiento=None,
    árbol_actividades=None,
    árbol_cc=None,
    árbol_ec=None,
    distribución_costes=None,
    obtener_descripciones=None,
) -> pl.DataFrame:
    """UC retributivas extra del PVI (proyecto NO general)."""
    return _generar_uc_pdi_pvi_extras(
        nóminas_filtradas, expedientes,
        sector_canónico="PVI",
        elemento_coste_fn=lambda row: _elemento_coste_pvi(
            row["categoría"], row["perceptor"], row["provisión"],
            row["concepto_retributivo"],
            row.get("categoría_plaza"), row.get("sector_plaza"),
        ),
        id_prefix="V",
        origen_tag="PVI",
        ctx_enriquecimiento=ctx_enriquecimiento,
        árbol_actividades=árbol_actividades,
        árbol_cc=árbol_cc,
        árbol_ec=árbol_ec,
        distribución_costes=distribución_costes,
        obtener_descripciones=obtener_descripciones,
    )


# Constantes para costes sociales calculados (régimen de clases pasivas).
# Valores para 2025. TODO: parametrizar por año mediante un xlsx de
# configuración (también el % MEI, que varía con los años).
_BASE_MÁXIMA_SS = 59_094.0
_TIPO_CONTINGENCIAS_COMUNES = 0.236
_TIPO_REDUCCIÓN_CC_TRABAJADOR = 0.065
_TIPO_MEI = 0.0067
_TIPO_FORMACIÓN_PROF = 0.0070
_FACTOR_TRAMO1_SOL = 1.1
_FACTOR_TRAMO2_SOL = 1.5
_TIPO_SOL_TRAMO1 = 0.0092
_TIPO_SOL_TRAMO2 = 0.0100
_TIPO_SOL_TRAMO3 = 0.0117

_CATEGORÍAS_PDI_FUNCIONARIO = ("CU", "TU", "TEU", "CEU")


def _calcular_coste_social_persona(total: float) -> dict[str, float]:
    """Aplica la fórmula de coste social calculado para clases pasivas."""
    base = min(total, _BASE_MÁXIMA_SS)
    cc_bruto = _TIPO_CONTINGENCIAS_COMUNES * base
    reducción_cc = _TIPO_REDUCCIÓN_CC_TRABAJADOR * cc_bruto
    cc = cc_bruto - reducción_cc
    mei = _TIPO_MEI * base
    fp = _TIPO_FORMACIÓN_PROF * base
    t1 = _BASE_MÁXIMA_SS * _FACTOR_TRAMO1_SOL
    t2 = _BASE_MÁXIMA_SS * _FACTOR_TRAMO2_SOL
    sol1 = max(0.0, min(total, t1) - _BASE_MÁXIMA_SS) * _TIPO_SOL_TRAMO1
    sol2 = max(0.0, min(total, t2) - t1) * _TIPO_SOL_TRAMO2
    sol3 = max(0.0, total - t2) * _TIPO_SOL_TRAMO3
    solidaridad = sol1 + sol2 + sol3
    return {
        "total_retribuido": total,
        "base": base,
        "contingencias_comunes": cc,
        "mei": mei,
        "formación_profesional": fp,
        "cuota_solidaridad_tramo1": sol1,
        "cuota_solidaridad_tramo2": sol2,
        "cuota_solidaridad_tramo3": sol3,
        "cuota_solidaridad": solidaridad,
        "importe_total": cc + mei + fp + solidaridad,
    }


def _generar_costes_sociales_calculados(
    nóminas: pl.DataFrame,
    expedientes: pl.DataFrame,
    dir_salida: Path,
) -> pl.DataFrame:
    """Coste social simulado para PDI funcionario en régimen de clases pasivas.

    Detecta personas con al menos un expediente PDI en categoría CU/TU/TEU/CEU
    sin ningún registro de nómina con #campo("aplicación") que empiece por
    «12» y aplica la fórmula de cotización simulada sobre el TOTAL retribuido
    (suma de todos sus expedientes — que por norma debe ser uno solo).
    Persiste el detalle en `costes_sociales_calculados.parquet`.

    Por spec, una persona en esta situación no debería tener otro expediente.
    Si lo tiene, se reporta como error (sin detener el flujo).

    Devuelve un DataFrame con (per_id, importe_total) listo para inyectar
    en el reparto SS por persona.
    """
    exp_per = expedientes.select("expediente", "per_id")
    es_ss = pl.col("aplicación").cast(pl.Utf8).str.starts_with("12")
    es_func = pl.col("categoría").cast(pl.Utf8).is_in(list(_CATEGORÍAS_PDI_FUNCIONARIO))

    nóminas_per = nóminas.join(exp_per, on="expediente", how="inner")

    # per_ids con cotización efectiva (aplicación 12*)
    per_con_ss = set(
        nóminas_per.filter(es_ss).get_column("per_id").drop_nulls().unique().to_list()
    )

    # per_ids con al menos una línea PDI funcionario (CU/TU/TEU/CEU)
    exp_pdi = _mapear_sector(expedientes).filter(pl.col("sector_mapeado") == "PDI")
    nóminas_pdi = nóminas_per.join(
        exp_pdi.select("expediente").unique(), on="expediente", how="inner",
    )
    per_pdi_func = set(
        nóminas_pdi.filter(es_func).get_column("per_id").drop_nulls().unique().to_list()
    )

    # Candidatos: PDI funcionario sin SS cotizada
    per_clases_pasivas = sorted(per_pdi_func - per_con_ss)

    if not per_clases_pasivas:
        pl.DataFrame(schema={
            "per_id": pl.Int64,
            "total_retribuido": pl.Float64,
            "base": pl.Float64,
            "contingencias_comunes": pl.Float64,
            "mei": pl.Float64,
            "formación_profesional": pl.Float64,
            "cuota_solidaridad_tramo1": pl.Float64,
            "cuota_solidaridad_tramo2": pl.Float64,
            "cuota_solidaridad_tramo3": pl.Float64,
            "cuota_solidaridad": pl.Float64,
            "importe_total": pl.Float64,
        }).write_parquet(dir_salida / "costes_sociales_calculados.parquet")
        return pl.DataFrame(schema={
            "per_id": pl.Int64, "importe_total": pl.Float64,
        })

    # Validación: por spec, estas personas solo pueden tener un expediente
    # en los sectores principales (PDI/PTGAS/PVI). Expedientes en sectores
    # secundarios (BEC, etc., agrupados como «Otros») se ignoran: son
    # típicamente vínculos históricos como becario que conviven con el
    # expediente PDI funcionario y no rompen la spec.
    exp_principales = _mapear_sector(expedientes).filter(
        pl.col("sector_mapeado").is_in(list(_SECTORES_CONOCIDOS))
    )
    exp_múltiples = (
        exp_principales.filter(pl.col("per_id").is_in(per_clases_pasivas))
        .group_by("per_id")
        .agg(pl.col("expediente").n_unique().alias("n_exp"))
        .filter(pl.col("n_exp") > 1)
    )
    if not exp_múltiples.is_empty():
        ids_err = exp_múltiples.get_column("per_id").to_list()
        print(
            f"    ⚠ ERROR: {len(ids_err):,} personas PDI funcionario en clases "
            f"pasivas con más de un expediente principal (la spec dice que no "
            f"puede pasar): {ids_err[:5]}…"
        )

    # TOTAL retribuido por persona: suma de líneas no-SS en TODOS sus expedientes
    total_por_persona = (
        nóminas_per
        .filter(~es_ss & pl.col("per_id").is_in(per_clases_pasivas))
        .group_by("per_id")
        .agg(pl.col("importe").sum().alias("total_retribuido"))
    )

    filas = []
    for row in total_por_persona.iter_rows(named=True):
        fila = {"per_id": row["per_id"]}
        fila.update(_calcular_coste_social_persona(float(row["total_retribuido"])))
        filas.append(fila)

    df = pl.DataFrame(filas).sort("importe_total", descending=True)
    df.write_parquet(dir_salida / "costes_sociales_calculados.parquet")
    print(
        f"  Costes sociales calculados (PDI funcionario clases pasivas): "
        f"{len(df):,} personas, {float(df['importe_total'].sum()):,.2f} €"
    )
    return df.select("per_id", "importe_total")


def _generar_multiexpediente(
    agrupado: pl.DataFrame,
    nóminas: pl.DataFrame,
    dir_salida: Path,
) -> None:
    """Genera multiexpediente.parquet: personas con expedientes en sectores distintos.

    Para cada per_id con expedientes en más de un sector (PDI/PTGAS/PVI),
    guarda los sectores, el conteo de expedientes por sector y la actividad
    mensual (qué meses tenía registros de nómina en cada expediente).
    """
    # Solo sectores principales (sin "Otros")
    principales = agrupado.filter(pl.col("sector_final") != "Otros")

    # Sectores distintos por persona
    per_sectores = (
        principales.group_by("per_id")
        .agg(
            pl.col("sector_final").unique().sort().alias("sectores"),
            pl.col("sector_final").n_unique().alias("n_sectores"),
        )
        .filter(pl.col("n_sectores") > 1)
    )

    if per_sectores.is_empty():
        # Guardar vacío para que la UI no falle
        pl.DataFrame(schema={
            "per_id": pl.Int64,
            "sectores": pl.List(pl.Utf8),
            "n_sectores": pl.UInt32,
        }).write_parquet(dir_salida / "multiexpediente.parquet")
        return

    # Conteo de expedientes por (per_id, sector)
    conteo = (
        principales.filter(pl.col("per_id").is_in(per_sectores["per_id"]))
        .group_by("per_id", "sector_final")
        .agg(pl.col("expediente").n_unique().alias("n_expedientes"))
    )
    # Pivot: columnas n_PDI, n_PTGAS, n_PVI
    conteo_pivot = conteo.pivot(
        on="sector_final", index="per_id", values="n_expedientes",
    ).fill_null(0)
    # Renombrar columnas pivotadas
    for s in ["PDI", "PTGAS", "PVI"]:
        if s in conteo_pivot.columns:
            conteo_pivot = conteo_pivot.rename({s: f"n_{s}"})
    for s in ["PDI", "PTGAS", "PVI"]:
        if f"n_{s}" not in conteo_pivot.columns:
            conteo_pivot = conteo_pivot.with_columns(pl.lit(0).alias(f"n_{s}"))

    result = per_sectores.join(conteo_pivot, on="per_id", how="left")

    # Actividad mensual: para cada per_id multiexpediente, qué meses tiene
    # registros en cada expediente (extraer mes de la columna fecha).
    multi_per_ids = per_sectores["per_id"]
    exp_sector = (
        principales.filter(pl.col("per_id").is_in(multi_per_ids))
        .select("expediente", "per_id", "sector_final")
        .unique()
    )
    actividad = (
        nóminas.filter(pl.col("expediente").is_in(exp_sector["expediente"]))
        .select("expediente", "fecha")
        .with_columns(pl.col("fecha").dt.month().alias("mes"))
        .select("expediente", "mes")
        .unique()
        .join(exp_sector, on="expediente", how="left")
    )
    actividad.write_parquet(dir_salida / "multiexpediente_actividad.parquet")

    n = len(result)
    print(f"  Multiexpediente: {n:,} personas con expedientes en sectores distintos")
    result.write_parquet(dir_salida / "multiexpediente.parquet")


def _generar_reparto_ss_persona(
    nóminas: pl.DataFrame,
    expedientes: pl.DataFrame,
    uc_ptgas: pl.DataFrame,
    uc_pvi: pl.DataFrame,
    uc_pdi: pl.DataFrame,
    uc_extra: list[pl.DataFrame],
    uc_por_expediente: dict[int, pl.DataFrame],
    dir_salida: Path,
    ss_calculados_por_persona: pl.DataFrame | None = None,
) -> None:
    """Genera el reparto de SS por persona agrupando UC por (actividad, centro de coste).

    Reúne todas las UC asociadas a los expedientes de cada persona (de nómina y
    de presupuesto), calcula el porcentaje de cada par (actividad, centro_de_coste)
    y reparte la SS proporcionalmente.

    Cubre los tres sectores (PTGAS, PDI, PVI). Para asignar el elemento
    de coste de la SS se usa el sector principal de la persona, con
    prelación PTGAS > PVI > PDI > Otros (véase la spec).

    Guarda:
      - persona_uc.parquet: todas las UC retributivas por persona (columnas completas)
      - persona_ss.parquet: reparto de SS por (per_id, actividad, centro_de_coste).
    """
    # Mapa expediente → per_id
    exp_per = expedientes.select("expediente", "per_id")

    # Reunir todas las UC con todas sus columnas. Quitamos per_id si
    # alguna UC ya lo trae, para añadirlo de forma uniforme con el join
    # contra exp_per.
    partes: list[pl.DataFrame] = []
    for df in (uc_ptgas, uc_pvi, uc_pdi, *uc_extra):
        if df is not None and not df.is_empty():
            if "per_id" in df.columns:
                df = df.drop("per_id")
            partes.append(df)
    for exp_id, df_uc in uc_por_expediente.items():
        if "actividad" in df_uc.columns and "centro_de_coste" in df_uc.columns:
            sub = df_uc.with_columns(pl.lit(exp_id).cast(pl.Int64).alias("expediente"))
            if "per_id" in sub.columns:
                sub = sub.drop("per_id")
            partes.append(sub)

    if not partes:
        # Guardar vacíos
        pl.DataFrame(schema={
            "per_id": pl.Int64, "expediente": pl.Int64, "id": pl.Utf8,
            "elemento_de_coste": pl.Utf8, "centro_de_coste": pl.Utf8,
            "actividad": pl.Utf8, "importe": pl.Float64, "origen": pl.Utf8,
            "origen_id": pl.Utf8, "origen_porción": pl.Float64, "tipo": pl.Utf8,
        }).write_parquet(dir_salida / "persona_uc.parquet")
        pl.DataFrame(schema={
            "per_id": pl.Int64, "actividad": pl.Utf8, "centro_de_coste": pl.Utf8,
            "importe_uc": pl.Float64, "pct": pl.Float64, "ss_total": pl.Float64,
            "ss_proporcional": pl.Float64,
        }).write_parquet(dir_salida / "persona_ss.parquet")
        return

    todas_uc = pl.concat(partes, how="diagonal")

    # Añadir per_id vía expediente
    todas_uc = todas_uc.join(exp_per, on="expediente", how="left")

    # Marcar como retributivas
    todas_uc = todas_uc.with_columns(pl.lit("retributiva").alias("tipo"))

    # SS total por persona: suma de registros de nómina con aplicación
    # que empieza por "12" en cualquiera de sus expedientes.
    es_ss = pl.col("aplicación").cast(pl.Utf8).str.starts_with("12")
    ss_por_exp = (
        nóminas.filter(es_ss)
        .group_by("expediente")
        .agg(pl.col("importe").sum().alias("ss"))
    )
    ss_por_persona = (
        ss_por_exp.join(exp_per, on="expediente", how="left")
        .group_by("per_id")
        .agg(pl.col("ss").sum().alias("ss_total"))
    )

    # Inyectar costes sociales calculados (clases pasivas PDI funcionario):
    # se suman al ss_total para repartirlos por (CC, actividad) igual que la
    # SS cotizada.
    if (
        ss_calculados_por_persona is not None
        and not ss_calculados_por_persona.is_empty()
    ):
        ss_por_persona = (
            ss_por_persona.join(
                ss_calculados_por_persona.rename({"importe_total": "ss_calc"}),
                on="per_id", how="full", coalesce=True,
            )
            .with_columns(
                (pl.col("ss_total").fill_null(0.0) + pl.col("ss_calc").fill_null(0.0))
                .alias("ss_total")
            )
            .drop("ss_calc")
        )

    # Agrupar UC retributivas por (per_id, actividad, centro_de_coste)
    agrup = (
        todas_uc
        .group_by("per_id", "actividad", "centro_de_coste")
        .agg(pl.col("importe").sum().alias("importe_uc"))
    )

    # Total UC por persona
    total_por_persona = (
        agrup.group_by("per_id")
        .agg(pl.col("importe_uc").sum().alias("total_uc"))
    )

    # Calcular porcentaje y reparto de SS
    reparto = (
        agrup
        .join(total_por_persona, on="per_id", how="left")
        .join(ss_por_persona, on="per_id", how="left")
        .with_columns(
            (pl.col("importe_uc") / pl.col("total_uc") * 100).round(2).alias("pct"),
            pl.col("ss_total").fill_null(0.0),
        )
        .with_columns(
            (pl.col("importe_uc") / pl.col("total_uc") * pl.col("ss_total"))
            .round(2)
            .alias("ss_proporcional"),
        )
        .drop("total_uc")
        .sort("per_id", "importe_uc", descending=[False, True])
    )

    reparto.write_parquet(dir_salida / "persona_ss.parquet")

    # Sector principal por persona (prelación PTGAS > PVI > PDI)
    exp_con_sector = _mapear_sector(expedientes)
    sector_principal = (
        exp_con_sector
        .with_columns(
            pl.col("sector_mapeado")
            .replace({s: str(i) for i, s in enumerate(_PRELACIÓN_SECTOR)})
            .alias("_prio"),
        )
        .sort("_prio")
        .group_by("per_id")
        .first()
        .select("per_id", pl.col("sector_mapeado").alias("sector_principal"))
    )

    # Mapeo sector → elemento de coste de SS (cotizada)
    _EC_SS = {"PTGAS": "ss-ptgas", "PDI": "ss-pdi-func", "PVI": "ss-pvi-otpersonal"}
    # Personas en clases pasivas: su SS no es cotizada sino calculada y va
    # a un elemento distinto («previsión social de funcionarios»).
    per_ids_pasivas: set[int] = set()
    if (
        ss_calculados_por_persona is not None
        and not ss_calculados_por_persona.is_empty()
    ):
        per_ids_pasivas = set(
            ss_calculados_por_persona.get_column("per_id").to_list()
        )

    # Generar UC de costes sociales (una por cada par actividad/CC con SS > 0)
    ss_base = (
        reparto.filter(pl.col("ss_proporcional") > 0)
        .join(sector_principal, on="per_id", how="left")
    )

    filas_ss = []
    ss_counter = 0
    for row in ss_base.iter_rows(named=True):
        ss_counter += 1
        if row["per_id"] in per_ids_pasivas:
            ec = "prevsoc-funcs-pdi"
        else:
            sector = row.get("sector_principal", "")
            ec = _EC_SS.get(sector, f"ss-{sector.lower()}")
        filas_ss.append({
            "per_id": row["per_id"],
            "expediente": None,
            "id": f"SS-{ss_counter:05d}",
            "elemento_de_coste": ec,
            "centro_de_coste": row["centro_de_coste"],
            "actividad": row["actividad"],
            "importe": row["ss_proporcional"],
            "origen": "nómina",
            "origen_id": f"SS-{row['per_id']}-{row['actividad']}",
            "origen_porción": 1.0,
            "tipo": "coste social",
        })

    if filas_ss:
        uc_ss = pl.DataFrame(filas_ss)
    else:
        uc_ss = pl.DataFrame(schema={
            "per_id": pl.Int64, "expediente": pl.Int64, "id": pl.Utf8,
            "elemento_de_coste": pl.Utf8, "centro_de_coste": pl.Utf8,
            "actividad": pl.Utf8, "importe": pl.Float64, "origen": pl.Utf8,
            "origen_id": pl.Utf8, "origen_porción": pl.Float64, "tipo": pl.Utf8,
        })

    # Unir retributivas + costes sociales
    todas = pl.concat([todas_uc, uc_ss], how="diagonal")
    todas.write_parquet(dir_salida / "persona_uc.parquet")

    n_personas = reparto["per_id"].n_unique()
    n_uc_ss = len(uc_ss)
    print(
        f"  Reparto SS por persona: {n_personas:,} personas, {len(reparto):,} pares act/CC, "
        f"{n_uc_ss:,} UC de costes sociales"
    )


def preprocesar_nóminas(
    ctx: ContextoNóminas,
    dir_salida: Path,
    ctx_enriquecimiento=None,
    árbol_actividades=None,
    árbol_cc=None,
    árbol_ec=None,
    distribución_costes=None,
    obtener_descripciones=None,
    ruta_base: Path = Path("data"),
) -> ResultadoNóminas:
    """Agrupa nóminas por expediente, clasifica por sector y guarda parquets.

    Genera un parquet por sector (PDI, PTGAS, PVI, Otros) en *dir_salida*.
    """
    dir_salida.mkdir(parents=True, exist_ok=True)

    nóminas = ctx.nóminas
    expedientes = ctx.expedientes

    if nóminas is None or expedientes is None:
        return ResultadoNóminas(
            expedientes_por_sector={},
            importe_por_sector={},
        )

    print(f"  Registros de nómina: {len(nóminas):,}")

    _reset_etiquetas_ec_faltantes()

    # Filtro de la spec: solo se consideran expedientes con alguna
    # retribución en el ejercicio analizado. Los expedientes que constan
    # en la tabla de RR.HH. pero no tienen ninguna línea de nómina (o la
    # suma total es 0 €) se descartan por completo del preprocesamiento.
    exp_con_actividad = (
        nóminas.group_by("expediente")
        .agg(pl.col("importe").sum().alias("_imp"))
        .filter(pl.col("_imp").abs() > 0)
        .get_column("expediente")
    )
    n_antes = expedientes.height
    expedientes = expedientes.filter(
        pl.col("expediente").is_in(exp_con_actividad)
    )
    if expedientes.height < n_antes:
        print(
            f"  Filtrados {n_antes - expedientes.height:,} expedientes "
            f"sin retribución en el ejercicio (quedan {expedientes.height:,})"
        )

    # Join para obtener per_id y sector de cada línea de nómina.
    con_sector = nóminas.join(
        expedientes.select("expediente", "per_id", "sector"),
        on="expediente",
        how="left",
    )

    # Mapear sector: PAS→PTGAS, PI→PVI, PDI→PDI, resto→Otros.
    con_sector = con_sector.with_columns(
        pl.col("sector")
        .replace(_MAPEO_SECTOR)
        .fill_null("Otros")
        .alias("sector_mapeado"),
    ).with_columns(
        pl.when(pl.col("sector_mapeado").is_in(_SECTORES_CONOCIDOS))
        .then(pl.col("sector_mapeado"))
        .otherwise(pl.lit("Otros"))
        .alias("sector_final"),
    )

    # Agrupar por expediente, per_id, sector_final.
    agrupado = (
        con_sector.group_by("expediente", "per_id", "sector_final")
        .agg(
            pl.col("importe").sum().alias("importe"),
            pl.len().alias("n_registros"),
        )
        .sort("expediente")
    )

    # Guardar un parquet por sector.
    expedientes_por_sector: dict[str, int] = {}
    importe_por_sector: dict[str, float] = {}

    for sector in ["PDI", "PTGAS", "PVI", "Otros"]:
        df_sector = (
            agrupado.filter(pl.col("sector_final") == sector)
            .drop("sector_final")
        )
        expedientes_por_sector[sector] = len(df_sector)
        importe_por_sector[sector] = float(
            df_sector["importe"].sum() if not df_sector.is_empty() else 0
        )
        df_sector.write_parquet(dir_salida / f"{sector}.parquet")

    # -- Multiexpediente: personas con expedientes en sectores distintos --
    _generar_multiexpediente(agrupado, nóminas, dir_salida)

    # -- UC de retribuciones ordinarias PTGAS --
    uc_ptgas = _generar_uc_ptgas(
        nóminas, expedientes,
        ctx_enriquecimiento=ctx_enriquecimiento,
        árbol_actividades=árbol_actividades,
        árbol_cc=árbol_cc,
        árbol_ec=árbol_ec,
        distribución_costes=distribución_costes,
        obtener_descripciones=obtener_descripciones,
    )
    if not uc_ptgas.is_empty():
        importe_ptgas = float(uc_ptgas["importe"].sum())
        print(f"  UC PTGAS retrib. ordinarias: {len(uc_ptgas):,} UC, {importe_ptgas:,.2f} €")
        uc_ptgas.write_parquet(dir_salida / "uc_ptgas.parquet")

    # -- UC «extra» PVI (proyecto NO general) --
    uc_pvi = _generar_uc_pvi(
        nóminas, expedientes,
        ctx_enriquecimiento=ctx_enriquecimiento,
        árbol_actividades=árbol_actividades,
        árbol_cc=árbol_cc,
        árbol_ec=árbol_ec,
        distribución_costes=distribución_costes,
        obtener_descripciones=obtener_descripciones,
    )
    if not uc_pvi.is_empty():
        importe_pvi = float(uc_pvi["importe"].sum())
        print(f"  UC PVI extras: {len(uc_pvi):,} UC, {importe_pvi:,.2f} €")
        uc_pvi.write_parquet(dir_salida / "uc_pvi.parquet")

    # -- UC «extra» PDI (proyecto NO general) --
    uc_pdi = _generar_uc_pdi(
        nóminas, expedientes,
        ctx_enriquecimiento=ctx_enriquecimiento,
        árbol_actividades=árbol_actividades,
        árbol_cc=árbol_cc,
        árbol_ec=árbol_ec,
        distribución_costes=distribución_costes,
        obtener_descripciones=obtener_descripciones,
    )
    if not uc_pdi.is_empty():
        importe_pdi = float(uc_pdi["importe"].sum())
        print(f"  UC PDI extras: {len(uc_pdi):,} UC, {importe_pdi:,.2f} €")
        uc_pdi.write_parquet(dir_salida / "uc_pdi.parquet")

    # Spec: validamos aquí —tras procesar las tres ramas— para reportar
    # todas las etiquetas faltantes en una sola pasada.
    _detener_si_etiquetas_ec_faltantes()

    # -- Regla 23: diccionarios de dedicación real (PDI/PVI) --
    generar_dedicación_docente(expedientes, ruta_base, dir_salida)
    pod_resuelto = generar_pod_resuelto(expedientes, ruta_base, dir_salida)
    generar_dedicación_titulaciones(pod_resuelto, dir_salida)
    generar_dedicación_estudios(pod_resuelto, dir_salida)
    generar_asignaturas_sin_titulación(expedientes, ruta_base, dir_salida)
    generar_estructura_estudios_titulaciones(expedientes, ruta_base, dir_salida)
    generar_horas_no_oficiales(
        expedientes, ruta_base, dir_salida,
        ctx_enriquecimiento=ctx_enriquecimiento,
    )
    generar_atrasos_y_apartados(nóminas, expedientes, dir_salida)
    _clasif_kw = dict(
        ctx_enriquecimiento=ctx_enriquecimiento,
        árbol_actividades=árbol_actividades,
        árbol_cc=árbol_cc,
        distribución_costes=distribución_costes,
        obtener_descripciones=obtener_descripciones,
    )
    uc_despidos = generar_uc_despidos(nóminas, expedientes, dir_salida, **_clasif_kw)
    uc_indemn = generar_uc_indemnizaciones_asistencias(
        nóminas, expedientes, dir_salida, **_clasif_kw,
    )
    uc_cargos = generar_uc_cargos(nóminas, expedientes, dir_salida, **_clasif_kw)

    # -- Costes sociales calculados (clases pasivas PDI funcionario) --
    ss_calculados = _generar_costes_sociales_calculados(
        nóminas, expedientes, dir_salida,
    )

    # -- Reparto de SS por persona (cotizada + calculada): incluye
    #    PTGAS + PVI + PDI + UC definidas (despidos, indemnizaciones
    #    por asistencias, cargos) --
    _generar_reparto_ss_persona(
        nóminas, expedientes,
        uc_ptgas, uc_pvi, uc_pdi,
        [uc_despidos, uc_indemn, uc_cargos],
        {},
        dir_salida,
        ss_calculados_por_persona=ss_calculados,
    )

    return ResultadoNóminas(
        uc_despidos=uc_despidos,
        uc_indemnizaciones_asistencias=uc_indemn,
        uc_cargos=uc_cargos,
        expedientes_por_sector=expedientes_por_sector,
        importe_por_sector=importe_por_sector,
        uc_ptgas=uc_ptgas,
        uc_pvi=uc_pvi,
        uc_pdi=uc_pdi,
    )
