"""Generación de unidades de coste por reparto de la masa regla 23.

La «masa regla 23» es el subconjunto de las nóminas PDI/PVI con:

- aplicación que NO empieza por #val("12") (no es seguridad social)
- proyecto presupuestario que SÍ está en
  #campo("TABLA-PROYECTOS-GENERALES-NÓMINA")
- concepto retributivo NO en #val("19"), #val("64"), #val("47"),
  #val("48") (esos conceptos tienen tratamiento propio: cargos,
  despidos, indemnizaciones).

Esa masa no se imputa a una actividad concreta línea a línea: se
reparte por persona entre las actividades y centros de coste en
proporción a las horas finales calculadas por la regla 23
(`dedicación_pdi_normalizada.parquet`).

Para cada (per_id, elemento_de_coste) calculamos su importe total y lo
distribuimos entre las parejas (actividad, centro) de la persona con
peso `horas_finales / sum(horas_finales)`. Cada combinación
(per_id, ec, actividad, centro) genera una unidad de coste.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.util.configuración import cfg_float


# Regla escoba: una persona sin dedicación cuya masa residual (la que
# iría a pendiente) es inferior a este umbral por persona se imputa a
# (UJI, UJI) como gasto general; si es igual o superior, se mantiene en
# (pendiente, pendiente) como anomalía real a revisar. Origen:
# data/configuración.xlsx (clave `umbral_residual_regla23`).
_UMBRAL_RESIDUAL = cfg_float("umbral_residual_regla23")


# Concepto retributivo de los incentivos / productividad anual («OTVARS»),
# que se abona en marzo por el ejercicio anterior. Cuando es la única
# retribución variable del año (V) y la persona no tiene dedicación
# detectable, asumimos que es un cobro residual de alguien que ya no está
# en activo y lo imputamos a (UJI, UJI) en vez de (pendiente, pendiente).
_CR_INCENTIVOS_AÑO_ANTERIOR = "67"
_MES_INCENTIVOS = 3

# Funcionarios en servicios especiales en otra administración: la UJI ya
# no les paga su salario (lo paga el destino), pero sigue abonándoles los
# trienios consolidados. Su perfil en la masa regla 23 es «solo trienios
# (CR 03) y, en su caso, paga extra, SIN sueldo base (CR 01)». Como no
# prestan actividad en la UJI, ese gasto se imputa a (UJI, UJI).
_CR_TRIENIOS = "03"
_CR_SOU_BASE = "01"

# Associats assistencials (PAA) de ciencias de la salud sin carga en el
# POD: imparten la práctica asistencial de los grados de salud, docencia
# que no figura en el POD. Su departamento (centro resuelto del servicio
# de nómina vía servicios.xlsx) decide a qué grado va su coste; siempre a
# la titulación de Grado y al centro `fcs` (Facultat de Ciències de la
# Salut). Se aplica antes del fallback a (pendiente / UJI).
_CATEGORÍA_PAA = "PAA"
_DEPTO_SALUD_A_GRADO: dict[str, tuple[str, str]] = {
    "upm":    ("grado-medicina",   "fcs"),  # Unitat Predepartamental de Medicina
    "upi":    ("grado-enfermería", "fcs"),  # Unitat Predepartamental d'Infermeria
    "dpbcp":  ("grado-psicología", "fcs"),  # Psicologia Bàsica, Clínica i Psicobiologia
    "dpeesm": ("grado-psicología", "fcs"),  # Psicologia Evolutiva, Educativa, Social i Metodologia
}

# Overrides individuales (casos super-específicos): personas cuya
# situación laboral no es inferible de los datos pero cuyo destino es
# conocido por revisión manual. Toda su masa regla 23 se imputa
# íntegramente al par (actividad, centro) indicado, con prioridad sobre
# el reparto por dedicación y sobre cualquier otro fallback. Ver la spec,
# §«Overrides individuales». Clave: per_id → (actividad, centro).
_OVERRIDES_INDIVIDUALES: dict[int, tuple[str, str]] = {
    # PAL del Centre d'Autoaprenentatge de Llengües (CAL) del Servei de
    # Llengües i Terminologia: no imparten docencia reglada; su coste es
    # asimilable al del PTGAS y va al servicio de lenguas.
    91758: ("cursos-idiomas", "slt"),   # María Carmen Monfort Manero
    148067: ("cursos-idiomas", "slt"),  # Maria Dolors Munar Ara
}

# Grupo 1: personal investigador (PVI/PI, elemento de coste con prefijo
# `piyotper`) cuya masa procede de proyectos generales sin proyecto ni
# grupo imputable. Cuando no tiene dedicación, su coste se lleva a la
# investigación con financiación propia del Vicerrectorado de
# Investigación.
_EC_INVESTIGADOR_PREFIJO = "piyotper"
_INVESTIGADOR_SIN_PROYECTO_ACT = "otras-ait-financiación-propia"
_INVESTIGADOR_SIN_PROYECTO_CC = "vi"

# Grupo 2: figuras puramente docentes (associats `pdi-as` y substituts
# `pdi-ps`) sin POD ni período oculto y fuera de los departamentos de
# salud (a estos últimos los captura antes la regla PAA→grado). Su coste
# se imputa a estudios oficiales de la UJI.
_EC_DOCENTE_PURO_PREFIJOS = ("pdi-as", "pdi-ps")
_DOCENTE_PURO_SIN_POD_ACT = "estudios-oficiales"
_DOCENTE_PURO_SIN_POD_CC = "UJI"


def generar_uc_reparto_regla_23(
    ruta_base: Path,
    año: int = 2025,
) -> pl.DataFrame:
    """Genera y persiste las UC por reparto de la masa regla 23."""
    from coana.fase1.nóminas import _elemento_coste_pdi, _elemento_coste_pvi
    from coana.fase1.nóminas.regla_23 import _PROYECTOS_GENERALES

    # Preferir la nómina aplicada (tras descuento del CR 68 por extras
    # de cargos y filtro de atrasos no vinculados) si está disponible.
    # En su ausencia (p. ej. ejecución parcial), caer al Excel original.
    nom_aplicada_path = (
        ruta_base / "fase1" / "auxiliares" / "nóminas" / "nominas_aplicadas.parquet"
    )
    nom_path = ruta_base / "entrada" / "nóminas" / "nóminas y seguridad social.xlsx"
    exp_path = ruta_base / "entrada" / "nóminas" / "expedientes recursos humanos.xlsx"
    norm_path = ruta_base / "fase1" / "regla23" / "dedicación_pdi_normalizada.parquet"
    if not (exp_path.exists() and norm_path.exists()):
        return _esquema_vacío()

    if nom_aplicada_path.exists():
        nom = pl.read_parquet(nom_aplicada_path)
    elif nom_path.exists():
        nom = read_excel(nom_path)
    else:
        return _esquema_vacío()
    exp = read_excel(exp_path)
    norm = pl.read_parquet(norm_path)
    if nom.is_empty() or exp.is_empty() or norm.is_empty():
        return _esquema_vacío()

    # Filtro de la masa regla 23.
    cr = pl.col("concepto_retributivo").cast(pl.Utf8)
    proy = pl.col("proyecto").cast(pl.Utf8)
    apli = pl.col("aplicación").cast(pl.Utf8)
    masa = (
        nom.filter(~apli.str.starts_with("12"))
        .filter(proy.is_in(list(_PROYECTOS_GENERALES)))
        .filter(~cr.is_in(["19", "64", "47", "48"]))
        .filter(pl.col("fecha").dt.year() == año)
    )
    if masa.is_empty():
        return _esquema_vacío()

    # Solo PDI y PVI (el reparto regla 23 aplica a estos sectores). En
    # `expedientes recursos humanos.xlsx` el PVI viene codificado como
    # `PI`; lo normalizamos para que el cargador lo trate como PVI.
    exp_pdi_pvi = exp.filter(pl.col("sector").is_in(["PDI", "PVI", "PI"]))
    exp_pdi_pvi = exp_pdi_pvi.with_columns(
        pl.col("sector").replace({"PI": "PVI"})
    )
    masa = masa.join(
        exp_pdi_pvi.select("expediente", "per_id", "sector"),
        on="expediente", how="inner",
    )
    if masa.is_empty():
        return _esquema_vacío()

    # Calcular elemento de coste fila a fila (depende del sector).
    ecs: list[str | None] = []
    for r in masa.iter_rows(named=True):
        if r["sector"] == "PDI":
            ec = _elemento_coste_pdi(r.get("categoría"), r.get("concepto_retributivo"))
        else:  # PVI
            ec = _elemento_coste_pvi(
                r.get("categoría"), r.get("perceptor"), r.get("provisión"),
                r.get("concepto_retributivo"),
                r.get("categoría_plaza"), r.get("sector_plaza"),
            )
        ecs.append(ec)
    masa = masa.with_columns(pl.Series("_ec", ecs))

    sin_ec = masa.filter(pl.col("_ec").is_null())
    if not sin_ec.is_empty():
        imp_err = float(sin_ec["importe"].sum())
        print(
            f"    ⚠ {len(sin_ec):,} registros de masa regla 23 sin elemento "
            f"de coste ({imp_err:,.2f} €) — se descartan."
        )
    masa = masa.filter(pl.col("_ec").is_not_null())
    if masa.is_empty():
        return _esquema_vacío()

    # Importe por (per_id, expediente, elemento_de_coste). Conservamos
    # `expediente` para que la UC resultante pueda atribuirse a un
    # expediente concreto (no a un «principal» elegido a posteriori).
    # Cuando una persona tiene varios expedientes PDI/PVI, cada uno
    # se queda con SU cacho de masa regla 23 prorrateado por sus
    # propias líneas; los pesos por (actividad, centro_de_coste) son
    # los de la persona (la regla 23 calcula horas a nivel per_id, no
    # por expediente).
    masa_pp = (
        masa.group_by("per_id", "expediente", "_ec")
        .agg(pl.col("importe").sum().alias("importe_ec"))
    )

    # Overrides individuales (máxima prioridad): se apartan ANTES de
    # cualquier reparto o reducción, de modo que toda su masa va íntegra
    # al par (actividad, centro) fijado manualmente.
    override_uc = None
    if _OVERRIDES_INDIVIDUALES:
        ids_ovr = list(_OVERRIDES_INDIVIDUALES.keys())
        ovr = masa_pp.filter(pl.col("per_id").is_in(ids_ovr))
        if not ovr.is_empty():
            masa_pp = masa_pp.filter(~pl.col("per_id").is_in(ids_ovr))
            mapping_ovr = pl.DataFrame(
                {
                    "per_id": ids_ovr,
                    "_act_ovr": [a for a, _ in _OVERRIDES_INDIVIDUALES.values()],
                    "_cc_ovr": [c for _, c in _OVERRIDES_INDIVIDUALES.values()],
                },
                schema={"per_id": pl.Int64, "_act_ovr": pl.Utf8, "_cc_ovr": pl.Utf8},
            )
            override_uc = ovr.join(mapping_ovr, on="per_id", how="inner").select(
                "per_id", "expediente", "_ec", "importe_ec",
                pl.col("_act_ovr").alias("actividad"),
                pl.col("_cc_ovr").alias("centro_de_coste"),
                pl.lit(1.0).alias("peso"),
            )
            print(
                f"    ℹ {override_uc['per_id'].n_unique():,} persona(s) con "
                f"override individual ({float(override_uc['importe_ec'].sum()):,.2f} €) "
                f"— imputada(s) al par fijado manualmente."
            )

    # Reducción sindical (tipo 8): si el expediente está en
    # `factores_x_sindical`, se aparta `(1−X) × importe_ec` como UC
    # sindical (CC `locales-sindicales`, actividad `acción-sindical`)
    # antes del reparto regla 23. La masa que entra al reparto queda
    # en `X × importe_ec`.
    from coana.fase1.nóminas.reducciones_sindicales import (
        ACTIVIDAD_SINDICAL, CC_SINDICAL,
        factor_x_por_expediente as _factor_x_sind,
    )
    factores_x = _factor_x_sind(ruta_base, año=año)
    uc_sindical_pre = pl.DataFrame()
    if factores_x:
        fx_df = pl.DataFrame(
            {
                "expediente": list(factores_x.keys()),
                "_x": list(factores_x.values()),
            },
            schema={"expediente": pl.Int64, "_x": pl.Float64},
        )
        masa_pp = masa_pp.join(fx_df, on="expediente", how="left").with_columns(
            pl.col("_x").fill_null(1.0)
        )
        uc_sindical_pre = (
            masa_pp.filter(pl.col("_x") < 1.0)
            .with_columns(
                (pl.col("importe_ec") * (1.0 - pl.col("_x"))).round(2).alias("importe"),
                pl.lit(CC_SINDICAL).alias("centro_de_coste"),
                pl.lit(ACTIVIDAD_SINDICAL).alias("actividad"),
                (1.0 - pl.col("_x")).alias("peso"),
            )
            .filter(pl.col("importe").abs() >= 0.01)
            .select(
                "per_id", "expediente", "_ec", "importe_ec",
                "actividad", "centro_de_coste", "peso", "importe",
            )
        )
        masa_pp = masa_pp.with_columns(
            (pl.col("importe_ec") * pl.col("_x")).alias("importe_ec")
        ).drop("_x")

    # Pesos por (per_id, actividad, centro_de_coste).
    pesos = (
        norm.filter(pl.col("horas_finales") > 0)
        .group_by("per_id", "actividad", "centro_de_coste")
        .agg(pl.col("horas_finales").sum().alias("h"))
    )
    totales = pesos.group_by("per_id").agg(pl.col("h").sum().alias("h_total"))
    pesos = pesos.join(totales, on="per_id", how="left").with_columns(
        (pl.col("h") / pl.col("h_total")).alias("peso")
    ).filter(pl.col("h_total") > 0)

    # Personas con masa pero sin dedicación → fallback a (pendiente,
    # pendiente) con peso 1 (toda la masa de la persona a esa única
    # fila) para no perder importe del coste analítico. Para personas
    # cuyo perfil retributivo encaja con «PDI cesado/fallecido cobrando
    # incentivos del año anterior» (única retribución variable en marzo,
    # concepto retributivo 67 y, opcionalmente, atrasos posteriores) se
    # imputan a (UJI, UJI) en vez de (pendiente, pendiente).
    sin_ded = masa_pp.join(
        totales.select("per_id"), on="per_id", how="anti",
    )
    if not sin_ded.is_empty():
        incentivos_residuales = _detecta_incentivos_residuales(
            masa, sin_ded.select("per_id").unique(),
        )
        sin_ded = sin_ded.join(incentivos_residuales, on="per_id", how="left")
        sin_ded = sin_ded.with_columns(
            pl.col("_es_incentivos_residuales").fill_null(False)
        )
        # PAA de ciencias de la salud sin POD → grado de su departamento
        # (prioritario sobre incentivos residuales y sobre pendiente).
        paa_salud = _override_paa_salud(masa, ruta_base)
        sin_ded = sin_ded.join(paa_salud, on="per_id", how="left")
        # Funcionarios en servicios especiales (solo trienios, sin sueldo
        # base) → (UJI, UJI).
        serv_esp = _detecta_servicios_especiales(
            masa, sin_ded.select("per_id").unique(),
        )
        sin_ded = sin_ded.join(serv_esp, on="per_id", how="left").with_columns(
            pl.col("_es_servicios_especiales").fill_null(False)
        )
        # Máscaras del fallback, mutuamente excluyentes por precedencia:
        # PAA-salud → incentivos residuales (UJI) → investigador grupo 1
        # (financiación propia del VI) → servicios especiales (UJI) →
        # pendiente.
        _inc = pl.col("_es_incentivos_residuales")
        _es_paa = pl.col("_act_paa").is_not_null()
        _es_invest = pl.col("_ec").str.starts_with(_EC_INVESTIGADOR_PREFIJO)
        _serv = pl.col("_es_servicios_especiales")
        _es_doc = pl.any_horizontal(
            [pl.col("_ec").str.starts_with(p) for p in _EC_DOCENTE_PURO_PREFIJOS]
        )
        m_paa = _es_paa
        m_uji = ~_es_paa & _inc
        m_inv = ~_es_paa & ~_inc & _es_invest
        m_serv = ~_es_paa & ~_inc & ~_es_invest & _serv
        m_doc = ~_es_paa & ~_inc & ~_es_invest & ~_serv & _es_doc
        # Lo que ninguna regla anterior captura. Regla escoba: si la masa
        # residual TOTAL de la persona es menor que el umbral, se barre a
        # (UJI, UJI); si no, se mantiene como anomalía en (pendiente).
        _resto_pend = ~_es_paa & ~_inc & ~_es_invest & ~_serv & ~_es_doc
        sin_ded = sin_ded.with_columns(
            pl.when(_resto_pend).then(pl.col("importe_ec")).otherwise(0.0)
              .sum().over("per_id").alias("_masa_residual_persona")
        )
        m_resid = _resto_pend & (pl.col("_masa_residual_persona") < _UMBRAL_RESIDUAL)
        m_pend = _resto_pend & (pl.col("_masa_residual_persona") >= _UMBRAL_RESIDUAL)
        sin_ded = sin_ded.with_columns(
            pl.when(m_paa).then(pl.col("_act_paa"))
              .when(m_uji).then(pl.lit("UJI"))
              .when(m_inv).then(pl.lit(_INVESTIGADOR_SIN_PROYECTO_ACT))
              .when(m_serv).then(pl.lit("UJI"))
              .when(m_doc).then(pl.lit(_DOCENTE_PURO_SIN_POD_ACT))
              .when(m_resid).then(pl.lit("UJI"))
              .otherwise(pl.lit("pendiente"))
              .alias("_act_fb"),
            pl.when(m_paa).then(pl.col("_cc_paa"))
              .when(m_uji).then(pl.lit("UJI"))
              .when(m_inv).then(pl.lit(_INVESTIGADOR_SIN_PROYECTO_CC))
              .when(m_serv).then(pl.lit("UJI"))
              .when(m_doc).then(pl.lit(_DOCENTE_PURO_SIN_POD_CC))
              .when(m_resid).then(pl.lit("UJI"))
              .otherwise(pl.lit("pendiente"))
              .alias("_cc_fb"),
        )

        def _resumen(mask: pl.Expr) -> tuple[int, float]:
            sub = sin_ded.filter(mask)
            return sub["per_id"].n_unique(), float(sub["importe_ec"].sum())

        n_paa, imp_paa = _resumen(m_paa)
        n_uji, imp_uji = _resumen(m_uji)
        n_inv, imp_inv = _resumen(m_inv)
        n_serv, imp_serv = _resumen(m_serv)
        n_doc, imp_doc = _resumen(m_doc)
        n_resid, imp_resid = _resumen(m_resid)
        n_pendientes, imp_pend = _resumen(m_pend)
        if n_paa > 0:
            print(
                f"    ℹ {n_paa:,} associats assistencials (PAA) de ciencias "
                f"de la salud sin POD ({imp_paa:,.2f} €) — repartido al grado "
                f"de su departamento."
            )
        if n_uji > 0:
            print(
                f"    ℹ {n_uji:,} personas con masa regla 23 sin "
                f"dedicación pero con perfil de incentivos residuales "
                f"({imp_uji:,.2f} €) — repartido a (UJI, UJI)."
            )
        if n_inv > 0:
            print(
                f"    ℹ {n_inv:,} investigadores (PVI/PI) sin proyecto ni "
                f"grupo imputable ({imp_inv:,.2f} €) — repartido a "
                f"(actividad={_INVESTIGADOR_SIN_PROYECTO_ACT}, "
                f"centro={_INVESTIGADOR_SIN_PROYECTO_CC})."
            )
        if n_serv > 0:
            print(
                f"    ℹ {n_serv:,} funcionarios en servicios especiales "
                f"(solo trienios, sin sueldo base) ({imp_serv:,.2f} €) — "
                f"repartido a (UJI, UJI)."
            )
        if n_doc > 0:
            print(
                f"    ℹ {n_doc:,} figuras puramente docentes (associat/"
                f"substitut) sin POD ({imp_doc:,.2f} €) — repartido a "
                f"(actividad={_DOCENTE_PURO_SIN_POD_ACT}, "
                f"centro={_DOCENTE_PURO_SIN_POD_CC})."
            )
        if n_resid > 0:
            print(
                f"    ℹ {n_resid:,} personas con masa residual < "
                f"{_UMBRAL_RESIDUAL:,.0f} € ({imp_resid:,.2f} €) — barrido "
                f"a (UJI, UJI) por la regla escoba."
            )
        if n_pendientes > 0:
            print(
                f"    ⚠ {n_pendientes:,} personas con masa regla 23 sin "
                f"dedicación y masa ≥ {_UMBRAL_RESIDUAL:,.0f} € "
                f"({imp_pend:,.2f} €) — se mantienen en (pendiente, "
                f"pendiente) como anomalía a revisar."
            )
        sin_ded_uc = sin_ded.select(
            "per_id", "expediente", "_ec", "importe_ec",
            pl.col("_act_fb").alias("actividad"),
            pl.col("_cc_fb").alias("centro_de_coste"),
            pl.lit(1.0).alias("peso"),
        )
    else:
        sin_ded_uc = None

    # Producto cartesiano (per_id × expediente × ec) × (per_id ×
    # actividad × centro). El expediente se conserva en la UC final.
    uc = masa_pp.join(pesos, on="per_id", how="inner").with_columns(
        (pl.col("importe_ec") * pl.col("peso")).round(2).alias("importe"),
    )
    cols_comunes = [
        "per_id", "expediente", "_ec", "importe_ec",
        "actividad", "centro_de_coste", "peso", "importe",
    ]
    if sin_ded_uc is not None:
        sin_ded_uc = sin_ded_uc.with_columns(
            (pl.col("importe_ec") * pl.col("peso")).round(2).alias("importe"),
        )
        uc = pl.concat([
            uc.select(cols_comunes), sin_ded_uc.select(cols_comunes),
        ], how="vertical_relaxed")
    if not uc_sindical_pre.is_empty():
        uc = pl.concat([
            uc.select(cols_comunes), uc_sindical_pre.select(cols_comunes),
        ], how="vertical_relaxed")
    if override_uc is not None:
        override_uc = override_uc.with_columns(
            (pl.col("importe_ec") * pl.col("peso")).round(2).alias("importe"),
        )
        uc = pl.concat([
            uc.select(cols_comunes), override_uc.select(cols_comunes),
        ], how="vertical_relaxed")

    # Esquema UC estándar.
    uc = uc.with_columns(
        (pl.lit("R23-") + pl.col("per_id").cast(pl.Utf8) + pl.lit("-")
         + pl.col("expediente").cast(pl.Utf8) + pl.lit("-")
         + pl.col("_ec") + pl.lit("-") + pl.col("actividad")
         + pl.lit("-") + pl.col("centro_de_coste")).alias("origen_id"),
    )
    uc_out = uc.with_row_index("_n", offset=1).select(
        (pl.lit("R23-") + pl.col("_n").cast(pl.Utf8).str.zfill(6)).alias("id"),
        pl.col("_ec").alias("elemento_de_coste"),
        pl.col("centro_de_coste"),
        pl.col("actividad"),
        pl.col("importe"),
        pl.lit("regla_23").alias("origen"),
        pl.col("origen_id"),
        pl.col("peso").alias("origen_porción"),
        pl.col("per_id"),
        pl.col("expediente"),
    )

    dir_out = ruta_base / "fase1" / "regla23"
    dir_out.mkdir(parents=True, exist_ok=True)
    uc_out.write_parquet(dir_out / "uc_reparto_regla_23.parquet")
    return uc_out


def _detecta_incentivos_residuales(
    masa: pl.DataFrame, personas: pl.DataFrame,
) -> pl.DataFrame:
    """Marca personas cuyo perfil retributivo es «solo incentivos del año
    anterior». Criterio sobre la *masa regla 23* del año:

    - Tiene al menos una línea con #campo("tipo_coste") = #val("V")
      (retribución variable propia del ejercicio).
    - Esa retribución variable está concentrada en marzo y el único
      #campo("concepto_retributivo") variable es #val("67") (OTVARS
      / incentivos del ejercicio anterior).

    Las atrasos (#campo("tipo_coste") = #val("I")) no cuentan: pueden
    estar o no, y en cualquier mes.

    Devuelve un DataFrame con (#campo("per_id"),
    #campo("_es_incentivos_residuales") = #val("True")). Sólo incluye
    filas marcadas, para que el `join` sea económico.
    """
    if personas.is_empty() or masa.is_empty():
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "_es_incentivos_residuales": pl.Boolean,
        })
    n_v = masa.filter(pl.col("tipo_coste") == "V")
    if n_v.is_empty():
        return pl.DataFrame(schema={
            "per_id": pl.Int64,
            "_es_incentivos_residuales": pl.Boolean,
        })
    perfil_v = (
        n_v.group_by("per_id")
        .agg(
            pl.col("fecha").dt.month().unique().sort().alias("_meses_V"),
            pl.col("concepto_retributivo").cast(pl.Utf8).unique().sort().alias("_crs_V"),
        )
    )
    marcadas = perfil_v.filter(
        (pl.col("_meses_V") == pl.lit([_MES_INCENTIVOS]))
        & (pl.col("_crs_V") == pl.lit([_CR_INCENTIVOS_AÑO_ANTERIOR]))
    ).join(personas, on="per_id", how="inner").select(
        "per_id",
        pl.lit(True).alias("_es_incentivos_residuales"),
    )
    return marcadas


def _detecta_servicios_especiales(
    masa: pl.DataFrame, personas: pl.DataFrame,
) -> pl.DataFrame:
    """Marca a los funcionarios en servicios especiales: en la masa
    regla 23 perciben trienios (#campo("CR 03")) pero no sueldo base
    (#campo("CR 01")). Es la huella de quien no presta servicio en la
    UJI pero cuyos trienios consolidados sigue pagando la UJI.

    Devuelve (#campo("per_id"), #campo("_es_servicios_especiales") =
    #val("True")) solo para las filas marcadas.
    """
    schema = {"per_id": pl.Int64, "_es_servicios_especiales": pl.Boolean}
    if personas.is_empty() or masa.is_empty():
        return pl.DataFrame(schema=schema)
    m = masa.join(personas.select("per_id").unique(), on="per_id", how="inner")
    if m.is_empty():
        return pl.DataFrame(schema=schema)
    perfil = m.group_by("per_id").agg(
        pl.col("concepto_retributivo").cast(pl.Utf8).unique().alias("_crs")
    )
    return perfil.filter(
        pl.col("_crs").list.contains(_CR_TRIENIOS)
        & ~pl.col("_crs").list.contains(_CR_SOU_BASE)
    ).select(
        "per_id", pl.lit(True).alias("_es_servicios_especiales"),
    )


def _override_paa_salud(
    masa: pl.DataFrame, ruta_base: Path,
) -> pl.DataFrame:
    """per_id → (actividad, centro) de grado para los PAA de ciencias de
    la salud, según el departamento (centro del servicio de su nómina).

    Devuelve solo las personas a las que aplica la regla, con columnas
    (#campo("per_id"), #campo("_act_paa"), #campo("_cc_paa")). Vacío si
    no hay PAA o falta `servicios.xlsx`.
    """
    vacío = pl.DataFrame(schema={
        "per_id": pl.Int64, "_act_paa": pl.Utf8, "_cc_paa": pl.Utf8,
    })
    if "categoría" not in masa.columns or "servicio" not in masa.columns:
        return vacío
    paa = masa.filter(
        (pl.col("categoría").cast(pl.Utf8) == _CATEGORÍA_PAA)
        & pl.col("servicio").is_not_null()
    )
    if paa.is_empty():
        return vacío
    serv_path = ruta_base / "entrada" / "inventario" / "servicios.xlsx"
    if not serv_path.exists():
        return vacío
    # Servicio predominante por persona (su departamento de referencia).
    serv_pers = (
        paa.group_by("per_id", pl.col("servicio").cast(pl.Utf8).alias("_serv"))
        .len()
        .sort("len", descending=True)
        .unique("per_id", keep="first")
        .select("per_id", "_serv")
    )
    serv = read_excel(serv_path).select(
        pl.col("servicio").cast(pl.Utf8).alias("_serv"),
        pl.col("centro").cast(pl.Utf8).alias("_depto"),
    )
    mapping = pl.DataFrame(
        {
            "_depto": list(_DEPTO_SALUD_A_GRADO.keys()),
            "_act_paa": [a for a, _ in _DEPTO_SALUD_A_GRADO.values()],
            "_cc_paa": [c for _, c in _DEPTO_SALUD_A_GRADO.values()],
        }
    )
    return (
        serv_pers.join(serv, on="_serv", how="left")
        .join(mapping, on="_depto", how="inner")
        .select("per_id", "_act_paa", "_cc_paa")
        .unique("per_id")
    )


def _esquema_vacío() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "id": pl.Utf8,
        "elemento_de_coste": pl.Utf8,
        "centro_de_coste": pl.Utf8,
        "actividad": pl.Utf8,
        "importe": pl.Float64,
        "origen": pl.Utf8,
        "origen_id": pl.Utf8,
        "origen_porción": pl.Float64,
        "per_id": pl.Int64,
        "expediente": pl.Int64,
    })
