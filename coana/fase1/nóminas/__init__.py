"""Preprocesamiento de nóminas: agrupación por expediente y sector."""

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from coana.fase1.nóminas.contexto import ContextoNóminas

# Mapeo de sectores codificados a nombres usados en el modelo.
_MAPEO_SECTOR = {"PAS": "PTGAS", "PI": "PVI"}
# Sectores reconocidos (tras mapeo).
_SECTORES_CONOCIDOS = {"PDI", "PTGAS", "PVI"}

# Prelación de sectores para asignar UC a expedientes cuando hay varios.
_PRELACIÓN_SECTOR = ["PTGAS", "PVI", "PDI", "Otros"]

# Proyectos que pasan el filtro de nóminas (spec: "Filtro de nóminas").
_PROYECTOS_NÓMINA = ["1G019", "23G019", "02G041", "11G006", "1G046", "00000"]

# Mapeo servicio → (centro_de_coste, actividad) para retribuciones ordinarias PTGAS.
_PTGAS_SERVICIO: dict[str, tuple[str, str]] = {
    "523": ("asesoría-jurídica", "dag-asesoría-jurídica"),
    "660": ("bibliotecas", "dag-biblioteca"),
    "640": ("cent", "dag-cent"),
    "263": ("consejo-social", "dag-consejo-social"),
    "2984": ("consejo-estudiantes", "dag-consejo-estudiantes"),
    "1862": ("cátedras-investigación-1I201", "otras-ait-financiación-propia-1I201"),
    "1662": ("cátedras-investigación-1I235", ""),
    "4267": ("delegado", "dag-delegado"),
    "101": ("daem", "dag-daem"),
    "93": ("deco", "dag-deco"),
    "3466": ("dea", "dag-dede"),
    "2103": ("dmc", "dag-dmc"),
    "81": ("deq", "dag-deq"),
    "2102": ("desid", "dag-desid"),
    "1442": ("dicc", "dag-dicc"),
    "1882": ("dea", "dag-dea"),
    "104": ("dhga", "dag-dhga"),
    "4207": ("dbbcn", "dag-dbbcn"),
    "2502": ("dcc", "dag-dcc"),
    "90": ("ddpub", "dag-ddpub"),
    "1883": ("dfce", "dag-dfce"),
    "2503": ("dfs", "dag-dfs"),
    "102": ("dfc", "dag-dfc"),
    "2283": ("dfis", "dag-dfis"),
    "1443": ("dlsi", "dag-dlsi"),
    "92": ("dmat", "dag-dmat"),
    "3465": ("dpdcsll", "dag-dpdcsll"),
    "97": ("dpbcp", "dag-dpbcp"),
    "96": ("dpeesm", "dag-dpeesm"),
    "2284": ("dqfa", "dag-dqfa"),
    "98": ("dqio", "dag-dqio"),
    "99": ("dtc", "dag-dtc"),
    "4": ("estce", "dag-estce"),
    "3165": ("ed", "dag-escuela-doctorado"),
    "2": ("fchs", "dag-fchs"),
    "3": ("fcje", "dag-fcje"),
    "2922": ("fcs", "dag-fcs"),
    "3405": ("rectorado", "dag-rectorado"),
    "261": ("gerencia", "dag-gerencia"),
    "4907": ("inspección-servicios", "dag-inspección-servicios"),
    "3145": ("iidl", "dag-iidl"),
    "3285": ("inam", "dag-inam"),
    "2603": ("init", "dag-init"),
    "2022": ("iupa", "dag-iupa"),
    "264": ("iutc", "dag-iutc"),
    "1982": ("labcom", "dag-labcom"),
    "4168": ("ol", "dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico"),
    "364": ("otop", "dag-otros-servicios-obras-proyectos"),
    "3408": ("oe", "dag-oe"),
    "3406": ("oir", "dag-otros-servicios-información-registro"),
    "3425": ("oiati", "dag-otros-servicios-ti"),
    "2883": ("oipep", "dag-oipep"),
    "1723": ("ocds", "cooperación"),
    "242": ("ocit", "dag-ocit"),
    "3847": ("opp", "dag-opp"),
    "4567": ("oppsm", "dag-otros-servicios-prevención-gestión-medioambiental"),
    "2882": ("ori", "dag-otros-servicios-relaciones-internacionales"),
    "1722": ("opaq", "dag-otros-servicios-promoción-evaluación-calidad"),
    "311": ("secretaría-general", "dag-secretaría-general"),
    "720": ("scic", "dag-scic"),
    "251": ("sasc", "cultura"),
    "760": ("se", "deportes"),
    "3004": ("sea", "dag-sea"),
    "1530": ("sic", "dag-sic"),
    "366": ("scp", "dag-otros-servicios-comunicación-publicaciones"),
    "1544": ("scag", "dag-scag"),
    "1529": ("sci", "dag-sci"),
    "1543": ("sge", "dag-sge"),
    "361": ("sgde", "dag-sgde"),
    "4887": ("sgit", "dag-sgit"),
    "350": ("slt", "dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico"),
    "362": ("srh", "dag-srh"),
    "2942": ("upi", "dag-upi"),
    "95": ("updtssee", "dag-updtssee"),
    "2943": ("upm", "dag-upm"),
    "3427": ("uadti", "dag-otros-servicios-ti"),
    "4167": ("gencisub", "dag-gencisub"),
    "2822": ("ui", "dag-otros-servicios-promoción-fomento-igualdad"),
    "218": ("uiic", "dag-otros-servicios-ti"),
    "4487": ("uo", "dag-uo"),
    "4687": ("udpea", "otras-extensión-universitaria-refinamiento"),
    "4488": ("udd", "dag-otros-servicios-atención-diversidad-apoyo-educativo"),
    "4489": ("ufie", "dag-ufie"),
    "344": ("sgit", "dag-sgit"),
    "3409": ("sgit", "dag-sgit"),
    "3445": ("sgit", "dag-sgit"),
    "345": ("sgit", "dag-sgit"),
    "347": ("sgit", "dag-sgit"),
    "346": ("sgit", "dag-sgit"),
    "348": ("sgit", "dag-sgit"),
    "349": ("sgit", "dag-sgit"),
    "2263": ("sgit", "dag-sgit"),
    "4647": ("sgit", "dag-sgit"),
    "2342": ("universidad-mayores", "universidad-mayores"),
    "4251": ("vevs", "dag-vevs"),
    "4252": ("vefp", "dag-vefp"),
    "4248": ("vis", "dag-vis"),
    "4250": ("vitdc", "dag-vitdc"),
    "4247": ("vi", "dag-vi"),
    "2224": ("voap", "dag-voap"),
    "4253": ("vcls", "dag-vcls"),
    "4255": ("vpee", "dag-vpee"),
    "4254": ("vri", "dag-vri"),
    "4249": ("vrspii", "dag-vrspii"),
}

# Excepción servicio 368: centro_plaza → (centro_de_coste, actividad).
_PTGAS_CENTRO_PLAZA: dict[str, tuple[str, str]] = {
    "2": ("ps-fchs", "dag-conserjería-fchs"),
    "3": ("ps-fcje", "dag-conserjería-fcje"),
    "4": ("ps-estce", "dag-conserjería-estce"),
    "212": ("ps-rectorado", "dag-conserjería-rectorado"),
    "263": ("ps-escuela-doctorado-consejo-social", "dag-conserjería-consejo-social"),
    "2402": ("ps-parque-tecnológico", "dag-conserjería-parque-tecnológico"),
    "2922": ("ps-fcs", "dag-conserjería-fcs"),
}


@dataclass
class ResultadoNóminas:
    """Estadísticas del preprocesamiento de nóminas."""

    expedientes_por_sector: dict[str, int]
    importe_por_sector: dict[str, float]
    n_antes_filtro: int
    n_filtrados: int
    importe_filtrado: float
    # UC de presupuesto inyectadas, por expediente.
    uc_por_expediente: dict[int, pl.DataFrame] = field(default_factory=dict)
    n_uc_inyectadas: int = 0
    # UC generadas a partir de retribuciones ordinarias PTGAS.
    uc_ptgas: pl.DataFrame = field(default_factory=pl.DataFrame)
    # UC generadas a partir de retribuciones PVI.
    uc_pvi: pl.DataFrame = field(default_factory=pl.DataFrame)


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


def _asignar_uc_a_expedientes(
    uc_presupuesto: pl.DataFrame,
    expedientes: pl.DataFrame,
) -> dict[int, pl.DataFrame]:
    """Asigna UC de presupuesto (cap.1 / apl.2321) a expedientes de nóminas.

    Usa per_id_endosatario para buscar expedientes de la persona.
    Prelación: PTGAS > PVI > PDI > Otros.
    """
    if uc_presupuesto.is_empty():
        return {}

    # Solo UC con per_id_endosatario
    uc = uc_presupuesto.filter(pl.col("per_id_endosatario").is_not_null())
    if uc.is_empty():
        return {}

    # Mapear expedientes con sector
    exp_con_sector = _mapear_sector(expedientes)

    # Para cada per_id, determinar el expediente preferido
    # Construir tabla per_id → expediente con prelación
    exp_con_sector = exp_con_sector.with_columns(
        pl.col("sector_mapeado")
        .replace({s: str(i) for i, s in enumerate(_PRELACIÓN_SECTOR)})
        .alias("_prioridad"),
    )

    # Escoger el expediente de mayor prioridad (menor _prioridad) por per_id
    mejor_exp = (
        exp_con_sector
        .sort("_prioridad")
        .group_by("per_id")
        .first()
        .select("per_id", "expediente")
    )

    # Join UC con el expediente escogido
    uc_con_exp = uc.join(
        mejor_exp,
        left_on="per_id_endosatario",
        right_on="per_id",
        how="inner",
    )

    if uc_con_exp.is_empty():
        return {}

    # Agrupar por expediente
    resultado: dict[int, pl.DataFrame] = {}
    for exp_id in uc_con_exp["expediente"].unique().to_list():
        df_exp = uc_con_exp.filter(pl.col("expediente") == exp_id).drop("expediente")
        resultado[int(exp_id)] = df_exp

    return resultado


# Mapeo categoría → XXX del elemento de coste PTGAS (ptgas-XXX-YYY).
# La excepción de FC + per_id 65214 → "dir" se trata en código.
_PTGAS_CAT_XXX: dict[str, str] = {
    "FC": "func", "FI": "func", "E": "ev",
    "LE": "lab", "LF": "lab", "LT": "lab",
}

# Mapeo concepto_retributivo → YYY del elemento de coste PTGAS.
_PTGAS_CR_YYY: dict[str, str] = {
    "01": "sueldo", "03": "trienios", "04": "paga-extra", "05": "paga-extra",
    "06": "esp", "10": "dst", "15": "esp", "25": "prod", "32": "prod",
    "34": "otvars", "47": "otvars", "48": "otvars", "53": "prod",
    "55": "prod", "71": "esp", "75": "cprof", "76": "cprof",
    "82": "sueldo", "83": "otvars", "87": "otvars", "90": "prod",
    "98": "trienios",
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


def _generar_uc_ptgas(nóminas_filtradas: pl.DataFrame, expedientes: pl.DataFrame) -> pl.DataFrame:
    """Genera UC a partir de retribuciones ordinarias del PTGAS.

    Agrupa por (expediente, elemento_de_coste, servicio).
    Elemento de coste = ptgas-XXX-YYY según categoría y concepto_retributivo.
    Excepción: servicio 368 usa centro_plaza para CC/actividad.
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

    # Solo retribuciones ordinarias (proyecto 1G019 o 23G019, no SS)
    es_ss = pl.col("aplicación").cast(pl.Utf8).str.starts_with("12")
    es_ord = pl.col("proyecto").cast(pl.Utf8).is_in(["1G019", "23G019"])
    ordinarias = registros.filter(~es_ss & es_ord)

    if ordinarias.is_empty():
        return pl.DataFrame()

    # Calcular elemento de coste para cada registro
    ecs = [
        _elemento_coste_ptgas(
            row["categoría"], row["concepto_retributivo"], row["per_id"],
        )
        for row in ordinarias.select("categoría", "concepto_retributivo", "per_id").iter_rows(named=True)
    ]
    ordinarias = ordinarias.with_columns(pl.Series("_ec", ecs))

    # Log errores
    errores = ordinarias.filter(pl.col("_ec").is_null())
    if not errores.is_empty():
        n_err = len(errores)
        imp_err = float(errores["importe"].sum())
        print(f"    ⚠ {n_err:,} registros PTGAS sin elemento de coste ({imp_err:,.2f} €)")

    ordinarias = ordinarias.filter(pl.col("_ec").is_not_null())

    srv = pl.col("servicio").cast(pl.Utf8)
    resto = ordinarias.filter(srv != "368")
    srv_368 = ordinarias.filter(srv == "368")

    uc_partes: list[pl.DataFrame] = []
    _id_counter = [0]

    def _next_id() -> str:
        _id_counter[0] += 1
        return f"N-{_id_counter[0]:05d}"

    # Resto: agrupar por (expediente, _ec, servicio) y mapear
    if not resto.is_empty():
        agrup = (
            resto.group_by("expediente", "_ec", "servicio")
            .agg(pl.col("importe").sum())
        )
        filas = []
        for row in agrup.iter_rows(named=True):
            srv_key = str(row["servicio"])
            mapping = _PTGAS_SERVICIO.get(srv_key)
            if mapping is None:
                continue
            cc, act = mapping
            filas.append({
                "id": _next_id(),
                "expediente": row["expediente"],
                "elemento_de_coste": row["_ec"],
                "centro_de_coste": cc,
                "actividad": act,
                "importe": row["importe"],
                "origen": "nómina",
                "origen_id": f"PTGAS-exp-{row['expediente']}-srv-{srv_key}",
                "origen_porción": 1.0,
            })
        if filas:
            uc_partes.append(pl.DataFrame(filas))

    # Servicio 368: agrupar por (expediente, _ec, centro_plaza) y mapear
    if not srv_368.is_empty() and "centro_plaza" in srv_368.columns:
        agrup_368 = (
            srv_368.group_by("expediente", "_ec", "centro_plaza")
            .agg(pl.col("importe").sum())
        )
        filas_368 = []
        for row in agrup_368.iter_rows(named=True):
            cp_key = str(row["centro_plaza"])
            mapping = _PTGAS_CENTRO_PLAZA.get(cp_key)
            if mapping is None:
                continue
            cc, act = mapping
            filas_368.append({
                "id": _next_id(),
                "expediente": row["expediente"],
                "elemento_de_coste": row["_ec"],
                "centro_de_coste": cc,
                "actividad": act,
                "importe": row["importe"],
                "origen": "nómina",
                "origen_id": f"PTGAS-exp-{row['expediente']}-srv-368-cp-{cp_key}",
                "origen_porción": 1.0,
            })
        if filas_368:
            uc_partes.append(pl.DataFrame(filas_368))

    if not uc_partes:
        return pl.DataFrame()

    return pl.concat(uc_partes)


def _generar_uc_pvi(nóminas_filtradas: pl.DataFrame, expedientes: pl.DataFrame) -> pl.DataFrame:
    """Genera UC a partir de retribuciones del PVI.

    Agrupa por (expediente, proyecto) y crea una UC por cada grupo.
    """
    exp_pvi = _mapear_sector(expedientes)
    exp_pvi = exp_pvi.filter(pl.col("sector_mapeado") == "PVI")
    if exp_pvi.is_empty():
        return pl.DataFrame()

    registros = nóminas_filtradas.join(
        exp_pvi.select("expediente", "per_id"),
        on="expediente",
        how="inner",
    )

    # Retribuciones = todo menos SS
    es_ss = pl.col("aplicación").cast(pl.Utf8).str.starts_with("12")
    retribuciones = registros.filter(~es_ss)

    if retribuciones.is_empty():
        return pl.DataFrame()

    agrup = (
        retribuciones.group_by("expediente", "proyecto")
        .agg(pl.col("importe").sum())
    )

    _id_counter = [0]

    def _next_id() -> str:
        _id_counter[0] += 1
        return f"V-{_id_counter[0]:05d}"

    filas = []
    for row in agrup.iter_rows(named=True):
        filas.append({
            "id": _next_id(),
            "expediente": row["expediente"],
            "elemento_de_coste": "",
            "centro_de_coste": "",
            "actividad": "",
            "importe": row["importe"],
            "origen": "nómina",
            "origen_id": f"PVI-exp-{row['expediente']}-proy-{row['proyecto']}",
            "origen_porción": 1.0,
        })

    if not filas:
        return pl.DataFrame()

    return pl.DataFrame(filas)


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
    uc_por_expediente: dict[int, pl.DataFrame],
    dir_salida: Path,
) -> None:
    """Genera el reparto de SS por persona agrupando UC por (actividad, centro de coste).

    Reúne todas las UC asociadas a los expedientes de cada persona (de nómina y
    de presupuesto), calcula el porcentaje de cada par (actividad, centro_de_coste)
    y reparte la SS proporcionalmente.

    Guarda:
      - persona_uc.parquet: todas las UC retributivas por persona (columnas completas)
      - persona_ss.parquet: reparto de SS por (per_id, actividad, centro_de_coste)
    """
    # Mapa expediente → per_id
    exp_per = expedientes.select("expediente", "per_id")

    # Reunir todas las UC con todas sus columnas
    partes: list[pl.DataFrame] = []
    if not uc_ptgas.is_empty():
        partes.append(uc_ptgas)
    if not uc_pvi.is_empty():
        partes.append(uc_pvi)
    for exp_id, df_uc in uc_por_expediente.items():
        if "actividad" in df_uc.columns and "centro_de_coste" in df_uc.columns:
            sub = df_uc.with_columns(pl.lit(exp_id).cast(pl.Int64).alias("expediente"))
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

    # SS total por persona: suma de registros de nómina con aplicación que empieza por "12"
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

    # Mapeo sector → elemento de coste de SS
    _EC_SS = {"PTGAS": "ss-ptgas", "PDI": "ss-pdi-func", "PVI": "ss-pvi-otpersonal"}

    # Generar UC de costes sociales (una por cada par actividad/CC con SS > 0)
    ss_base = (
        reparto.filter(pl.col("ss_proporcional") > 0)
        .join(sector_principal, on="per_id", how="left")
    )

    filas_ss = []
    ss_counter = 0
    for row in ss_base.iter_rows(named=True):
        ss_counter += 1
        sector = row.get("sector_principal", "")
        filas_ss.append({
            "per_id": row["per_id"],
            "expediente": None,
            "id": f"SS-{ss_counter:05d}",
            "elemento_de_coste": _EC_SS.get(sector, f"ss-{sector.lower()}"),
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
    uc_presupuesto: pl.DataFrame | None = None,
) -> ResultadoNóminas:
    """Filtra, agrupa nóminas por expediente, clasifica por sector y guarda parquets.

    Genera un parquet por sector (PDI, PTGAS, PVI, Otros) en *dir_salida*.

    Si se pasan *uc_presupuesto* (UC de capítulo 1 o aplicación 2321 con
    per_id_endosatario), se inyectan en los expedientes correspondientes.
    """
    dir_salida.mkdir(parents=True, exist_ok=True)

    nóminas = ctx.nóminas
    expedientes = ctx.expedientes

    if nóminas is None or expedientes is None:
        return ResultadoNóminas(
            expedientes_por_sector={},
            importe_por_sector={},
            n_antes_filtro=0,
            n_filtrados=0,
            importe_filtrado=0.0,
        )

    # -- Filtro de nóminas --
    n_antes = len(nóminas)
    # Regla 1: La SS (aplicación 1211) pasa siempre.
    # Regla 2: Solo pasan los proyectos de la lista.
    es_ss = pl.col("aplicación").cast(pl.Utf8) == "1211"
    es_proyecto = pl.col("proyecto").cast(pl.Utf8).is_in(_PROYECTOS_NÓMINA)
    mask = es_ss | es_proyecto
    filtrados = nóminas.filter(~mask)
    nóminas = nóminas.filter(mask)
    n_filtrados = len(filtrados)
    importe_filtrado = float(filtrados["importe"].sum()) if not filtrados.is_empty() else 0.0
    print(
        f"  Filtro de nóminas: {n_antes:,} → {len(nóminas):,} "
        f"({n_filtrados:,} filtradas)"
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
    uc_ptgas = _generar_uc_ptgas(nóminas, expedientes)
    if not uc_ptgas.is_empty():
        importe_ptgas = float(uc_ptgas["importe"].sum())
        print(f"  UC PTGAS retrib. ordinarias: {len(uc_ptgas):,} UC, {importe_ptgas:,.2f} €")
        uc_ptgas.write_parquet(dir_salida / "uc_ptgas.parquet")

    # -- UC de retribuciones PVI --
    uc_pvi = _generar_uc_pvi(nóminas, expedientes)
    if not uc_pvi.is_empty():
        importe_pvi = float(uc_pvi["importe"].sum())
        print(f"  UC PVI retribuciones: {len(uc_pvi):,} UC, {importe_pvi:,.2f} €")
        uc_pvi.write_parquet(dir_salida / "uc_pvi.parquet")

    # -- Inyectar UC de presupuesto en expedientes --
    uc_por_expediente: dict[int, pl.DataFrame] = {}
    n_uc_inyectadas = 0
    if uc_presupuesto is not None and not uc_presupuesto.is_empty():
        uc_por_expediente = _asignar_uc_a_expedientes(
            uc_presupuesto, expedientes,
        )
        n_uc_inyectadas = sum(len(df) for df in uc_por_expediente.values())
        n_expedientes = len(uc_por_expediente)
        print(
            f"  UC presupuesto inyectadas en nóminas: {n_uc_inyectadas:,} "
            f"en {n_expedientes:,} expedientes"
        )

    # -- Reparto de SS por persona --
    _generar_reparto_ss_persona(
        nóminas, expedientes, uc_ptgas, uc_pvi, uc_por_expediente, dir_salida,
    )

    return ResultadoNóminas(
        expedientes_por_sector=expedientes_por_sector,
        importe_por_sector=importe_por_sector,
        n_antes_filtro=n_antes,
        n_filtrados=n_filtrados,
        importe_filtrado=importe_filtrado,
        uc_por_expediente=uc_por_expediente,
        n_uc_inyectadas=n_uc_inyectadas,
        uc_ptgas=uc_ptgas,
        uc_pvi=uc_pvi,
    )
