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
    mask = pl.col("proyecto").cast(pl.Utf8).is_in(_PROYECTOS_NÓMINA)
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

    return ResultadoNóminas(
        expedientes_por_sector=expedientes_por_sector,
        importe_por_sector=importe_por_sector,
        n_antes_filtro=n_antes,
        n_filtrados=n_filtrados,
        importe_filtrado=importe_filtrado,
        uc_por_expediente=uc_por_expediente,
        n_uc_inyectadas=n_uc_inyectadas,
    )
