"""Cargos académicos: personas que han ocupado cargos en departamentos.

Entrada:
- ``data/entrada/nóminas/personas cargos.xlsx`` — relaciones persona-cargo.
- ``data/entrada/inventario/servicios.xlsx`` — mapping servicio→centro de coste.

Salida:
- ``regla_cargos_departamentos.parquet`` — para cada departamento, las
  personas que han ocupado un cargo en al menos un día del año analizado.

De momento sólo cubre departamentos. Otras actividades (titulaciones, etc.)
se irán incorporando.
"""

from datetime import datetime
from pathlib import Path

import polars as pl

from coana.util.excel_cache import read_excel


# Centros de coste de departamentos (TABLA-TRADUCCIÓN-DEPARTAMENTOS).
_DEPTO_CC: tuple[str, ...] = (
    "daem", "dbbcn", "dcc", "ddpri", "ddpub", "updtssee", "dea", "dicc",
    "deco", "dede", "dmc", "desid", "dfs", "dfc", "dfis", "dfce", "dhga",
    "upi", "deq", "dlsi", "dmat", "upm", "dpdcsll", "dpbcp", "dpeesm",
    "dqfa", "dqio", "dtc",
)


def _filtrar_personas_cargos_año(pc: pl.DataFrame, año: int) -> pl.DataFrame:
    """Filtra ``personas cargos`` a filas con al menos un día activo en el año."""
    inicio = datetime(año, 1, 1)
    fin = datetime(año, 12, 31)
    return pc.filter(
        (pl.col("fecha_inicio") <= fin)
        & (pl.col("fecha_fin").is_null() | (pl.col("fecha_fin") >= inicio))
    )


def generar_categoría_última_pdi_pvi(
    ruta_base: Path,
    dir_salida: Path,
) -> pl.DataFrame:
    """Última categoría PDI/PVI de cada persona según concepto 19/64.

    Para cada per_id con expediente PDI/PVI que haya cobrado al menos
    una vez con concepto_retributivo 19 o 64, localiza la fila más
    reciente y guarda su categoría junto con datos del cobro.

    Persiste ``categoría_última_pdi_pvi.parquet`` con columnas:
    `per_id, categoría, fecha, importe, concepto_retributivo, proyecto,
    centro, aplicación, programa`.
    """
    nom_path = Path(ruta_base) / "entrada" / "nóminas" / "nóminas y seguridad social.xlsx"
    exp_path = Path(ruta_base) / "entrada" / "nóminas" / "expedientes recursos humanos.xlsx"
    if not nom_path.exists() or not exp_path.exists():
        return pl.DataFrame()

    nom = read_excel(nom_path)
    exp = read_excel(exp_path).filter(
        pl.col("sector").is_in(["PDI", "PI"])
    ).select("expediente", "per_id").unique()
    if exp.is_empty():
        return pl.DataFrame()

    n = nom.join(exp, on="expediente", how="inner").filter(
        pl.col("concepto_retributivo").cast(pl.Utf8).is_in(["19", "64"])
    )
    if n.is_empty():
        return pl.DataFrame()

    # Última fila por per_id (max fecha)
    última = (
        n.sort("fecha", descending=True)
        .group_by("per_id")
        .head(1)
        .select(
            "per_id", "categoría", "fecha", "importe",
            "concepto_retributivo", "proyecto", "centro",
            "aplicación", "programa",
        )
        .sort("per_id")
    )

    dir_salida.mkdir(parents=True, exist_ok=True)
    última.write_parquet(dir_salida / "categoría_última_pdi_pvi.parquet")
    print(
        f"  Categoría última PDI/PVI (concepto 19/64): {len(última):,} personas"
    )
    return última


def generar_cargos_departamentos(
    ruta_base: Path,
    dir_salida: Path,
    año: int,
) -> pl.DataFrame:
    """Persona-cargo por departamento, activas en *año*.

    Filtros previos:
    - ``personas cargos.xlsx``: filas con al menos un día activo en el año.
    - ``cargos.xlsx``: sólo cargos con ``cuantía > 0``.

    Guarda ``cargos_departamentos.parquet``.
    """
    pc_path = Path(ruta_base) / "entrada" / "nóminas" / "personas cargos.xlsx"
    serv_path = Path(ruta_base) / "entrada" / "inventario" / "servicios.xlsx"
    cargos_path = Path(ruta_base) / "entrada" / "nóminas" / "cargos.xlsx"
    if not pc_path.exists() or not serv_path.exists() or not cargos_path.exists():
        return pl.DataFrame()

    pc = _filtrar_personas_cargos_año(read_excel(pc_path), año)
    if pc.is_empty():
        return pl.DataFrame()

    # Filtro previo: cargos con cuantía > 0
    cargos_validos = (
        read_excel(cargos_path)
        .filter(pl.col("cuantía") > 0)
        .select("cargo")
    )
    pc = pc.join(cargos_validos, on="cargo", how="inner")
    if pc.is_empty():
        return pl.DataFrame()

    servicios = read_excel(serv_path).select("servicio", "centro")
    serv_dept = servicios.filter(pl.col("centro").is_in(list(_DEPTO_CC)))
    if serv_dept.is_empty():
        return pl.DataFrame()

    cargos = pc.join(serv_dept, on="servicio", how="inner").select(
        pl.col("centro").alias("centro_cc"),
        "per_id", "cargo", "servicio",
        "fecha_inicio", "fecha_fin",
        "fecha_inicio_cobra", "fecha_fin_cobra",
    )
    if cargos.is_empty():
        return pl.DataFrame()

    dir_salida.mkdir(parents=True, exist_ok=True)
    cargos.write_parquet(dir_salida / "cargos_departamentos.parquet")

    n_dep = cargos["centro_cc"].n_unique()
    n_per = cargos["per_id"].n_unique()
    print(
        f"  Cargos departamentos: {len(cargos):,} relaciones, "
        f"{n_dep:,} departamentos, {n_per:,} personas (año {año})"
    )
    return cargos
