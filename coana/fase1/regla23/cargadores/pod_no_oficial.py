"""Cargador POD no oficial → dedicación PDI en docencia no oficial.

Lee `estimación horas docencia propia.xlsx` y produce filas de
dedicación a actividades de docencia no oficial (formación permanente,
cursos UJI, OAD…) que se incorporan a `dedicación_pdi.parquet`.

Filtro: el proyecto presupuestario asociado a la fila debe ser de uno
de los tipos #val("EPM"), #val("EPDE"), #val("EPDEX"), #val("EPC"),
#val("EPMI"), #val("CUID"), #val("CUES") u #val("OAD"). El resto (con
financiación genérica) se descarta.

Regularización de horas: los datos de origen guardan tres campos por
fila — #campo("unidad") (horas), #campo("importe") (€/hora) y
#campo("total") (= unidad × importe). En la práctica hay casos en que
se ha registrado `unidad = 1` con un importe por hora descabellado, lo
que falsea las horas. Heurística: si #campo("importe") > #val("130 €/h")
asumimos que el campo unidad no es fiable y aproximamos las horas como
#campo("total") / #val("80 €/h"); si no, dejamos #campo("unidad") como
está.

Actividad y centro de coste:
- OAD + centro_origen UMAJ → actividad #etqact("universidad-mayores"),
  CC #etqcc("universidad-mayores").
- OAD + cualquier otro centro_origen → actividad
  #etqact("otros-docencia-propia"), CC «pendiente».
- Resto de tipos (EPM, EPDE, EPDEX, EPC, EPMI, CUID, CUES) → actividad
  #etqact("otros-docencia-propia"), CC «pendiente».

Las horas se imputan con factor ×2,5 (igual que la docencia oficial:
una hora de impartición lleva preparación + tutorías + evaluación) y
grupo `docencia_no_oficial`. Método `et` (estimación por tipología).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.util.configuración import cfg_float

_FACTOR_DOCENTE: float = cfg_float("factor_impartición_docente")

# Tipos de proyecto presupuestario que financian docencia no oficial
# propia (formación permanente, cursos UJI, OAD, idiomas…).
TIPOS_PROYECTO_NO_OFICIAL: tuple[str, ...] = (
    "EPM", "EPDE", "EPDEX", "EPC", "EPMI", "CUID", "CUES", "OAD",
)

# Umbral de €/h por encima del cual no nos fiamos del campo `unidad`.
_UMBRAL_IMPORTE_HORA: float = 130.0
# Divisor que aproxima las horas a partir del `total` cuando el dato
# original es sospechoso (≈ tarifa razonable €/h).
_TARIFA_REGULARIZADA: float = 80.0


def cargar_pod_no_oficial(
    ruta_base: Path,
    año: int = 2025,
    expedientes: pl.DataFrame | None = None,
    dir_salida_auxiliar: Path | None = None,
) -> pl.DataFrame:
    """Lee `estimación horas docencia propia.xlsx` y produce filas de
    dedicación PDI en docencia no oficial.

    Si se pasa `expedientes`, se restringe a per_id de PDI/PVI. Si se
    pasa `dir_salida_auxiliar`, se persiste un parquet de detalle
    `regla_23_horas_no_oficiales.parquet` con todas las columnas
    originales más `tipo_proyecto`, `centro_origen` y `unidad_efectiva`
    para auditoría desde la app.
    """
    src = ruta_base / "entrada" / "docencia" / "estimación horas docencia propia.xlsx"
    if not src.exists():
        return _esquema_vacío()
    df = read_excel(src)
    if df.is_empty():
        return _esquema_vacío()

    proy_path = ruta_base / "entrada" / "presupuesto" / "proyectos.xlsx"
    if not proy_path.exists():
        return _esquema_vacío()
    proy_ref = read_excel(proy_path).select(
        pl.col("proyecto").cast(pl.Utf8),
        pl.col("tipo").cast(pl.Utf8).str.strip_chars().alias("tipo_proyecto"),
        pl.col("centro_origen").cast(pl.Utf8).str.strip_chars().alias("centro_origen"),
    )

    df = (
        df.with_columns(pl.col("proyecto").cast(pl.Utf8))
        .join(proy_ref, on="proyecto", how="left")
        .filter(pl.col("tipo_proyecto").is_in(list(TIPOS_PROYECTO_NO_OFICIAL)))
    )
    if df.is_empty():
        return _esquema_vacío()

    # Regularización de horas: si €/h > umbral, aproximar por total/tarifa.
    df = df.with_columns(
        pl.when(pl.col("importe") > _UMBRAL_IMPORTE_HORA)
        .then(pl.col("total") / pl.lit(_TARIFA_REGULARIZADA))
        .otherwise(pl.col("unidad"))
        .alias("unidad_efectiva")
    )

    # Solo PDI/PVI (en la nómina el sector PVI viene codificado como `PI`).
    if expedientes is not None and not expedientes.is_empty():
        pdi_pvi = (
            expedientes.filter(pl.col("sector").is_in(["PDI", "PI", "PVI"]))
            .select(pl.col("per_id").alias("perid"))
            .unique()
        )
        if not pdi_pvi.is_empty():
            df = df.join(pdi_pvi, on="perid", how="inner")
    if df.is_empty():
        return _esquema_vacío()

    # Persistir detalle de auditoría.
    if dir_salida_auxiliar is not None:
        dir_salida_auxiliar.mkdir(parents=True, exist_ok=True)
        df.write_parquet(dir_salida_auxiliar / "regla_23_horas_no_oficiales.parquet")

    # Resolver actividad y centro_de_coste fila a fila.
    es_oad = pl.col("tipo_proyecto") == "OAD"
    es_umaj = pl.col("centro_origen") == "UMAJ"
    actividad = (
        pl.when(es_oad & es_umaj).then(pl.lit("universidad-mayores"))
        .otherwise(pl.lit("otros-docencia-propia"))
    )
    centro_cc = (
        pl.when(es_oad & es_umaj).then(pl.lit("universidad-mayores"))
        .otherwise(pl.lit("pendiente"))
    )

    detalle = pl.concat_str([
        pl.lit("Proyecto "), pl.col("proyecto"),
        pl.lit(" ("), pl.col("tipo_proyecto"), pl.lit(")"),
        pl.lit(" · "), pl.col("nombre").fill_null(""),
        pl.lit(" · "), pl.col("unidad_efectiva").round(2).cast(pl.Utf8),
        pl.lit(" h ("), pl.col("total").round(2).cast(pl.Utf8), pl.lit(" €)"),
    ])

    return (
        df.filter(pl.col("unidad_efectiva") > 0)
        .with_columns(actividad.alias("actividad"), centro_cc.alias("centro_de_coste"))
        .select(
            pl.col("perid").cast(pl.Int64).alias("per_id"),
            pl.col("actividad"),
            pl.col("centro_de_coste"),
            pl.col("unidad_efectiva").cast(pl.Float64).alias("horas"),
            pl.lit("et").alias("método"),
            pl.lit(_FACTOR_DOCENTE).alias("factor"),
            pl.lit("docencia_no_oficial").alias("grupo"),
            pl.lit("POD_no_oficial").alias("origen"),
            pl.col("gre_id").cast(pl.Utf8).alias("origen_id"),
            detalle.alias("detalle"),
            pl.lit(None, dtype=pl.Utf8).alias("anomalía"),
        )
    )


def _esquema_vacío() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "per_id": pl.Int64,
        "actividad": pl.Utf8,
        "centro_de_coste": pl.Utf8,
        "horas": pl.Float64,
        "método": pl.Utf8,
        "factor": pl.Float64,
        "grupo": pl.Utf8,
        "origen": pl.Utf8,
        "origen_id": pl.Utf8,
        "detalle": pl.Utf8,
        "anomalía": pl.Utf8,
    })
