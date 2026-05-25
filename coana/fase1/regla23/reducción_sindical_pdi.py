"""Reducciones sindicales del PDI (tipos 37-40, basadas en créditos).

A diferencia de la reducción sindical del PTGAS — tipo 8 de
`reducciones laborales.xlsx`, medida en días y porcentaje de jornada
trabajada (ver `coana/fase1/nóminas/reducciones_sindicales.py`) — la del
PDI se informa en `data/entrada/reducciones pdi/` en CRÉDITOS de
reducción docente. Son dos mecanismos independientes y disjuntos por
sector; este módulo traduce los créditos del PDI a una FRACCIÓN de la
jornada anual.

Para cada PDI con reducción sindical en el curso analizado — tipo de
reducción 37 (UGT), 38 (STEPV), 39 (CCOO) o 40 (CSI-F) en
`reducciones docentes.xlsx` — la fracción de jornada dedicada a la
representación sindical es:

    fracción = créditos_sindicales
               ────────────────────────────────────────────────
               capacidad − reducción_total + créditos_sindicales

donde `capacidad` (columna `creditos`) y `reducción_total` (columna
`creditos reduccion`, que YA incluye los créditos sindicales) salen de
`carga docente.xlsx`. El denominador es, por tanto, la docencia neta
impartida más los créditos sindicales: la fracción mide qué parte de la
actividad «docencia + sindicato» es sindicato.

La fracción se aplica sobre la jornada anual del PDI
(`jornada_anual_pdi`, 1642 h): la persona dedica `fracción × 1642` h a
la representación sindical y el resto se reparte por la regla 23 (o, si
es asociado, va toda a docencia).

Casos límite (aceptados tal cual, sin corrección — ver especificación):

- Docencia neta nula (`capacidad = reducción_total`): el denominador
  colapsa a los créditos sindicales y la fracción es 1 (100 % de la
  jornada a la representación sindical) por pequeña que sea la
  reducción sindical.
- La fracción se acota a [0, 1].
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from coana.util import read_excel
from coana.util.configuración import cfg_int

# Tipos de reducción docente que corresponden a representación sindical.
# Origen: `data/entrada/reducciones pdi/tipos reducciones docentes.xlsx`.
TIPOS_SINDICALES: dict[int, str] = {
    37: "UGT",
    38: "STEPV",
    39: "CCOO",
    40: "CSI-F",
}

_DIR_ENTRADA = ("entrada", "reducciones pdi")


def _esquema_vacío() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "per_id": pl.Int64,
        "tipo": pl.Int64,
        "sindicato": pl.Utf8,
        "creditos_capacidad": pl.Float64,
        "creditos_reduccion": pl.Float64,
        "creditos_sindicales": pl.Float64,
        "fraccion_sindical": pl.Float64,
        "horas_sindicales": pl.Float64,
    })


def _calcular(ruta_base: Path, año: int) -> pl.DataFrame:
    """Calcula la tabla de reducciones sindicales del PDI (función pura)."""
    red_path = ruta_base.joinpath(*_DIR_ENTRADA, "reducciones docentes.xlsx")
    carga_path = ruta_base.joinpath(*_DIR_ENTRADA, "carga docente.xlsx")
    if not (red_path.exists() and carga_path.exists()):
        return _esquema_vacío()
    red = read_excel(red_path)
    carga = read_excel(carga_path)
    if red.is_empty() or carga.is_empty():
        return _esquema_vacío()

    # Reducciones sindicales del curso (tipos 37-40). Si una persona
    # tuviera varias filas (p. ej. dos sindicatos), se suman los créditos
    # y se toma como sindicato representativo el de mayor crédito.
    sind = red.filter(
        (pl.col("curso aca") == año)
        & pl.col("tipo reducción").is_in(list(TIPOS_SINDICALES))
    ).sort("creditos", descending=True)
    if sind.is_empty():
        return _esquema_vacío()
    por_persona = sind.group_by("per_id").agg(
        pl.col("creditos").sum().alias("creditos_sindicales"),
        pl.col("tipo reducción").first().alias("tipo"),
    )

    # Carga docente del curso: capacidad y reducción total por persona
    # (se suman si hubiera varias filas por persona).
    carga_año = (
        carga.filter(pl.col("curso aca") == año)
        .group_by("per_id")
        .agg(
            pl.col("creditos").sum().alias("creditos_capacidad"),
            pl.col("creditos reduccion").sum().alias("creditos_reduccion"),
        )
    )

    df = por_persona.join(carga_año, on="per_id", how="left")
    sin_carga = df.filter(pl.col("creditos_capacidad").is_null())
    if not sin_carga.is_empty():
        print(
            f"    ⚠ {sin_carga.height} representantes sindicales del PDI sin "
            f"carga docente {año} — se descartan."
        )
    df = df.filter(pl.col("creditos_capacidad").is_not_null())
    if df.is_empty():
        return _esquema_vacío()

    # fracción = créditos_sindicales / (capacidad − reducción + sindicales).
    df = df.with_columns(
        (pl.col("creditos_capacidad") - pl.col("creditos_reduccion")
         + pl.col("creditos_sindicales")).alias("_denom"),
    ).with_columns(
        pl.when(pl.col("_denom") > 0)
        .then((pl.col("creditos_sindicales") / pl.col("_denom")).clip(0.0, 1.0))
        .otherwise(None)
        .alias("fraccion_sindical"),
    )
    sin_frac = df.filter(pl.col("fraccion_sindical").is_null())
    if not sin_frac.is_empty():
        print(
            f"    ⚠ {sin_frac.height} representantes sindicales del PDI con "
            f"denominador ≤ 0 — se descartan."
        )
    df = df.filter(pl.col("fraccion_sindical").is_not_null())
    if df.is_empty():
        return _esquema_vacío()

    jornada = float(cfg_int("jornada_anual_pdi"))
    df = df.with_columns(
        pl.col("tipo").replace_strict(TIPOS_SINDICALES).alias("sindicato"),
        (pl.col("fraccion_sindical") * jornada).round(2).alias("horas_sindicales"),
    )
    return df.select(
        "per_id", "tipo", "sindicato",
        "creditos_capacidad", "creditos_reduccion", "creditos_sindicales",
        "fraccion_sindical", "horas_sindicales",
    ).sort("fraccion_sindical", descending=True)


def reducciones_sindicales_pdi(
    ruta_base: Path = Path("data"),
    año: int = 2025,
) -> pl.DataFrame:
    """Calcula y persiste `reducciones_sindicales_pdi.parquet`.

    Una fila por PDI con representación sindical en el curso, con la
    capacidad y reducción docentes, la fracción de jornada sindical y
    las horas anuales correspondientes.
    """
    ruta_base = Path(ruta_base)
    df = _calcular(ruta_base, año)
    dir_out = ruta_base / "fase1" / "regla23"
    dir_out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(dir_out / "reducciones_sindicales_pdi.parquet")
    return df


def fracción_sindical_pdi(
    ruta_base: Path = Path("data"),
    año: int = 2025,
) -> dict[int, float]:
    """Devuelve ``{per_id: fracción}`` para el PDI con reducción sindical.

    Solo incluye personas con fracción > 0. No persiste nada (lo hace
    :func:`reducciones_sindicales_pdi`)."""
    df = _calcular(Path(ruta_base), año)
    if df.is_empty():
        return {}
    return {
        int(p): float(f)
        for p, f in zip(df["per_id"], df["fraccion_sindical"])
        if f is not None and f > 0.0
    }
