"""Reducciones por representación sindical (tipo 8).

Para cada expediente con al menos un día de reducción tipo 8 en el año
analizado, computa el factor anual ponderado X = fracción del año
efectivamente trabajada:

    X_anual = Σ (días_solape_i × pct_trabajado_i) + días_sin_reducción
              ─────────────────────────────────────────────────────────
                                   días_del_año

Para los días sin reducción se asume `pct_trabajado = 1`. El resultado
queda en [0, 1]; los expedientes no presentes en `reducciones
laborales.xlsx` con tipo 8 quedan implícitamente con X = 1.

La fracción `1 − X` se imputa a centro `locales-sindicales` y actividad
`acción-sindical`; la masa restante se procesa con la lógica habitual
(retribuciones extras, masa regla 23, etc.).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from coana.util import read_excel


_TIPO_SINDICAL = "8"

CC_SINDICAL = "locales-sindicales"
ACTIVIDAD_SINDICAL = "acción-sindical"


def aplicar_reducción(
    filas: list[dict],
    factores_x: dict[int, float],
    next_id,
) -> None:
    """Divide las filas de UC cuyos `expediente` estén en `factores_x`
    en una parte normal (`X × importe`) y otra sindical
    (`(1−X) × importe`, con CC=`locales-sindicales`, actividad=
    `acción-sindical`).

    Modifica `filas` in-place: la UC original se reduce; las
    sindicales se añaden al final. Cada fila debe contener al menos
    `expediente`, `importe`, `centro_de_coste`, `actividad`,
    `origen_id`, `origen_porción`. El `id` de la UC sindical se
    obtiene con `next_id()`.
    """
    if not factores_x:
        return
    sindicales: list[dict] = []
    for fila in filas:
        try:
            exp = int(fila["expediente"])
        except (KeyError, TypeError, ValueError):
            continue
        x = factores_x.get(exp)
        if x is None or x >= 1.0:
            continue
        importe = float(fila["importe"])
        porción = float(fila.get("origen_porción") or 1.0)
        sind = dict(fila)
        sind["id"] = next_id()
        sind["centro_de_coste"] = CC_SINDICAL
        sind["actividad"] = ACTIVIDAD_SINDICAL
        sind["importe"] = round(importe * (1.0 - x), 2)
        sind["origen_porción"] = round(porción * (1.0 - x), 6)
        sind["origen_id"] = f"{fila.get('origen_id', '') or ''}-sind"
        sindicales.append(sind)
        # Reducir la UC original.
        fila["importe"] = round(importe * x, 2)
        fila["origen_porción"] = round(porción * x, 6)
    filas.extend(sindicales)


def factor_x_por_expediente(
    ruta_base: Path = Path("data"),
    año: int = 2025,
) -> dict[int, float]:
    """Devuelve ``{expediente: X_anual}`` para los expedientes con
    reducción tipo 8 que solapa el año.

    Solo se incluyen los expedientes cuyo factor anual sea < 1 (es
    decir, tuvieron al menos un día de reducción en el año). Los demás
    deben asumirse con X = 1 implícitamente.
    """
    src = Path(ruta_base) / "entrada" / "nóminas" / "reducciones laborales.xlsx"
    if not src.exists():
        return {}

    df = read_excel(src)
    if df.is_empty():
        return {}

    inicio_año = date(año, 1, 1)
    fin_año = date(año, 12, 31)

    # Normalizar tipo y filtrar tipo 8 con solape al año.
    tipo = pl.col("tipo reduccion").cast(pl.Utf8).str.strip_chars()
    fi = pl.col("fecha inicio").cast(pl.Date)
    ff = pl.col("fecha fin").cast(pl.Date)
    df = df.filter(tipo == _TIPO_SINDICAL).filter(
        (fi.is_null() | (fi <= pl.lit(fin_año)))
        & (ff.is_null() | (ff >= pl.lit(inicio_año)))
    )
    if df.is_empty():
        return {}

    # `porcentaje trabajado`: string con coma decimal; vacío → 0 (liberado
    # al 100 % = sin trabajar).
    pct_raw = pl.col("porcentaje trabajado").cast(pl.Utf8).fill_null("0")
    pct = pct_raw.str.replace(",", ".").cast(pl.Float64, strict=False).fill_null(0.0)

    inicio_ef = pl.max_horizontal(fi.fill_null(pl.lit(inicio_año)), pl.lit(inicio_año))
    fin_ef = pl.min_horizontal(ff.fill_null(pl.lit(fin_año)), pl.lit(fin_año))
    días_solape = (
        pl.when(fin_ef >= inicio_ef)
        .then((fin_ef - inicio_ef).dt.total_days().cast(pl.Int64) + 1)
        .otherwise(0)
    )

    df = df.with_columns(días_solape.alias("_días"), pct.alias("_pct"))

    días_año = (fin_año - inicio_año).days + 1  # 365 / 366

    # Suma de días×pct por expediente.
    agg = df.group_by("expediente").agg(
        (pl.col("_días") * pl.col("_pct")).sum().alias("dias_pct"),
        pl.col("_días").sum().alias("dias_red"),
    )
    agg = agg.with_columns(
        ((pl.col("dias_pct") + (días_año - pl.col("dias_red"))) / días_año)
        .alias("X_anual")
    )
    # Solo expedientes con X < 1 (al menos un día reduce el factor).
    agg = agg.filter(pl.col("X_anual") < 1.0)

    out: dict[int, float] = {}
    for r in agg.iter_rows(named=True):
        out[int(r["expediente"])] = float(r["X_anual"])
    return out
