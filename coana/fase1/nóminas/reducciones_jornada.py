"""Reducciones de jornada laboral (núcleo común).

`reducciones laborales.xlsx` registra, por expediente, períodos con un
`porcentaje trabajado` y un `tipo reduccion`. Hay dos familias con
tratamiento contable distinto pero idéntica mecánica de cálculo:

- **Sindical (tipo 8):** la parte no trabajada va a la actividad
  `acción-sindical` / centro `locales-sindicales`
  (ver `reducciones_sindicales.py`).
- **Absentismo (resto de tipos):** reducciones por conciliación,
  cuidado de familiares, lactancia, etc. La parte no trabajada es un
  coste propio de la organización y va a la actividad `absentismo` /
  centro `UJI`.

Este módulo concentra lo común: el factor anual de fracción trabajada
por expediente (cálculo día a día, robusto a solapes) y la partición de
una lista de UC en dos trozos (trabajo real / destino de la reducción).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from pathlib import Path

import polars as pl

from coana.util import read_excel


_TIPO_SINDICAL = "8"

# Destino de la fracción no trabajada por absentismo.
CC_ABSENTISMO = "UJI"
ACT_ABSENTISMO = "absentismo"


def fracción_trabajada_anual(
    ruta_base: Path = Path("data"),
    año: int = 2025,
    *,
    tipos_incluidos: set[str] | None = None,
    tipos_excluidos: set[str] | None = None,
) -> dict[int, float]:
    """Devuelve ``{expediente: Y_anual}`` con la fracción del año
    efectivamente trabajada, para los expedientes cuyas reducciones
    (filtradas por tipo) solapan el año.

    El cálculo es **día a día** sobre los 365/366 días del año: para
    cada día, la fracción trabajada es ``max(0, 1 − Σ(1 − pct_i))``
    sumando las reducciones activas ese día (1.0 si no hay ninguna). Así
    los solapes no descuentan días dos veces y ``Y ∈ [0, 1]``. ``Y`` es
    la media de la fracción diaria.

    Solo se devuelven expedientes con ``Y < 1`` (al menos un día de
    reducción). Los demás deben asumirse con ``Y = 1`` implícitamente.

    Filtrado por tipo: si ``tipos_incluidos`` se da, solo esos tipos; si
    ``tipos_excluidos`` se da, todos menos esos. (Tipos como cadenas.)
    """
    src = Path(ruta_base) / "entrada" / "nóminas" / "reducciones laborales.xlsx"
    if not src.exists():
        return {}

    df = read_excel(src)
    if df.is_empty():
        return {}

    inicio_año = date(año, 1, 1)
    fin_año = date(año, 12, 31)
    días_año = (fin_año - inicio_año).days + 1  # 365 / 366

    tipo = pl.col("tipo reduccion").cast(pl.Utf8).str.strip_chars()
    if tipos_incluidos is not None:
        df = df.filter(tipo.is_in(list(tipos_incluidos)))
    if tipos_excluidos is not None:
        df = df.filter(~tipo.is_in(list(tipos_excluidos)))
    if df.is_empty():
        return {}

    fi = pl.col("fecha inicio").cast(pl.Date)
    ff = pl.col("fecha fin").cast(pl.Date)
    df = df.filter(
        (fi.is_null() | (fi <= pl.lit(fin_año)))
        & (ff.is_null() | (ff >= pl.lit(inicio_año)))
    )
    if df.is_empty():
        return {}

    # `porcentaje trabajado`: string con coma decimal; vacío → 0 (sin
    # trabajar). Solape recortado al año.
    pct = (
        pl.col("porcentaje trabajado").cast(pl.Utf8)
        .str.replace(",", ".").cast(pl.Float64, strict=False).fill_null(0.0)
    )
    df = df.with_columns(
        pl.max_horizontal(fi.fill_null(pl.lit(inicio_año)), pl.lit(inicio_año)).alias("_ini"),
        pl.min_horizontal(ff.fill_null(pl.lit(fin_año)), pl.lit(fin_año)).alias("_fin"),
        pct.alias("_pct"),
    )

    por_exp: dict[int, list[dict]] = defaultdict(list)
    for r in df.select("expediente", "_ini", "_fin", "_pct").iter_rows(named=True):
        por_exp[int(r["expediente"])].append(r)

    out: dict[int, float] = {}
    for exp, filas in por_exp.items():
        # Reducción acumulada por día (índice 0 = 1 de enero).
        reducción = [0.0] * días_año
        for r in filas:
            i0 = max(0, (r["_ini"] - inicio_año).days)
            i1 = min(días_año - 1, (r["_fin"] - inicio_año).days)
            amt = 1.0 - float(r["_pct"])
            for d in range(i0, i1 + 1):
                reducción[d] += amt
        trabajado = sum(max(0.0, 1.0 - x) for x in reducción) / días_año
        if trabajado < 1.0:
            out[exp] = trabajado
    return out


def partir_uc(
    filas: list[dict],
    factores: dict[int, float],
    *,
    centro: str,
    actividad: str,
    next_id,
    sufijo: str,
    saltar_actividades: set[str] = frozenset(),
) -> None:
    """Parte in-place cada UC cuyo ``expediente`` esté en ``factores``.

    La UC original se reduce a ``Y × importe`` (su parte trabajada) y se
    añade una nueva UC con ``(1 − Y) × importe`` en ``centro`` /
    ``actividad`` (la parte no trabajada). ``saltar_actividades`` permite
    no volver a partir filas ya derivadas de otra reducción (cascada): al
    aplicar el absentismo tras el sindical, se salta ``acción-sindical``.

    Cada fila debe contener al menos ``expediente``, ``importe``,
    ``centro_de_coste``, ``actividad``, ``origen_id`` y
    ``origen_porción``. El ``id`` de la UC nueva se obtiene con
    ``next_id()``.
    """
    if not factores:
        return
    saltar = set(saltar_actividades)
    nuevas: list[dict] = []
    for fila in filas:
        if fila.get("actividad") in saltar:
            continue
        try:
            exp = int(fila["expediente"])
        except (KeyError, TypeError, ValueError):
            continue
        y = factores.get(exp)
        if y is None or y >= 1.0:
            continue
        importe = float(fila["importe"])
        porción = float(fila.get("origen_porción") or 1.0)
        nueva = dict(fila)
        nueva["id"] = next_id()
        nueva["centro_de_coste"] = centro
        nueva["actividad"] = actividad
        nueva["importe"] = importe * (1.0 - y)
        nueva["origen_porción"] = round(porción * (1.0 - y), 6)
        nueva["origen_id"] = f"{fila.get('origen_id', '') or ''}{sufijo}"
        nuevas.append(nueva)
        # Reducir la UC original a su parte trabajada.
        fila["importe"] = importe * y
        fila["origen_porción"] = round(porción * y, 6)
    filas.extend(nuevas)


def factor_absentismo_por_expediente(
    ruta_base: Path = Path("data"),
    año: int = 2025,
) -> dict[int, float]:
    """``{expediente: Y_anual}`` para reducciones de jornada **no**
    sindicales (todos los tipos excepto el 8) que solapan el año."""
    return fracción_trabajada_anual(
        ruta_base, año, tipos_excluidos={_TIPO_SINDICAL},
    )
