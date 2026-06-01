"""Reducciones por representación sindical (tipo 8).

Para cada expediente con al menos un día de reducción tipo 8 en el año
analizado, computa el factor anual X = fracción del año efectivamente
trabajada (cálculo día a día, ver
:func:`coana.fase1.nóminas.reducciones_jornada.fracción_trabajada_anual`).

La fracción `1 − X` se imputa a centro `locales-sindicales` y actividad
`acción-sindical`; la masa restante se procesa con la lógica habitual
(retribuciones extras, masa regla 23, etc.). Los expedientes no presentes
en `reducciones laborales.xlsx` con tipo 8 quedan implícitamente con X = 1.

El cálculo del factor y la partición de UC son comunes a las reducciones
de absentismo y viven en `reducciones_jornada.py`; aquí solo se fija el
tipo (8) y el destino (sindical).
"""

from __future__ import annotations

from pathlib import Path

from coana.fase1.nóminas.reducciones_jornada import (
    _TIPO_SINDICAL,
    fracción_trabajada_anual,
    partir_uc,
)


CC_SINDICAL = "locales-sindicales"
ACTIVIDAD_SINDICAL = "acción-sindical"


def aplicar_reducción(
    filas: list[dict],
    factores_x: dict[int, float],
    next_id,
) -> None:
    """Divide las filas de UC cuyos `expediente` estén en `factores_x`
    en una parte normal (`X × importe`) y otra sindical (`(1−X) × importe`,
    con CC=`locales-sindicales`, actividad=`acción-sindical`).

    Modifica `filas` in-place: la UC original se reduce; las sindicales
    se añaden al final.
    """
    partir_uc(
        filas, factores_x,
        centro=CC_SINDICAL, actividad=ACTIVIDAD_SINDICAL,
        next_id=next_id, sufijo="-sind",
    )


def factor_x_por_expediente(
    ruta_base: Path = Path("data"),
    año: int = 2025,
) -> dict[int, float]:
    """Devuelve ``{expediente: X_anual}`` para los expedientes con
    reducción tipo 8 que solapa el año (solo los de X < 1)."""
    return fracción_trabajada_anual(
        ruta_base, año, tipos_incluidos={_TIPO_SINDICAL},
    )
