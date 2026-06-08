"""Registro **canónico** de las fuentes de unidades de coste (UC).

Este módulo es la ÚNICA lista de la que cuelgan todos los consumidores web
que agregan UC (`resultados`, `persona_360`, `personal`). Históricamente
cada uno mantenía su propia lista paralela y se desincronizaban (faltaban
`cargos_uc`, `uc_absentismo`…), produciendo totales infravalorados y
cuadres rotos. Al añadir una fuente nueva de UC basta tocar AQUÍ.

Cada fuente declara:
- `nombre`: etiqueta estable (la usa `resultados` como columna `_origen`).
- `ruta`: parquet de origen.
- `clave`: cómo se asocia a una persona/expediente
  (`"expediente"`, `"per_id"` o `"ninguna"`).
- `importe_col`: columna de importe (algunas usan `importe_uc` o
  `ss_proporcional`).
- `rol`: clasificación funcional:
  - `"retributiva"`: coste de personal imputable por persona (entra en la
    suma de UC retributivas del cuadre persona 360º).
  - `"coste-social"`: reparto de seguridad social (persona_ss).
  - `"otra"`: presupuesto, amortizaciones, suministros (no por persona).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from coana.web.deps import DIR_AUX, DIR_FASE1

DIR_NOMINAS = DIR_AUX / "nóminas"
DIR_REGLA23 = DIR_FASE1 / "regla23"


@dataclass(frozen=True)
class FuenteUC:
    nombre: str
    ruta: Path
    clave: str  # "expediente" | "per_id" | "ninguna"
    importe_col: str = "importe"
    rol: str = "retributiva"  # "retributiva" | "coste-social" | "otra"
    # Si se da, filtra el parquet por `tipo == filtro_tipo` (p. ej.
    # persona_uc contiene UC retributivas Y de coste social en el mismo
    # fichero; aquí queremos solo las de coste social).
    filtro_tipo: str | None = None


# ----------------------------------------------------------------------
# LA lista canónica. Orden = orden de presentación en el combinado.
# ----------------------------------------------------------------------
FUENTES_UC: list[FuenteUC] = [
    FuenteUC("presupuesto",      DIR_FASE1 / "uc presupuesto.parquet",      "ninguna", rol="otra"),
    FuenteUC("amortizaciones",   DIR_FASE1 / "uc amortizaciones.parquet",   "ninguna", rol="otra"),
    FuenteUC("suministros",      DIR_FASE1 / "uc suministros.parquet",      "ninguna", rol="otra"),
    FuenteUC("nóminas-PTGAS",    DIR_NOMINAS / "uc_ptgas.parquet",          "expediente"),
    FuenteUC("nóminas-PVI",      DIR_NOMINAS / "uc_pvi.parquet",            "expediente"),
    FuenteUC("nóminas-PDI",      DIR_NOMINAS / "uc_pdi.parquet",            "expediente"),
    FuenteUC("despidos",         DIR_NOMINAS / "uc_despidos.parquet",       "per_id"),
    FuenteUC("indemnizaciones",  DIR_NOMINAS / "uc_indemnizaciones_asistencias.parquet", "per_id"),
    FuenteUC("cargos",           DIR_NOMINAS / "uc_cargos.parquet",         "per_id"),
    FuenteUC("cargos-general",   DIR_NOMINAS / "cargos_uc.parquet",         "per_id", importe_col="importe_uc"),
    FuenteUC("absentismo",       DIR_NOMINAS / "uc_absentismo.parquet",     "per_id"),
    FuenteUC("regla-23",         DIR_REGLA23 / "uc_reparto_regla_23.parquet", "per_id"),
    # Coste social: se lee de persona_uc (tipo=coste social), que SÍ trae
    # `elemento_de_coste` (ss-pdi-func, ss-ptgas, prevsoc-funcs-pdi…), a
    # diferencia de persona_ss. Mismo importe total, pero clasificable.
    FuenteUC("seguridad-social", DIR_NOMINAS / "persona_uc.parquet",        "per_id", rol="coste-social", filtro_tipo="coste social"),
]

# Vistas filtradas de uso frecuente.
FUENTES_RETRIBUTIVAS: list[FuenteUC] = [f for f in FUENTES_UC if f.rol == "retributiva"]


def fuente(nombre: str) -> FuenteUC:
    """Devuelve la fuente por nombre (KeyError si no existe)."""
    for f in FUENTES_UC:
        if f.nombre == nombre:
            return f
    raise KeyError(nombre)
