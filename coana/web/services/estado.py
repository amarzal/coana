"""Estado de frescura del pipeline (Fase 1 → Reparto → Fase 2).

El visor lee artefactos de disco de distintas etapas. Si se reejecuta una
etapa temprana (p. ej. Fase 1) y no las posteriores, las pantallas que
leen artefactos posteriores (Reparto, Informes) muestran números
incoherentes sin avisar. Este servicio compara los mtime de las salidas
de cada etapa con las de su entrada y marca las que están *obsoletas*.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel

from coana.web.deps import DIR_BASE, DIR_FASE1

PATH_FASE1 = DIR_FASE1 / "unidades de coste.xlsx"
PATH_REPARTO = DIR_FASE1 / "reparto" / "uc_post_reparto.parquet"
DIR_INFORMES = DIR_BASE / "informes"


def _mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except OSError:
        return None


def _mtime_dir(directorio: Path, patrón: str) -> float | None:
    """mtime más RECIENTE de los ficheros que casan el patrón (o None)."""
    if not directorio.exists():
        return None
    mtimes = [p.stat().st_mtime for p in directorio.glob(patrón)]
    return max(mtimes) if mtimes else None


class EtapaEstado(BaseModel):
    clave: str
    nombre: str
    existe: bool
    mtime: float | None
    obsoleta: bool
    # Texto listo para mostrar (vacío si está al día).
    motivo: str


class EstadoPipeline(BaseModel):
    etapas: list[EtapaEstado]
    # True si alguna etapa derivada está obsoleta.
    hay_obsoletas: bool


def estado_pipeline() -> EstadoPipeline:
    f1 = _mtime(PATH_FASE1)
    rep = _mtime(PATH_REPARTO)
    inf = _mtime_dir(DIR_INFORMES, "*.yaml")

    etapas: list[EtapaEstado] = []

    etapas.append(EtapaEstado(
        clave="fase1", nombre="Fase 1 · Unidades de coste",
        existe=f1 is not None, mtime=f1, obsoleta=False,
        motivo="" if f1 is not None else "Sin ejecutar",
    ))

    # Reparto: obsoleto si su entrada (Fase 1) es más reciente.
    rep_obsoleta = rep is not None and f1 is not None and rep < f1
    etapas.append(EtapaEstado(
        clave="reparto", nombre="Reparto de actividades",
        existe=rep is not None, mtime=rep, obsoleta=rep_obsoleta,
        motivo=(
            "Obsoleto: la Fase 1 se reejecutó después. Pulsa «Reparto actividades»."
            if rep_obsoleta else
            ("" if rep is not None else "Sin ejecutar")
        ),
    ))

    # Informes (Fase 2): obsoletos si Fase 1 o Reparto son más recientes.
    ref_inf = max([t for t in (f1, rep) if t is not None], default=None)
    inf_obsoleta = inf is not None and ref_inf is not None and inf < ref_inf
    etapas.append(EtapaEstado(
        clave="informes", nombre="Fase 2 · Informes",
        existe=inf is not None, mtime=inf, obsoleta=inf_obsoleta,
        motivo=(
            "Obsoletos: Fase 1 o Reparto se reejecutaron después. Pulsa «Generar informes»."
            if inf_obsoleta else
            ("" if inf is not None else "Sin generar")
        ),
    ))

    return EstadoPipeline(
        etapas=etapas,
        hay_obsoletas=any(e.obsoleta for e in etapas),
    )
