"""Fase de **reparto de actividades** (reparto de costes dag).

Toma el conjunto consolidado de unidades de coste de la fase 1 y reparte
cada UC cuya actividad es de «gestión agregada» (prefijo ``dag-``) entre
las actividades finalistas (no-dag) de su mismo centro de coste, en
proporción al coste no-dag de cada actividad en ese centro. La UC dag
original desaparece y se sustituye por sus fragmentos; cada fragmento
recuerda su procedencia (``origen_id``) y la etiqueta dag de la que
proviene (``marca_dag``).

Es una fase posterior e independiente de la fase 1, con su propio botón
en la aplicación. Los informes de la fase 2 la consumirán más adelante.
"""

from __future__ import annotations

__all__ = ["ejecutar", "main"]


def ejecutar(*args, **kwargs):  # pragma: no cover - thin wrapper
    from coana.reparto.reparto import ejecutar as _ejecutar

    return _ejecutar(*args, **kwargs)


def main() -> None:  # pragma: no cover
    ejecutar()
