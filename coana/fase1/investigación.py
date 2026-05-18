"""Creación de centros de coste para grupos de investigación.

Lee ``data/entrada/investigación/grupos a institutos.xlsx`` y, para
cada grupo, crea (si no existe ya) un nodo
``grupo-investigación-{id_grupo}`` bajo el centro del instituto al que
está adscrito. Los grupos no adscritos a ningún instituto se cuelgan
de un nodo virtual ``inves`` ("Grupos no adscritos a institutos") que
se crea bajo ``institutos`` la primera vez que se necesita.

El identificador del nodo del grupo es independiente del instituto:
``grupo-investigación-XXX``. Eso permite identificar el grupo solo por
su código.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from coana.util import Árbol, read_excel

# Mapping código del yaml → etiqueta en el árbol de centros de coste.
# Los nombres de los institutos en el árbol son la fuente oficial.
_INSTITUTO_A_ETIQUETA: dict[str, str] = {
    "INAM":   "inam",
    "INIT":   "init",
    "IIDL":   "iidl",
    "IIEI":   "iei",
    "IIFV":   "ifv",
    "IIG":    "iigeo",
    "IILP":   "ii-lópez-piñero",
    "IMAC":   "imac",
    "IUPA":   "iupa",
    "IUDSP":  "idsp",
    "IUDT":   "iudt",
    "IUT":    "iuturismo",
    "IUCE":   "iuce",
    "IUTC":   "iutc",
    "IULMA":  "ilma",
    "IUEFG":  "iuef",
    "INVES":  "inves",  # nodo virtual, se crea al vuelo
}

_INVES_DESCRIPCIÓN = "Grupos no adscritos a institutos"


def enriquecer_árbol_cc_con_grupos(
    ruta_base: Path, árbol_cc: Árbol,
) -> tuple[int, int]:
    """Inserta los CC de grupos de investigación bajo su instituto.

    Devuelve ``(n_creados, n_omitidos)``. ``n_omitidos`` son grupos cuyo
    código de instituto en el xlsx no tiene mapeo conocido o cuya
    etiqueta de árbol no existe.
    """
    path = ruta_base / "entrada" / "investigación" / "grupos a institutos.xlsx"
    if not path.exists():
        return 0, 0

    df = read_excel(path).select(
        pl.col("id_grupo").cast(pl.Utf8),
        pl.col("nombre_grupo").cast(pl.Utf8),
        pl.col("instituto").cast(pl.Utf8),
    )
    if df.is_empty():
        return 0, 0

    n_creados = 0
    n_omitidos = 0
    inves_creado = False

    for row in df.iter_rows(named=True):
        cod_yaml = row["instituto"]
        etiqueta_instituto = _INSTITUTO_A_ETIQUETA.get(cod_yaml)
        if etiqueta_instituto is None:
            n_omitidos += 1
            continue

        # Crear nodo virtual INVES la primera vez que se necesite.
        if cod_yaml == "INVES" and not inves_creado:
            try:
                árbol_cc.añadir_hijo(
                    "institutos", _INVES_DESCRIPCIÓN, "inves",
                    id_completo="inves",
                )
            except KeyError:
                # 'institutos' no existe en este árbol; nada que hacer.
                n_omitidos += 1
                continue
            inves_creado = True

        # Crear el nodo del grupo bajo el instituto. La etiqueta es
        # independiente del padre: grupo-investigación-XXX. Saneamos la
        # descripción para evitar saltos de línea u otros caracteres
        # que rompan el formato .tree.
        nombre = (row["nombre_grupo"] or "").replace("\n", " ").replace("|", "/")
        nombre = " ".join(nombre.split())
        if not nombre:
            nombre = f"Grupo {row['id_grupo']}"
        try:
            árbol_cc.añadir_hijo(
                etiqueta_instituto,
                nombre,
                row["id_grupo"],
                id_completo=f"grupo-investigación-{row['id_grupo']}",
            )
            n_creados += 1
        except (KeyError, ValueError):
            n_omitidos += 1

    return n_creados, n_omitidos
