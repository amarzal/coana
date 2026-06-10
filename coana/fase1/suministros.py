"""Generación de unidades de coste a partir de suministros especiales.

Los ficheros ``energía.xlsx``, ``agua.xlsx`` y ``gas.xlsx`` contienen
costes por prefijo de zona del campus.  Para cada línea, se distribuye
el coste entre los centros con presencia en esa zona/edificación/complejo,
usando concordancia del prefijo más largo.
"""

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from coana.fase1.inventario.procesamiento import (
    ResultadoInventario,
    reparto_por_prefijo_más_largo,
)
from coana.util.excel_cache import read_excel


# Configuración de los tres ficheros de suministros:
# (fichero, elemento de coste, origen, columna del corrector de superficie).
# Energía y gas (coste energético) usan `corrector_energía`; agua usa
# `corrección_otros` (hoy vacía ⇒ sin corrección).
_SUMINISTROS: list[tuple[str, str, str, str]] = [
    ("energía", "energía-eléctrica", "energía", "corrector_energía"),
    ("agua", "agua", "agua", "corrección_otros"),
    ("gas", "gas", "gas", "corrector_energía"),
]


@dataclass
class EstadísticaSuministro:
    """Estadísticas de un fichero de suministro para la traza."""

    nombre: str
    elemento_de_coste: str
    n_líneas: int
    importe_original: float
    n_uc: int
    importe_uc: float
    n_prefijos_sin_match: int
    prefijos_sin_match: list[tuple[str, float, str]]  # (prefijo, coste, comentario)


def generar_uc_suministros(
    resultado_inv: ResultadoInventario,
    ruta_base: Path = Path("data"),
) -> tuple[pl.DataFrame, list[EstadísticaSuministro]]:
    """Genera unidades de coste de suministros (energía, agua, gas).

    Cada zona del campus se asigna al prefijo más largo del fichero que
    concuerda con su código, y el coste de cada línea se reparte entre
    los centros según su presencia (m² corregidos por el corrector de
    superficie) en las zonas asignadas a su prefijo.

    Returns
    -------
    tuple[pl.DataFrame, list[EstadísticaSuministro]]
        (DataFrame de UCs con esquema estándar, estadísticas por fichero)
    """
    dir_consumos = ruta_base / "entrada" / "consumos"
    todas_uc: list[pl.DataFrame] = []
    stats: list[EstadísticaSuministro] = []
    id_counter = 0

    for nombre_fichero, elemento_de_coste, origen_suministro, col_corrector in _SUMINISTROS:
        ruta = dir_consumos / f"{nombre_fichero}.xlsx"
        try:
            df = read_excel(ruta)
        except FileNotFoundError:
            continue

        filas_uc: list[dict] = []
        prefijos_sin_match: list[tuple[str, float, str]] = []
        importe_original = float(df["coste"].sum())

        # Cada zona se asigna al prefijo más largo del fichero que
        # concuerda con su código; los pesos son m² corregidos.
        tablas = reparto_por_prefijo_más_largo(
            [str(p).strip() for p in df["prefijo"].drop_nulls().to_list()],
            resultado_inv.presencia_zona,
            resultado_inv.corrector_superficie,
            col_corrector,
        )

        for row in df.iter_rows(named=True):
            prefijo = str(row["prefijo"]).strip()
            coste = float(row["coste"])
            comentario = str(row.get("comentario") or "")

            centros_en = tablas.get(prefijo)
            if centros_en is None:
                prefijos_sin_match.append((prefijo, coste, comentario))
                continue

            for row_c in centros_en.iter_rows(named=True):
                pct = row_c["pct"]  # ya en % (0-100)
                importe = coste * pct / 100
                id_counter += 1
                filas_uc.append({
                    "id": f"S-{id_counter:05d}",
                    "elemento_de_coste": elemento_de_coste,
                    "centro_de_coste": row_c["centro"],
                    "actividad": "dag-general-universidad",
                    "importe": importe,
                    "origen": origen_suministro,
                    "origen_id": f"{nombre_fichero}:{prefijo}",
                    "origen_porción": pct / 100,
                })

        if filas_uc:
            df_uc = pl.DataFrame(filas_uc)
            todas_uc.append(df_uc)
            n_uc = len(df_uc)
            importe_uc = float(df_uc["importe"].sum())
        else:
            n_uc = 0
            importe_uc = 0.0

        stats.append(EstadísticaSuministro(
            nombre=nombre_fichero,
            elemento_de_coste=elemento_de_coste,
            n_líneas=len(df),
            importe_original=importe_original,
            n_uc=n_uc,
            importe_uc=importe_uc,
            n_prefijos_sin_match=len(prefijos_sin_match),
            prefijos_sin_match=prefijos_sin_match,
        ))

        print(
            f"  Suministro {nombre_fichero}: {len(df)} líneas → "
            f"{n_uc} UC ({importe_uc:,.2f} € de {importe_original:,.2f} €)"
        )
        if prefijos_sin_match:
            print(f"    Prefijos sin match: {[p for p, _, _ in prefijos_sin_match]}")

    if todas_uc:
        resultado = pl.concat(todas_uc)
    else:
        resultado = pl.DataFrame(
            schema={
                "id": pl.Utf8,
                "elemento_de_coste": pl.Utf8,
                "centro_de_coste": pl.Utf8,
                "actividad": pl.Utf8,
                "importe": pl.Float64,
                "origen": pl.Utf8,
                "origen_id": pl.Utf8,
                "origen_porción": pl.Float64,
            }
        )

    return resultado, stats
