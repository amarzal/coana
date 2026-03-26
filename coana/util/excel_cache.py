"""Carga de ficheros Excel con caché automática en formato Parquet.

Si el fichero .parquet existe y es más reciente que el .xlsx/.xls origen,
se lee directamente el parquet.  En caso contrario se lee el Excel,
se convierte a parquet y se devuelve el DataFrame resultante.
"""

from pathlib import Path

import polars as pl


def _parquet_path(excel_path: Path) -> Path:
    return excel_path.parent / "_parquet" / (excel_path.stem + ".parquet")


def _needs_refresh(excel_path: Path, parquet_path: Path) -> bool:
    if not parquet_path.exists():
        return True
    return excel_path.stat().st_mtime > parquet_path.stat().st_mtime


def read_excel(
    path: str | Path,
    *,
    sheet_name: str | None = None,
) -> pl.DataFrame:
    """Lee un fichero Excel usando caché parquet transparente.

    Parameters
    ----------
    path:
        Ruta al fichero .xlsx / .xls.
    sheet_name:
        Nombre de la hoja.  Si es ``None`` se lee la primera hoja.

    Returns
    -------
    pl.DataFrame
    """
    excel_path = Path(path)
    parquet = _parquet_path(excel_path)

    if not excel_path.exists():
        if parquet.exists():
            return pl.read_parquet(parquet)
        raise FileNotFoundError(f"No se encuentra el fichero Excel: {excel_path}")

    if _needs_refresh(excel_path, parquet):
        kwargs: dict = {}
        if sheet_name is not None:
            kwargs["sheet_name"] = sheet_name
        df = pl.read_excel(excel_path, **kwargs)
        parquet.parent.mkdir(exist_ok=True)
        df.write_parquet(parquet)
        return df

    return pl.read_parquet(parquet)
