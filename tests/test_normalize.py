"""Tests para coana.util.normalize."""

import polars as pl

from coana.util.normalize import col_sin_tildes, sin_tildes


def test_sin_tildes_basico():
    assert sin_tildes("José") == "jose"
    assert sin_tildes("MAYÚSCULAS") == "mayusculas"
    assert sin_tildes("Núñez") == "nunez"


def test_sin_tildes_no_altera_eñe_porque_es_n_con_tilde():
    # 'ñ' se descompone como n + ̃ (NFD); al quitar diacríticos queda 'n'.
    assert sin_tildes("año") == "ano"


def test_col_sin_tildes_match():
    df = pl.DataFrame({"nom": ["José", "MARIA", "Núñez", "JoSE"]})
    norm = df.with_columns(_n=col_sin_tildes("nom"))
    assert norm["_n"].to_list() == ["jose", "maria", "nunez", "jose"]


def test_col_sin_tildes_busqueda_substring():
    """Filtrar 'jose' (sin tildes) tras normalizar encuentra 'José' y 'JOSÉ MARIA'."""
    df = pl.DataFrame({"nom": ["José", "JOSÉ MARIA", "Pepe", "Maria"]})
    filtrado = df.filter(col_sin_tildes("nom").str.contains("jose", literal=True))
    assert sorted(filtrado["nom"].to_list()) == ["JOSÉ MARIA", "José"]
