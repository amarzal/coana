"""Tests para coana.web.services.query."""

import polars as pl

from coana.web.services.query import QueryParams, apply_query


def _df():
    return pl.DataFrame({
        "id": ["P-001", "P-002", "P-003", "P-004", "P-005"],
        "ec": ["energía-eléctrica", "agua", "gas", "ENERGÍA", "Energia"],
        "importe": [100.0, 50.0, 200.0, 75.0, 25.0],
    })


def test_apply_query_sin_filtros_devuelve_todo():
    df = _df()
    out, total = apply_query(df, QueryParams(limit=10))
    assert total == 5
    assert out.height == 5


def test_apply_query_filtro_substring_insensible():
    df = _df()
    out, total = apply_query(df, QueryParams(q="energ"))
    # Encuentra 'energía-eléctrica', 'ENERGÍA', 'Energia' (3 filas).
    assert total == 3
    assert out.height == 3


def test_apply_query_filtro_columna_concreta():
    df = _df()
    # 'P' aparece en la columna id pero no en ec → restringir a ec da 0.
    out, total = apply_query(df, QueryParams(q="P", column="ec"))
    assert total == 0
    assert out.is_empty()


def test_apply_query_sort_asc_y_desc():
    df = _df()
    asc, _ = apply_query(df, QueryParams(sort_by="importe", desc=False, limit=10))
    desc, _ = apply_query(df, QueryParams(sort_by="importe", desc=True, limit=10))
    assert asc["importe"].to_list() == [25.0, 50.0, 75.0, 100.0, 200.0]
    assert desc["importe"].to_list() == [200.0, 100.0, 75.0, 50.0, 25.0]


def test_apply_query_paginacion():
    df = _df()
    out, total = apply_query(df, QueryParams(sort_by="id", offset=2, limit=2))
    assert total == 5
    assert out["id"].to_list() == ["P-003", "P-004"]


def test_apply_query_df_vacio_no_falla():
    df = pl.DataFrame({"id": [], "importe": []}, schema={"id": pl.Utf8, "importe": pl.Float64})
    out, total = apply_query(df, QueryParams(q="nada"))
    assert total == 0
    assert out.is_empty()
