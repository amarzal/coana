"""Tests del router de Presupuesto: contrato de los tres endpoints.

Asume que existe ``data/fase1/uc presupuesto.parquet``. Si no, los tests
se saltan: el contrato se cubre con tests unitarios de query.
"""

import pytest
from fastapi.testclient import TestClient

from coana.web.app import app
from coana.web.services.presupuesto import PATH_UC

pytestmark = pytest.mark.skipif(
    not PATH_UC.exists(),
    reason="Falta data/fase1/uc presupuesto.parquet (ejecuta la fase 1 primero)",
)

client = TestClient(app)


def test_resumen_devuelve_kpis():
    r = client.get("/api/presupuesto/_resumen")
    assert r.status_code == 200
    data = r.json()
    assert "kpis" in data
    labels = {k["label"] for k in data["kpis"]}
    assert {"UC generadas", "Importe UC"}.issubset(labels)


def test_listar_uc_paginacion_y_columnas():
    r = client.get("/api/presupuesto/uc?limit=5")
    assert r.status_code == 200
    data = r.json()
    assert "columns" in data and "rows" in data and "total" in data
    assert len(data["rows"]) <= 5
    assert data["total"] >= len(data["rows"])
    nombres = {c["name"] for c in data["columns"]}
    assert {"id", "elemento_de_coste", "importe"}.issubset(nombres)


def test_listar_uc_filtro_insensible_a_tildes():
    r = client.get("/api/presupuesto/uc?q=PRESTAMOS&limit=5")
    assert r.status_code == 200
    data = r.json()
    # 'préstamos' aparece en intereses-préstamos. Sin tildes/mayúsculas debe encontrar.
    assert data["total"] > 0


def test_listar_uc_sort_por_importe_descendente():
    r = client.get("/api/presupuesto/uc?sort_by=importe&desc=true&limit=5")
    assert r.status_code == 200
    data = r.json()
    importes = [row["importe"] for row in data["rows"]]
    assert importes == sorted(importes, reverse=True)


def test_obtener_uc_por_id_existente():
    # Tomamos un ID conocido del listado.
    lista = client.get("/api/presupuesto/uc?limit=1").json()
    assert lista["rows"], "El parquet no debería estar vacío"
    uc_id = lista["rows"][0]["id"]

    r = client.get(f"/api/presupuesto/uc/{uc_id}")
    assert r.status_code == 200
    data = r.json()
    assert "main" in data
    main_names = {f["name"] for f in data["main"]}
    assert {"id", "elemento_de_coste", "centro_de_coste"}.issubset(main_names)


def test_obtener_uc_id_inexistente_da_404():
    r = client.get("/api/presupuesto/uc/INEXISTENTE")
    assert r.status_code == 404
