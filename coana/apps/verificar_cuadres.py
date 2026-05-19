"""Comprueba el cuadre cobrado/UC para todo el personal PDI y PVI.

Genera #ruta("auxiliares", "cuadres_pendientes.parquet") con la lista
de personas cuyo |Δ| > 0,01 € y un resumen por sector.

Uso:
    uv run python -m coana.apps.verificar_cuadres
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from coana.web.services.persona_360 import listar_personas_sector
from coana.web.services.query import QueryParams


def verificar(ruta_base: Path = Path("data"), tolerancia: float = 0.01) -> None:
    descuadres: list[dict] = []
    print()
    print(f"{'Sector':<6} {'Personas':>10} {'|Δ|<0,01':>10} {'|Δ|>=100':>10} {'Σ|Δ|':>14}")
    print("-" * 60)
    for sector in ("PDI", "PVI"):
        r = listar_personas_sector(sector, QueryParams(limit=10_000))
        n = len(r.rows)
        if n == 0:
            print(f"{sector:<6} (sin datos)")
            continue
        n_ok = sum(1 for row in r.rows if abs(row.get("delta") or 0) < tolerancia)
        n_grande = sum(1 for row in r.rows if abs(row.get("delta") or 0) >= 100)
        suma = sum(abs(row.get("delta") or 0) for row in r.rows)
        print(f"{sector:<6} {n:>10,} {n_ok:>10,} {n_grande:>10,} {suma:>12,.2f} €")
        for row in r.rows:
            d = row.get("delta") or 0
            if abs(d) >= tolerancia:
                descuadres.append({
                    "sector": sector,
                    "per_id": row.get("per_id"),
                    "persona": row.get("persona"),
                    "bruto": row.get("bruto"),
                    "ss_cot": row.get("ss_cot"),
                    "ss_calc": row.get("ss_calc"),
                    "uc_retr": row.get("uc_retr"),
                    "uc_ss": row.get("uc_ss"),
                    "delta": d,
                })

    if descuadres:
        df = pl.DataFrame(descuadres).sort("delta", descending=True)
        out = ruta_base / "fase1" / "auxiliares" / "cuadres_pendientes.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out)
        print()
        print(f"⚠ {len(descuadres):,} personas con |Δ| >= {tolerancia} €")
        print(f"  Detalle en: {out}")
        print(f"  Δ total: {sum(r['delta'] for r in descuadres):,.2f} €")
        print(f"  Σ|Δ|:    {sum(abs(r['delta']) for r in descuadres):,.2f} €")
    else:
        print()
        print("✓ Todos los cuadres dentro de la tolerancia.")


def main() -> None:
    verificar()


if __name__ == "__main__":
    main()
