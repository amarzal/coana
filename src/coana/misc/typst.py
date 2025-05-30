import sys
from datetime import datetime
from io import StringIO
from typing import Any

import polars as pl

from coana.misc.euro import E
from coana.árbol import Árbol


def preámbulo() -> str:
    return f"""
#import "@preview/use-tabler-icons:0.12.0": tabler-icon
#set text(font: "Fira Sans", size: 7pt, lang: "es")
#set heading(numbering: "1.1")
#set page(numbering: "1")
#align(center, text(size: 18pt)[Traza de ejecución de Coana\ {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}])
#align(center, text(size: 12pt, `{" ".join(sys.argv)}`))
#outline()
"""


def S(contenido: Any) -> str:
    return f"#{str(contenido)}"


def align(contenido: Any, align: str = "center") -> str:
    return f"align({align}, {str(contenido)})"


def dataframe_a_tabla(df: pl.DataFrame, alignment: tuple[str, ...] = ("right",), size: str = "7pt") -> str:
    cols = df.columns
    for col in cols:
        if df[col].dtype == pl.Decimal:
            df = df.with_columns(pl.col(col).map_elements(lambda x: str(E(x)), return_dtype=pl.Utf8).alias(col))
    df = df.select(cols)

    s = StringIO()
    align_str = ", ".join(alignment )
    s.write(f"[#set text(size: {size})\n")
    s.write(f"#table(columns: {len(df.columns)}, align: ({align_str}), inset: (y: 0.2em), stroke: none,")
    s.write(
        f"table.header(table.hline(),{', '.join(f'[*{col if col is not None else ""}*]' for col in df.columns)},table.hline(stroke: 1pt)),\n"
    )
    for row in df.iter_rows(named=False):
        s.write(f"{', '.join(f'[{cell if cell is not None else ""}]' for cell in row)}, ")
        s.write("table.hline(stroke: 0.2pt),\n")
    s.write(")]")

    return align(s.getvalue())


def árbol_a_tabla(título: str, árbol: Árbol) -> str:
    s = StringIO()
    s.write(f"""
    #{{
        set text(size: 7pt)
        set par(leading: 0.2em)
        table(
            columns: (1fr, auto),
            align: (left, right),
            inset: (y: .2em),
            stroke: none,
            table.header(
                table.hline(stroke: 1pt),
                [*{título}*],
                [*Etiqueta*],
                table.hline(stroke: 1pt),
            ),
    """)
    celdas = []
    for ruta, ident, desc in árbol.como_tripletas():
        ruta = ruta_a_texto(ruta)
        celdas.append(f"grid(columns: 2, [{ruta}] + h(1em), [{desc}],)")
        celdas.append(f"[{ident}]")
    s.write(',\n'.join(celdas))
    s.write("""
            ,
            table.hline(stroke: 1pt),
        )
    }""")

    return s.getvalue()

def ruta_a_texto(ruta: tuple[int, ...]) -> str:
    match len(ruta):
        case 0:
            return ""
        case 1:
            return f'#text(font: "Fira Code")[{ruta[0]:02d}]'
        case _:
            ruta0 = ".".join(f"{r:02d}" for r in ruta[:-1])
            ruta1 = f"{ruta[-1]:02d}"
            return f'#text(font: "Fira Code")[#text(fill: gray)[{ruta0}.]{ruta1}]'

def árbol_a_tabla_índice(árbol: Árbol) -> str:
    cols = sorted(list(árbol.ruta2identificador.items()), key=lambda x: x[1])
    df = pl.DataFrame({
        "identificador": [ident for _, ident in cols],
        "ruta": [ruta_a_texto(ruta) for ruta, _ in cols],
    })
    return dataframe_a_tabla(df, ("left", "left"))

def normaliza_texto(s: str) -> str:
    return s.replace("//", "/").replace("*", "\\*").replace("@", "\\@").replace("_", "\\_").strip()
