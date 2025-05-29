from dataclasses import dataclass, field
from io import StringIO
from typing import Any, Iterator

import polars as pl

from coana.configuración import Configuración
from coana.misc import euro
from coana.misc.euro import E
from coana.misc.utils import num, porcentaje


@dataclass(slots=True)
class Regla:
    etiqueta: str = field(init=False)
    clave_valor: dict[str, str] = field(init=False)
    tabla_clave_valor: dict[tuple[str, str], str] = field(init=False)
    usos: int = field(init=False)
    importe: E = field(init=False)

    def __init__(self, etiqueta: str, clave_valor: dict[str, str]) -> None:
        self.etiqueta = etiqueta
        self.clave_valor = {}
        self.tabla_clave_valor = {}
        for k, v in clave_valor.items():
            if ":" in k:
                tabla, campo = k.split(":")
                self.tabla_clave_valor[(tabla, campo)] = v
            else:
                self.clave_valor[k] = v
        self.usos = 0
        self.importe = euro.zero

    def __call__(self, objeto: Any, tablas_auxiliares: dict[str, dict] = {}, importe: str = "importe") -> bool:
        for k, v in self.clave_valor.items():
            if getattr(objeto, k) != v:
                return False
        for (t, k), v in self.tabla_clave_valor.items():
            registro = tablas_auxiliares[t]
            if getattr(registro, k) != v:
                return False
        self.usos += 1
        self.importe += getattr(objeto, importe, euro.zero)
        return True

    def __str__(self) -> str:
        s = StringIO()
        for k, v in self.clave_valor.items():
            s.write(f"{k}=={v} ")
        for (t, k), v in self.tabla_clave_valor.items():
            s.write(f"{t}:{k}=={v} ")
        s.write(f" => {self.etiqueta}")
        s.write(f" ({self.usos} usos, {self.importe} euros)")
        return s.getvalue()

@dataclass
class ReglasEtiquetador:
    reglas: list[Regla] = field(init=False)
    df: pl.DataFrame = field(init=False)

    def __init__(self, cfg: Configuración, clave_fichero: str) -> None:
        self.df = cfg.fichero(clave_fichero).carga_dataframe()
        self.df = self.df.sort("prioridad", descending=True)
        reglas_con_prioridad = []
        columnas_de_filtrado = self.df.columns[2:]
        for row in self.df.iter_rows(named=True):
            clave_valor = {}
            for col in columnas_de_filtrado:
                if row[col] is not None:
                    clave_valor[col] = row[col]
            reglas_con_prioridad.append((
                row["prioridad"],
                Regla(
                    etiqueta=row["etiqueta"],
                    clave_valor=clave_valor,
                ),
            ))
        self.reglas = [r[1] for r in reglas_con_prioridad]


    def __iter__(self) -> Iterator[Regla]:
        return iter(self.reglas)

    def __len__(self) -> int:
        return len(self.reglas)

    def para_traza(self, etiqueta: str, objetos: Any) -> str:
        s = StringIO()
        s.write(f"= Asignación de etiqueta `{etiqueta}` a apuntes\n")

        s.write(f"== Etiquetas `{etiqueta}` asignadas\n")
        claves = set()
        for regla in self.reglas:
            claves.update(regla.clave_valor.keys())
        ncols = 1 + len(claves) + 2
        s.write(f"""#[
            #set par(leading: 0.2em)
            #align(
                center,
                table(
                  columns: {ncols},
                  align: (left,) + (center,) * {ncols - 3} + (right,) * 2,
                  stroke: none,
                  inset: (y: 0.2em),
                  table.vline(x: 1, start: 1, end: {len(self.reglas) + 1}),
                  table.vline(x: {ncols - 2}, start: 1, end: {len(self.reglas) + 1}),
                  table.header(
                    table.hline(),
                    [*Etq*], {", ".join([f"[*{k}*]" for k in claves])}, [*Usos*], [*Importe*],
                    table.hline(),
                  ),
        """)
        for regla in self.reglas:
            s.write(f"  [{regla.etiqueta}],")
            for k in claves:
                s.write(f"[{regla.clave_valor.get(k, '')}],")
            s.write(f"[{num(regla.usos)}], [{regla.importe}],\n")
            s.write("table.hline(stroke: .2pt),\n")
        s.write("table.hline(stroke: 1pt),")
        registros_totales = sum(1 for _ in objetos)
        usos_totales = sum((regla.usos for regla in self.reglas))
        importe_total = sum((getattr(objeto, "importe", euro.zero) for objeto in objetos))
        importe_etiquetado = sum((regla.importe for regla in self.reglas))

        s.write(
            "[*Con etiqueta*], "
            + "[], " * (ncols - 3)
            + f"[({porcentaje(usos_totales, registros_totales)})"
            + f" *{num(usos_totales)}*],"
            + f"[({porcentaje(importe_etiquetado, importe_total)})"
            + f" *{importe_etiquetado}*],"
        )
        s.write(
            "[Sin etiqueta], "
            + "[], " * (ncols - 3)
            + f"[({porcentaje(registros_totales - usos_totales, registros_totales)})"
            + f" {num(registros_totales - usos_totales)}],"
            + f"[({porcentaje(importe_total - importe_etiquetado, importe_total)}) "
            + f" {importe_total - importe_etiquetado}],"
        )
        s.write("table.hline(stroke: .5pt),")
        s.write(
            "[Total], "
            + "[], " * (ncols - 3)
            + f"[{num(registros_totales)}],"
            + f" [{importe_total}],"
        )
        s.write("""table.hline(),
                )
            )]
        """)
        return s.getvalue()
