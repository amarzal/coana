from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

import polars as pl

import coana.misc.typst as ty
from coana.misc.euro import E
from coana.misc.traza import Traza
from coana.misc.utils import num, porcentaje

traza = Traza()


@dataclass
class Etiquetador:
    reglas: pl.DataFrame
    columnas_de_filtrado: list[str] = field(init=False)

    def __post_init__(self) -> None:
        self.reglas = self.reglas.sort("prioridad", descending=True)
        self.columnas_de_filtrado = self.reglas.columns[2:]

    def __call__(
        self, tipo_registro: str, columna: str, col_identificador: str, df: pl.DataFrame, col_importe: str = "importe"
    ) -> pl.DataFrame:
        "Genera un nuevo DataFrame con una columna `columna` a la que se asigna una etiqueta para cada fila."
        usos = [0] * len(self.reglas)
        importes = [Decimal("0.00")] * len(self.reglas)
        importe_total = df.select("importe").sum().item()
        pendientes = df.clone()
        etiquetados = df.clear().with_columns(pl.Series(name=columna, values=[], dtype=pl.Utf8))
        for i, regla in enumerate(self.reglas.iter_rows(named=True)):
            seleccionados = pendientes
            for col in self.columnas_de_filtrado:  # Es como un AND
                if regla[col] is not None:
                    seleccionados = seleccionados.filter(pl.col(col) == regla[col])
            ids = seleccionados.select(col_identificador).to_series()
            usos[i] = len(ids)
            importes[i] = seleccionados.select(col_importe).sum().item()
            pendientes = pendientes.filter(~pl.col(col_identificador).is_in(ids))
            etiquetados = pl.concat([
                etiquetados,
                seleccionados.with_columns(pl.lit(regla["etiqueta"]).alias(columna)),
            ])
        etiquetados = pl.concat([etiquetados, pendientes.with_columns(pl.lit(None).alias(columna))])
        reglas_usos_importes = self.reglas.with_columns(
            pl.Series("usos", usos),
            pl.Series("importe", importes),
        )
        importe_etiquetado = reglas_usos_importes.select("importe").sum().item()

        traza(f"= Asignación de etiqueta `{columna}` a {tipo_registro}")
        traza(f"== Etiquetas `{columna}` asignadas")
        traza(str(ty.S(ty.dataframe_a_tabla(reglas_usos_importes))))
        importe_no_etiquetado = importe_total - importe_etiquetado
        traza(f"== Apuntes sin etiqueta `{columna}` asignada")
        traza(
            f"Apuntes no etiquetados: {num(len(pendientes))}/{num(len(df))} "
            + f"({porcentaje(len(pendientes), len(df))}) por importe "
            + f"{E(importe_no_etiquetado)}/{E(importe_total)} ({porcentaje(importe_no_etiquetado, importe_total)})"
        )
        pendientes_resumen = (
            pendientes.group_by(self.columnas_de_filtrado)
            .agg(
                pl.count().alias("registros"),
                pl.col(col_importe).sum().alias("importe"),
            )
            .sort(by=pl.col("registros"), descending=True)
        )
        pendientes_resumen = pendientes_resumen.with_columns(
            pl.Series("registros", [num(r) for r in pendientes_resumen["registros"]]),
            pl.Series("importe", [str(E(i)) for i in pendientes_resumen["importe"]]),
        )
        traza(str(ty.S(ty.dataframe_a_tabla(pendientes_resumen))))

        return etiquetados

    def _etiqueta(self, objeto: Any) -> str | None:
        campos = objeto.__dict__
        for etiqueta, _, patrón in self.reglas:
            for k, v in patrón.items():
                c = campos.get(k, None)
                if c != v:
                    break
            else:
                return etiqueta
        return None

    def itera_etiquetas(self) -> Iterator[str]:
        yield from self.reglas.select("ETIQUETA").unique().sort("ETIQUETA").to_series().to_list()
