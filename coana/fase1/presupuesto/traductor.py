"""Traductor de apuntes presupuestarios a unidades de coste.

Aplica las reglas de generación definidas en la especificación para
producir unidades de coste a partir de los apuntes del presupuesto
de gasto.  Las reglas se evalúan en orden; la primera que encaja gana.
"""

import logging
from pathlib import Path

import polars as pl

from coana.fase1.clasificador_actividades import (
    clasificar_actividades,
    enriquecer_para_actividades,
)
from coana.fase1.clasificador_centros_coste import clasificar_centros_coste
from coana.fase1.presupuesto.contexto import ContextoPresupuesto

log = logging.getLogger(__name__)

# ======================================================================
# (Las constantes de centros de coste están en clasificador_centros_coste.py)

# Concepto presupuestario (3 dígitos) → elemento de coste

# (Las constantes de actividades están en clasificador_actividades.py)

# ======================================================================
# Helpers
# ======================================================================



def _match_aplicación_prefijos(prefijos: list[str]) -> pl.Expr:
    """Condición: aplicación empieza con alguno de los prefijos dados."""
    cond = pl.lit(False)
    for p in prefijos:
        if len(p) == 4:
            cond = cond | (pl.col("aplicación") == p)
        else:
            cond = cond | pl.col("aplicación").str.starts_with(p)
    return cond


class TraductorPresupuesto:
    """Traduce apuntes presupuestarios en unidades de coste."""

    def __init__(
        self,
        ctx: ContextoPresupuesto,
        distribución_costes: pl.DataFrame | None = None,
    ) -> None:
        self.ctx = ctx
        self._distribución_costes = distribución_costes

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def traducir(self) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Genera unidades de coste a partir de los apuntes presupuestarios.

        Returns
        -------
        tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]
            (unidades_de_coste, apuntes_sin_clasificar, df_completo)

            *df_completo* es el DataFrame tras asignar actividad, con las
            columnas de trabajo (``_actividad``, ``_centro_de_coste``, etc.)
            todavía presentes.  Se usa para generar la traza.
        """
        if self.ctx.apuntes is None:
            log.warning("No hay apuntes presupuestarios que traducir")
            return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

        filtrados = self._filtrar(self.ctx.apuntes)
        importe_filtrados = float(filtrados["importe"].sum() or 0)
        df = self._enriquecer(filtrados)

        print("  Asignando centros de coste…")
        df = self._asignar_centro_de_coste(df)
        con_cc = sum(n for nom, n, imp in self.conteo_cc if "Sin asignar" not in nom)
        sin_cc = next((n for nom, n, imp in self.conteo_cc if "Sin asignar" in nom), 0)
        print(f"    Con centro de coste: {con_cc:,}  Sin: {sin_cc:,}")

        print("  Asignando elementos de coste…")
        df = self._asignar_elemento_de_coste(df)
        con_ec = sum(n for nom, n, imp in self.conteo_ec if "Sin asignar" not in nom)
        sin_ec = next((n for nom, n, imp in self.conteo_ec if "Sin asignar" in nom), 0)
        print(f"    Con elemento de coste: {con_ec:,}  Sin: {sin_ec:,}")

        print("  Asignando actividades…")
        df = self._asignar_actividad(df)

        df_completo = df

        clasificados = df.filter(pl.col("_actividad").is_not_null())
        sin_clasificar = df.filter(pl.col("_actividad").is_null())

        print(f"    Con actividad: {len(clasificados):,}  Sin: {len(sin_clasificar):,}")

        # Expandir suministros distribuidos (SC001 + aplicación específica)
        _SENTINEL = "__DISTRIBUIR__"
        if self._distribución_costes is not None:
            mask_dist = (pl.col("_centro_de_coste") == _SENTINEL).fill_null(False)
            df_dist = clasificados.filter(mask_dist)
            clasificados_normal = clasificados.filter(~mask_dist)

            if not df_dist.is_empty():
                n_orig = len(df_dist)
                imp_orig = float(df_dist["importe"].sum())
                tabla = self._distribución_costes.select(
                    pl.col("centro").alias("_cc_dist"),
                    pl.col("porcentaje").alias("_pct_dist"),
                )
                expandidos = (
                    df_dist
                    .join(tabla, how="cross")
                    .with_columns(
                        (pl.col("importe") * pl.col("_pct_dist")).alias("importe"),
                        pl.col("_cc_dist").alias("_centro_de_coste"),
                        pl.col("_pct_dist").alias("_origen_porción"),
                    )
                    .drop("_cc_dist", "_pct_dist")
                )
                n_centros = len(tabla)
                print(
                    f"  Suministros distribuidos: {n_orig} apuntes × "
                    f"{n_centros} centros = {len(expandidos):,} UC "
                    f"({imp_orig:,.2f} €)"
                )
                # Guardar resumen para la traza (top por centro+elemento)
                self.suministros_distribuidos: pl.DataFrame = (
                    expandidos
                    .group_by("_centro_de_coste", "_elemento_de_coste")
                    .agg(
                        pl.col("importe").sum().alias("importe"),
                        pl.col("_origen_porción").first().alias("porcentaje"),
                        pl.len().alias("n"),
                    )
                    .sort("importe", descending=True)
                )
                # Detalle del centro con más importe
                if not expandidos.is_empty():
                    top_centro = (
                        expandidos
                        .group_by("_centro_de_coste")
                        .agg(pl.col("importe").sum())
                        .sort("importe", descending=True)
                        .head(1)
                        .item(0, "_centro_de_coste")
                    )
                    self.suministros_top_detalle: pl.DataFrame = (
                        expandidos
                        .filter(pl.col("_centro_de_coste") == top_centro)
                        .select(
                            pl.col("_centro_de_coste").alias("centro_de_coste"),
                            pl.col("_elemento_de_coste").alias("elemento_de_coste"),
                            pl.col("_actividad").alias("actividad"),
                            "aplicación",
                            "importe",
                            pl.col("_origen_porción").alias("porcentaje"),
                            "asiento",
                        )
                        .sort("importe", descending=True)
                    )
                    clasificados = pl.concat([clasificados_normal, expandidos])
                else:
                    self.suministros_top_detalle = pl.DataFrame()
                    clasificados = clasificados_normal
            else:
                self.suministros_distribuidos = pl.DataFrame()
                self.suministros_top_detalle = pl.DataFrame()
                clasificados = clasificados_normal
        else:
            self.suministros_distribuidos = pl.DataFrame()
            self.suministros_top_detalle = pl.DataFrame()
            # Sin tabla de distribución: marcar como sin CC
            clasificados = clasificados.with_columns(
                pl.when(pl.col("_centro_de_coste") == _SENTINEL)
                .then(pl.lit(None))
                .otherwise(pl.col("_centro_de_coste"))
                .alias("_centro_de_coste")
            )

        unidades = self._construir_unidades(clasificados)

        # Verificación de conservación del importe
        importe_uc = float(unidades["importe"].sum() or 0) if not unidades.is_empty() else 0.0
        importe_sin = float(
            sin_clasificar["importe"].cast(pl.Float64).sum() or 0
        ) if not sin_clasificar.is_empty() else 0.0
        diferencia = importe_filtrados - importe_uc - importe_sin
        print(
            f"  Importe filtrado:  {importe_filtrados:>16,.2f}\n"
            f"  Importe UC:        {importe_uc:>16,.2f}\n"
            f"  Importe sin UC:    {importe_sin:>16,.2f}\n"
            f"  Diferencia:        {diferencia:>16,.2f}"
            + ("  ✓" if abs(diferencia) < 0.02 else "  ← ¡diferencia!")
        )

        # Eliminar columnas de trabajo de los apuntes sin clasificar
        sin_clasificar = sin_clasificar.drop(
            [c for c in sin_clasificar.columns if c.startswith("_")]
        )

        return unidades, sin_clasificar, df_completo

    def guardar(
        self,
        unidades: pl.DataFrame,
        sin_clasificar: pl.DataFrame,
        dir_salida: Path = Path("data/fase1"),
    ) -> None:
        """Escribe los ficheros de salida del presupuesto."""
        dir_salida.mkdir(parents=True, exist_ok=True)

        ruta_uc = dir_salida / "uc presupuesto.xlsx"
        ruta_sin = dir_salida / "presupuesto sin uc.xlsx"

        if not unidades.is_empty():
            unidades.write_excel(ruta_uc)
        else:
            ruta_uc.unlink(missing_ok=True)

        if not sin_clasificar.is_empty():
            sin_clasificar.write_excel(ruta_sin)
        else:
            ruta_sin.unlink(missing_ok=True)
        print(f"  Ficheros guardados en {dir_salida}")

    # ------------------------------------------------------------------
    # Filtro previo (spec §2-1-1)
    # ------------------------------------------------------------------

    def _filtrar(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aplica el filtro del presupuesto descrito en la especificación.

        Guarda en ``self.apuntes_filtrados`` un DataFrame con las filas
        descartadas y una columna ``motivo`` que indica la regla aplicada.
        """
        n_original = len(df)
        partes_filtradas: list[pl.DataFrame] = []

        cap = pl.col("aplicación").cast(pl.Utf8).str.slice(0, 1)
        apl = pl.col("aplicación").cast(pl.Utf8)

        # Regla 1: Filtro de capítulos financieros
        mask1 = cap.is_in(["8", "9"])
        partes_filtradas.append(
            df.filter(mask1).with_columns(
                pl.lit("Filtro de capítulos financieros").alias("motivo")
            )
        )
        df = df.filter(~mask1)

        # Regla 2: Filtro del capítulo 1 (va por nóminas)
        mask2 = cap == "1"
        partes_filtradas.append(
            df.filter(mask2).with_columns(
                pl.lit("Filtro del capítulo 1 que va por nóminas").alias("motivo")
            )
        )
        df = df.filter(~mask2)

        # Regla 3: Filtro otros gastos que van por nóminas (2321 asistencias, 2281 patentes)
        mask3 = apl.is_in(["2321", "2281"])
        partes_filtradas.append(
            df.filter(mask3).with_columns(
                pl.lit("Filtro otros gastos que van por nóminas").alias("motivo")
            )
        )
        df = df.filter(~mask3)

        # Regla 4: Supresión de capítulo 6 excepto 6711
        mask4 = (cap == "6") & (apl != "6711")
        partes_filtradas.append(
            df.filter(mask4).with_columns(
                pl.lit("Supresión de capítulo 6 excepto 6711").alias("motivo")
            )
        )
        df = df.filter(~mask4)

        # Regla 5: Supresión de consumos de energía, agua y gas
        _SUMINISTROS = {"2231": "energía eléctrica", "2232": "agua", "2233": "gas"}
        suministros = df.filter(apl.is_in(list(_SUMINISTROS.keys())))
        self.filtro_suministros: list[tuple[str, int, float]] = []
        for código, nombre in _SUMINISTROS.items():
            filas = suministros.filter(apl == código)
            if not filas.is_empty():
                self.filtro_suministros.append(
                    (f"{código} ({nombre})", len(filas), float(filas["importe"].sum()))
                )
        partes_filtradas.append(
            suministros.with_columns(
                pl.lit("Supresión de consumos de energía, agua y gas").alias("motivo")
            )
        )
        df = df.filter(~apl.is_in(list(_SUMINISTROS.keys())))

        # Guardar apuntes filtrados con motivo
        if partes_filtradas:
            self.apuntes_filtrados = pl.concat(partes_filtradas, how="diagonal")
        else:
            self.apuntes_filtrados = pl.DataFrame()

        n_filtrado = n_original - len(df)
        print(
            f"  Filtro del presupuesto: {n_original:,} filas → {len(df):,} "
            f"({n_filtrado:,} filtradas)"
        )

        return df

    # ------------------------------------------------------------------
    # Enriquecimiento: añadir campos de referencia
    # ------------------------------------------------------------------

    def _enriquecer(self, df: pl.DataFrame) -> pl.DataFrame:
        """Añade tipo_proyecto, tipo_línea y capítulo."""
        return enriquecer_para_actividades(df, self.ctx)

    # ------------------------------------------------------------------
    # Centro de coste
    # ------------------------------------------------------------------

    # Aplicaciones de suministros centrales que se distribuyen por centro de coste
    def _asignar_centro_de_coste(self, df: pl.DataFrame) -> pl.DataFrame:
        """Asigna centro de coste usando el módulo compartido."""
        df, self.conteo_cc = clasificar_centros_coste(
            df,
            self.ctx.centros_de_coste,
            self._distribución_costes,
            self._obtener_descripciones,
        )
        return df

    # Elemento de coste
    # ------------------------------------------------------------------

    def _asignar_elemento_de_coste(self, df: pl.DataFrame) -> pl.DataFrame:
        """Asigna elemento de coste según la aplicación presupuestaria."""
        aplicación = pl.col("aplicación")

        # Tabla de aplicación (4 dígitos) → elemento de coste
        ec_apl = {k: v for k, v in _EC_APLICACIÓN.items() if v != "xxx"}
        if ec_apl:
            df_apl = pl.DataFrame(
                list(ec_apl.items()),
                schema=["aplicación", "_ec_apl"],
                orient="row",
            )
            df = df.join(df_apl, on="aplicación", how="left")

        # Regla EC: inicialmente la tabla cubre lo que tiene match
        df = df.with_columns(
            pl.when(pl.col("_ec_apl").is_not_null())
            .then(pl.lit("Clasificación por aplicación presupuestaria"))
            .otherwise(pl.lit(None).cast(pl.Utf8))
            .alias("_regla_ec")
        )

        # Reglas condicionales con nombre (sobreescriben _ec_apl y _regla_ec)
        _reglas_cond: list[tuple[str, pl.Expr, pl.Expr]] = [
            (
                "Conferenciantes u otras empresas",
                aplicación == "2322",
                pl.when(
                    pl.col("_tipo_proyecto").is_in(
                        ["EPM", "EPDE", "EPDEX", "EPC", "EPMI", "CUID", "CUEX"]
                    )
                    | (pl.col("centro") == "UMAJ")
                )
                .then(pl.lit("piyotper-conferenciantes"))
                .otherwise(pl.lit("trabajos-otras-empresas")),
            ),
            (
                "Material de docencia, deportivo u otro",
                aplicación.is_in(["2236", "2238"]),
                pl.when(pl.col("centro").is_in(["ECTEC", "FCCHS", "FCCJS", "FCCS"]))
                .then(pl.lit("material-docencia"))
                .when(pl.col("subcentro").is_in(["C2", "C3"]))
                .then(pl.lit("material-deportivo-cultural"))
                .otherwise(pl.lit("otros-suministros")),
            ),
            (
                "Material de laboratorio, conservación, investigación o docencia",
                aplicación == "2239",
                pl.when(pl.col("programa") == "541-A")
                .then(
                    pl.when(pl.col("proyecto") == "00000")
                    .then(pl.lit("material-laboratorio"))
                    .when(pl.col("proyecto") == "8G022")
                    .then(pl.lit("conservación-instalaciones"))
                    .otherwise(pl.lit("bienes-investigación"))
                )
                .otherwise(pl.lit("material-docencia")),
            ),
            (
                "Material de docencia o de oficina en centros",
                aplicación.is_in(["2271", "2278"]),
                pl.when(pl.col("centro").is_in(["ECTEC", "FCCHS", "FCCJE", "FCCS"]))
                .then(pl.lit("material-docencia"))
                .otherwise(pl.lit("material-oficina")),
            ),
            (
                "Publicaciones o servicios profesionales",
                aplicación == "2258",
                pl.when(pl.col("subcentro") == "I5")
                .then(pl.lit("publicaciones"))
                .otherwise(pl.lit("servicios-profesionales")),
            ),
        ]

        for nombre, condición, resultado in _reglas_cond:
            df = df.with_columns(
                pl.when(condición).then(resultado)
                .otherwise(pl.col("_ec_apl"))
                .alias("_ec_apl"),
                pl.when(condición).then(pl.lit(nombre))
                .otherwise(pl.col("_regla_ec"))
                .alias("_regla_ec"),
            )

        # Conteo por regla
        def _ec_stats(mask_expr: pl.Expr) -> tuple[int, float]:
            filtrado = df.filter(mask_expr)
            n = filtrado.height
            imp = float(filtrado.select(pl.col("importe").sum()).item()) if n > 0 else 0.0
            return n, imp

        self.conteo_ec: list[tuple[str, int, float]] = []
        for nombre in ["Clasificación por aplicación presupuestaria"] + [
            r[0] for r in _reglas_cond
        ]:
            n, imp = _ec_stats(pl.col("_regla_ec") == nombre)
            if n > 0:
                self.conteo_ec.append((nombre, n, imp))
        n_sin, i_sin = _ec_stats(pl.col("_regla_ec").is_null())
        if n_sin > 0:
            self.conteo_ec.append(("Sin asignar", n_sin, i_sin))

        # Filas sin asignar → regla "Sin asignar"
        df = df.with_columns(
            pl.col("_regla_ec").fill_null("Sin asignar").alias("_regla_ec")
        )

        self.fallthrough_ec: list[tuple[str, pl.DataFrame]] = []
        if n_sin > 0:
            self.fallthrough_ec.append((
                "Aplicaciones sin elemento de coste",
                df.filter(pl.col("_ec_apl").is_null())
                .group_by("aplicación")
                .agg(pl.len().alias("n"), pl.col("importe").sum().alias("importe"))
                .sort("importe", descending=True)
                .head(20)
                .rename({"aplicación": "código"}),
            ))

        df = df.with_columns(
            pl.col("_ec_apl").alias("_elemento_de_coste")
        ).drop("_ec_apl")

        return df

    # ------------------------------------------------------------------
    # Actividad (reglas secuenciales — primera que encaja gana)
    # ------------------------------------------------------------------

    def _asignar_actividad(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aplica las reglas de actividad usando el módulo compartido."""
        df, self.conteo_reglas = clasificar_actividades(
            df,
            self.ctx.actividades,
            self._obtener_descripciones,
        )
        return df

    # Las reglas de actividad están en clasificador_actividades.py
    # Los métodos _reglas_actividad, _reglas_actividad_dinámicas y
    # _expandir_repartos se han eliminado.

    def _obtener_descripciones(self, col: str, valores: list) -> dict[str, str]:
        """Obtiene descripciones para los valores de un campo."""
        if col == "proyecto" and self.ctx.proyectos is not None:
            vals_str = [str(v) for v in valores]
            df = self.ctx.proyectos.filter(
                pl.col("proyecto").cast(pl.Utf8).is_in(vals_str)
            )
            if "nombre" in df.columns:
                return dict(zip(
                    df["proyecto"].cast(pl.Utf8).to_list(),
                    df["nombre"].cast(pl.Utf8).to_list(),
                ))
        return {}

    # ------------------------------------------------------------------
    # Construcción del DataFrame de unidades de coste
    # ------------------------------------------------------------------

    def _construir_unidades(self, clasificados: pl.DataFrame) -> pl.DataFrame:
        """Monta el DataFrame final con la estructura de UnidadDeCoste."""
        origen_porción = (
            pl.col("_origen_porción")
            if "_origen_porción" in clasificados.columns
            else pl.lit(1.0)
        )
        return (
            clasificados
            .with_row_index("_seq")
            .with_columns(
                pl.concat_str([
                    pl.lit("P-"),
                    (pl.col("_seq") + 1).cast(pl.Utf8).str.zfill(5),
                ]).alias("id"),
            )
            .select(
                "id",
                pl.col("_elemento_de_coste").fill_null("").alias("elemento_de_coste"),
                pl.col("_centro_de_coste").fill_null("").alias("centro_de_coste"),
                pl.col("_actividad").alias("actividad"),
                "importe",
                pl.lit("presupuesto").alias("origen"),
                pl.col("asiento").cast(pl.Utf8).alias("origen_id"),
                origen_porción.alias("origen_porción"),
                pl.col("_regla_actividad").fill_null("").alias("regla_actividad"),
                pl.col("_regla_cc").fill_null("").alias("regla_cc"),
                pl.col("_regla_ec").fill_null("").alias("regla_ec"),
            )
        )

