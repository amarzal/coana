"""Fase 1: generación de unidades de coste.

Obtiene unidades de coste a partir de los datos extraídos de la base de
datos corporativa.  Actualmente implementa:

- Apuntes presupuestarios de gasto (TraductorPresupuesto)
- Registros de inventario (amortizaciones y suministros)
- Nóminas

La salida se deja en ``data/fase1/``.
"""

import json
import logging
from pathlib import Path

import polars as pl

from coana.fase1.amortizaciones import generar_uc_amortizaciones
from coana.fase1.cargos import (
    calcular_extras_cargos_por_persona,
    generar_cargos_uc,
)
from coana.fase1.inventario import ContextoInventario, procesar_inventario
from coana.fase1.investigación import enriquecer_árbol_cc_con_grupos
from coana.fase1.nóminas import (
    ContextoNóminas, generar_reparto_ss_persona, preprocesar_nóminas,
)
from coana.fase1.presupuesto import ContextoPresupuesto, TraductorPresupuesto
from coana.fase1.regla23 import generar_dedicación_pdi
from coana.fase1.suministros import generar_uc_suministros

log = logging.getLogger(__name__)


def _fmt_n(n: int) -> str:
    """Formatea un entero con separador de miles."""
    return f"{n:,}".replace(",", ".")


def ejecutar(ruta_base: Path = Path("data"), año: int = 2025) -> None:
    """Ejecuta la fase 1 completa."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    dir_salida = ruta_base / "fase1"
    dir_salida.mkdir(parents=True, exist_ok=True)

    # -- Cargar contexto --
    print("Cargando datos de entrada…")
    ctx = ContextoPresupuesto(ruta_base)

    todas_uc: list[pl.DataFrame] = []

    # -- Superficies e inventario (antes de presupuesto, porque se necesitan las matrices) --
    print("Procesando inventario (amortizaciones y superficies)…")
    ctx_inv = ContextoInventario(ruta_base)
    resultado_inv = procesar_inventario(ctx_inv, año=año)
    print(f"  Registros enriquecidos: {_fmt_n(resultado_inv.n_registros_tras_filtro)} de {_fmt_n(resultado_inv.n_registros_original)}")
    print(f"  Importe total amortización: {resultado_inv.importe_total:,.2f} €")
    print(f"  Centros con presencia: {resultado_inv.n_centros_con_presencia}")

    # Guardar datos intermedios de amortizaciones para el visor
    dir_amort = dir_salida / "auxiliares" / "amortizaciones"
    dir_amort.mkdir(parents=True, exist_ok=True)
    resultado_inv.inventario_enriquecido.write_parquet(dir_amort / "inventario_enriquecido.parquet")
    resultado_inv.filtrados_estado_df.write_parquet(dir_amort / "filtrados_estado.parquet")
    resultado_inv.filtrados_cuenta_df.write_parquet(dir_amort / "filtrados_cuenta.parquet")
    resultado_inv.sin_cuenta_df.write_parquet(dir_amort / "sin_cuenta.parquet")
    resultado_inv.sin_fecha_alta_df.write_parquet(dir_amort / "sin_fecha_alta.parquet")
    resultado_inv.filtrados_fecha_df.write_parquet(dir_amort / "filtrados_fecha.parquet")
    resultado_inv.detalle_cuentas_filtradas.write_parquet(dir_amort / "detalle_cuentas_filtradas.parquet")

    # Enriquecimiento del árbol de centros de coste con los grupos de
    # investigación (un CC por grupo, bajo su instituto; los no adscritos
    # bajo un nodo virtual `inves`).
    if ctx.centros_de_coste is not None:
        n_grupos, n_grupos_om = enriquecer_árbol_cc_con_grupos(
            ruta_base, ctx.centros_de_coste,
        )
        if n_grupos or n_grupos_om:
            print(
                f"  CC grupos investigación: {n_grupos} creados, "
                f"{n_grupos_om} omitidos (sin mapeo de instituto o colisión)"
            )

    # Nodos de actividades y centros de coste antes de traducir
    nodos_act_antes = len(ctx.actividades._por_id) - 1 if ctx.actividades else 0
    ids_cc_originales = set(ctx.centros_de_coste._por_id.keys()) if ctx.centros_de_coste else set()

    # -- Presupuesto --
    print("Traduciendo apuntes presupuestarios…")
    traductor = TraductorPresupuesto(
        ctx, distribución_costes=resultado_inv.distribución_costes,
    )
    uc_pres, sin_pres, df_completo = traductor.traducir()

    # Guardar estadísticas y parquets para el visor
    dir_stats = dir_salida / "auxiliares"
    dir_stats.mkdir(parents=True, exist_ok=True)

    for nombre_stats, datos_stats in [
        ("conteo_reglas_presupuesto", traductor.conteo_reglas),
        ("conteo_cc_presupuesto", traductor.conteo_cc),
        ("conteo_ec_presupuesto", traductor.conteo_ec),
    ]:
        pl.DataFrame(
            datos_stats, schema=["regla", "n", "importe"], orient="row"
        ).write_parquet(dir_stats / f"{nombre_stats}.parquet")

    if not uc_pres.is_empty():
        uc_pres.write_parquet(dir_salida / "uc presupuesto.parquet")

    if not sin_pres.is_empty():
        sin_pres.write_parquet(dir_salida / "presupuesto sin uc.parquet")

    # Distribución OTOP: resumen por (centro, EC) y detalle del centro
    # top, persistidos para que el visor pueda consultarlos sin
    # recalcularlos.
    dir_otop = dir_stats / "presupuesto"
    dir_otop.mkdir(parents=True, exist_ok=True)
    sd = getattr(traductor, "suministros_distribuidos", None)
    td = getattr(traductor, "suministros_top_detalle", None)
    if sd is not None and not sd.is_empty():
        sd.write_parquet(dir_otop / "suministros_distribuidos.parquet")
    if td is not None and not td.is_empty():
        td.write_parquet(dir_otop / "suministros_top_detalle.parquet")

    # sin_clasificar enriquecido con tipo_proyecto para el visor
    _non_private = [c for c in df_completo.columns if not c.startswith("_")]
    _sin_cols = _non_private.copy()
    _rename: dict[str, str] = {}
    if "_tipo_proyecto" in df_completo.columns:
        _sin_cols.append("_tipo_proyecto")
        _rename["_tipo_proyecto"] = "tipo_proyecto"
    (
        df_completo
        .filter(pl.col("_actividad").is_null())
        .select(_sin_cols)
        .rename(_rename)
    ).write_parquet(dir_stats / "sin_clasificar_presupuesto.parquet")

    # Apuntes filtrados por el filtro previo (con motivo)
    if not traductor.apuntes_filtrados.is_empty():
        traductor.apuntes_filtrados.write_parquet(
            dir_stats / "filtrados_presupuesto.parquet"
        )

    # Resumen JSON
    n_act_despues = len(ctx.actividades._por_id) - 1 if ctx.actividades else 0
    n_cc_despues = len(ctx.centros_de_coste._por_id) - 1 if ctx.centros_de_coste else 0
    resumen_dict = {
        "n_uc_presupuesto": len(uc_pres),
        "importe_uc_presupuesto": float(uc_pres["importe"].sum() or 0) if not uc_pres.is_empty() else 0.0,
        "n_sin_presupuesto": len(sin_pres),
        "importe_sin_presupuesto": float(sin_pres["importe"].cast(pl.Float64).sum() or 0) if not sin_pres.is_empty() else 0.0,
        "n_filtrados_presupuesto": len(traductor.apuntes_filtrados),
        "importe_filtrados_presupuesto": float(traductor.apuntes_filtrados["importe"].sum() or 0) if not traductor.apuntes_filtrados.is_empty() else 0.0,
        "n_actividades_antes": nodos_act_antes,
        "n_actividades_despues": n_act_despues,
        "n_cc_antes": len(ids_cc_originales),
        "n_cc_despues": n_cc_despues,
        # Amortizaciones
        "amort_n_original": resultado_inv.n_registros_original,
        "amort_n_enriquecidos": resultado_inv.n_registros_tras_filtro,
        "amort_importe_total": resultado_inv.importe_total,
        "amort_n_filtrados_estado": resultado_inv.n_filtrados_estado,
        "amort_n_sin_cuenta": resultado_inv.n_sin_cuenta,
        "amort_n_filtrados_cuenta": resultado_inv.n_filtrados_cuenta,
        "amort_n_sin_fecha_alta": resultado_inv.n_sin_fecha_alta,
        "amort_n_filtrados_fecha": resultado_inv.n_filtrados_fecha,
    }
    (dir_stats / "resumen.json").write_text(
        json.dumps(resumen_dict, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    if not uc_pres.is_empty():
        todas_uc.append(uc_pres)

    # -- Suministros (energía, agua, gas) --
    print("Generando UC de suministros (energía, agua, gas)…")
    uc_sumin, stats_sumin = generar_uc_suministros(resultado_inv, ruta_base)
    if not uc_sumin.is_empty():
        todas_uc.append(uc_sumin)
        uc_sumin.write_parquet(dir_salida / "uc suministros.parquet")

    # -- Amortizaciones → UC --
    print("Generando UC de amortizaciones…")
    uc_amort, sin_amort, stats_amort = generar_uc_amortizaciones(resultado_inv, ctx_inv)
    if not uc_amort.is_empty():
        todas_uc.append(uc_amort)
        uc_amort.write_parquet(dir_salida / "uc amortizaciones.parquet")
    if not sin_amort.is_empty():
        sin_amort.write_parquet(dir_amort / "sin_uc.parquet")

    # -- Cargos académicos: pre-cálculo de extras del cargo (CR 68) --
    # Antes de preprocesar nóminas calculamos la extra estimada por
    # persona para descontarla del CR 68 en proyecto general dentro del
    # preprocesamiento (evita duplicación con la UC del cargo).
    extras_cargos_por_persona = calcular_extras_cargos_por_persona(
        ruta_base, año=año,
    )

    # -- Nóminas --
    print("Preprocesando nóminas…")
    ctx_nom = ContextoNóminas(ruta_base)
    dir_nominas = dir_salida / "auxiliares" / "nóminas"
    resultado_nom = preprocesar_nóminas(
        ctx_nom, dir_nominas,
        ctx_enriquecimiento=ctx,
        árbol_actividades=ctx.actividades,
        árbol_cc=ctx.centros_de_coste,
        árbol_ec=ctx.elementos_de_coste,
        distribución_costes=resultado_inv.distribución_costes,
        obtener_descripciones=traductor._obtener_descripciones,
        ruta_base=ruta_base,
        extras_cargos_por_persona=extras_cargos_por_persona,
    )
    for sector, n_exp in resultado_nom.expedientes_por_sector.items():
        importe = resultado_nom.importe_por_sector.get(sector, 0)
        print(f"  {sector}: {_fmt_n(n_exp)} expedientes, {importe:,.2f} €")

    # UC generadas a partir de nóminas PTGAS
    if not resultado_nom.uc_ptgas.is_empty():
        todas_uc.append(resultado_nom.uc_ptgas)
    if not resultado_nom.uc_pvi.is_empty():
        todas_uc.append(resultado_nom.uc_pvi)
    if not resultado_nom.uc_pdi.is_empty():
        todas_uc.append(resultado_nom.uc_pdi)
    if not resultado_nom.uc_despidos.is_empty():
        todas_uc.append(resultado_nom.uc_despidos)
    if not resultado_nom.uc_indemnizaciones_asistencias.is_empty():
        todas_uc.append(resultado_nom.uc_indemnizaciones_asistencias)
    if not resultado_nom.uc_cargos.is_empty():
        todas_uc.append(resultado_nom.uc_cargos)
    # UC de absentismo (persona-mes con bonificación de SS): selecciona
    # las columnas estándar (lleva además per_id/expediente/mes para el
    # visor, que no entran en el fichero combinado de UC).
    if not resultado_nom.uc_absentismo.is_empty():
        todas_uc.append(resultado_nom.uc_absentismo.select(
            "id", "elemento_de_coste", "centro_de_coste", "actividad",
            "importe", "origen", "origen_id", "origen_porción",
        ))

    # -- Cargos académicos: reparto CR 19/64 proyecto general entre cargos --
    print("Procesando cargos académicos…")
    dir_cargos = dir_salida / "auxiliares" / "nóminas"
    cargos_uc = generar_cargos_uc(
        ruta_base, dir_cargos, año=año,
        extras_aplicadas_por_persona=getattr(
            ctx_nom, "_extras_aplicadas_por_persona", {},
        ),
    )
    if not cargos_uc.is_empty():
        todas_uc.append(cargos_uc.select(
            "id", "elemento_de_coste", "centro_de_coste", "actividad",
            pl.col("importe_uc").alias("importe"),
            "origen", "origen_id", "origen_porción",
        ))

    # -- Regla 23: dedicación del PDI a actividades --
    print("Generando dedicación PDI (regla 23)…")
    dedicación = generar_dedicación_pdi(
        ruta_base, año=año, árbol_actividades=ctx.actividades,
    )
    print(
        f"  Dedicación PDI: {len(dedicación):,} filas, "
        f"{dedicación['horas'].sum():,.0f} h, "
        f"{dedicación['per_id'].n_unique():,} personas"
    )

    # UC por reparto de la masa regla 23 ya escritas por el módulo
    # `generar_dedicación_pdi`; las agregamos al combinado.
    uc_r23_path = ruta_base / "fase1" / "regla23" / "uc_reparto_regla_23.parquet"
    uc_r23 = pl.DataFrame()
    if uc_r23_path.exists():
        uc_r23 = pl.read_parquet(uc_r23_path)
        if not uc_r23.is_empty():
            todas_uc.append(uc_r23.select(
                "id", "elemento_de_coste", "centro_de_coste", "actividad",
                "importe", "origen", "origen_id", "origen_porción",
            ))
            print(
                f"  UC reparto regla 23: {len(uc_r23):,} UC, "
                f"{float(uc_r23['importe'].sum()):,.2f} €"
            )

    # -- Reparto de seguridad social (cotizada + calculada) por persona --
    # Lo ejecutamos aquí, una vez que ya están todas las UC retributivas
    # (PTGAS / PVI / PDI / despidos / indemnizaciones / cargos / reparto
    # regla 23). Así los porcentajes (actividad, centro_de_coste) que
    # se aplican a la SS reflejan el total real del coste retributivo
    # de cada persona, incluida la masa regla 23.
    print("Repartiendo seguridad social entre actividades/centros…")
    generar_reparto_ss_persona(
        resultado_nom, dir_nominas,
        uc_por_persona=[uc_r23] if not uc_r23.is_empty() else None,
    )
    ss_parquet = dir_nominas / "persona_ss.parquet"
    if ss_parquet.exists():
        ss_df = pl.read_parquet(ss_parquet)
        if not ss_df.is_empty():
            ss_válidas = ss_df.filter(~pl.col("ss_proporcional").is_nan())
            print(
                f"  SS repartida: {ss_válidas.height:,} filas, "
                f"{float(ss_válidas['ss_proporcional'].sum()):,.2f} €"
            )

    # UC de seguridad social y previsión social. Generadas en
    # `persona_uc.parquet` con `tipo == "coste social"`; tienen
    # elemento_de_coste, centro_de_coste y actividad ya resueltos.
    persona_uc_path = dir_nominas / "persona_uc.parquet"
    if persona_uc_path.exists():
        persona_uc = pl.read_parquet(persona_uc_path)
        uc_ss = persona_uc.filter(pl.col("tipo") == "coste social")
        if not uc_ss.is_empty():
            todas_uc.append(uc_ss.select(
                "id", "elemento_de_coste", "centro_de_coste", "actividad",
                "importe", "origen", "origen_id", "origen_porción",
            ))

    # -- Fichero combinado --
    if todas_uc:
        combinado = pl.concat(todas_uc, how="diagonal")
        print("Guardando fichero combinado de UC…")
        # El importe se guarda con precisión completa (la fase 2 lo relee
        # como fuente de verdad); el formato de celda lo MUESTRA al céntimo.
        combinado.write_excel(
            dir_salida / "unidades de coste.xlsx",
            column_formats={"importe": '#,##0.00\\ "€"'},
        )
        print(f"  Total UC (todas las fuentes): {len(combinado):,}")
        print(f"  Escrito: {dir_salida / 'unidades de coste.xlsx'}")
    else:
        print("No se generaron unidades de coste.")

    # -- Árboles finales (salida para fase 2) --
    print("Guardando árboles finales…")
    if ctx.actividades is not None:
        ctx.actividades.to_file(dir_salida / "actividades.tree")
    if ctx.centros_de_coste is not None:
        ctx.centros_de_coste.to_file(dir_salida / "centros de coste.tree")
    if ctx.elementos_de_coste is not None:
        ctx.elementos_de_coste.to_file(dir_salida / "elementos de coste.tree")


def main() -> None:
    ejecutar()


if __name__ == "__main__":
    main()
