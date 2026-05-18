"""Cargador proyectos de investigación y contratos de transferencia.

Una persona puede figurar en MUCHOS contratos del mismo proyecto
presupuestario (cada acto administrativo en una cátedra o un contrato
artículo 60 abre un contrato nuevo en el SGIT). Para no inflar la
dedicación, **agrupamos por (per_id, proyecto presupuestario)** y
generamos UNA sola fila por par:

1. Filtramos contratos vivos en el año natural (solape ≥ 1 día con
   el periodo del proyecto en ``proyectos en contratos investigación.xlsx``).
2. Para cada (per_id, contrato) calculamos el *periodo efectivo* como
   la intersección de:
   - Las fechas del contrato.
   - Las fechas de solicitud principal (o alternativa) del per_id en
     ``investigadores en contratos.xlsx``.
   - El año natural.
3. Agrupamos por (per_id, proyecto presupuestario de la línea 1 del
   contrato):
   - *Selección de tipo*: el anexo con MAYOR h/sem entre los
     contratos del grupo. Desempate: mayor ``importe_concedido``,
     luego mayor id de contrato.
   - *Días efectivos*: **unión** de los periodos efectivos de los
     contratos del grupo (días distintos cubiertos por al menos uno).
4. Generamos 1 fila por (per_id, proyecto presupuestario) con:
   - ``actividad`` = mapeo del tipo elegido, con sufijo
     ``-{proyecto presupuestario}`` (o ``transf-60-{proyecto}`` si
     ``1AA``).
   - ``h_sem`` = del tipo elegido.
   - ``horas`` = ``h_sem × días_unión / 7``.
   - ``origen_id`` = proyecto presupuestario.
5. Si un contrato no tiene proyecto presupuestario en línea 1, se
   trata como su propio "proyecto" con clave ``contrato-{id}`` para
   no perder la dedicación.

Mapeo concat(tipo, subtipo, micro) → (actividad, h/sem). Orden
importa: las reglas más específicas se evalúan antes que las que
llevan comodín ``*``.

El centro de coste se obtiene del grupo de investigación al que está
adscrita la persona. Si la persona está en N grupos activos, sus horas
se reparten proporcionalmente a los días activos en cada grupo (una
fila por grupo). Si no está en ningún grupo, se emite una sola fila
con ``centro_de_coste = pendiente`` y anomalía.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from coana.util import read_excel, Árbol

# Reglas en orden de evaluación. La primera que casa gana.
# `*` en cualquier posición es comodín.
_REGLAS: list[tuple[str, str, int]] = [
    ("2PE", "ai-internacional", 10),
    ("2PN", "ai-nacional", 10),
    ("2PV", "ai-regional", 10),
    ("2PA", "ai-nacional", 10),
    ("2PI", "ai-internacional", 10),
    ("2PU", "ai-plan-propio", 3),
    ("1CE", "cátedras-aulas-empresa", 2),
    ("1AA", "transf", 1),
    ("1**", "transf", 8),
]


def _aplicar_reglas(t: str | None, s: str | None, m: str | None) -> tuple[str, int] | None:
    cadena = "".join([(t or " "), (s or " "), (m or " ")])
    for patrón, act, h in _REGLAS:
        if all(p == "*" or p == c for p, c in zip(patrón, cadena)):
            return act, h
    return None


def cargar_proyectos(
    ruta_base: Path,
    árbol_actividades: Árbol | None = None,
    año: int = 2025,
) -> pl.DataFrame:
    inicio_año = date(año, 1, 1)
    fin_año = date(año, 12, 31)

    inv_path = ruta_base / "entrada" / "investigación" / "investigadores en contratos.xlsx"
    proy_path = ruta_base / "entrada" / "investigación" / "proyectos en contratos investigación.xlsx"
    anex_path = ruta_base / "entrada" / "investigación" / "anexos proyectos.xlsx"
    grupos_path = ruta_base / "entrada" / "investigación" / "investigadores en grupos.xlsx"
    for p in (inv_path, proy_path, anex_path):
        if not p.exists():
            return _esquema_vacío()

    inv = read_excel(inv_path)
    proy_raw = read_excel(proy_path)
    anex = read_excel(anex_path)

    # Filtrar líneas sin importe (importe_concedido nulo o 0): no aportan
    # ni vigencia ni proyecto presupuestario significativos.
    proy_raw = proy_raw.filter(
        pl.col("importe_concedido").is_not_null()
        & (pl.col("importe_concedido") > 0)
    )

    # Vigencia del contrato (uniendo líneas) e importe acumulado (para
    # desempate de tipo).
    proy = (
        proy_raw.group_by("contrato").agg(
            pl.col("fecha_inicio").min().alias("contrato_inicio"),
            pl.col("fecha_fin").max().alias("contrato_fin"),
            pl.col("importe_concedido").sum().alias("importe_contrato"),
        )
        .filter(
            (pl.col("contrato_inicio") <= pl.lit(fin_año))
            & (pl.col("contrato_fin") >= pl.lit(inicio_año))
        )
    )
    if proy.is_empty():
        return _esquema_vacío()

    # Proyecto presupuestario de la línea de menor número del contrato
    # (los contratos antiguos no siempre tienen línea 1; tomamos la
    # primera disponible).
    proy_l1 = (
        proy_raw.sort("línea")
        .group_by("contrato", maintain_order=True)
        .agg(pl.col("proyecto").first().alias("proyecto_l1"))
    )
    proy = proy.join(proy_l1, on="contrato", how="left")

    # Anexo (uno por contrato): mapeo → actividad y h/sem
    anex_min = anex.select(
        "contrato", "codex",
        pl.col("tipo_anexo").cast(pl.Utf8),
        pl.col("subtipo_anexo").cast(pl.Utf8),
        pl.col("microtipo_anexo").cast(pl.Utf8),
    )
    proy = proy.join(anex_min, on="contrato", how="left")

    resultados = [
        _aplicar_reglas(r["tipo_anexo"], r["subtipo_anexo"], r["microtipo_anexo"])
        for r in proy.iter_rows(named=True)
    ]
    proy = proy.with_columns(
        pl.Series(
            "actividad_base",
            [r[0] if r else None for r in resultados],
            dtype=pl.Utf8,
        ),
        pl.Series(
            "h_sem",
            [float(r[1]) if r else None for r in resultados],
            dtype=pl.Float64,
        ),
    )

    # Investigadores de contratos vivos
    df = inv.join(proy, on="contrato", how="inner")

    # Periodo efectivo por (per_id, contrato): intersección de fechas
    # de solicitud (o alternativa), fechas del contrato y año.
    ini_part = pl.coalesce(
        pl.col("fecha_inicio_solicitud"),
        pl.col("fecha_inicio_solicitud_alternativa"),
        pl.col("contrato_inicio"),
    )
    fin_part = pl.coalesce(
        pl.col("fecha_fin_solicitud"),
        pl.col("fecha_fin_solicitud_alternativa"),
        pl.col("contrato_fin"),
    )
    df = df.with_columns(
        pl.max_horizontal(ini_part, pl.col("contrato_inicio"), pl.lit(inicio_año)).alias("_ini"),
        pl.min_horizontal(fin_part, pl.col("contrato_fin"), pl.lit(fin_año)).alias("_fin"),
    )
    df = df.with_columns(
        ((pl.col("_fin") - pl.col("_ini")).dt.total_days() + 1)
        .clip(lower_bound=0)
        .alias("días_solape")
    ).filter(pl.col("días_solape") > 0)

    # Clave de agrupación: proyecto presupuestario de la línea 1. Si no
    # existe, usar el propio contrato como clave (proyecto suelto).
    df = df.with_columns(
        pl.coalesce(
            pl.col("proyecto_l1"),
            pl.concat_str([pl.lit("contrato-"), pl.col("contrato").cast(pl.Utf8)]),
        ).alias("proy_clave")
    )

    # Agrupar por (per_id, proy_clave):
    # - Elegir contrato con mayor h_sem (desempate: importe, luego contrato)
    # - Unión de periodos efectivos
    df_agg = _agrupar_por_proyecto_presupuestario(df)
    if df_agg.is_empty():
        return _esquema_vacío()

    # Actividad detallada: {actividad_base}-{proyecto_l1}, salvo 1AA → transf-60-{proyecto_l1}.
    es_1aa = (
        (pl.col("tipo_anexo") == "1")
        & (pl.col("subtipo_anexo") == "A")
        & (pl.col("microtipo_anexo") == "A")
    )
    sufijo_aplicable = (
        pl.col("actividad_base").is_not_null()
        & pl.col("proyecto_l1").is_not_null()
    )
    df_agg = df_agg.with_columns(
        pl.when(es_1aa & pl.col("proyecto_l1").is_not_null())
        .then(pl.concat_str([pl.lit("transf-60-"), pl.col("proyecto_l1")]))
        .when(sufijo_aplicable)
        .then(pl.concat_str([
            pl.col("actividad_base"), pl.lit("-"), pl.col("proyecto_l1"),
        ]))
        .otherwise(pl.col("actividad_base"))
        .alias("actividad")
    )

    # Crear los nodos dinámicos en el árbol de actividades.
    if árbol_actividades is not None:
        nuevos_1aa = (
            df_agg.filter(es_1aa & pl.col("proyecto_l1").is_not_null())
            .select("proyecto_l1").unique()
            .get_column("proyecto_l1").to_list()
        )
        for proyecto in nuevos_1aa:
            try:
                árbol_actividades.añadir_hijo(
                    "transf",
                    f"Asistencia técnica (art 60) en proyecto {proyecto}",
                    proyecto,
                    id_completo=f"transf-60-{proyecto}",
                )
            except (KeyError, ValueError):
                pass
        nuevos = (
            df_agg.filter(~es_1aa & sufijo_aplicable)
            .select("actividad_base", "proyecto_l1").unique()
        )
        for r in nuevos.iter_rows(named=True):
            base, proyecto = r["actividad_base"], r["proyecto_l1"]
            try:
                árbol_actividades.añadir_hijo(
                    base,
                    f"{base} · proyecto {proyecto}",
                    proyecto,
                    id_completo=f"{base}-{proyecto}",
                )
            except (KeyError, ValueError):
                pass

    # Horas por persona en el proyecto: h_sem (del tipo elegido) × días_unión / 7.
    df = df_agg.with_columns(
        (pl.col("h_sem") * pl.col("días_unión").cast(pl.Float64) / 7.0)
        .alias("horas_persona")
    )

    # Centro de coste = grupo de investigación de la persona.
    # Si la persona está en N grupos, repartir horas proporcionalmente a
    # los días activos. Si no, dejar pendiente con anomalía.
    grupos_por_persona = _cargar_grupos_por_persona(grupos_path, año)

    if grupos_por_persona.is_empty():
        df = df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("id_grupo"),
            pl.col("horas_persona").alias("horas"),
            pl.lit(0.0).alias("_días_grupo"),
        )
    else:
        df = df.join(grupos_por_persona, on="per_id", how="left")
        # Suma días activos por (per_id, proy_clave) entre grupos válidos
        tot = (
            df.filter(pl.col("id_grupo").is_not_null())
            .group_by("per_id", "proy_clave")
            .agg(pl.col("_días_grupo").sum().alias("_tot_días"))
        )
        df = df.join(tot, on=["per_id", "proy_clave"], how="left")
        df = df.with_columns(
            pl.when(pl.col("id_grupo").is_not_null() & (pl.col("_tot_días") > 0))
            .then(pl.col("horas_persona") * pl.col("_días_grupo") / pl.col("_tot_días"))
            .otherwise(pl.col("horas_persona"))
            .alias("horas")
        )

    cc = (
        pl.when(pl.col("id_grupo").is_not_null())
        .then(pl.concat_str([pl.lit("grupo-investigación-"), pl.col("id_grupo")]))
        .otherwise(pl.lit("pendiente"))
        .alias("centro_de_coste")
    )

    anomalía = (
        pl.when(pl.col("actividad").is_null())
        .then(pl.concat_str([
            pl.lit("contrato con anexo (codex "),
            pl.col("codex").fill_null("?"),
            pl.lit(") sin mapeo (tipo="), pl.col("tipo_anexo").fill_null("?"),
            pl.lit(", subtipo="), pl.col("subtipo_anexo").fill_null("?"),
            pl.lit(", micro="), pl.col("microtipo_anexo").fill_null("?"),
            pl.lit(")"),
        ]))
        .when(pl.col("id_grupo").is_null())
        .then(pl.lit("persona sin grupo de investigación activo en el año"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("anomalía")
    )

    detalle = pl.concat_str([
        pl.lit("Proyecto "), pl.col("proy_clave"),
        pl.lit(" · contratos "), pl.col("n_contratos").cast(pl.Utf8),
        pl.lit(" · tipo "), pl.col("tipo_anexo").fill_null("?"),
        pl.col("subtipo_anexo").fill_null(""),
        pl.col("microtipo_anexo").fill_null(""),
        pl.lit(" · "), pl.col("h_sem").fill_null(0.0).cast(pl.Utf8), pl.lit(" h/sem"),
        pl.lit(" · "), pl.col("días_unión").cast(pl.Utf8), pl.lit(" días unión"),
        pl.when(pl.col("id_grupo").is_not_null())
        .then(pl.concat_str([pl.lit(" · grupo "), pl.col("id_grupo")]))
        .otherwise(pl.lit("")),
    ])

    return df.select(
        pl.col("per_id").cast(pl.Int64),
        pl.col("actividad").fill_null("pendiente").alias("actividad"),
        cc,
        pl.col("horas").fill_null(0.0).cast(pl.Float64),
        pl.lit("et").alias("método"),
        pl.lit(1.0).alias("factor"),
        pl.lit("investigación").alias("grupo"),
        pl.lit("proyecto").alias("origen"),
        pl.col("proy_clave").alias("origen_id"),
        detalle.alias("detalle"),
        anomalía,
    ).filter(pl.col("horas") > 0)


def _agrupar_por_proyecto_presupuestario(df: pl.DataFrame) -> pl.DataFrame:
    """Agrupa (per_id, proy_clave) eligiendo tipo de mayor h/sem y
    calculando la unión de periodos efectivos."""
    if df.is_empty():
        return df

    # Selección de tipo: max h_sem (con null al final), desempate por
    # importe_contrato y luego por contrato. La fila ganadora aporta
    # actividad_base, tipo/sub/micro, codex, proyecto_l1, h_sem.
    sel = (
        df.sort(
            ["per_id", "proy_clave", "h_sem", "importe_contrato", "contrato"],
            descending=[False, False, True, True, True],
            nulls_last=True,
        )
        .group_by(["per_id", "proy_clave"], maintain_order=True)
        .agg(
            pl.col("actividad_base").first(),
            pl.col("h_sem").first(),
            pl.col("tipo_anexo").first(),
            pl.col("subtipo_anexo").first(),
            pl.col("microtipo_anexo").first(),
            pl.col("codex").first(),
            pl.col("proyecto_l1").first(),
            pl.len().alias("n_contratos"),
        )
    )

    # Unión de periodos efectivos por (per_id, proy_clave)
    días_unión: list[int] = []
    claves: list[tuple] = []
    for (pid, pc), grp in (
        df.select(["per_id", "proy_clave", "_ini", "_fin"])
          .sort(["per_id", "proy_clave", "_ini"])
          .group_by(["per_id", "proy_clave"], maintain_order=True)
    ):
        intervalos = sorted(zip(grp["_ini"].to_list(), grp["_fin"].to_list()))
        # Merge de intervalos
        total = 0
        cur_ini, cur_fin = intervalos[0]
        for ini, fin in intervalos[1:]:
            if ini <= cur_fin:  # solape o contiguo (días, no microsegundos)
                if fin > cur_fin:
                    cur_fin = fin
            else:
                total += (cur_fin - cur_ini).days + 1
                cur_ini, cur_fin = ini, fin
        total += (cur_fin - cur_ini).days + 1
        claves.append((pid, pc))
        días_unión.append(total)

    unión_df = pl.DataFrame({
        "per_id": [c[0] for c in claves],
        "proy_clave": [c[1] for c in claves],
        "días_unión": días_unión,
    })

    return sel.join(unión_df, on=["per_id", "proy_clave"], how="left")


def _cargar_grupos_por_persona(path: Path, año: int) -> pl.DataFrame:
    """Devuelve (per_id, id_grupo, _días_grupo) con grupos activos en el año.

    Filtra a los grupos «de verdad»: se cruza con
    ``grupos a institutos.xlsx`` para descartar los institutos (que en
    ``investigadores en grupos.xlsx`` también figuran con sus códigos
    INIT/INAM/etc. pero no son grupos de investigación a efectos de
    centro de coste).
    """
    if not path.exists():
        return pl.DataFrame(schema={
            "per_id": pl.Int64, "id_grupo": pl.Utf8, "_días_grupo": pl.Float64,
        })
    inicio = date(año, 1, 1)
    fin = date(año, 12, 31)
    g = read_excel(path)
    g = g.with_columns(
        pl.col("fecha_baja").fill_null(fin).alias("_fin"),
    ).with_columns(
        pl.max_horizontal(pl.col("fecha_alta"), pl.lit(inicio)).alias("_ini_sol"),
        pl.min_horizontal(pl.col("_fin"), pl.lit(fin)).alias("_fin_sol"),
    )
    g = g.with_columns(
        ((pl.col("_fin_sol") - pl.col("_ini_sol")).dt.total_days() + 1)
        .clip(lower_bound=0)
        .alias("_días_grupo")
    ).filter(pl.col("_días_grupo") > 0)

    # Filtrar a los grupos válidos según `grupos a institutos.xlsx`.
    mapeo = path.parent / "grupos a institutos.xlsx"
    if mapeo.exists():
        válidos = (
            read_excel(mapeo)
            .select(pl.col("id_grupo").cast(pl.Utf8))
            .get_column("id_grupo").to_list()
        )
        g = g.filter(pl.col("id_grupo").cast(pl.Utf8).is_in(válidos))

    return (
        g.group_by(pl.col("per_id").cast(pl.Int64), pl.col("id_grupo").cast(pl.Utf8))
        .agg(pl.col("_días_grupo").max().alias("_días_grupo"))
    )


def _esquema_vacío() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "per_id": pl.Int64,
        "actividad": pl.Utf8,
        "centro_de_coste": pl.Utf8,
        "horas": pl.Float64,
        "método": pl.Utf8,
        "factor": pl.Float64,
        "grupo": pl.Utf8,
        "origen": pl.Utf8,
        "origen_id": pl.Utf8,
        "detalle": pl.Utf8,
        "anomalía": pl.Utf8,
    })
