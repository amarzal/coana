// Documento de informes de la Fase 2.
//
// Cada cuadro se carga desde `data/informes/<id>.yaml`, que se
// regenera al ejecutar `uv run coana informes`. El texto prosa lo
// mantiene a mano el usuario; las tablas se reconstruyen sin
// intervención.

#set page(paper: "a4", flipped: true, margin: 1.5cm)
#set text(font: "Calibri", lang: "es", size: 9pt)
#set heading(numbering: "1.1")
#set table(
    inset: (x: 0.5em, y: 0.25em),
    // Zebra striping: filas impares sombreadas para guiar la vista
    // entre el concepto y los importes. La fila 0 es el header.
    fill: (_, row) => if row > 0 and calc.odd(row) { luma(95%) } else { none },
)

#let euro(n) = {
    if n == none { return [—] }
    let v = if type(n) == int { float(n) } else { n }
    let neg = v < 0
    let abs_v = if neg { -v } else { v }
    let entero = calc.floor(abs_v)
    let centavos = calc.round((abs_v - entero) * 100)
    if centavos == 100 {
        centavos = 0
        entero += 1
    }
    // Inserta separadores de miles con punto.
    let s = str(entero)
    let con-puntos = ""
    let n-cifras = s.len()
    for (i, c) in s.codepoints().enumerate() {
        if i > 0 and calc.rem(n-cifras - i, 3) == 0 { con-puntos = con-puntos + "." }
        con-puntos = con-puntos + c
    }
    let signo = if neg { "−" } else { "" }
    [#signo#con-puntos,#{ if centavos < 10 { "0" + str(centavos) } else { str(centavos) } }]
}

#let porcentaje(p) = {
    if p == none { return [] }
    let v = if type(p) == int { float(p) } else { p }
    let entero = calc.floor(v)
    let dec = calc.round((v - entero) * 100)
    if dec == 100 {
        dec = 0
        entero += 1
    }
    let s_dec = if dec < 10 { "0" + str(dec) } else { str(dec) }
    [#str(entero),#s_dec %]
}

#align(center)[
    #text(size: 18pt, weight: "bold")[Informes de contabilidad analítica]

    #v(0.4cm)
    #text(size: 11pt, fill: gray)[Generados a partir de la Fase 1 — Universitat Jaume I]
]

#v(1cm)

= Informes normalizados

== Informe sobre elementos de coste

#let c10_1 = yaml("../../data/informes/cuadro_10_1.yaml")

#table(
    columns: (1fr, auto, auto, auto),
    stroke: none,
    align: (left, right, right, right),
    table.hline(),
    [*Elemento de coste*], [*Importe (€)*], [*% elemento*], [*% total*],
    table.hline(),
    ..for (i, fila) in c10_1.filas.enumerate() {
        let es_grupo = fila.nivel == 1
        let concepto = grid(
            columns: (auto, 1fr),
            column-gutter: 0.6em,
            text(fill: gray, fila.código),
            fila.nombre,
        )
        let imp = euro(fila.importe)
        let pe = porcentaje(fila.pct_elemento)
        let pt = if fila.pct_total != none { porcentaje(fila.pct_total) } else { [] }
        let celdas = if es_grupo {
            (strong(concepto), strong(imp), strong(pe), strong(pt))
        } else {
            (concepto, imp, pe, pt)
        }
        // Separador horizontal antes de cada grupo (excepto el primero).
        if es_grupo and i > 0 {
            (table.hline(stroke: 0.6pt + luma(35%)),) + celdas
        } else {
            celdas
        }
    },
    table.hline(),
    strong[Total], strong(euro(c10_1.total)), [], strong[100,00 %],
    table.hline(),
)

== Informe sobre elementos de ingreso

=== Informe general de ingresos por naturaleza

#block(
    width: 100%,
    inset: 1em,
    fill: luma(95%),
    stroke: 0.5pt + luma(70%),
    radius: 3pt,
    [
        #text(fill: luma(40%))[
            *Pendiente.* El cuadro de elementos de ingreso por
            naturaleza se generará cuando la Fase 1 procese la
            información de ingresos. De momento no hay datos
            disponibles.
        ]
    ],
)

=== Informe general de ingresos por actividades

#let c10_3 = yaml("../../data/informes/cuadro_10_3.yaml")

#block(
    width: 100%,
    inset: 0.6em,
    fill: rgb("#fff4e0"),
    stroke: 0.5pt + rgb("#d8a040"),
    radius: 3pt,
    [
        #text(fill: rgb("#7a5010"), size: 9pt)[
            *Estructura preliminar.* Los importes están a 0,00 €
            mientras la Fase 1 no procese los ingresos. La estructura
            (códigos, jerarquía y denominaciones) sigue ya el cuadro 10.3
            del modelo oficial.
        ]
    ],
)

#v(0.4em)

#table(
    columns: (1fr, auto, auto, auto),
    stroke: none,
    align: (left, right, right, right),
    table.hline(),
    [*Elemento de ingreso por actividad*], [*Importe (€)*], [*% elemento*], [*% total*],
    table.hline(),
    ..for (i, fila) in c10_3.filas.enumerate() {
        let es_grupo = fila.nivel == 1
        let sangría = (fila.nivel - 1) * 0.8em
        let concepto = grid(
            columns: (auto, 1fr),
            column-gutter: 0.6em,
            box(width: sangría)[] + text(fill: gray, fila.código),
            fila.nombre,
        )
        let imp = euro(fila.importe)
        let pe = if fila.pct_elemento != none { porcentaje(fila.pct_elemento) } else { [] }
        let pt = if fila.pct_total != none { porcentaje(fila.pct_total) } else { [] }
        let celdas = if es_grupo {
            (strong(concepto), strong(imp), strong(pe), strong(pt))
        } else {
            (concepto, imp, pe, pt)
        }
        if es_grupo and i > 0 {
            (table.hline(stroke: 0.6pt + luma(35%)),) + celdas
        } else {
            celdas
        }
    },
    table.hline(),
    strong[Total], strong(euro(c10_3.total)), [], strong[—],
    table.hline(),
)

== Informes sobre centros de coste

#let tabla_d_i_p(filas, total, centros_sup, actividades_aux, total_final) = table(
    columns: (1fr, auto, auto, auto),
    stroke: none,
    align: (left, right, right, right),
    table.hline(),
    [*Elemento de coste*], [*Directo (€)*], [*Indirecto (€)*], [*Primario (D+I) (€)*],
    table.hline(),
    ..for (i, fila) in filas.enumerate() {
        let es_grupo = fila.nivel == 1
        let concepto = grid(
            columns: (auto, 1fr),
            column-gutter: 0.6em,
            text(fill: gray, fila.código),
            fila.nombre,
        )
        let d = euro(fila.directo)
        let ii = euro(fila.indirecto)
        let p = euro(fila.primario)
        let celdas = if es_grupo {
            (strong(concepto), strong(d), strong(ii), strong(p))
        } else {
            (concepto, d, ii, p)
        }
        if es_grupo and i > 0 {
            (table.hline(stroke: 0.6pt + luma(35%)),) + celdas
        } else {
            celdas
        }
    },
    table.hline(),
    strong[Total coste primario], strong(euro(total.directo)), strong(euro(total.indirecto)), strong(euro(total.primario)),
    [#text(fill: luma(50%))[Centros superiores #emph[(pendiente Fase 3.a)]]],
        text(fill: luma(60%), euro(centros_sup.directo)),
        text(fill: luma(60%), euro(centros_sup.indirecto)),
        text(fill: luma(60%), euro(centros_sup.primario)),
    [#text(fill: luma(50%))[Actividades auxiliares #emph[(pendiente Fase 3.d)]]],
        text(fill: luma(60%), euro(actividades_aux.directo)),
        text(fill: luma(60%), euro(actividades_aux.indirecto)),
        text(fill: luma(60%), euro(actividades_aux.primario)),
    table.hline(),
    strong[Total], strong(euro(total_final.directo)), strong(euro(total_final.indirecto)), strong(euro(total_final.primario)),
    table.hline(),
)

=== Informe de costes por centros de coste según su finalidad

#let c10_4 = yaml("../../data/informes/cuadro_10_4.yaml")

#table(
    columns: (1fr, auto, auto, auto),
    stroke: none,
    align: (left, right, right, right),
    table.hline(),
    [*#c10_4.encabezado_concepto*], [*Importe (€)*], [*% elemento*], [*% total*],
    table.hline(),
    ..for (i, fila) in c10_4.filas.enumerate() {
        let es_grupo = fila.nivel == 1
        let concepto = grid(
            columns: (auto, 1fr),
            column-gutter: 0.6em,
            text(fill: gray, fila.código),
            fila.nombre,
        )
        let imp = euro(fila.importe)
        let pe = porcentaje(fila.pct_elemento)
        let pt = if fila.pct_total != none { porcentaje(fila.pct_total) } else { [] }
        let celdas = if es_grupo {
            (strong(concepto), strong(imp), strong(pe), strong(pt))
        } else {
            (concepto, imp, pe, pt)
        }
        if es_grupo and i > 0 {
            (table.hline(stroke: 0.6pt + luma(35%)),) + celdas
        } else {
            celdas
        }
    },
    table.hline(),
    strong[Total], strong(euro(c10_4.total)), [], strong[100,00 %],
    table.hline(),
)

=== Informe de costes primarios por centro de coste

#let c10_5 = yaml("../../data/informes/cuadro_10_5.yaml")

El coste primario de un centro es la suma de los costes directos (su CC se conoce con
exactitud desde el dato original) e indirectos (asignados por una regla de reparto) que
le han sido atribuidos en la Fase 1. Las filas «Centros superiores» y «Actividades
auxiliares» están pendientes de implementar la Fase 3 del modelo (imputación desde
centros de nivel superior y desde actividades auxiliares); por eso aparecen a 0 €.

#for centro in c10_5.centros [
    ==== #centro.código_sue --- #centro.nombre

    #tabla_d_i_p(centro.filas, centro.total_coste_primario, centro.centros_superiores, centro.actividades_auxiliares, centro.total)

    #v(0.6em)
]

== Informes sobre actividades

=== Composición del coste de las actividades finalistas

#let c10_7 = yaml("../../data/informes/cuadro_10_7.yaml")

Cruce «actividad finalista × tipo de centro». Solo se cuentan las UC cuya actividad
cuelga del subárbol _principales_ (docencia, investigación y extensión universitaria);
las UC de actividades DAG, anexas, organización, TRUPI y demás no aparecen. Mientras
no se implemente la Fase 3 del modelo, los costes de bibliotecas, laboratorios y
aulas (centros que prestan servicio a las actividades finalistas) siguen en sus
actividades originales y no se desagregan aquí.

#let c10_7_cols = c10_7.columnas

#table(
    columns: (1fr,) + (auto,) * c10_7_cols.len(),
    stroke: none,
    align: (left,) + (right,) * c10_7_cols.len(),
    table.hline(),
    [*Actividad*],
    ..c10_7_cols.map(c => [*#c.nombre (€)*]),
    table.hline(),
    ..for (i, fila) in c10_7.filas.enumerate() {
        let es_grupo = fila.nivel == 1
        let concepto = grid(
            columns: (auto, 1fr),
            column-gutter: 0.6em,
            text(fill: gray, fila.código),
            fila.nombre,
        )
        let celdas = (concepto,) + c10_7_cols.map(c => euro(fila.valores.at(c.id)))
        let celdas = if es_grupo {
            celdas.map(x => strong(x))
        } else {
            celdas
        }
        if es_grupo and i > 0 {
            (table.hline(stroke: 0.6pt + luma(35%)),) + celdas
        } else {
            celdas
        }
    },
    table.hline(),
    strong[Total],
    ..c10_7_cols.map(c => strong(euro(c10_7.total.at(c.id)))),
    table.hline(),
)
