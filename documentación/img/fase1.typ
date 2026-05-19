// Diagramas de la Fase 1 con cetz: árbol de ficheros de entrada → engranaje
// (etapa) → árbol de ficheros de salida.
//
// Uso desde la spec:
//   #import "img/fase1.typ": fase1-diagrama, etapa-presupuesto, etapa-amortizaciones, ...
//   #fase1-diagrama()

#import "@preview/cetz:0.4.2"
#import "@preview/fontawesome:0.6.0": *

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#let _color(t) = {
    if t == "dir" { orange.darken(20%) } else if t == "key" { rgb("#b85450") } else if t == "tree" {
        green.darken(40%)
    } else {
        blue.darken(20%)
    }
}

#let _icon(t) = {
    if t == "dir" { fa-icon("folder") } else if t == "tree" { fa-icon("chart-diagram") } else if t == "key" {
        fa-icon("file-excel")
    } else { fa-icon("file-excel") }
}

// Renderiza un árbol de ficheros como contenido Typst (no cetz).
// items: lista de tuplas (profundidad, tipo, etiqueta).
#let _arbol(items) = text(size: 7pt, {
    for (depth, kind, name) in items {
        let weight = if kind == "dir" or kind == "key" { "bold" } else { "regular" }
        block(
            above: 0.3em,
            below: 0pt,
            [#h(depth * 0.7em)#text(fill: _color(kind), weight: weight)[#_icon(kind) #name]],
        )
    }
})

// Caja central con engranaje y dos flechas (con cetz). El ancho de la caja
// se ajusta a la longitud del título.
#let _proceso(titulo) = {
    // 1 unidad cetz ~ 1 cm. ~0.18 cm por carácter en Fira Sans 10pt;
    // dejamos un colchón de 0.6 cm a cada lado.
    let half-w = calc.max(0.9, 0.5 + titulo.len() * 0.13)
    cetz.canvas({
        import cetz.draw: *
        let arr-stroke = 1.5pt + rgb("#b85450")
        line(
            (-half-w - 1.1, 0),
            (-half-w, 0),
            mark: (end: ">", scale: 1.5),
            stroke: arr-stroke,
        )
        rect(
            (-half-w, -0.8),
            (half-w, 0.8),
            fill: rgb("#fce4d6"),
            stroke: 1pt + rgb("#b85450"),
            radius: 0.15,
        )
        content((0, 0.35), text(size: 14pt)[⚙])
        content((0, -0.35), text(weight: "bold", size: 10pt)[#titulo])
        line(
            (half-w, 0),
            (half-w + 1.1, 0),
            mark: (end: ">", scale: 1.5),
            stroke: arr-stroke,
        )
    })
}

// Escala un contenido para que ocupe `width` y/o `height`. Si sólo se pasa
// uno, se mantiene la proporción.
#let _redimensionar(content, width: auto, height: auto) = {
    if width == auto and height == auto { return content }
    layout(_ => {
        let m = measure(content)
        let sx = if width != auto { width / m.width } else { none }
        let sy = if height != auto { height / m.height } else { none }
        if sx == none { sx = sy }
        if sy == none { sy = sx }
        scale(x: sx * 100%, y: sy * 100%, reflow: true, content)
    })
}

// Compone el flujo entradas → proceso → salida como un grid de 3 columnas.
#let _flujo(
    entradas,
    salida,
    titulo-caja: "Fase 1",
    titulo-entradas: "Entradas",
    titulo-salida: "Salida (data/fase1/)",
    width: auto,
    height: auto,
) = _redimensionar(
    grid(
        columns: 3,
        column-gutter: 0.4em,
        align: (top + left, horizon + center, top + left),
        [#text(size: 9pt, weight: "bold", fill: gray)[#titulo-entradas] #v(0.3em) #_arbol(entradas)],
        _proceso(titulo-caja),
        [#text(size: 9pt, weight: "bold", fill: gray)[#titulo-salida] #v(0.3em) #_arbol(salida)],
    ),
    width: width,
    height: height,
)

// ---------------------------------------------------------------------------
// Datos por etapa
// ---------------------------------------------------------------------------

// Cada item: (profundidad, tipo, etiqueta)
// Tipos: "dir", "key" (fichero clave), "tree" (.tree), "file"

#let _entradas-fase1 = (
    (0, "dir", "entradas/"),
    (1, "dir", "estructuras/"),
    (2, "tree", "actividades.tree"),
    (2, "tree", "elementos de coste.tree"),
    (2, "tree", "centros de coste.tree"),
    (2, "tree", "centros de coste por comportamiento.tree"),
    (1, "dir", "presupuesto/"),
    (2, "key", "apuntes presupuesto de gasto.xlsx"),
    (2, "file", "aplicaciones de gasto.xlsx"),
    (2, "file", "artículos de gasto.xlsx"),
    (2, "file", "capítulos de gasto.xlsx"),
    (2, "file", "centros.xlsx"),
    (2, "file", "subcentros.xlsx"),
    (2, "file", "conceptos de gasto.xlsx"),
    (2, "file", "líneas de financiación.xlsx"),
    (2, "file", "tipos de línea.xlsx"),
    (2, "file", "programas presupuestarios.xlsx"),
    (2, "file", "proyectos.xlsx"),
    (2, "file", "subproyectos.xlsx"),
    (2, "file", "tipos de proyecto.xlsx"),
    (1, "dir", "inventario/"),
    (2, "key", "inventario.xlsx"),
    (2, "file", "años amortización por cuenta.xlsx"),
    (2, "file", "ubicaciones a servicios.xlsx"),
    (2, "file", "servicios.xlsx"),
    (1, "dir", "nóminas/"),
    (2, "key", "nóminas y seguridad social.xlsx"),
    (2, "file", "expedientes recursos humanos.xlsx"),
    (2, "file", "categorías recursos humanos.xlsx"),
    (2, "file", "categorías plazas.xlsx"),
    (2, "file", "tipos coste plantilla.xlsx"),
    (2, "file", "cargos.xlsx"),
    (2, "file", "tipos cargo.xlsx"),
    (2, "file", "personas cargos.xlsx"),
    (2, "file", "personas.xlsx"),
    (2, "file", "perceptores.xlsx"),
    (2, "file", "provisiones.xlsx"),
    (2, "file", "conceptos retributivos.xlsx"),
    (1, "dir", "superficies/"),
    (2, "file", "ubicaciones.xlsx"),
    (2, "file", "zonas.xlsx"),
    (2, "file", "edificaciones.xlsx"),
    (2, "file", "complejos.xlsx"),
    (2, "file", "tipos de ubicación.xlsx"),
    (2, "file", "corrector superficie.xlsx"),
    (1, "dir", "consumos/"),
    (2, "file", "energía.xlsx"),
    (2, "file", "agua.xlsx"),
    (2, "file", "gas.xlsx"),
    (2, "file", "distribución OTOP.xlsx"),
    (1, "dir", "docencia/"),
    (2, "key", "pod.xlsx"),
    (2, "key", "pod másteres.xlsx"),
    (2, "file", "estimación horas docencia propia.xlsx"),
    (2, "file", "estudios.xlsx"),
    (2, "file", "grados.xlsx"),
    (2, "file", "másteres.xlsx"),
    (2, "file", "asignaturas grados.xlsx"),
    (2, "file", "asignaturas másteres.xlsx"),
    (2, "file", "microcredenciales.xlsx"),
    (2, "file", "doctorados.xlsx"),
    (1, "dir", "investigación/"),
    (2, "file", "grupos investigación.xlsx"),
    (2, "file", "investigadores en grupos.xlsx"),
    (2, "file", "colaboradores en grupos.xlsx"),
    (2, "file", "tesis.xlsx"),
)

#let _salida-fase1 = (
    (0, "dir", "fase1/"),
    (1, "tree", "actividades.tree"),
    (1, "tree", "centros de coste.tree"),
    (1, "tree", "elementos de coste.tree"),
    (1, "key", "unidades de coste.xlsx"),
    (1, "file", "uc presupuesto.parquet"),
    (1, "file", "uc amortizaciones.parquet"),
    (1, "file", "uc suministros.parquet"),
    (1, "file", "presupuesto sin uc.parquet"),
    (1, "dir", "auxiliares/"),
    (2, "dir", "nóminas/"),
    (3, "file", "uc_ptgas.parquet"),
    (3, "file", "uc_pvi.parquet"),
    (3, "file", "uc_pdi.parquet"),
    (3, "file", "uc_despidos.parquet"),
    (3, "file", "uc_indemnizaciones_asistencias.parquet"),
    (3, "file", "uc_cargos.parquet"),
    (3, "file", "regla_23_dedicación_docente.parquet"),
    (3, "file", "regla_23_dedicación_titulaciones.parquet"),
    (3, "file", "regla_23_dedicación_estudios.parquet"),
    (3, "file", "regla_23_estructura_estudios.parquet"),
    (3, "file", "regla_23_horas_no_oficiales.parquet"),
    (3, "file", "regla_23_atrasos.parquet"),
    (3, "file", "regla_23_expedientes_apartados.parquet"),
    (3, "file", "regla_23_anomalías_resolución.parquet"),
    (3, "file", "regla_23_múltiples_con_grado.parquet"),
    (3, "file", "regla_23_pod_resuelto.parquet"),
    (3, "file", "persona_uc.parquet"),
    (3, "file", "persona_ss.parquet"),
    (3, "file", "multiexpediente.parquet"),
    (2, "dir", "amortizaciones/"),
    (3, "file", "inventario_enriquecido.parquet"),
    (3, "file", "filtrados_*.parquet"),
    (3, "file", "sin_uc.parquet"),
    (2, "file", "cargos_departamentos.parquet"),
    (2, "file", "categoría_última_pdi_pvi.parquet"),
    (2, "file", "filtrados_presupuesto.parquet"),
    (2, "file", "sin_clasificar_presupuesto.parquet"),
    (2, "file", "conteo_*_presupuesto.parquet"),
    (2, "file", "resumen.json"),
)

// ----- Etapa: Presupuesto -----------------------------------------------------
#let _entradas-presupuesto = (
    (0, "dir", "entradas/"),
    (1, "dir", "estructuras/"),
    (2, "tree", "actividades.tree"),
    (2, "tree", "elementos de coste.tree"),
    (2, "tree", "centros de coste.tree"),
    (1, "dir", "presupuesto/"),
    (2, "key", "apuntes presupuesto de gasto.xlsx"),
    (2, "file", "aplicaciones de gasto.xlsx"),
    (2, "file", "centros.xlsx"),
    (2, "file", "subcentros.xlsx"),
    (2, "file", "líneas de financiación.xlsx"),
    (2, "file", "proyectos.xlsx"),
    (2, "file", "tipos de proyecto.xlsx"),
    (2, "file", "programas presupuestarios.xlsx"),
)

#let _salida-presupuesto = (
    (0, "dir", "fase1/"),
    (1, "tree", "actividades.tree"),
    (1, "tree", "centros de coste.tree"),
    (1, "tree", "elementos de coste.tree"),
    (1, "file", "uc presupuesto.parquet"),
    (1, "file", "presupuesto sin uc.parquet"),
    (1, "dir", "auxiliares/"),
    (2, "file", "filtrados_presupuesto.parquet"),
    (2, "file", "sin_clasificar_presupuesto.parquet"),
    (2, "file", "conteo_reglas_presupuesto.parquet"),
    (2, "file", "conteo_cc_presupuesto.parquet"),
    (2, "file", "conteo_ec_presupuesto.parquet"),
    (2, "file", "resumen.json"),
)

// ----- Etapa: Amortizaciones --------------------------------------------------
#let _entradas-amortizaciones = (
    (0, "dir", "entradas/"),
    (1, "dir", "inventario/"),
    (2, "key", "inventario.xlsx"),
    (2, "file", "años amortización por cuenta.xlsx"),
    (2, "file", "ubicaciones a servicios.xlsx"),
    (2, "file", "servicios.xlsx"),
    (1, "dir", "superficies/"),
    (2, "file", "ubicaciones.xlsx"),
    (2, "file", "zonas.xlsx"),
    (2, "file", "edificaciones.xlsx"),
    (2, "file", "complejos.xlsx"),
    (2, "file", "tipos de ubicación.xlsx"),
    (2, "file", "corrector superficie.xlsx"),
    (1, "dir", "consumos/"),
    (2, "file", "distribución OTOP.xlsx"),
)

#let _salida-amortizaciones = (
    (0, "dir", "fase1/"),
    (1, "file", "uc amortizaciones.parquet"),
    (1, "dir", "auxiliares/"),
    (2, "dir", "amortizaciones/"),
    (3, "file", "inventario_enriquecido.parquet"),
    (3, "file", "filtrados_estado.parquet"),
    (3, "file", "filtrados_cuenta.parquet"),
    (3, "file", "sin_cuenta.parquet"),
    (3, "file", "sin_fecha_alta.parquet"),
    (3, "file", "filtrados_fecha.parquet"),
    (3, "file", "detalle_cuentas_filtradas.parquet"),
    (3, "file", "sin_uc.parquet"),
)

// ----- Etapa: Suministros (energía/agua/gas) ----------------------------------
#let _entradas-suministros = (
    (0, "dir", "entradas/"),
    (1, "dir", "consumos/"),
    (2, "file", "energía.xlsx"),
    (2, "file", "agua.xlsx"),
    (2, "file", "gas.xlsx"),
    (1, "dir", "inventario/"),
    (2, "file", "ubicaciones a servicios.xlsx"),
    (2, "file", "servicios.xlsx"),
    (1, "dir", "superficies/"),
    (2, "file", "ubicaciones.xlsx"),
    (2, "file", "zonas.xlsx"),
)

#let _salida-suministros = (
    (0, "dir", "fase1/"),
    (1, "file", "uc suministros.parquet"),
)

// ----- Etapa: Nóminas (UC retributivas + Regla 23) ----------------------------
#let _entradas-nominas = (
    (0, "dir", "entradas/"),
    (1, "dir", "estructuras/"),
    (2, "tree", "actividades.tree"),
    (2, "tree", "centros de coste.tree"),
    (2, "tree", "elementos de coste.tree"),
    (1, "dir", "nóminas/"),
    (2, "key", "nóminas y seguridad social.xlsx"),
    (2, "file", "expedientes recursos humanos.xlsx"),
    (2, "file", "categorías recursos humanos.xlsx"),
    (2, "file", "categorías plazas.xlsx"),
    (2, "file", "conceptos retributivos.xlsx"),
    (2, "file", "perceptores.xlsx"),
    (2, "file", "provisiones.xlsx"),
    (1, "dir", "presupuesto/"),
    (2, "file", "proyectos.xlsx"),
    (2, "file", "centros.xlsx"),
    (2, "file", "subcentros.xlsx"),
    (1, "dir", "docencia/"),
    (2, "key", "pod.xlsx"),
    (2, "key", "pod másteres.xlsx"),
    (2, "file", "estimación horas docencia propia.xlsx"),
    (2, "file", "estudios.xlsx"),
    (2, "file", "grados.xlsx"),
    (2, "file", "másteres.xlsx"),
    (2, "file", "asignaturas grados.xlsx"),
    (2, "file", "asignaturas másteres.xlsx"),
)

#let _salida-nominas = (
    (0, "dir", "fase1/"),
    (1, "dir", "auxiliares/"),
    (2, "dir", "nóminas/"),
    (3, "file", "uc_ptgas.parquet"),
    (3, "file", "uc_pvi.parquet"),
    (3, "file", "uc_pdi.parquet"),
    (3, "file", "uc_despidos.parquet"),
    (3, "file", "uc_indemnizaciones_asistencias.parquet"),
    (3, "file", "uc_cargos.parquet"),
    (3, "file", "regla_23_dedicación_docente.parquet"),
    (3, "file", "regla_23_dedicación_titulaciones.parquet"),
    (3, "file", "regla_23_dedicación_estudios.parquet"),
    (3, "file", "regla_23_estructura_estudios.parquet"),
    (3, "file", "regla_23_horas_no_oficiales.parquet"),
    (3, "file", "regla_23_atrasos.parquet"),
    (3, "file", "regla_23_expedientes_apartados.parquet"),
    (3, "file", "regla_23_anomalías_resolución.parquet"),
    (3, "file", "regla_23_múltiples_con_grado.parquet"),
    (3, "file", "regla_23_pod_resuelto.parquet"),
    (3, "file", "multiexpediente.parquet"),
    (3, "file", "PDI.parquet"),
    (3, "file", "PVI.parquet"),
    (3, "file", "PTGAS.parquet"),
)

// ----- Etapa: Cargos académicos -----------------------------------------------
#let _entradas-cargos = (
    (0, "dir", "entradas/"),
    (1, "dir", "nóminas/"),
    (2, "key", "personas cargos.xlsx"),
    (2, "file", "cargos.xlsx"),
    (2, "file", "nóminas y seguridad social.xlsx"),
    (2, "file", "expedientes recursos humanos.xlsx"),
    (1, "dir", "inventario/"),
    (2, "file", "servicios.xlsx"),
)

#let _salida-cargos = (
    (0, "dir", "fase1/"),
    (1, "dir", "auxiliares/"),
    (2, "file", "cargos_departamentos.parquet"),
    (2, "file", "categoría_última_pdi_pvi.parquet"),
)

// ----- Etapa: Seguridad Social ------------------------------------------------
#let _entradas-ss = (
    (0, "dir", "entradas/"),
    (1, "dir", "nóminas/"),
    (2, "key", "nóminas y seguridad social.xlsx"),
    (2, "file", "expedientes recursos humanos.xlsx"),
    (0, "dir", "fase1/auxiliares/nóminas/"),
    (1, "key", "uc_ptgas.parquet"),
    (1, "file", "uc_pvi.parquet"),
    (1, "file", "uc_pdi.parquet"),
    (1, "file", "uc_despidos.parquet"),
    (1, "file", "uc_cargos.parquet"),
)

#let _salida-ss = (
    (0, "dir", "fase1/auxiliares/nóminas/"),
    (1, "file", "persona_uc.parquet"),
    (1, "file", "persona_ss.parquet"),
)

// ---------------------------------------------------------------------------
// Funciones públicas
// ---------------------------------------------------------------------------

#let fase1-diagrama(width: auto, height: auto) = _flujo(
    _entradas-fase1, _salida-fase1,
    width: width, height: height,
)

#let etapa-presupuesto(width: auto, height: auto) = _flujo(
    _entradas-presupuesto, _salida-presupuesto,
    titulo-caja: "Presupuesto",
    width: width, height: height,
)

#let etapa-amortizaciones(width: auto, height: auto) = _flujo(
    _entradas-amortizaciones, _salida-amortizaciones,
    titulo-caja: "Amortizaciones",
    width: width, height: height,
)

#let etapa-suministros(width: auto, height: auto) = _flujo(
    _entradas-suministros, _salida-suministros,
    titulo-caja: "Suministros",
    width: width, height: height,
)

#let etapa-nominas(width: auto, height: auto) = _flujo(
    _entradas-nominas, _salida-nominas,
    titulo-caja: "Nóminas",
    width: width, height: height,
)

#let etapa-cargos(width: auto, height: auto) = _flujo(
    _entradas-cargos, _salida-cargos,
    titulo-caja: "Cargos académicos",
    width: width, height: height,
)

#let etapa-ss(width: auto, height: auto) = _flujo(
    _entradas-ss, _salida-ss,
    titulo-caja: "Seguridad social",
    width: width, height: height,
)

// ---------------------------------------------------------------------------
// Vista standalone (cuando este fichero se compila directamente).
// ---------------------------------------------------------------------------

#set page(width: 21cm, height: 22cm, margin: 0.4cm)
#set text(font: "Fira Sans", size: 9pt)

#fase1-diagrama()
#pagebreak()
#etapa-presupuesto()
#pagebreak()
#etapa-amortizaciones()
#pagebreak()
#etapa-suministros()
#pagebreak()
#etapa-nominas()
#pagebreak()
#etapa-cargos()
#pagebreak()
#etapa-ss()
