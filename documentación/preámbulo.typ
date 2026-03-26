#import "@preview/itemize:0.2.0" as el
#import "@preview/t4t:0.4.3": get
#import "@preview/fontawesome:0.6.0": fa-icon
#import "@preview/splash:0.5.0": google


// --- Funciones auxiliares ---

#let tree2dict(ruta) = {
    let líneas = read(ruta).split("\n")

    // pila: array de (nivel, código-padre, nº-hijos)
    // Empezamos con un nodo raíz virtual a nivel -1
    let pila = ((-1, "", 0),)
    let dict = (:)

    for línea in líneas {
        let stripped = línea.trim()
        if stripped == "" or stripped.starts-with("#") { continue }

        // Calcular nivel por indentación (4 espacios por nivel)
        let indent = línea.len() - línea.trim(at: start).len()
        let nivel = calc.div-euclid(indent, 4)

        // Separar descripción | identificador
        let partes = stripped.split("|")
        let ident = if partes.len() > 1 { partes.last().trim() } else { "" }

        // Retroceder en la pila hasta encontrar el padre (nivel < actual)
        while pila.last().at(0) >= nivel {
            pila.pop()
        }
        let padre = pila.last()

        // Incrementar nº de hijos del padre
        let n-hijos = padre.at(2) + 1
        pila.last() = (padre.at(0), padre.at(1), n-hijos)

        // Generar código
        let num = str(n-hijos)
        if n-hijos < 10 { num = "0" + num }
        let código = if padre.at(1) == "" { num } else { padre.at(1) + "." + num }

        // Apilar este nodo (sin hijos aún)
        pila.push((nivel, código, 0))

        // Registrar en el diccionario
        if ident != "" {
            dict.insert(ident, código)
        }
    }

    return dict
}

#let act = tree2dict("data/entrada/estructuras/actividades.tree")
#let ele = tree2dict("data/entrada/estructuras/elementos de coste.tree")
#let cen = tree2dict("data/entrada/estructuras/centros de coste.tree")

#let color = (
    "act": google.red-berry,
    "ele": google.dark-green-1,
    "cen": google.orange.darken(20%),
    "": red,
)

#let val(v) = text(fill: blue)[#raw(get.text(v))] // Valor

#let etq(v, clave-color: "") = text(fill: color.at(clave-color))[#raw(get.text(v))] // Etiqueta

#let etqcod(diccionario, clave, clave-color: "") = (
    if clave-color != "" {
        (
            etq(clave, clave-color: clave-color)
                + text(size: 0.65em, baseline: 0.25em, font: "Fira Code", raw(get.text(diccionario.at(
                    clave,
                    default: "?",
                ))))
        )
    } else {
        etq(clave, clave-color: "")
    }
)

#let etqact(clave) = etqcod(act, clave, clave-color: "act")
#let etqele(clave) = etqcod(ele, clave, clave-color: "ele")
#let etqcen(clave) = etqcod(cen, clave, clave-color: "cen")

#let código(c) = raw(get.text(c)) // Código
#let inexistente() = highlight(fill: red, text(fill: yellow)[INEXISTENTE])
#let nombre-regla(nombre) = [_*#nombre*_ \ ]


#let ruta(..path) = {
    let path = path.pos()
    if not path.last().contains(".") {
        let icon = fa-icon("folder")
        let text = text(weight: "bold")[#raw(path.join("/"))]
        return [#icon~#text]
    } else {
        let ext = path.last().split(".").last()
        let icon = if ext == "xlsx" {
            fa-icon("file-excel", size: .8em)
        } else if ext == "tree" {
            fa-icon("sitemap", size: .8em)
        } else if ext == "typ" {
            fa-icon("file-code", size: .8em)
        } else {
            fa-icon("file", size: .8em)
        }
        let text = text(weight: "bold")[#raw(path.join("/"))]
        return [#icon~#text]
    }
}

#let campo(nombre) = text(fill: purple, raw(get.text(nombre)))
#let nota(texto) = highlight(text(fill: red, size: 0.8em, texto))
// --- Formato del documento ---
#let reglas(body) = {
    show list: it => {
        el.default-enum-list(
            body-format: (
                outer: (
                    stroke: gray,
                    inset: 5pt,
                ),
            ),
        )[#it]
    }
    body
}

#let coana = text(weight: "bold")[CoAna]
#let app = text(weight: "bold")[app]


#let tabula_ficheros_y_campos(fyc) = align(center, table(
    columns: 2,
    align: (left, left),
    stroke: none,
    table.header(
        table.hline(),
        table.cell(fill: gray.lighten(20%))[*Fichero/Campo*], table.cell(fill: gray.lighten(20%))[*Descripción*],
        table.hline(),
    ),

    ..for (fichero, contenido) in fyc.pairs() {
        (
            (
                table.hline(stroke: .5pt),
                table.cell(fill: gray.lighten(70%), ruta(fichero)),
                table.cell(fill: gray.lighten(70%), contenido.descripción),
                ..for (c, descripción) in contenido.campos.pairs() {
                    (campo(c), descripción)
                },
            )
        )
    },
    table.hline(),
))


#let formato = doc => {
    set text(font: "Fira Sans", size: 9pt, lang: "es", number-width: "tabular")
    show raw: set text(font: "Fira Code", size: 1.2em)
    set page(margin: (left: 2cm, right: 1cm, y: 1.5cm), numbering: "1/1", number-align: center)
    set par(justify: true)
    show table: set par(justify: false)
    show table: set block(breakable: true)
    set heading(numbering: "1.1.1.1.1")
    show heading: it => {
        v(1.5em, weak: true)
        let text-head = if (it.level == 1) {
            text.with(14pt, weight: "extrabold")
        } else if (it.level == 2) {
            text.with(12pt, weight: "bold")
        } else if (it.level == 3) {
            text.with(11pt, weight: "semibold")
        } else {
            text.with(10pt, weight: "medium")
        }
        let núm = if counter(heading).get() != (0,) {
            counter(heading).display()
        } else {
            []
        }
        rect(
            stroke: none,
            inset: 0pt,
            place(
                right,
                dx: -100% - 0.618em,
                text-head(núm),
            )
                + text-head(it.body),
        )
        v(1.5em, weak: true)
    }

    set list(body-indent: 1em)

    doc
}
