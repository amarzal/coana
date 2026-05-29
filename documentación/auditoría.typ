// =====================================================================
// Auditoría de alineación entre la especificación y el código de CoAna.
// Compila con `typst compile documentación/auditoría.typ`.
// =====================================================================

#set document(
    title: "Auditoría · CoAna spec ↔ código",
    author: "Auditoría automatizada",
)
#set page(paper: "a4", margin: (x: 2cm, y: 2.2cm))
#set text(size: 10.5pt, lang: "es")
#set par(justify: true, leading: 0.6em)
#set heading(numbering: "1.")
#show heading.where(level: 1): set text(size: 14pt)
#show heading.where(level: 2): set text(size: 12pt)
#show heading.where(level: 1): it => block(above: 1.2em, below: 0.6em)[#it]
#show heading.where(level: 2): it => block(above: 0.9em, below: 0.4em)[#it]

#let ok = text(fill: rgb("#1a7f37"))[*✓*]
#let warn = text(fill: rgb("#bf8700"))[*⚠*]
#let miss = text(fill: rgb("#cf222e"))[*✗*]
#let ref(path) = raw(path)
#let cita(sec) = text(fill: luma(45%), size: 9pt)[(#sec)]

#align(center)[
    #text(size: 18pt, weight: "bold")[Auditoría · alineación spec ↔ código]
    #v(0.3em)
    #text(size: 10pt, fill: luma(40%))[CoAna · proyecto `nominas-pdi` · revisión 2026-05-29]
]

#v(0.8em)

#block(
    fill: luma(96%), inset: 10pt, radius: 4pt, stroke: 0.5pt + luma(80%),
)[
    *Alcance.* Comprobar que `coana/` implementa fielmente lo que dice
    `documentación/especificación.typ`. Se han revisado los ejes
    funcionales del sistema (configuración, clasificadores, filtros,
    cargos —con el nuevo resolver multinivel—, tesis, sexenios, reparto
    de la regla 23, centro virtual de no adscritos, incentivos
    residuales, figuras puramente docentes, SS, costes sociales
    calculados, cuadros fase 2, catálogo de pantallas y esquemas de
    Excel) cruzando constantes, tablas de mapeo y fórmulas. La revisión
    es estática (lectura de código y datos de entrada), no ejecuta la
    fase 1.

    *Leyenda.*  #ok alineado    #warn deriva (no rompe pero no es 1:1)    #miss falta o no implementado.
]

= Resumen ejecutivo

#table(
    columns: (auto, auto),
    align: (left, right),
    stroke: 0.5pt + luma(75%),
    inset: 7pt,
    table.header([*Estado*], [*Hallazgos*]),
    [#ok Alineado], [62],
    [#warn Deriva], [6],
    [#miss Falta], [0],
    [*Total revisado*], [*68*],
)

#v(0.4em)

La implementación sigue sustancialmente alineada con la especificación
y ha *mejorado* respecto a la auditoría anterior: cuatro de las ocho
derivas previas están resueltas (rename a
`categorías_docencia_pura_plaza`, literal `2.0` de `cargos.py:308`,
caso `07G` Tesis Doctorales y cobertura de `_MÁSTERES_OFICIALES`).
Las seis derivas vivas son menores; en su mayoría no cambian los
números actuales y la fuente de divergencia es la propia spec, que en
algunos puntos va por detrás del código (catálogo de pantallas, tablas
maestras de tipos de proyecto). El bloque de trabajo reciente
—resolver de cargos multinivel, centro virtual
`no-adscritos-a-grupo-de-investigación`, incentivos
residuales y PS como figura puramente docente— está implementado *y*
documentado en la spec de forma consistente.

= Cambios desde la auditoría anterior

#table(
    columns: (auto, 1fr),
    align: (left, left),
    stroke: 0.5pt + luma(75%),
    inset: 6pt,
    table.header([*Estado*], [*Punto*]),

    [#ok resuelto],
    [*Rename `categorías_asociado_plaza` → `categorías_docencia_pura_plaza`*
    y valor ampliado con `32` (PS). Código y spec coinciden
    (#ref("data/configuración.xlsx"), spec §357).],

    [#ok resuelto],
    [*Literal `2.0` de `cargos.py:308`*: esa línea ya no calcula pagas
    extra; el sitio principal usa `_PAGAS_EXTRA_CARGO`
    (#ref("coana/fase1/cargos.py:242")). Queda un residual en otra ruta
    (deriva 1).],

    [#ok resuelto],
    [*Caso `07G` Tesis Doctorales*: el código asigna
    `dag-escuela-doctorado` (spec §2617), no el literal `dag-doctorado`.
    La regla DOCT/541-A genera `doctorado + proyecto` aparte (§1935).],

    [#ok resuelto],
    [*`_MÁSTERES_OFICIALES`*: la lista de códigos MO del código coincide
    ahora 1:1 con la tabla de la spec.],

    [#ok resuelto],
    [*`VRSPII → dag-vrspii`*: ya figura en `_SUBCENTRO_VICERRECT` y en
    la spec (§1870).],
)

= Estado por área

== Configuración y constantes anuales  #cita[§332-360]

- #ok `cfg_int/cfg_float/cfg_str/cfg_set/cfg_tuple` definidos en
  #ref("coana/util/configuración.py:52-76") con la API esperada.
- #ok `data/configuración.xlsx` contiene las 27 constantes citadas en la
  spec: `año_analizado=2025`, `jornada_anual_pdi=1642`,
  `factor_impartición_docente=2.5`, `sexenio_vivo_años=6`, las cuatro de
  tesis (`104/52/0.10/0.90`), las diez de SS, `pagas_extra_cargo=2`,
  `proyectos_generales_nómina/cargos`, etc.
- #ok `categorías_docencia_pura_plaza =
  07,08,18,21,22,23,24,31,32,36,44,46` (incluye `32`, PS) coincide con
  la spec.
- #ok `categorías_pdi_funcionario = CU,TU,TEU,CEU` coincide con la spec.
- #ok Todos los usos en `clasificador_*`, `cargos.py`,
  `regla23/reparto.py`, `regla23/cargadores/*`, `nóminas/__init__.py` y
  `nóminas/regla_23.py` leen vía `cfg_*`; sin literales hardcoded
  (salvo la deriva 1).
- #warn Residual literal `2.0` en
  #ref("coana/fase1/cargos.py:391") (ruta de reparto de
  `generar_cargos_uc`) en `(2.0 * importe_rd * días / 365)` en lugar de
  `_PAGAS_EXTRA_CARGO`. Mismo resultado hoy; rompe el principio §341.

== Clasificador de actividades  #cita[§1736-2875]

- #ok `_TIPOS_IT`, `_CENTRO_DEPTOS` (29 depts) y `_SUBCENTRO_VICERRECT`
  (incl. `VRSPII`/`VRS`) reproducen las tablas de la spec.
- #ok Reglas Becas, Plan propio, Cátedras y aulas, Artículos 60,
  Investigación nacional/internacional/regional, Otra investigación
  competitiva, Transferencia, Proyectos europeos, Innovación y
  emprendimiento y Extensión universitaria coinciden con la spec.
- #ok `07G`/`DOCT` → `dag-escuela-doctorado` (§2617); DOCT/541-A →
  `doctorado + proyecto` (§1935). *(Deriva previa resuelta.)*
- #ok `_MÁSTERES_OFICIALES` coincide con la tabla MO de la spec.
  *(Deriva previa resuelta.)*
- #ok `MIG` y `UEGD` se manejan en las reglas de investigación nacional
  e internacional (#ref("clasificador_actividades.py:489-490,555-560"))
  conforme a §1909/§1912.
- #warn Comparación `tl != "00"` en investigación nacional
  (#ref("clasificador_actividades.py:556")): la spec §1909 dice «no
  empieza por 00». Equivale solo si `tl` es siempre cadena de
  exactamente 2 caracteres.
- #warn `MIG`/`UEGD` no aparecen en la tabla maestra de tipos de
  proyecto (§1760+); solo en la prosa de las reglas. La spec está
  incompleta, no el código.

== Clasificador de centros de coste  #cita[§2877-3380]

- #ok `_APLICACIONES_SUMINISTROS_DISTRIBUIDOS = {2251, 2252, 2222,
  2223, 2225}` (#ref("clasificador_centros_coste.py:156-158"))
  coincide con spec §2889.
- #ok `_CC_CÁTEDRAS_PROYECTOS`, `_CC_GENÉRICO`, `_CC_SUBCENTRO` y
  `_CC_ESPECÍFICO = {("VI","ED"): "ed"}` reproducen sus tablas de la
  spec (§2920-3375), con la prioridad de `_CC_ESPECÍFICO` aplicada
  primero.
- #ok Regla «Por servicio existe servicio»: integración con
  `servicios.xlsx` + fallback cubre la tabla §2969-3239.
- #ok Excepción del servicio 368 con `_CENTRO_PLAZA_CC`
  (#ref("clasificador_centros_coste.py:365-373")) coincide con §3253-3265.
- #warn Regla «Lo que es de INVES debe ir al centro_origen»
  (#ref("clasificador_centros_coste.py:469-486")) cruza con todo
  `_CC_GENÉRICO`; la spec §3269 dice estrictamente
  TABLA-TRADUCCIÓN-DEPARTAMENTOS (un subconjunto). Funcionalmente
  correcto para el caso de departamento, pero más amplio que lo escrito.

== Filtro del presupuesto  #cita[§3403-3426]

- #ok Regla 1 (capítulos 8 y 9 → fuera):
  #ref("coana/fase1/presupuesto/traductor.py:271").
- #ok Regla 2 (capítulo 1 → fuera): #ref("traductor.py:280").
- #ok Regla 3 (aplicaciones 2321 y 2281 → fuera): #ref("traductor.py:289").
- #ok Regla 4 (capítulo 6 excepto aplicación 6711 → fuera):
  #ref("traductor.py:298").
- #ok Regla 5 (suministros 2231/2232/2233 → fuera):
  #ref("traductor.py:307-321"), con descripciones explícitas.

== Cargos académicos · resolver multinivel  #cita[§4833-4851, §5177-5184]

- #ok Asimilación al RD 1086/1989: cruce con `cargos real decreto.xlsx`
  (`importe_mensual → importe_rd`), peso `días × importe_rd`, reparto
  proporcional por persona (#ref("coana/fase1/cargos.py:365-430")).
- #ok Lógica CR 19/64: proyectos generales → reparto por persona;
  proyectos NO generales → UC propia
  (#ref("coana/fase1/nóminas/regla_23.py:897-954")).
- #ok Cargador de cargos para regla 23:
  #ref("coana/fase1/regla23/cargadores/cargos.py:73-112") aplica
  `dedicación_porcentual > 0` → porcentaje sobre horas no docentes;
  fallback `dedicación_horaria`; filas sin dato se descartan.
- #ok Prorrateo por días inclusive: `_dias_solape_2025_expr`
  (#ref("coana/fase1/cargos.py:110-142")).
- #ok *Resolver de cargos multinivel* (nuevo): `_resolver`
  (#ref("coana/fase1/cargos.py:479-565")) resuelve los tokens
  `act_cargo`/`cc_cargo` de `cargos.xlsx` en cascada — override de
  titulación, propagación por moda de `servicio`/`titulación`,
  patrón SERVICIO, patrón TITULACIÓN/CENTROTITULACION vía
  `titulaciones actividad centro.xlsx`, fallback cruzado por servicio,
  override de centro, y anomalía «patrón sin resolver» como última red.
  Refina (no contradice) la spec.

== Tesis  #cita[§4747-4779]

- #ok Constantes leídas de `configuración.xlsx`: `104/52/0.10/0.90`
  (#ref("coana/fase1/regla23/cargadores/tesis.py:50-53")).
- #ok Filtros: fecha de lectura ≥ 1/1/año, inicio ≤ fin año, descarte
  de estados `B/BV/BM`, solape > 0 (#ref("tesis.py:67-93")).
- #ok Horas = `base × días/365` (#ref("tesis.py:111")).
- #ok Reparto: tutor 10 %, directores 90 % / N_directores
  (#ref("tesis.py:104-149")).
- #ok Enriquecimiento con `doctorados.xlsx` y
  `doctorados actividad centro.xlsx`; fallback a actividad/centro
  `pendiente` con anomalía explícita.

== Sexenios  #cita[§56, §4897]

- #ok Umbral `date(año − cfg_int("sexenio_vivo_años"), 12, 31)`
  = `año − 6` (#ref("coana/fase1/regla23/reparto.py:442")).
- #ok El sexenio es *dato informativo*: `sexenio_vivo` se calcula y se
  expone, pero la cascada (`doc/ges/inv_final`) no lo usa. Coherente
  con la spec §4897.

== Reparto de la masa · incentivos residuales  #cita[§4952-4965]

- #ok Detección automática de «solo incentivos del año anterior»:
  `_detecta_incentivos_residuales`
  (#ref("coana/fase1/regla23/uc_reparto.py:288")) marca a quien tiene
  coste `V` solo en marzo (`_MES_INCENTIVOS=3`) y CR `67`
  (`_CR_INCENTIVOS_AÑO_ANTERIOR`), ignorando atrasos `I`.
- #ok El override se aplica *condicionalmente* en la rama de
  fallback sin dedicación: las personas marcadas → (UJI, UJI), el
  resto → (pendiente, pendiente)
  (#ref("uc_reparto.py:195-237")).
- #ok La spec documenta el patrón exacto y nombra las constantes
  (§4957-4963).

== Centro virtual de no adscritos  #cita[§1364, §4870, §4936]

- #ok `NO_ADSCRITOS_CC = "no-adscritos-a-grupo-de-investigación"` y
  `asegurar_no_adscritos` crean *siempre* el nodo como hijo de `inves`
  (#ref("coana/fase1/investigación.py:53,74,100")), exista o no el xlsx
  de grupos.
- #ok El cargador de proyectos usa `NO_ADSCRITOS_CC` (no `pendiente`)
  cuando la persona no está en ningún grupo
  (#ref("coana/fase1/regla23/cargadores/proyectos.py:312-318")).
- #ok Las filas sintéticas de investigación rellenan el centro nulo con
  `NO_ADSCRITOS_CC` (#ref("coana/fase1/regla23/reparto.py:294,314")).
- #ok La spec lo describe en los tres puntos del flujo.

== Figuras puramente docentes (associats + PS)  #cita[§58-63, §4928]

- #ok `_CATEGORIAS_DOCENCIA_PURA = cfg_set("categorías_docencia_pura_plaza")`
  (#ref("coana/fase1/regla23/reparto.py:68")); `_per_ids_docencia_pura`
  lee la nueva clave; toda la jornada va a docencia (guardado con
  `DOC > 0`) en #ref("reparto.py:178-180").
- #ok El nombre de columna `es_asociado` se mantiene por compatibilidad
  del parquet, con semántica «figura puramente docente»
  (#ref("reparto.py:111-114")).
- #ok La spec actualiza Vocabulario, Configuración, Fase de reparto y el
  esquema de columnas para nombrar a associats (PAA/PAL) y substituts
  (PS) como figuras puramente docentes.
- #ok Etiqueta del visor: `es_asociado → "Solo docencia"`
  (#ref("coana/web/services/reducciones_sindicales.py:99")).

== Reparto SS por persona  #cita[§4918-4965]

- #ok Algoritmo en dos pasos: persona→expediente proporcional a bruto,
  después expediente→(actividad, centro) proporcional al importe de las
  UC retributivas (#ref("coana/fase1/nóminas/__init__.py:1135-1178")).
- #ok `ss_total_persona = ss_cotizada + ss_calculada`;
  `persona_ss.parquet` y `ss_por_expediente.parquet` como artefactos.
- #ok Detección de SS cotizada: aplicación que empieza por `12`.

== Costes sociales calculados (clases pasivas)  #cita[§4515-4534]

- #ok Filtro «PDI funcionario sin SS cotizada» cruzando
  `categorías_pdi_funcionario = {CU, TU, TEU, CEU}` con personas sin
  aplicación `12*` (#ref("coana/fase1/nóminas/__init__.py:800-801")).
- #ok Fórmula completa (`BASE = min(TOTAL, ss_base_máxima)`, `CC =
  23,6 %`, reducción `6,5 %` del CC, `MEI 0,67 %`, `FP 0,70 %`, tramos
  de solidaridad 1,1×/1,5× con tipos 0,92/1,00/1,17 %), todos los
  porcentajes vía `cfg_float`
  (#ref("nóminas/__init__.py:732-746")).

== Cuadros Fase 2  #cita[§4966-5066]

- #ok Los cinco cuadros (`10_1, 10_3, 10_4, 10_5, 10_7`) se registran y
  ejecutan en orden (#ref("coana/fase2/__init__.py:16-30")); 10.2 y
  10.6 ausentes a propósito.
- #ok `_cargar_árbol` prefiere los árboles enriquecidos en
  `data/fase1/*.tree` y cae a entrada
  (#ref("coana/fase2/calculo.py:85-94")).
- #ok Generan `.yaml` + `.xlsx`. Los cambios recientes en
  `data/informes/cuadro_10_*` son regeneración numérica, sin cuadros
  ni esquemas nuevos.

== App: catálogo de pantallas  #cita[§1480-1692]

- #ok Existen y se registran los once bloques: Entradas, Presupuesto,
  Amortizaciones, Personal (con vista 360º), Regla 23, Investigación,
  Cargos académicos, Superficies, Resultados Fase 1, Informes a la
  carta y Reducciones sindicales (#ref("coana/web/app.py:50-68")).
- #warn La tabla maestra de la barra lateral (§1480-1490) sigue listando
  solo 7-8 grupos y omite Investigación, Reducciones sindicales e
  Informes; además describe Personal con el layout antiguo
  «Expedientes PDI / Persona» en vez de la vista 360º (§1601). Las dos
  tablas-catálogo de la spec son internamente inconsistentes. Deriva de
  la spec, no del código.

== Schemas de ficheros de entrada  #cita[§543-1316]

- #ok `apuntes presupuesto de gasto.xlsx`, `pod.xlsx`, `tesis.xlsx` y
  `sexenios.xlsx`: todas las columnas usadas por el código existen.
- #ok `nóminas y seguridad social.xlsx`: las columnas citadas existen;
  `sector` y `per_id` provienen de `expedientes recursos humanos.xlsx`
  (join por `expediente`), no del fichero de nóminas. Sin mismatch.

== Tablas de mapeo críticas  #cita[§5050-5200]

- #ok `_PTGAS_CAT_XXX` (#ref("coana/fase1/nóminas/__init__.py:88-91")),
  `_PDI_CAT_XXX` (`:95-109`), `_PTGAS_CR_YYY` (`:114-132`),
  `_MAPEO_SECTOR = {PAS→PTGAS, PI→PVI}` (`:33`) y
  `_PRELACIÓN_SECTOR = [PTGAS, PVI, PDI, Otros]` (`:38`) reproducen
  §5118-5200.
- #warn La spec §5003 afirma que «ya no existe la idea de sector
  principal con prelación PTGAS > PVI > PDI > Otros», pero el apéndice
  §5200 documenta `_PRELACIÓN_SECTOR` como vivo y la constante sigue en
  uso (`__init__.py:38` y `web/services/cargos.py:212`). Contradicción
  interna de la spec; el código es correcto y consistente.

= Derivas detectadas (en orden de impacto)

#table(
    columns: (auto, 1fr, 1.4fr),
    align: (left, left, left),
    stroke: 0.5pt + luma(75%),
    inset: 6pt,
    table.header([*Nº*], [*Hallazgo*], [*Acción sugerida*]),

    [1],
    [#warn *Literal `2.0`* en #ref("coana/fase1/cargos.py:391")
    (ruta de reparto de `generar_cargos_uc`) en lugar de
    `_PAGAS_EXTRA_CARGO`. Uno de los dos sitios de estimación de extras
    se migró; este quedó sin migrar.],
    [Cambiar `2.0` por `_PAGAS_EXTRA_CARGO`.],

    [2],
    [#warn *Catálogo de pantallas desactualizado en la spec*: la tabla
    maestra §1480-1490 omite Investigación, Reducciones sindicales e
    Informes, y describe Personal con el layout previo a la vista 360º
    (§1601).],
    [Actualizar la tabla maestra de la barra lateral para reflejar los
    once bloques reales y la vista 360º de Personal.],

    [3],
    [#warn *Contradicción §5003 ↔ §5200* sobre `_PRELACIÓN_SECTOR`: una
    sección dice que el «sector principal» ya no existe; el apéndice y
    el código lo documentan vivo.],
    [Reconciliar ambos pasajes de la spec (la constante sí se usa).],

    [4],
    [#warn *Regla «INVES → centro_origen»*
    (#ref("clasificador_centros_coste.py:469-486")) cruza con todo
    `_CC_GENÉRICO`; spec §3269 dice estrictamente
    TABLA-TRADUCCIÓN-DEPARTAMENTOS (un subconjunto).],
    [Restringir el lookup a las claves de departamentos, o ampliar la
    spec para incluir el resto de centros.],

    [5],
    [#warn *Comparación `tl != "00"`* en investigación nacional
    (#ref("clasificador_actividades.py:556")) en lugar de «no empieza
    por 00» que dice spec §1909.],
    [Cambiar a `~tl.str.starts_with("00")` o aclarar en spec que `tl`
    es siempre cadena de dos caracteres.],

    [6],
    [#warn *`MIG`/`UEGD`* manejados en código
    (#ref("clasificador_actividades.py:489-490,555-560")) pero ausentes
    de la tabla maestra de tipos de proyecto §1760+ de la spec.],
    [Añadirlos a la tabla maestra. Es deriva de la spec, no del código.],
)

= Recomendaciones

+ *Sustituir el literal `2.0`* de #ref("cargos.py:391") por
    `_PAGAS_EXTRA_CARGO`, cerrando la última ocurrencia de la
    externalización de constantes (deriva 1).
+ *Actualizar el catálogo de pantallas de la spec* (§1480-1490):
    añadir Investigación, Reducciones sindicales e Informes y describir
    Personal como vista 360º (deriva 2).
+ *Reconciliar §5003 con §5200* sobre la prelación de sector: dejar una
    única versión (deriva 3).
+ *Decidir la amplitud de la regla INVES*: estrechar el lookup a
    departamentos o ampliar la spec (deriva 4).
+ *Cambiar `tl != "00"` por `~starts_with("00")`* (o documentar la
    invariante de longitud) y *añadir `MIG`/`UEGD`* a las tablas
    maestras de tipos de proyecto (derivas 5 y 6).

= Conclusión

La implementación es fiel a la especificación en sus partes principales
(filtros, clasificadores, fórmulas de SS y costes calculados, cargos
con el nuevo resolver multinivel, tesis, cascada de la regla 23,
incentivos residuales, centro virtual de no adscritos, figuras
puramente docentes y cuadros 10.x). No se ha detectado ningún bloque
sin implementar. El estado ha mejorado: cuatro derivas previas están
resueltas y las seis vivas son menores. La fuente principal de
divergencia restante es la propia spec, que en varios puntos va por
detrás del código (catálogo de pantallas, tablas maestras de tipos de
proyecto, contradicción sobre la prelación de sector). El único
hallazgo del lado del código es el literal `2.0` en `cargos.py:391`.
