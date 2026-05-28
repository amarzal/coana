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
    #text(size: 10pt, fill: luma(40%))[CoAna · proyecto `nominas-pdi`]
]

#v(0.8em)

#block(
    fill: luma(96%), inset: 10pt, radius: 4pt, stroke: 0.5pt + luma(80%),
)[
    *Alcance.* Comprobar que `coana/` implementa fielmente lo que dice
    `documentación/especificación.typ`. Se han revisado los doce ejes
    funcionales del sistema (configuración, clasificadores, filtros,
    cargos, tesis, sexenios, SS, costes sociales calculados, cuadros
    fase 2, catálogo de pantallas, esquemas de Excel) cruzando las
    constantes, tablas de mapeo y fórmulas. La revisión es estática
    (lectura de código y datos de entrada), no ejecuta la fase 1.

    *Leyenda.*  #ok alineado    #warn deriva (no rompe pero no es 1:1)    #miss falta o no implementado.
]

= Resumen ejecutivo

#table(
    columns: (auto, auto),
    align: (left, right),
    stroke: 0.5pt + luma(75%),
    inset: 7pt,
    table.header([*Estado*], [*Hallazgos*]),
    [#ok Alineado], [53],
    [#warn Deriva], [8],
    [#miss Falta], [0],
    [*Total revisado*], [*61*],
)

#v(0.4em)

La implementación está sustancialmente alineada con la especificación.
Los ocho puntos de deriva son menores: en la mayoría de los casos no
cambiarían los números actuales — afectarían solo si aparecieran datos
con códigos no cubiertos —, pero rompen el principio 1:1 con la spec.
No se han detectado bloques no implementados.

= Estado por área

== Configuración y constantes anuales  #cita[§332-360]

- #ok `cfg_int/cfg_float/cfg_str/cfg_set/cfg_tuple` definidos en
  #ref("coana/util/configuración.py:42-76") con la API esperada.
- #ok `data/configuración.xlsx` contiene las 24 constantes citadas en
  la spec: `año_analizado=2025`, `jornada_anual_pdi=1642`,
  `factor_impartición_docente=2.5`, `sexenio_vivo_años=6`,
  `tesis_horas_tiempo_completo=104`, `tesis_horas_tiempo_parcial=52`,
  `tesis_pct_tutor=0.10`, `tesis_pct_directores=0.90`, los diez de SS,
  `pagas_extra_cargo=2`, etc.
- #ok `categorías_asociado_plaza = 07,08,18,21,22,23,24,31,36,44,46`
  coincide con la spec.
- #ok `categorías_pdi_funcionario = CU,TU,TEU,CEU` coincide con la spec.
- #ok `proyectos_generales_nómina` y `proyectos_generales_cargos`
  coinciden con sus listados en spec.
- #ok Todos los usos en `clasificador_centros_coste.py`, `cargos.py`,
  `regla23/reparto.py`, `regla23/cargadores/tesis.py`,
  `regla23/cargadores/pod*.py`, `nóminas/__init__.py`,
  `nóminas/regla_23.py` leen vía `cfg_*`; no hay literales hardcoded.
- #warn #ref("coana/fase1/cargos.py:308") usa literal `2.0` en
  `(2.0 * importe_rd * días / 365)` en lugar de `_PAGAS_EXTRA_CARGO`;
  la otra ocurrencia (#ref("cargos.py:194")) sí usa la constante. Mismo
  resultado hoy, pero rompe el principio de la spec §254.

== Clasificador de actividades  #cita[§1736-2875]

- #ok `_TIPOS_IT` reproduce los 31 tipos IT de §1754-1785.
- #ok TABLA-TRADUCCIÓN-DEPARTAMENTOS y TABLA-TRADUCCIÓN-VICES están
  como `_CENTRO_DEPTOS` y `_SUBCENTRO_VICERRECT` en
  #ref("coana/fase1/clasificador_actividades.py:53-64") y `27-34`.
- #ok Reglas Becas, Plan propio, Cátedras y aulas, Artículos 60,
  Investigación nacional/internacional/regional, Otra investigación
  competitiva, Otras actividades de transferencia, Proyectos europeos,
  Innovación y emprendimiento y Otras actividades de extensión
  universitaria coinciden con sus tablas y proyectos especiales en
  spec §2133-2484.
- #ok Tabla de Formación permanente cubierta para EPM, EPDE, EPDEX,
  EPC, EPMI, CUID y PAU en #ref("clasificador_actividades.py:759-789").
- #warn Spec §2620 dice que `07G` (Tesis Doctorales) genera
  `etqact("doctorado") + proyecto`, pero el código
  (#ref("clasificador_actividades.py:755-757")) lo asigna a literal
  `dag-doctorado` (sin sumar proyecto y con prefijo `dag-`). Es la
  deriva más visible.
- #warn `_MÁSTERES_OFICIALES` (#ref("clasificador_actividades.py:638-645"))
  cubre solo `MO, MO08, MO09, MO10, MO12`. La spec §1796-1806
  enumera además `MO06, MO07, MO11, MO14, MO15`. Probablemente no
  haya proyectos vivos de esos tipos, pero la lista no coincide 1:1.
- #warn Comparación `tl != "00"` en investigación nacional/
  internacional (#ref("clasificador_actividades.py:582,586,612")):
  la spec §1904 dice «no empieza por 00». Equivale solo si `tl` es
  siempre cadena de exactamente 2 caracteres.
- #warn `MIG` y `UEGD` se manejan en código
  (#ref("clasificador_actividades.py:515-516,586")) pero no aparecen
  en la tabla maestra §1754-1786. Es la spec la que está incompleta.
- #warn `VRSPII → dag-vrspii` en `_SUBCENTRO_VICERRECT` no aparece
  en TABLA-TRADUCCIÓN-VICES (spec solo tiene `VRS`). Compatible pero
  no documentado.

== Clasificador de centros de coste  #cita[§2877-3380]

- #ok `_APLICACIONES_SUMINISTROS_DISTRIBUIDOS = {2251, 2252, 2222,
  2223, 2225}` en #ref("clasificador_centros_coste.py:150-152")
  coincide con spec §2885.
- #ok `_CC_CÁTEDRAS_PROYECTOS` (#ref("clasificador_centros_coste.py:22-31"))
  reproduce los 45 códigos de §2906-2954 (incluidos los marcados
  «NO ENCONTRADO»).
- #ok `_CC_GENÉRICO` (#ref("clasificador_centros_coste.py:79-147"))
  cubre los `centro/%` de §3263-3375.
- #ok `_CC_SUBCENTRO` (#ref("clasificador_centros_coste.py:39-76"))
  cubre los `%/subcentro` de §3273-3308.
- #ok `_CC_ESPECÍFICO = {("VI","ED"): "ed"}` coincide con §3368.
- #ok Regla «Por servicio existe servicio»: integración con
  `servicios.xlsx` + `_SERVICIO_CC_FALLBACK` cubre la tabla §2969-3239.
- #ok Excepción del servicio 368 con `_CENTRO_PLAZA_CC`
  (#ref("clasificador_centros_coste.py:359-367")) coincide con §3244-3255.
- #warn Regla «Lo que es de INVES debe ir al centro_origen»
  (#ref("clasificador_centros_coste.py:462-484")) usa todo
  `_CC_GENÉRICO`; la spec §3257 dice estrictamente
  TABLA-TRADUCCIÓN-DEPARTAMENTOS. Funcionalmente correcto pero más
  amplio que la regla escrita.

== Filtro del presupuesto  #cita[§3403-3426]

- #ok Regla 1 (capítulos 8 y 9 → fuera):
  #ref("coana/fase1/presupuesto/traductor.py:271").
- #ok Regla 2 (capítulo 1 → fuera): #ref("traductor.py:280").
- #ok Regla 3 (aplicaciones 2321 y 2281 → fuera): #ref("traductor.py:289").
- #ok Regla 4 (capítulo 6 excepto aplicación 6711 → fuera):
  #ref("traductor.py:298").
- #ok Regla 5 (suministros 2231/2232/2233 → fuera):
  #ref("traductor.py:307"), con descripciones explícitas.

== Cargos académicos  #cita[§4833-4851, §5177-5184]

- #ok Asimilación al RD 1086/1989: cruce con `cargos real decreto.xlsx`
  en #ref("coana/fase1/cargos.py:97-126").
- #ok Lógica CR 19/64 / proyectos NO generales en
  #ref("coana/fase1/nóminas/regla_23.py:897-954").
- #ok Cargador de cargos para regla 23:
  #ref("coana/fase1/regla23/cargadores/cargos.py:5-22") aplica
  `dedicación_porcentual > 0` → porcentaje sobre horas no docentes;
  fallback a `dedicación_horaria`.
- #ok Prorrateo por días: `_dias_solape_2025_expr`
  (#ref("coana/fase1/cargos.py:62-94")).
- #warn Literal `2.0` en #ref("coana/fase1/cargos.py:308") repetido
  (ver §«Configuración»).

== Tesis  #cita[§4747-4779]

- #ok Constantes leídas de `configuración.xlsx`:
  `BASE_HORAS_C=104`, `BASE_HORAS_P=52`, `RATIO_TUTOR=0.10`,
  `RATIO_DIRECTORES=0.90`
  (#ref("coana/fase1/regla23/cargadores/tesis.py:50-53")).
- #ok Filtros: fecha de lectura ≥ 1/1/año, inicio ≤ fin año,
  descarte de estados `B/BV/BM`, solape > 0
  (#ref("tesis.py:66-93")).
- #ok Horas = `base × días/365` (#ref("tesis.py:110-114")).
- #ok Reparto: tutor 10 %, directores 90 % / N_directores
  (#ref("tesis.py:122-149")).
- #ok Enriquecimiento con `doctorados.xlsx` y
  `doctorados actividad centro.xlsx`; fallback a actividad/centro
  `pendiente` con anomalía explícita.

== Sexenios  #cita[§56, §4897]

- #ok Umbral `año − cfg_int("sexenio_vivo_años") = año − 6`
  (#ref("coana/fase1/regla23/reparto.py:429")).
- #ok Tras la revisión de la cascada, el sexenio queda como dato
  informativo (no afecta al reparto). Coherente con la spec §4897.
- #ok Columna `sexenio_vivo` expuesta en la tabla maestra de
  Personal · PDI (#ref("coana/web/services/persona_360.py")).

== Reparto SS por persona  #cita[§4918-4965]

- #ok Algoritmo en dos pasos documentado y aplicado:
  persona→expediente proporcional a bruto, después
  expediente→(actividad, centro) proporcional al importe de las UC
  retributivas. Ver #ref("coana/fase1/nóminas/__init__.py:981-1040").
- #ok `persona_ss.parquet` con `per_id, actividad, centro_de_coste,
  ss_proporcional`.
- #ok Detección de SS cotizada: aplicación que empieza por `12`.
- #ok Artefacto adicional `ss_por_expediente.parquet`.

== Costes sociales calculados (clases pasivas)  #cita[§4515-4534]

- #ok Filtro «PDI funcionario sin SS cotizada» cruzando
  `cfg_tuple("categorías_pdi_funcionario") = {CU, TU, TEU, CEU}`
  con personas sin aplicación `12*`
  (#ref("coana/fase1/nóminas/__init__.py:786-801")).
- #ok Fórmula completa: `BASE = min(TOTAL, ss_base_máxima)`,
  `CC = 23,6 % × BASE`, `REDUCCIÓN = 6,5 % × CC`,
  `CC_neto = CC − REDUCCIÓN`, `MEI = 0,67 % × BASE`,
  `FP = 0,70 % × BASE`, tramos de solidaridad 1,1× y 1,5× de la base
  máxima con tipos 0,92 / 1,00 / 1,17 %. Todos los porcentajes leídos
  vía `cfg_float`. Coincide exactamente con la spec
  (#ref("nóminas/__init__.py:732-758")).

== Cuadros Fase 2  #cita[§4966-5014]

- #ok Los cinco cuadros previstos existen y se registran en
  #ref("coana/fase2/__init__.py:14-31"):
  `cuadro_10_1`, `cuadro_10_3`, `cuadro_10_4`, `cuadro_10_5`,
  `cuadro_10_7`.
- #ok Cargan árboles a través de `_cargar_árbol`
  (#ref("coana/fase2/calculo.py:85-94")), dando preferencia al
  enriquecido en `data/fase1/*.tree` y cayendo al de entrada si no
  existe. Coherente con spec §4974-4980.
- #ok Generan los artefactos `.yaml` + `.xlsx` previstos.

== App: catálogo de pantallas  #cita[§1546-1692]

- #ok Bloque «Entradas» (`routers/entradas.py`, `services/entradas.py`).
- #ok Bloque «Presupuesto».
- #ok Bloque «Amortizaciones».
- #ok Bloque «Personal» (`personal.py` + `persona_360.py` con vista 360º).
- #ok Bloque «Regla 23».
- #ok Bloque «Investigación».
- #ok Bloque «Cargos académicos».
- #ok Bloque «Superficies».
- #ok Bloque «Resultados Fase 1» (resumen, UC, árboles, anomalías).
- #ok Bloque «Informes a la carta» (`routers/informes_carta.py` + endpoints xlsx/pdf).
- #ok Bloque «Reducciones sindicales» (PDI + PTGAS, añadido en esta rama).

== Schemas de ficheros de entrada  #cita[§543-1316]

- #ok `apuntes presupuesto de gasto.xlsx`: todas las columnas usadas
  por `_filtrar` y `_enriquecer` existen en el Excel.
- #ok `nóminas y seguridad social.xlsx`: todas las columnas citadas
  en la spec existen.
- #ok `pod.xlsx`: columnas coinciden con §1044-1058.
- #ok `tesis.xlsx`: columnas coinciden con §4747-4779.
- #ok `sexenios.xlsx`: columnas cubren las usadas por `_sexenios_vivos`.

== Tablas de mapeo críticas  #cita[§5050-5184]

- #ok `_PTGAS_CAT_XXX` (#ref("coana/fase1/nóminas/__init__.py:88-91"))
  reproduce §5064-5070.
- #ok `_PDI_CAT_XXX` (#ref("nóminas/__init__.py:95-109")) reproduce
  §5085-5097.
- #ok `_PTGAS_CR_YYY` (#ref("nóminas/__init__.py:114-…")) cubre los CR
  01-90 de §5111-5128.
- #ok `_MAPEO_SECTOR = {PAS → PTGAS, PI → PVI}` y
  `_PRELACIÓN_SECTOR = [PTGAS, PVI, PDI, Otros]`
  (#ref("nóminas/__init__.py:33,38")) coinciden con §5142-5151.

= Derivas detectadas (en orden de impacto)

#table(
    columns: (auto, 1fr, 1.4fr),
    align: (left, left, left),
    stroke: 0.5pt + luma(75%),
    inset: 6pt,
    table.header([*Nº*], [*Hallazgo*], [*Acción sugerida*]),

    [1],
    [#warn *`07G` Tesis Doctorales en clasificador de actividades.*
    #ref("clasificador_actividades.py:755-757") asigna literal
    `dag-doctorado`; spec §2620 lista 07G como
    `doctorado + proyecto` en la regla «Formación permanente».],
    [Decidir cuál es la verdad y reconciliar. Si el comportamiento
    correcto es el del código, eliminar 07G de la tabla §2620 (o
    moverlo a una regla aparte).],

    [2],
    [#warn *`_MÁSTERES_OFICIALES` solo cubre `MO, MO08, MO09, MO10,
    MO12`* (#ref("clasificador_actividades.py:638-645")); spec
    §1796-1806 enumera además `MO06, MO07, MO11, MO14, MO15`.],
    [Comprobar si esos tipos tienen proyectos vivos. Si sí, añadirlos
    al código; si no, retirarlos de la spec.],

    [3],
    [#warn *Comparación `tl != "00"`* en investigación nacional/
    internacional (#ref("clasificador_actividades.py:582,586,612"))
    en lugar de «no empieza por 00» que dice spec §1904.],
    [Cambiar a `~tl.str.starts_with("00")` o aclarar en spec que
    `tl` es siempre cadena de dos caracteres.],

    [4],
    [#warn *`MIG`/`UEGD`* manejados en código
    (#ref("clasificador_actividades.py:515-516,586")) pero ausentes
    de la tabla maestra §1754-1786 de la spec.],
    [Añadirlos a la tabla maestra. Es deriva de la spec, no del código.],

    [5],
    [#warn *Regla «INVES → centro_origen»* (#ref("clasificador_centros_coste.py:462-484"))
    une con todo `_CC_GENÉRICO`; spec §3257 dice estrictamente
    TABLA-TRADUCCIÓN-DEPARTAMENTOS (un subconjunto).],
    [Restringir el lookup a las claves de departamentos, o ampliar
    la spec para incluir facultades/servicios/institutos.],

    [6],
    [#warn *`VRSPII → dag-vrspii`* añadido en
    `_SUBCENTRO_VICERRECT` (clasificador de actividades) sin estar
    en TABLA-TRADUCCIÓN-VICES de la spec.],
    [Añadirlo a la spec.],

    [7],
    [#warn *Literal `2.0`* en
    #ref("coana/fase1/cargos.py:308") en lugar de la constante
    `_PAGAS_EXTRA_CARGO`. Funciona, pero rompe el principio de
    spec §254 («no debe requerir modificaciones en Python»).],
    [Cambiar `2.0` por `_PAGAS_EXTRA_CARGO`.],

    [8],
    [#warn *`CUES` vs `CUEX`*: la spec usa `CUEX` en la tabla de
    actividades §2627 y `CUES` en la sección de docencia no oficial
    §4723. El código maneja ambos en
    #ref("coana/fase1/regla23/cargadores/pod_no_oficial.py:73-76").],
    [Unificar la spec a un único código (probablemente `CUEX`).],
)

= Recomendaciones

+ *Reconciliar el caso 07G* (Tesis Doctorales) antes que nada: es la
    única deriva con impacto funcional inmediato si llegan apuntes
    presupuestarios de proyectos 07G; hoy generarían actividad
    `dag-doctorado` agregada en lugar de `doctorado-<proyecto>`
    disgregada por proyecto. Decidir agrupado vs disgregado y aplicar
    en ambos lados.
+ *Sustituir los literales `2.0`* en cargos por la constante
    `_PAGAS_EXTRA_CARGO` para que un cambio de pagas extra (poco
    probable, pero posible) solo requiera tocar `configuración.xlsx`.
+ *Cambiar `tl != "00"` por `~starts_with("00")`* (o documentar la
    invariante de longitud) en el clasificador de actividades.
+ *Sincronizar las listas* en la spec: añadir `MIG`/`UEGD`/`VRSPII` a
  las tablas maestras de tipos de proyecto y vicerrectorados; eliminar
  `MO06/MO07/MO11/MO14/MO15` si efectivamente no se usan; unificar
  `CUES`/`CUEX`.
+ *Estrechar la regla INVES* a la sub-tabla de departamentos o
    ampliar la spec; cualquiera de las dos opciones es válida pero
    debe quedar reflejada.

= Conclusión

La implementación es fiel a la especificación en sus partes principales
(filtros, clasificadores, fórmulas de SS y costes calculados, cargos,
tesis, cascada de la regla 23, reducciones sindicales y cuadros 10.x).
No se ha detectado ningún bloque sin implementar; las ocho derivas son
menores y, salvo la del tipo 07G, no cambian las cifras actuales.
La fuente principal de divergencia es la propia spec, que en algunos
puntos está más estrecha que el código (listas incompletas) o más
laxa (`tl != "00"` literal). El código respeta el principio de
externalizar constantes a `data/configuración.xlsx` salvo una
ocurrencia del literal `2.0`.
