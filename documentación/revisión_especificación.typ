#import "preámbulo.typ": *
#show: formato

#set document(title: [Revisión crítica de la especificación #coana])

#align(center, [
    #v(2cm)
    #text(size: 22pt, weight: "bold")[Revisión crítica de la especificación]\
    #v(0.4cm)
    #text(size: 14pt)[#coana — pendientes detectados]\
    #v(0.4cm)
    #text(
        style: "italic",
        fill: gray,
    )[Estado tras varias rondas de revisión. Solo se enumeran puntos abiertos.]
])

#v(1.5cm)

#outline()

#pagebreak()

= Criterios

Este documento se elabora cotejando el texto de
#ruta("documentación/especificación.typ") con el estado del código en
#ruta("coana") y de los datos de entrada en #ruta("data", "entrada"). Se
identifican tres tipos de hallazgos: errores (la spec contradice al
código), incoherencias (la spec se contradice consigo misma) y
omisiones (cosas implementadas no documentadas). Los puntos resueltos
en revisiones anteriores se han retirado del documento; los
explícitamente diferidos a fases posteriores (los árboles de
elementos de ingreso, los árboles instrumentales por comportamiento)
también se han sacado de la lista viva. Lo que sigue son los
pendientes que afectan a la fase 1 actual.

Para cada apartado se da una referencia de sección o regla y, cuando
aplica, una sugerencia de redacción.


= Etapas de la fase 1

== Nóminas

- *Generación de UC para PDI/PVI con CC y actividad vacíos*. Las UC
    que produce la fase de elemento de coste antes de la #nombre-regla[Regla 23]
    salen con #campo("centro_de_coste") y #campo("actividad") vacíos.
    La spec lo señala con una nota; convendría dejarlo escrito de
    forma destacada (cabecera de la sección «Tratamiento del PVI y
    del PDI») para que un lector no se sorprenda al ver UC con esos
    campos vacíos durante la traza intermedia.

- *Reglas XXX para PVI/PDI: orden de evaluación*. La tabla con
    #campo("categoría_plaza") #val("41J") y #val("41S") cubre dos
    categorías observadas, pero #campo("provisión") añade #val("P4"),
    #val("PD"), #val("P2") con regla #emph[first-match-wins]. La
    interacción entre los dos bloques (PVI por #campo("categoría_plaza")
    vs PVI por #campo("provisión")) no está totalmente explicada — el
    orden importa.

- *Tabla de despidos*. Se dice _«si el proyecto es 23G019 → CC=vi,
    actividad=otras-ait-financiación-propia; en otro caso, vía
    módulos»_. Está bien pero los #emph[módulos] no se documentan
    aparte y dependen, a su vez, de la regla INVES → centro_origen
    (que sí está aclarada).

== Cargos académicos

- *Departamentos hardcoded*. La constante `_DEPTO_CC` en
    #ruta("coana/fase1/cargos.py") enumera los códigos CC de los 28
    departamentos. Debería derivarse de TABLA-TRADUCCIÓN-DEPARTAMENTOS
    para evitar la duplicación.

- *Categoría última PDI/PVI: empates*. El algoritmo se describe pero
    no se especifica cómo se rompen empates cuando hay varias filas
    con la misma fecha máxima. El código toma la primera tras `sort`,
    sin desempate explícito.

- *Cita de salidas*. La sub-sección «Departamentos» no cita por
    nombre #ruta("auxiliares", "cargos_departamentos.parquet") (sí
    aparece en el apéndice de artefactos, pero conviene una mención
    cruzada en el cuerpo del texto).

== Seguridad social

La spec describe el reparto SS a alto nivel, pero el código tiene
matices que faltan por documentar:

- *Pares con CC o actividad vacíos*: no participan en el reparto.
- *Deduplicación*: hay deduplicación de UC por persona antes de
    repartir; el criterio (claves usadas) tampoco está en la spec.


= Lista priorizada de pendientes

#emph[Bloqueantes para reproducir el sistema sin leer el código:]


#emph[Importantes pero no bloqueantes:]

+ Detallar el reparto SS pendiente: deduplicaciones y pares con
    CC/actividad vacíos (la prelación de sectores y el formato del `id`
    ya están documentados).
+ Aclarar el orden de evaluación entre los bloques XXX de PVI
    (categoría_plaza vs provisión).

#emph[Cosméticos y mantenimiento:]

+ Derivar `_DEPTO_CC` de TABLA-TRADUCCIÓN-DEPARTAMENTOS.
+ Citar en el cuerpo del texto los parquets que ya están en el
    apéndice (especialmente para cargos académicos).
