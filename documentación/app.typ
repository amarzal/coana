#import "preámbulo.typ": *
#show: formato

#set document(title: [Descripción de la #app])

#align(center, text(18pt, weight: "extrabold")[Descripción de la #app])

#v(1cm)

#outline(depth: 3)

= Introducción

La #app de #coana es una aplicación web interactiva construida con Streamlit que permite explorar, auditar y analizar todo el _pipeline_ de contabilidad analítica de la Universitat Jaume I. Se accede ejecutando:

```
uv run coana
```

La aplicación presenta un panel lateral de navegación con secciones colapsables organizadas en ocho bloques: *Entradas*, *Presupuesto*, *Amortizaciones*, *Personal*, *Regla 23*, *Cargos académicos*, *Superficies* y *Resultados Fase 1*. Cada bloque se despliega en un árbol de subsecciones seleccionables.

Además, el panel lateral contiene un botón *Ejecutar Fase 1* que lanza el procesamiento completo, recarga las cachés y actualiza todas las vistas con los nuevos resultados.

Cada sección navegable muestra un botón *?* junto al título. Al pulsarlo se despliega un _popover_ con una descripción contextual de la sección, extraída de este mismo documento.

= Entradas

La sección *Entradas* permite inspeccionar los ficheros de datos de entrada almacenados en #ruta("data", "entrada"). Los ficheros se agrupan por subdirectorio (estructuras, presupuesto, superficies, nóminas, docencia, inventario, consumos, investigación) y cada entrada del menú abre la visualización del fichero correspondiente.

== Ficheros de estructura (`.tree`)

Los ficheros #val(".tree") se muestran como un árbol jerárquico interactivo desplegable, construido con elementos HTML `<details>`. Para cada nodo se muestra:

- *Código*: la posición jerárquica (p.ej. #código[01.02.03]).
- *Descripción*: el nombre del nodo.
- *Identificador*: la etiqueta _slug_ única (p.ej. #etq("dag-docencia", clave-color: "act")).

Se ofrecen métricas (número de nodos, profundidad máxima) y tres filtros de texto: por código, descripción o etiqueta. Si se activa algún filtro, la vista cambia a tabla plana con los nodos coincidentes.

== Ficheros de datos (`.xlsx`)

Los ficheros Excel se presentan como tablas con conteo de filas y columnas. Disponen de un sistema de filtrado avanzado:

- Búsqueda de texto libre (sensible o insensible a mayúsculas).
- Coincidencia por expresión regular.
- Coincidencia por palabra completa.
- Selección de columna específica o búsqueda en todas.

Al seleccionar una fila de la tabla, se despliega una *ficha de registro* con los valores del apunte y enlaces a datos de referencia. Estos enlaces consultan las tablas maestras (aplicaciones de gasto, centros, proyectos, etc.) para enriquecer la información mostrada.

= Presupuesto

El bloque *Presupuesto* muestra los resultados del procesamiento de los apuntes presupuestarios de gasto: las unidades de coste generadas, los apuntes filtrados, los apuntes sin clasificar, las reglas aplicadas y los árboles de estructuras resultantes.

== Resumen

Panel de métricas global con cinco indicadores:

#align(center, table(
    columns: 5,
    align: center,
    stroke: none,
    table.header(
        table.hline(),
        [*UC generadas*], [*Importe UC*], [*Apuntes filtrados*], [*Sin clasificar*], [*Actividades*],
        table.hline(),
    ),
    [Nº de UC], [Importe total], [Apuntes excluidos], [Apuntes sin UC], [Nodos antes → después],
    table.hline(),
))

== Unidades de coste

Muestra las UC generadas a partir del presupuesto, cargadas desde #ruta("data", "fase1", "uc presupuesto.parquet"). La tabla se filtra y se ordena con los controles del patrón general (ver _Filtrado y ordenación de tablas_) y permite seleccionar filas para ver el detalle.

== Sin clasificar

Presenta los apuntes presupuestarios que no han podido clasificarse como unidades de coste, con su conteo e importe total.

== Apuntes filtrados

Tabla de apuntes eliminados en el filtro previo, agrupados por motivo de exclusión. Se muestra un resumen por motivo y la tabla completa con filtrado interactivo.

== Suministros

Visualización de las UC generadas por distribución de suministros (energía, agua, gas), cargadas desde #ruta("data", "fase1", "uc suministros.parquet"). Se presenta un desglose por tipo de suministro (#campo[origen]) y un resumen por centro de coste.

== Distribución mantenimientos OTOP

Distribución de los costes de mantenimiento OTOP entre centros de coste usando la presencia superficial de cada centro en las zonas, edificaciones y complejos del campus.

== Reglas de actividad / centro de coste / elemento de coste

Tres secciones gemelas que muestran la efectividad de las reglas de clasificación. Para cada regla se indica:

- *Nombre* de la regla.
- *n*: número de apuntes clasificados por esa regla.
- *Importe*: importe total de los apuntes asignados.

Las reglas con cero coincidencias se resaltan en rojo. Al seleccionar una regla de actividad, se muestran las UC generadas por esa regla con posibilidad de _drill-down_ hasta el apunte presupuestario original.

== Árboles: Actividades / Centros de coste / Elementos de coste

Tres secciones que comparan el árbol original (datos de entrada) con el árbol modificado tras la aplicación de reglas (datos de fase 1). Los nodos nuevos — creados dinámicamente durante el procesamiento — se resaltan en verde. Se muestran métricas (total de nodos, nodos nuevos, profundidad máxima) y la vista de árbol completa con filtros.

= Amortizaciones

El bloque *Amortizaciones* presenta el procesamiento de los registros de inventario patrimonial.

== Resumen

Panel de métricas global con cuatro indicadores:

#align(center, table(
    columns: 4,
    align: center,
    stroke: none,
    table.header(
        table.hline(),
        [*Registros originales*], [*Enriquecidos*], [*Importe total*], [*Filtrados*],
        table.hline(),
    ),
    [En inventario], [Tras enriquecimiento], [Amortización total], [Excluidos],
    table.hline(),
))

== Inventario con amortización

Tabla del inventario enriquecido con los datos de amortización calculados, incluyendo un resumen de los motivos de exclusión.

== Filtrados por estado / cuenta / fecha

Tres subsecciones que muestran los registros de inventario excluidos, respectivamente, por tener estado de baja, por cuenta contable no válida, o por estar fuera del período de amortización. La vista por cuenta permite seleccionar una cuenta concreta para ver el detalle de sus registros.

== Sin cuenta

Registros de inventario que no tienen cuenta contable asignada.

== Por cuenta

Vista jerárquica: resumen por cuenta → selección de cuenta → registros individuales → selección de registro → UC generadas a partir de ese registro.

== UC generadas

Tabla de todas las UC de origen inventario. Al seleccionar una UC se muestra el registro de inventario del que procede.

== Sin centro

Registros de inventario cuya UC no ha podido asignarse a un centro de coste, cargados desde #ruta("data", "fase1", "auxiliares", "amortizaciones", "sin_uc.parquet").

= Personal

El bloque *Personal* muestra los datos de nóminas y seguridad social clasificados por tipo de personal.

== Resumen

Panel de métricas global: expedientes totales e importe total, con una tabla de desglose por categoría de personal (PDI, PTGAS, PVI, Otros) mostrando el número de expedientes e importe de cada una.

== Expedientes por categoría

Cuatro subsecciones, una por cada tipo de personal:

- *PDI*: Personal Docente e Investigador.
- *PTGAS*: Personal Técnico, de Gestión y de Administración y Servicios.
- *PVI*: Personal Visitante Investigador.
- *Otros*: personal no clasificado en las categorías anteriores.

Para cada categoría se muestra la tabla filtrable de expedientes. Al seleccionar un expediente se desglosa:

- *Costes sociales*: líneas de aplicación presupuestaria que empiezan por #val("12").
- *Retribuciones*: agrupadas por proyecto y concepto retributivo, con subcategorías específicas según el tipo de personal:
  - PDI: docencia, gestión, investigación, incentivos.
  - PTGAS: retribuciones ordinarias y extraordinarias.
  - PVI: fondos UJI frente a financiación afectada.
- *Resumen por concepto retributivo*: tabla agregada con nombre del concepto.

=== Docencia (solo PDI)

Para expedientes PDI, se muestra adicionalmente la carga docente: asignaturas impartidas con su titulación, créditos y resumen por titulación.

== Multiexpediente

Personas que en el año analizado han tenido expedientes en sectores distintos (PTGAS + PDI, PTGAS + PVI, PDI + PVI o las tres combinaciones). Para cada combinación se muestra una pestaña con la lista de personas, la actividad asignada al expediente principal y los importes por sector.

== Persona

Vista por persona del reparto de Seguridad Social y de todas sus UC retributivas. Lee #ruta("data", "fase1", "auxiliares", "nóminas", "persona_ss.parquet") y #ruta("data", "fase1", "auxiliares", "nóminas", "persona_uc.parquet") y permite seleccionar a una persona (con nombre real cuando #ruta("personas.xlsx") está disponible) para ver: importes retributivos totales, costes sociales repartidos, y el detalle de cada UC asociada (de nómina o de presupuesto).

== Anomalías PDI

Identifica asignaturas cuya titulación no se encuentra en las tablas de referencia (grados, másteres, estudios). Muestra el número de asignaturas afectadas, el profesorado implicado y el porcentaje de créditos sin titulación conocida.

= Regla 23

El bloque *Regla 23* expone los pasos intermedios y los resultados parciales del reparto de la masa retributiva indiferenciada del PDI/PVI. Cada subsección lee un parquet generado en #ruta("data", "fase1", "auxiliares", "nóminas").

== Dedicación docente

Por expediente, dedicación en créditos a las distintas asignaturas del #ruta("data", "entrada", "docencia", "pod.xlsx"). Origen: #ruta("regla_23_dedicación_docente.parquet"). Se completa con dos pestañas adicionales construidas al vuelo (dedicación por titulación y dedicación por estudio) que provienen de #ruta("regla_23_dedicación_titulaciones.parquet") y #ruta("regla_23_dedicación_estudios.parquet").

== Docencia no oficial

Horas dedicadas a estudios propios, microcredenciales, doctorado y otras actividades docentes no asociadas a titulaciones oficiales. Origen: #ruta("regla_23_horas_no_oficiales.parquet").

== Estructura estudios

Catálogo de estudios y titulaciones del año, con créditos impartidos por titulación. Permite ver qué titulaciones están activas (con créditos > 0) y cuáles aparecen sin actividad este curso. Origen: #ruta("regla_23_estructura_estudios.parquet").

== Bolsa de atrasos

Líneas de PDI/PVI con #campo("concepto_retributivo") = #val("30") o #val("87") (atrasos), apartadas del reparto de la regla 23 del año. Origen: #ruta("regla_23_atrasos.parquet"). Se muestra el total de la bolsa y el desglose por persona y concepto.

== Despidos

UC de PDI/PVI por #campo("concepto_retributivo") de despido. Origen: #ruta("uc_despidos.parquet").

== Indemnizaciones asistencias

UC de PDI/PVI por #campo("concepto_retributivo") de indemnización por asistencias a tribunales y similares. Origen: #ruta("uc_indemnizaciones_asistencias.parquet").

== Cargos

UC de PDI/PVI por #campo("concepto_retributivo") = #val("19") o #val("64") con proyecto identificado (cargos asociados a un proyecto específico, fuera de la TABLA-PROYECTOS-GENERALES). Origen: #ruta("uc_cargos.parquet").

== Expedientes apartados

Expedientes que tras separar la bolsa de atrasos quedan sin ingresos reales en el año (solo atrasos o sin masa retributiva ordinaria). Origen: #ruta("regla_23_expedientes_apartados.parquet").

== Asignaturas sin titulación

Asignaturas con créditos impartidos cuya titulación no aparece en ninguno de los catálogos de referencia. Origen: #ruta("regla_23_asignaturas_sin_titulación.parquet"). Es un subconjunto del cruce entre pod y los catálogos de docencia.

== Anomalías

Dos pestañas:

- *Pod sin titulación efectiva*: filas de pod cuya titulación no se puede resolver tras aplicar las reglas de desambiguación (incluido el pod de másteres). Origen: #ruta("regla_23_anomalías_resolución.parquet").
- *Múltiples con grado*: asignaturas con varias titulaciones donde alguna no es máster, lo que incumple la regla del catálogo de pod de másteres. Origen: #ruta("regla_23_múltiples_con_grado.parquet").

= Cargos académicos

El bloque *Cargos académicos* presenta los cálculos previos y los resultados del tratamiento de cargos asociados a actividades. Cubre, de momento, la categoría última de cada PDI/PVI y los cargos por departamento.

== Categoría PDI/PVI

Por #campo("per_id"), categoría de PDI/PVI tras el último cobro por #campo("concepto_retributivo") = #val("19") o #val("64"). Origen: #ruta("data", "fase1", "auxiliares", "categoría_última_pdi_pvi.parquet"). Para cada persona se muestra la fecha y el importe del último cobro 19/64 junto con los campos contextuales (proyecto, centro, aplicación, programa).

== Departamentos

Cargos académicos asociados a cada departamento, filtrados por #campo("cuantía") > 0 y al menos un día activo en el año. Origen: #ruta("data", "fase1", "auxiliares", "cargos_departamentos.parquet"). Selección de departamento → tabla de cargos con persona, período y categoría de la persona en su última nómina.

= Superficies

El bloque *Superficies* permite analizar la distribución del espacio físico del campus y la presencia de los centros de coste en él.

== Resumen

Panel de métricas global con cinco indicadores: superficie total en m², número de complejos, edificaciones, zonas y ubicaciones del campus.

== Totales

Verificación de coherencia de las superficies a cuatro niveles jerárquicos:

+ *Zonas*: la unidad espacial mínima (área + edificio).
+ *Edificaciones*: agrupación de zonas (área + edificación).
+ *Complejos*: agrupación de edificaciones (área).
+ *Campus* (UJI): totalidad.

Se muestra un sistema de selectores en cascada (complejo → edificación → zona) con métricas de m² en cada nivel y tablas de desglose. Se verifica que los totales de cada nivel coinciden con la agregación del nivel inferior.

== Presencia centros

Análisis de la presencia de cada centro de coste en el espacio físico, con dos modos de visualización:

- *Por centro*: seleccionar un centro y ver su distribución en complejos, edificaciones y zonas (los tres principales de cada nivel), junto con los servicios asignados.
- *Por nivel*: seleccionar un nivel de agregación y un elemento concreto (complejo, edificación o zona) para ver qué centros tienen presencia en él.

El cálculo de presencia se realiza en tres pasos:
+ Asignación directa de servicios a ubicaciones (dividiendo metros cuadrados entre el número de servicios por ubicación).
+ Redistribución intrazona: los espacios sin servicio asignado se reparten entre los centros presentes en la zona.
+ Redistribución global: las zonas sin ningún centro asignado se reparten proporcionalmente según la presencia global de cada centro.

= Resultados Fase 1

El bloque *Resultados* presenta la visión consolidada de todas las unidades de coste generadas por la Fase 1, combinando las cuatro fuentes: presupuesto, amortizaciones, nóminas y suministros.

== Resumen

Panel de métricas global: total de unidades de coste e importe total, con una tabla de desglose por origen (presupuesto, amortizaciones, nóminas, suministros) mostrando el número de UC e importe de cada fuente.

== Todas las UC

Tabla con todas las unidades de coste. La tabla dispone de filtrado por texto, selección de columna, ordenación con el desplegable «Ordenar por» y selección de fila para consultar el detalle de cada UC.

== Actividades / Centros de coste / Elementos de coste

Tres secciones con la misma estructura. Para cada nodo del árbol correspondiente (en orden DFS), se muestra una fila con:

#align(center, table(
    columns: 7,
    align: (right, left, left, right, right, right, right),
    stroke: none,
    table.header(
        table.hline(),
        [*Nº*], [*Código*], [*Nombre*], [*pto €*], [*amort €*], [*nóm €*], [*sumin €*],
        table.hline(),
    ),
    [1], [01], [Docencia], [1.234,56], [567,89], [2.345,67], [123,45],
    [⋮], [⋮], [⋮], [⋮], [⋮], [⋮], [⋮],
    table.hline(),
))

Cada columna de importe corresponde a un origen de UC. Se calcula también la columna *total*. Al seleccionar un nodo se despliegan las UC individuales que lo componen, con posibilidad de _drill-down_ al detalle de cada una.

== Anomalías UC

Comprobación de integridad referencial: detecta UC que referencian nodos inexistentes en los árboles finales (elemento de coste, centro de coste o actividad). Se presenta un resumen por fuente y tipo de anomalía, con conteo e importe afectado, y la tabla de detalle con filtrado.

= Funcionalidades transversales

== Ayuda contextual

Cada sección navegable muestra un botón *?* junto al título. Al pulsarlo se despliega un _popover_ con una descripción breve de la sección: qué datos presenta, qué métricas incluye y cómo interactuar con ellos. Los textos de ayuda se mantienen en un diccionario interno de la aplicación.

== Filtrado y ordenación de tablas

Todas las tablas de la aplicación disponen de una barra de controles con:

- *Filtrar*: campo de texto para búsqueda por _substring_, insensible a mayúsculas/minúsculas y a tildes.
- *Columna*: selector para limitar la búsqueda a una columna concreta o a todas (por defecto, todas).
- *Ordenar por*: selector de columna para ordenación _server-side_ (en polars).
- *Desc*: conmutador para alternar entre orden ascendente y descendente.

La ordenación se ejecuta en el servidor (no en el navegador), lo que permite trabajar fluidamente con tablas de cientos de miles de filas.

La ordenación al pinchar la cabecera de la tabla está deshabilitada por una limitación conocida de Streamlit 1.54: al combinar `dataframe` con selección de filas, el reordenamiento visual de la tabla no se refleja en los índices que devuelve la selección, lo que provoca que la ficha mostrada no se corresponda con la fila pinchada. La única vía de ordenación es, por tanto, el desplegable «Ordenar por».

== Formato de importes

Las columnas de importe (#campo[importe], #campo[coste], #campo[valor_inicial], etc.) se formatean automáticamente en notación europea (#val("1.234,56")) y se alinean a la derecha mediante _figure spaces_ (U+2007). Bajo cada tabla se muestra una línea de totales.

== Ficha de registro

Al seleccionar una fila en cualquier tabla, se despliega un panel de detalle (_ficha_) que muestra todos los campos del registro. Los campos que son claves foráneas se enriquecen con datos de las tablas de referencia: por ejemplo, un código de aplicación presupuestaria se amplía con su nombre y la jerarquía capítulo → artículo → concepto → aplicación.

== Ejecución de la Fase 1

El botón *Ejecutar Fase 1* del panel lateral:

+ Recarga todos los módulos de `coana.fase1` (para incorporar cambios de código sin reiniciar la app).
+ Ejecuta `coana.fase1.ejecutar()`.
+ Muestra un _log_ en vivo dentro de un contenedor de estado: cada línea de salida aparece inmediatamente y la etiqueta del contenedor refleja el paso en curso (p.ej. «Traduciendo apuntes presupuestarios…»).
+ Limpia todas las cachés de datos (`st.cache_data`) para que las vistas reflejen los nuevos resultados.
+ Muestra una notificación de éxito al completar.

= Otras aplicaciones

Además de la #app principal, el sistema ofrece:

== Editor de árboles

Aplicación gráfica en tkinter para editar ficheros #val(".tree"). Se lanza con:

```
uv run editor_de_arboles
```

o bien:

```
uv run coana editor-tree
```

Permite crear, modificar y reorganizar nodos de los árboles de estructuras de forma visual.
