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

La aplicación presenta un panel lateral de navegación con secciones colapsables organizadas en seis grandes bloques: *Entradas*, *Presupuesto*, *Amortizaciones*, *Personal*, *Superficies* y *Resultados*. Cada bloque se despliega en un árbol de subsecciones seleccionables.

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

Muestra las UC generadas a partir del presupuesto, cargadas desde #ruta("data", "fase1", "uc presupuesto.parquet"). La tabla se puede filtrar por texto y por columna, ordenar por cualquier columna (ascendente o descendente) y permite seleccionar filas para ver el detalle.

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

== Anomalías PDI

Identifica asignaturas cuya titulación no se encuentra en las tablas de referencia (grados, másteres, estudios). Muestra el número de asignaturas afectadas, el profesorado implicado y el porcentaje de créditos sin titulación conocida.

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

Tabla con todas las unidades de coste. La tabla dispone de filtrado por texto, selección de columna, ordenación por cualquier columna y selección de fila para consultar el detalle de cada UC.

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

- *Filtrar*: campo de texto para búsqueda (subcadena, insensible a mayúsculas).
- *Columna*: selector para limitar la búsqueda a una columna concreta o a todas.
- *Ordenar por*: selector de columna para ordenación _server-side_ (en polars).
- *Desc*: conmutador para alternar entre orden ascendente y descendente.

La ordenación se ejecuta en el servidor (no en el navegador), lo que permite trabajar fluidamente con tablas de cientos de miles de filas.

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
