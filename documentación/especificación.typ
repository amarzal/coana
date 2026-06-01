#import "preámbulo.typ": *
#import "img/fase1.typ": (
    etapa-amortizaciones, etapa-cargos, etapa-nominas, etapa-presupuesto, etapa-regla23, etapa-ss, etapa-suministros,
    fase1-diagrama,
)
#show: formato

#set document(title: [Especificación #coana])
#set table(fill: cebra, stroke: none)

#align(center, title())

#v(1cm)

#outline()



= Especificación del sistema de contabilidad analítica

Este documento es la especificación del sistema de contabilidad analítica #coana para la Universitat Jaume I de Castelló. La especificación se puede ir modificando a lo largo del tiempo, conforme se construye el sistema, así que es importante que cada vez que edite este documento de especificación, examines el contenido para ajustar el código Python a los cambios que detectes.

El sistema es una aplicación Streamlit interactiva que permite visualizar los datos de entrada, los datos intermedios y los datos finales. Además, puede crear diferentes informes en PDF (usando Typst como lenguaje de marcado) para disponer de trazas concretas o del informe o informes finales. Nos referimos a la aplicación interactiva como la #app.

El fichero #ruta("documentación", "especificación.typ"), que es el que estás leyendo, es el documento de especificación general del sistema. En él se describe la arquitectura general del sistema, los datos de entrada, los datos intermedios y los datos de salida, así como las reglas que se aplican para transformar unos en otros. Además, se describen los informes que se generan a partir de los datos intermedios y finales.

Este es un _work in progress_, así que es posible que los documentos cambien sobre la marcha.

= Arquitectura

== Glosario

A lo largo del documento usamos un vocabulario específico que es importante fijar para evitar ambigüedades:

/ Unidad de coste (UC): : Registro atómico del sistema. Cada UC tiene un #campo("id"), un #campo("elemento_de_coste"), un #campo("centro_de_coste"), una #campo("actividad"), un #campo("importe") en euros, y los campos de trazabilidad #campo("origen"), #campo("origen_id"), #campo("origen_porción"). La fase 1 produce decenas de miles de UC desde fuentes diversas (presupuesto, amortizaciones, suministros, nóminas, regla 23, seguridad social…); la fase 2 las consolida en informes.

/ Expediente: : Identificador interno (entero) de una relación laboral concreta con la UJI. Cada persona (#campo("per_id")) puede tener varios expedientes a lo largo del tiempo y simultáneamente (mono o multiexpediente). Es la clave por la que se relacionan nóminas y RR.HH.

/ Sector: : Clasificación funcional del personal: #val("PDI") (Personal Docente e Investigador), #val("PTGAS") (Personal Técnico, de Gestión y Administración), #val("PVI") (Personal Vinculado a Investigación) u #val("Otros"). Cada expediente tiene un sector. Una persona con varios expedientes puede tener varios sectores; el algoritmo de seguridad social usa una *prelación* (PTGAS > PVI > PDI > Otros) para elegir el sector principal de la persona.

/ Proyecto general: : Proyecto presupuestario que no se imputa a una actividad concreta sino que sostiene el funcionamiento general de la universidad. Hay *dos tablas* de proyectos generales (centralizadas en #ruta("data", "configuración.xlsx")):

    - *TABLA-PROYECTOS-GENERALES-NÓMINA* (constante #campo("proyectos_generales_nómina")): #val("00000"), #val("02G041"), #val("11G006"), #val("1G019"), #val("1G046"), #val("23G019"). Las retribuciones ordinarias de PDI/PVI imputadas a estos proyectos entran en la masa regla 23.
    - *TABLA-PROYECTOS-GENERALES* (constante #campo("proyectos_generales_cargos")): la anterior más #val("07G011"), #val("11G003"), #val("1I235"), #val("22G010") (cuatro proyectos adicionales que financian cargos académicos). Los CR 19/64 en estos proyectos NO generan UC de cargo línea a línea sino que se reparten entre los cargos vigentes de la persona.

/ Regla 23: : Regla del Modelo de Contabilidad Analítica para Universidades (cuadro 9.7) que reparte la masa retributiva ordinaria del PDI/PVI en proyecto general entre las actividades en que cada persona participa, proporcionalmente a sus *horas* de dedicación. El reparto se hace en horas, no en euros: primero se determinan las horas y luego se traducen a coste mediante el cociente $#campo("horas_finales") / #campo("jornada_anual_pdi")$.

/ Masa regla 23: : Subconjunto de las nóminas PDI/PVI que satisface a la vez: #campo("aplicación") NO empieza por #val("12") (no es seguridad social), #campo("proyecto") está en TABLA-PROYECTOS-GENERALES-NÓMINA y #campo("concepto_retributivo") NO es #val("19"), #val("64"), #val("47") ni #val("48"). Es lo que se reparte vía regla 23.

/ Factor (×2,5): : Coeficiente por el que se multiplican las horas de impartición efectiva para obtener *horas efectivas* docentes (incluye preparación, evaluación, tutorías, dirección académica). Solo se aplica a la docencia. Se define en #ruta("data", "configuración.xlsx") como #campo("factor_impartición_docente").

/ Horas efectivas, horas iniciales, horas finales: : *Horas efectivas* o *iniciales*: producto #campo("horas") × #campo("factor") en la tabla #campo("dedicación_pdi"). *Horas finales*: las que devuelve la fase de reparto (5-7 de la regla 23) tras normalizar a la jornada anual; aparecen en #campo("dedicación_pdi_normalizada") y son las que se usan para repartir el coste.

/ Sobrante (horas no distribuidas): : Holgura $S = T - H_"D" - H_G$ que queda tras situar la docencia y la gestión —ambas rígidas— en la cascada de reparto de la regla 23. Se imputa íntegro a investigación (todo PDI investiga por defecto).

/ Sexenio vivo: : Sexenio de investigación cuya #campo("fecha_fin_sexenio") está a menos de #campo("sexenio_vivo_años") (6) años del fin del año analizado. Indica que la persona tiene actividad investigadora reciente acreditada. Es un dato informativo: en el reparto de la regla 23, como docencia y gestión son rígidas, el sobrante siempre va a investigación, así que no entra en el cálculo.

/ Figura puramente docente: : PDI cuyo rol contractual es exclusivamente la docencia, sin dedicación estructural a investigación ni a gestión. Incluye dos colectivos:

  - *Professors associats* (PAA, PAL y variantes): contratos a tiempo parcial con dedicación docente acotada.
  - *Professors substituts* (PS): contratos temporales para cubrir bajas docentes.

  Se identifican porque la categoría de plaza vigente en el año está en #campo("categorías_docencia_pura_plaza") (#val("07"), #val("08"), #val("18"), #val("21"), #val("22"), #val("23"), #val("24"), #val("31"), #val("32"), #val("36"), #val("44"), #val("46")). Reciben tratamiento especial en la regla 23: toda su jornada disponible (la jornada anual #val("1 642 h") prorrateada por la fracción del año trabajada) se imputa a docencia, sin gestión ni investigación. La columna #campo("es_asociado") de #ruta("regla23", "dedicación_pdi_normalizada.parquet") marca a estas personas (el nombre se conserva por compatibilidad; semánticamente significa «figura puramente docente»).

/ Retribuciones «extra» o «extras»: : Líneas de nómina en proyecto NO general (es decir, fuera de TABLA-PROYECTOS-GENERALES-NÓMINA), o líneas con conceptos retributivos especiales (CR 19/64 que no quepan en el reparto de cargos, CR 47 de despidos en proyecto específico, etc.). Generan UC línea a línea (clasificadas por los módulos de actividad y centro), en contraposición a la masa regla 23 que se reparte por horas.

/ #campo(
        "origen_porción",
    ): : Campo de las UC que cuantifica qué parte del registro originario corresponde a esa UC (por ejemplo, si una amortización se reparte entre tres centros con pesos 0,5 / 0,3 / 0,2, cada UC lleva esa fracción como #campo("origen_porción")). Permite trazar de la UC al apunte original y reconstruir importes proporcionales.

/ #app: : Aplicación web de inspección/depuración (FastAPI + React). Es solo herramienta de análisis: no afecta al cálculo, lee los parquets producidos por la fase 1.

== Stack tecnológico y arranque

La aplicación se desarrolla en Python 3.14 (gestionado con #raw("uv")). La librería de procesamiento de datos es #raw("polars") (nunca pandas). Los datos se modelan con #raw("pydantic v2"). La aplicación web es FastAPI (backend) + React/Vite + Tailwind (frontend, compilado y servido estáticamente desde el backend). La documentación se compila con Typst desde #ruta("documentación", "especificación.typ"). Hay un editor gráfico (tkinter) para los ficheros #raw(".tree") de los árboles de la contabilidad analítica.

=== Dependencias principales

#table(
    columns: (auto, 1fr),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*Componente*], [*Para qué*], table.hline()),
    [Python 3.14], [Lenguaje del backend y de las fases.],
    [#raw("uv")], [Gestor de entornos y dependencias.],
    [#raw("polars")], [DataFrames; lectura/escritura de Parquet.],
    [#raw("fastexcel"), #raw("openpyxl"), #raw("xlsxwriter")], [Lectura y escritura de Excel.],
    [#raw("pydantic v2")], [Modelos (UC, esquemas de respuesta API).],
    [#raw("typer")], [CLI principal (#raw("coana ...")).],
    [#raw("fastapi"), #raw("uvicorn[standard]")], [Backend web y servidor ASGI.],
    [#raw("streamlit"), #raw("streamlit-antd-components")], [Visor heredado (visor_legacy, en transición).],
    [#raw("tkinter")], [Editor gráfico de árboles (parte de la biblioteca estándar).],
    [#raw("typst")], [Compilador de la especificación a PDF.],
    [Node.js, npm, Vite], [Bundling del frontend React (#ruta("web", "frontend")).],
    table.hline(),
)

=== Layout del proyecto

#raw(
    "
coana/                          # paquete Python principal
  cli.py                        # CLI Typer: coana visor / editor-tree / web / version
  apps/
    editor_tree.py              # editor gráfico (tkinter) de ficheros .tree
    gen_especificacion.py       # compila documentación/especificación.typ
    visor_entradas.py           # visor heredado en Streamlit
  fase1/                        # generación de UC
    __init__.py                 # orquestador `ejecutar()` con las 8 etapas
    presupuesto/                # ContextoPresupuesto + TraductorPresupuesto
    inventario/                 # ContextoInventario + procesamiento
    nóminas/                    # preprocesar_nóminas, _generar_reparto_ss_persona
    regla23/                    # dedicación PDI, fases 5-7 y UC de reparto
      cargadores/               # pod, tesis, cargos, proyectos, grupos
      reparto.py                # fases 5-7 de la regla 23
      uc_reparto.py             # reparto de la masa a UC por persona
    amortizaciones.py           # UC de amortización a partir de inventario
    suministros.py              # energía / agua / gas por presencia
    cargos.py                   # reparto CR 19/64 en proyecto general
    investigación.py            # CC para grupos de investigación
    clasificador_actividades.py # módulo de reglas para asignar actividad
    clasificador_centros_coste.py # idem para centros de coste
  util/
    arbol.py                    # NodoÁrbol + Árbol (formato .tree)
    excel_cache.py              # read_excel con caché Parquet transparente
    euro.py                     # tipo Euro (céntimos como int)
    configuración.py            # acceso tipado a data/configuración.xlsx
    unidad_de_coste.py          # modelo UC + OrigenUC (enum)
  web/                          # backend FastAPI
    app.py                      # registra routers, sirve dist estático
    routers/                    # un fichero por bloque (presupuesto, regla23…)
    services/                   # lógica de consulta sobre parquets
    schemas/                    # respuestas tipadas (ListResponse, KpiPanel…)
    dist/                       # frontend compilado (Vite)

web/frontend/                   # código fuente React/Vite (npm)
  src/
  package.json

data/
  configuración.xlsx            # constantes anuales y de política
  entrada/                      # datos crudos (Excel y .tree)
    consumos/, docencia/, estructuras/, inventario/,
    investigación/, nóminas/, presupuesto/, superficies/
  fase1/                        # salidas
    uc presupuesto.parquet, uc amortizaciones.parquet,
    uc suministros.parquet, unidades de coste.xlsx,
    actividades.tree, centros de coste.tree,
    elementos de coste.tree
    auxiliares/                 # parquets intermedios para depurar/auditar
    regla23/                    # dedicación_pdi, normalizada, uc_reparto_regla_23
documentación/
  especificación.typ            # fuente única de la spec
  especificación.pdf            # generado por `uv run especificación`
",
)

=== Comandos de bootstrap

#raw(
    "# instalación del paquete y dependencias (Python)
uv sync                                # crea el venv y resuelve dependencias
uv pip install -e .                    # opcional: instalación editable

# compilación del frontend (una sola vez tras clonar)
cd web/frontend && npm install && npm run build

# verificación
uv run coana version                   # versión del paquete
uv run especificación                  # compila el PDF
",
)

=== Entry points

#table(
    columns: (auto, 1fr),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*Comando*], [*Qué hace*], table.hline()),
    [#raw("uv run coana")],
    [Lanza el visor web (FastAPI + frontend compilado) en #raw("http://127.0.0.1:8765"). Atajo de #raw("coana web").],
    [#raw("uv run coana web --port 8000")], [Variante con flags (#raw("--reload"), #raw("--dev"), puerto, etc.).],
    [#raw("uv run coana editor-tree")],
    [Editor gráfico tkinter para los #raw(".tree") (actividades, centros de coste, elementos de coste, ingresos). Permite mover, renombrar, ver/editar identificadores y reasignar códigos automáticamente.],
    [#raw("uv run coana visor-legacy")],
    [Visor antiguo en Streamlit. En transición; se mantiene mientras se cierran las vistas del visor nuevo.],
    [#raw("uv run especificación")],
    [Compila #ruta("documentación", "especificación.typ") a PDF. Equivale a #raw("typst compile documentación/especificación.typ").],
    [#raw("uv run editor_de_arboles")],
    [Lanza directamente #ruta("coana", "apps", "editor_tree.py") (alias del comando anterior).],
    table.hline(),
)

=== Ejecución de la fase 1

La fase 1 se dispara desde la #app: en el menú lateral, *Resultados Fase 1 · Ejecutar fase 1* abre una página con un botón que ejecuta #campo("coana.fase1.ejecutar()"). Alternativamente, desde Python:

#raw(
    "uv run python -c \"from pathlib import Path; from coana.fase1 import ejecutar; ejecutar(Path('data'), año=2025)\"",
)

El año analizado se lee también de #ruta("data", "configuración.xlsx") (clave #campo("año_analizado")); el argumento de #campo("ejecutar") lo sobrescribe.

== Invariantes del sistema

Esta lista enumera las propiedades que el sistema debe cumplir en todas sus ejecuciones. Un implementador puede usarla como suite de aceptación de su reconstrucción.

=== Cuadre por persona (Δ)

Para toda persona del sector PDI o PVI debe cumplirse, con tolerancia #val("0,01") €:

$ "Bruto cobrado" + "SS cotizada" + "SS calculada" = sum "UC retributivas" + sum "UC SS" $

Donde:

- *Bruto cobrado*: suma de #campo("importe") de las líneas de nómina del año en cualquier expediente de la persona con #campo("aplicación") NO empezando por #val("12") (es decir, todo lo retributivo: incluye despidos, indemnizaciones, paga extra, etc.).
- *SS cotizada*: suma de #campo("importe") de líneas con #campo("aplicación") empezando por #val("12").
- *SS calculada*: importe de #ruta("auxiliares", "nóminas", "costes_sociales_calculados.parquet") para la persona (solo PDI funcionario en clases pasivas).
- *Σ UC retributivas*: suma de #campo("importe") de las UC de la persona en #ruta("uc_ptgas"), #ruta("uc_pdi"), #ruta("uc_pvi"), #ruta("uc_despidos"), #ruta("uc_indemnizaciones_asistencias"), #ruta("uc_cargos") (proyecto específico), #ruta("cargos_uc") (reparto general) y #ruta("regla23", "uc_reparto_regla_23.parquet").
- *Σ UC SS*: suma de #campo("ss_proporcional") de #ruta("auxiliares", "nóminas", "persona_ss.parquet") para la persona.

La pantalla *Personal · PDI/PVI* expone este cuadre en su columna #campo("delta") del master y desglosado por concepto en la pestaña *Resumen / Cuadre* del detalle. Cualquier persona con $|Δ| ≥ 0{,}01$ € es una anomalía a depurar (típicamente apunta a un servicio sin mapeo en #ruta("data", "entrada", "inventario", "servicios.xlsx"), a una extra-paga del cargo que no se ha podido descontar del CR 68 por falta de masa disponible, o a discrepancias de redondeo en cálculos de SS).

=== Conservación de masas

+ La suma de los importes de las UC presupuestarias (#ruta("uc presupuesto.parquet")) más los importes de los apuntes filtrados (#ruta("auxiliares", "filtrados_presupuesto.parquet")) más los apuntes sin clasificar (#ruta("presupuesto sin uc.parquet")) coincide con la suma de los importes de los apuntes de entrada (#ruta("data", "entrada", "presupuesto", "apuntes presupuesto de gasto.xlsx")).

+ La suma de los importes amortizados (#ruta("uc amortizaciones.parquet")) más los descartados por estado/cuenta/fecha (los parquets de #ruta("auxiliares", "amortizaciones")) coincide con la base de cálculo derivada de #ruta("data", "entrada", "inventario", "inventario.xlsx") aplicando los años de amortización.

+ La suma de los importes de #ruta("auxiliares", "nóminas", "uc_ptgas.parquet") + #ruta("uc_pvi.parquet") + #ruta("uc_pdi.parquet") + #ruta("uc_despidos.parquet") + #ruta("uc_indemnizaciones_asistencias.parquet") + #ruta("uc_cargos.parquet") + #ruta("fase1", "regla23", "uc_reparto_regla_23.parquet") coincide con la masa retributiva total no de seguridad social (#campo("aplicación") no empieza por #val("12"), categoría no en CR 48 si proyecto no es general, etc.) de PDI + PVI + PTGAS para el año analizado, salvo el residual de personas con masa pero sin reparto (los avisos del log).

+ La suma de los importes de las UC de seguridad social (filas de #ruta("auxiliares", "nóminas", "persona_ss.parquet")) coincide con la SS cotizada del año (#campo("aplicación") empezando por #val("12")) más los costes sociales calculados de los PDI funcionarios en clases pasivas.

=== Reparto de la regla 23

+ Para todo #campo("per_id") presente en #ruta("regla23", "dedicación_pdi_normalizada.parquet"), la suma de #campo("horas_finales") es su *jornada disponible* #campo("jornada_anual_pdi") $times$ #campo("fracción_año") (la jornada anual #val("1 642") prorrateada por los meses trabajados): la cascada reparte $T$ exactamente entre docencia, gestión e investigación, y la fracción de reducción sindical completa esa jornada. Para quien trabaja el año completo es #val("1 642"). Única excepción: personas cuyas filas iniciales tienen todas #val("0") horas efectivas, que no admiten repercusión y quedan a #val("0").

+ El sobrante de la cascada va siempre al grupo #val("investigación"); ninguna fila de #val("docencia_oficial"), #val("docencia_no_oficial") o #val("gestión") tiene #campo("horas_finales") superior a su #campo("horas_iniciales") (docencia y gestión son rígidas).

+ Toda persona con #campo("es_asociado") = #val("true") tiene #campo("horas_finales") > 0 únicamente en filas del grupo #val("docencia_oficial") o #val("docencia_no_oficial"); las de #val("gestión") e #val("investigación") quedan a #val("0").

+ Para cada (#campo("per_id"), #campo("actividad"), #campo("centro_de_coste")) la suma de #campo("origen_porción") de las UC de #ruta("regla23", "uc_reparto_regla_23.parquet") iguala (con redondeo) la proporción de #campo("horas_finales") en ese par sobre el total de la persona.

+ Para cada #campo("per_id") en #ruta("regla23", "uc_reparto_regla_23.parquet"), la suma de #campo("importe") iguala la masa regla 23 de esa persona (las nóminas que cumplen el filtro de §«Reparto de la masa regla 23 → unidades de coste»). Al no haber redondeos intermedios, la igualdad es exacta salvo el error de coma flotante (despreciable).

=== Integridad referencial

+ Toda etiqueta de #campo("elemento_de_coste") que aparezca en cualquier UC debe existir como identificador del árbol final de elementos de coste (#ruta("elementos de coste.tree") tras el proceso). Lo mismo para #campo("centro_de_coste") y #campo("actividad") respecto de sus árboles. Las excepciones (#val("pendiente")) están explícitas y se reportan en la vista de Anomalías.

+ Toda referencia a #campo("expediente") en una UC debe existir en #ruta("data", "entrada", "nóminas", "expedientes recursos humanos.xlsx").

+ Toda referencia a #campo("per_id") en una UC debe existir en #ruta("data", "entrada", "nóminas", "personas.xlsx").

+ Para cada par (#campo("actividad"), #campo("centro_de_coste")) con UC asignadas, ambos identificadores deben pertenecer al árbol final correspondiente. La #app, en la pantalla «Resultados Fase 1 · Anomalías UC», muestra cualquier violación.

=== Constantes año a año

+ Cambiar #ruta("data", "configuración.xlsx") y volver a ejecutar la fase 1 debe producir resultados coherentes sin tocar código: cambiar #campo("ss_base_máxima"), #campo("año_analizado"), #campo("jornada_anual_pdi"), etc. no debe requerir modificaciones en Python.

=== Verificación rápida (cifras de referencia 2025)

A modo de checkpoint, una ejecución completa sobre los datos de 2025 debe producir aproximadamente:

#table(
    columns: (auto, auto, auto),
    align: (left, right, right),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*Fuente*], [*UC*], [*Importe (€)*], table.hline()),
    [presupuesto], val("68 169"), val("29 637 337,19"),
    [amortizaciones], val("85 401"), val("6 347 526,73"),
    [suministros], val("936"), val("2 622 111,61"),
    [nóminas-PTGAS], val("7 156"), val("26 257 593,97"),
    [nóminas-PVI], val("1 340"), val("9 899 442,92"),
    [nóminas-PDI], val("1 161"), val("2 199 432,47"),
    [despidos], val("104"), val("195 588,03"),
    [indemnizaciones], val("361"), val("295 959,24"),
    [cargos (proy. específico, #ruta("uc_cargos.parquet"))], val("46"), val("165 020,83"),
    [cargos (proy. general, reparto en #ruta("cargos_uc.parquet"))], val("723"), val("1 786 371,42"),
    [regla-23], val("54 821"), val("50 989 760,52"),
    [seguridad-social], val("11 204"), val("21 529 118,93"),
    table.hline(),
    [*Total*], val("≈ 230 700"), val("≈ 150 138 892,43"),
    table.hline(),
)

Anomalías esperadas en 2025: #val("190") personas con masa regla 23 sin dedicación calculada (≈ #val("347 894 €") repartidos a #etqact("pendiente") / #etqcen("pendiente")) y #val("2") asociados sin docencia.

=== Procedimiento de verificación de la reconstrucción

Quien quiera reconstruir el sistema desde cero puede seguir esta secuencia para validar que sus cifras coinciden con las de referencia:

+ *Instalar* la pila (Python 3.14, uv, dependencias del pyproject, npm, Vite). Compilar el frontend.
+ *Copiar* la carpeta #ruta("data", "entrada") tal cual del repositorio canónico.
+ *Ejecutar* la fase 1: #raw("uv run python -c \"from pathlib import Path; from coana.fase1 import ejecutar; ejecutar(Path('data'), año=2025)\"")
+ *Comparar* la salida combinada (`Total UC` y el desglose por fuente que imprime el log) con la tabla del apartado anterior. Las cifras deben coincidir hasta el céntimo (al trabajar internamente con precisión completa y redondear solo al presentar, ya no hay variaciones por redondeos compuestos).
+ *Inspeccionar* la pantalla «Resultados Fase 1 · Anomalías UC» y verificar que el número de anomalías por integridad referencial coincide con el esperado.
+ *Repetir* la ejecución cambiando #campo("año_analizado") en #ruta("data", "configuración.xlsx") (con los datos correspondientes en #ruta("data", "entrada")) para confirmar que la parametrización por año funciona.

Si en alguno de los puntos las cifras divergen, los logs de la fase 1 incluyen avisos con el número de registros descartados en cada filtro, lo que permite localizar la divergencia con bisección por etapa (presupuesto, amortizaciones, suministros, nóminas, cargos, regla 23, SS).

== Convenios tipográficos

Para que el documento sea consistente, distinguimos visualmente cinco tipos de elementos. Cada uno se introduce con una función Typst dedicada (definida en el preámbulo) y debe usarse de forma sistemática. La tabla resume la convención:

#table(
    columns: (auto, auto, 1fr),
    align: (left, center, left),
    table.header(
        table.hline(),
        [*Función*], [*Aspecto*], [*Para qué*],
        table.hline(),
    ),
    [#raw("#val(\"...\")")],
    [#val("ejemplo")],
    [Valor literal de un dato (códigos, identificadores en datos, valores enumerados de un campo).],
    [#raw("#campo(\"...\")")], [#campo("ejemplo")], [Nombre de campo o columna en una tabla o fichero.],
    [#raw("#ruta(\"...\")")], [#ruta("ejemplo.xlsx")], [Nombre o ruta de un fichero o directorio.],
    [#raw("#etqele/#etqcen/#etqact(\"...\")")],
    [#etqele("ejemplo")],
    [Identificador de un nodo de los árboles (elementos de coste, centros de coste o actividades, respectivamente).],
    [#raw("#nombre-regla[...]")], [#nombre-regla[ejemplo]], [Nombre de una regla.],
    table.hline(),
)

Para los demás casos: las comillas españolas «...» se reservan para nombres de menús de la #app, secciones referenciadas, términos que se introducen por primera vez o citas literales dentro de prose; el formato monoespaciado #raw("`...`") se usa para fragmentos de código, extensiones de fichero y plantillas/placeholders. No se deben usar comillas tipográficas inglesas (`"..."`) en el cuerpo del documento; si aparecen, son argumentos a las funciones anteriores y nunca deben verse renderizadas en el PDF.

== Datos de entrada

La carpeta #ruta("datos", "entrada") contiene los datos que se van a procesar para generar las unidades de coste. Se organizan en siete grupos, cada uno con su propia carpeta:

En las siguientes secciones los describimos y describimos también algunos filtros y preprocesos sobre ellos, de modo que lleguen a la fase de generación de unidades de coste con los datos preparados. En algunos casos, se generan tablas intermedias que pueden ser útiles para depurar el proceso, y que se describen también en estas secciones.

Los filtros se expresan con reglas que son ítems de listas. Si empiezan con un texto entre corchetes, ese texto es el nombre o descripción de la regla, que se puede usar para identificar su aplicación en la #app.

=== Configuración

El fichero #ruta("data", "configuración.xlsx") centraliza las constantes anuales y de política del sistema. Tiene tres columnas: #campo("nombre"), #campo("valor") y #campo("descripción"). Al cambiar de ejercicio analizado (o al cambiar una norma — base máxima de cotización, tipos de SS, jornada anual…) debería bastar con editar este Excel; el código no contiene literales para ninguna de estas constantes (las lee vía #campo("coana.util.configuración")).

Las constantes están agrupadas conceptualmente:

#table(
    columns: (auto, 1fr),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(
        table.hline(),
        [*Grupo*], [*Constantes*],
        table.hline(),
    ),
    [*Ejercicio*], [#campo("año_analizado")],
    [*Regla 23*], [#campo("jornada_anual_pdi") · #campo("factor_impartición_docente") · #campo("sexenio_vivo_años") · #campo("másteres_ficticios_pod") · #campo("umbral_residual_regla23")],
    [*Tesis*],
    [#campo("tesis_horas_tiempo_completo") · #campo("tesis_horas_tiempo_parcial") · #campo("tesis_pct_tutor") · #campo("tesis_pct_directores")],
    [*Grupos de investigación*], [#campo("grupos_horas_coordinador_semana")],
    [*Seguridad social calculada*],
    [#campo("ss_base_máxima") · #campo("ss_tipo_contingencias_comunes") · #campo("ss_tipo_reducción_cc_trabajador") · #campo("ss_tipo_mei") · #campo("ss_tipo_formación_profesional") · #campo("ss_cuota_solidaridad_factor_tramo1") · #campo("ss_cuota_solidaridad_factor_tramo2") · #campo("ss_cuota_solidaridad_tipo_tramo1") · #campo("ss_cuota_solidaridad_tipo_tramo2") · #campo("ss_cuota_solidaridad_tipo_tramo3")],
    [*Categorías*], [#campo("categorías_docencia_pura_plaza") · #campo("categorías_pdi_funcionario")],
    [*Proyectos generales*], [#campo("proyectos_generales_nómina") · #campo("proyectos_generales_cargos")],
    [*Cargos académicos*], [#campo("pagas_extra_cargo")],
    table.hline(),
)

Para las constantes cuyo valor es una lista (categorías, proyectos generales), la celda contiene los códigos separados por comas. El loader (#campo("cfg_set"), #campo("cfg_tuple")) se encarga de descomponerla.

=== Estructuras de la contabilidad analítica

El modelo #coana define cuatro estructuras, que son descripciones jerárquicas de diferentes componentes de la contabilidad analítica:

- elementos de coste
- centros de coste
- actividades
- elementos de ingreso

Cada una de estas estructuras se describe en un fichero `.tree` en el directorio #ruta("data", "entrada", "estructuras"). Es un formato de texto con líneas que representan una jerarquía entre elementos (un bosque, es decir, un conjunto de árboles) mediante el sangrado. Cada línea tiene una descripción y, separado por un tubo («|»), un identificador.

Cuando se cargan en memoria las líneas, se asocia a cada línea un código de la forma #código[01.02.03.01], en función de su nivel de profundidad y orden (#código[05.03.01] significa «primer hijo del tercer hijo del quinto árbol»).

El identificador de cada línea se puede usar para referirse al nodo. De cada nodo se puede conocer su lista ordenada de hijos y quién es su padre. Aunque no se especifique en los fichero, podemos asumir que los árboles del bosque dependen de una raíz única: un nodo sin descripción y cuyo código es la cadena vacía. Llamamos UJI (es la organización en su conjunto) a ese nodo.

Un ejemplo de un fragmento de árbol. Si el contenido de un fichero `.tree` es el siguiente:

#text(size: .85em, ```text
Centros de docencia | docencia
    Facultades y escuelas | facultades-escuelas
        Escuela Superior de Tecnología y Ciencias Experimentales | estce
        Facultad de Ciencias Humanas y Sociales | fchs
        Facultad de Ciencias Jurídicas y Económicas | fcje
        Facultad de Ciencias de la Salud | fcs
    Aulas y laboratorios docentes | aulas
        Aulas Escuela Superior de Tecnología y Ciencias Experimentales | aulas-estce
        Aulas Facultad de Ciencias Humanas y Sociales | aulas-fchs
        Aulas Facultad de Ciencias Jurídicas y Económicas | aulas-fcje
        Aulas Facultad de Ciencias de la Salud | aulas-fcs
    Otros centros docentes | otros-docentes
        Universidad de los Mayores | mayores
Centros de investigación | investigación
```)

Se puede representar como una tabla con las siguientes filas:

#{
    set text(size: .85em)
    align(center, table(
        columns: 4,
        align: (left, left, left, left),

        table.header(
            table.hline(),
            [*código*], [*nombre*], [*identificador*], [*padre*],
            table.hline(),
        ),
        código[01], [Centros de docencia], etq("docencia", clave-color: "cen"), etq("UJI", clave-color: "cen"),
        código[01.01], [Facultades y escuelas], etq("facs", clave-color: "cen"), etq("docencia", clave-color: "cen"),
        código[01.01.01],
        [Escuela Superior de Tecnología y Ciencias Experimentales],
        etq("estce", clave-color: "cen"),
        etq("facs", clave-color: "cen"),
        código[01.01.02],
        [Facultad de Ciencias Humanas y Sociales],
        etq("fchs", clave-color: "cen"),
        etq("facs", clave-color: "cen"),
        código[01.01.03],
        [Facultad de Ciencias Jurídicas y Económicas],
        etq("fcje", clave-color: "cen"),
        etq("facs", clave-color: "cen"),
        código[01.01.04],
        [Facultad de Ciencias de la Salud],
        etq("fcs", clave-color: "cen"),
        etq("facs", clave-color: "cen"),
        código[01.02],
        [Aulas y laboratorios docentes],
        etq("aulas", clave-color: "cen"),
        etq("docencia", clave-color: "cen"),
        código[01.02.01],
        [Aulas Escuela Superior de Tecnología y Ciencias Experimentales],
        etq("aulas-estce", clave-color: "cen"),
        etq("aulas", clave-color: "cen"),
        código[01.02.02],
        [Aulas Facultad de Ciencias Humanas y Sociales],
        etq("aulas-fchs", clave-color: "cen"),
        etq("aulas", clave-color: "cen"),
        código[01.02.03],
        [Aulas Facultad de Ciencias Jurídicas y Económicas],
        etq("aulas-fcje", clave-color: "cen"),
        etq("aulas", clave-color: "cen"),
        código[01.02.04],
        [Aulas Facultad de Ciencias de la Salud],
        etq("aulas-fcs", clave-color: "cen"),
        etq("aulas", clave-color: "cen"),
        código[01.03],
        [Otros centros docentes],
        etq("otros-docentes", clave-color: "cen"),
        etq("docencia", clave-color: "cen"),
        código[01.03.01],
        [Universidad de los Mayores],
        etq("mayores", clave-color: "cen"),
        etq("otros-docentes", clave-color: "cen"),
        código[02],
        [Centros de investigación],
        etq("investigación", clave-color: "cen"),
        etq("UJI", clave-color: "cen"),
        table.hline(),
    ))
}

Otras columnas pueden añadir atributos a los nodos. Por ejemplo, hará falta un atributo de color si queremos destacar ciertos nodos. Más adelante querremos destacar nodos que se añadirán a la estructura, para diferenciarlos de los que había originalmente.

En el código los árboles han de poder responder a las preguntas:

- Dado su identificador o su código, ¿quiénes son los hijos de un nodo?
- Dado su identificador o su código, ¿quién es el padre de un nodo?
- Dado su identificador, ¿qué código tiene?
- Dado su identificador o su código, ¿qué descripción tiene un nodo?
- Dado su código, ¿qué identificador tiene?
- Dado su identificador, ¿qué código tiene un nodo ?
- ¿Qué identificadores tienen los nodos que tienen esta subcadena en su código, descripción o identificador?

Además, ha de soportar una acción que modifica el árbol: añadir un nodo con una descripción y un sufijo de identificador, como hijo de un nodo particular dado su identificador. Cuando se ejecute esa acción se ha de marcar que el nodo ha sido insertado, porque ha de añadirse después de los que había originalmente, pero en orden alfabético respecto de los que se añaden al árbol y no estaban originalmente. Además, los nodos añadidos se muestran en un color distinto. Estos nodos añadidos son los que llevan información de color, que debería ser distinto según la etapa de la fase 1 en la que se añadan.

Te pongo un ejemplo. Si quiero añadir un nodo con descripción _Facultad de Ciencias Sociales_ y sufijo de identificador #etq("ciencias-sociales", clave-color: "cen") como hijo del nodo con identificador #etq("facs", clave-color: "cen"), el resultado sería un nuevo nodo en el árbol, lo que lo dejaría como si el fichero de texto que lo generó fuera:

#text(size: .85em, ```text
Centros de docencia | docencia
    Facultades y escuelas | facultades-escuelas
        Escuela Superior de Tecnología y Ciencias Experimentales | estce
        Facultad de Ciencias Humanas y Sociales | fchs
        Facultad de Ciencias Jurídicas y Económicas | fcje
        Facultad de Ciencias de la Salud | fcs
        Facultad de Ciencias Sociales | facs-ciencias-sociales
    Aulas y laboratorios docentes | aulas
        Aulas Escuela Superior de Tecnología y Ciencias Experimentales | aulas-estce
        Aulas Facultad de Ciencias Humanas y Sociales | aulas-fchs
        Aulas Facultad de Ciencias Jurídicas y Económicas | aulas-fcje
        Aulas Facultad de Ciencias de la Salud | aulas-fcs
    Otros centros docentes | otros-docentes
        Universidad de los Mayores | mayores
Centros de investigación | investigación
```)


con código #código("01.01.05") y padre #etq("facs", clave-color: "cen"). Esta operación solo es efectiva si no existía previamente un nodo con identificador #etq("facs-ciencias-sociales", clave-color: "cen"). Si ya existía, se ha de comprobar si el nombre del nodo existente coincide con el nombre que se quiere añadir. Si coincide, no se hace nada. Si no coincide, se ha de lanzar un error porque hay una colisión de identificadores.

La inserción se hace en orden alfabético respecto de los nodos que se han añadido al árbol, pero después de los que ya estaban originalmente. En el ejemplo, el nuevo nodo se insertaría después de _Facultad de Ciencias de la Salud_ y antes de _Otros centros docentes_, aunque el orden alfabético respecto de los nodos añadidos lo situaría antes de _Facultad de Ciencias de la Salud_, pero como ese nodo ya estaba originalmente, el nuevo nodo se sitúa después.

Cuando pidan representar el árbol, se han de mostrar los nodos nuevos usando colores, para que destaquen. Los colores serán distintos en función de la etapa de la fase 1 en la que se han añadido.

Las estructuras, descritas en ficheros `.tree`, son:

#align(center, table(
    columns: 2,
    align: (left, left),

    table.header(table.hline(), [*Fichero*], [], table.hline()),

    ruta("elementos de coste.tree"),
    [Árbol que define la estructura jerárquica de los elementos de coste. Con _elemento de coste_ nos referimos a un componente de esta estructura. Por ejemplo, si en el fichero hay una línea con la descripción _Costes de personal_ y el identificador `costes-personal`, el tipo de elemento de coste #etqele("costes-personal") se refiere a esa línea del árbol.],

    ruta("centros de coste.tree"),
    [Árbol que define la estructura jerárquica de los centros de coste. Cuando decimos _centro de coste_, nos referimos a un identificador de esta estructura.],

    ruta("actividades.tree"),
    [Árbol que define la estructura jerárquica de las actividades. Cuando decimos _actividad_, nos referimos a un identificador de esta estructura.],

    ruta("elementos de ingreso.tree"),
    [Árbol que define la estructura de los elementos de ingreso. Cuando decimos _tipo de elemento de ingreso_, nos referimos a un identificador de esta estructura.],
    table.hline(),
))

#nota[El de elementos de ingreso lo definiremos más tarde. Los alternativos a centros de coste (por comportamiento) y actividades los veremos más tarde.]

==== Editor gráfico de árboles

Los ficheros `.tree` se pueden editar como texto (un editor convencional respeta la sintaxis de sangrado e identificadores), pero hay también un editor gráfico dedicado que se lanza con #raw("uv run coana editor-tree"). Está implementado con tkinter (sin dependencias externas, parte de la biblioteca estándar).

Funciones del editor:

- Carga cualquier `.tree` y muestra el árbol con codificación incremental (los códigos #código("01.02.03") se recalculan automáticamente conforme se mueven o renombran los nodos).
- Búsqueda por subcadena en descripciones e identificadores; se resaltan las coincidencias.
- Detección de identificadores duplicados (colisiones): se marcan en rojo.
- Mueve/copia/borra nodos respetando la jerarquía.
- Soporta operaciones de teclado y ratón habituales (deshacer, copiar, pegar, arrastrar).

El comando equivalente directo (sin pasar por el CLI principal) es #raw("uv run editor_de_arboles").


=== Inventario

El fichero de inventario contiene registros vinculados a ubicaciones físicas en el campus. Cada ubicación está asociada a un «servicio» y cada servicio está asociado a un centro de coste, así que la ubicación es un dato importante para determinar el centro de coste al que se asigna el bien.

Los siguientes ficheros se encuentran en #ruta("data", "entrada", "inventario"):

#nota[Deberíamos añadir el SQL que produce cada fichero para cada descripción de un fichero de los diferentes conjuntos de datos de entrada.]

#let ficheros_campos_inventario = (
    "inventario.xlsx": (
        descripción: [Fichero con el inventario de bienes de la universidad],
        campos: (
            id: [Un identificador único del registro en el inventario],
            valor_inicial: [Valor de compra del bien (en euros)],
            fecha_alta: [Fecha de alta del bien],
            cuenta: [Cuenta contable en la que se registró la compra, que se usará para determinar los años de amortización. Ver #ruta("años amortización por cuenta.xlsx").],
            id_ubicación: [Un entero que identifica el espacio. Ver #ruta("ubicaciones.xlsx").],
            descripción: [Una descripción del bien],
            estado: [describe el estado del bien en el inventario (#val("A"): activo, #val("B"): baja, …)],
        ),
    ),
    "años amortización por cuenta.xlsx": (
        descripción: [Fichero con los años de amortización según la cuenta contable en la que se registró la compra.],
        campos: (
            cuenta: [Cuenta contable],
            nombre: [descripción de la cuenta],
            años_amortización: [los años de amortización de ese tipo de bien],
        ),
    ),
    "ubicaciones a servicios.xlsx": (
        descripción: [Contiene la asignación de ubicaciones a servicios. Esta asignación es importante para determinar el centro de coste al que se asigna cada bien del inventario, porque cada servicio está asociado a un centro de coste.],
        campos: (
            id_ubicación: [Código numérico de la ubicación],
            servicio: [Código (entero) del servicio. Ver #ruta("servicios.xlsx").],
        ),
    ),
    "servicios.xlsx": (
        descripción: [Fichero con los servicios de la universidad],
        campos: (
            servicio: [Un identificador único del servicio],
            nombre: [El nombre del servicio],
            vivo: [1 si el servicio tiene alguna ubicación asignada y 0 si no (son servicios que se han dado de baja pero que no se han eliminado de la base de datos)],
            centro: [Etiqueta del centro de coste asociado al servicio en el árbol de centros de coste. Por ejemplo, la Facultat de Ciències de la Salut tiene como identificador de servicio el número #val("2922"). El centro de coste asociado a ese servicio es #etqcen("fcs").],
        ),
    ),
)

#tabula_ficheros_y_campos(ficheros_campos_inventario)



=== Ubicaciones

Ciertos repartos de costes guardan relación con la superficie de los centros de coste a los que se asignan, y para eso es importante conocer la superficie de cada ubicación física del campus, así como su ubicación en la jerarquía de espacios del campus (complejo, edificio, zona, ubicación).

El campus se organiza en:

- *Complejos* (por ejemplo, la Escuela Superior de Tecnología y Ciencias Experimentales (ESTCE) es un complejo)
- *Edificios* (en la ESTCE hay varios edificios: edificio docente, edificio de departamentos experimentales, edificio de informática y matemáticas, módulo de talleres e invernaderos).
- *Zonas*. Cada edificio puede constar de una sola zona (por ejemplo, el invernadero) o tener más de una zona (por ejemplo, el módulo docente, que tiene dos zonas).
- *Ubicaciones*. Cada zona tiene una o más ubicaciones (cada aula, cada despacho, cada laboratorio, cada pasillo, cada lavabo…) que se identifica con un número
    - Cada ubicación es de un tipo (aula, despacho, sala de reuniones…).
    - Cada ubicación ocupa unos metros cuadrados.

Se utiliza una cadena para identificar cada ubicación. La cadena puede llevar guiones para facilitar la lectura, pero los guiones no aportan información relevant. Lo importante es la posición que ocupa cada letra o dígito, porque su lectura posicional permite ubicar la dependencia en el campus.

- la primera letra identifica el complejo
- la segunda letra identifica la edificación dentro del complejo
- la tercera letra identifica la zona dentro de la edificación
- a continuación, hay un número de tres dígitos que identifica la ubicación (típicamente un dígito es la planta y los otros un número seriado dentro de la planta)
- las dos última letras identifican el tipo de ubicación (aula, despacho, sala de reuniones…)

Por ejemplo, MD0114FG (que se podría haber escrito MD-0114-FG, por ejemplo) se lee así:

- M: complejo de la Facultad de Ciencias de la Salud
- D: módulo docente
- 0: zona 0 (hay tres zonas en ese módulo docente: 0, 1 y 2)
- 1: planta 1
- 14: número de ubicación dentro de la planta
- FG: tipo de ubicación (en este caso, es un despacho de gestión)

En adelante usaremos la palabra #emph[edificio] para referirnos a la edificación identificada con las dos primeras letras, y llamaremos zona al grupo de tres letras (lo que podría crear confusión con la zona entendida como la tercera letra, pero esperamos que el contexto ayude a desambiguar cada caso).

La #app ha de permitir visualizar la información de metros cuadrados de cada ubicación, zona, edificio y complejo, y comprobar que la suma de los metros cuadrados de todas las ubicaciones de una zona es igual a los metros cuadrados de esa zona, que la suma de los metros cuadrados de todas las zonas de un edificio es igual a los metros cuadrados de ese edificio, y que la suma de los metros cuadrados de todos los edificios de un complejo es igual a los metros cuadrados del complejo.

Los ficheros de entrada sobre ubicaciones están en el directorio #ruta("data", "entrada", "superficies") y son los siguientes:

#let ficheros_campos_ubicaciones = (
    "complejos.xlsx": (
        descripción: [Fichero con los complejos del campus],
        campos: (
            complejo: [Un identificador único de complejo (es una letra: A, B, C…)],
            descripción: [Una descripción del complejo],
        ),
    ),
    "edificaciones.xlsx": (
        descripción: [Fichero con las edificaciones dentro de cada complejo],
        campos: (
            complejo: [Identificador del complejo al que pertenece],
            edificación: [Identificador de edificación (una letra o dígito) dentro del complejo],
            descripción: [Una descripción de la edificación],
        ),
    ),
    "zonas.xlsx": (
        descripción: [Fichero con las zonas de edificaciones y espacios del campus (urbanizados o no), como jardines, viales, parcelas…],
        campos: (
            complejo: [Identificador del complejo],
            edificación: [Identificador de la edificación],
            zona: [Identificador de zona (una letra o dígito) dentro de la edificación],
            descripción: [Una descripción de la zona],
        ),
    ),
    "ubicaciones.xlsx": (
        descripción: [Fichero con las ubicaciones del campus],
        campos: (
            id_ubicación: [El número entero que identifica la ubicación, que se corresponde con el campo `id_ubicación` del inventario],
            complejo: [Identificador de complejo (ejemplo: J)],
            edificación_zona: [Código de edificación y zona combinados (ejemplo: C2)],
            planta: [Identifica la planta de la edificación (ejemplo: 2)],
            dependencia: [Identifica la dependencia en la planta (ejemplo: 14)],
            tipo_ubicación: [Identifica el tipo de la ubicación (ejemplo: SD)],
            metros_cuadrados: [Los metros cuadrados de la ubicación],
            descripción: [Una descripción de la ubicación],
        ),
    ),
    "tipos de ubicación.xlsx": (
        descripción: [Fichero con los tipos de ubicación del campus],
        campos: (
            tipo_ubicación: [Un identificador único de tipo de dependencia],
            descripción: [Una descripción del tipo de dependencia],
        ),
    ),
    "corrector superficie.xlsx": (
        descripción: [Tabla de coeficientes correctores que se aplican a las superficies de zonas/edificaciones cuyo prefijo de ubicación encaja con una de las filas. Permite ajustar el reparto de costes (energía, limpieza, otros) en espacios que no se comportan como un aula o despacho típico: pistas deportivas al aire libre, galerías de servicios, etc. El loader del contexto de inventario aplica cada coeficiente como factor multiplicativo sobre la superficie nominal en el cálculo de los pesos por centro.],
        campos: (
            prefijo: [Prefijo del código de ubicación al que aplica el corrector. Se hace match por prefijo (#val("DC"), #val("DA"), #val("A"), …), de modo que todas las ubicaciones cuyo código empiece por ese prefijo reciben el coeficiente.],
            corrector_energía: [Coeficiente multiplicativo sobre la superficie a efectos de reparto del coste energético (entre #val("0") y #val("1") típicamente: #val("0,1") para una pista deportiva descontará 90 % del peso de su superficie).],
            corrección_limpieza: [Coeficiente análogo para el reparto del coste de limpieza.],
            corrección_otros: [Coeficiente para otros gastos de reparto por superficie (puede estar vacío si no aplica).],
            descripción: [Descripción libre del tipo de espacio al que se aplica el corrector.],
        ),
    ),
)

#tabula_ficheros_y_campos(ficheros_campos_ubicaciones)


#nota[Quizá tendría sentido que #ruta("ubicaciones a servicios") tuviera #campo("fecha_inicio") y #campo("fecha_fin") (posiblemente vacío) para indicar el período de tiempo en el que esa asignación es válida, de modo que se puedan tener en cuenta cambios en la asignación a lo largo del tiempo. Esto puede resultar importante Por simplicidad, de momento no se incluyen esos campos. Esto es algo para estudiar más adelante.]


=== Presupuesto

Se usan los siguientes ficheros, que son tablas que se pueden obtener con explotaciones sencillas de la base de datos.

#let ficheros_campos_presupuesto = (
    "apuntes presupuesto de gasto.xlsx": (
        descripción: [Fichero con los apuntes presupuestarios de gasto. Es el fichero principal, porque cada línea de este fichero genera una o más unidades de coste. El resto de ficheros que se describen sirven para interpretar el contenido de este y su contenido se usa en las reglas.],
        campos: (
            asiento: [Identificador del asiento contable al que corresponde el apunte presupuestario],
            registro: [Número de registro de la contabilidad presupuestaria],
            aplicación: [Cuatro dígitos que identifican la aplicación presupuestaria. Ver #ruta("aplicaciones de gasto.xlsx").],
            programa: [Código del programa presupuestario. Ver #ruta("programas presupuestarios.xlsx").],
            centro: [Código del centro presupuestario o centro de gasto (en puridad, el centro de gasto es el par centro-subcentro, pero a veces el centro determina la información de forma suficiente). Ver #ruta("centros.xlsx").],
            subcentro: [Código del subcentro (la clave del subcentro es la combinación de centro y subcentro). Ver #ruta("subcentros.xlsx").],
            proyecto: [Código del proyecto. Ver #ruta("proyectos.xlsx").],
            subproyecto: [Código del subproyecto (la clave completa del subproyecto es la combinación de proyecto y subproyecto). Ver #ruta("subproyectos.xlsx").],
            línea: [Código de la línea de financiación. Ver #ruta("líneas de financiación.xlsx").],
            importe: [Importe en euros],
            descripción: [Texto descriptivo del apunte presupuestario],
            fecha: [Fecha del apunte],
            per_id_endosatario: [Identificador de la persona endosataria. Interesa a efectos de identtificar el perceptor de una nómina cuando la tratamos desde el presupuesto, y no desde directamente desde nóminas. #nota[No se usa y se puede suprimir.]],
        ),
    ),
    "centros.xlsx": (
        descripción: [Contiene los centros de la estructura presupuestaria],
        campos: (
            centro: [Un identificador],
            nombre: [El nombre del centro],
        ),
    ),
    "subcentros.xlsx": (
        descripción: [Contiene los subcentros de la estructura presupuestaria],
        campos: (
            centro: [Un identificador. Ver #ruta("centros.xlsx").],
            subcentro: [Un identificador (debe tenerse en cuenta que la clave del subcentro es la combinación de centro y subcentro)],
            nombre: [El nombre del subcentro],
        ),
    ),
    "proyectos.xlsx": (
        descripción: [Contiene los proyectos de la estructura presupuestaria],
        campos: (
            proyecto: [Un identificador],
            nombre: [El nombre del proyecto],
            tipo: [Un identificador del tipo de proyecto (debe existir en #ruta("tipos de proyecto")). Se ha obtenido editando a mano el tipo que tenía en el GRE.],
            centro_origen: [Un identificador del centro presupuestario que gestiona el proyecto (debe existir en #ruta("centros"))],
            actividad: [Un código de actividad que se ha puesto en el proyecto a mano],
        ),
    ),
    "subproyectos.xlsx": (
        descripción: [Contiene los subproyectos de la estructura presupuestaria],
        campos: (
            proyecto: [Un identificador. Ver #ruta("proyectos.xlsx").],
            subproyecto: [Un identificador (debe tenerse en cuenta que la clave del subproyecto es la combinación de proyecto y subproyecto)],
            nombre: [El nombre del subproyecto],
        ),
    ),
    "tipos de proyecto.xlsx": (
        descripción: [Un fichero con los tipos de proyecto],
        campos: (
            tipo: [Un identificador],
            nombre: [El nombre del tipo de proyecto],
        ),
    ),
    "líneas de financiación.xlsx": (
        descripción: [Líneas que financian los proyectos y determinan la afectación de la financiación],
        campos: (
            línea: [Un identificador],
            nombre: [El nombre de la línea],
            tipo: [Un identificador del tipo de línea (debe existir en #ruta("tipos de línea")],
            afectada: [Un booleano que indica si la línea es genérica (false) o afectada (true)],
            financiador: [Una descripción del financiador ],
        ),
    ),
    "tipos de línea.xlsx": (
        descripción: [Tipos de línea de financiación],
        campos: (
            tipo: [Un identificador],
            nombre: [El nombre del tipo de línea],
        ),
    ),
    "programas presupuestarios.xlsx": (
        descripción: [Programa presupuestarios que determinan la naturaleza del gasto presupuestario],
        campos: (
            programa: [Un identificador (422-A, 422-C, 422-D o 541-A)],
            nombre: [El nombre del programa presupuestario],
        ),
    ),
    "aplicaciones de gasto.xlsx": (
        descripción: [Un fichero con las aplicaciones de gasto],
        campos: (
            aplicación: [Un identificador formado con 4 dígitos],
            nombre: [El nombre de la aplicación de gasto],
        ),
    ),
    "aplicaciones a elementos de coste.xlsx": (
        descripción: [Tabla de traducción de aplicación presupuestaria al identificador de un elemento de coste del árbol. Es la regla por defecto del paso de asignación de elemento de coste: si la aplicación tiene una entrada en este fichero (y el #campo("elemento_de_coste") no está vacío ni vale #val("xxx")), el apunte recibe ese elemento de coste; si no, queda como pendiente y son las reglas condicionales con nombre las que pueden cubrirlo. La columna #campo("nombre") es informativa (copiada de #ruta("aplicaciones de gasto.xlsx")) para facilitar la edición. El valor #val("xxx") marca aplicaciones todavía sin clasificar (quedan reservadas para que se rellenen explícitamente).],
        campos: (
            aplicación: [Cuatro dígitos. Identifica la aplicación presupuestaria.],
            nombre: [Nombre de la aplicación, copiado de #ruta("aplicaciones de gasto.xlsx") como ayuda al editor. No se usa en el código.],
            elemento_de_coste: [Identificador (slug) de un nodo del árbol de elementos de coste. Si está vacío o vale #val("xxx"), la aplicación se considera sin asignación por defecto.],
        ),
    ),
    "capítulos de gasto.xlsx": (
        descripción: [Un fichero con los capítulos de gasto],
        campos: (
            capítulo: [Un identificador formado por un dígito (1, 2, 3, 4, 5 o 6)],
            nombre: [El nombre del capítulo de gasto],
        ),
    ),
    "artículos de gasto.xlsx": (
        descripción: [Un fichero con los artículos de gasto],
        campos: (
            artículo: [Un identificador formado por un dígito (1, 2, 3, 4, 5 o 6)],
            nombre: [El nombre del artículo de gasto],
        ),
        "conceptos de gasto.xlsx": (
            descripción: [Un fichero con los conceptos de gasto],
            campos: (
                concepto: [Un identificador formado por un dígito (1, 2, 3, 4, 5 o 6)],
                nombre: [El nombre del concepto de gasto],
            ),
        ),
    ),
)

#tabula_ficheros_y_campos(ficheros_campos_presupuesto)


=== Nóminas

#let ficheros_campos_nóminas = (
    "nóminas y seguridad social.xlsx": (
        descripción: [Fichero con las nóminas y seguridad social de recursos humanos],
        campos: (
            id: [Código de dos números (enteros) separados por barra (ejemplo #val("10192422/1"))],
            tipo_coste: [Código del tipo de coste asociado al apunte de nómina (Ver #ruta("tipos coste plantilla.xlsx")).],
            expediente: [Entero (ejemplo #val("5913")). Ver #ruta("expedientes recursos humanos.xlsx").],
            categoría: [Código de categoría. Ver #ruta("categorías recursos humanos.xlsx").],
            perceptor: [Código de perceptor. Ver #ruta("perceptores.xlsx").],
            provisión: [Código del sistema de provisión. Ver #ruta("provisiones.xlsx").],
            fecha: [Fecha retribución/pago.],
            importe: [Importe.],
            atrasos: [Marca o importe de atrasos asociado a este apunte. Sirve como pista complementaria al filtro por #campo("concepto_retributivo") = #val("30") o #val("87") cuando se separa la bolsa de atrasos del PDI/PVI.],
            concepto_retributivo: [Código del concepto retributivo. Ver #ruta("conceptos retributivos.xlsx").],
            proyecto: [Código de proyecto presupuestario. Ver #ruta("data", "entrada", "presupuesto", "proyectos.xlsx").],
            subproyecto: [Código de subproyecto presupuestario. Ver #ruta("data", "entrada", "presupuesto", "subproyectos.xlsx")],
            aplicación: [Aplicación presupuestaria. Ver #ruta("data", "entrada", "presupuesto", "aplicaciones de gasto.xlsx")],
            programa: [Programa presupuestario. Ver #ruta("data", "entrada", "presupuesto", "programas presupuestarios.xlsx")],
            línea: [Línea de financiación. Ver #ruta("data", "entrada", "presupuesto", "líneas de financiación.xlsx")],
            centro: [Centro de gasto presupuestario. Ver #ruta("data", "entrada", "presupuesto", "centros.xlsx")],
            subcentro: [subcentro de gasto presupuestario. Ver #ruta("data", "entrada", "presupuesto", "subcentros.xlsx")],
            servicio: [Código (entero) del servicio. Ver #ruta("servicios.xlsx").],
            centro_plaza: [
                Solo es útil en el caso de las personas en conserjerías, porque están en un servicio determinado (servicio #val("368")), pero su coste se imputa al del servicio en el que están físicamente. Ver #ruta("servicios.xlsx").
            ],
            categoría_plaza: [Es parte de la clave para #ruta("categorías plazas.xlsx")],
            sector_plaza: [Es la otra parte de la clave para #ruta("categorías plazas.xlsx")],
        ),
    ),
    "categorías plazas.xlsx": (
        descripción: [Fichero con las categorías de plazas de recursos humanos],
        campos: (
            categoría: [Código de categoría..],
            sector: [Código de sector..],
            nombre: [Nombre de la categoría],
        ),
    ),
    "provisiones.xlsx": (
        descripción: [Fichero con los modos de provisión de un puesto de trabajo (¿hace falta?)],
        campos: (
            provisión: [Código del modo de provisión (una letra o dos letras o dígitos)],
            nombre: [Nombre del modo de provisión],
        ),
    ),
    "categorías recursos humanos.xlsx": (
        descripción: [Fichero con las categorías de recursos humanos],
        campos: (
            sector: [#val("BEC"), #val("JUB"), #val("PAS"), #val("PDI"), #val("PI"), #val("PRA") (en principio no aparecerán #val("JUB") o #val("PRA"))],
            categoría: [Código de la categoría (por ejemplo, #val("PCU"), PD…)],
            nombre: [Nombre de la categoría],
            es_funcionario: [0 o 1],
        ),
    ),
    "expedientes recursos humanos.xlsx": (
        descripción: [Relaciona el expediente con la persona y el sector al que se asigna el expediente.],
        campos: (
            expediente: [Identificador (entero).],
            per_id: [Identificador de la persona (entero). Ver #ruta("personas.xlsx").],
            sector: [#val("BEC"), #val("JUB"), #val("PAS"), #val("PDI"), #val("PI"), #val("PRA"). Los valores #val("JUB") y #val("PRA") no deberían aparecer, porque no hay expedientes de jubilados ni de personal en prácticas.],
        ),
    ),
    "cargos.xlsx": (
        descripción: [Fichero con los cargos de recursos humanos.],
        campos: (
            cargo: [Identificador (entero).],
            nombre: [Nombre del cargo.],
            cargo_asimilado: [Cargo del RD 1086/1989 al que se asimila a efectos retributivos. Ver #ruta("cargos real decreto.xlsx").],
            dedicación_porcentual: [Dedicación del cargo expresada como tanto por uno (p. ej. #val("0,375") para un 37,5 %). Tiene prioridad sobre #campo("dedicación_horaria"): si está informada y > 0, se aplica como porcentaje sobre las horas no docentes del PDI en la regla 23.],
            dedicación_horaria: [Dedicación del cargo expresada como horas anuales absolutas. Solo se utiliza cuando #campo("dedicación_porcentual") es nula o cero. Si ambas son nulas o cero, el cargo no aporta horas en la regla 23.],
            actividad: [Actividad a la que se asocia el cargo. Puede ser una concreta del árbol de actividades o un patrón para su cálculo.],
            centro: [Etiqueta del centro de coste al que se asigna el cargo. Puede ser una concreta del árbol de centros de coste o un patrón para su cálculo.],
        ),
    ),
    "cargos real decreto.xlsx": (
        descripción: [Cargos del Real Decreto 1086/1989 para asimilación de cargos UJI y determinación del importe mensual con el que se retribuye.],
        campos: (
            cargo_real_decreto: [Código del cargo en el Real Decreto 1086/1989.],
            nombre: [Nombre del cargo],
            importe_mensual: [Cuantía mensual que se retribuye por ese cargo.],
        ),
    ),
    "personas cargos.xlsx": (
        descripción: [Dice qué persona ocupa qué cargo de qué fecha a qué fecha.],
        campos: (
            per_id: [Identificador de la persona. Ver #ruta("personas.xlsx").],
            expediente: [Identificador del expediente. Ver #ruta("expedientes recursos humanos.xlsx").],
            cargo: [Identificador del cargo. Ver #ruta("cargos.xlsx").],
            servicio: [Departamento, Facultad... Ver #ruta("servicios.xlsx").],
            titulación: [Titulación en la que desempeña el cargo (en el caso de los cargos docentes). Puede estar grado, máster o doctorado. Ver #ruta("grados.xlsx"), #ruta("másteres.xlsx"), #ruta("doctorados.xlsx").],
            fecha_inicio: [Fecha de nombramiento],
            fecha_fin: [Fecha de cese],
            fecha_inicio_cobra: [Fecha de efectos económicos],
            fecha_fin_cobra: [Fecha fin de efectos económicos],
        ),
    ),
    "tipos coste plantilla.xlsx": (
        descripción: [Catálogo de tipos de coste de plantilla. Sirve como referencia descriptiva para la columna #campo("tipo_coste") de #ruta("nóminas y seguridad social.xlsx"); no interviene en las reglas de la fase 1, pero permite informes y depuración por tipo.],
        campos: (
            tipo_coste: [Identificador del tipo de coste.],
            nombre: [Nombre o descripción del tipo de coste.],
        ),
    ),
    "perceptores.xlsx": (
        descripción: [
            La #campo("descripción") debería llevar otro nombre y el contenido es un poco delirante (y con erratas).
        ],
        campos: (
            perceptor: [Código de dos dígitos],
            nombre: [
                Nombre del tipo de perceptor (#val("Funcionari docent"), #val("Professor Vinculat"),  #val("Funcionari Interi docent")…)
            ],
            descripción: [
                Régimen social (#val("EXENTOS"), #val("EXENTOS SS"), #val("MUFACE"), #val("MUFACE O SEG.SOC. TP"), #val("MUNPAL"), #val("NINGUNA"), #val("NO TIENE"), #val("SEGURIDAD SOCAIL"), #val("SEGURIDAD SOCIAL"), #val("SEGURIDAD SOCIAL O MUFACE"), #val("SEGURIDAD SOCIAL/MUFACE"), #val("SS/ OTRAS EXENCIONES"), #val("TC/ SS/ MUFACE") o blanco)
            ],
        ),
    ),
    "conceptos retributivos.xlsx": (
        descripción: [Fichero con los conceptos retributivos de recursos humanos],
        campos: (
            concepto_retributivo: [Código de dos dígitos],
            nombre: [descripción del concepto retributivo],
        ),
    ),
    "personas.xlsx": (
        descripción: [Fichero con las personas de recursos humanos],
        campos: (
            per_id: [Clave (entero) asociado a la persona],
            nombre: [],
            apellido1: [],
            apellido2: [],
            tipo: [Indica si es una persona física #val("Física") o una persona jurídica #val("Jurídica")],
        ),
    ),
    "reducciones laborales.xlsx": (
        descripción: [
            Histórico de reducciones de jornada por expediente. Cada fila
            registra un periodo de reducción con su porcentaje trabajado.
            En la fase 1 solo se procesan las filas de #campo("tipo reduccion") =
            #val("8") (representación sindical) con solape al año analizado;
            el resto de tipos (lactancia, cuidado de hijos, etc.) se ignoran.
        ],
        campos: (
            expediente: [Identificador del expediente. Ver #ruta("expedientes recursos humanos.xlsx").],
            "fecha inicio": [Fecha de inicio del periodo de reducción (puede preceder al año analizado).],
            "fecha fin": [Fecha de fin del periodo. Si está vacía la reducción sigue vigente.],
            "porcentaje trabajado": [
                Tanto por uno con coma decimal (#val("0,6666"), #val("0,87")…) que indica
                la fracción efectivamente trabajada durante el periodo. Un #val("0") indica
                liberación al 100 %. Si está vacío se interpreta como #val("0").
            ],
            "tipo reduccion": [
                Código del tipo de reducción. Cruza con #ruta("tipos_reducciones.xlsx").
                Solo el #val("8") (representación sindical) interviene en el modelo.
            ],
        ),
    ),
    "tipos_reducciones.xlsx": (
        descripción: [
            Catálogo de los tipos de reducción de jornada (la tabla de
            referencia de la columna #campo("tipo reduccion") de
            #ruta("reducciones laborales.xlsx")). Incluye el tipo #val("8")
            (liberación sindical parcial), los de conciliación (lactancia,
            cuidado de hijos o familiares, monoparentalidad…) y los de salud
            o discapacidad. Algunas filas son alias en desuso (su nombre
            indica «no usar, es el N»).
        ],
        campos: (
            id: [Código del tipo de reducción (entero como texto: #val("1"), #val("8")…).],
            nombre: [Descripción del tipo (en valenciano).],
            reduccion: [
                Indicador (#val("1") / #val("0")) de si el tipo supone una
                reducción efectiva de jornada a efectos del modelo.
            ],
        ),
    ),
)

#tabula_ficheros_y_campos(ficheros_campos_nóminas)



=== Consumos
La energía eléctrica se factura conjuntamente, pero la OTOP puede asignar un coste exacto a cada edificio, porque tiene medidores de consumo por edificio. Por lo tanto, el coste de la energía se puede asignar a cada edificio, y a cada zona dentro de cada edificio, con exactitud.

#let ficheros_campos_consumos = (
    "energía.xlsx": (
        descripción: [Fichero con los datos de coste de energía eléctrica según diferentes contadores],
        campos: (
            prefijo: [Prefijo de complejo-edificación-zona a la que se asocia el coste],
            coste: [Importe],
            comentario: [Comentario para ayudar a entender el prefijo],
        ),
    ),
    "agua.xlsx": (
        descripción: [Fichero con los datos de coste de del agua según diferentes contadores],
        campos: (
            prefijo: [Prefijo de complejo-edificación-zona a la que se asocia el coste],
            coste: [Importe],
            comentario: [Comentario para ayudar a entender el prefijo],
        ),
    ),
    "gas.xlsx": (
        descripción: [Fichero con los datos de coste de gas según diferentes contadores],
        campos: (
            prefijo: [Prefijo de complejo-edificación-zona a la que se asocia el coste],
            coste: [Importe],
            comentario: [Comentario para ayudar a entender el prefijo],
        ),
    ),
    "distribución OTOP.xlsx": (
        descripción: [
            Fichero proporcionado por la OTOP que distribuye los costes de mantenimiento, limpieza y seguridad por zonas, edificios o complejos. Permite distribuir el gasto de SC001 en ciertas aplicaciones.
        ],
        campos: (
            prefijo: [
                Cadena que determina, en función de su longitud, el complejo, edificación o zona a la que se asina un coste.
            ],
            porcentaje: [
                Porcentaje del coste total de mantenimiento, limpieza o seguridad que se asigna a ese prefijo. En tanto por uno.
            ],
            comentario: [
                Campo de texto con comentarios sobre el coste asignado a ese prefijo.
            ],
        ),
    ),
)

#tabula_ficheros_y_campos(ficheros_campos_consumos)


Los ficheros #ruta("energía.xlsx"), #ruta("agua.xlsx") y #ruta("gas.xlsx") los facilita la OTOP. Esos datos no están actualmente en la base de datos corporativa.


=== Docencia

Los datos de docencia sirven para determinar a qué actividades docentes se dedica cada profesor.

Las tablas se almacenan en el directorio #ruta("datos", "entrada", "docencia") y son las siguientes:

#let ficheros_campos_docencia = (
    "pod.xlsx": (
        descripción: [Fichero con los datos de docencia de cada profesor],
        campos: (
            per_id: [Identificador (entero) de persona. Ver #ruta("data", "entrada", "nóminas", "personas.xlsx").],
            curso_académico: [Año (actual o anterior).],
            semestre: [#val("1"): primer semestre, #val("2"): segundo semestre, #val("1-2"): ambos, #val("A"): anual],
            asignatura: [Código de asignatura (de grado o máster). Ver #ruta("grados.xlsx") y #ruta("másteres.xlsx").],
            departamento: [Identificador (entero) de departamento.],
            reducción: [Valor (flotante). Reducción de créditos. #nota[¿Es necesario?]],
            tutorías: [Valor (flotante).],
            créditos_impartidos: [Valor (flotante). Créditos impartidos por el profesor en esa asignatura ese curso académico ese semestre.],
            créditos_computables: [Valor (flotante). Créditos computables para el reparto de costes de esa asignatura ese curso académico ese semestre. #nota[¿Es necesario?]],
            grupo: [Grupo de impartición de la asignatura],
            subgrupo: [Subgrupo de impartición de la asignatura],
        ),
    ),
    "pod másteres.xlsx": (
        descripción: [Resolución de la titulación efectiva cuando una asignatura del POD pertenece a varias titulaciones simultáneamente. Para cada terna #campo("per_id"), #campo("asignatura") concreta, fija el #campo("máster") en el que el profesor imparte realmente esa asignatura. Por la regla del catálogo, todas las titulaciones implicadas deben ser másteres; si aparece un grado, se marca como anomalía.],
        campos: (
            per_id: [Identificador (entero) de persona. Ver #ruta("data", "entrada", "nóminas", "personas.xlsx").],
            curso_académico: [Año (actual o anterior).],
            asignatura: [Código de asignatura. Ver #ruta("asignaturas másteres.xlsx").],
            máster: [Código del máster en el que se imparte la asignatura para este profesor. Ver #ruta("másteres.xlsx").],
        ),
    ),
    "asignaturas grados.xlsx": (
        descripción: [Fichero con las asignaturas de grado],
        campos: (
            asignatura: [Código de la asignatura, por ejemplo #val("FC14")],
            nombre: [Nombre de la asignatura, por ejemplo #val("Comerç Just i Desenvolupament Sostenible")],
            grado: [Identificador (entero) del grado en la que se imparte la asignatura, por ejemplo #val("73"). Ver #ruta("grados.xlsx").],
        ),
    ),
    "asignaturas másteres.xlsx": (
        descripción: [Fichero con las asignaturas de máster],
        campos: (
            asignatura: [Código de la asignatura, por ejemplo #val("EAA002")],
            nombre: [Nombre de la asignatura, por ejemplo #val("Marc normatiu i estratègic per a l'aplicació de les polítiques d'igualtat")],
            máster: [Identificador (entero) de la titulación en la que se imparte la asignatura, por ejemplo #val("58005"). Ver #ruta("másteres.xlsx").],
        ),
    ),
    "grados.xlsx": (
        descripción: [Fichero con los grados],
        campos: (
            grado: [Identificador (entero). Por ejemplo, #val("73")],
            nombre: [Nombre del grado],
            estudio: [Identificador (entero), Ver #ruta("estudios.xlsx").],
        ),
    ),
    "másteres.xlsx": (
        descripción: [Fichero con los másteres],
        campos: (
            máster: [Identificador (entero >= 42000)],
            nombre: [Nombre del máster],
            oficial: [#val("S") o #val("N"), según sea oficial o no],
            interuniversitario: [#val("S") o #val("N"), según sea interuniversitario o no],
            estudio: [Código del estudio (entero, >90000). Ver #ruta("estudios.xlsx").],
        ),
    ),
    "estudios.xlsx": (
        descripción: [Fichero con los estudios de grado y máster. Un mismo estudio puede instanciarse con varias titulaciones. Por ejemplo, cada edición del plan de estudios de una misma titulación se representa como una titulación diferente de un mismo estudio. *Nota:* un mismo código de #campo("estudio") puede aparecer en varias filas (distintas ediciones del nombre). Al construir el catálogo de titulaciones se deduplica por código de estudio y el número de titulaciones por asignatura se cuenta por titulaciones #emph[distintas]; en caso contrario, una asignatura de grado con su estudio repetido se confundiría con una asignatura de varias titulaciones y se marcaría, erróneamente, como #val("máster múltiple no resuelto").],
        campos: (
            estudio: [Identificador (entero, >90000).],
            nombre: [Nombre del estudio],
        ),
    ),
    "estimación horas docencia propia.xlsx": (
        descripción: [Fichero con las horas y el importe retribuido de formación permanente en el modulo gre],
        campos: (
            gre_ejercicio: [Ejercicio de la retribución],
            fecha: [Fecha de la retribución],
            perid: [Identificador de la persona que ha recibido la retribución #nota[Debería ser `per_id`, pero en la hoja está mal escrito.]],
            motivo: [Justificación del curso, suele ser el nombre del curso],
            horas: [Número de horas impartidas],
            importe_hora: [Precio del coste hora],
            importe_total: [Valor total de la retribución, debe coincidir con importe $times$ unidad],
            proyecto: [Codigo del proyecto ver #ruta("proyectos.xlsx") ],
            nombre: [Tipo de curso],
            gre_id: [Identificador de la retribución],
        ),
    ),
    "doctorados.xlsx": (
        descripción: [Catálogo de programas de doctorado. La fase 1 lo lista pero todavía no consume sus columnas en ninguna regla. #nota[Estos datos deberían ir, a futuro, al apartado de investigación.]],
        campos: (
            doctorado: [Identificador (entero) del programa de doctorado.],
            nombre: [Nombre del programa.],
            interuniversitario: [#val("S") o #val("N").],
            estudio: [Código del estudio (entero, > 90000) si aplica. Ver #ruta("estudios.xlsx").],
        ),
    ),
    "microcredenciales.xlsx": (
        descripción: [Catálogo de microcredenciales. La fase 1 utiliza, vía la regla #nombre-regla[Microcredenciales (EPMI)], la actividad #etqact("microcredenciales") + #campo("proyecto") y, en algunos proyectos generales (p. ej. #val("24G112")), una actividad agregada #etqact("microcredenciales"). Las columnas adicionales (apuntados, edición, profesores…) son informativas y no participan en reglas.],
        campos: (
            PER_ID: [Identificador de persona del profesor que imparte.],
            APUNTADOS: [Número de apuntados a la microcredencial.],
            CURSO_ID: [Identificador de la microcredencial.],
            NOMBRE_CURSO: [Nombre de la microcredencial.],
            ANYO: [Año de impartición.],
            URL: [URL informativa.],
            CREDITOS_ECTS: [Créditos ECTS.],
            EDIC: [Número de edición.],
            PROFES: [Nombre del profesor o profesores que la imparten.],
        ),
    ),
)

#tabula_ficheros_y_campos(ficheros_campos_docencia)


=== Reducciones del PDI

El PDI puede tener reducciones de su capacidad docente por motivos diversos (cargos, sexenios, tesis dirigidas, representación sindical, etc.), expresadas en *créditos*. La fase 1 las utiliza —de momento, solo las de representación sindical, tipos 37-40— para determinar qué fracción de la jornada anual dedica cada profesor a la actividad sindical. Ver §«Reducciones por representación sindical».

Las tablas se almacenan en el directorio #ruta("datos", "entrada", "reducciones pdi") y son las siguientes:

#let ficheros_campos_reducciones_pdi = (
    "tipos reducciones docentes.xlsx": (
        descripción: [Catálogo de tipos de reducción docente. Los tipos #val("37") (UGT), #val("38") (STEPV), #val("39") (CCOO) y #val("40") (CSI-F) son los de representación sindical.],
        campos: (
            id: [Código (entero) del tipo de reducción.],
            nombre: [Descripción del tipo de reducción.],
        ),
    ),
    "reducciones docentes.xlsx": (
        descripción: [Reducciones de capacidad docente concedidas al PDI: una fila por (persona, tipo de reducción, curso).],
        campos: (
            "tipo reducción": [Código del tipo de reducción. Ver #ruta("tipos reducciones docentes.xlsx").],
            "curso aca": [Curso académico (entero, p. ej. #val("2025")).],
            per_id: [Identificador (entero) de persona. Ver #ruta("data", "entrada", "nóminas", "personas.xlsx").],
            creditos: [Créditos de reducción de ese tipo en ese curso.],
        ),
    ),
    "carga docente.xlsx": (
        descripción: [Capacidad y reducción docentes del PDI: una fila por (persona, curso).],
        campos: (
            per_id: [Identificador (entero) de persona.],
            "creditos reduccion": [Créditos de reducción docente totales de la persona en el curso (incluye todos los tipos, sindicales y no sindicales).],
            creditos: [Capacidad docente (créditos) de la persona en el curso.],
            "curso aca": [Curso académico (entero).],
        ),
    ),
)

#tabula_ficheros_y_campos(ficheros_campos_reducciones_pdi)


=== Investigación

Las tablas se almacenan en el directorio #ruta("datos", "entrada", "investigación") y son las siguientes:

#let ficheros_campos_investigación = (
    "grupos investigación.xlsx": (
        descripción: [Catálogo de grupos de investigación de la universidad. Incluye también los institutos de investigación (cuyo identificador es alfabético: #val("IEI"), #val("INAM"), #val("IUDT"), etc.) — éstos se filtran en la fase 1 cuando solo se quieren los grupos *propiamente dichos*.],
        campos: (
            grupo: [Identificador del grupo. Para los grupos de investigación es un código numérico con ceros a la izquierda (#val("003"), #val("034"), #val("335"), …). Para los institutos es un código alfabético (#val("IEI"), #val("INAM"), …).],
            nombre: [Nombre del grupo (en valenciano o castellano según el caso).],
            fecha_alta: [Fecha de constitución del grupo.],
            activo: [#val("S") o #val("N").],
        ),
    ),
    "grupos a institutos.xlsx": (
        descripción: [Mapeo de cada grupo de investigación al instituto en el que está adscrito. Los grupos no adscritos a ningún instituto se etiquetan con #val("INVES"). Generado a partir de un fichero auxiliar (`grupos_alternativo.yaml`); solo incluye grupos «de verdad», no institutos.],
        campos: (
            id_grupo: [Identificador del grupo (mismo que la columna #campo("grupo") de #ruta("grupos investigación.xlsx")).],
            nombre_grupo: [Nombre del grupo.],
            instituto: [Código del instituto al que está adscrito el grupo: #val("INAM"), #val("INIT"), #val("IIDL"), #val("IIEI"), #val("IIFV"), #val("IIG"), #val("IILP"), #val("IMAC"), #val("IUPA"), #val("IUDSP"), #val("IUDT"), #val("IUT"), #val("IUCE"), #val("IUTC"), #val("IULMA"), #val("IUEFG"); o #val("INVES") si el grupo no está adscrito a ningún instituto.],
        ),
    ),
    "investigadores en grupos.xlsx": (
        descripción: [Pertenencias de investigadores titulares a grupos. Una persona puede aparecer en varios grupos y/o líneas a lo largo del tiempo, con sus respectivas fechas de alta y baja.],
        campos: (
            per_id: [Identificador (entero) de persona. Ver #ruta("data", "entrada", "nóminas", "personas.xlsx").],
            id_grupo: [Identificador del grupo. Ver #ruta("grupos investigación.xlsx").],
            línea: [Identificador de la línea de investigación dentro del grupo.],
            fecha_alta: [Fecha de incorporación al grupo/línea.],
            fecha_baja: [Fecha de baja (vacía si sigue activo).],
            participación: [Tanto por ciento de participación de la persona en esa línea.],
            principal: [#val("S") o #val("N"), si es el grupo principal de la persona.],
            coordinador: [#val("S") o #val("N"), si coordina la línea.],
            interlocutor: [#val("S") o #val("N"), si actúa como interlocutor.],
        ),
    ),
    "colaboradores en grupos.xlsx": (
        descripción: [Igual que #ruta("investigadores en grupos.xlsx") pero para personas en régimen de colaboración (no titulares del grupo). No tiene los campos #campo("principal"), #campo("coordinador") ni #campo("interlocutor").],
        campos: (
            per_id: [Identificador (entero) de persona.],
            id_grupo: [Identificador del grupo.],
            línea: [Identificador de la línea.],
            fecha_alta: [Fecha de alta.],
            fecha_baja: [Fecha de baja (vacía si sigue activo).],
            participación: [Tanto por ciento de participación.],
        ),
    ),
    "tesis.xlsx": (
        descripción: [Información de tesis doctorales con sus directores, fechas, estado, programa de doctorado y régimen de dedicación. Cada fila es un *periodo de matrícula*: una misma tesis (identificada por #campo("per_id_alumno")) puede tener varios periodos (p. ej. cambios entre tiempo completo y parcial, bajas y reincorporaciones). Es uno de los insumos de la regla 23.],
        campos: (
            per_id_alumno: [Identificador (entero) del doctorando.],
            fecha_inicio_tiempo: [Fecha de inicio del cómputo de tiempo del periodo.],
            fecha_inicio_tesis: [Fecha de inicio formal de la tesis (igual o anterior a #campo("fecha_inicio_tiempo")).],
            fecha_fin_tiempo: [Fecha de fin del cómputo de tiempo del periodo (vacía si el periodo sigue abierto).],
            fecha_lectura_tesis: [Fecha de lectura (vacía si no leída todavía).],
            per_id_director: [Identificador del director principal.],
            per_id_tutor: [Identificador del tutor (cuando aplica).],
            per_id_codirector: [Identificador del codirector (opcional).],
            per_id_codirector2: [Identificador de un segundo codirector (opcional).],
            estado: [Régimen del periodo: #val("C") tiempo completo · #val("P") tiempo parcial · #val("B") baja · #val("BM") baja por maternidad · #val("BV") baja por otra causa.],
            estudio: [Código del programa de doctorado (#val("90xxx")). Se cruza con #ruta("data", "entrada", "docencia", "doctorados.xlsx") (nombre) y con #ruta("data", "entrada", "docencia", "doctorados actividad centro.xlsx") (etiqueta de actividad y centro de coste).],
        ),
    ),
    "investigadores en contratos.xlsx": (
        descripción: [Participación de personas en contratos del SGIT (proyectos de investigación, contratos de transferencia y otros). Una persona puede figurar en varios contratos y un contrato suele tener varios participantes. Insumo del cargador #emph[proyectos] de la regla 23.],
        campos: (
            per_id: [Identificador de la persona participante.],
            contrato: [Identificador interno del contrato en el SGIT. Cruza con #ruta("proyectos en contratos investigación.xlsx") y #ruta("anexos proyectos.xlsx").],
            horas_contratadas_semana: [Horas/semana con las que está contratada formalmente la persona en el proyecto, cuando se han fijado (puede ser nulo).],
            principal: [#val("S") o #val("N"), si es el investigador principal del contrato.],
            interlocutor: [#val("S") o #val("N"), si actúa como interlocutor administrativo.],
            fecha_inicio_solicitud: [Fecha desde la que la persona figura como participante (según la solicitud original).],
            fecha_fin_solicitud: [Fecha hasta la que figura como participante.],
            fecha_inicio_solicitud_alternativa: [Fecha alternativa de incorporación. No se utiliza en la fase 1.],
            fecha_fin_solicitud_alternativa: [Fecha alternativa de finalización. No se utiliza en la fase 1.],
        ),
    ),
    "proyectos en contratos investigación.xlsx": (
        descripción: [Líneas presupuestarias de cada contrato: cada contrato puede tener varias líneas con proyectos presupuestarios distintos (o el mismo proyecto en sucesivas anualidades). Lo usamos para conocer la *vigencia* del contrato (mínimo y máximo de fechas entre sus líneas) y el *proyecto presupuestario* asociado (línea de menor número con importe > 0).],
        campos: (
            contrato: [Identificador del contrato.],
            línea: [Número de línea dentro del contrato. La línea de menor número se considera la principal.],
            proyecto: [Código del proyecto presupuestario asociado a la línea (cruza con #ruta("data", "entrada", "presupuesto", "proyectos.xlsx")).],
            subproyecto: [Subdivisión del proyecto presupuestario.],
            fecha_inicio: [Inicio del periodo de la línea.],
            fecha_fin: [Fin del periodo de la línea.],
            importe_concedido: [Importe asignado a la línea. Las líneas con importe nulo o cero se descartan en la fase 1.],
        ),
    ),
    "anexos proyectos.xlsx": (
        descripción: [Caracterización del contrato vista desde la convocatoria de la que procede. Hay exactamente un anexo por contrato. La concatenación #campo("tipo_anexo") + #campo("subtipo_anexo") + #campo("microtipo_anexo") identifica el tipo de financiación (proyecto europeo, nacional, regional, propio, art. 60, cátedra, etc.) y se usa en la fase 1 para determinar la actividad y las horas/semana de cada miembro del contrato.],
        campos: (
            contrato: [Identificador del contrato.],
            codex: [Código externo del expediente de la convocatoria (p. ej. #val("PID2024-159788NB-I00") para proyectos del Ministerio).],
            id_anexo: [Identificador del anexo dentro del contrato. Solo hay un anexo por contrato, así que su valor es siempre #val("1") en la práctica.],
            ejercicio_convocatoria: [Año de la convocatoria.],
            id_convocatoria: [Identificador interno de la convocatoria.],
            tipo_anexo: [Primer dígito del código del tipo (#val("1"), #val("2"), #val("4"), …).],
            subtipo_anexo: [Segundo carácter del código (#val("A"), #val("C"), #val("P"), …).],
            microtipo_anexo: [Tercer carácter del código (#val("A"), #val("E"), #val("L"), #val("N"), #val("U"), #val("V"), …).],
        ),
    ),
    "contratos a departamentos.xlsx": (
        descripción: [Adscripción administrativa de cada contrato a una unidad estructural de la UJI. El campo clave para la fase 1 es #campo("tuest_id"): los contratos cuya única adscripción es a una unidad de tipo #val("VI") (vicerrectorado), #val("CT") (cátedra) o #val("SE") (servicio) se *excluyen* del cómputo de horas de la regla 23, porque la participación de personas en esos contratos suele venir vinculada a un cargo institucional, no a trabajo investigador efectivo.],
        campos: (
            contrato: [Identificador del contrato.],
            id_dep: [Código alfabético del departamento a efectos presupuestarios (p. ej. #val("AEYM"), #val("TECN")).],
            uest_id: [Identificador numérico interno de la unidad estructural. Es el código que prima como *servicio* y se tabula en otra fuente.],
            nombre: [Nombre de la unidad.],
            tuest_id: [Tipo de unidad estructural: #val("DE") departamento, #val("IN") instituto, #val("VI") vicerrectorado, #val("CT") cátedra, #val("SE") servicio.],
        ),
    ),
    "sexenios.xlsx": (
        descripción: [Sexenios de investigación reconocidos al PDI. Cada fila es un sexenio (o transferencia) concedido a una persona. Lo usamos en la fase de reparto de la regla 23 para identificar quién tiene un *sexenio vivo* (PDI con un sexenio finalizado hace menos de seis años respecto al fin del año analizado): en esas personas, las horas no distribuidas (HND) se imputan íntegramente al grupo de investigación.],
        campos: (
            per_id: [Identificador (entero) de la persona.],
            fecha_inicio_sexenio: [Inicio del sexenio (los seis años de investigación que se evaluaron).],
            fecha_fin_sexenio: [Fin del sexenio. Es la fecha relevante para determinar si está #emph[vivo]: #campo("fecha_fin_sexenio") $gt.eq$ fin_año $-$ 6 años.],
            fecha_efecto: [Fecha desde la que el sexenio tiene efectos retributivos.],
            cantidad: [Importe ligado al sexenio (puede ser 0 si es transferencia o si está pendiente de cobro).],
            es_transferencia: [#val("S") si el sexenio es de transferencia, #val("N") si es de investigación clásica.],
        ),
    ),
    "horas kalendas.xlsx": (
        descripción: [Horas de dedicación declaradas y validadas por el personal investigador (sistema Kalendas de imputación horaria). Cada fila es una validación de horas de una persona en un contrato, para un tipo de actividad. Al cargarlo se *filtra* para quedarse solo con las filas de #campo("tipo_actividad") = #val("Proyecto de investigacion") y se agregan en el diccionario #campo("per_id") → (#campo("contrato") → Σ #campo("horas_declaradas")): por cada investigador, la suma de horas declaradas a proyectos de investigación en cada contrato del SGIT.],
        campos: (
            per_id: [Identificador (entero) de la persona que declara las horas.],
            fecha_validación: [Fecha en la que las horas declaradas quedaron validadas.],
            horas_declaradas: [Número de horas de dedicación declaradas y validadas.],
            contrato: [Identificador interno del contrato en el SGIT al que se imputan las horas. Cruza con #ruta("investigadores en contratos.xlsx") y #ruta("proyectos en contratos investigación.xlsx").],
            tipo_actividad: [Tipo de actividad al que corresponden las horas declaradas (#val("Proyecto de investigacion"), #val("Altres activitats I+D"), #val("Resta de docència"), #val("Vacances"), #val("Baixa laboral")…). Solo se conservan las de #val("Proyecto de investigacion").],
        ),
    ),
)

#tabula_ficheros_y_campos(ficheros_campos_investigación)


== Unidades de coste

Una *unidad de coste* representa un coste cierto para la universidad y proviene
- de la contabilidad presupuestaria (o de información relacionada con esta, como los consumos de energía, agua o gas),
- de las amortizaciones de elementos del inventario,
- de la nómina de los trabajadores
- o de la división de otras unidades de coste.

Es importante tener trazabilidad sobre el origen de cada unidad de coste. Esto obliga a registrarlo en cada unidad de coste.

Desde el punto de vista de su implementación, una unidad de coste es un registro que tiene, al menos, los siguientes campos:

#align(center, table(
    columns: 2,
    align: (left, left),

    table.header(table.hline(), [#strong[Campo]], [#strong[Descripción]], table.hline()),

    campo("id"), [Un código único que identifica la unidad de coste],
    campo("elemento_de_coste"), [Un identificador de `elementos de coste.tree`],
    campo("centro_de_coste"), [Un identificador de `centros de coste.tree`],
    campo("actividad"), [Un identificador de `actividades.tree`],
    campo("importe"), [Un importe en euros. *Precisión:* los importes se manejan internamente como números de coma flotante de doble precisión (#val("float64")) a lo largo de todo el pipeline, sin redondeos intermedios; el redondeo al céntimo más próximo se aplica *únicamente al presentar* la información al usuario (vistas web, cuadros de fase 2, informes a la carta, exportaciones). Los ficheros `.xlsx` de la fase 1 guardan el valor con precisión completa y aplican un formato de celda de dos decimales para su visualización.],
    campo("origen"),
    [Presupuesto, energía, agua, gas, nómina, inventario, o unidad de coste (valores de un `Enum`), dependiendo de si el elemento procede de un apunte presupuestario, de un coste de energía, agua o gas, de un pago por nómina, de un registro de inventario o de otra unidad de coste.],

    campo("origen_id"),
    [Si el elemento viene de un apunte presupuestario, el identificador del apunte presupuestario del que procede, que es el valor de `asiento`. Si el elemento viene de un pago por nómina, el identificador del pago por nómina del que procede. Si el elemento viene de un registro de inventario, el identificador del registro de inventario del que procede. Si el elemento procede de otra unidad de coste, el identificador de la unidad de coste del que procede.],

    campo("origen_porción"),
    [Tanto por uno del importe de la unidad de coste del que procede que se ha asignado a este elemento de coste. Por ejemplo, si una unidad de coste de 100 euros se reparte al 50% entre otras dos unidades de coste, cada una de ellas tendrá un origen con `origen` = unidad, `origen_id` = identificador de la unidad original y `porción` = 0.5.],
    table.hline(),
))


== Proceso secuencial

El programa trabaja secuencialmente en dos fases. Cada fase tiene una serie de etapas y cada etapa genera un conjunto de datos que, o bien forma parte del producto final, o bien alimenta a las etapas siguientes:

- *Fase 1*: generación de unidades de coste a partir de los datos de entrada. El orquestador (#campo("coana.fase1.ejecutar")) ejecuta las etapas en este orden:
    + *Inventario y superficies* — enriquece el inventario, calcula las matrices de presencia por centro y prepara la distribución de superficies necesaria para suministros y amortizaciones.
    + *Enriquecimiento del árbol de centros de coste con los grupos de investigación* — añade un nodo por grupo de #ruta("entrada", "investigación", "grupos a institutos.xlsx") bajo su instituto o bajo el nodo virtual #etqcen("inves"). Además, siempre crea bajo #etqcen("inves") el centro virtual #etqcen("no-adscritos-a-grupo-de-investigación"), que absorbe el coste del PDI con horas repercutidas a investigación pero sin grupo adscrito.
    + *Presupuesto* — filtra los apuntes presupuestarios de gasto, aplica las reglas de clasificación de elementos de coste, centros de coste y actividades, y produce las UC presupuestarias.
    + *Suministros (energía, agua, gas)* — reparte el coste de los apuntes especiales de SC001 entre los centros con presencia en cada zona/edificio/complejo.
    + *Amortizaciones* — calcula la amortización anual de los bienes inventariables vivos y los reparte entre centros de coste según presencia.
    + *Cargos académicos: pre-cálculo* — estima la extra «camuflada» en CR 68 antes del preprocesamiento de nóminas (necesario para evitar duplicar la masa de cargos).
    + *Nóminas — preprocesamiento* — agrupa nóminas por expediente, separa por sector y produce las UC retributivas «extras» (PTGAS, PVI extras, PDI extras), las UC de despidos, indemnizaciones por asistencias y costes sociales calculados (clases pasivas).
    + *Cargos académicos: reparto* — reparte la masa CR 19/64 en proyecto general entre los cargos de cada persona, ponderando por días×cuantía mensual del RD asimilado.
    + *Regla 23 — dedicación PDI* — ejecuta los cinco cargadores (POD, tesis, cargos, proyectos, grupos), normaliza a la jornada anual mediante las fases 5-7 del modelo y reparte la masa regla 23 en UC con peso #emph[horas_finales / Σ horas_finales] por persona.
    + *Reparto de seguridad social* — distribuye la SS (cotizada + calculada) entre los pares (#campo("actividad"), #campo("centro_de_coste")) de cada persona, ponderando por importe total de UC retributivas (incluyendo las del reparto regla 23 que acaban de generarse).
    + *Consolidación* — agrega todas las UC de las etapas anteriores en #ruta("fase1", "unidades de coste.xlsx") y serializa los árboles finales modificados.

- *Fase 2*: generación de informes consolidados a partir de las UC de la fase 1 (pendiente de especificar en detalle).

== Especificación mediante reglas

El filtro de elementos de tablas o la generación de las unidades de coste se describe con secuencias de reglas.

Las reglas permiten indicar qué condiciones se han de observar para que un registro sea filtrado o pase el filtro y, cuando se están creando unidades de coste, permite asignar una etiqueta de elemento de coste, de centro de coste y de actividad a cada unidad de coste a la unidad o unidades de coste que se crean a partir de un registro en función de condiciones que cumple este.

Las reglas siguen un orden. Cuando una regla tiene éxito y asigna una etiqueta, no hay que seguir con las reglas que asignan ese tipo de etiquetas. Es importante, por tanto, que las reglas de mayor detalle aparezcan antes que las más generales, para que se asignen las etiquetas más concretas posibles a cada unidad de coste.


=== La interpretación de las reglas cuando tienen condiciones comunes

En general, las reglas tienen una condición, que puede ser compleja, y proporcionan un resultado, que es la etiqueta que se asigna si se cumple la condición.

Por ejemplo, una regla podría ser

#reglas[
    - Si el #campo("programa") es #val("500-X"), la actividad es #etq("act-ejemplo", clave-color: "act").
]

Cuando hay elementos comunes en las condiciones de una regla, se expresan en un ítem de nivel superior con «subítems» para cada caso particular. La condición de cada «subítem» es lo que resulta de aplicar la condición del ítem de nivel superior y la condición del «subítem». Por ejemplo,

#reglas[
    - Si el #campo("programa") es #val("500-X")
        - y si el #campo("tipo de proyecto") es #val("99"), la actividad es #etq("act-ejemplo", clave-color: "act");
        - y si el #campo("tipo de proyecto") es #val("98"), la actividad es #etq("act-contraejemplo", clave-color: "act");
        - en otro caso, la actividad es #etq("act-otro-ejemplo", clave-color: "act").
]

En realidad son tres reglas:

#reglas[
    - Si el #campo("programa") es #val("500-X") y el #campo("tipo de proyecto") es #val("98"), la actividad es #etq("act-ejemplo", clave-color: "act")
    - Si el #campo("programa") es #val("500-X") y el #campo("tipo de proyecto") no es #val("98"), la actividad es #etq("act-contraejemplo", clave-color: "act")
    - Si el #campo("programa") es #val("500-X"), la actividad es #etq("act-otro-ejemplo", clave-color: "act")
]
Nótese que el orden es importante y que la primera regla que tiene éxito se aplica y se deja de evaluar el resto de reglas (de los contrario, las dos primeras nunca serían efectivas).

En la #app se de mostrar un listado con las etiquetas que no existen en el fichero `.tree` correspondiente y se han aplicado a una unidad de coste. Se ha de mostrar la regla en la que se menciona esa etiqueta inexistente, de modo que el analista de la aplicación pueda corregirla.

=== Reglas con identificación

Para poder referirnos a las reglas, usaremos una notación como la de este ejemplo, que se puede usar para identificar la regla en la #app. Por ejemplo,

#reglas[
    - #nombre-regla("Etiquetado de actividades ejemplo")
        Si el #campo("programa") es #val("500-X")
        - y si el #campo("tipo de proyecto") es #val("99"), la actividad es #etq("act-ejemplo", clave-color: "act");
        - y si el #campo("tipo de proyecto") es #val("98"), la actividad es #etq("act-contraejemplo", clave-color: "act");
        - en otro caso, la actividad es #etq("act-otro-ejemplo", clave-color: "act").
]

Con #nombre-regla[Etiquetado de actividades ejemplo] nos referimos al conjunto de reglas. Como hemos dicho, esa regla en realidad se compone de tres reglas. Cada una de ellas sería #nombre-regla[Etiquetado de actividades ejemplo (1)], #nombre-regla[Etiquetado de actividades ejemplo (2)] y #nombre-regla[Etiquetado de actividades ejemplo (3)]. Es decir, equivale a

#reglas[
    - #nombre-regla("Etiquetado de actividades ejemplo (1)")
        Si el #campo("programa") es #val("500-X") y el #campo("tipo de proyecto") es #val("98"), la actividad es #etq("act-ejemplo", clave-color: "act")
    - #nombre-regla("Etiquetado de actividades ejemplo (2)")
        Si el #campo("programa") es #val("500-X") y el #campo("tipo de proyecto") no es #val("98"), la actividad es #etq("act-contraejemplo", clave-color: "act")
    - #nombre-regla("Etiquetado de actividades ejemplo (3)")
        Si el #campo("programa") es #val("500-X"), la actividad es #etq("act-otro-ejemplo", clave-color: "act")
]

Alternativamente, es posible que un subítem tenga un nombre específico:

#reglas[
    - #nombre-regla("Etiquetado de actividades ejemplo")
        Si el #campo("programa") es #val("500-X")
        - y si el #campo("tipo de proyecto") es #val("99"), la actividad es #etq("act-ejemplo", clave-color: "act");
        - y si el #campo("tipo de proyecto") es #val("98"), la actividad es #etq("act-contraejemplo", clave-color: "act");
        - #nombre-regla("Resto")
            en otro caso, la actividad es #etq("act-otro-ejemplo", clave-color: "act").
]

En ese caso, la tercera regla se llamaría #nombre-regla[Etiquetado de actividades ejemplo. Resto]. En cualquier caso, lo importante es que cada regla tenga un nombre que permita identificarla en la #app. De ese modo, cuando se muestre el número de veces que se ha aplicado cada regla, el analista podrá identificarla y, al seleccionar la regla, podrá ver los apuntes a los que se ha aplicado esa regla y las unidades de coste que se han generado a partir de esos apuntes.


=== Reglas que modifican el mismo árbol del que asignan etiquetas

Los árboles de elementos de coste, centros de coste y actividades se pueden modificar a través de reglas, creando nuevos elementos de coste, centros de coste o actividades. Para eso usaremos el operador suma (+) y diremos cómo formar una etiqueta nueva y qué posición ha de ocupar en el árbol.

Una etiqueta con suma, como #etq("act-ejemplo", clave-color: "act") + #campo("proyecto") sobre un apunte cuyo proyecto es #val("28I000") genera una nueva etiqueta #etq("act-ejemplo-28I000", clave-color: "act"). Esa etiqueta se ha de crear en el árbol de actividades, como hijo de #etq("act-ejemplo", clave-color: "act"). En ese caso, no te preocupes de su código, porque el código se asigna automáticamente en función de la posición que ocupe en el árbol. Cuando vayas a poner su códido en el documento de especificación, si #etq("act-ejemplo", clave-color: "act") es el nodo #código("01.02") y #etq("act-ejemplo-28I000", clave-color: "act") es hijo suyo, usa algo como #código("01.02.01.XX"). En el documento basta con saber que es un hijo de #etq("act-ejemplo", clave-color: "act"), pero el código exacto se asigna automáticamente en función de la posición que ocupe en el árbol y ese es un subproducto de esta fase.

He aquí un ejemplo de regla con suma:

#reglas[
    - Si el #campo("programa") es #val("500-X") y el #campo("tipo de proyecto") es #val("99"), entonces la actividad es #etq("act-ejemplo", clave-color: "act") +  #campo("proyecto").
]




= La aplicación

Todo el proceso (lectura de datos, ejecución de la fase 1, exploración de resultados y revisión de anomalías) se opera desde una única aplicación de escritorio en el navegador, basada en _Streamlit_, a la que en este documento llamamos #app. Lo que sigue describe su organización; las reglas concretas que aplica al ejecutar la fase 1 se detallan en los capítulos siguientes.

== Estructura general

La #app se organiza en un panel lateral con un árbol de navegación y un área principal donde se muestra la vista seleccionada. El árbol tiene seis grupos en su nivel raíz, con sub-secciones dentro:

#align(center, table(
    columns: 2,
    align: (left, left),
    table.header(table.hline(), [*Grupo*], [*Sub-secciones*], table.hline()),
    [Presupuesto],
    [Resumen, Unidades de coste, Sin clasificar, Apuntes filtrados, Suministros, Distribución mantenimientos OTOP, Reglas de actividad, Reglas de centro de coste, Reglas de elemento de coste, Árbol: Actividades, Árbol: Centros de coste, Árbol: Elementos de coste],
    [Amortizaciones], [Resumen, Sin cuenta, Por cuenta, UC generadas, Sin centro],
    [Personal],
    [Resumen, Expedientes PDI, Expedientes PTGAS, Expedientes PVI, Expedientes otros, Multiexpediente, Persona, Anomalías PDI],
    [Regla 23],
    [Dedicación docente, Docencia no oficial, Estructura estudios, Cargos, Asignaturas sin titulación, Anomalías],
    [Cargos académicos], [Resumen, Por persona, Personas cargos, Catálogo de cargos],
    [Superficies], [Resumen, Totales, Presencia centros],
    [Resultados Fase 1], [Resumen, Todas las UC, Actividades, Centros de coste, Elementos de coste, Anomalías UC],
    table.hline(),
))

A estos grupos se suma una entrada *Entradas*, generada dinámicamente, que permite inspeccionar cualquiera de los ficheros de #ruta("data/entrada/").

Existe además un botón *Ejecutar Fase 1* en cabecera que dispara el proceso completo (recargando los módulos Python relevantes para que recoja cambios en el código sin reiniciar la #app) y limpia la caché de datos para que las vistas siguientes lean los nuevos parquets.

== Patrón de las vistas

Casi todas las vistas comparten el mismo patrón:

+ Una *tabla principal* con un widget de filtro arriba (búsqueda de texto, selector de columna y selector de ordenación). El filtro busca por _substring_, es insensible a tildes y a mayúsculas/minúsculas, y por defecto se aplica sobre todas las columnas; el desplegable de columna permite restringir la búsqueda a una columna concreta. Para evitar discrepancias entre lo que el usuario ve y la fila que se selecciona, la ordenación por cabecera está desactivada (limitación conocida de Streamlit 1.54 al combinar `dataframe` con selección de filas: al reordenar visualmente la tabla, los índices que devuelve la selección no siguen el nuevo orden) y la única forma de ordenar es el desplegable «Ordenar por».
+ Al pinchar una fila se muestra una *ficha de registro* con todos los campos de esa fila desplegados, incluyendo enriquecimientos (p. ej., el nombre de la persona si hay un #campo("per_id")).
+ Cuando aplica, debajo de la ficha aparecen *tablas de detalle* secundarias (apuntes de nómina del expediente, asignaturas con docencia del per_id…) que repiten el mismo patrón filtro → tabla → ficha.
+ Las cifras de importes se muestran siempre en notación europea (1.234,56 €) y, en cabeceras y resúmenes, se incluye el total de las columnas relevantes.

Las tablas se construyen sobre los parquets generados por la fase 1. La #app no recalcula nada al renderizar, sólo lee y filtra.

== Anomalías y depuración

Una preocupación recurrente del proceso es detectar registros que no encajan con las reglas y exponerlos al analista para que los revise. La #app tiene varias vistas dedicadas a esto. Cada vista lee uno (o dos) parquets generados por la fase 1 en #ruta("data", "fase1", "auxiliares") (o el subdirectorio que corresponda) y se construye sobre el patrón habitual «filtro + tabla + ficha». La tabla siguiente recoge el inventario canónico de anomalías:

#table(
    columns: (1fr, 1.3fr, 1.6fr),
    align: (left, left, left),
    table.header(
        table.hline(),
        [*Vista*], [*Parquet de origen*], [*Qué contiene y por qué se separa*],
        table.hline(),
    ),
    [Resultados → Anomalías UC],
    [(ninguno; se calcula al vuelo cruzando los parquets de UC con los árboles finales en #ruta("data", "fase1"))],
    [UC cuyo #campo("centro_de_coste"), #campo("actividad") o #campo("elemento_de_coste") no aparece como nodo en el árbol final correspondiente. Indica que una regla ha generado un identificador huérfano (typo, nodo eliminado en el árbol, o regla pendiente de añadir el nodo).],

    [Personal → Anomalías PDI],
    [#ruta("regla_23_asignaturas_sin_titulación.parquet")],
    [Asignaturas con docencia impartida (créditos > 0) cuya titulación no aparece en ninguno de los catálogos de referencia (grados, másteres oficiales, estudios propios, doctorados, microcredenciales). Origen: el filtro previo a la #nombre-regla[Regla 23] sobre #ruta("pod.xlsx") y los catálogos de titulaciones.],

    [Regla 23 → Anomalías (pestaña «Pod sin titulación efectiva»)],
    [#ruta("regla_23_anomalías_resolución.parquet")],
    [Filas de #ruta("pod.xlsx") cuya titulación no se puede resolver de forma efectiva tras aplicar las reglas de desambiguación (incluyendo el pod de másteres). Origen: paso de resolución de titulación efectiva en la #nombre-regla[Regla 23].],

    [Regla 23 → Anomalías (pestaña «Múltiples con grado»)],
    [#ruta("regla_23_múltiples_con_grado.parquet")],
    [Asignaturas que aparecen asociadas a varias titulaciones y al menos una de ellas no es máster, contra la regla del catálogo de pod de másteres (que solo desambigua entre másteres). Origen: paso de resolución de titulación efectiva en la #nombre-regla[Regla 23].],

    table.hline(),
)

Por convención, cualquier vista de anomalía indica el número de registros afectados y el importe (o créditos) en juego, y permite navegar al detalle de cada caso para depurarlo. Si el parquet correspondiente no existe (porque la fase 1 no detectó ninguna anomalía de ese tipo), la vista muestra un mensaje de éxito en lugar de tabla vacía.

== Resumen final y descargas

La sección *Resultados Fase 1 → Resumen* recoge contadores agregados (UC totales por origen, importes por sector, número de nodos añadidos a cada árbol…). La sub-sección *Todas las UC* permite ver y descargar el conjunto completo de unidades de coste (presupuesto, amortizaciones, suministros y todas las variantes de nómina) tras la fase 1.

Los árboles finales (con los nodos creados dinámicamente por las reglas) se exponen en *Presupuesto → Árbol: …* y se persisten en #ruta("data/fase1/") en formato `.tree` para que la fase 2 los consuma.

== Catálogo de pantallas

La estructura del menú lateral refleja las grandes etapas. Para cada bloque, la lista de entradas y los parquets que consumen es:

=== Bloque «Entradas» (dinámico)

Refleja los ficheros de #ruta("data", "entrada"): un sub-menú por cada subdirectorio (consumos, docencia, estructuras, inventario, investigación, nóminas, presupuesto, superficies) y, dentro, una entrada por fichero (Excel o `.tree`). Cada entrada abre el contenido tabular del fichero con búsqueda y exportación.

=== Bloque «Presupuesto»

#table(
    columns: (auto, 1fr),
    stroke: 0.5pt + luma(80%),
    inset: 4pt,
    table.header(table.hline(), [*Entrada*], [*Origen*], table.hline()),
    [Resumen], [KPIs sobre #ruta("uc presupuesto.parquet") y filtros.],
    [Unidades de coste], [#ruta("uc presupuesto.parquet").],
    [Sin clasificar], [#ruta("auxiliares", "sin_clasificar_presupuesto.parquet").],
    [Apuntes filtrados], [#ruta("auxiliares", "filtrados_presupuesto.parquet"), con la regla que descartó cada apunte.],
    [Suministros], [#ruta("uc suministros.parquet") (energía, agua, gas).],
    [Distribución mantenimientos OTOP],
    [Matriz por centro de presencia para los apuntes con #campo("centro") = #val("SC001").],
    [Reglas de actividad / CC / EC],
    [#ruta("auxiliares", "conteo_reglas_presupuesto.parquet"), #ruta("conteo_cc_presupuesto.parquet"), #ruta("conteo_ec_presupuesto.parquet").],
    [Árbol: actividades / centros / elementos],
    [Árboles finales tras la traducción presupuestaria (nodos dinámicos resaltados).],
    table.hline(),
)

=== Bloque «Amortizaciones»

#table(
    columns: (auto, 1fr),
    stroke: 0.5pt + luma(80%),
    inset: 4pt,
    table.header(table.hline(), [*Entrada*], [*Origen*], table.hline()),
    [Resumen], [KPIs sobre #ruta("uc amortizaciones.parquet").],
    [Inventario con amortización], [#ruta("auxiliares", "amortizaciones", "inventario_enriquecido.parquet").],
    [Descartados / Filtrados por …],
    [Los parquets de descarte de #ruta("auxiliares", "amortizaciones") (estado, cuenta, fecha, sin cuenta, sin fecha alta).],
    [UC generadas], [#ruta("uc amortizaciones.parquet").],
    [Sin centro],
    [#ruta("auxiliares", "amortizaciones", "sin_uc.parquet") (bienes con cuenta y fecha pero sin ubicación con centro).],
    table.hline(),
)

=== Bloque «Personal»

#table(
    columns: (auto, 1fr),
    stroke: 0.5pt + luma(80%),
    inset: 4pt,
    table.header(table.hline(), [*Entrada*], [*Origen*], table.hline()),
    [Resumen], [KPIs y contadores por sector.],
    [PDI / PVI *(vista 360º)*],
    [Master por persona del sector con la métrica clave *Δ cuadre* = (bruto + SS) − UC. Detalle en cinco pestañas: *Resumen / Cuadre* (KPIs + desglose por concepto), *Relación laboral* (categoría y meses), *Nómina* (líneas crudas por expediente), *Dedicación regla 23* (reparto por grupo, totales por actividad/centro, detalle por actividad), *UC generadas* (todas las UC vinculadas con cabecera de importe). Endpoint base: #raw("/api/persona360/{PDI|PVI}/personas").],
    [Expedientes PTGAS / Otros],
    [Los parquets sectoriales (por expediente, no por persona): PTGAS no tiene regla 23, así que mantiene la vista clásica por expediente con pestañas por grupo conceptual de nómina.],
    [Multiexpediente], [#ruta("auxiliares", "nóminas", "multiexpediente.parquet").],
    [Costes sociales calculados], [#ruta("auxiliares", "nóminas", "costes_sociales_calculados.parquet").],
    [Atrasos a no vinculados],
    [#ruta("auxiliares", "nóminas", "atrasos_no_vinculados.parquet"): personas que solo cobran atrasos (CR 30/87) en el año, sin vinculación laboral activa. Su importe queda fuera del reparto y la pantalla cuantifica cuántas personas y cuánto dinero.],
    [Despidos], [#ruta("auxiliares", "nóminas", "uc_despidos.parquet").],
    [Indemnizaciones asistencias], [#ruta("auxiliares", "nóminas", "uc_indemnizaciones_asistencias.parquet").],
    [Anomalías PDI], [Subconjuntos de los parquets sectoriales con marcas de anomalía.],
    table.hline(),
)

#nota[
    *Vista 360º PDI/PVI — diseño*: el master está por #campo("per_id") (no por expediente), porque la regla 23 y el reparto de SS también lo están. Una persona se considera del sector PDI/PVI si tiene al menos un expediente del sector activo en el año; sus cifras se calculan sobre TODOS sus expedientes (PDI + cualquier otro), porque el cuadre exige que todo lo cobrado y cotizado de la persona termine en alguna UC. Las personas multisector aparecen en las dos vistas (PDI y PVI) con las mismas cifras de cuadre.

    La columna *Δ cuadre* se calcula como
    $ Delta = ("Bruto cobrado" + "SS cotizada" + "SS calculada") − (sum "UC retributivas" + sum "UC SS") $
    y debería ser #val("0") (con tolerancia #val("0,01") €) para toda persona. Cualquier descuadre es síntoma de un problema: un servicio sin mapeo, una UC duplicada, un dato faltante en los catálogos auxiliares.
]

=== Bloque «Regla 23»

#table(
    columns: (auto, 1fr),
    stroke: 0.5pt + luma(80%),
    inset: 4pt,
    table.header(table.hline(), [*Entrada*], [*Origen*], table.hline()),
    [Resumen], [KPIs de la regla 23.],
    [Dedicación docente],
    [#ruta("auxiliares", "nóminas", "regla_23_dedicación_titulaciones.parquet") + #ruta("regla_23_dedicación_estudios.parquet") (vista legacy en transición).],
    [Docencia no oficial], [#ruta("auxiliares", "nóminas", "regla_23_horas_no_oficiales.parquet").],
    [Estructura estudios], [#ruta("auxiliares", "nóminas", "regla_23_estructura_estudios.parquet").],
    [Cargos], [Vista heredada que precede a «Cargos académicos» del nuevo flujo.],
    [Asignaturas sin titulación], [#ruta("auxiliares", "nóminas", "regla_23_asignaturas_sin_titulación.parquet").],
    [Anomalías], [Anomalías de resolución de POD y desambiguación múltiple.],
    table.hline(),
)

#nota[La antigua entrada *Regla 23 · Dedicación PDI* ha sido absorbida en *Personal · PDI/PVI*. La URL antigua redirige a la nueva.]

=== Bloque «Investigación»

#table(
    columns: (auto, 1fr),
    stroke: 0.5pt + luma(80%),
    inset: 4pt,
    table.header(table.hline(), [*Entrada*], [*Origen*], table.hline()),
    [Grupos],
    [#ruta("data", "entrada", "investigación", "grupos investigación.xlsx") enriquecido con instituto (#ruta("grupos a institutos.xlsx")) y miembros (#ruta("investigadores en grupos.xlsx")). Permite ver qué grupos generaron CC.],
    table.hline(),
)

=== Bloque «Cargos académicos»

#table(
    columns: (auto, 1fr),
    stroke: 0.5pt + luma(80%),
    inset: 4pt,
    table.header(table.hline(), [*Entrada*], [*Origen*], table.hline()),
    [Resumen], [KPIs.],
    [Por persona], [#ruta("auxiliares", "nóminas", "cargos_uc.parquet") (master-detail).],
    [Personas cargos], [#ruta("data", "entrada", "nóminas", "personas cargos.xlsx") filtrado al año.],
    [Catálogo de cargos], [#ruta("data", "entrada", "nóminas", "cargos.xlsx") + RD 1086.],
    table.hline(),
)

=== Bloque «Superficies»

#table(
    columns: (auto, 1fr),
    stroke: 0.5pt + luma(80%),
    inset: 4pt,
    table.header(table.hline(), [*Entrada*], [*Origen*], table.hline()),
    [Resumen], [Superficies totales y reparto.],
    [Totales], [Por complejo, edificación y zona.],
    [Presencia centros],
    [Matriz de presencia (#ruta("auxiliares", "amortizaciones", "inventario_enriquecido.parquet")).],
    table.hline(),
)

=== Bloque «Resultados Fase 1»

#table(
    columns: (auto, 1fr),
    stroke: 0.5pt + luma(80%),
    inset: 4pt,
    table.header(table.hline(), [*Entrada*], [*Origen*], table.hline()),
    [Resumen], [KPIs por fuente con importe absoluto y porcentaje del total (12 fuentes incluida regla-23 y SS).],
    [Todas las UC],
    [Lista consolidada de todas las UC de la fase 1. Cada fila es clicable y abre un modal con la ficha completa de la UC (sus cuatro valores principales — elemento de coste, centro de coste, actividad e importe — más la traza al registro de origen: apunte presupuestario, expediente, contrato, etc.). Visión plana, sin estructura jerárquica.],
    [Actividades · Centros de coste · Elementos de coste *(jerárquicas)*],
    [Pantalla master-detail con dos zonas. *Arriba*: árbol del catálogo correspondiente con expand/collapse, búsqueda con resaltado (insensible a tildes y mayúsculas) y, por cada nodo, código, descripción, identificador, #emph[N UC subárbol] y #emph[Σ importe subárbol] (totales agregados del nodo y todos sus descendientes; los nodos con N=0 se ven atenuados pero no se ocultan). *Abajo* al seleccionar un nodo: tabla de UC imputadas *directamente* a ese nodo (clic-para-ficha igual que la vista plana) y tabla de hijos directos con N UC, importe y % sobre hermanos. Tres rutas: #ruta("/resultados/actividades"), #ruta("/resultados/centros-de-coste"), #ruta("/resultados/elementos-de-coste"). Pinchar en una fila de la tabla de hijos navega al hijo correspondiente.],
    [Anomalías UC], [UC que referencian nodos inexistentes en los árboles finales (integridad referencial).],
    table.hline(),
)


= Fase 1: Obtención de unidades de coste

En esta fase se generan unidades de coste a partir de datos extraídos de la base de datos corporativa (y, en ocasiones, de otras fuentes, como la lectura de contadores de OTOP o cálculos de distribución de costes por edificios).

Esta figura ilustra el proceso que parte de los datos de entrada (carpeta #ruta("data", "entrada"), que tiene carpetas dentro, una por cada grupo de datos hasta llegar a los de salida en la fase 1.

#figure(
    align(center, fase1-diagrama(height: 24cm)),
    placement: top,
    caption: [Proceso de Fase 1: ficheros de entrada (izquierda), transformación y salidas (derecha).],
)

La #app es la encargada de ejecutar el proceso que va desde los datos de entrada hasta los datos de salida, aplicando las reglas que se definen en esta especificación y mostrando los resultados intermedios y finales.

Queremos generar dos tablas internas:

- `unidades_de_coste_por_presupuesto`: con las unidades de coste generadas a partir de la estructura presupuestaria, con sus campos fijados en función de las reglas definidas
- `apuntes_presupuesto_sin_uc`: con los apuntes presupuestarios con los que no se ha podido generar una unidad de coste, para que el analista pueda revisar esos apuntes y refinar las reglas para asignarles un elemento de coste, centro de coste o actividad.

La #app ha de mostrar las dos tablas mediante opciones de un desplegable «Presupuesto» y permitir descargarlas en formato Excel. Además, se ha de mostrar un resumen de la información que contienen, con el número de filas y el importe total de cada una de ellas.

== Creación de centros de coste para grupos de investigación

El fichero `investigadores en grupos.xlsx` debe procesarse para quedarse solo con las filas que tienen al menos un día de actividad en el año que estamos estudiando. la ida es asociar cada `per_id` a uno o más grupo de investigación (`id_grupo`). Hemos de recordar si se trata de un coordinador (`coordinador` vale #val(`S`)) y si es interlocutor (`interlocutor` vale #val(`S`)), porque eso puede ser relevante para asignar el centro de coste correspondiente a cada unidad de coste que se genere a partir de un gasto con ese `per_id`.

El nombre de los grupos de investigación se puede extrar de `grupos investigación.xlsx`.

Los grupos de investigación con 0 personas, fuera. Los institutos de investigación, fuera.

Crea un instituto de investigación virtual llamado `INVES`. Como sabemos a qué instituto (o INVES) pertenece cada grupo, hemos de enriquecer el árbol de centros de coste creando un centro de coste con el nombre del grupo de investigación debajo de cada instituto. La etiqueta del centro de coste será `grupo-investigación-XXX` donde `XXX` es el `id_grupo` del grupo de investigación.

En la #app, esos centros que hemos construido dinámicamente han de mostrarse en un color distinto.

De momento, en la #app, en la sección de Investigación,  quiero ver el listado de grupos y para cada grupo quiero ver las personas que forman parte de él, destacando en primer lugar al interlocutor, en segundo lugar a  los coordinadores distintos del interlocutor y luego al resto de miembros.

#nota[Falta una tabla que dice de cada proyectos de investigación: su IP, su código interno presupuestario, el grupo de investigación del IP (identificador del grupo).]

#nota[Ojo con gastos que están en proyectos generales, como 19I001, y que, por su naturaleza, deben asociarse a un grupo. Por ejemplo, una predoc UJI que tiene un beneficiario y deberíamos llegar al grupo de investigación a través suyo. Si no está en el censo del grupo: problema. Quizá información de su tutor permita establecer la asociación.]



== Preparación de un módulo para clasificar actividades

Tanto en presupuesto como en nóminas necesitaré establecer la actividad a la que un registro asocia el gasto. Vamos a preparar un módulo que permita clasificar actividades a partir de reglas. Usará campos que tenemos tanto en presupuesto como en nóminas. En particular, el capítulo del gasto, su aplicación, su proyecto, el centro y subcentro... y puede haber más. Algunas de las actividades ya existirán en árbol de actividades y otras se crearán a partir de las reglas.

El árbol que hemos de leer *y editar* para determinar actividades es el de actividades, que se encuentra en #ruta("data", "entrada", "estructuras", "actividades.tree").

El árbol de actividades modificado por las reglas se ha de mostrar en la #app, con una opción de un desplegable #val("Tras fase 1") para mostrarlo o descargarlo en formato `.tree`. Además, se ha de mostrar un resumen de la información que contiene, con el número de filas y el importe total de cada una de ellas. Los nodos añadidos se han de mostrar de un color distinto y se ha de indicar cuantos nodos se han añadido.

=== Conceptos previos

Sean estos los #campo("tipo de proyecto") de investigación y transferencia:

#align(center, table(
    columns: 2,
    align: (left, left),

    table.header(table.hline(), campo("tipo de proyecto"), [#strong[Descripción]], table.hline()),

    val("0000I"), [TIPO GENERAL DE PROYECTO DE INV.],
    val("000I"), [DESPESES GENERALS INVESTIGACIÓ],
    val("06I"), [PROJECTES INVESTIGACIÓ UJI VARIOS],
    val("A11I"), [ASISTENCIA TÈCNICA ART 60 LOSU (ART 83 LOU)],
    val("A1TI"), [CONTRACTES ART 60 LOSU (ART 83 LOU)],
    val("A83CA"), [CATEDRA/AULAEMPRESA],
    val("BECI"), [BEQUES INVESTIGACIÓ],
    val("CA"), [CÀTEDRA/AULA EMPRESA],
    val("COBEI"), [CONTRACTES BECARIS MEC],
    val("COF"), [COFINANÇAMENT BEQUES I CONTRACTES INVESTIGACIÓ],
    val("CONI"), [CURSOS I CONGRESSOS],
    val("CONVI"), [CONVENI INVESTIGACIO],
    val("DIPI"), [DIPUTACIÓ],
    val("FGVI"), [FEDER GV],
    val("GVI"), [GENERALITAT VALENCIANA],
    val("IDI"), [LÍNIA I+D+I],
    val("MCTFE"), [ PROYECTOS MCT/FEDER],
    val("MCTI"), [MINISTERIO DE CIENCIA Y TECNOLOGIA],
    val("MEC"), [MINISTERIO DE ECONOMIA Y COMPETITIVIDAD],
    val("MECD"), [MINISTERIO DE EDUCACION, CULTURA Y DEPORTE],
    val("MECI"), [MINISTERIO DE EDUCACION Y CIENCIA],
    val("MIE"), [MINISTERIO DE EDUCACIÓN],
    val("MPEI"), [MINISTERIO DE LA PRESIDENCIA],
    val("MSI"), [MINISTERIO DE SANIDAD],
    val("MSP"), [MINISTERIO DE SANIDAD, POLÍTICA SOCIAL E IGUALDAD],
    val("MTAI"), [MINISTERIO DE TRABAJO Y ASUNTOS SOCIALES],
    val("MTD"), [MINISTERIO DE TRANSFORMACION DIGITAL],
    val("PCT"), [PATENTES],
    val("PII"), [PLA PROPI INVESTIGACIÓ],
    val("PPSI"), [PROJECTE PERSONAL SUPORT A LA INVESTIGACIÓ],
    val("UEI"), [UNIÓN EUROPEA],
    table.hline(),
))

Sean estos los #campo("tipo de proyecto") de másteres oficiales:

#align(center, table(
    columns: 2,
    align: (left, left),

    table.header(table.hline(), campo("tipo de proyecto"), [#strong[Descripción]], table.hline()),
    table.hline(),
    val("MO"), [MASTER OFICIAL],
    val("MO06"), [MÀSTERS OFICIALS CURS 06/07],
    val("MO07"), [MÀSTERS OFICIALS CURS 07/08],
    val("MO08"), [MÀSTERS OFICIALS CURS 08/09],
    val("MO09"), [MÀSTERS OFICIALS CURS 2009/2010],
    val("MO10"), [MÀSTERS OFICIALS CURS 2010/2011],
    val("MO11"), [MÀSTERS OFICIALS 2011/2012],
    val("MO12"), [MÀSTERS OFICIALS 2012/2013],
    val("MO14"), [MASTERS OFICIALES CURSO 2014/2015],
    val("MO15"), [MÀSTERS OFICIAL CURSO 2015/2016],
    table.hline(),
))

La siguiente tabla, a la que llamamos TABLA-TRADUCCIÓN-DEPARTAMENTOS, contiene las traducciones de #campo("centro") según la codificación del presupuesto a nuestra nomenclatura para las reglas en las que haya etiquetas de la forma #val("prefijo-DEPARTAMENTO-sufijo") (donde prefijo y sufijo son opcionales):

#align(center, table(
    columns: 2,
    align: (left, left),

    table.header(table.hline(), [#campo("centro")], [DEPARTAMENTO], table.hline()),

    val("DADEM"), etqcen("daem"),
    val("DCAMN"), etqcen("dbbcn"),
    val("DCICO"), etqcen("dcc"),
    val("DDPRI"), etqcen("ddpri"),
    val("DDPUB"), etqcen("ddpub"),
    val("DDTSE"), etqcen("updtssee"),
    val("DEANG"), etqcen("dea"),
    val("DECIC"), etqcen("dicc"),
    val("DECON"), etqcen("deco"),
    val("DEDES"), etqcen("dede"),
    val("DEMEC"), etqcen("dmc"),
    val("DESID"), etqcen("desid"),
    val("DFICE"), etqcen("dfs"),
    val("DFICO"), etqcen("dfc"),
    val("DFISI"), etqcen("dfis"),
    val("DFISO"), etqcen("dfce"),
    val("DHIST"), etqcen("dhga"),
    val("DINFE"), etqcen("upi"),
    val("DIQUI"), etqcen("deq"),
    val("DLSIN"), etqcen("dlsi"),
    val("DMATE"), etqcen("dmat"),
    val("DMEDI"), etqcen("upm"),
    val("DPDID"), etqcen("dpdcsll"),
    val("DPSIB"), etqcen("dpbcp"),
    val("DPSIE"), etqcen("dpeesm"),
    val("DQFIA"), etqcen("dqfa"),
    val("DQUIO"), etqcen("dqio"),
    val("DTRAD"), etqcen("dtc"),

    table.hline(),
))

Esta tabla, a la que llamamos TABLA-TRADUCCIÓN-VICES, contiene las traducciones de #campo("subcentro") a nuestra nomenclatura para las reglas en las que haya etiquetas de la forma #val("prefijo-VICE-sufijo") (donde prefijo y sufijo son opcionales):

#align(center, table(
    columns: 2,
    align: (left, left),

    table.header(table.hline(), [#campo("subcentro")], [VICE], table.hline()),

    val("VCL"), etqcen("vcls"),
    val("VEV"), etqcen("vevs"),
    val("VI"), etqcen("vi"),
    val("VIN"), etqcen("vis"),
    val("VTD"), etqcen("vitdc"),
    val("VA"), etqcen("voap"),
    val("VPE"), etqcen("vpee"),
    val("VRI"), etqcen("vri"),
    val("VRS"), etqcen("vrspii"),
    val("VEF"), etqcen("vefp"),
    val("R10"), etqcen("delegado"),

    table.hline(),
))

=== Gastos del #campo("capítulo") 4 (ayudas) en un #campo("programa") distinto del #val("541-A")

Vamos con las reglas de esta sección:

#reglas[
    - #nombre-regla[Becas]
        Si el #campo("capítulo") es 4
        - y el #campo("tipo de proyecto") no es de investigación y transferencia
            - #nombre-regla[Becas genéricas]
                y el #campo("proyecto") = #val("00000"), la actividad es #etqact("ayudas-genéricas-estudiantes").
            - #nombre-regla[Becas de proyectos]
                en otro caso, la actividad es #etqact("otras-ayudas-estudiantes") + #campo("proyecto").
        - y el #campo("tipo de proyecto") es de investigación y transferencia pero el #campo("programa") no es #val("541-A"), la actividad es #etqact("otras-ayudas-estudiantes") + #campo("proyecto").
]

=== Costes en proyectos de Investigación y transferencia

#reglas[
    - #nombre-regla[Proyectos de investigación, artículos 60, cátedras, patentes y líneas]
        Si el #campo("tipo de proyecto") es #val("0000I"), #val("A11I"), #val("A1TI"), #val("A83CA"), #val("CA"), #val("PCT") o #val("IDI")

        - #nombre-regla[Plan propio]
            y el #campo("tipo de línea de financiación") es #val("00"), la actividad es #etqact("ait-plan-propio") + #campo("proyecto").
        - #nombre-regla[Cátedras y aulas]
            si la #campo("descripción") del #campo("proyecto") concuerda en una subcadena con la expresión regular #val("C.TEDRA") o #val("AULA EMPRESA") (_case insensitive_), la actividad es #etqact("cátedras-aulas-empresa") + #campo("proyecto").
        - #nombre-regla[Artículos 60]
            en otro caso, la actividad es #etqact("transf-60") + #campo("proyecto").

    - #nombre-regla[Investigación regional]
        Si el #campo("tipo de proyecto") es #val("DIPI"), #val("FGVI") o #val("GVI"), y #campo("programa") = #val("541-A") y #campo("tipo de línea de financiación") no es #val("00"), la actividad es #etqact("ai-regional") + #campo("proyecto").

    - #nombre-regla[Investigación nacional]
        Si #campo("tipo de proyecto") es #val("06I"), #val("COBEI"), #val("MCTFE"), #val("MCTI"), #val("MEC"), #val("MECD"), #val("MECI"), #val("MIE"), #val("MIG"), #val("MPEI"), #val("MSI"), #val("MSP"), #val("MTAI") o #val("MTD"), y #campo("programa") = #val("541-A"), y #campo("tipo de línea de financiación") no empieza por #val("00"), la actividad es #etqact("ai-nacional") + #campo("proyecto").

    - #nombre-regla[Investigación internacional]
        Si #campo("tipo de proyecto") #val("UEI") o #val("UEGD"), y #campo("programa") = #val("541-A"), y #campo("tipo de línea de financiación") no es #val("00"), la actividad es #etqact("ai-internacional") + #campo("proyecto").

    - #nombre-regla[Otra investigación competitiva]
        Si #campo("tipo de proyecto") #val("BECI"), #val("CONI"), #val("CONVI"), y #campo("programa") = #val("541-A"), y #campo("tipo de línea de financiación") no es #val("00"), la actividad es #etqact("ai-otras-competitivas") + #campo("proyecto").

    - #nombre-regla[Proyectos de investigación que pueden llevar co-financiación]
        Si el #campo("tipo de proyecto") es #val("000I"), #val("06I"), #val("BECI"), #val("COBEI"), #val("COF"), #val("CONI"), #val("CONVI"), #val("DIPI"), #val("FGVI"), #val("GVI"), #val("MCTFE"), #val("MCTI"), #val("MEC"), #val("MECD"), #val("MECI"), #val("MIE"), #val("MIG"), #val("MPEI"), #val("MSI"), #val("MSP"), #val("MTAI"), #val("MTD"), #val("PII"), #val("PPSI"), #val("UEI") o #val("UEGD"), y el #campo("programa") = #val("541-A")

        - #nombre-regla[UJI]
            y el #campo("tipo de línea de financiación") es #val("00"),
            - #nombre-regla[PPSI]
                si #campo("descripción de proyecto") contiene #val("PPSI"), la actividad es #etqact("ppsi") + #campo("proyecto").
            - #nombre-regla[Plan propio]
                si #campo("descripción de proyecto") contiene #val("UJI-") o #val("GACUJIMA"), la actividad es #etqact("ait-plan-propio") + #campo("proyecto").
            - #nombre-regla[Otros]
                si no, la actividad es #etqact("otras-ait-financiación-propia") + #campo("proyecto").
        - #nombre-regla[Externa]
            en otro caso, la actividad es #etqact("ait-financiación-externa") + #campo("proyecto").

    - #nombre-regla[Proyectos con financiación propia]
        Si #campo("tipo de proyecto") = #val("000TR"), y #campo("programa") = #val("541-A"), y #campo("tipo de línea de financiación") es #val("00"), la actividad es #etqact("ait-financiación-propia") + #campo("proyecto").

    - #nombre-regla[Doctorado]
        Si #campo("tipo de proyecto") = #val("DOCT"), y #campo("programa") = #val("541-A"), la actividad es #etqact("doctorado") + #campo("proyecto").

    - #nombre-regla[Doctorado: acreditación de los programas]
        Si #campo("proyecto") = #val("19I005"), la actividad es #etqact("doctorado").

    - #nombre-regla[Internacional]
        Si #campo("tipo de proyecto") #val("05G"), y #campo("tipo de línea de financiación") no es #val("00"), la actividad es #etqact("ai-internacional").
]


=== Distinto de investigación y transferencia

Sean los #campo("subcentro") de vicerrectorados: #val("VA"), #val("VCL"), #val("VEF"), #val("VEV"), #val("VI"), #val("VIN"), #val("VPE"), #val("VRI"), #val("VRS"), #val("VTD").

#reglas[
    - #nombre-regla("No son ayudas")
        Si #campo("capítulo") <> #val("4")

        - #nombre-regla[Encargos]
            y el #campo("proyecto") es #val("23G010"), hay que crear varias unidades de coste que se reparten el importe y se asignan a actividades diferentes, según esta tabla, en la que el SUB #campo("proyecto") se ha de tener en cuenta para tomar la decisión:

            #align(center, table(
                columns: 3,
                align: (center, right, left),

                table.header(
                    table.hline(), campo("subproyecto"), campo("porcentaje"), campo("actividad"), table.hline()
                ),

                val("03"), [10.0630%], etqact("dag-encargos-gestión-estudios-propios"),
                val("03"), [19.0887%], etqact("máster-igualdad-género"),
                val("03"), [36.6774%], etqact("dag-otros-servicios-promoción-fomento-igualdad"),
                val("03"), [34.1709%], etqact("dag-otros-servicios-promoción-fomento-igualdad"),
                val("02"), [38.8486%], etqact("dag-encargos-gestión-estudios-propios"),
                val("02"), [9.5053%], etqact("dag-encargos-gestión"),
                val("02"), [9.5053%], etqact("dag-encargos-gestión-estudios-propios"),
                val("02"), [5.1648%], etqact("dag-encargos-gestión-proyectos-internacionales"),
                val("02"), [5.6195%], etqact("dag-encargos-proyectos-investigación-europeos"),
                val("02"), [14.9992%], etqact("dag-encargos-gestión-transferencia"),
                val("02"), [12.4470%], etqact("dag-encargos-gestión-transferencia"),
                val("02"), [3.9104%], etqact("dag-otros-servicios-relaciones-internacionales"),
                val("01"), [46.6979%], etqact("dag-encargos-gestión-transferencia"),
                val("01"), [10.6870%], etqact("dag-apoyo-estudiantes"),
                val("01"), [9.7964%], etqact("máster-traducción-medicina"),
                val("01"), [28.8112%], etqact("escola-estiu"),
                val("01"), [4.0076%], etqact("dag-otros-servicios-comunicación-publicaciones"),
                table.hline(),
            ))

        - #nombre-regla[Másteres oficiales]
            y #campo("tipo de proyecto") es de máster oficial, la actividad depende del #campo("proyecto"):

            #align(center, table(
                columns: 3,
                align: (left, left, left),

                table.header(
                    table.hline(), campo("proyecto"), campo("actividad"), [Descripción del proyecto], table.hline()
                ),

                val("09G010"),
                etqact("dag-vefp"),
                [PLA DE FINANÇAMENT ALS MÀSTERS UNIVERSITARIS],

                val("07G067"),
                etqact("máster-márqueting"),
                [MÀRQUETING I INVESTIGACIÓ DE MERCATS],

                val("07G069"),
                etqact("máster-rsc"),
                [SOSTENIBILITAT I RESPONSABILITAT SOCIAL CORPORATIVA],

                val("07G071"),
                etqact("máster-cooperación-desarrollo"),
                [COOPERACIÓ AL DESENVOLUPAMENT],

                val("07G073"),
                etqact("máster-traducción-medicina"),
                [TRADUCCIÓ MEDICOSANITÀRIA],

                val("07G075"),
                etqact("máster-mediación-familiar"),
                [INTERVENCIÓ I MEDIACIÓ FAMILIAR],

                val("07G077"),
                etqact("máster-psicología-trabajo"),
                [PSICOLOGIA DEL TREBALL, DE LES ORGANITZACIONS I EN RECURSOS HUMANS],

                val("07G079"),
                etqact("máster-innovación-comunicación"),
                [NOVES TENDÈNCIES I PROCESSOS D'INNOVACIÓ EN COMUNICACIÓ],

                val("07G081"),
                etqact("máster-diseño-fabricación"),
                [DISSENY I FABRICACIÓ],

                val("07G082"),
                etqact("máster-eficiencia-energética"),
                [EFICIÈNCIA ENERGÈTICA I SOSTENIBILITAT EN INSTAL·LACIONS I EDIFICACIÓ],

                val("07G083"),
                etqact("máster-riesgos-laborales"),
                [PREVENCIÓ DE RISCOS LABORALS],

                val("07G084"),
                etqact("máster-química-sostenible"),
                [],

                val("07G085"),
                etqact("máster-química-cromatográfica"),
                [TÈCNIQUES CROMATROGRÀFIQUES APLICADES],

                val("07G093"),
                etqact("máster-igualdad-género"),
                [IGUALTAT DE GÈNERE EN L'ÀMBIT PÚBLIC I PRIVAT],

                val("14G020"),
                etqact("máster-dirección-empresas"),
                [MASTER OF BUSINESS ADMINISTRATION],

                val("14G109"),
                etqact("máster-ingeniería-industrial"),
                [MÀSTER UNIVERSITARI EN ENGINYERIA INDUSTRIAL],

                val("14G110"),
                etqact("máster-psicologia-sanitaria"),
                [MÀSTER UNIVERSITARI EN PSICOLOGIA GENERAL SANITÀRIA],

                val("15G059"),
                etqact("máster-dirección-empresas"),
                [MÁSTER UNIVERSITARI EN DIRECCIÓ D'EMPRESES/MASTER IN MANAGEMENT],

                val("15G061"),
                etqact("máster-cerebro"),
                [MÀSTER UNIVERSITARI EN INVESTIGACIÓ EN CERVELL I CONDUCTA],

                val("22G138"),
                etqact("máster-enfermería-urgencias"),
                [MÀSTER UNIVERSITARI EN INFERMERIA D'URGÈNCIES, EMERGÈNCIES I CURES CRÍTIQUES],

                val("08G170"),
                etqact("máster-historia-mediterráneo"),
                [HISTÓRIA I IDENTITATS HISPÀNIQUES EN LA MEDITERRÀNIA OCCIDENTAL],

                val("08G175"),
                etqact("máster-comunicación-intercultural"),
                [COMUNICACIÓN INTERCULTURAL Y DE ENSEÑANZA DE LENGUAS],

                val("08G178"),
                etqact("máster-inglés-multilingüe"),
                [ENSENYAMENT I ADQUISICIÓ DE LA LLENGUA ANGLESA EN CONTEXTOS MULTILINGÜES],

                val("09G046"),
                etqact("máster-gestión-financiera"),
                [MÀSTER EN GESTIÓ FINANCERA I COMPTABILITAT AVANÇADA],

                val("09G078"),
                etqact("máster-paz"),
                [ESTUDIS DE LA PAU, CONFLICTES I DESENVOLUPAMENT],

                val("09G109"),
                etqact("máster-secundaria"),
                [MÀSTER PROFESSOR/A D'EDUCACIÓ SECUNDÀRIA OBLIGATÒRIA I BATXILLERAT, FORMACIÓ PROFESSIONAL I ENSENYAMENT D'IDIOMES],

                val("10G055"),
                etqact("máster-penal"),
                [MÀSTER OFICIAL SISTEMA DE JUSTÍCIA PENAL],

                val("10G056"),
                etqact("máster-historia-arte"),
                [MÀSTER OFICIAL HISTÒRIA DE L'ART I CULTURA VISUAL],

                val("10G057"),
                etqact("máster-inglés-comercio"),
                [MÀSTER OFICIAL LLENGUA ANGLESA PER AL COMERÇ INTERNACIONAL (E.L.I.T)],

                val("10G058"),
                etqact("máster-género"),
                [MÀSTER OFICIAL INVESTIGACIÓ I DOCÈNCIA EN ESTUDIS FEMINISTES, DE GÈNERE I CIUTADANIA],

                val("10G120"),
                etqact("máster-traducción"),
                [MÀSTER OFICIAL INVESTIGACIÓ EN TRADUCCIÓ I INTERPRETACIÓ],

                val("10G123"),
                etqact("máster-química-aplicada"),
                [MÀSTER OFICIAL QUÍMICA APLICADA I FARMACOLÒGICA],

                val("12G103"),
                etqact("máster-psicopedagogía"),
                [MÁSTER UNIVERSITARIO EN PSICOPEDAGOGÍA],

                val("12G104"),
                etqact("máster-salud-mental"),
                [MÁSTER UNIVERSITARIO EN REHABILITACIÓN PSICOSOCIAL EN SALUD MENTAL COMUNITARIA],

                val("13G001"),
                etqact("máster-abogacía"),
                [MÀSTER UNIVERSITARI EN ADVOCACIA I PROCURA],

                table.hline(),
            ))

        - #nombre-regla[Proyectos singulares]
            Esta tabla recoge una serie de #campo("proyecto") que dan origen a actividades cuya etiqueta se forma con un prefijo y el código de proyecto:

            #align(center, table(
                columns: 3,
                align: (left, left, left),

                table.header(table.hline(), campo("proyecto"), [actividad], [Descripción del proyecto], table.hline()),

                val("23I373"),
                etqact("ai-nacional-23I373"),
                [CONVENIO COLABORACIÓN UJI-FAECC - FUNDACIÓN CIENTÍFICA DE LA ASOCIACIÓN ESPAÑOLA CONTRA EL CÁNCER],

                val("21I321"),
                etqact("ai-internacional-21I321"),
                [GLOBAL MEDIA AND INTERNET CONCENTRATION PROJECT],

                val("9I113"),
                etqact("cooperación-9I113"),
                [PROYECTO URBAN-CASTELLO],

                val("24G002"),
                etqact("cooperación-24G002"),
                [PROGRAMA DE DIVERSITAT ÈTNICA-CULTURAL],

                val("25G032"),
                etqact("grado-medicina"),
                [SUBVENCIONES A UNIVERSIDADES PÚBLICAS FINANCIACIÓN INCREMENTO PLAZAS GRADO MEDICINA Y INVERSIONES DESTINADAS A MEJORAR LA CALIDAD DOCENTE CURSO 2024-2025. RD 874/2024],

                val("06G008"),
                etqact("acceso-enseñanzas-oficiales"),
                [COORDINACIÓ PROVES ACCÉS MAJORS],

                val("1G041"),
                etqact("acceso-enseñanzas-oficiales"),
                [COORDINACIÓ ACCÉS UNIVERSITAT (PAU)],

                val("16G095"),
                etqact("universidad-mayores"),
                [PROMOCIÓ DELS PROJECTES EUROPEUS IP. R. ESTELLER],

                val("23I235"),
                etqact("ait-financiación-propia"),
                [GESTIÓ DE SERVEIS CIENTIFICOTÈCNICS DE RECOLZAMENT A LA INVESTIGACIÓ],

                val("24I351"),
                etqact("ait-financiación-propia"),
                [GESTIÓ DE SERVEIS CIENTIFICOTÈCNICS DE RECOLZAMENT A LA INVESTIGACIÓ - PTA INAM],

                val("24I352"),
                etqact("ait-financiación-propia"),
                [GESTIÓ DE SERVEIS CIENTIFICOTÈCNICS DE RECOLZAMENT A LA TRANSFERÈNCIA I LA INNOVACIÓ],

                val("1I235"),
                etqact("cátedras-aulas-empresa"),
                [CÀTEDRES],

                val("22G023"),
                etqact("dag-biblioteca"),
                [COMPRA DE BIBLIOGRAFIA],

                val("22G025"),
                etqact("dag-biblioteca"),
                [MANTENIMENT, SUPORT I PROGRAMES],

                val("22G013"),
                etqact("dag-otros-servicios-ti"),
                [DESENVOLUPAMENT DE PROGRAMARI CORPORATIU],

                val("04G117"),
                etqact("dag-deportes"),
                [USO DE INSTALACIONES DEPORTIVAS],

                val("23G011"),
                etqact("deportes"),
                [ACTIVITAT FÍSICA I SALUT],

                val("18G048"),
                etqact("cultura"),
                [ACTIVITATS CULTURALS LLOTJA DEL CÀNEM I MENADOR ESPAI CULTURAL],

                val("8G015"),
                etqact("cultura"),
                [CAMPUS OBERT],

                val("08G023"),
                etqact("dag-rectorado"),
                [ACTOS ACADÉMICOS],

                val("23G012"),
                etqact("dag-otros-servicios-comunicación-publicaciones"),
                [PLA DE COMUNICACIÓ],

                val("22G019"),
                etqact("dag-voap"),
                [CONCURSOS PDI],

                val("19G006"),
                etqact("dag-vevs"),
                [UJI HÀBITAT SALUDABLE],

                val("22G020"),
                etqact("dag-vevs"),
                [OFICINA ATENCIÓ ASSOCIACIONS],

                val("23G058"),
                etqact("dag-vri"),
                [EUROPEAN DIGITAL UNIVERCITY - BUILDING THE BRIDGING ALLIANCE],

                val("23G001"),
                etqact("dag-cultura"),
                [CATALOGACIÓ I ORDENACIÓ DEL PATRIMONI I EL FONS ARTÍSTIC],

                val("19G007"),
                etqact("dag-vis"),
                [EQUIPAMENT LABORATORIS DOCENTS I PROGRAMARI DOCÈNCIA],

                val("16G071"),
                etqact("dag-gerencia"),
                [FONS REPOSICIÓ],

                val("25G134"),
                etqact("dag-consejo-estudiantes"),
                [SUBVENCIÓN UNIVERSIDADES PÚBLICAS DE LA CV FOMENTO DE CONGRESOS Y ENCUENTROS DE ÁMBITO NACIONAL ESTUDIANTADO 2025],

                val("05G006"),
                etqact("dag-org-vicerrectorados-transportes-comunicaciones"),
                [TELECOMUNICACIONS CORPORATIVES],

                val("07G001"),
                etqact("dag-otros-servicios-relaciones-internacionales"),
                [Internacionalització de l'UJI],

                val("22G014"),
                etqact("dag-otros-servicios-comunicación-publicaciones"),
                [EDITORIAL PUBLICACIONS UJI],

                val("22G015"),
                etqact("dag-otros-servicios-comunicación-publicaciones"),
                [DESTÍ UJI],

                val("22G016"),
                etqact("dag-otros-servicios-comunicación-publicaciones"),
                [PUBLICITAT],

                val("22G017"),
                etqact("dag-otros-servicios-comunicación-publicaciones"),
                [COMUNICACIÓ DE L'ACTIVITAT ACADÈMICA, INVESTIGADORA I CULTURAL],

                val("22G018"),
                etqact("dag-otros-servicios-comunicación-publicaciones"),
                [MÀRQUETING I PROMOCIÓ DE LA UNIVERSITAT],

                val("24G107"),
                etqact("dag-otros-servicios-comunicación-publicaciones"),
                [COMUNICACIÓ DIGITAL],

                val("23G002"),
                etqact("dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico"),
                [FORMACIÓ EN LLENGÜES],

                val("23G003"),
                etqact("dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico"),
                [TRADUCCIÓ DE LLENGÜES],

                val("23G004"),
                etqact("dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico"),
                [PROMOCIÓ DE LLENGÜES],

                val("23G007"),
                etqact("dag-gerencia"),
                [CANON PROPIETAT INTELECTUAL],

                val("04G016"),
                etqact("dag-otros-servicios-ti"),
                [ESTUDIS, PROSPECTIVA I INNOVACIÓ TECNOLOGICA],

                val("22G013"),
                etqact("dag-otros-servicios-ti"),
                [DESENVOLUPAMENT DE PROGRAMARI CORPORATIU],

                val("04G007"),
                etqact("dag-otros-servicios-promoción-evaluación-calidad"),
                [FORMACIÓ DIRECTIVA PDI],

                val("19G002"),
                etqact("dag-otros-servicios-relaciones-internacionales"),
                [PROGRAMES DE MOBILITAT DE PERSONAL],

                val("1G033"),
                etqact("dag-otros-servicios-ti"),
                [SEGURETAT I AUDITORIA SISTEMES D'INFORMACIÓ],

                val("19G012"),
                etqact("dag-vis"),
                [PAD],

                val("25G080"),
                etqact("dag-otros-servicios-relaciones-internacionales"),
                [ESTADES DOCENTS BREUS PROFESSORAT VISITANT ESTRANGER CURS 2025/2026],

                val("25G058"),
                etqact("ámbito-filología"),
                [PROGRAMARI ÚS DOCENT PER A TRADUCCIÓ I COMUNICACIÓ],

                val("25G059"),
                etqact("ámbito-industrial"),
                [PROGRAMARI ÚS DOCENT PER A ENGINYERIA DE SISTEMES INDUSTRIALS I DISSENY],

                val("25G060"),
                etqact("ámbito-industrial"),
                [PROGRAMARI ÚS DOCENT PER A ENGINYERIA DE SISTEMES INDUSTRIALS I DISSENY],

                val("25G061"),
                etqact("ámbito-industrial"),
                [PROGRAMARI ÚS DOCENT PER A ENGINYERIA DE SISTEMES INDUSTRIALS I DISSENY],

                val("22G026"),
                etqact("dag-vi"),
                [QUOTES I CÀNONS],

                val("1G021"),
                etqact("dag-sgde"),
                [TÍTOLS],

                val("07G011"),
                etqact("dag-vefp"),
                [Harmonització Europea],

                val("22G021"),
                etqact("dag-oipep"),
                [INSERCIÓ PROFESSIONAL],

                val("24G112"),
                etqact("microcredenciales"),
                [PLA PER AL DESENVOLUPAMENT DE MICROCREDENCIALS UNIVERSITÀRIES],

                val("8G055"),
                etqact("acción-sindical"),
                [DESPESES CENTRALS SINDICALS],

                table.hline(),
            ))

        - #nombre-regla[Erasmus y similares]
            Estos proyectos se asignan a las siguientes actividades:

            #align(center, table(
                columns: 3,
                align: (left, left, left),

                table.header(
                    table.hline(), campo("proyecto"), campo("actividad"), [Descripción del proyecto], table.hline()
                ),

                val("23G057"),
                [#etqact("universidad-mayores") + #campo("proyecto")],
                [LIVAI-MAKING ADULT EDUCATION LIVELY THROUGH ARTIFICIAL INTELLIGENCE],

                val("23G156"),
                [#etqact("universidad-mayores") + #campo("proyecto")],
                [INTERGENIC-SUPPORTING EU'S TWIN TRANSITIONS THROUGH INTERGENERATIONAL LEARNING, EXCHANGES OF KNOWLEDGE AND JOINT ACTIONS],

                val("24G137"),
                [#etqact("universidad-mayores") + #campo("proyecto")],
                [CONVENI ERASMUS+ CONSORCI 2024-1-ES01-KA121-ADU-00020559 MOBILITAT PER ESTUDIANTAT UNIVERSITAT DE MAJORS I STAFF DE LA UM],

                val("23G121"),
                [#etqact("dag-otros-servicios-relaciones-internacionales") + #campo("proyecto")],
                [2023-1-ES01-KA131-HED-000116313 PROGRAMA ERASMUS+2023, MOVILIDAD EDUCATIVA DE LAS PERSONAS],

                val("23G131"),
                [#etqact("dag-otros-servicios-relaciones-internacionales") + #campo("proyecto")],
                [2023-1-ES01-KA131-HED-000116976 PROGRAMA ERASMUS+, MOVILIDAD EDUCATIVA DE LAS PERSONAS],

                val("25G036"),
                [#etqact("dag-otros-servicios-relaciones-internacionales") + #campo("proyecto")],
                [ESTADES DOCENTS ERASMUS + MOBILITAT SUÏSSA I EL REGNE UNIT PER A PERSONAL UJI 2025],

                val("25G037"),
                [#etqact("dag-otros-servicios-relaciones-internacionales") + #campo("proyecto")],
                [ESTADES FORMATIVES ERASMUS + MOBILITAT SUÏSSA I EL REGNE UNIT PER A PERSONAL UJI 2025],

                val("22G097"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [SAMEUROPE-STUDENT ATHLETES ERASMUS+MOPBILITY IN EUROPE],

                val("23G038"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [DIGIUGOV-DIGITALIZATION MEETS UNIVERSITY GOVERNANCE],

                val("23G117"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [MENTALHIGH-BUILDING MENTAL HEALTH CAPACITY AT HIGHER EDUCATION INSTITUTES IN SOUTHEAST ASIA],

                val("23G143"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [CONVENI ERASMUS + 2023-1-ES01-KA171-HED-000141833],

                val("23G154"), [#etqact("ai-internacional") + #campo("proyecto")], [DISEDER-EU],

                val("23G155"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [OSCAR-PROMOTING CROSS-CUTTING DIGITAL SKILLS THROUGHT EUROPE-WIDE NON-CONVENTIONAL LEARNING EXPERIENCES],

                val("23G157"), [#etqact("ai-internacional") + #campo("proyecto")], [SURF-SUSTAINABLE RURAL FUTURE],

                val("24G003"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [CONVENI ERASMUS+ UJI 2024-1-ES01-KA131-HED-000238667],

                val("24G011"), [#etqact("ai-internacional") + #campo("proyecto")], [METAVERSE ACADEMY],

                val("24G016"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [SPACESUITE-SPACE DOWNSTREAM SKILLS DEVELOPMENT AND USER UPTAKE THROUGHT INNOVATIVE CURRICULA IN TRAINING AND EDUCATION],

                val("24G041"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [SHAKE-SHARING HEAT AND KNOWLEDGE ON ENERGY COMMUNITIES],

                val("24G046"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [EDU-VERSE; VET LEARNERS AND VET TRAINERS COLLABORATE IN CREATING THE LEARNING VERSE],

                val("24G084"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [ARCHEOPOLIS; FACE TO FACE WITH PRACTICAL ARCHAEOLOGICAL TRAINING IN HIGHER EDUCATION],

                val("24G110"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [EUTELL - CONSTRUCTING EUROPEAN IDENTITIES THROUGH LINGUISTIC, LITERARY AND CULTURAL DISCOURSES IN ENGLISH],

                val("24G126"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [SEEN: SOCIAL ENTREPRENEURSHIP ECOSYSTEMS NETWORK],

                val("25G033"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [INNERWORLDS: A VIDEOGAME JOURNEY THROUGH TEEN DEPRESSION AND SOCIAL PRESSURES],

                val("25G063"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [AYUDAS PARA LA COORDINACIÓN DE PROGRAMAS INTENSIVOS COMBINADOS BIP ERASMUS + 2021-2027 ACCIÓN KA-131],

                val("25G064"),
                [#etqact("ai-internacional") + #campo("proyecto")],
                [ERASMUS-EDU-2024-CBHE - NANOMER: : NANOSCIENCES WITH LATIN AMERICA: SHARING KNOWLEDGE THROUGH PEDAGOGICAL INNOVATION],

                val("21I321"), [#etqact("ai-internacional") + #campo("proyecto")], [],

                val("23I373"), [#etqact("ai-nacional") + #campo("proyecto")], [],
            ))

        - #nombre-regla[Delegado de la rectora]
            y #campo("tipo de proyecto") = #val("26G") o #val("16G"), y #campo("subcentro") = #val("R10"), la actividad es #etqact("dag-delegado")

        - #nombre-regla[Másteres oficiales y otros VEFP]
            y #campo("tipo de proyecto") es #val("QMG"), #val("EST"), #val("21G"), #val("20G") o uno de los de másteres oficiales,

            - y #campo("centro") = #val("VEPF") y #campo("subcentro") es #val("VEF") o #val("MA"), la actividad es #etqact("dag-vefp")
            - #nombre-regla[DAG Vicerrectorados]
                si no, el #campo("subcentro") determina la actividad, que es de la forma #val("dag-VICE") de acuerdo con la tabla TABLA-TRADUCCIÓN-VICES.

        - #nombre-regla[DAG Consejo de Estudiantes]
            y #campo("tipo de proyecto") = #val("EST"), y #campo("subcentro") = #val("D7"), entonces la actividad es #etqact("dag-consejo-estudiantes")

        - #nombre-regla[DAG Vicerrectorados]
            y #campo("tipo de proyecto") = #val("00G"), y #campo("proyecto") = #val("00000"),

            - si el #campo("subcentro") es #val("R9"), la actividad es #etqact("dag-otros-servicios-promoción-fomento-igualdad"),
            - si el #campo("subcentro") es #val("I5"), la actividad es #etqact("dag-biblioteca"),
            - si no, el #campo("subcentro") determina la actividad, que es de la forma #val("dag-VICE") de acuerdo con la tabla TABLA-TRADUCCIÓN-VICES.

        - #nombre-regla[DAG de actividades de servicios]
            y #campo("tipo de proyecto") = #val("SLG"), #val("23G") o #val("20G"), el #campo("subcentro") determina la actividad en estos casos:

            #align(center, table(
                columns: 3,
                align: (left, left, left),

                table.header(
                    table.hline(), campo("subcentro"), campo("actividad"), [Descripción del subcentro], table.hline()
                ),

                val("CP"),
                [#etqact("dag-otros-servicios-comunicación-publicaciones")],
                [SERVEI DE COMUNICACIONS],

                val("P3"),
                [#etqact("dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico")],
                [SERVEI DE LLENGÜES I TERMINOLOGIA],

                val("OL"),
                [#etqact("dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico")],
                [OBSERVATORI LINGÜÍSTIC],

                val("F3"),
                [#etqact("dag-otros-servicios-prevención-gestión-medioambiental")],
                [OFICINA DE PREVENCIÓ I GESTIÓ MEDIAMBIENTAL],

                val("I2"),
                [#etqact("dag-otros-servicios-ti")],
                [SERVEI D'INFORMÀTICA],

                val("GA"),
                [#etqact("dag-otros-servicios-ti")],
                [UNITAT D'ANALISI I DESENVOLUPAMENT TI],

                val("F2"),
                [#etqact("dag-otros-servicios-obras-proyectos")],
                [OFICINA TÈCNICA D'OBRES I PROJECTES],

                val("S9"),
                [#etqact("dag-otros-servicios-información-registro")],
                [INFOCAMPUS],

                val("OC"),
                [#etqact("dag-otros-servicios-promoción-evaluación-calidad")],
                [OFICINA DE PROMOCIÓ I AVALUACIÓ DE LA QUALITAT],

                val("L2"),
                [#etqact("dag-otros-servicios-relaciones-internacionales")],
                [OFICINA DE RELACIONS INTERNACIONALS],

                val("GI"),
                [#etqact("dag-otros-servicios-ti")],
                [OFICINA D'INNOVACIÓ I AUDITORIA],

                val("DI"),
                [#etqact("dag-otros-servicios-atención-diversidad-apoyo-educativo")],
                [UNITAT DE DIVERSITAT],

                val("R9"),
                [#etqact("dag-otros-servicios-promoción-fomento-igualdad")],
                [UNITAT D'IGUALTAT],

                table.hline(),
            ))

    - #nombre-regla[DAG Departamento]
        y #campo("tipo de proyecto") = #val("QMG") y #campo("proyecto") = #val("24G006"), la actividad es de la forma #val("dag-DEPARTAMENTO") según la tabla TABLA-TRADUCCIÓN-DEPARTAMENTOS.

    - y #campo("tipo de proyecto") es #val("EST"), #val("26G") o #val("20G"),

        - #nombre-regla[CENT]
            y #campo("centro") es #val("CENT"), la actividad es #etqact("dag-cent")

        - #nombre-regla[Servicios relacionados con estudios/estudiantes]
            el #campo("subcentro") determina la actividad en estos casos:

            #align(center, table(
                columns: 2,
                align: (left, left),

                table.header(table.hline(), [#campo("subcentro")], [actividad], table.hline()),
                table.hline(),
                val("D2"), etqact("dag-sgde"),
                val("IH"), etqact("dag-oe"),
                val("D6"), etqact("dag-oipep"),
                val("O3"), etqact("dag-opp"),
                val("D8"), etqact("dag-uo"),

                table.hline(),
            ))

    - #nombre-regla[Cooperación]
        y #campo("tipo de proyecto") es #val("21G") y #campo("proyecto")<> #val("00000") y #campo("subcentro") = #val("DS"), la actividad es #etqact("dag-cooperación")

    - #nombre-regla[Cooperación]
        y #campo("tipo de proyecto") es #val("COOP"), la actividad es #etqact("dag-cooperación") + #campo("proyecto").

    - #nombre-regla[Innovación y emprendimiento (es EMP)]
        y #campo("tipo de proyecto") es #val("20G") y #campo("subcentro") = #val("EMP"),

        - y #campo("tipo de línea de financiación") es #val("00"), la actividad es #etqact("ait-financiación-propia")
        - en otro caso, la actividad es #etqact("ait-financiación-externa")

    - #nombre-regla[Tratamiento específico del doctorado]
        - y #campo("tipo de proyecto") es #val("DOCT") (proyectos doctorado) o #val("07G"), la actividad es #etqact("dag-escuela-doctorado")

    - #nombre-regla[Formación permanente]
        en los siguientes casos, el #campo("tipo de proyecto") determina la actividad sumando el proyecto:

        #align(center, table(
            columns: 2,
            align: (left, left),

            table.header(table.hline(), campo("tipo de proyecto"), [actividad], table.hline()),
            table.hline(),
            val("EPM"), [#etqact("másteres-formación-permanente") + #campo("proyecto")],
            val("EPDE"), [#etqact("diplomas-especialización") + #campo("proyecto")],
            val("EPDEX"), [#etqact("diplomas-experto") + #campo("proyecto")],
            val("EPC"), [#etqact("cursos-formación-permanente") + #campo("proyecto")],
            val("EPMI"), [#etqact("microcredenciales") + #campo("proyecto")],
            val("CUID"), [#etqact("cursos-idiomas") + #campo("proyecto")],
            val("CUEX"), [#etqact("cursos-extranjeros") + #campo("proyecto")],
            val("PAU"), [#etqact("acceso-enseñanzas-oficiales") + #campo("proyecto")],

            table.hline(),
        ))

    - #nombre-regla[Otras actividades de docencia]
        y #campo("tipo de proyecto") es #val("OAD"),

        - y el #campo("subcentro") es #val("UMAJ"), la actividad es #etqact("universidad-mayores")
        - en otro caso, la actividad es #etqact("otros-docencia-propia")

    - #nombre-regla[Otras actividades de transferencia]
        - y #campo("tipo de proyecto") es #val("OAT")
            - y #campo("tipo de línea de financiación") es #val("00"), la actividad es #etqact("ait-financiación-propia") + #campo("proyecto")
            - en otro caso, la actividad es #etqact("ait-financiación-externa") + #campo("proyecto")

    - #nombre-regla[Otras actividades de extensión universitaria]
        y #campo("tipo de proyecto") es #val("15G"),

        - y #campo("proyecto") es #val("0G009") (CURS PER A PERSONES MAJORS), la actividad es #etqact("universidad-mayores")
        - y #campo("proyecto") es #val("9G008") (PROJECTE TERRITORI), la actividad #etqact("otras-extensión-universitaria")

    - #nombre-regla[Proyectos europeos]
        y #campo("tipo de proyecto") es #val("UEG") (UNIÓ EUROPEA), el #campo("proyecto") determina la actividad en estos casos:

        #align(center, table(
            columns: 2,
            align: (left, left),

            table.header(table.hline(), campo("proyecto"), [actividad], table.hline()),

            val("22G045"), etqact("máster-geoespacial"),
            val("22G132"), etqact("máster-robótica-marina"),
            val("22G131"), [#etqact("ai-internacional") + #campo("proyecto")],

            table.hline(),
        ))

    - #nombre-regla[Otras actividades deportivas, culturales y de extensión universitaria]
        en estos casos, el #campo("tipo de proyecto") determina la actividad (posiblemente añadiendo el #campo("proyecto")):

        #align(center, table(
            columns: 3,
            align: (left, left, left),

            table.header(
                table.hline(), campo("tipo de proyecto"), campo("actividad"), [Descripción del proyecto], table.hline()
            ),

            val("DEP"), etqact("deportes"), [Otras actividades deportivas],
            val("18G"), etqact("cultura"), [DIPUTACIÓ],
            val("19G"), etqact("deportes"), [ACTIVITATS ESPORTIVES],
            val("09G"), [#etqact("otras-extensión-universitaria") + #campo("proyecto")], [DIPUTACIÓ],

            table.hline(),
        ))

    - #nombre-regla[Microcredenciales]
        y #campo("tipo de proyecto") es #val("10G") (GENERALITAT VALENCIANA), y proyecto es #val("24G112") (PLA PER AL DESENVOLUPAMENT DE MICROCREDENCIALS UNIVERSITÀRIES), la actividad es #etqact("microcredenciales")

    - #nombre-regla[DAG Institutos, centros de investigación y similares]
        y #campo("tipo de proyecto") es #val("DAGI") (Dirección, administración y soporte investigación), y #campo("tipo de línea de financiación") es #val("00"), la actividad es #etqact("dag-institutos-centros-investigación")

    - #nombre-regla[Proyectos variados de investigación]
        y #campo("tipo de proyecto") es #val("VARI") (VARIOS), y #campo("programa") #val("541-A"), y #campo("tipo de línea de financiación") es #val("00"), la actividad es #etqact("ait-financiación-propia")

    - #nombre-regla[Gastos generales desde rectorado, diversos servicios, centros... y gerencia y servicios centrales]
        y #campo("tipo de proyecto") es #val("00G") (DESPESES GENERALS) y #campo("proyecto") es #val("00000") (PROJECTE GENERAL), la actividad la determina el #campo("subcentro") en estos casos:

        #align(center, table(
            columns: 2,
            align: (left, left),

            table.header(table.hline(), campo("subcentro"), campo("actividad"), table.hline()),

            val("R1"), etqact("dag-rectorado"),
            val("R5"), etqact("dag-síndico-agravios"),
            val("R10"), etqact("dag-delegado"),
            val("S1"), etqact("dag-secretaría-general"),
            val("S4"), etqact("dag-junta-electoral"),
            val("D7"), etqact("dag-consejo-estudiantes"),
            val("CP"), etqact("dag-otros-servicios-comunicación-publicaciones"),
            val("P3"), etqact("dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico"),
            val("OL"), etqact("dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico"),
            val("F3"), etqact("dag-otros-servicios-prevención-gestión-medioambiental"),
            val("GA"), etqact("dag-otros-servicios-ti"),
            val("I2"), etqact("dag-otros-servicios-ti"),
            val("F2"), etqact("dag-otros-servicios-obras-proyectos"),
            val("S9"), etqact("dag-otros-servicios-información-registro"),
            val("OC"), etqact("dag-otros-servicios-promoción-evaluación-calidad"),
            val("L2"), etqact("dag-otros-servicios-relaciones-internacionales"),
            val("GI"), etqact("dag-otros-servicios-ti"),
            val("DI"), etqact("dag-otros-servicios-atención-diversidad-apoyo-educativo"),
            val("C2"), etqact("dag-deportes"),
            val("C3"), etqact("dag-cultura"),
            val("DS"), etqact("dag-cooperación"),
            val("Y1"), etqact("dag-estce"),
            val("J1"), etqact("dag-fcje"),
            val("H1"), etqact("dag-fchs"),
            val("SA"), etqact("dag-fcs"),
            val("CC"), etqact("dag-cent"),
            val("LB"), etqact("dag-labcom"),
            val("D2"), etqact("dag-sgde"),
            val("D6"), etqact("dag-oipep"),
            val("O3"), etqact("dag-opp"),
            val("D8"), etqact("dag-uo"),
            val("I4"), etqact("dag-sgit"),
            val("SD"), etqact("dag-sala-disección"),
            val("ED"), etqact("dag-escuela-doctorado"),
        ))

        - #nombre-regla[Gastos generales desde gerencia o servicios centrales]
            en estos otros casos, la determina #campo("centro")/#campo("capítulo"), #campo("centro")/#campo("artículo"), #campo("centro")/#campo("concepto") O #campo("centro")/#campo("aplicación"):

            #align(center, table(
                columns: (1fr, auto),
                align: (left, left),

                table.header(
                    table.hline(),
                    [#campo("centro") / (#campo("capítulo"), #campo("artículo"), #campo("concepto") o #campo("aplicación"))],
                    campo("actividad"),
                    table.hline(),
                ),

                val("GEREN/21"), etqact("dag-org-gerencia-tributos"),
                val("GEREN/221"), etqact("dag-org-gerencia-arrendamiento-bienes"),
                val("GEREN/222"), etqact("dag-org-gerencia-reparación-conservación"),
                val("GEREN/223"), etqact("dag-org-gerencia-suministros"),
                val("GEREN/224"), etqact("dag-org-gerencia-transportes-comunicaciones"),
                val("GEREN/225"), etqact("dag-org-gerencia-trabajos-realizados-otras-empresas"),
                val("GEREN/226"), etqact("dag-org-gerencia-primas-seguros"),
                val("GEREN/227"), etqact("dag-org-gerencia-material-oficina"),
                val("GEREN/228"), etqact("dag-org-gerencia-gastos-diversos"),
                val("GEREN/3"), etqact("dag-org-gerencia-gastos-financieros"),
                val("GEREN/6711"), etqact("dag-org-gerencia-adquisiciones-bibliográficas"),
                val("GEREN/23"), etqact("dag-org-gerencia-indemnizaciones-razón-servicio"),
                val("SC001/21"), etqact("dag-sgc-tributos"),
                val("SC001/221"), etqact("dag-sgc-arrendamiento-bienes"),
                val("SC001/222"), etqact("dag-sgc-reparación-conservación"),
                val("SC001/223"), etqact("dag-sgc-suministros"),
                val("SC001/224"), etqact("dag-sgc-transportes-comunicaciones"),
                val("SC001/225"), etqact("dag-sgc-trabajos-realizados-otras-empresas"),
                val("SC001/226"), etqact("dag-sgc-primas-seguros"),
                val("SC001/227"), etqact("dag-sgc-material-oficina"),
                val("SC001/228"), etqact("dag-sgc-gastos-diversos"),
                val("SC001/3"), etqact("dag-sgc-gastos-financieros"),
                val("SC001/6711"), etqact("dag-sgc-adquisiciones-bibliográficas"),
                val("SC001/23"), etqact("dag-sgc"),

                table.hline(),
            ))

        - #nombre-regla[Institutos, Consejo Social, CENT, SCIC y SEA]
            y en estos, el #campo("centro"):

            #align(center, table(
                columns: 2,
                align: (left, left),

                table.header(table.hline(), campo("centro"), campo("actividad"), table.hline()),

                val("CONSE"), etqact("dag-consejo-social"),
                val("IUDT"), etqact("dag-iudt"),
                val("IUEFG"), etqact("dag-iuef"),
                val("IMAC"), etqact("dag-imac"),
                val("INAM"), etqact("dag-inam"),
                val("INIT"), etqact("dag-init"),
                val("IUPA"), etqact("dag-iupa"),
                val("IUTC"), etqact("dag-iutc"),
                val("IUTUR"), etqact("dag-iuturismo"),
                val("IDL"), etqact("dag-iidl"),
                val("IEI"), etqact("dag-iei"),
                val("IFV"), etqact("dag-ifv"),
                val("IIG"), etqact("dag-iigeo"),
                val("IILP"), etqact("dag-ii-lópez-piñero"),
                val("I5"), etqact("dag-biblioteca"),
                val("CENT"), etqact("dag-cent"),
                val("IULMA"), etqact("dag-ilma"),
                val("IDSP"), etqact("dag-idsp"),
                val("SCIC"), etqact("dag-scic"),
                val("SEA"), etqact("dag-sea"),

                table.hline(),
            ))

    - #nombre-regla[Gastos generales desde departamentos]
        y (#campo("tipo de proyecto") = #val("00G") (DESPESES GENERALS), y #campo("proyecto") es #val("00000") (PROJECTE GENERAL), #val("24G103") (ESTADES DOCENTS BREUS PROFESSORAT VISITANT ESTRANGER CURS 2024/2025), #val("25G080") (ESTADES DOCENTS BREUS PROFESSORAT VISITANT ESTRANGER CURS 2025/2026) o #val("25G109") (AJUDES DE MOBILITAT PER A PERSONAL UJI A AMÈRICA, ÀSIA I OCEANIA 2025)) o (#campo("tipo de proyecto") = #val("14G") (LABORATORIS DOCENTS)), la actividad es de #val("dag-DEPARTAMENTO") según la tabla TABLA-TRADUCCIÓN-DEPARTAMENTOS.

    - #nombre-regla[Gastos generales desde vicerrectorados]
        y #campo("tipo de proyecto") = #val("06G") (PROJECTES DESPESA UJI VARIOS), y #campo("proyecto") = #val("9G077") (DESPESES GENERALS), y #campo("subcentro") es uno de los subcentros de vicerrectorado, el #campo("capítulo")/#campo("artículo")/#campo("concepto")/#campo("aplicación") determina la actividad en estos casos:

        #align(center, table(
            columns: 2,
            align: (left, left),

            table.header(
                table.hline(),
                [#campo("capítulo")/#campo("artículo")/#campo("concepto")/#campo("aplicación")],
                [actividad],
                table.hline(),
            ),
            val("%/21/%/%"), [#etqact("dag-org-vicerrectorados-tributos")],
            val("%/%/221/%"), [#etqact("dag-org-vicerrectorados-arrendamiento-bienes")],
            val("%/%/222/%"), [#etqact("dag-org-vicerrectorados-reparación-conservación")],
            val("%/%/223/%"), [#etqact("dag-org-vicerrectorados-suministros")],
            val("%/%/224/%"), [#etqact("dag-org-vicerrectorados-transportes-comunicaciones")],
            val("%/%/225/%"), [#etqact("dag-org-vicerrectorados-trabajos-realizados-otras-empresas")],
            val("%/%/226/%"), [#etqact("dag-org-vicerrectorados-primas-seguros")],
            val("%/%/227/%"), [#etqact("dag-org-vicerrectorados-material-oficina")],
            val("%/%/228/%"), [#etqact("dag-org-vicerrectorados-gastos-diversos")],
            val("%/23/%/%"), [#etqact("dag-org-vicerrectorados-indemnizaciones-razón-servicio")],
            val("3/%/%/%"), [#etqact("dag-org-vicerrectorados-gastos-financieros")],
            val("%/%/%/6711"), [#etqact("dag-org-vicerrectorados-adquisiciones-bibliográficas")],
            table.hline(),
        ))

    - #nombre-regla[Gastos generales desde servicios]
        y proyecto es #val("1G010") (INFRAESTRUCTURA TIC BÀSICA) o #val("9G082") (MANTENIMIENTO OTOP), y #campo("subcentro") es #val("CP"), #val("P3"), #val("OL"), #val("F3"), #val("GA"), #val("I2"), #val("F2"), #val("S9"), #val("OC"), #val("L2"), #val("GI"), #val("DI") o #val("R9"), el #campo("capítulo")/#campo("artículo")/#campo("concepto")/#campo("aplicación") determina la actividad en estos casos:

        #align(center, table(
            columns: 2,
            align: (left, left),
            table.header(
                [#campo("capítulo")/#campo("artículo")/#campo("concepto")/#campo("aplicación")],
                [actividad],
                table.hline(),
            ),
            table.hline(),
            val("%/21/%/%"), etqact("dag-sgc-tributos"),
            val("%/%/221/%"), etqact("dag-sgc-arrendamiento-bienes"),
            val("%/%/222/%"), etqact("dag-sgc-reparación-conservación"),
            val("%/%/223/%"), etqact("dag-sgc-suministros"),
            val("%/%/224/%"), etqact("dag-sgc-transportes-comunicaciones"),
            val("%/%/225/%"), etqact("dag-sgc-trabajos-realizados-otras-empresas"),
            val("%/%/226/%"), etqact("dag-sgc-primas-seguros"),
            val("%/%/227/%"), etqact("dag-sgc-material-oficina"),
            val("%/%/228/%"), etqact("dag-sgc-gastos-diversos"),
            val("%/23/%/%"), etqact("dag-sgc-indemnizaciones-razón-servicio"),
            val("3/%/%/%"), etqact("dag-sgc-gastos-financieros"),
            val("%/%/%/6711"), etqact("dag-sgc-adquisiciones-bibliográficas"),
        ))

    - #nombre-regla[Departamentos]
        y #campo("proyecto") es #val("8G022") (MANTENIMENT D'EQUIPS D'INVESTIGACIÓ), la actividad es #etq("dag-DEPARTAMENTO") usando la TABLA-TRADUCCIÓN-DEPARTAMENTOS. No se baja a más detalle (no se añade el sufijo de aplicación) porque el elemento de coste, que aquí siempre es #etqele("conservación-instalaciones"), ya aporta esa granularidad.

]

== Preparación de un módulo para clasificar centros de coste

Del mismo modo que antes hemos usado información que puede estar en registros de nómina o de presupuesto para conocer la actividad, queremos hacer los mismo para obtener el centro de coste.

El árbol de centros de coste modificado por las reglas se ha de mostrar en la #app, con una opción de un desplegable #val("Presupuesto") para mostrarlo o descargarlo en formato `.tree`. Además, se ha de mostrar un resumen de la información que contiene, con el número de filas y el importe total de cada una de ellas. Los nodos añadidos se han de mostrar de un color distinto y se ha de indicar cuantos nodos se han añadido.

#reglas[
    - #nombre-regla[Gastos de servicios centrales en mantenimientos, limpieza y seguridad distribuidos por % OTOP]
        Los apuntes con #campo("centro") = #val("SC001") y #campo("aplicación") en #val("2251") (limpieza), #val("2252") (seguridad), #val("2222") (conservación de construcciones), #val("2223") (conservación de instalaciones) o #val("2225") (conservación de mobiliario) son gastos centrales que la OTOP ya nos da repartidos por zonas, edificios o complejos del campus. No los podemos asignar a un centro de coste único: tienen que diluirse entre los centros que tienen presencia física donde se incurre el gasto.

        El reparto se calcula previamente en la etapa de inventario (subapartado «Establecimiento de un porcentaje de distribución de ciertos costes a cada centro de coste») y se materializa en una tabla con esquema (#campo("centro_de_coste"), #campo("porcentaje")) que cubre todos los centros con presencia distinta de cero, sumando 1 (el 100%). Para reproducirla hace falta:

        + Las matrices de presencia de centros de coste en zonas, edificios y complejos, calculadas a partir de #ruta("data", "entrada", "superficies", "ubicaciones.xlsx"), #ruta("data", "entrada", "superficies", "ubicaciones a servicios.xlsx") y #ruta("data", "entrada", "inventario", "servicios.xlsx") según el subapartado «Asignación de metros a cada servicio y porcentaje de presencia de cada centro en cada zona, edificio y complejo». La #emph[presencia] de un centro en una zona se define ahí: m² ocupados por el centro / m² totales de la zona, con redistribución intra-zona de los espacios sin servicio asignado y redistribución global de las zonas que no tienen ningún servicio.

        + El fichero #ruta("data", "entrada", "superficies", "distribución OTOP.xlsx") con filas (#campo("prefijo"), #campo("porcentaje"), #campo("comentario")) que distribuye cada euro de gasto entre prefijos. Cada prefijo identifica un complejo (1 carácter), edificación (2 caracteres) o zona ($\geq$ 3 caracteres). Para cada fila, su porcentaje se reparte entre los centros con presencia en ese nivel proporcionalmente a esa presencia; los porcentajes así obtenidos se acumulan por centro.

        Con esa tabla, cada apunte que cumple la condición #val("SC001") + aplicación se expande en tantas unidades de coste como centros distintos tenga la tabla, con #campo("importe") = importe del apunte $times$ porcentaje del centro (el porcentaje viene normalizado a tanto por uno). La actividad y el elemento de coste de cada UC expandida son los que la regla de actividad y la regla de elemento de coste hayan asignado al apunte original; lo único que cambia entre las UC hermanas es el #campo("centro_de_coste") y la fracción del importe. En el #campo("origen_porción") de cada UC se guarda el porcentaje aplicado, lo que permite recomponer el apunte original sumando las porciones.

        En la #app, el visor del traductor de presupuesto muestra el número de apuntes #val("SC001") detectados, el número de UC en que se han expandido, el importe original y un agregado por centro de coste y elemento de coste para verificación.

    - #nombre-regla[Cátedras y aulas de empresa]
        Si #campo("centro") es #val("INVES"), el #campo("proyecto") determina que el centro de coste sea #etqcen("cátedras-investigación") + #campo("proyecto") en estos casos:

        #align(center, table(
            columns: 2,
            align: (left, left),

            table.header(table.hline(), campo("proyecto"), [Descripción], table.hline()),

            val("1I235"), [CÀTEDRES],
            val("12I327"), [AJUNTAMENT DE VILA-REAL - CÀTEDRA D'INNOVACIÓ CERÀMICA «CIUTAT DE VILA-REAL»],
            val("13I037"), [AULA CERÁMICA - CONVENIO COLABORACIÓN ASCER],
            val("15I116"), [CATEDRA DE MEDIACIÓ POLICIAL - AJUNTAMENT DE VILA-REAL],
            val("15I129"), [CATEDRA FACSA DE INNOVACIÓN EN EL CICLO INTEGRAL DEL AGUA],
            val("16I028"), [CÁTEDRA RECIPLASA DE GESTIÓN DE RESIDUOS URBANOS],
            val("18I352"), [CÀTEDRA BP DE MEDI AMBIENT INDUSTRIAL],
            val("19I055"), [CÁTEDRA INDUSTRIA 4.0],
            val("20I035"), [AULA FUNDACIÓN TORRECID DE LIDERAZGO E INNOVACIÓN],
            val("21I159"), [CONVENIO UJI-COLOROBBIA ESPAÑA, S.A.-AULA VITTORIANO BITOSSI DE INNOVACIÓN SOSTENIBLE],
            val("21I221"), [FUNDACIÓ PRIVADA BANC SABADELL],
            val("21I242"), [#strong[NO ENCONTRADO]],
            val("21I616"), [CATEDRA DE ACTIVIDAD FISICA Y ONCOLOGIA-FUNDACIÓN JOSÉ SORIANO RAMOS],
            val("21I633"), [CÁTEDRA UBE DE PLÁSTICOS SOSTENIBLES],
            val("22I070"), [CATEDRA RTVE «CULTURA AUDIOVISUAL Y ALFABETIZACIÓN MEDIÁTICA»],
            val("22I242"), [AULA PORCELANOSA DE TALENTO Y EXCELENCIA],
            val("22I248"), [CÁTEDRA BIENVENIDO OLIVER-UJI DE DERECHO REGISTRAL],
            val("22I618"), [AULA D'ARQUEOLOGIA MEDITERRÀNIA-CONVENIO COL·LABORACIÓ AJUNTAMENT BORRIANA],
            val("23G030"), [CATEDRA SMART PORTS-CONVENIO AUTORIDAD PORTUARIA CASTELLÓ Y UJI],
            val("23G044"), [#strong[NO ENCONTRADO]],
            val("23G051"), [CÁTEDRA ALTADIA DEL CONOCIMIENTO CERÁMICO],
            val("23G069"), [AULA DE NEUROCIENCIAS-JAZZ PHARMACEUTICALS IBERIA, S.L.],
            val("23G070"), [CATEDRA DE INVESTIGACIÓN FARMACÉUTICA EN ENFERMEDADES CRÓNICAS],
            val("23G071"), [AULA INTUR DE TURISMO],
            val("23G144"), [#strong[NO ENCONTRADO]],
            val("24G012"), [#strong[NO ENCONTRADO]],
            val("24G015"), [#strong[NO ENCONTRADO]],
            val("24G019"), [#strong[NO ENCONTRADO]],
            val("24G022"), [#strong[NO ENCONTRADO]],
            val("24G025"), [CÁTEDRA ALCORA DE INVESTIGACIÓN MUSICAL Y CALIDAD DE VIDA DE LA UJI],
            val("24G026"), [#strong[NO ENCONTRADO]],
            val("24G028"), [#strong[NO ENCONTRADO]],
            val("24G034"), [CÁTEDRA ENDAVANT VILLARREAL C.F. DEL DEPORTE DE LA UJI],
            val("24G035"), [CÁTEDRA ARQUITECTURA CIRCULAR],
            val("24I137"), [#strong[NO ENCONTRADO]],
            val("24I256"), [#strong[NO ENCONTRADO]],
            val("24I308"),
            [CATEDRA SOBRE HUMANIZACIÓN DE LA ASISTENCIA SANITARIA DE VIU, FUNDACIÓN ASISA Y PROYECTO HUCI],
            val("24I557"), [CONVENI CÀTEDRA DE COOPERACIÓ I DESENVOLUPAMENT SOSTENIBLE, EIX ALIANCES 2024],
            val("25I016"),
            [AULA CRIMINALITAT BLAVA-CONVENIO COLABORACION ENTRE UJI Y FUNDACIÓN PARA CONSERVACIÓN DE IBIZA Y FORMENTERA],
            val("25I030"), [CÁTEDRA DE PATRIMONIO Y DESARROLLO SOSTENIBLE],
            val("25I042"), [CÁTEDRA DE COHESIÓN E INNOVACIÓN TERRITORIAL],
            val("25I130"), [CÁTEDRA ALCORA DE INVESTIGACIÓN MUSICAL Y CALIDAD DE VIDA DE LA UJI],
            val("25I254"),
            [CÀTEDRA D'ANÀLISI I PROSPECTIVA DE L'AUDIOVISUAL - CONSELL DE L'AUDIOVISUAL DE LA CV-UJI],
            val("09G013"), [PROJECTE ARTÍSTIC PARANIMF],
            val("22G011"), [GALERIA OCTUBRE],

            table.hline(),
        ))



    - #nombre-regla[Por servicio existe servicio y el proyecto uno de la línea]
        Si hay un servicio y el proyecto es #val("1G019"), #val("23G019"), #val("02G041"), #val("11G006"), #val("1G046") o #val("00000"), el servicio indicado en el registro permite decidir el centro de coste con esta tabla de mapeo y una excepción que te digo después de la tabla para el servicio #val("368") (personal de suport):

        #table(
            columns: (auto, 1fr, 1fr, 1fr),
            align: (right, left, left, left),
            table.header(
                table.hline(), [*Servicio*], [*Nombre del servicio*], [*Centro de coste*], [*Actividad*], table.hline()
            ),
            [523], [Assessoria Jurídica], [#etqcen("asesoría-jurídica")], [#etqact("dag-asesoría-jurídica")],
            [660], [Biblioteca], [#etqcen("bibliotecas")], [#etqact("dag-biblioteca")],
            [640], [Centre d'Educació i Noves Tecnologies], [#etqcen("cent")], [#etqact("dag-cent")],
            [263], [Consell Social], [#etqcen("consejo-social")], [#etqact("dag-consejo-social")],
            [2984], [Consell de l'Estudiantat], [#etqcen("consejo-estudiantes")], [#etqact("dag-consejo-estudiantes")],
            [1862],
            [Càtedra INCREA d'Innovació, Creativitat i Aprenentatge],
            [#etqcen("increa")],
            [#etqact("dag-innovación-emprendeduría")],

            [1662],
            [Càtedra UNESCO Filosofia per a la Pau],
            [#etqcen("cátedras-investigación-1I235")],
            [#etqact("cát-unesco-esclavitudes-afrodescendencia")],

            [4267],
            [Delegat de la Rectora per a la Transformació Docent, la Comunicació i la Direcció del Gabinet],
            [#etqcen("delegado")],
            [#etqact("dag-delegado")],

            [101], [Dep. d'Administració d'Empreses i Màrqueting], [#etqcen("daem")], [#etqact("dag-daem")],
            [93], [Dep. d'Economia], [#etqcen("deco")], [#etqact("dag-deco")],
            [3466], [Dep. d'Educació i Didàctiques Específiques], [#etqcen("dea")], [#etqact("dag-dede")],
            [2103], [Dep. d'Enginyeria Mecànica i Construcció], [#etqcen("dmc")], [#etqact("dag-dmc")],
            [81], [Dep. d'Enginyeria Química], [#etqcen("deq")], [#etqact("dag-deq")],
            [2102], [Dep. d'Enginyeria de Sistemes Industrials i Disseny], [#etqcen("desid")], [#etqact("dag-desid")],
            [1442], [Dep. d'Enginyeria i Ciència dels Computadors], [#etqcen("dicc")], [#etqact("dag-dicc")],
            [1882], [Dep. d'Estudis Anglesos], [#etqcen("dea")], [#etqact("dag-dea")],
            [104], [Dep. d'Història, Geografia i Art], [#etqcen("dhga")], [#etqact("dag-dhga")],
            [4207], [Dep. de Biologia, Bioquímica i Ciències Naturals], [#etqcen("dbbcn")], [#etqact("dag-dbbcn")],
            [2502], [Dep. de Ciències de la Comunicació], [#etqcen("dcc")], [#etqact("dag-dcc")],
            [90], [Dep. de Dret Públic], [#etqcen("ddpub")], [#etqact("dag-ddpub")],
            [1883], [Dep. de Filologia i Cultures Europees], [#etqcen("dfce")], [#etqact("dag-dfce")],
            [2503], [Dep. de Filosofia i Sociologia], [#etqcen("dfs")], [#etqact("dag-dfs")],
            [102], [Dep. de Finances i Comptabilitat], [#etqcen("dfc")], [#etqact("dag-dfc")],
            [2283], [Dep. de Física], [#etqcen("dfis")], [#etqact("dag-dfis")],
            [1443], [Dep. de Llenguatges i Sistemes Informàtics], [#etqcen("dlsi")], [#etqact("dag-dlsi")],
            [92], [Dep. de Matemàtiques], [#etqcen("dmat")], [#etqact("dag-dmat")],
            [3465],
            [Dep. de Pedagogia i Didàctica de les Ciències Socials, la Llengua i la Literatura],
            [#etqcen("dpdcsll")],
            [#etqact("dag-dpdcsll")],

            [97], [Dep. de Psicologia Bàsica, Clínica i Psicobiologia], [#etqcen("dpbcp")], [#etqact("dag-dpbcp")],
            [96],
            [Dep. de Psicologia Evolutiva, Educativa, Social i Metodologia],
            [#etqcen("dpeesm")],
            [#etqact("dag-dpeesm")],

            [2284], [Dep. de Química Física i Analítica], [#etqcen("dqfa")], [#etqact("dag-dqfa")],
            [98], [Dep. de Química Inorgànica i Orgànica], [#etqcen("dqio")], [#etqact("dag-dqio")],
            [99], [Dep. de Traducció i Comunicació], [#etqcen("dtc")], [#etqact("dag-dtc")],
            [4], [Escola Superior de Tecnologia i Ciències Experimentals], [#etqcen("estce")], [#etqact("dag-estce")],
            [3165], [Escola de Doctorat], [#etqcen("ed")], [#etqact("dag-escuela-doctorado")],
            [2], [Facultat de Ciències Humanes i Socials], [#etqcen("fchs")], [#etqact("dag-fchs")],
            [3], [Facultat de Ciències Jurídiques i Econòmiques], [#etqcen("fcje")], [#etqact("dag-fcje")],
            [2922], [Facultat de Ciències de la Salut], [#etqcen("fcs")], [#etqact("dag-fcs")],
            [3405], [Gabinet de Rectorat], [#etqcen("rectorado")], [#etqact("dag-rectorado")],
            [261], [Gerència], [#etqcen("gerencia")], [#etqact("dag-gerencia")],
            [4907], [Inspecció de Serveis], [#etqcen("inspección-servicios")], [#etqact("dag-inspección-servicios")],
            [3145], [Institut Interuniversitari de Desenvolupament Local], [#etqcen("iidl")], [#etqact("dag-iidl")],
            [3285],
            [Institut Universitari d'Investigació de Materials Avançats],
            [#etqcen("inam")],
            [#etqact("dag-inam")],

            [2603], [Institut Universitari de Noves Tecnologies de la Imatge], [#etqcen("init")], [#etqact("dag-init")],
            [2022], [Institut Universitari de Plaguicides i Aigües - IUPA], [#etqcen("iupa")], [#etqact("dag-iupa")],
            [264],
            [Institut Universitari de Tecnologia Ceràmica Agustín Escardino],
            [#etqcen("iutc")],
            [#etqact("dag-iutc")],

            [1982], [Laboratori de Comunicació Audiovisual i Publicitat], [#etqcen("labcom")], [#etqact("dag-labcom")],
            [4168],
            [Observatori Lingüístic],
            [#etqcen("ol")],
            [#etqact("dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico")],

            [364],
            [Oficina Tècnica d'Obres i Projectes],
            [#etqcen("otop")],
            [#etqact("dag-otros-servicios-obras-proyectos")],

            [3408], [Oficina d'Estudis], [#etqcen("oe")], [#etqact("dag-oe")],
            [3406],
            [Oficina d'Informació i Registre (InfoCampus)],
            [#etqcen("oir")],
            [#etqact("dag-otros-servicios-información-registro")],

            [3425], [Oficina d'Innovació i Auditoria TI], [#etqcen("oiati")], [#etqact("dag-otros-servicios-ti")],
            [2883],
            [Oficina d'Inserció Professional i Estades en Pràctiques],
            [#etqcen("oipep")],
            [#etqact("dag-oipep")],

            [1723],
            [Oficina de Cooperació al Desenvolupament i Solidaritat],
            [#etqcen("ocds")],
            [#etqact("cooperación")],

            [242],
            [Oficina de Cooperació en Investigació i Desenvolupament Tecnològic],
            [#etqcen("sgit")],
            [#etqact("dag-sgit")],

            [3847], [Oficina de Planificació i Prospectiva (OPP)], [#etqcen("opp")], [#etqact("dag-opp")],
            [4567],
            [Oficina de Prevenció, Promoció de la Salut i Medi Ambient],
            [#etqcen("oppsm")],
            [#etqact("dag-otros-servicios-prevención-gestión-medioambiental")],

            [2882],
            [Oficina de Relacions Internacionals],
            [#etqcen("ori")],
            [#etqact("dag-otros-servicios-relaciones-internacionales")],

            [1722],
            [Oficina de la Promoció i Avaluació de la Qualitat],
            [#etqcen("opaq")],
            [#etqact("dag-otros-servicios-promoción-evaluación-calidad")],
            // [368], [#etqcen("")], [#etqact("")],
            [311], [Secretaria General], [#etqcen("secretaría-general")], [#etqact("dag-secretaría-general")],
            [720], [Servei Central d'Instrumentació Científica], [#etqcen("scic")], [#etqact("dag-scic")],
            [251], [Servei d'Activitats Socioculturals], [#etqcen("sasc")], [#etqact("cultura")],
            [760], [Servei d'Esports], [#etqcen("se")], [#etqact("deportes")],
            [3004], [Servei d'Experimentació Animal], [#etqcen("sea")], [#etqact("dag-sea")],
            [1530], [Servei d'Informació Comptable], [#etqcen("sic")], [#etqact("dag-sic")],
            [366],
            [Servei de Comunicació i Publicacions],
            [#etqcen("scp")],
            [#etqact("dag-otros-servicios-comunicación-publicaciones")],

            [1544], [Servei de Contractació i Assumptes Generals], [#etqcen("scag")], [#etqact("dag-scag")],
            [1529], [Servei de Control Intern], [#etqcen("sci")], [#etqact("dag-sci")],
            [1543], [Servei de Gestió Econòmica], [#etqcen("sge")], [#etqact("dag-sge")],
            [361], [Servei de Gestió de la Docència i Estudiants], [#etqcen("sgde")], [#etqact("dag-sgde")],
            [4887], [Servei de Gestió de la Investigació i Transferència], [#etqcen("sgit")], [#etqact("dag-sgit")],
            [350],
            [Servei de Llengües i Terminologia],
            [#etqcen("slt")],
            [#etqact("dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico")],

            [362], [Servei de Recursos Humans], [#etqcen("srh")], [#etqact("dag-srh")],
            [2942], [Unitat Predepartamental d'Infermeria], [#etqcen("upi")], [#etqact("dag-upi")],
            [95],
            [Unitat Predepartamental de Dret del Treball/SS/Eclesiàstic i de l'Estat],
            [#etqcen("updtssee")],
            [#etqact("dag-updtssee")],

            [2943], [Unitat Predepartamental de Medicina], [#etqcen("upm")], [#etqact("dag-upm")],
            [3427], [Unitat d'Anàlisi i Desenvolupament TI], [#etqcen("uadti")], [#etqact("dag-otros-servicios-ti")],
            [4167], [Unitat d'Encàrrecs, Convenis i  Subvencions], [#etqcen("gencisub")], [#etqact("dag-gencisub")],
            [2822], [Unitat d'Igualtat], [#etqcen("ui")], [#etqact("dag-otros-servicios-promoción-fomento-igualdad")],
            [218], [Servei d'Informàtica], [#etqcen("uiic")], [#etqact("dag-otros-servicios-ti")],
            [4667],
            [Unitat d'Infraestructures Informàtiques de Campus],
            [#etqcen("uiic")],
            [#etqact("dag-otros-servicios-ti")],

            [4487], [Unitat d'Orientació], [#etqcen("uo")], [#etqact("dag-uo")],
            [4687],
            [Unitat de Dinamització i Participació de l'Estudiantat i Associacions],
            [#etqcen("udpea")],
            [#etqact("otras-extensión-universitaria-refinamiento")],

            [4488],
            [Unitat de Diversitat i Discapacitat],
            [#etqcen("udd")],
            [#etqact("dag-otros-servicios-atención-diversidad-apoyo-educativo")],

            [4489], [Unitat de Formació i Innovació Educativa], [#etqcen("ufie")], [#etqact("dag-ufie")],
            [344], [Unitat de Gestió 1], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [3409], [Unitat de Gestió 12], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [3445], [Unitat de Gestió 13], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [345], [Unitat de Gestió 2], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [347], [Unitat de Gestió 4], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [346], [Unitat de Gestió 3], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [348], [Unitat de Gestió 5], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [349], [Unitat de Gestió 6], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [2263], [Unitat de Gestió 7], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [4647],
            [Unitat de Gestió de la Investigació, Transferència i Justificacions],
            [#etqcen("sgit")],
            [#etqact("dag-sgit")],
            // UG
            [2342], [Universitat per a Majors], [#etqcen("universidad-mayores")], [#etqact("universidad-mayores")],
            [4251], [Vicerectorat d'Estudiantat i Vida Saludable], [#etqcen("vevs")], [#etqact("dag-vevs")],
            [4252], [Vicerectorat d'Estudis i Formació Permanent], [#etqcen("vefp")], [#etqact("dag-vefp")],
            [4248], [Vicerectorat d'Infraestructures i Sostenibilitat], [#etqcen("vis")], [#etqact("dag-vis")],
            [4250],
            [Vicerectorat d'Innovació, Transferència i Divulgació Científica],
            [#etqcen("vitdc")],
            [#etqact("dag-vitdc")],

            [4247], [Vicerectorat d'Investigació], [#etqcen("vi")], [#etqact("dag-vi")],
            [2224], [Vicerectorat d'Ordenació Acadèmica i Professorat], [#etqcen("voap")], [#etqact("dag-voap")],
            [4253], [Vicerectorat de Cultura, Llengües i Societat], [#etqcen("vcls")], [#etqact("dag-vcls")],
            [4255], [Vicerectorat de Planificació Econòmica i Estratègica], [#etqcen("vpee")], [#etqact("dag-vpee")],
            [4254], [Vicerectorat de Relacions Internacionals], [#etqcen("vri")], [#etqact("dag-vri")],
            [4249],
            [Vicerectorat de Responsabilitat Social, Polítiques Inclusives i Igualtat],
            [#etqcen("vrspii")],
            [#etqact("dag-vrspii")],
            // Servicios añadidos para resolver el patrón SERVICIO en
            // cargos.xlsx (cargos académicos: directores de departamento,
            // decanos, vicedecanos, directores de instituto, etc.).
            [82], [Dep. de Filologia Anglesa i Romànica], [#etqcen("dea")], [#etqact("dag-dea")],
            // REVISAR — Filologia Anglesa i Romànica: ¿dea (Estudios
            // Ingleses) o dfce (Filología y Culturas Europeas)?
            [84], [Dep. d'Educació], [#etqcen("dede")], [#etqact("dag-dede")],
            [89], [Dep. de Dret Privat], [#etqcen("ddpri")], [#etqact("dag-ddpri")],
            [1622],
            [Dep. de Filosofia, Sociologia i Comunicació Audiovisual i Publicitat],
            [#etqcen("dfs")],
            [#etqact("dag-dfs")],

            [2282], [Dep. de Ciències Agràries i del Medi Natural], [#etqcen("dbbcn")], [#etqact("dag-dbbcn")],
            // REVISAR — Ciències Agràries i del Medi Natural: ¿dbbcn?
            [1344],
            [Deganat de la Facultat de Ciències Jurídiques i Econòmiques],
            [#etqcen("fcje")],
            [#etqact("dag-fcje")],

            [1345], [Deganat de la Facultat de Ciències Humanes i Socials], [#etqcen("fchs")], [#etqact("dag-fchs")],
            [2230], [Vicerectorat de Postgrau], [#etqcen("vefp")], [#etqact("dag-vefp")],
            // REVISAR — Vicerectorat de Postgrau: ¿vefp?
            [372], [Institut Interuniversitari d'Economia Internacional], [#etqcen("iei")], [#etqact("dag-iei")],
            [1020], [Institut de Filologia Valenciana], [#etqcen("ifv")], [#etqact("dag-ifv")],
            [1165], [Institut Interuniversitari de Filologia Valenciana], [#etqcen("ifv")], [#etqact("dag-ifv")],
            [2062],
            [Institut Universitari de Llengües Modernes Aplicades - IULMA],
            [#etqcen("ilma")],
            [#etqact("dag-ilma")],

            [2482],
            [Institut Interuniversitari de Desenvolupament Local de la Comunitat Valenciana],
            [#etqcen("iidl")],
            [#etqact("dag-iidl")],

            [2604],
            [Institut Universitari de Matemàtiques i Aplicacions de Castelló],
            [#etqcen("imac")],
            [#etqact("dag-imac")],

            [2605],
            [Institut Universitari d'Estudis Feministes i de Gènere Purificación Escribano],
            [#etqcen("iuef")],
            [#etqact("dag-iuef")],

            [2767], [Institut Interuniversitari de Geografia], [#etqcen("iigeo")], [#etqact("dag-iigeo")],
            [2862],
            [Institut Interuniversitari de Desenvolupament Social i Pau],
            [#etqcen("idsp")],
            [#etqact("dag-idsp")],

            [3083], [Institut Universitari de Dret del Transport], [#etqcen("iudt")], [#etqact("dag-iudt")],
            [3505],
            [Institut Interuniversitari López Piñero],
            [#etqcen("ii-lópez-piñero")],
            [#etqact("dag-ii-lópez-piñero")],

            [4527], [Institut Universitari de Turisme], [#etqcen("iuturismo")], [#etqact("dag-iuturismo")],
            [4987], [Institut Universitari en Ciències de l'Educació], [#etqcen("iuce")], [#etqact("dag-iuce")],
            // Servicios 86 (Ciències Experimentals) y 88 (Tecnologia)
            // son departamentos históricos sin equivalencia clara en el
            // árbol actual; se dejan sin mapear hasta confirmación.
        )

        Con el servicio #val("368") hay una información adicional en el campo #campo("centro_plaza"). Su valor determina el centro de coste y la actividad con esta otra tabla:

        #table(
            columns: 3,
            align: (left, left, left),
            table.header(table.hline(), [*Centro plaza*], [*Centro de coste*], [*Actividad*], table.hline()),
            [2], etqcen("ps-fchs"), etqact("dag-conserjería-fchs"),
            [3], etqcen("ps-fcje"), etqact("dag-conserjería-fcje"),
            [4], etqcen("ps-estce"), etqact("dag-conserjería-estce"),
            [212], etqcen("ps-rectorado"), etqact("dag-conserjería-rectorado"),
            [263], etqcen("ps-escuela-doctorado-consejo-social"), etqact("dag-conserjería-consejo-social"),
            [2402], etqcen("ps-parque-tecnológico"), etqact("dag-conserjería-parque-tecnológico"),
            [2922], etqcen("ps-fcs"), etqact("dag-conserjería-fcs"),
        )

    - #nombre-regla[Lo que es de INVES debe ir al centro_origen]
        Si el #campo("centro") es #val("INVES"), el centro de coste es el que resulta de aplicar la TABLA-TRADUCCIÓN-DEPARTAMENTOS al #campo("centro_origen") del proyecto (es decir, se traduce el código del centro presupuestario al identificador del centro de coste correspondiente, p. ej. #val("DADEM") → #etqcen("daem"), #val("DFICO") → #etqcen("dfc")).
        #nota[Esto, en un futuro tendrá el grupo de investigación.]

    - #nombre-regla[Por centro y subcentro]
        La siguiente tabla relaciona pares #campo("centro")/#campo("subcentro") con un centro de coste. El % significa #emph[cualquier valor].

        #align(center, table(
            columns: 2,
            align: (left, left),

            table.header(
                table.hline(), [#campo("centro")/#campo("subcentro")], campo("centro de coste"), table.hline()
            ),

            val("CENT/%"), etqcen("cent"),
            val("%/C2"), etqcen("deportes"),
            val("%/C3"), etqcen("sasc"),
            val("%/CC"), etqcen("cent"),
            val("%/CP"), etqcen("scp"),
            val("%/D2"), etqcen("sgde"),
            val("%/D6"), etqcen("oipep"),
            val("%/D7"), etqcen("consejo-estudiantes"),
            val("%/D8"), etqcen("uo"),
            val("%/DI"), etqcen("udd"),
            val("%/DS"), etqcen("ocds"),
            val("%/ED"), etqcen("ed"),
            val("%/F2"), etqcen("otop"),
            val("%/F3"), etqcen("oppsm"),
            val("%/GA"), etqcen("uadti"),
            val("%/GI"), etqcen("oiati"),
            val("%/H1"), etqcen("fchs"),
            val("%/I2"), etqcen("uiic"),
            val("%/I4"), etqcen("sgit"),
            val("%/IH"), etqcen("oe"),
            val("%/J1"), etqcen("fcje"),
            val("%/L2"), etqcen("ori"),
            val("%/LB"), etqcen("labcom"),
            val("%/O3"), etqcen("opp"),
            val("%/OC"), etqcen("opaq"),
            val("%/OL"), etqcen("ol"),
            val("%/P3"), etqcen("slt"),
            val("%/R1"), etqcen("rectorado"),
            val("%/R10"), etqcen("delegado"),
            val("%/R5"), etqcen("síndico-agravios"),
            val("%/R9"), etqcen("ui"),
            val("%/S1"), etqcen("secretaría-general"),
            val("%/S4"), etqcen("junta-electoral"),
            val("%/S9"), etqcen("oir"),
            val("%/SA"), etqcen("fcs"),
            val("%/SD"), etqcen("sala-disección"),
            val("%/Y1"), etqcen("estce"),
            val("CONSE/%"), etqcen("consejo-social"),
            val("DADEM/%"), etqcen("daem"),
            val("DCAMN/%"), etqcen("dbbcn"),
            val("DCICO/%"), etqcen("dcc"),
            val("DDPRI/%"), etqcen("ddpri"),
            val("DDPUB/%"), etqcen("ddpub"),
            val("DDTSE/%"), etqcen("updtssee"),
            val("DEANG/%"), etqcen("dea"),
            val("DECIC/%"), etqcen("dicc"),
            val("DECON/%"), etqcen("deco"),
            val("DEDES/%"), etqcen("dede"),
            val("DEMEC/%"), etqcen("dmc"),
            val("DESID/%"), etqcen("desid"),
            val("DFICE/%"), etqcen("dfs"),
            val("DFICO/%"), etqcen("dfc"),
            val("DFISI/%"), etqcen("dfis"),
            val("DFISO/%"), etqcen("dfce"),
            val("DHIST/%"), etqcen("dhga"),
            val("DINFE/%"), etqcen("upi"),
            val("DIQUI/%"), etqcen("deq"),
            val("DLSIN/%"), etqcen("dlsi"),
            val("DMATE/%"), etqcen("dmat"),
            val("DMEDI/%"), etqcen("upm"),
            val("DPDID/%"), etqcen("dpdcsll"),
            val("DPSIB/%"), etqcen("dpbcp"),
            val("DPSIE/%"), etqcen("dpeesm"),
            val("DQFIA/%"), etqcen("dqfa"),
            val("DQUIO/%"), etqcen("dqio"),
            val("DTRAD/%"), etqcen("dtc"),
            val("ECTEC/%"), etqcen("estce"),
            val("FCCHS/%"), etqcen("fchs"),
            val("FCCJE/%"), etqcen("fcje"),
            val("FCCS/%"), etqcen("fcs"),
            val("GEREN/%"), etqcen("gerencia"),
            val("IDL/%"), etqcen("iidl"),
            val("IDSP/%"), etqcen("idsp"),
            val("IEI/%"), etqcen("iei"),
            val("IFV/%"), etqcen("ifv"),
            val("IIG/%"), etqcen("iigeo"),
            val("IILP/%"), etqcen("ii-lópez-piñero"),
            val("IMAC/%"), etqcen("imac"),
            val("INAM/%"), etqcen("inam"),
            val("INIT/%"), etqcen("init"),
            val("INVES/%"), etqcen("otros-investigación"),
            val("IUDT/%"), etqcen("iudt"),
            val("IUEFG/%"), etqcen("iuef"),
            val("IULMA/%"), etqcen("ilma"),
            val("IUPA/%"), etqcen("iupa"),
            val("IUTC/%"), etqcen("iutc"),
            val("IUTUR/%"), etqcen("iuturismo"),
            val("LABCOM/%"), etqcen("labcom"),
            val("REC/%"), etqcen("rectorado"),
            val("SCIC/%"), etqcen("scic"),
            val("SEA/%"), etqcen("sea"),
            val("SECRE/%"), etqcen("secretaría-general"),
            val("UMAJ/%"), etqcen("universidad-mayores"),
            val("VCLS/%"), etqcen("vcls"),
            val("VEFP/%"), etqcen("vefp"),
            val("VEVS/%"), etqcen("vevs"),
            val("VI/ED"), etqcen("ed"),
            val("VI/%"), etqcen("vi"),
            val("VINS/%"), etqcen("vis"),
            val("VITDC/%"), etqcen("vitdc"),
            val("VOAP/%"), etqcen("voap"),
            val("VPEE/%"), etqcen("vpee"),
            val("VRI/%"), etqcen("vri"),
            val("VRSPII/%"), etqcen("vrspii"),
            val("SC001/%"), etqcen("UJI"),

            table.hline(),
        ))

    - #nombre-regla[Servicios centrales como coste de la organización]
        Los apuntes con #campo("centro") = #val("SC001") que NO entren en la distribución OTOP (aplicaciones distintas de #val("2251"), #val("2252"), #val("2222"), #val("2223") y #val("2225")) son gasto de la organización en su conjunto y se imputan al centro raíz #etqcen("UJI"). Desde ahí se reparten downstream entre los centros productivos en la proporción que les toque.

]

== Generación de UC a partir de presupuesto

#figure(
    align(center, etapa-presupuesto()),
    caption: [Etapa de presupuesto: ficheros de entrada y salidas que produce.],
)

=== Reglas para generar unidades de coste a partir de apuntes presupuestarios

Lo primero es filtrar cieros registros del presupuesto de gasto, para quedarnos solo con los que nos interesan para generar unidades de coste. Para eso se definen una serie de reglas de filtrado, que se aplican a cada registro del presupuesto. Si un registro no pasa el filtro, no se genera unidad de coste a partir de él.

Una vez filtrado el presupuesto, se aplican una serie de reglas para generar unidades de coste a partir de los apuntes presupuestarios. Cada regla tiene una condición y un resultado. El resultado es la etiqueta de actividad, centro de coste o elemento de coste que se asigna a la unidad de coste que se genera a partir del apunte presupuestario si se cumple la condición.
Las reglas se aplican en orden y la primera que tiene éxito asigna actividad, centro de coste o elemento de coste a la unidad de coste. La división en secciones no tiene efectos en cuanto a la regla de _primera en coincidir, se aplica y detiene la búsqueda_.

Cuando se han aplicado las reglas de un fichero, se ha asignado una actividad, un centro de coste y un elemento de coste a cada unidad de coste. Si una unidad de coste no tiene asignados los tres campos, se considera incompleta.

La #app tendrá un desplegable «Presupuesto» con opciones para poder examinar todo lo que se hace en este paso de generación de unidades de coste a partir de apuntes presupuestarios.

En la #app se ha mostrar luego cuántas veces se ha aplicado cada regla de cada conjunto de reglas. Al seleccionar una regla, se mostrará los apuntes que han se han tratado y al pinchar en el apunte, las unidades de coste a las que ha dado lugar, con su importe y el resto de campos que tienen asignados.


==== Filtro del presupuesto

El fichero #ruta("apuntes presupuesto de gasto.xlsx"), que se obtiene del presupuesto liquidado, contiene muchas filas que no deben considerarse para generar unidades de coste porque son registros que ya encuentran tratamiento por otros medios. En particular, el capítulo #val("1") se trata en nóminas y el capítulo #val("6") por amortizaciones de inventario, exceptuando aplicación #val("6711"), que es de adquisiciones bibliográficas y se trata como el resto del presupuesto.

Hay ciertos suministros que pueden imputarse con relativa precisión si se conoce la superficie que ocupa cada centro de coste en cada edificio. La OTOP lleva un registro de gasto en energía eléctrica, agua y gas en función de dónde tiene contadores (suelen ser edificios o conjuntos de edificios). Como se tratan en otra sección, se eliminan sus líneas del presupuesto en esta etapa.

Las reglas de filtrado que recogen estas ideas son las siguientes:

#reglas[
    - #nombre-regla[Filtro de capítulos financieros]
        Si el #campo("capítulo") es #val("8") o #val("9"), no pasa el filtro.

    - #nombre-regla[Filtro del capítulo 1 que va por nóminas]
        Si el #campo("capítulo") es #val("1"), no pasa el filtro.

    - #nombre-regla[Filtro otros gastos que van por nóminas]
        Si la #campo("aplicación") es #val("2321") (asistencias) o #val("2281") (patentes), no pasa el filtro.

    - #nombre-regla[Supresión de capítulo 6 excepto 6711]
        Si #campo("capítulo") #val("6") y el #campo("aplicación") es distinto de #val("6711"), no pasa el filtro.

    - #nombre-regla[Supresión de consumos de energía, agua y gas]
        Si la #campo("aplicación") es #val("2231"), #val("2232") o #val("2233"), estamos ante un suministro de energía eléctrica, agua o gas, respectivamente. Esas filas las eliminamos porque tenemos un procedimiento distinto para generar unidades de coste a partir de los datos que nos facilita la OTOP, que conoce el coste desglosado por zonas, edificios o complejos de la universidad.
]

En la #app, hay que informar al usuario de cuántas filas se han filtrado por cada uno de estos motivos y qué importe se ha eliminado de cada capítulo, concepto o aplicación. También se ha de poder acceder a cada una de las filas filtradas, que han de estar enriquecidas con información de la regla que la ha filtrado.

#nota[Ver qué pasa con los apuntes de limpieza, seguridad y mantenimientos.]


==== Determinación de la actividad

Hay que utilizar el módulo de clasificación de actividades con los registros del presupuesto.


==== Determinación del centro de coste

Hay que utilizar el módulo de clasificación de centros de coste con los registros del presupuesto.

==== Determinación del elemento de coste para apuntes presupuestarios
El árbol de elementos de coste modificado por las reglas se ha de mostrar en la #app, con una opción de un desplegable #val("Presupuesto") para mostrarlo o descargarlo en formato `.tree`. Además, se ha de mostrar un resumen de la información que contiene, con el número de filas y el importe total de cada una de ellas. Los nodos añadidos se han de mostrar de un color distinto y se ha de indicar cuantos nodos se han añadido.

#reglas[
    - #nombre-regla[Clasificación por aplicación presupuestaria]
        La #campo("aplicación") presupuestaria determina el elemento de coste, según la tabla que se reproduce a continuación. La fuente de verdad de esta tabla es el fichero #ruta("data", "entrada", "presupuesto", "aplicaciones a elementos de coste.xlsx"): si se quiere reasignar una aplicación o añadir una nueva entrada, se edita ese fichero. Aplicaciones cuyo #campo("elemento_de_coste") esté vacío o valga #val("xxx") se consideran sin asignación por defecto y, en su caso, son las reglas condicionales con nombre las que les dan elemento de coste. (Hay alguna duda con #etqele("arrendamientos-instalaciones") y #etqele("arrendamientos-utillaje"), porque los dos están en #val("2213"). Ídem con conservación.)

        #align(center, table(
            columns: 3,
            align: (left, left, left),

            table.header(table.hline(), campo("aplicación"), campo("elemento de coste"), [Descripción], table.hline()),

            val("2111"),
            etqele("tributos-locales"),
            [TRIBUTS LOCALS],

            val("2112"),
            etqele("tributos-autonómicos"),
            [TRIBUTS AUTONÒMICS],

            val("2113"),
            etqele("tributos-estatales"),
            [TRIBUTS ESTATALS],

            val("2211"),
            etqele("arrendamientos-terrenos"),
            [TERRENOS Y BIENES NATURALES],

            val("2212"),
            etqele("arrendamientos-construcciones"),
            [ARRENDAMENT DE BÉNS. EDIFICIS I ALTRES CONSTRUCCIONS],

            val("2213"),
            etqele("arrendamientos-instalaciones"),
            [ARRENDAMENT DE BÉNS. MAQUINÀRIA INSTAL·LACIONS I UTILLATGE],

            val("2214"),
            etqele("arrendamientos-transporte"),
            [ARRENDAMENT DE BÉNS. MATERIAL DE TRANSPORT],

            val("2215"),
            etqele("arrendamientos-mobiliario"),
            [ARRENDAMENT DE BÉNS. MOBILIARI I BÉNS],

            val("2216"),
            etqele("arrendamientos-aplicaciones-informáticas"),
            [ARRENDAMENT DE BÉNS. APLICACIONS INFORMÀTIQUES],

            val("2217"),
            etqele("arrendamientos-equipos-informáticos"),
            [ARRENDAMENT DE BÉNS. EQUIPS PROCESSAMENT DE LA INFORMACIÓ],

            val("2218"),
            etqele("otros-arrendamientos"),
            [ARRENDAMENT DE BÉNS. ALTRES],

            val("2221"),
            etqele("conservación-terrenos"),
            [REPARACIÓ I CONSERVACIÓ DE BÉNS. TERRENYS I BÉNS NATURALS],

            val("2222"),
            etqele("conservación-construcciones"),
            [REPARACIÓ I CONSERVACIÓ DE BÉNS. EDIFICIS I ALTRES CONSTRUCCIONS],

            val("2223"),
            etqele("conservación-instalaciones"),
            [REPARACIÓ I CONSERVACIÓ DE BÉNS. MAQUINÀRIA, INSTAL·LACIONS I UTILLATGE],

            val("2224"),
            etqele("conservación-transporte"),
            [REPARACIÓ I CONSERVACIÓ DE BÉNS. MATERIAL DE TRANSPORT],

            val("2225"),
            etqele("conservación-mobiliario"),
            [REPARACIÓ I CONSERVACIÓ DE BÉNS. MOBILIARI I BÉNS],

            val("2226"),
            etqele("conservación-equipos-información"),
            [REPARACIÓ I CONSERVACIÓ DE BÉNS. EQUIPS PROCESSOS D' INFORMACIÓ],

            val("2228"),
            etqele("otras-conservaciones"),
            [REPARACIÓ I CONSERVACIÓ DE BÉNS. ALTRES],

            val("2234"),
            etqele("combustibles"),
            [SUBMINISTRAMENTS. COMBUSTIBLES],

            val("2235"),
            etqele("vestuario"),
            [SUBMINISTRAMENTS. VESTUARI],

            val("2237"),
            etqele("farmacia"),
            [SUBMINISTRAMENTS. PRODUCTOS FARMACEUTICOS],

            val("2241"),
            etqele("transportes"),
            [TRANSPORTS I COMUNICACIONS. PARC MÒBIL UNIVERSITAT JAUME I],

            val("2242"),
            etqele("transportes"),
            [TRANSPORTS I COMUNICACIONS. ALTRES TRANSPORTS],

            val("2243"),
            etqele("telefonía"),
            [TRANSPORTS I COMUNICACIONS. TELÈFON],

            val("2244"),
            etqele("correo"),
            [TRANSPORTS I COMUNICACIONS. CORREU],

            val("2245"),
            etqele("otras-comunicaciones"),
            [TRANSPORTS I COMUNICACIONS. TELÈGRAF],

            val("2246"),
            etqele("otras-comunicaciones"),
            [TRANSPORTS I COMUNICACIONS. TÈLEX],

            val("2248"),
            etqele("otras-comunicaciones"),
            [TRANSPORTS I COMUNICACIONS. ALTRES],

            val("2251"),
            etqele("limpieza-aseo"),
            [TREBALLS REALITZATS PER ALTRES EMPRESES. NETEJA I HIGIENE],

            val("2252"),
            etqele("seguridad"),
            [TREBALLS REALITZATS PER ALTRES EMPRESES. SEGURETAT],

            val("2253"),
            etqele("trabajos-otras-empresas"),
            [TREBALLS REALITZATS PER ALTRES EMPRESES. VALORACION Y PERITAJES],

            val("2254"),
            etqele("trabajos-otras-empresas"),
            [TREBALLS REALITZATS PER ALTRES EMPRESES. POSTALS],

            val("2255"),
            etqele("trabajos-otras-empresas"),
            [TREBALLS REALITZATS PER ALTRES EMPRESES. IMPARTICIÓ ESTUDIS],

            val("2256"),
            etqele("trabajos-otras-empresas"),
            [TREBALLS REALITZATS PER ALTRES EMPRESES. PROCESOS ELECTORALES],

            val("2257"),
            etqele("trabajos-otras-empresas"),
            [TREBALLS REALITZATS PER ALTRES EMPRESES. ESTUDIS I TREBALLS TÈCNICS],

            val("2261"),
            etqele("primas-seguro"),
            [PRIMES D'ASSEGURANÇA. EDIFICIOS Y LOCALES],

            val("2262"),
            etqele("primas-seguro"),
            [PRIMES D'ASSEGURANÇA. VEHICLES],

            val("2263"),
            etqele("primas-seguro"),
            [PRIMES D'ASSEGURANÇA. ALTRE IMMOBILITZAT],

            val("2268"),
            etqele("primas-seguro"),
            [PRIMES D'ASSEGURANÇA. ALTRES],

            val("2272"),
            etqele("publicaciones"),
            [MATERIAL D'OFICINA. PREMSA, REVISTES I PUBLICACIONS PERIÒDIQUES],

            val("2273"),
            etqele("publicaciones"),
            [MATERIAL D'OFICINA. LLIBRES I ALTRES PUBLICACIONS],

            val("2274"),
            etqele("material-informático"),
            [MATERIAL D'OFICINA. MATERIAL INFORMÀTIC],

            val("2275"),
            etqele("publicaciones"),
            [MATERIAL D'OFICINA. DESPESES DE PUBLICACIONS],

            val("2276"),
            etqele("fotocopias"),
            [MATERIAL D'OFICINA. MATERIAL FOTOCOPIADORAS],

            val("2277"),
            etqele("publicaciones"),
            [MATERIAL D'OFICINA. ENQUADERNACIONS],

            val("2280"),
            etqele("costes-diversos"),
            [DESPESES DIVERSES. SUBJECTES EXPERIMENTALS],

            val("2281"),
            etqele("cánones"),
            [DESPESES DIVERSES. CÀNONS (PROPIETAT INDUSTRIAL)],

            val("2282"),
            etqele("publicidad"),
            [DESPESES DIVERSES. PUBLICITAT I PROPAGANDA],

            val("2283"),
            etqele("costes-diversos"),
            [DESPESES DIVERSES. JURÍDICS CONTENCIOSOS],

            val("2284"),
            etqele("relaciones-públicas"),
            [DESPESES DIVERSES. ATENCIONS PROTOCOL·LÀRIES],

            val("2285"),
            etqele("costes-diversos"),
            [DESPESES DIVERSES. REUNIONS I CONFERÈNCIES],

            val("2286"),
            etqele("publicación-en-revistas-científicas"),
            [DESPESES DIVERSES. PUBLICACIÓ EN REVISTES CIENTÍFIQUES],

            val("2287"),
            etqele("trabajos-otras-empresas"),
            [DESPESES DIVERSES. ACTIVITATS CULTURALS],

            val("2288"),
            etqele("otros-bienes-servicios"),
            [ALTRES DESPESES],

            val("2289"),
            etqele("formación"),
            [INSCRIPCIONS A CURSOS, CONGRESSOS I SIMILARS],

            val("2311"),
            etqele("indemnizaciones-servicio"),
            [DIETES],

            val("2312"),
            etqele("indemnizaciones-servicio"),
            [LOCOMOCIÓ],

            val("2313"),
            etqele("indemnizaciones-servicio"),
            [TRASLLAT],

            val("2314"),
            etqele("indemnizaciones-servicio"),
            [DESPESES COMISSIONS DE SERVEI],

            val("3111"),
            etqele("otros-costes-financieros"),
            [DESPESES EMISSIÓ D'OBLIGACIONS A LLLARG TERMINI],

            val("3121"),
            etqele("intereses-préstamos"),
            [INTERESES DE OBLIGACIONES I BONS],

            val("3221"),
            etqele("intereses-préstamos"),
            [INTERESSOS DE PRÉSTECS],

            val("3411"),
            etqele("otros-costes-financieros"),
            [INTERESSOS DE DEMORA],

            val("3421"),
            etqele("servicios-bancarios"),
            [ALTRES DESPESES FINANCERES],

            val("4111"),
            etqele("transferencias-otras-organizaciones"),
            [TRANSFERÈNCIES CORRENTS. A LA ADMINISTRACION DEL ESTADO],

            val("4211"),
            etqele("transferencias-otras-organizaciones"),
            [TRANSFERÈNCIES CORRENTS. A ORGANISMOS AUTONOMOS ADMINISTRATIVOS],

            val("4411"),
            etqele("transferencias-otras-organizaciones"),
            [TRANSFERÈNCIES CORRENTS. A AJUNTAMENTS],

            val("4421"),
            etqele("transferencias-otras-organizaciones"),
            [TRANSFERÈNCIES CORRENTS. A DIPUTACIONS],

            val("4431"),
            etqele("transferencias-otras-organizaciones"),
            [TRANSFERÈNCIES CORRENTS. A ALTRES CORPORACIONS LOCALS],

            val("4432"),
            etqele("costes-financieros"),
            [INGRESSOS PER FÀCTORING. DEUTE GV.],

            val("4441"),
            etqele("transferencias-otras-organizaciones"),
            [TRANSFERÈNCIES CORRENTS. A COMUNITATS AUTÒNOMES.],

            val("4511"),
            etqele("transferencias-otras-organizaciones"),
            [TRANSFERÈNCIES CORRENTS. A EMPRESES PÚBLIQUES],

            val("4521"),
            etqele("transferencias-otras-organizaciones"),
            [TRANSFERÈNCIES CORRENTS. A ALTRES ENTITATS PÚBLIQUES],

            val("4531"),
            etqele("transferencias-otras-organizaciones"),
            [SUBV. EXPLOTACION EMPRESAS PUBLICAS],

            val("4541"),
            etqele("transferencias-otras-organizaciones"),
            [SUBV. EXPLOTACION OTROS ENTES PUBLICOS],

            val("4611"),
            etqele("transferencias-otras-organizaciones"),
            [TRANSFERÈNCIES CORRENTS. A EMPRESES PRIVADES],

            val("4621"),
            etqele("transferencias-otras-organizaciones"),
            [SUBV.EXPLOTACION EMPRESAS PRIVADAS],

            val("4711"),
            etqele("transferencias-alumnos"),
            [TRANSFERÈNCIES CORRENTS. A BECARIS],

            val("4712"),
            etqele("transferencias-alumnos"),
            [SEGURETAT SOCIAL BECARIS],

            val("4721"),
            etqele("transferencias-otras-organizaciones"),
            [TRANSFERÈNCIES CORRENTS. INSTITUCIONS SENSE FINALITAT DE LUCRE],

            val("4722"),
            etqele("transferencias-otras-organizaciones"),
            [TRANSFERÈNCIES CORRENTS. A ASSOCIACIONS D'ESTUDIANTS],

            val("4723"),
            etqele("transferencias-otras-organizaciones"),
            [A JUNTAS DE PERSONAL Y COMITE DE EMPRESA],

            val("4724"),
            etqele("transferencias-otras-organizaciones"),
            [A CENTROS E INSTITUCIONES PARA LA ORGANIZACION CONJUNTA DE CURSOS Y CONGRESOS],

            val("4731"),
            etqele("transferencias-organizaciones-grupo"),
            [TRANSFERÈNCIES CORRENTS. ORGANITZACIONS DEL GRUP],

            val("4811"),
            etqele("transferencias-otras-organizaciones"),
            [TRANSFERÈNCIES CORRENTS. A L'EXTERIOR],

            val("6711"),
            etqele("publicaciones"),
            [INVERSIONS EN FONS BIBLIOGRÀFICS],

            val("7700"),
            etqele("transferencias-otras-organizaciones"),
            [TRANSFERÈNCIES DE CAPITAL. A EMPRESES PRIVADES],

            val("7721"),
            etqele("transferencias-alumnos"),
            [TRANSFERENCIAS DE CAPITAL. A FAMILIAS],

            val("7731"),
            etqele("transferencias-otras-organizaciones"),
            [TRANSFERÈNCIES DE CAPITAL. A INSTITUCIONS SENSE FINALITAT DE LUCRE],

            val("7732"),
            etqele("transferencias-organizaciones-grupo"),
            [TRANSFERÈNCIES DE CAPITAL. ORGANITZACIONS DEL GRUP],

            table.hline(),
        ))

    - #nombre-regla[Conferenciantes u otras empresas]
        Si la #campo("aplicación") es #val("2322")
        - si #campo("tipo de proyecto") es #val("EPM"), #val("EPDE"), #val("EPDEX"), #val("EPC"), #val("EPMI"), #val("CUID") o #val("CUEX"), o si el #campo("centro") = #val("UMAJ"), el elemento de coste es #etqele("piyotper-conferenciantes")
        - en otro caso, es #etqele("trabajos-otras-empresas")

    - #nombre-regla[Material de docencia, deportivo u otro]
        Si la #campo("aplicación") es #val("2236") ó #val("2238")

        - si el #campo("centro") es #val("ECTEC"), #val("FCCHS"), #val("FCCJE"), #val("FCCS"), el elemento de coste es #etqele("material-docencia");
        - si el #campo("subcentro") es #val("C2"), #val("C3"), es #etqele("material-deportivo-cultural");
        - en otro caso, es #etqele("otros-suministros")

    - #nombre-regla[Material de laboratorio, de conservación de instalaciones, de investigación u de docencia]
        Si la #campo("aplicación") es #val("2239"),

        - y el #campo("programa") es #val("541-A"),
            - si el #campo("proyecto") = #val("00000"), el elemento de coste es #etqele("material-laboratorio")
            - si el #campo("proyecto") = #val("8G022"), es #etqele("conservación-instalaciones")
            - si no, el elemento de coste es #etqele("bienes-investigación")
        - si no, es #etqele("material-docencia")

    - #nombre-regla[Material de docencia o de oficina en centros]
        Si la #campo("aplicación") es #val("2271") o #val("2278"),

        - si el #campo("centro") es #val("ECTEC"), #val("FCCHS"), #val("FCCJE") o #val("FCCS"), el elemento de coste es #etqele("material-docencia"),
        - en otro caso, es #etqele("material-oficina").

    - #nombre-regla[Publicaciones o servicios profesionales]
        Si la #campo("aplicación") es #val("2258"),

        - y el #campo("subcentro") es #val("I5"), el elemento de coste es #etqele("publicaciones")
        - en otro caso, es #etqele("servicios-profesionales")
]


==== Reglas para suministros especiales (energía, agua, gas)

#figure(
    align(center, etapa-suministros()),
    caption: [Etapa de suministros (energía, agua, gas): ficheros de entrada y salidas.],
)

En los ficheros #ruta("energía.xlsx"), #ruta("agua.xlsx") y #ruta("gas.xlsx"), que están en el directorio `data/entrada/consumos` se recogen los gastos de energía eléctrica, agua y gas, respectivamente, por zonas del campus.

Explicamos el método detallado para #ruta("energía.xlsx"), pero hay que hacer lo mismo con los otros dos ficheros y el procedimiento es el mismo.

Nos vamos a referir a conceptos que están en tablas de #ruta("data", "entrada", "superficies"), porque el reparto va a tener que ver con la presencia de cada centro de coste en un edificio. Averiguaremos la presencia de cada centro en cada zona y, para una de esas zonas en las que hay presencia, averiguaremos el % de ocupación del centro, al llamamos X. Ahora veremos esa zona con quien hace concordancia de prefijo en #ruta("energía.xlsx"). Solo puede concordar con uno. Si hay más de un prefijo válido, escogemos siempre el más largo. Con eso tendremos un importe. De ese importe, asignamos a ese centro un porcentaje X del importe.

Las concordancias de prefijos hay que explicarlas bien. Imaginemos que dos prefijos que presentan solapamiento, como #val(" ") y #val("TIN"). Si un coste está asociado a #val("TI0111AL"), está claro que encaja con #val("TI"). Pero ojo con un coste asociado a #val("TIN0111AL"), porque también encaja con #val("TI"). En ese caso, hay que asignar el coste a #val("TIN"), porque es el prefijo más largo que encaja.

Imaginemos esta línea del fichero #ruta("energía.xlsx"):

#align(center, table(
    columns: 3,
    align: (left, right, left),

    table.header(table.hline(), [prefijo], [Coste], [Comentario], table.hline()),
    val("FF0"), [36.006,35], [ESCUELA DOCTORADO],
    table.hline(),
))

Hemos de determinar todos los centros que tienen presencia en la zona FF0 y qué porcentaje de presencia tienen en esa zona (si en vez zona, el prefijo fuera de edificio o complejo, el razonamiento es el mismo). Imaginemos que el centro de coste X tiene ocupa eln 20% de la zona FF0, en tal caso, creamos una unidad de coste:

- importe: 20% de 36.006,35 euros
- elemento de coste: #etqele("energía-eléctrica")
- centro de coste: #etqcen("ed")
- actividad: #etqact("dag-general-universidad")

Nótese que se crearán tantas unidades de coste como centros tengan presencia en la zona FF0, cada una con el porcentaje de presencia que tenga cada centro en esa zona.

Lo mismo hay que hacer con cada línea de #ruta("energía.xlsx"), con cada línea de #ruta("agua.xlsx") (su elemento de costes es #etqele("agua")) y con cada línea de #ruta("gas.xlsx") (su elemento de coste es #etqele("gas")), teniendo en cuenta que el prefijo puede concordar con una zona, un edificio o un complejo, pero no con más de uno, porque si concordara con más de uno, habría que escoger el prefijo más largo.

Si un prefijo de la fila no encaja con ninguna zona, edificio ni complejo (por ejemplo, #val("EE0"), porque la zona ya no existe en superficies), esa línea se ignora: no genera ninguna unidad de coste y su importe queda fuera del reparto. La #app reporta la lista de prefijos sin match y el importe agregado correspondiente para que pueda revisarse y, en su caso, corregirse en el siguiente ejercicio (añadiendo la zona o ajustando el prefijo en el fichero de suministro).

En la #app debes informar de cuántas unidades de coste se han generado a partir de cada línea de cada uno de los ficheros (energía, agua, gas), y qué importe supone cada una de esas unidades de coste. Además, al final del proceso, se ha de informar del importe total que se ha asignado a cada centro de coste por cada uno de los suministros (energía, agua y gas), comprobando que la suma de las unidades generadas (más los descartes por prefijo sin match) coincide con el importe total de cada uno de los suministros.



== Generación de unidades de coste a partir de información de amortizaciones

#figure(
    align(center, etapa-amortizaciones()),
    caption: [Etapa de amortizaciones: ficheros de entrada y salidas que produce.],
)

La #app tendrá un desplegable «Amortizaciones» con nuevas entradas para mostrar los diferentes elementos de esta etapa.

=== Cálculo del importe de amortización en el año analizado y reglas de filtrado

Lo primero que se hace es descartar línea del inventario aplicando estas reglas:

#reglas[
    - #nombre-regla[Supresión de elementos de baja]
        El campo #campo("estado") puede tomar los valores #val("A") (activo), #val("B") (baja), #val("M") y #val("O"). Solo se descartan las líneas con #campo("estado") = #val("B"); cualquier otro valor (incluidos #val("M") y #val("O")) se mantiene.

    - #nombre-regla[Supresión por cuentas contables]
        Solo han de pasar el filtro las cuentas contables que empiecen por uno de estos prefijos de 3 dígitos: #val("202"), #val("203"), #val("204"), #val("205"), #val("206"), #val("211"), #val("214"), #val("215"), #val("216"), #val("217"), #val("218"). Cualquier otra cuenta (#val("231x"), #val("28xx"), #val("29xx"), etc.) queda fuera y se descarta. La lista en negativo se obtiene por complemento; en la #app puede consultarse el detalle de cuentas filtradas con su número de líneas e importe agregado, lo que permite revisar si alguna cuenta debería haber sido aceptada.

    - #nombre-regla[Supresión por falta de información del alta]
        Primero hay que eliminar la filas que no tienen #campo("fecha de alta").
]

A continuación, los registros del inventario que pasan el filtro han de enriquecerse con cierta información para generar unidades de coste.

La primera es la #campo("fecha de amortización completa") del bien. A la fecha de alta se les suman los años de amortización que correspondan a ese tipo de bien, según la cuenta contable en la que se registró la compra. A continuación, se calcula el #campo("número de días del año") para el que se hace la contabilidad analítica que coinciden con el período de amortización.

Por ejemplo, si el bien se dio de alta el 1 de marzo de 2020 y se amortiza en 5 años, la #[fecha de amortización completa] es el 1 de marzo de 2025. Si el período de contabilidad analítica es el año 2025, entonces el #[número de días del año] 2025 que coinciden con el período de amortización es 60 (del 1 de enero al 1 de marzo).

El #campo("importe de amortización anual") del bien, que es el valor de compra dividido por los años de amortización. En el ejemplo anterior, si el valor de compra del bien es de 10.000 euros y se amortiza en 5 años, el importe de amortización anual es de 2.000 euros.

El registro se enriquece con campo #campo("importe amortización") que es el #campo("importe de amortización anual") multiplicado por el #campo("número de días del año") que coinciden con el período de amortización, dividido por el número de días que tiene ese año (365 o 366).

En el ejemplo anterior, el importe sería de 2.000 euros multiplicado por 60 días, dividido por 365, lo que da un importe de amortización para el año 2025 de aproximadamente 328,77 euros.

Hay un caso especial que debe tenerse en cuenta. Los años de amortización de la cuenta #val("2060") dependen de la fecha de alta. En la hoja de cálculo #ruta("años amortización por cuenta.xlsx") dice que son 8 años, pero solo es así si la #campo("fecha de alta") es anterior al #val("2018-12-31"). Si la fecha es posterior, los años de amortización son 6.

En la #app se han de poder ver las líneas de inventario con importe imputable al ejercicio analizado y la información enriquecida para facilitar comprobaciones.

#reglas[
    - #nombre-regla[Supresión de elementos que no se amortizan en el año]
        Luego, solo han de sobrevivir a este proceso las líneas en las que el #campo("importe amortización") sea mayor que cero y el número de días del año que coinciden con el período de amortización sea mayor que cero, porque solo esas líneas pueden dar lugar a unidades de coste.

    - #nombre-regla[Supresión por falta de cuenta contable]
        Algunos registros de inventario no tienen #campo("cuenta contable"), lo que impide calcular el importe de amortización anual. Esos registros se han de eliminar, porque no pueden dar lugar a unidades de coste. Su importe agregado no es significativo (y lo sería aún menos si se dividiera este entre los años de amortización que le hubieran correspondido).

    - #nombre-regla[Supresión por falta de información de ubicación]
        Para generar unidades de coste a partir de las amortizaciones, es necesario conocer la #campo("ubicación") del bien, porque el coste se asigna a los edificios. Por eso, hay que eliminar las líneas que no tienen información de ubicación. Nuevamente, es un importe despreciable y relacionado con compras antiguas, por lo que no es un gran problema eliminar esas líneas.
]

La #app debe permitir ver los registros eliminados y para cada uno hemos de conocer la regla que lo ha eliminado. Además, para cada regla hemos de conocer el número de líneas que ha eliminado, el importe inicial que suponían y el importe de amortización que suponían, para comprobar los filtros producen resultados razonables.

=== Cálculo de metros cuadrados por zona, edificación y complejo

==== Superficie total de cada zona, edificación y complejo

En primer lugar, hemos de calcular el total de metros cuadrados que tiene cada zona, edificación y complejo. Para ello, se suman los metros cuadrados de las ubicaciones que se encuentran en cada zona, edificación o complejo.

En la #app se ha de mostrar esta información en una opción Totales, comprobando que la suma de los metros cuadrados de todas las zonas es igual a la suma de los metros cuadrados de todas las edificaciones, y que esta última es igual a la suma de los metros cuadrados de todos los complejos, que a su vez es igual al número total de metros cuadrados del campus.

==== Asignación de metros a cada servicio y porcentaje de presencia de cada centro en cada zona, edificio y complejo
Para toda la UJI y para cada complejo, edificio y zona querremos saber qué presencia, en metros cuadrados tiene cada centro de coste de los que tienen presencia en ese complejo, edificio o zona.

El primer problema es que no tenemos esta relación directa, porque la OTOP mantiene una relación entre #emph[servicios] y ubicaciones. Un servicio se identifica con un número y tiene una descripción que no casa con las nuestras. No obstante, hemos mapeado los servicios existentes en el año de interés a los centros de coste que podemos asociar con ellos, de modo que podemos traducir la información de presencia de servicios en complejos, edificios y zonas a presencia de centros de coste, y a partir de ahí, calcular la presencia de cada centro de coste en la UJI, en cada complejo, en cada edificio y en cada zona en términos de:

- metros cuadrados
- y porcentaje de presencia en el mismo.

Si dos servicios mapean a un mismo centro, el centro acumula los metros de los dos servicios.

Eso es lo que queremos conocer para cada centro asociado a un servicio vivo.

Hay una segunda dificultad. Hay complejos, edificios o zonas que no tienen asignado ningún servicio (por ejemplo, todo lo que define la urbanización del campus). Los metros cuadrados de esas zonas se han de repartir entre los servicios en función del reparto de metros cuadrados que tiene el servicio en la UJI con la información de las ubicaciones que tiene.

Imagina que el campus tiene tres zonas, A, B y C, y dos centros de coste, X e Y. Cuando vemos las ocupaciones, vemos que X solo está en la zona A y ocupa 100 metros; y que Y está en las zona A y B con 100 y 200 metros, respectivamente. La zona C, que ocupa 200 metros, no tiene ningún servicio/centro asignado. Hay que hacer los siguiente. Una primera tabla solo tiene, inicialmente, los metros cuadrados que cada centro tiene en cada zona:

#align(center, table(
    columns: 4,
    align: (left, auto, auto, auto),

    table.header(table.hline(), [Centro], [A], [B], [C], table.hline()),
    table.hline(),
    [X], [100], [0], [0],
    [Y], [100], [200], [0],
    table.hline(),
))

La tabla se ha de actualizar para repartir los metros cuadrados de la zona C entre X e Y con el porcentaje de presencia de cada centro en la UJI (X: 25%, Y: 75%). El resultado es el siguiente:

#align(center, table(
    columns: 4,
    align: (left, auto, auto, auto),

    table.header(table.hline(), [Centro], [A], [B], [C], table.hline()),
    table.hline(),
    [X], [100], [0], [50],
    [Y], [100], [200], [150],
    table.hline(),
))

Ahora podemos determinar la presencia de cada centro en cada zona:

#align(center, table(
    columns: 4,
    align: (left, auto, auto, auto),

    table.header(table.hline(), [Centro], [A], [B], [C], table.hline()),
    table.hline(),
    [X], [50%], [0%], [25%],
    [Y], [50%], [100%], [75%],
    table.hline(),
))

Además, en la UJI, la presencia es:

#align(center, table(
    columns: 2,
    align: (left, auto),

    table.header(table.hline(), [Centro], [UJI], table.hline()),
    table.hline(),
    [X], [25%],
    [Y], [75%],
    table.hline(),
))

Hay que hacer esto a nivel de toda la UJI, de cada complejo, de cada edificio y de cada zona, porque cuando haya un coste asociado a la UJI, a un complejo, a un edificio o a una zona, se ha de repartir entre los centros de coste que tienen presencia en ese complejo, edificio o zona con el porcentaje de presencia que cada centro tiene en ese complejo, edificio o zona. Parte de esa presencia es real, y parte le toca en el reparto.

El objetivo de esta etapa es construir matrices con el número de metros que cada centro de coste tiene en cada zona, edificio, complejo y en la UJI y el porcentaje que supone esa presencia de cada zona, edificio, complejo y la propia UJI.

Y ahora queremos lo contrario, para la UJI, queremos saber qué porcentaje de presencia tiene cada centro. Para cada complejo, queremos saber qué porcentaje de presencia tiene cada centro. Para cada edificio, queremos saber qué porcentaje de presencia tiene cada centro. Para cada zona, queremos saber qué porcentaje de presencia tiene cada centro.

La #app ha presentar información para facilitar comprobaciones de que todo está bien. La información será:

- Para cada centro una tabla con una columna para la UJI y otra por cada complejo, edificio y zona en los que tiene presencia (bueno, limitaremos el número de columnas a una para la UJI y 9 más, las más importantes). El valor de la celda correspondiente será el número de metros cuadrados que tiene en cada uno de ellos, además, del porcentaje de presencia que eso supone en cada uno de ellos.

- Para la UJI los metros y % que tiene cada centro. Ahora, para cada complejo (columna 1) la relación de centros (de mayor a menor m2) con presencia en el centro, con los metros y su porcentaje (todo como texto separado por comas en una columna 2). Lo mismo para los edificios y lo mismo para las zonas.

- Además, la #app mostrará todos los servicios por ocupación de metros cuadrados en la UJI, en cada complejo, en cada edificio y en cada zona.

==== Establecimiento de un porcentaje de distribución de ciertos costes a cada centro de coste
Hay ciertos gastos presupuestarios centrales que se pueden asignar a zonas, edificios o complejos específicos. Son gastos de limpieza, seguridad y mantenimiento diversos (Audiovisual, Climatización, Fontanería, Electricidad, Obra Civil, Ascensores, Tabiquería modular, Puertas automáticas, Sistema gestión , Jardinería, Extinción incendios, Seguridad y Salud, GESTIÓN ENERGÉTICA, Servicio Ingeneria y Deliniante). Aquellos que se corresponden claramente con una aplicación presupuestaria se pueden repercutir a los centros de coste que tienen presencia en esa zona, edificio o complejo con el porcentaje de presencia que cada centro tiene en esa zona, edificio o complejo.

El fichero #ruta("distribución OTOP.xlsx") recoge el porcentaje de cada uno de esos gastos que se asigna a cada zona, edificio o complejo. Con las tablas que ya hemos calculado, de presencia de cada centro en cada zona, edificio y complejo, podemos asignar a cada centro de coste un porcentaje de cada uno de esos gastos presupuestarios centrales. Hay que ser cuidadoso porque un centro puede tener presencia en varias zonas, edificios o complejos, y el mismo gasto presupuestario puede estar asociado a varias zonas, edificios o complejos. En ese caso, el centro de coste acumula el porcentaje que le corresponde por cada zona, edificio o complejo.

Si un mismo prefijo tiene dos porcentajes, antes de empezar, simplifica eso con una sola fila con el mismo prefijo y el porcentaje que resulte de sumar los dos porcentajes.

Si queda algún porcentaje sin asignar a ningún centro de coste, ese porcentaje se ha de repartir entre los centros de coste en función del porcentaje que les ha tocado con el resto de información (parece que eso pasa con costes de la Residencia de Estudiantes).

La tabla resultante se llamará algo así como #val("porcentaje de coste de suministros por centro de coste").

En la #app, muestra una tabla con cada centro de coste y el porcentaje de coste de esos gastos que le corresponde, en sendas columnas. Una tercera columna recogerá el listado de zonas, edificios o complejos que le asignan ese coste, con el porcentaje que le asigna cada uno de ellos (resultado de multiplicar el coste de esa zona por el porcentaje de presencia del centro sobre el total de esa zona, edificio o complejo ---si se puede, muestra el producto y el resultado---), de modo que la suma se corresponda con el porcentaje asignado al centro.




=== Reglas para generar unidades de coste a partir de amortizaciones

Para cada línea hemos de ver la ubicación del bien y con ella llegar al centro.

La unidad de coste se forma con:

- importe: el importe de amortización del bien para el año analizado.

- centro de coste: el centro de coste al que corresponde la #campo("id_ubicación") del bien,

    #reglas[
        - #nombre-regla[Sin ubicación, pero descripción informativa]
            Si no tiene #campo("id_ubicación"), podemos asignar un centro en función de la existencia de una cadena dentro de #campo("descripción") (case insensitive y sin prestar atención a acentos):
            - si #campo("descripción") contiene #val("rectorado") y #val("biblioteca"), el 50% va a #etqcen("rectorado") y el 50% a #etqcen("bibliotecas")
            - si #campo("descripción") contiene #val("humanas") o #val("fchs"), asignamos el centro de coste #etqcen("fchs")
            - si #campo("descripción") contiene #val("jurid") o #val("jj") o #val("jco") o #val("jc"), es #etqcen("fcje")
            - si #campo("descripción") contiene #val("salud"), es #etqcen("fcs")
            - si #campo("descripción") contiene #val("tecnología") o #val("ciencias experimentales") o #val("cientifico") o #val("tec") o #val("talleres") o #val("estce") o #val("tc"), es #etqcen("estce")
            - si #campo("descripción") contiene #val("rectorat") o #val("rectorado"), es #etqcen("rectorado")
            - si #campo("descripción") contiene #val("itc"), es #etqcen("iutc")
            - si #campo("descripción") contiene #val("lonja") o #val("llotja"), es #etqcen("llotja-cànem")
            - si #campo("descripción") contiene #val("biblioteca"), es #etqcen("bibliotecas")
            - si #campo("descripción") contiene #val("consell"), es #etqcen("consejo-social")
            - si #campo("descripción") contiene #val("modulo ti") o #val("estyce"), es #etqcen("estce")
            - si #campo("descripción") contiene #val("scic"), es #etqcen("scic")
            - si #campo("descripción") contiene #val("deport"), es #etqcen("se")
            - si #campo("descripción") contiene #val("paran"), es #etqcen("paraninfo")
            - si #campo("descripción") contiene #val("nb") o #val("investigacio ii"), es #etqcen("edificio-investigación-2")
            - si #campo("descripción") contiene #val("piscina"), es #etqcen("se")
            - si #campo("descripción") contiene #val("mat") o #val("inf"), es 33% #etqcen("dmat") y 34% #etqcen("dlsi") y 33% #etqcen("dicc")
            - si #campo("descripción") contiene #val("central del parc"), es #etqcen("edificio-espaitec-2")
            - si #campo("descripción") contiene #val("animal"), es #etqcen("sea")
            - si #campo("descripción") contiene #val("docente") o #val("direccion obra") y año de #campo("fecha alta") es #val("1998"), es #etqcen("estce")
            - si #campo("descripción") contiene #val("residencia"), es #etqcen("residencia-universitaria")
            - si #campo("descripción") contiene #val("urbaniza"), es #etqcen("urbanización")
    ]

- elemento de coste: se asignan según la #campo("cuenta"), de acuerdo con esta tabla:

    #align(center, table(
        columns: 3,
        align: (left, left, left),

        table.header(
            table.hline(),
            campo("cuenta"), campo("elemento de coste"), [Nombre cuenta],
            table.hline(),
        ),
        val("2020"), etqele("amortización-construcciones"), [EDIFICIOS Y OTRAS CONSTRUCCIONES],
        val("2021"), etqele("amortización-construcciones"), [ADMINISTRATIVOS],
        val("2022"), etqele("amortización-construcciones"), [COMERCIALES],
        val("2023"), etqele("amortización-construcciones"), [OTRAS CONSTRUCCIONES],
        val("2024"), etqele("amortización-construcciones"), [EDUCATIVOS-CULTURALES],
        val("2025"), etqele("amortización-construcciones"), [SANITARIOS],
        val("2026"), etqele("amortización-construcciones"), [RESIDENCIA DE ESTUDIANTES (REGIMEN CONCESION)],
        val("2027"), etqele("amortización-construcciones"), [LOCALES AGORA UNIVERSITARIA],
        val("2030"), etqele("amortización-maquinaria"), [MAQUINARIA],
        val("2031"), etqele("amortización-instalaciones"), [INSTALACIONES],
        val("2032"), etqele("amortización-transporte"), [ELEMENTOS DE TRANSPORTE INTERNO],
        val("2033"), etqele("amortización-utillaje"), [UTILES Y HERRAMIENTAS],
        val("2034"), etqele("amortización-utillaje"), [INSTALACIONES Y MATERIAL DEPORTIVO],
        val("2035"), etqele("amortización-utillaje"), [EQUIPAMIENTO DE AUDIOVISUALES],
        val("2040"), etqele("amortización-transporte"), [AUTOMOVIL],
        val("2050"), etqele("amortización-mobiliario"), [MOBILIARIO],
        val("2051"), etqele("amortización-otro-inmovilizado-material"), [EQUIPOS DE OFICINA],
        val("2052"), etqele("amortización-mobiliario"), [MATERIAL DE OFICINA],
        val("2056"), etqele("amortización-mobiliario"), [MOBILIARIO RESIDENCIA DE ESTUDIANTES (REGIMEN CONCESION)],
        val("2059"), etqele("amortización-mobiliario"), [MOBILIARIO ARTISTICO],
        val("2060"), etqele("amortización-equipos-informáticos"), [EQUIPOS PARA PROCESO DE LA INFORMACION],
        val("2110"), etqele("amortización-construcciones"), [EDIFICIOS Y OTRAS CONSTRUCCIONES],
        val("2111"), etqele("amortización-construcciones"), [ADMINISTRATIVOS],
        val("2112"), etqele("amortización-construcciones"), [COMERCIALES],
        val("2113"), etqele("amortización-construcciones"), [OTRAS CONSTRUCCIONES],
        val("2114"), etqele("amortización-construcciones"), [EDUCATIVOS-CULTURALES],
        val("2115"), etqele("amortización-construcciones"), [RESIDENCIA DE ESTUDIANTES (REGIMEN CONCESION)],
        val("2117"), etqele("amortización-construcciones"), [LOCALES AGORA UNIVERSITARIA],
        val("2140"), etqele("amortización-maquinaria"), [MAQUINARIA],
        val("2141"), etqele("amortización-instalaciones"), [INSTALACIONES],
        val("2142"), etqele("amortización-transporte"), [ELEMENTOS DE TRANSPORTE INTERNO],
        val("2143"), etqele("amortización-utillaje"), [UTILES Y HERRAMIENTAS],
        val("2144"), etqele("amortización-utillaje"), [INSTALACIONES Y MATERIAL DEPORTIVO],
        val("2145"), etqele("amortización-utillaje"), [EQUIPAMIENTO DE AUDIOVISUALES],
        val("2150"), etqele("amortización-aplicaciones-informáticas"), [LICENCIAS DE USO DE SOFTWARE],
        val("2160"), etqele("amortización-mobiliario"), [MOBILIARIO],
        val("2161"), etqele("amortización-otro-inmovilizado-material"), [EQUIPOS DE OFICINA],
        val("2162"), etqele("amortización-mobiliario"), [MATERIAL DE OFICINA],
        val("2165"), etqele("amortización-mobiliario"), [MOBILIARIO RESIDENCIA DE ESTUDIANTES (REGIMEN CONCESION)],
        val("2169"), etqele("amortización-mobiliario"), [MOBILIARIO ARTISTICO],
        val("2170"), etqele("amortización-equipos-informáticos"), [EQUIPOS PARA PROCESO DE LA INFORMACIÓN],
    ))

    - actividad: se asigna a todos #etqact("dags")

En la #app informa de cuántos casos han quedado sin poder cumplimentar.

==== Reparto cuando un bien tiene presencia en varios centros

Un mismo bien de inventario puede aparecer asociado a varias ubicaciones (porque la base de datos refleja el reparto físico real: un equipo informático compartido, mobiliario en una sala mancomunada, etc.) y, por tanto, a varios centros de coste. En ese caso, en lugar de generar una única unidad de coste, se generan tantas unidades de coste como centros distintos haya, repartiendo el #campo("importe") amortizado a partes iguales entre ellas.

Cada una de las unidades generadas comparte el mismo #campo("origen_id") (el identificador del bien en el inventario) y lleva un #campo("origen_porción") = #val("1/n"), donde #val("n") es el número de centros distintos asociados al bien. Esto permite reconstruir, si hace falta, el coste total del bien sumando las porciones; y deja claro en el visor por qué un mismo identificador de inventario aparece varias veces en las unidades de coste de amortizaciones. La #app informa del número de bienes con presencia en más de un centro.



== Generación de unidades de coste a partir de información de nóminas

#figure(
    align(center, etapa-nominas()),
    caption: [Etapa de nóminas: ficheros de entrada y salidas que produce.],
)

=== Preprocesamiento nóminas

==== Filtros previos de saneamiento

Antes de agrupar nada, las nóminas pasan por una cadena de filtros que descartan cobros que NO corresponden a actividad imputable del ejercicio. El orden en que se aplican es el siguiente.

/ Atrasos a personal no vinculado: : Una persona se considera *no vinculada* a la UJI en el año analizado si todas sus líneas de nómina del año caen en concepto retributivo #val("30") o #val("87") (atrasos) y no tiene ninguna línea con otro CR no nulo. Son típicamente personas que ya no trabajan en la UJI pero cobran un pago retroactivo por un ejercicio anterior. Sus importes NO entran al reparto y se persisten en #ruta("auxiliares", "nóminas", "atrasos_no_vinculados.parquet") con detalle (per_id, sectores, expedientes, nº meses, nº líneas, importe total). Se exponen en la #app bajo *Personal · Atrasos a no vinculados*. En 2025 son #val("≈ 110") personas y #val("≈ 8 800 €").

/ CR 19/64 sin cargo asimilable activo: : Líneas con concepto retributivo #val("19") o #val("64") en proyecto general cobradas por personas que NO tienen ningún cargo asimilado al RD 1086/1989 vigente en el año (todos sus cargos asimilables han cerrado antes del ejercicio). Son atrasos / regularizaciones tardías de cargos ya extinguidos: no corresponden a actividad imputable. Se filtran de la nómina y se persisten en #ruta("auxiliares", "nóminas", "cr_19_64_sin_cargo_activo.parquet").

/ Expedientes sin retribución de personal: : Tras los filtros anteriores, se descartan los expedientes cuyo único cobro del año cae en alguna de estas dos bolsas:

    - *SS cotizada* (#campo("aplicación") empieza por #val("12")): si un expediente solo tiene cotización de Seguridad Social por parte del empleador pero ninguna línea retributiva al trabajador, no hay actividad imputable (regularizaciones residuales de SS por nóminas del ejercicio anterior).
    - *Capítulo 4* (#campo("aplicación") empieza por #val("4")): son transferencias corrientes — becarios. No son personal PDI/PVI/PTGAS.

    Se filtran tanto el expediente del listado como sus líneas de la nómina, de modo que las etapas posteriores no las vean. En 2025 son #val("≈ 150") expedientes adicionales y #val("≈ 8 400 €") de capítulo 4 que quedan fuera del coste analítico de personal.

Adicionalmente, los cargadores de la regla 23 (POD, tesis, cargos, proyectos, grupos) descartan al final cualquier #campo("per_id") sin nómina vinculada en el año, de modo que no aparecen en #ruta("regla23", "dedicación_pdi.parquet") personas que se hayan colado por POD u otras fuentes sin tener cobro activo en el año.

==== Reducciones por representación sindical

Algunos miembros del personal disfrutan de una reducción de jornada por *representación sindical* — total (liberados al 100 %) o parcial. El coste que dejan de aportar a su actividad ordinaria se imputa al centro #etqcen("locales-sindicales") y a la actividad #etqact("acción-sindical"). Hay *dos mecanismos independientes y disjuntos por sector*: el PTGAS se mide en días y porcentaje de jornada (tipo 8 de #ruta("entrada", "nóminas", "reducciones laborales.xlsx")) y el PDI en créditos de reducción docente (tipos 37-40 de #ruta("entrada", "reducciones pdi", "reducciones docentes.xlsx")).

*PTGAS — tipo 8.* De #ruta("entrada", "nóminas", "reducciones laborales.xlsx"), histórico de todas las reducciones laborales, tomamos solo las filas con #campo("tipo reduccion") = #val("8") (representación sindical) que solapan el año analizado. Las demás (lactancia, cuidado de hijos, etc.) se ignoran.

*Factor anual X por expediente.* Para cada expediente con al menos un día de reducción tipo 8 en el año, calculamos el factor anual ponderado por días:

$ X_"anual" = (sum_i d_i times p_i) + (D - sum_i d_i) / D $

donde $d_i$ es el número de días de solape del periodo de reducción $i$ con el año, $p_i$ es el #campo("porcentaje trabajado") de esa reducción (#val("0") si está vacío) y $D$ son los días del año ($365$ o $366$). Los días sin reducción aportan $p = 1$. El resultado $X_"anual" in [0, 1]$ es la fracción anual efectivamente trabajada por la persona en ese expediente. Si la persona no aparece en el fichero, $X = 1$ implícitamente.

*Aplicación al PTGAS.* Las UC retributivas ordinarias del PTGAS (proyectos en TABLA-PROYECTOS-GENERALES-NÓMINA) se dividen en dos:

- $X times "importe"$ se imputa a la actividad y centro de coste habituales (servicio → CC, servicio → actividad).
- $(1 - X) times "importe"$ se imputa a #etqact("acción-sindical") en #etqcen("locales-sindicales"), conservando el mismo #campo("elemento_de_coste").

Las UC retributivas *extras* (artículo 60 y otros proyectos no generales) son finalistas y no se tocan.

El helper que calcula $X$ está acotado a los expedientes PDI/PVI además de a los de PTGAS, pero en la práctica #ruta("reducciones laborales.xlsx") solo contiene PTGAS: la reducción sindical del PDI viene por la vía de los créditos que se describe a continuación.

*PDI — tipos 37-40.* La representación sindical del PDI se informa en la carpeta #ruta("entrada", "reducciones pdi") en *créditos* de reducción docente. Los tipos de reducción #val("37") (UGT), #val("38") (STEPV), #val("39") (CCOO) y #val("40") (CSI-F) de #ruta("reducciones docentes.xlsx") son los sindicales.

*Fracción de jornada sindical.* Para cada PDI con reducción sindical en el curso analizado:

$ f_"sind" = c_"sind" / (c_"cap" - c_"red" + c_"sind") $

donde $c_"sind"$ son los créditos sindicales (suma de los tipos 37-40 en #ruta("reducciones docentes.xlsx")), y $c_"cap"$ (#campo("creditos")) y $c_"red"$ (#campo("creditos reduccion")) son la capacidad y la reducción docente total de la persona en #ruta("carga docente.xlsx"). Como #campo("creditos reduccion") ya incluye los créditos sindicales, el denominador es la docencia neta impartida ($c_"cap" - c_"red"$) más los créditos sindicales: la fracción mide qué parte de la actividad «docencia + sindicato» es sindicato. Se acota a $[0, 1]$ y, junto con las horas $f_"sind" times "JORNADA_ANUAL_PDI"$, se persiste en #ruta("regla23", "reducciones_sindicales_pdi.parquet").

*Integración en la regla 23.* La fracción sindical es un destino más de la dedicación de la persona:

- La jornada de reparto de las fases 5-7 pasa de $T = "JORNADA_ANUAL_PDI"$ a $X_"persona" times T$, con $X_"persona" = 1 - f_"sind"$: el reparto entre docencia, gestión e investigación opera sobre la jornada *no* sindical.
- Se emite una fila en #ruta("regla23", "dedicación_pdi_normalizada.parquet") con actividad #etqact("acción-sindical"), centro #etqcen("locales-sindicales") y $#campo("horas_finales") = f_"sind" times "JORNADA_ANUAL_PDI" times "fracción_año"$, de modo que la suma de #campo("horas_finales") de la persona sigue siendo su jornada disponible.
- La masa regla 23 se reparte en proporción a #campo("horas_finales"); al ser la fila sindical un peso más, la fracción correspondiente de la masa se imputa automáticamente a #etqact("acción-sindical") / #etqcen("locales-sindicales").

*Figuras puramente docentes (associats y substituts).* Para un professor associat (PAA/PAL) o substitut (PS) la regla 23 no reparte entre grupos: toda su jornada va a docencia. Si además es representante sindical, la fracción sindical se separa igual y el resto, $(1 - f_"sind") times "JORNADA_ANUAL_PDI"$, va íntegro a docencia (sin gestión ni investigación).

*Prioridad de la reducción sindical.* El sindicato manda: la fracción sindical se descuenta primero y lo que queda, $T = X_"persona" times "JORNADA_ANUAL_PDI"$, es la jornada que reparte la regla 23. De ahí dos casos:

- Cuando la docencia neta es nula ($c_"cap" = c_"red"$) el denominador colapsa a $c_"sind"$ y la fracción es #val("1"), por pequeña que sea la reducción sindical; se acepta así, sin corrección. La persona está liberada al 100 % ($X_"persona" = 0$): toda su jornada va a #etqact("acción-sindical") y su docencia, gestión e investigación quedan sin dedicación final — imparte clase, pero sin coste imputable a docencia. Se marca con la anomalía «liberación sindical del 100 %».
- Cuando la fracción es parcial pero la docencia y la gestión iniciales superan $T$, se escalan ambas proporcionalmente para caber en $T$ (la investigación queda en 0). Se marca con «docencia y gestión escaladas a la jornada no sindical».

En ambos casos la suma de #campo("horas_finales") de la persona es exactamente la jornada anual. Para el PDI #emph[sin] reducción sindical, un exceso de docencia + gestión sobre la jornada sigue siendo una anomalía que se reporta sin corregir.

*Cargos académicos.* La masa del cargo (CR 19/64 + parte extra del CR 68 en proyecto general) *no* se reduce: la coexistencia de cargo asimilado al RD y representación sindical es excepcional y el cargo manda. Las UC del cargo se generan con su lógica propia.

*Costes sociales.* La SS cotizada y los costes calculados de funcionarios no se procesan aparte: el reparto SS se hace por persona en proporción a sus UC retributivas, que ya están divididas (sindical / habitual). En consecuencia, la SS hereda automáticamente la fracción sindical sin código adicional.

*Cifras de referencia 2025.* PTGAS: #val("21") expedientes con reducción tipo 8 — todos del sector PAS —, que generan #val("201") UC sindicales por importe total #val("423 108,49 €") (incluye #val("86 734 €") de SS cotizada). PDI: #val("44") representantes sindicales (tipos 37-40), de los que #val("5") son profesores asociados; suman #val("26 978") horas imputadas a #etqact("acción-sindical") y #val("875 747 €") de masa regla 23 repartida a #etqcen("locales-sindicales").

==== Agrupamiento por expediente y sector

En primer lugar, vamos a agrupar todas la entradas de #ruta("nóminas y seguridad social.xlsx") por #campo("expediente"). Los expedientes se van a clasificar en una lista (o tabla) de PDI y PVI (el PVI está codificado como sector PI) y otra de PTGAS. Solo han de considerarse expedientes con alguna retribución en el ejercicio que estamos considerando.

En la #app, quiero poder ver, por separado, los expedientes de cada uno de estos sectores. Si aparece algún expediente que no se pueda clasificar en ninguno de estos sectores, quiero poder verlo también para analizarlo.

También quiero ver en la #app la relación de personas que tiene más de un expediente de tipos distintos. En una entrada de menú aparecerá Multiexpediente y dentro, en un tabs, tendré: PTGAS + PDI, PTGAS + PVI, PDI + PVI, PTGAS + PDI + PDI. En cada uno de esos tabs, veré la relación de personas que tienen expedientes de ambos tipos, con el número de expedientes de cada tipo que tienen, para analizar si es correcto o no que tengan expedientes de ambos tipos. Al seleccionar la persona, veré los doce meses del año y en qué meses tenía activos que expedientes (número y colectivo al que pertenece).

Cada una de esas listas (PDI+PVI y PTGAS) se va a procesar de un modo distinto, lo que vamos a describir en los siguientes apartados.

==== Agrupamiento de los registros

===== PTGAS

Cada expediente del PTGAS tendrá las siguientes pestañas en la #app, en este orden:

+ #campo("Retribuciones ordinarias (sin CR 48)"): nómina ordinaria cuando #campo("proyecto") es #val("1G019"), #val("23G019"), #val("02G041"), #val("11G006"), #val("1G046") o #val("00000"), exceptuando el concepto retributivo #val("48").
+ #campo("Retribuciones ordinarias CR 48"): líneas con concepto retributivo #val("48") (indemnizaciones por asistencias), independientemente del proyecto. Su tratamiento es uniforme (actividad #etqact("dag-sgc-indemnizaciones-asistencias")).
+ #campo("Retribuciones extra"): el resto de líneas no encuadradas en las anteriores.
+ #campo("Costes sociales"): registros asociados a la Seguridad Social (#campo("aplicación") que empieza por #val("12")).

Las pestañas vacías se ocultan.

Debajo del bloque de pestañas, una tabla independiente *Unidades de coste generadas* lista todas las UC creadas para el expediente: retributivas (#ruta("auxiliares", "nóminas", "uc_ptgas.parquet")), indemnizaciones por asistencias (#ruta("auxiliares", "nóminas", "uc_indemnizaciones_asistencias.parquet")) y la parte de las UC de seguridad social (#ruta("auxiliares", "nóminas", "persona_uc.parquet") con #campo("tipo") = #val("coste social")) que corresponde al per_id del expediente. Sirve para verificar la clasificación y evitar duplicidades sin tener que cambiar de pantalla.

La #app mostrará los totales de cada pestaña y comprobará que la suma de las cuatro coincide con el total de la nómina del expediente, para detectar errores en la clasificación.

===== PDI + PVI

Sea TABLA-PROYECTOS-GENERALES-NÓMINA esta serie de proyectos:

#table(
    columns: 2,
    stroke: none,
    table.header(
        table.hline(),
        [*Proyecto*],
        [*Descripción*],
        table.hline(),
    ),
    val("00000"), [Proyecto general],
    val("02G041"), [Retribución a profesarado por gestión de intercambios],
    val("11G006"), [Plan de acciones de gobierno],
    val("1G019"), [Plantilla universidad],
    val("1G046"), [Incentivos PDI],
    val("23G019"), [Fondo de contingencia para despidos],
    table.hline(),
)

Cada expediente del PDI/PVI tendrá las siguientes pestañas en la #app, en este orden:

+ #campo("Retribuciones ordinarias para regla 23"): la "bolsa gorda" — líneas con proyecto en TABLA-PROYECTOS-GENERALES-NÓMINA cuyo concepto retributivo no es #val("47"), #val("48"), #val("19") ni #val("64"). Incluye los atrasos (#val("30") y #val("87")), que no se separan en bolsa propia.
+ #campo("Ordinarias despidos (CR 47)"): proyecto en TABLA-PROYECTOS-GENERALES-NÓMINA y concepto retributivo #val("47").
+ #campo("Ordinarias indemnizaciones por asistencias (CR 48)"): concepto retributivo #val("48"), independientemente del proyecto (su tratamiento es uniforme: actividad #etqact("dag-sgc-indemnizaciones-asistencias")).
+ #campo("Ordinarias cargos (CR 19/64)"): proyecto en TABLA-PROYECTOS-GENERALES-NÓMINA y concepto retributivo #val("19") o #val("64").
+ #campo("Retribuciones extra"): líneas con proyecto NO incluido en TABLA-PROYECTOS-GENERALES-NÓMINA (excluyendo las CR 48, que ya aparecen en su pestaña).
+ #campo("Costes sociales"): registros con aplicación presupuestaria que empieza por #val("12"). Para los trabajadores en el régimen de clases pasivas (los que no tienen líneas con aplicación que empiece por #val("12")) esta pestaña se sustituye por #campo("Costes sociales calculados") con el detalle del cálculo simulado.

Las pestañas vacías se ocultan automáticamente, de modo que un expediente sin atrasos ni despidos, por ejemplo, solo verá las pestañas con contenido.

Debajo del bloque de pestañas, una tabla independiente *Unidades de coste generadas* consolida todas las UC ya creadas para el expediente. Incluye:

- UC retributivas individuales de #ruta("auxiliares", "nóminas", "uc_pdi.parquet") o #ruta("auxiliares", "nóminas", "uc_pvi.parquet").
- UC de despidos en proyecto general de #ruta("auxiliares", "nóminas", "uc_despidos.parquet").
- UC de indemnizaciones por asistencias de #ruta("auxiliares", "nóminas", "uc_indemnizaciones_asistencias.parquet").
- UC de cargos académicos en proyecto específico de #ruta("auxiliares", "nóminas", "uc_cargos.parquet").
- UC del reparto de cargos académicos en proyecto general (CR 19/64 + parte extra del CR 68) de #ruta("auxiliares", "nóminas", "cargos_uc.parquet"), asociadas al expediente vía #campo("per_id").
- UC del reparto de seguridad social (real o calculada) de #ruta("auxiliares", "nóminas", "persona_uc.parquet") con #campo("tipo") = #val("coste social"), también vía #campo("per_id").

En este momento, el único concepto que queda *sin* generar UC es la masa destinada a regla 23 (la bolsa gorda de la primera pestaña), pendiente de la fase de reparto por dedicación. Los costes sociales que aparecen son provisionales: sus importes se reajustarán cuando se cierre el reparto definitivo.


=== Decisión del elemento de coste a partir de los registros de la nómina

==== Tabla para determinar el elemento de coste a partir del concepto retributivo

Esta tabla, que llamamos TABLA-CONCEPTO-A-ELEMENTO, se va a usar para determinar parte del elemento de coste de las unidades de coste que se van a generar a partir de los registros de nómina. Es común a todos (PDI+PVI y PTGAS) La tabla es la siguiente, teniendo en cuenta que el concepto retributivo #val("48") (indemnización por asistencias) va a tener reglas propias:

#table(
    columns: 3,
    align: (center, left, center),

    table.header(
        table.hline(),
        [Concepto retributivo], [Descripción], [Etiqueta para elemento de coste],
        table.hline(),
    ),
    [#val("01")], [Sou base], [#val("sueldo")],
    [#val("03")], [Triennis], [#val("trienios")],
    [#val("04")], [Paga extraordinària], [#val("paga-extra")],
    [#val("05")], [Paga addicional complement específic], [#val("esp")],
    [#val("06")], [Component compensatori del complement específic], [#val("esp")],
    [#val("10")], [Complement de destinació], [#val("dst")],
    [#val("12")], [Complement de destinació ajudants], [#val("dst")],
    [#val("13")], [Complement art. 55.2 LOU], [#val("otvars")],
    [#val("15")], [Complement específic del lloc de treball], [#val("esp")],
    [#val("17")], [Complement associats Consell de Govern 26/05/08], [#val("otfij")],
    [#val("18")], [Complement específic professorat], [#val("esp")],
    [#val("19")], [Complement específic per càrrecs acadèmics (Docents)], [#val("cargos")],
    [#val("20")], [Complement per mèrits docents], [#val("quin")],
    [#val("24")], [Complement de destinació professorat associat], [#val("dst")],
    [#val("25")], [Complement activitat professional (no docent)], [#val("otvars")],
    [#val("26")], [Complement de productivitat per investigació], [#val("sexinv")],
    [#val("30")], [Endarreriments Carrecs], [#val("cargos")],
    [#val("32")], [Complement activitat professional PTGAS], [#val("prod")],
    [#val("34")], [Component d'exercici lloc de treball LD], [#val("otfij")],
    [#val("35")], [Rendiments de llicència de patents], [#val("otvars")],
    [#val("43")], [Millora addicional], [#val("otvars")],
    [#val("44")], [Complement per antiguitat], [#val("trienios")],
    [#val("47")], [Indemnització finalització contracte laboral], [#val("otvars")],
    // [#val("48")], [Indemnització per assistències], [#val("otvars")],
    [#val("53")], [Gratificació per serveis de caràcter extraordinari (art.60 LOSU)], [#val("otvars")],
    [#val("55")], [Gratificacions per serveis de caràcter extraordinari], [#val("otvars")],
    [#val("56")], [Complement específic C Sanitat], [#val("esp")],
    [#val("57")], [Complement carrera professional Sanitat], [#val("otfij")],
    [#val("59")], [Complement de destinació professorat visitant], [#val("dst")],
    [#val("62")], [Despeses de trasllat], [#val("otvars")],
    [#val("64")], [Retribució addicional mèrit individual projectes UE (art.76 LOSU)], [#val("otvars")],
    [#val("67")], [Retribució addicional del professorat universitari. Decret 174/2002], [#val("otvars")],
    [#val("68")], [Paga addicional complement específic pdi], [#val("esp")],
    [#val("70")], [Retribució addicional mèrit indicvidual càtedres i aules (art.76 LOSU)], [#val("otvars")],
    [#val("71")], [Complement per càrrec de gerent], [#val("esp")],
    [#val("72")], [Antiguitat Conselleria de Sanitat], [#val("trienios")],
    [#val("75")], [Complement carrera professional], [#val("cprof")],
    [#val("76")], [Complement compensatòri carrera professional], [#val("cprof")],
    [#val("77")], [Complement de productivitat per transferència], [#val("sextransf")],
    [#val("78")], [Col.laboració en projectes d'investigació], [#val("otvars")],
    [#val("80")], [Ajust nòmines], [#val("otvars")],
    [#val("82")], [Sou Base], [#val("sueldo")],
    [#val("83")], [Deducció per vaga], [#val("otvars")],
    [#val("86")], [Diferència mèrits docents RD 1086/89 art 5.9], [#val("quin")],
    [#val("87")], [Endarreriments], [#val("otvars")],
    [#val("90")], [Retribució per cursos impartits], [#val("otvars")],
    [#val("98")], [Diferència valor triennis Llei 1/96], [#val("trienios")],
    [#val("99")], [Complement per mèrits docents no universitaris], [#val("quin")],
    table.hline(),
)

==== Reglas para determinar el elemento de coste
#nota[¿y, para el PTGAS, estamos determinando el resto de componentes (centro y actividad)?]

Hay un regla previa al resto, que son más complejas, para quitarnos de encima las indemnizaciones por asistencias:

#reglas[
    - todas las retribuciones correspondientes al #campo("concepto retributivo") #val("48") (Indemnización por asistencias), tanto las correspondientes a PTGAS como a PDI o PVI se imputarán al elemento de coste #etqele("otras-indemnizaciones").
]

Y ahora vamos con una forma de construcción del elemento de coste que los considera formado por tres componentes:

#reglas[
    - En primer lugar hemos de agrupar los registros que hemos puesto en #campo("retribuciones ordinarias") por el par `elemento de coste`-`servicio` (una `persona`, desde un expediente, puede haber trabajado en más de un servicio a lo largo del año).

    La etiqueta del elemento de coste tiene la forma `ZZZ-XXX-YYY`, donde
    - `ZZZ` depende del sector:
    - `XXX` depende de la categoría u otros campos,
    - e `YYY` depende del tipo de retribución.
]

Para determinar `ZZZ` hemos de prestar atención al sector:

#reglas[
    - el sector PTGAS se corresponde con `ptgas`
    - el sector PDI se corresponde con `pdi`
    - el sector PVI se corresponde con `piyotper`
]

Para determinar el valor de `XXX`:

#reglas[
    - En el caso del PTGAS, miramos el campo #campo("categoría") del registro. Estas son las reglas:

        - Si el valor es #val("FC") y el #campo("per_id") es #val("65214") (AMV), `XXX` es #val("dir").

        - Si no, si el valor es #val("FC") o #val("FI"), `XXX` es #val("func").

        - Si no, si el valor es #val("E"), `XXX` es #val("ev").

        - Si no, si el valor es #val("LF") (laboral fijo), `XXX` es #val("labfijo").

        - Si no, si el valor es #val("LT") (laboral temporal) o #val("LE") (laboral eventual), `XXX` es #val("labtemp"). LE y LT comparten subtipo porque ambos son no-fijos.

        - Si no, marca un error, porque no debería de pasar.

    - En el caso del PVI o PDI, los campos relevantes son #campo("categoría"), #campo("perceptor") y #campo("provisión"). Estas son las reglas (las celdas en blanco significan que no importa el valor de ese campo):

        #table(
            columns: 6,
            table.header(
                table.hline(),
                [#campo("categoría")],
                [#campo("perceptor")],
                [#campo("provisión")],
                [#campo("categoría_plaza")],
                [#campo("sector_plaza")],
                [Valor de `XXX`],
                table.hline(),
            ),
            table.cell(colspan: 6, align: center)[_PVI — se aplica la primera regla que encaja_],
            [], [], val("P4"), [], [], val("act"),
            [], val("35"), [], [], [], val("act"),
            [], [], [], val("41J"), val("PI"), val("act"),
            [], [], [], val("41S"), val("PI"), val("act"),
            val("PREDO"), [], [], [], [], val("pif"),
            [], [], val("PD"), [], [], val("pif"),
            [], [], val("P2"), [], [], val("idi"),
            [], [], [], [], [], val("pid"),
            table.hline(),
            table.cell(colspan: 6, align: center)[_PDI — por #campo("categoría") exacta_],
            val("CU"), [], [], [], [], val("cu"),
            val("TU"), [], [], [], [], val("tu"),
            val("TUI"), [], [], [], [], val("tu"),
            val("CEU"), [], [], [], [], val("ceu"),
            val("TEU"), [], [], [], [], val("teu"),
            val("AJ"), [], [], [], [], val("aj"),
            val("AJD"), [], [], [], [], val("aj"),
            val("AJDII"), [], [], [], [], val("aj"),
            val("PAA"), [], [], [], [], val("as"),
            val("PAL"), [], [], [], [], val("as"),
            val("PS"), [], [], [], [], val("ps"),
            val("PEME"), [], [], [], [], val("em"),
            val("PPL"), [], [], [], [], val("pl"),
            val("PPLV"), [], [], [], [], val("pl"),
            val("PVI"), [], [], [], [], val("pv"),
            val("PD"), [], [], [], [], val("pd"),
            val("PCD"), [], [], [], [], val("pcd"),
            val("PC"), [], [], [], [], val("pc"),
            table.hline(),
        )
]

Para determinar el valor de `YYY`:
#reglas[
    - Hay que mirar el campo #campo("concepto_retributivo") del registro y usar la tabla que hemos definido antes para determinar la etiqueta del elemento de coste a partir del concepto retributivo. Por ejemplo, si el concepto retributivo es #val("01"), el valor de `YYY` es #val("sueldo"). Si el concepto retributivo es #val("03"), el valor de `YYY` es #val("trienios"). Y así sucesivamente.

    Excepción: el personal con contrato de actividades científico-técnicas (`XXX` = #val("act") en PVI) no tiene rama de #val("cprof") (carrera profesional) en el árbol de elementos de coste. Para esa categoría, los conceptos retributivos #val("75") y #val("76") (carrera profesional) usan #val("otvars") como `YYY` en lugar de #val("cprof").

    Si el #campo("concepto_retributivo") del registro no aparece en la tabla, o si el #campo("categoría") del registro no permite resolver `XXX` con las reglas previas, el registro se descarta de la generación de UC y se reporta como aviso en la #app (con el número de registros e importe agregado), para que el analista pueda decidir si la tabla debe ampliarse. Es decir, la falta de entrada en la tabla no detiene el proceso, pero queda explícitamente registrada en lugar de propagarse silenciosamente.

    Cuando generas un etiqueta `ZZZ-XXX-YYY` has de comprobar que existe en el árbol de elementos de coste. Si no existe (por ejemplo, porque combinaciones perfectamente válidas en aislado dan una etiqueta no contemplada en el árbol), has de dar un error que sí detenga el proceso, porque señala una incoherencia entre las reglas y el árbol de elementos de coste que requiere intervención antes de continuar.

    Para cada par elemento-servicio, tomamos sus registros de #campo("retribuciones ordinarias") y hacemos la suma, porque de cada par elemento-servicio, para ese expediente, vamos a crear una unidad de coste.

    // A continuación, mapeamos cada servicio al centro de coste y a la actividad que le corresponden usando la tabla de la regla #nombre-regla[Por servicio existe servicio y el proyecto uno de la línea] (en la sección «Preparación de un módulo para clasificar centros de coste»). Esa tabla da, para cada #campo("servicio"), un par (#campo("centro_de_coste"), #campo("actividad")) que es la asignación más precisa que tenemos. Si para algún servicio la actividad de la tabla está vacía o no hay entrada en la tabla, se usa #etqact("dag-general-universidad") como atrapalotodo (es decir, el «sitio donde aparcar» el coste cuando no se ha conseguido mayor precisión).

    // Caso especial: para el servicio #val("368") (personal de suport, conserjerías), la tabla anterior no es suficiente porque el coste se imputa físicamente al centro donde la persona presta el servicio, no al servicio nominal. En ese caso se usa el #campo("centro_plaza") del registro de nómina y una tabla análoga (TABLA-CENTROPLAZA→CC) que mapea cada #campo("centro_plaza") a su par (CC, actividad). Si ahí tampoco hay entrada, se aplica también #etqact("dag-general-universidad").

    // El importe de la unidad de coste creada es el importe total de las retribuciones ordinarias del par elemento-servicio (o elemento-centro_plaza para el servicio #val("368")).

    // La granularidad efectiva de agrupación es (#campo("expediente"), elemento de coste, #campo("servicio") o #campo("centro_plaza"), centro de coste): si el clasificador asigna centros de coste distintos a filas distintas con el mismo servicio, se generan tantas UC como pares (CC, servicio) distintos. El #campo("origen_id") tiene la forma `PTGAS-exp-{N}-srv-{N}` para servicios distintos a 368, y `PTGAS-exp-{N}-srv-368-cp-{N}` para el servicio 368.

    // Para las #campo("retribuciones extra") (las que NO están en proyecto general), tanto el centro de coste como la actividad se determinan con los módulos de clasificación de presupuesto (no con la tabla servicio→CC), igual que se hace en el traductor de presupuesto. El elemento de coste se construye con el mismo esquema `ZZZ-XXX-YYY` que las ordinarias. Se genera una unidad de coste por fila de retribución extra. El #campo("origen_id") tiene la forma `PTGAS-extra-exp-{N}-{proyecto}`.

    // Si para alguna fila (ordinarias o extras) el clasificador no resuelve centro de coste o actividad, esa fila se descarta y se reporta en la #app como aviso (número de registros e importe agregado), análogamente a lo que se hace cuando no resuelven XXX o YYY.
]

// Las unidades de coste de seguridad social se crean *por persona* (no por expediente), agregando todos los expedientes que tenga la persona en el año. El detalle del cálculo se desarrolla en la sección «Tratamiento de las personas (mono o multiexpediente) para creación de unidades de coste de seguridad social». La idea es: para cada persona se conoce su coste social total cotizado en el año (suma de los registros con #campo("aplicación") que empieza por #val("12") en todos sus expedientes); y se conocen las unidades de coste retributivas que hemos generado para sus expedientes (con sus pares centro de coste, actividad). El coste social total se reparte proporcionalmente al peso retributivo de cada par (centro de coste, actividad).



// === Tratamiento del PVI y del PDI

// El agrupamiento de registros es común al de PTGAS (véase la sección «Preprocesamiento nóminas / Agrupamiento de los registros»).

// Vamos a generar unidades de costes con sus tres coordenadas: elemento de coste, centro de coste y actividad. Primero habrá unas unidades (de poco importe normalmente) que irán a unidades completamente definidas y luego nos quedara una masa económica normalmente grande que irá a unas reglas de reparto complejas: lo que denominamos #nombre-regla[Regla 23].

// ==== Tratamiento de atrasos

// Hay un problema con los atrasos (#campo("concepto_retributivo") igual a #val("30") o #val("87")). Puede ser una masa económica grande y su reparto no dependen, para cada expediente, de la actividad que haya desarrollado el profesor en ese año, sino de la actividad que haya desarrollado en años anteriores. Lo que haremos es agrupar todos esos pagos de atrasos en una bolsa común y apartarlos de la regla 23. Más adelante repartiremos todo lo que hay en esa bolsa con una distribución similar a la que se aplica en promedio a todo el PDI + PVI. De momento, por tanto, gestiona esa bolsa y en la #app muestra el total que hay en esa bolsa común de atrasos y que podamos ver el detalle.

// Después de esto, si algún expediente no ha tenido ingresos en el año, apártalo y, con la #app permiteme saber cuántos y cuáles has apartado. Además, destaca de algún modo expedientes que solo han tenido retribución por atrasos.

// ==== Tratamiento de despidos

// Cuando el #campo("concepto_retributivo") es #val("47"), estamos antes despidos que pueden financiarse con cargo a un proyecto específico o con cargo a fondos generales.

// Si el #campo("proyecto") es #val("23G019"), la actividad es #val("otras-ait-financiación-propia") y el centro de coste es #val("vi"). En otro caso, el centro de coste y la actividad se decide de acuerdo a los módulos que hemos usado en presupuesto y nóminas para decidir estos valores.

// ==== Tratamiento de indemnizaciones por asistencias (tribunales y otros)

// Cuando el #campo("concepto_retributivo") es #val("48"), estamos antes indemnizaciones por asistencias a tribunales y similares. Estas indemnizaciones se van al elemento de coste #val("otras-indemnizaciones"). #nota[Podemos refinar esto en función de la figura.]

// El centro de coste y la actividad de estas indemnizaciones se han de obtener del mismo modo que hacemos con el presupuesto y nóminas. Aquí, el proyecto es importante, pero usa las tablas de información que han servido en otras ocasiones para decidir centro de coste y actividad.

==== Tratamiento de costes sociales calculados

Solo en el caso del PDI funcionario (categorías #val("CU"), #val("TU"), #val("TEU") o #val("CEU")) se da el caso de personas que no están en el régimen de Seguridad Social, es decir, en su nómina no aparece gasto de la aplicación presupuestaria empezando por #val("12"), y para ellos hay que calcular una unidad de coste que es el coste que tendríamos que asumir por este concepto (coste calculado).

El cálculo de los coste sociales es un poco enrevesado. Te lo detallo:

- Se toma el total percibido por la persona en el año (sumando todos sus expedientes). Hay un importe de referencia importante que llamaos `BASE_MÁXIMA` y que en 2025 era de #val("59094") euros (es la base máxima de cotización general, 4.909,50, por doce mensualidades).

    Del total retribuido a la persona, a lo que denominamos `TOTAL`, nos quedamos con el mínimo entre `TOTAL` y `BASE_MÁXIMA` y  llamamos al resultado `BASE`. El cálculo paso a paso es:
    - En principio `CONTINGENCIAS_COMUNES = 23,60% de BASE` (cotización por contingencias comunes)
        - pero se calcula `REDUCCIÓN = 0,065 * CONTINGENCIAS_COMUNES` y se actualiza el valor `CONTINGENCIAS_COMUNES = CONTINGENCIAS_COMUNES - REDUCCIÓN` (reducción por la cotización a cargo de la persona trabajadora, que en el régimen de clases pasivas es del 6,5% de la cotización por contingencias comunes).
    - `MEI = 0,67% de BASE` (Mecanismo de Equidad Intergeneracional)
    - `FORMACIÓN_PROFESIONAL = 0,70% de BASE` (cotización por formación profesional)
    - Para la cuota de solidaridad, que llamaremos `CUOTA_SOLIDARIDAD`, definimos las constantes `TRAMO1 = BASE_MÁXIMA * 1.1` y `TRAMO2 = BASE_MÁXIMA * 1.5` para sumar tres elementos:
        - De lo que ha cobrado (`TOTAL`), el importe que caen entre `BASE_MÁXIMA` y `TRAMO1` se cotiza al #val("0,92%")
        - De lo que ha cobrado (`TOTAL`), el importe que caen entre `TRAMO1` y `TRAMO2` se cotiza al #val("1%").
        - De lo que ha cobrado (`TOTAL`), el importe que supera `TRAMO2` se cotiza al #val("1,17%").
La unidad de coste que creamos tendrá como importe la suma de `CONTINGENCIAS_COMUNES`, `MEI`, `FORMACIÓN_PROFESIONAL` y `CUOTA_SOLIDARIDAD`. El elemento de coste es #etqele("prevsoc-funcs-pdi").

En la #app hemos de poder ver todas las personas que tienen costes sociales calculados y, al pinchar en una fila, su detalle de cálculo (con el desglose de los conceptos que componen el coste social calculado), así como los datos de su relación funcionarial con la universidad.


=== Decisión de centro de coste y de la actividad a partir de registros de nómina

==== Reglas para el tratamiento de los costes del PTGAS

/ Primero.- Retribuciones extras: :

    En primer lugar, para todos los conceptos retributivos de las retribuciones extras (cuando el proyecto es distinto a #val("1G019"), #val("23G019"), #val("02G041"), #val("11G006"), #val("1G046") o #val("00000")), el centro de coste y la actividad se han de determinar usando el módulo de clasificación de actividades (que ya has usado para el presupuesto).

/ Segundo.- Retribuciones ordinarias: :

    + *Tratamiento de indemnizaciones por asistencias (tribunales y otros)*: Cuando el concepto_retributivo es #val("48"), estamos ante indemnizaciones por asistencias a tribunales y similares. La actividad a la que se han de aplicar estas retribuciones es la #etqact("dag-sgc-indemnizaciones-asistencias"). Podemos refinar esto en función de la figura. El centro de coste ha de ser el que corresponda al Servicio indicado en la tabla de la regla «Por servicio existe servicio y el proyecto uno de la línea» (en la sección «Preparación de un módulo para clasificar centros de coste»). Esa tabla da, para cada servicio, un par (centro_de_coste, actividad) que es la asignación más precisa que tenemos. Si para algún servicio la actividad de la tabla está vacía o no hay entrada en la tabla, se usa #etqact("dag-general-universidad") como atrapalotodo (es decir, el «sitio donde aparcar» el coste cuando no se ha conseguido mayor precisión).

    + *Resto de retribuciones ordinarias*: Para el resto de conceptos retributivos de las retribuciones ordinarias, mapeamos cada servicio al centro de coste y a la actividad que le corresponden usando la tabla de la regla «Por servicio existe servicio y el proyecto uno de la línea»(en la sección «Preparación de un módulo para clasificar centros de coste»).

        / Caso especial: :  para el servicio #val("368") (personal de suport, conserjerías), la tabla anterior no es suficiente porque el coste se imputa físicamente al centro donde la persona presta el servicio, no al servicio nominal. En ese caso se usa el centro_plaza del registro de nómina, que es un código de servicio que hemos de mapear al centro de coste correspondiente para formar el par (centro de coste, actividad). Si ahí tampoco hay entrada de actividad, se aplica también #etqact("dag-general-universidad").

            El importe de la unidad de coste creada es el importe total de las retribuciones ordinarias del par elemento-servicio (o elemento-centro_plaza para el servicio #val("368"))).

/ Tercero.- Costes sociales: :

    Tras ejecutar los pasos primero y segundo, cada trabajador tendra un importe de coste aplicado a uno/a ó varios/as centros y actividades. Pues bien, el importe de seguridad social de cada trabajador se ha de imputar de forma proporcional a dichos centros y actividades.

==== Reglas para el tratamiento de los costes del PDI/PVI

El agrupamiento de registros es común al de PTGAS (véase la sección «Preprocesamiento nóminas / Agrupamiento de los registros»).
Vamos a generar ahora las dos unidades de coste pendientes: centros de coste y actividades. Primero habrá unas unidades (de poco importe normalmente) que irán a unidades completamente definidas y luego nos quedará una masa económica normalmente grande que irá a unas reglas de reparto complejas: lo que denominamos Regla 23.

Es crucial distinguir las *dos* tablas de proyectos generales involucradas, ya documentadas en el glosario:

#table(
    columns: (auto, 1fr),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*Tabla*], [*Contenido y uso*], table.hline()),
    [TABLA-PROYECTOS-GENERALES-NÓMINA],
    [#val("00000"), #val("02G041"), #val("11G006"), #val("1G019"), #val("1G046"), #val("23G019"). Determina si una línea de nómina entra en la *masa regla 23* (proyecto general + CR distinto de 19/64/47/48 + no es SS).],
    [TABLA-PROYECTOS-GENERALES],
    [Lo anterior más #val("07G011"), #val("11G003"), #val("1I235"), #val("22G010") (cuatro proyectos adicionales que financian cargos académicos). Determina si los CR #val("19") y #val("64") se reparten por persona entre cargos (proyectos generales) o generan UC línea a línea (proyectos específicos).],
    table.hline(),
)

El criterio se aplica fila a fila:

/ Primero.- Retribuciones extras (UC línea a línea): : Una línea de nómina genera UC retributiva extra si su proyecto NO está en TABLA-PROYECTOS-GENERALES-NÓMINA y su CR no es ni #val("19") ni #val("64") (cargos académicos, tienen su flujo propio: #ruta("uc_cargos.parquet") en proyecto específico, #ruta("cargos_uc.parquet") en proyecto general) ni #val("48") (indemnizaciones por asistencia, generan #ruta("uc_indemnizaciones_asistencias.parquet") siempre). Las despidos (CR #val("47")) en proyecto NO general sí entran aquí como retribuciones extras ordinarias; las de proyecto general van a #ruta("uc_despidos.parquet"). Para las líneas que encajan en este bloque, el centro de coste y la actividad se determinan con el módulo de clasificación de actividades (el mismo que se usa para presupuesto). Estas UC se escriben en #ruta("auxiliares", "nóminas", "uc_pvi.parquet") o #ruta("uc_pdi.parquet").

/ Segundo.- Retribuciones ordinarias (con tratamientos especiales y reparto final por regla 23): : Las líneas con proyecto en TABLA-PROYECTOS-GENERALES-NÓMINA, junto con los CR #val("19") y #val("64") de los cuatro proyectos adicionales de TABLA-PROYECTOS-GENERALES, siguen los pasos siguientes:

    - *Tratamiento de atrasos*: Los atrasos (concepto_retributivo igual a #val("30") u #val("87")) son cuantías relativamente pequeñas que, al final, se repartirán con la misma distribución promedio que el resto de la masa de regla 23. Por simplicidad, no los separamos en una bolsa propia: las líneas con CR #val("30") u #val("87") en proyectos de TABLA-PROYECTOS-GENERALES-NÓMINA se integran directamente en la bolsa de #emph[Retribuciones ordinarias para regla 23] y se reparten junto con el resto cuando se cierre la fase de reparto por dedicación.

    - *Tratamiento de despidos*: Cuando el concepto_retributivo es #val("47"), estamos ante despidos. Distinguimos dos casos según el proyecto:

        / *Proyectos específicos* (los que NO están en TABLA-PROYECTOS-GENERALES): : el despido es coste del propio proyecto y se trata como cualquier otra retribución extra: el centro de coste y la actividad se determinan con los módulos de clasificación del presupuesto. Estos importes *no* van a #ruta("auxiliares", "nóminas", "uc_despidos.parquet"); generan UC retributivas normales en #ruta("auxiliares", "nóminas", "uc_pdi.parquet") o #ruta("auxiliares", "nóminas", "uc_pvi.parquet").

        / *Proyectos generales* (los diez de TABLA-PROYECTOS-GENERALES): : el despido se imputa de forma especial y se aparta de la regla 23 en #ruta("auxiliares", "nóminas", "uc_despidos.parquet"). Si el proyecto es #val("23G019") (fondo de contingencia para despidos), la actividad es #etqact("otras-ait-financiación-propia") y el centro de coste es #val("vi"). En el resto de proyectos generales, el centro de coste y la actividad se deciden con los módulos de clasificación del presupuesto.

    - *Tratamiento de indemnizaciones por asistencias (tribunales y otros)*: Cuando el concepto_retributivo es #val("48"), estamos ante indemnizaciones por asistencias a tribunales y similares. La actividad a las que se han de aplicar estas retribuciones es la etiqueta #etqact("dag-sgc-indemnizaciones-asistencias"). Podemos refinar esto en función de la figura. El centro de coste ha de ser el que corresponda al Servicio indicado en la tabla de la regla Por servicio existe servicio y el proyecto uno de la línea (en la sección «Preparación de un módulo para clasificar centros de coste»).

    - *Tratamiento de los cargos académicos*: Cuando el #campo("concepto_retributivo") es #val("19") (Complement específic per càrrecs acadèmics) o #val("64") (Retribució addicional mèrit individual projectes UE), estamos ante el ejercicio de cargos académicos. A efectos de los cargos, llamamos *TABLA-PROYECTOS-GENERALES* al conjunto de los diez proyectos siguientes: #val("07G011"), #val("1I235"), #val("22G010"), #val("11G003"), #val("1G019"), #val("23G019"), #val("02G041"), #val("11G006"), #val("1G046") y #val("00000"). Cualquier otro proyecto se considera *específico* para esta regla. Distinguimos dos casos:

        / *Proyectos específicos* (los que NO están en TABLA-PROYECTOS-GENERALES): : el importe se imputa línea a línea como retribución extra: el centro de coste y la actividad se determinan con los módulos de clasificación del presupuesto. Estos importes generan UC en #ruta("auxiliares", "nóminas", "uc_cargos.parquet").

        / *Proyectos generales* (los diez de TABLA-PROYECTOS-GENERALES): : los importes *no se imputan línea a línea*: se reparten por persona entre los cargos que ostenta, ponderando por (días cobrados × cuantía mensual del RD asimilado).

            Para cada persona con cobro CR 19/64 en proyecto general en el año:

            + Sea `TOTAL` la suma anual de CR 19/64 de la persona en proyecto general.
            + Sea `CARGOS` el conjunto de filas de #ruta("personas cargos.xlsx") de la persona que cumplen:
                - #campo("cargo_asimilado") no nulo (el cargo se asimila a uno de los 8 tipos del RD 1086/1989, ver #ruta("cargos real decreto.xlsx")).
                - El periodo de cobro solapa con el año analizado. El periodo se construye con #campo("fecha_inicio_cobra") y #campo("fecha_fin_cobra"); si alguna de las dos está vacía, se sustituye por #campo("fecha_inicio") y #campo("fecha_fin") respectivamente (periodo del cargo en sí). Esto permite reconocer cargos cuyas fechas de cobro no se han cumplimentado pero cuyo periodo del cargo sí solapa el ejercicio.
            + Para cada cargo `c ∈ CARGOS`:
                - `días(c)` = días naturales de solape del periodo de cobro con el año.
                - `importe_rd(c)` = #campo("importe_mensual") del tipo RD asimilado.
                - `peso(c) = días(c) × importe_rd(c)`.
            + `importe_uc(c) = TOTAL × peso(c) / Σ pesos`.
            + Se crea una UC por cargo con ese importe en #ruta("auxiliares", "nóminas", "cargos_uc.parquet").

            *Parte extra del cargo (paga adicional)*: lo que la persona cobra por el cargo se compone de doce mensualidades ordinarias con CR 19/64 (recogidas en `TOTAL`) y dos pagas extras integradas en el CR #val("68") («Paga addicional complement específic pdi»). El CR 68 no es separable porque también recoge la paga adicional del complemento específico ordinario. *Esta extra "camuflada" solo aparece para los cargos pagados en proyectos generales*: los cargos pagados en proyectos específicos se cobran línea a línea (CR 19/64 mes a mes) sin parte adicional integrada en el CR 68, por lo que el ajuste del CR 68 que se describe a continuación se aplica únicamente a las personas con CR 19/64 > 0 en proyecto general.

            Estimación de la parte extra del cargo, por cargo:

            $ "extra"(c) = 2 times "importe_rd"(c) times "días"(c) / 365 $

            La UC del cargo recibe `importe_uc(c) = importe_uc_ord(c) + importe_uc_extra(c)`, donde `importe_uc_extra(c)` es la parte de la extra realmente aplicada (ver más abajo) repartida entre los cargos en proporción a la extra estimada de cada uno.

            *Ajuste al CR 68*: para no contar dos veces el mismo dinero, antes del preprocesamiento de nóminas se descuenta `Σ_c extra(c)` de las líneas CR 68 en proyecto general de la persona, repartiendo el descuento proporcionalmente entre esas líneas. Si la suma del CR 68 disponible es menor que la extra estimada, el descuento se acota a lo disponible y la diferencia se reporta como anomalía «extra estimada > CR 68 disponible» en #ruta("auxiliares", "nóminas", "cargos_extras_aplicadas.parquet").

            *Personas sin CR 68 disponible* (típicamente no funcionarios: el complemento específico del cargo va ya prorrateado en su CR 19/64 mensual, no en una paga adicional separada). Para estas personas la extra aplicada es #val("0") y, en consecuencia, `importe_uc_extra(c) = 0` para todos sus cargos: la UC del cargo refleja solo la parte ordinaria. De lo contrario se imputaría una extra que el sistema no tiene contrapartida con la que descontar y se introduciría un descuadre por persona del orden de la extra estimada.

            *Elemento de coste*: depende del sector principal de la persona:
            - *PDI*: `pdi-XXX-cargos`, donde `XXX` se deriva de la última categoría RR.HH. en CR 19/64 de la persona vía la tabla de mapeo de categorías PDI (Apéndice §«Mapeo categoría → XXX del elemento de coste PDI»). Si la categoría no encaja con ninguna entrada de la tabla, el elemento queda vacío y se reporta anomalía.
            - *PVI*: `piyotper-pid-cargos`. Para PVI el campo de categoría no determina por sí solo el XXX (haría falta cruzar perceptor + provisión, lo que añadiría complejidad sin ganancia: los cargos de PVI son muy infrecuentes), así que se usa por defecto `pid` (personal investigador docente).
            - Otros sectores: no aplica (los cargos académicos solo existen en PDI y PVI).

            *Centro de coste y actividad*: campos #campo("centro") y #campo("actividad") de la fila del cargo en #ruta("cargos.xlsx"). Pueden contener etiquetas literales del árbol (p. ej. #etqcen("secretaría-general"), #etqact("dag-secretaría-general")) o patrones que se resuelven en tiempo de ejecución a partir de la fila correspondiente de #ruta("personas cargos.xlsx") y de los catálogos auxiliares. Los patrones reconocidos son:

            #table(
                columns: (auto, 1fr),
                stroke: 0.5pt + luma(80%),
                inset: 5pt,
                table.header([*Patrón*], [*Resolución*]),
                val("SERVICIO"),
                [Se sustituye por el centro de coste del #campo("servicio") de la fila vía #ruta("entrada", "inventario", "servicios.xlsx"). Si la actividad tiene #val("dag-SERVICIO"), el resultado es #val("dag-{centro}") (ya que la actividad del servicio en el catálogo es siempre #val("dag-{slug}")).],
                val("CENTROTITULACION"),
                [Se sustituye por el centro de coste de la #campo("titulación") de la fila vía #ruta("entrada", "docencia", "titulaciones actividad centro.xlsx"). Aplica en ambos campos (actividad y centro).],
                val("TITULACIÓN"),
                [Se sustituye por la actividad propia de la #campo("titulación") en el mismo catálogo (p. ej. #etqact("grado-derecho")).],
            )

            *Propagación entre periodos.* `personas cargos.xlsx` lleva una fila por curso académico. Es habitual que el periodo activo no tenga #campo("servicio")/#campo("titulación") informados y los anteriores sí (o viceversa). Antes de aplicar el resolver — y antes incluso de filtrar por periodo de cobro, para que se aproveche también el histórico cerrado del mismo cargo — los nulos se rellenan con la *moda* del grupo (#campo("per_id"), #campo("cargo")): el valor más frecuente entre las filas no nulas. La moda es más robusta que «primer no nulo» cuando convive un código antiguo (de un plan de estudios extinguido) con muchas filas modernas del código vigente.

            *Fallback cruzado.* Si tras la resolución por titulación los patrones #val("CENTROTITULACION") o #val("TITULACIÓN") siguen presentes (porque la titulación no está informada en ese cargo), se reintenta con el #campo("servicio") y su mapeo en #ruta("servicios.xlsx"). Cubre los cargos asociados al centro y no a una titulación concreta — vicedecanos «de centro», directores de departamento, etc.

            *Overrides hiper-específicos.* Como última red de seguridad para casos en que ni titulación ni servicio están informados ni se pueden inferir del histórico, hay dos tablas en #campo("coana/fase1/cargos.py"):

            - `_OVERRIDES_TITULACION_CARGO: (per_id, cargo) → titulación`. Se inyecta *antes* de la propagación, así que tiene prioridad sobre el histórico (útil cuando el histórico apunta a otra titulación distinta de la que la persona coordina ahora). A partir de ahí el resolver actúa con normalidad y produce actividad+centro vía #ruta("titulaciones actividad centro.xlsx").
            - `_OVERRIDES_CENTRO_CARGO: (per_id, cargo) → centro`. Es la última red: si tras todo lo anterior siguen presentes #val("SERVICIO") o #val("CENTROTITULACION"), se sustituyen por este centro. Aplica también cuando el cargo no es de docencia (p. ej. una vicedecana que opera «de centro» sin coordinar titulación, o una subdirectora de calidad adscrita a un centro que no concuerda con el servicio académico de la persona).

            Si tras todos los pasos persiste algún patrón en mayúsculas, la UC se marca con la anomalía #val("patrón sin resolver (servicio/titulación faltante)") y se muestra en la #app para depuración manual.

            *Resumen del orden de resolución* (de mayor a menor prioridad): (1) override de titulación, (2) propagación por moda, (3) resolver de patrones con titulación/servicio según catálogos, (4) fallback cruzado titulación↔servicio, (5) override de centro.

            *Cargos vigentes sin periodo de cobro* (cargos no remunerados, como direcciones de cátedras no retribuidas): aparecen en la #app como informativos pero no entran al reparto.

            *Anomalías reportadas en la #app*:
            - Cargo con periodo de cobro solapado pero #campo("cargo_asimilado") nulo: el cobro queda sin imputar y se etiqueta como «sin asimilación a RD».
            - Persona con `TOTAL > 0` sin ningún cargo en `CARGOS`: anomalía global; toda la masa CR 19/64 de la persona en proyecto general queda sin imputar.

            *Pantallas en la #app, bajo «Cargos académicos»*:
            - *Por persona* — master-detail que consume #ruta("auxiliares", "nóminas", "cargos_uc.parquet"). Arriba, una fila por persona con cargos remunerados: #campo("per_id"), persona, sector, número de cargos remunerados, importe ordinario (suma de `importe_uc_ord`), importe extra (suma de `importe_uc_extra`), importe UC total y extra no aplicada (anomalía agregada). Al pinchar fila, una sub-tabla con una fila por cargo remunerado: cargo, nombre, tipo RD, cuantía RD mensual, fechas de cobro, días en el año, peso, importe ordinario, extra estimada, extra aplicada, importe UC y propuesta de elemento de coste, centro de coste y actividad. Los cargos no remunerados de la persona (cargos vigentes sin periodo de cobro) no aparecen en esta pantalla; se ven en la pantalla #emph[Personas cargos].
            - *Personas cargos* — vista bruta de #ruta("personas cargos.xlsx") filtrada a filas con al menos un día activo en el año y a personas con expediente PDI/PVI.
            - *Catálogo de cargos* — #ruta("cargos.xlsx") enriquecido con la cuantía mensual del RD asimilado y conteos sintéticos por sector (PDI · PVI · PTGAS · Otros · TOTAL).

    - *REGLA 23*: El resto de retribuciones ordinarias de cada PDI/PVI serán costes Regla 23, que habrán de ser repartidos posteriormente entre todas las actividades realizadas por cada trabajador. Dicho reparto se habrá de efectuar atendiendo al porcentaje de horas de dedicación del PDI/PVI en cuestión a cada una de estas actividades.

    Para el cálculo de dichos porcentajes de dedicación se utilizará el procedimiento establecido en el apartado siguiente:


// ---- XXXX ----

==== Regla 23 — invariante #campo("dedicación_pdi")

#figure(
    align(center, etapa-regla23()),
    caption: [Etapa de Regla 23: ficheros de entrada y salidas que produce.],
)

La regla 23 del Modelo de Contabilidad Analítica para Universidades (cuadro 9.7 del modelo) reparte los costes del PDI entre las actividades en que cada persona participa. El reparto se hace en *horas*, no en euros: primero se determinan las horas que cada PDI dedica a cada actividad concreta, y solo después se traduce esa dedicación al coste imputado a cada actividad, en proporción a la jornada anual.

Para soportar este reparto construimos, como artefacto intermedio único, un parquet llamado #ruta("fase1", "regla23", "dedicación_pdi.parquet") con una fila por #emph[(per_id, actividad, origen, origen_id)] y las siguientes columnas:

#table(
    columns: (auto, 1fr),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*Columna*], [*Significado*], table.hline()),
    [#campo("per_id")], [PDI al que se imputa la dedicación],
    [#campo("actividad")], [Etiqueta del árbol de actividades (o #val("pendiente")) ],
    [#campo("centro_de_coste")], [Etiqueta del árbol de centros (o #val("pendiente"))],
    [#campo("horas")], [Horas registradas (sin factor ×2,5)],
    [#campo("método")],
    [#val("md") / #val("ep") / #val("et") / #val("pr") — medición directa, estimación porcentual (cargos), estimación por tipología, peso relativo (HND)],
    [#campo("factor")], [#val("2,5") para impartición de docencia, #val("1,0") para el resto],
    [#campo("grupo")],
    [#val("docencia_oficial") / #val("docencia_no_oficial") / #val("gestión") / #val("investigación") / #val("extensión")],
    [#campo("origen")], [#val("POD") / #val("tesis") / #val("cargo") / #val("proyecto") / …],
    [#campo("origen_id")], [Identificador en la fuente origen],
    [#campo("anomalía")], [Texto explicativo si hay dato pendiente o nulo],
    table.hline(),
)

Cada *fuente* contribuye con un cargador que produce filas de este esquema. La tabla crece a medida que se incorporan fuentes; lo que no esté registrado acabará formando parte de las horas no distribuidas (HND) en la fase de reparto.

La jornada anual del PDI se fija en #val("1 642 h") (constante #campo("JORNADA_ANUAL_PDI")). Es el tope sobre el que se calculan tanto las horas no docentes (input del cargador de cargos) como los porcentajes que se muestran en el visor.

===== Cargador #emph[POD] (docencia oficial)

Para cada fila de #ruta("entrada", "docencia", "pod.xlsx") aplicamos el siguiente peso de año natural:

#table(
    columns: (auto, auto, auto),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*curso académico*], [*semestre*], [*peso*], table.hline()),
    [2024], [#val("2")], [#val("100 %")],
    [2025], [#val("1")], [#val("100 %")],
    [2024], [#val("A") / #val("1-2")], [#val("50 %")],
    [2025], [#val("A") / #val("1-2")], [#val("50 %")],
    [resto], [resto], [#val("0 %")],
    table.hline(),
)

Las horas brutas registradas son #val("créditos_computables × 10 × peso"). El factor #val("2,5") de la regla 23 (que recoge la preparación de clases, exámenes, tutorías, etc.) se almacena en la columna #campo("factor") para auditar; el cálculo final lo aplicará la fase de reparto.

*Rescate de períodos «ocultos».* Los semestres con peso #val("0 %") (sem 1 del curso anterior y sem 2 del curso actual) son la docencia de ambos cursos que cae fuera del año natural. Normalmente se descartan, pero hay personas —típicamente associats o substituts— a quienes se les ha asignado POD en los dos cursos pero *únicamente* en esos semestres fuera de rango, de modo que su docencia en-rango suma #val("0") créditos y caerían a #etqact("pendiente"). Para ellas se aplica una segunda instancia: si una persona no tiene *ningún* crédito computable en el rango del año natural, se rescatan sus filas de los períodos ocultos a peso completo (#val("100 %")) y su coste se imputa a las titulaciones que impartió en esos períodos, en proporción a sus créditos. Quien sí tiene docencia en rango (aunque sea poca) ignora por completo los períodos ocultos. Estas filas se marcan en #campo("detalle") como rescatadas de período fuera del año natural.

La titulación de cada asignatura se resuelve cruzando con #ruta("entrada", "docencia", "asignaturas grados.xlsx") y #ruta("entrada", "docencia", "asignaturas másteres.xlsx"). Antes del cruce se descartan del mapeo de másteres los *másteres ficticios* listados en la configuración (clave #val("másteres_ficticios_pod"), p.ej. el #val("49900")): son másteres sin alumnado matriculado cuyas asignaturas pertenecen también a algún máster real, por el que se captura su coste y dedicación. Filtrarlos no pierde ninguna asignatura y evita la duplicación espuria de horas que provocaría el cruce. La actividad y centro de coste asociados a cada titulación los toma de #ruta("entrada", "docencia", "titulaciones actividad centro.xlsx"). Las titulaciones todavía no mapeadas se emiten con #val("actividad = pendiente"), #val("centro = pendiente") y anomalía #val("titulación sin mapeo a actividad/centro").

===== Cargador #emph[POD] (docencia no oficial)

La docencia no oficial (formación permanente, cursos UJI, OAD, idiomas, etc.) se carga desde #ruta("entrada", "docencia", "estimación horas docencia propia.xlsx"). El fichero registra una fila por #emph[acción formativa] con los campos #campo("perid") (PDI/PVI que la imparte), #campo("proyecto") (proyecto presupuestario que la financia), #campo("nombre") (tipo: cursos, mesa redonda, etc.), #campo("horas") (horas declaradas), #campo("importe_hora") (€/hora) y #campo("importe_total") (= horas × importe_hora).

*Filtro previo: solo proyectos propios de docencia.* La fila se conserva si el #campo("tipo") del proyecto en #ruta("entrada", "presupuesto", "proyectos.xlsx") está en

#table(
    columns: (auto, 1fr),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*Tipo*], [*Descripción*], table.hline()),
    val("EPM"), [Estudio propio máster],
    val("EPDE"), [Estudio propio diploma de especialización],
    val("EPDEX"), [Estudio propio diploma de experto],
    val("EPC"), [Estudio propio curso],
    val("EPMI"), [Estudio propio microcredencial],
    val("CUID"), [Cursos universitarios de idiomas],
    val("CUES"), [Cursos universitarios de español],
    val("OAD"), [Otras actividades docentes],
    table.hline(),
)

El resto de filas (proyectos con financiación genérica, externa o no docente) se descartan. Esta acotación garantiza que solo computan como dedicación las acciones efectivamente impartidas con financiación propia de docencia.

*Regularización de horas.* La triple información #campo("horas") · #campo("importe_hora") · #campo("importe_total") no es fiable: en una fracción de las filas el responsable ha registrado #campo("horas") = #val("1") con un #campo("importe_hora") descabellado, lo que infla el €/h y rebaja el número real de horas dedicadas. Heurística:

- Si #campo("importe_hora") ≤ #val("130 €/h"): se acepta #campo("horas") como horas.
- Si #campo("importe_hora") > #val("130 €/h"): el campo #campo("horas") no es fiable; aproximamos las horas como #campo("importe_total") / #val("130 €/h") (tarifa razonable de docencia propia).

El número resultante se llama #campo("horas_efectivas") y es lo que se usa como horas brutas de la fila. Como en el POD oficial, el factor #val("2,5") (preparación, tutorías, evaluación) se almacena en la columna #campo("factor") para auditar; la fase de reparto lo aplicará al traducir horas a coste.

*Actividad y centro de coste.* La actividad se determina con la regla:

- Tipo #val("OAD") y #campo("centro_origen") del proyecto = #val("UMAJ") (#emph[Universitat per a Majors]) → actividad #etqact("universidad-mayores"), centro de coste #etqcen("universidad-mayores").
- Resto de filas con tipo #val("OAD") con otro #campo("centro_origen") → actividad #etqact("otros-docencia-propia"); para determinar el centro de coste, utiliza las misma reglas que hubieras utilizado para procesar un gasto de este proyecto.
- El resto de tipos de proyecto, #val("EPM"), #val("EPDE"), #val("EPDEX"), #val("EPC"), #val("EPMI"), #val("CUID"), #val("CUES"), para determinar tanto la actividad como el centro de coste, utiliza las misma reglas que hubieras utilizado para procesar un gasto de este proyecto.

Las filas se emiten con #campo("grupo") = #val("docencia_no_oficial"), #campo("método") = #val("et") (estimación por tipología) y #campo("origen") = #val("POD_no_oficial"), con #campo("origen_id") = #campo("gre_id") (identificador único de la acción en el fichero). Adicionalmente se persiste en #ruta("auxiliares", "nóminas", "regla_23_horas_no_oficiales.parquet") la tabla completa con #campo("horas_efectivas"), #campo("tipo_proyecto") y #campo("centro_origen") para auditoría desde la #app.

*Asignación al `per_id` de las horas correspondiente.* Las `horas_efectivas` de cada `per_id` han de tenerse en cuenta para la regla 23, insertándolas en el diccionario correspondiente tras multiplicarlas por 2,5. Al visualizar con la #app la información de actividades/centros para regla 23, hemos de ver qué docencia no oficial tiene la persona en horas.

===== Cargador #emph[tesis]

Cada fila de #ruta("entrada", "investigación", "tesis.xlsx") es un *periodo de matrícula* (no una tesis completa: una tesis identificada por #campo("per_id_alumno") puede tener varios periodos a lo largo del tiempo). El estado del periodo es #val("C") (tiempo completo), #val("P") (tiempo parcial), #val("B") (baja), #val("BM") (baja por maternidad) o #val("BV") (baja por otra causa).

*Filtros para considerar un periodo activo en el año natural:*

+ Si #campo("fecha_lectura_tesis") < #val("1/1/año") → fuera (ya leída antes del año).
+ Si #campo("fecha_inicio_tiempo") > #val("31/12/año") o #campo("fecha_inicio_tesis") > #val("31/12/año") → fuera (empieza después del año).
+ Descartar todas las filas con #campo("estado") en #val("B"), #val("BV") o #val("BM") (bajas, no producen dedicación).
+ Mantener las que tienen al menos un día del rango #emph[[fecha_inicio_tiempo, fecha_fin_tiempo]] dentro del año natural.
+ El estado superviviente es #val("C") o #val("P").

*Cálculo de horas:*

La asignación anual base por fila es de #val("104 h") (#val("2 h/semana") × #val("52 semanas")) si #campo("estado") = #val("C") y de #val("52 h") si #campo("estado") = #val("P") (la dedicación parcial recibe la mitad). Las horas de la fila para el año natural son:

$ "horas_tesis" = "base_anual" dot "días_activos" / 365 $

donde #campo("base_anual") es #val("104") o #val("52") según estado, y #campo("días_activos") son los días del periodo dentro del año.

*Reparto por persona:*

#campo("per_id_tutor") recibe el #val("10 %") y los miembros de la lista de directores (no nulos: #campo("per_id_director"), #campo("per_id_codirector"), #campo("per_id_codirector2")) se reparten el #val("90 %") a partes iguales:

$ "horas_tutor" = "horas_tesis" dot 0.10 quad "horas_director" = "horas_tesis" dot 0.90 \/ N_"directores" $

Si una misma persona figura como tutor y como director, recibe ambas slices (no se deduplica).

*Programa de doctorado:*

El campo #campo("estudio") (códigos #val("90xxx")) se cruza con #ruta("entrada", "docencia", "doctorados.xlsx") para obtener el #campo("nombre") del programa, y con #ruta("entrada", "docencia", "doctorados actividad centro.xlsx") para obtener la etiqueta de actividad y centro. Si ese mapeo no existe todavía, la fila se emite con #val("actividad = doctorado") (umbrella) y #val("centro = pendiente") con anomalía que incluye el código y nombre del programa para facilitar el mapeo posterior.

El #campo("origen_id") es la concatenación #val("per_id_alumno/rol") (donde #val("rol") es #val("tutor") o #val("director")), de modo que cada slice queda identificada de forma única.

*Participación en proyectos de investigación y contratos de transferencia:*

La tabla #ruta("entrada", "investigación", "investigadores en contratos.xlsx") contiene información de participacón de cada `per_id` en proyectos de investigación y contratos de transferencia. Para cada `per_id` puede haber más de una fila y en cada fila aparece un `contrato`. Es un código interno del SGIT para identficar contratos y proyectos.

Es necesario consultar #ruta("entrada", "investigación", "proyectos en contratos investigación.xlsx") para saber si un `contrato` está vivo o no. En esta tabla hay `fecha_inicio` y `fecha_fin` de cada contrato. Así pues, hemos de filtrar para quedarnos primero con los contratos con un día o más activos en el año del análisis. Hemos de eliminar, también, las filas  con `importe_concedido` cero o nulo, se han de suprimir. Y con eso, filtrar también la tabla de investigadores en contratos para quedarnos con las filas de contratos activos.

*Filtro por adscripción.* Antes de seguir, cruzamos con #ruta("entrada", "investigación", "contratos a departamentos.xlsx") para descartar los contratos cuya única adscripción sea a una unidad de tipo #val("VI") (vicerrectorado), #val("CT") (cátedra) o #val("SE") (servicio). En esos contratos la participación de las personas es función del cargo institucional, no de trabajo investigador efectivo, y no debe generar horas de la regla 23. Los contratos con alguna adscripción a departamento (#val("DE")) o instituto (#val("IN")) se mantienen.

La información del proyecto se enriquece con #ruta("entrada", "investigación", "anexos proyectos.xlsx"). En particular, hay un `codex` que permite obtener información sobre el financiador usando los campos `tipo_anexo`,	`subtipo_anexo` y `microtipo_anexo`. Con ellos formamos una cadena que concatenas sus tres valores y usamos este mapeo (el `*` es comodín y el orden importa):

#table(
    columns: 3,
    align: (left, left, right),
    stroke: none,
    table.hline(),
    [*`tipo_anexo`+`subtipo_anexo`+`microtipo_anexo`*], [*Actividad*], [*Horas/semana estimadas*],
    table.hline(),
    val("2PE"), etqact("ai-internacional"), val("10"),
    val("2PN"), etqact("ai-nacional"), val("10"),
    val("2PV"), etqact("ai-regional"), val("10"),
    val("2PA"), etqact("ai-nacional"), val("10"),
    val("2PI"), etqact("ai-internacional"), val("10"),
    val("2PU"), etqact("ai-plan-propio"), val("3"),
    // Eran 6 en el modelo, pero sabiendo lo que son los proyectos de plan propio, 3 va bien.
    val("1CE"), etqact("cátedras-aulas-empresa"), val("2"),
    val("1AA"), etqact("transf"), val("1"),
    val("1**"), etqact("transf"), val("8"),
    table.hline(),
)


Con esta información hemos de asignar un cupo de horas semanales a las personas que participan en proyectos de investigación y contratos de transferencia, prorrateado por los días de vigencia en el año natural.

*Cálculo por contrato.* Las horas se calculan *por contrato* (no se agrupan por proyecto presupuestario). Para cada (per_id, contrato) vivo en el año:
+ *Periodo efectivo.* Intersección de [fechas del contrato] ∩ [fechas de solicitud principal o alternativa del per_id] ∩ [año natural]. Los #emph[días de solape] son la *unión* de los periodos efectivos del contrato para esa persona (sin doble conteo si hay varias filas de solicitud).
+ *Horas.* Dos casos:
  - *Si el contrato está en Kalendas* (#ruta("entrada", "investigación", "horas kalendas.xlsx")) para esa persona: se toman sus *horas reales declaradas* —la suma del contrato en Kalendas, que ya solo cuenta #campo("tipo_actividad") = #val("Proyecto de investigacion")—. No se estima.
  - *Si no está en Kalendas*: se estima a partir de las *horas anuales* del tipo de anexo (la columna «horas/semana estimadas» de la tabla anterior equivale a horas anuales $times 7 slash 365$) prorrateadas por el solape: `horas = horas_anuales × (días_solape / 365)` (equivalente a `h_sem × días_solape / 7`).

Cuando un investigador PDI ha declarado y validado en Kalendas sus horas a un contrato, mandan esas horas reales; en otro caso se estima por tipo de anexo y solape. (Los associats y substituts no se ven afectados: en la cascada de la regla 23 toda su jornada va a docencia, así que sus horas de investigación se anulan después. Si una persona acumula muchos contratos y sus horas superan la jornada, la cascada acota su investigación al sobrante —jornada − docencia − gestión—, de modo que las horas solo determinan el reparto entre sus contratos/grupos, no el total.)

El #campo("origen") es #val("proyecto") y el #campo("origen_id") es el proyecto presupuestario del contrato (línea de menor número en #ruta("entrada", "investigación", "proyectos en contratos investigación.xlsx"), o la clave artificial `contrato-{id}` cuando no haya). La actividad debe ser tan detallada como sea posible:
- `1AA` (art 60) → `transf-60-{proyecto presupuestario}`.
- Resto con proyecto presupuestario conocido → `{actividad_base}-{proyecto presupuestario}` (p. ej. `ai-nacional-XXX`, `transf-XXX`, `cátedras-aulas-empresa-XXX`).
- Sin proyecto presupuestario → actividad base genérica.

Los nodos `{actividad_base}-XXX` o `transf-60-XXX` se insertan dinámicamente en el árbol de actividades como hijos de su actividad base.

El *centro de coste* ha de ser el del grupo de investigación al que está adscrita la persona. Si la persona está en N grupos activos en el año, las horas se reparten proporcionalmente a los días activos en cada grupo (una fila por grupo). Si la persona no está en ningún grupo, se emite la fila con #campo("centro_de_coste") = #etqcen("no-adscritos-a-grupo-de-investigación") y anomalía `persona sin grupo de investigación activo en el año`. Si el anexo no casa con ninguna regla, la fila se emite con `actividad = pendiente` y anomalía con el `codex`.

En la #app, muestra para cada `per_id` su participación en proyectos y contratos.

===== Cargador #emph[cargos académicos]

Lee #ruta("fase1", "auxiliares", "nóminas", "cargos_uc.parquet") (que ya tiene cargo asimilado al RD 1086/1989, días de solape y actividad/centro resueltos) y cruza con #ruta("entrada", "nóminas", "cargos.xlsx") para obtener la dedicación del cargo. La regla es:

- Si #campo("dedicación_porcentual") está informada y > 0: se aplica como porcentaje sobre las horas no docentes de la persona, prorrateado por los días de cobro en el año natural:

    $ "horas_cargo" = ("días_cargo" / 365) dot "dedicación_porcentual" dot "horas_no_docentes" $

- Si #campo("dedicación_porcentual") es nula o cero pero #campo("dedicación_horaria") > 0: se interpreta como una cantidad anual absoluta de horas, prorrateada por los días de cobro:

    $ "horas_cargo" = ("días_cargo" / 365) dot "dedicación_horaria" $

- Si ambas columnas son nulas o cero: el cargo no aporta horas (no se emite fila), aunque siga teniendo #campo("cargo_asimilado") al RD 1086/1989.

Donde #campo("horas_no_docentes") = #campo("JORNADA_ANUAL_PDI") − suma de #campo("horas") × #campo("factor") sobre las filas de la persona con #campo("grupo") igual a #val("docencia_oficial") o #val("docencia_no_oficial") ya cargadas. Si la docencia ya supera la jornada, las horas no docentes se ponen a #val("0") y la persona no recibe horas por gestión.

El valor de #campo("dedicación_porcentual") del catálogo tiene prioridad sobre el porcentaje orientativo del cuadro 9.7 de la regla 23: permite afinar caso a caso (p. ej. un rector con actividad investigadora figurando con < 100 %). El cuadro 9.7 sirve solo como referencia para rellenar el xlsx.

El #campo("origen_id") es el #campo("id") de la UC del cargo (#val("CARGO-NNNNN")). El #campo("método") es #val("ep") (estimación porcentual).

===== Cargador #emph[grupos de investigación]

Lee #ruta("entrada", "investigación", "investigadores en grupos.xlsx") y filtra las filas con #campo("coordinador") = #val("S"). Para cada coordinador imputa #val("2 h/semana") (cuadro 9.7, fila "Coordinación/dirección de grupos de investigación"), prorrateado por los días de solape entre el periodo de coordinación (#campo("fecha_alta") a #campo("fecha_baja"), o fin del año si no hay baja) y el año natural:

$ "horas_grupo" = 2 dot ("días_solape" / 7) $

Si una persona figura como coordinadora en varias líneas del mismo grupo se cuenta una sola vez. Los miembros no coordinadores y los colaboradores no reciben horas por este cargador: sus horas vendrán por los proyectos concretos en los que participen (cargador futuro).

El #campo("origen_id") es el #campo("id_grupo"). El centro de coste es el del grupo de investigación (`grupo-investigación-XXX`). La actividad se crea como `dag-grupo-investigación-XXX`, donde `XXX` es el id del grupo. Esa actividad se inserta como hija del nodo del instituto (`dag-{instituto}` bajo `dag-institutos-centros-investigación`, o `dag-inves` si el grupo no está adscrito a ningún instituto). En la #app, muestra para cada `per_id` su participación como coordinador en grupos de investigación.



===== Próximos cargadores

- Docencia no oficial: másteres propios, expertos, microcredenciales… con el mismo tratamiento que #emph[POD] (×2,5 para impartición).
- Extensión universitaria: actividades culturales, deportivas, de cooperación, de promoción.

===== Vista en la #app

En el menú lateral, bloque #emph[Regla 23], la entrada #emph[Dedicación PDI] abre una pantalla master-detail con tres áreas:

+ *Lista de personas* (master): per_id, persona, *% docencia*, *% gestión*, *% investigación* y *% jornada cubierta* (los cuatro calculados sobre las horas finales tras el reparto), nº actividades y nº filas con anomalía. Los porcentajes deben sumar #val("100 %") salvo en casos anómalos (exceso de docencia + gestión, asociados sin docencia registrada).
+ *Relación laboral* (panel al seleccionar persona): una fila por cada combinación (expediente, categoría plaza, categoría RR.HH.) observada en las nóminas del año, con el primer y último mes de cobro, el número de meses, si es funcionario y el #emph[cobrado] del año (suma de #campo("importe") en esa fila excluyendo las líneas de SS, aplicación #val("12*")). Permite ver al vuelo qué categoría tiene la persona, durante cuánto tiempo, si ha habido cambios de plaza en el año y cuánto ha cobrado por cada relación.
+ *Reparto por grupo y origen* (panel resumen al seleccionar persona): una fila por cada par (grupo, origen) con horas registradas, factor medio, horas iniciales efectivas, horas finales tras el reparto y porcentaje sobre la jornada anual (calculado sobre las horas finales). Si la jornada no llega a #val("1 642 h") aparece una fila #emph[Sin asignación (HND)] con el déficit.
+ *Detalle por actividad*: las filas de #campo("dedicación_pdi") para la persona, con #campo("origen_id"), #campo("método"), #campo("factor"), las *horas finales* repercutidas y el *% jornada* de cada actividad concreta, además de las anomalías. Las filas sintéticas con #campo("origen") = #val("reparto") aparecen al final cuando ha sido necesario crear una actividad #etqact("ai") para absorber la HND.

El usuario puede así ver de un vistazo no solo cuántas horas dedica cada PDI a cada grupo de la regla 23, sino *de dónde vienen* (POD, tesis, coordinación de grupo, cargo) y *qué porcentaje* de su jornada se imputará a cada actividad y centro de coste.

===== Fase de reparto (fases 5-7 de la regla 23)

Una vez completada la tabla #campo("dedicación_pdi"), un módulo final (#campo("reparto.py")) normaliza las horas registradas a la *jornada disponible* de cada PDI y obtiene la dedicación que se llevará a coste. La salida es #ruta("fase1", "regla23", "dedicación_pdi_normalizada.parquet") con el mismo grano que la tabla origen y una columna añadida #campo("horas_finales") (las que se usarán para repartir el coste retributivo).

*Jornada proporcional al año trabajado.* La jornada base no es fija: si una persona no ha trabajado el año completo, su jornada es la parte proporcional. Como no hay fechas de alta/baja en los datos, el periodo trabajado se estima por los *meses con sueldo base* (concepto retributivo #val("01") en PDI funcionario y associats/substituts, #val("82") en PVI): la fracción del año es el número de meses distintos con sueldo base dividido por 12 (granularidad mensual; quien empieza a mitad de mes cuenta el mes entero). Así, $T = "jornada_anual_pdi" times "fracción_año" times X_"persona"$, donde #campo("fracción_año") $in [0, 1]$ (1 si no hay dato de sueldo base, para no anular dedicaciones por un hueco) y $X_"persona"$ es la fracción no sindical (§«Reducciones sindicales»). Esta reducción temporal es independiente del porcentaje de reducción de jornada por conciliación o parcialidad, que se trata aparte.

*Horas efectivas iniciales por grupo.* Para cada persona se calculan, a partir de #campo("dedicación_pdi"), las horas efectivas (#campo("horas") $times$ #campo("factor")) agregadas en cuatro grupos: $H_"DO"$ (docencia oficial), $H_"DNO"$ (docencia no oficial), $H_G$ (gestión, ya prorrateada por el cargador de cargos) y $H_I$ (investigación + transferencia). No hay $H_E$ (extensión) en la UJI: si en el futuro se incorporan registros de extensión, se sumarán a docencia para el reparto.

*Caso especial: figuras puramente docentes (associats y substituts).* Si la categoría de plaza vigente en el año está entre las listadas en #campo("categorías_docencia_pura_plaza") (códigos #val("07"), #val("08"), #val("18"), #val("21"), #val("22"), #val("23"), #val("24"), #val("31"), #val("32"), #val("36"), #val("44"), #val("46") en #ruta("entrada", "nóminas", "categorías plazas.xlsx")), la jornada $T$ entera se imputa a sus actividades docentes proporcionalmente a las horas iniciales efectivas. No hay gestión ni investigación. Esto incluye dos colectivos con tratamiento idéntico: los #emph[professors associats] (PAA, PAL y variantes) y los #emph[professors substituts] (PS), ambos contractualmente docentes y sin obligación de investigar.

*Caso general — reparto en cascada.* Para el resto del PDI la jornada $T$ se reparte por *prioridad estricta*: primero docencia, luego gestión, y la investigación absorbe lo que quede. *Docencia y gestión son rígidas*: se respetan tal cual si caben y se recortan si no, pero *nunca se inflan*. La investigación es elástica: se contrae si su valor inicial no cabe en el hueco disponible, o absorbe las horas no distribuidas si cabe.

+ *Docencia.* $H_"D"^"def" = min(H_"DO" + H_"DNO", T)$. Si la docencia impartida cabe en $T$ se toma entera; si la supera (poco habitual), consume toda la jornada disponible.
+ *Gestión.* $H_G^"def" = min(H_G, T - H_"D"^"def")$, sobre lo que quede tras la docencia.
+ *Investigación.* $H_I^"def" = T - H_"D"^"def" - H_G^"def"$ (el hueco que queda). Si la $H_I$ inicial superaba el hueco, queda contraída a él; si era menor, las horas no distribuidas se imputan a investigación (todo PDI investiga por defecto).

Por construcción, docencia + gestión + investigación suman siempre exactamente $T$. Si la docencia y la gestión iniciales no caben en $T$, la gestión —o, en el límite, la propia docencia— se recorta y se marca la anomalía #val("docencia + gestión superan la jornada disponible"). Si a la persona le corresponden horas de investigación y no tiene ninguna fila de ese grupo, se sintetiza una con actividad #etqact("ai") (umbrella) y el centro de su grupo principal; si la persona no está adscrita a ningún grupo de investigación, ese centro es #etqcen("no-adscritos-a-grupo-de-investigación") (un nodo virtual hijo de #etqcen("inves") que se crea siempre en el árbol de centros de coste).

El #campo("sexenio_vivo") (último sexenio finalizado en los últimos seis años) se conserva como dato informativo de la persona pero *no afecta al reparto*: como docencia y gestión son rígidas, las horas no distribuidas solo pueden ir a investigación.

*Caso especial: vicerrectorados.* Un vicerrector tiene el #val("75 %") de dedicación al cargo (su $H_G$ es el 75 % de las horas no docentes); el 25 % restante queda como sobrante y se reparte según la regla anterior. El rector, con el #val("100 %") de dedicación al cargo, no deja sobrante.

*Repercusión a actividades concretas.* Una vez determinadas las horas finales por grupo, se reparten entre las actividades concretas que la persona aportó a ese grupo (con sus distintos #campo("origen_id")) en proporción a las horas iniciales efectivas. Las dos categorías docentes (oficial y no oficial) comparten total: si la docencia oficial era 200 h y la no oficial 50 h, se conservan ambos números íntegros.

La tabla resultante #campo("dedicación_pdi_normalizada") se usa después para repartir el coste de las retribuciones de regla 23 entre actividades y centros de coste: cada euro de masa salarial regla 23 se distribuye con la proporción $#campo("horas_finales") / T$.

===== Reparto de la masa regla 23 → unidades de coste

La «masa regla 23» es el subconjunto de las nóminas PDI/PVI que satisface a la vez: #campo("aplicación") que NO empieza por #val("12") (no es seguridad social), #campo("proyecto") en #campo("TABLA-PROYECTOS-GENERALES-NÓMINA") y #campo("concepto_retributivo") NO en #val("19"), #val("64"), #val("47") ni #val("48") (esos conceptos generan sus propias UC: cargos, despidos, indemnizaciones por asistencia).

El módulo #campo("uc_reparto.py") (#ruta("coana", "fase1", "regla23", "uc_reparto.py")) realiza el reparto en dos pasos:

+ Por cada persona se agrega su masa por #campo("elemento_de_coste") (calculado con la misma función `_elemento_coste_pdi` / `_elemento_coste_pvi` que las UC extras). Los registros sin elemento de coste resoluble se descartan con aviso.
+ Para cada (per_id, elemento_de_coste) se distribuye el importe entre los pares (#campo("actividad"), #campo("centro_de_coste")) de la persona con peso #emph[horas_finales] / Σ #emph[horas_finales] (esto es, equivalente al #emph[% de jornada] que devuelve la #app). Cada combinación (per_id, ec, actividad, centro) genera una unidad de coste con origen #val("regla_23") y origen_id codificando los cuatro campos.

Las personas con masa regla 23 pero sin ninguna fila en #campo("dedicación_pdi_normalizada") (PDI/PVI sin POD, tesis, cargos ni proyectos en el año) reciben su masa íntegra en una UC con #etqact("pendiente") / #etqcen("pendiente") y aparecen reportadas como aviso en la salida de fase 1.

Hay un caso especial sistemático: el PDI fallecido o cesado que cobra en el año en curso únicamente los #emph[incentivos] devengados el año anterior (más, eventualmente, algún atraso). Su perfil retributivo en la masa regla 23 es:

- Al menos una línea con #campo("tipo_coste") = #val("V") (retribución variable propia del ejercicio).
- Esa retribución variable está concentrada exclusivamente en el mes de marzo y con un único #campo("concepto_retributivo") = #val("67") (OTVARS / incentivos del ejercicio anterior).
- Las atrasos (#campo("tipo_coste") = #val("I")) pueden estar o no estar y en cualquier mes; no cuentan a estos efectos.

A esas personas, en vez de imputarles la masa a (#etqcen("pendiente"), #etqact("pendiente")), se les imputa a (#etqcen("UJI"), #etqact("UJI")): el coste se reconoce como gasto general de la institución, no atribuible ya a ninguna actividad concreta (la actividad sucedió el año anterior). La detección automática se hace en #campo("_detecta_incentivos_residuales") y los parámetros (mes y concepto retributivo) son las constantes #campo("_MES_INCENTIVOS") y #campo("_CR_INCENTIVOS_AÑO_ANTERIOR") en #ruta("coana", "fase1", "regla23", "uc_reparto.py").

*Caso especial sistemático: associats assistencials (PAA) de ciencias de la salud sin docencia en el POD.* Los #etq("PAA") (#emph[Professor/a Associat/da Assistencial]) son personal clínico que imparte la práctica asistencial de los grados de ciencias de la salud, una docencia que no figura en el POD. Cuando un #etq("PAA") no tiene ninguna carga en el POD (y por tanto caería a #etqact("pendiente")), es su *departamento* —resuelto a partir del #campo("servicio") de su nómina vía #ruta("entrada", "inventario", "servicios.xlsx")— el que decide la titulación a la que va su coste, siempre al *Grado* correspondiente y con centro #etqcen("fcs") (Facultat de Ciències de la Salut):

#table(
    columns: (auto, auto, auto),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*Departamento (centro)*], [*actividad*], [*centro*], table.hline()),
    [#etqcen("upm") · Medicina], [#etqact("grado-medicina")], [#etqcen("fcs")],
    [#etqcen("upi") · Infermeria], [#etqact("grado-enfermería")], [#etqcen("fcs")],
    [#etqcen("dpbcp") · Psicologia Bàsica, Clínica i Psicobiologia], [#etqact("grado-psicología")], [#etqcen("fcs")],
    [#etqcen("dpeesm") · Psicologia Evolutiva, Educativa, Social i Metodologia], [#etqact("grado-psicología")], [#etqcen("fcs")],
    table.hline(),
)

Esta regla se aplica antes del fallback genérico a #etqact("pendiente") / #etqcen("UJI"); un #etq("PAA") de un departamento no listado mantiene el comportamiento por defecto. La detección y el mapeo están en #campo("_DEPTO_SALUD_A_GRADO") (#ruta("coana", "fase1", "regla23", "uc_reparto.py")).

*Caso especial sistemático: personal investigador (PVI/PI) sin proyecto ni grupo imputable.* El personal investigador (elemento de coste con prefijo #val("piyotper")) cobra a menudo de proyectos generales sin estar adscrito a ningún proyecto ni grupo concreto, de modo que no tiene dedicación calculable y caería a #etqact("pendiente"). En ese caso su masa se imputa a la investigación con financiación propia del Vicerrectorado de Investigación: actividad #etqact("otras-ait-financiación-propia"), centro #etqcen("vi"). Las constantes son #campo("_INVESTIGADOR_SIN_PROYECTO_ACT") y #campo("_INVESTIGADOR_SIN_PROYECTO_CC") en #ruta("coana", "fase1", "regla23", "uc_reparto.py").

*Caso especial sistemático: funcionarios en servicios especiales.* Un funcionario en situación de servicios especiales en otra administración deja de prestar servicio en la UJI —y por tanto no tiene docencia, gestión ni investigación que imputar—, pero la UJI le sigue abonando sus *trienios* consolidados (la antigüedad corre a cargo de la administración de origen). Su huella en la masa regla 23 es inconfundible: percibe trienios (#campo("CR 03")) y, en su caso, la parte proporcional de la paga extra, *sin sueldo base* (#campo("CR 01")). Ese gasto, que no corresponde a ninguna actividad realizada en la UJI, se imputa a (#etqcen("UJI"), #etqact("UJI")) como gasto general de la institución. La detección está en #campo("_detecta_servicios_especiales") y los conceptos en las constantes #campo("_CR_TRIENIOS") y #campo("_CR_SOU_BASE") (#ruta("coana", "fase1", "regla23", "uc_reparto.py")).

*Caso especial sistemático: figuras puramente docentes sin POD.* Los associats (#campo("pdi-as")) y substituts (#campo("pdi-ps")) que no figuran en el POD del año (ni en sus períodos ocultos) y que no son #etq("PAA") de ciencias de la salud son, en su mayoría, contratos breves o finiquitos que no llegaron a generar carga docente registrada. Su masa se imputa a estudios oficiales de la institución: actividad #etqact("estudios-oficiales"), centro #etqcen("UJI"). Las constantes son #campo("_DOCENTE_PURO_SIN_POD_ACT") y #campo("_DOCENTE_PURO_SIN_POD_CC"), con los prefijos de elemento de coste en #campo("_EC_DOCENTE_PURO_PREFIJOS") (#ruta("coana", "fase1", "regla23", "uc_reparto.py")).

*Regla escoba (última instancia).* Lo que ninguna regla anterior captura es ruido residual: cobros puntuales de poca cuantía (finiquitos, atrasos sueltos) sin patrón común. Para no dejarlos en #etqact("pendiente") indefinidamente, se barren a (#etqcen("UJI"), #etqact("UJI")) *siempre que la masa residual total de la persona sea inferior al umbral* #campo("umbral_residual_regla23") (configurable, por defecto #val("500 €")). Si la masa residual es igual o superior al umbral, la persona se mantiene en (#etqcen("pendiente"), #etqact("pendiente")) como anomalía real: es la red de seguridad que evita que un importe material e inexplicado se diluya silenciosamente en gasto general.

El orden de precedencia de los fallbacks para personas con masa pero sin dedicación es, de mayor a menor: override individual (se aparta antes del reparto) → #etq("PAA") de salud → incentivos residuales (#etqcen("UJI")) → investigador sin proyecto (#etqcen("vi")) → servicios especiales (#etqcen("UJI")) → figura puramente docente sin POD (#etqact("estudios-oficiales")) → residual bajo umbral (#etqcen("UJI")) → #etqact("pendiente") (masa ≥ umbral, anomalía).

*Overrides individuales (casos super-específicos).* Algunas personas tienen una situación laboral que ninguna regla automática puede inferir a partir de los datos disponibles (nóminas, POD, proyectos…), pero cuyo destino correcto es conocido por revisión manual. Para esos casos se mantiene una tabla de overrides por #campo("per_id") en #ruta("coana", "fase1", "regla23", "uc_reparto.py"): cuando una persona figura en ella, *toda* su masa regla 23 se imputa íntegramente al par (#campo("actividad"), #campo("centro_de_coste")) indicado, con prioridad sobre el reparto por dedicación y sobre cualquier otro fallback (incluidos #etqcen("pendiente") y #etqcen("UJI")). La tabla actual:

#table(
    columns: (auto, 1.4fr, auto, auto),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*per_id*], [*Motivo*], [*actividad*], [*centro*], table.hline()),
    [#val("91758")],
    [PAL que no impartía docencia reglada: prestaba apoyo en el Centre d'Autoaprenentatge de Llengües (CAL). Su coste, aunque figura como docente, es asimilable al del PTGAS y corresponde íntegramente al servicio de lenguas.],
    [#etqact("cursos-idiomas")], [#etqcen("slt")],
    table.hline(),
    [#val("148067")],
    [PAL del Centre d'Autoaprenentatge de Llengües (CAL), mismo caso que #val("91758").],
    [#etqact("cursos-idiomas")], [#etqcen("slt")],
    table.hline(),
)

La salida es #ruta("fase1", "regla23", "uc_reparto_regla_23.parquet") con esquema UC estándar (#campo("id"), #campo("elemento_de_coste"), #campo("centro_de_coste"), #campo("actividad"), #campo("importe"), #campo("origen") = #val("regla_23"), #campo("origen_id"), #campo("origen_porción")) más una columna adicional #campo("per_id") para trazabilidad. Estas UC se incorporan al combinado de fase 1 y bajan a la fase 2 como cualquier otra UC retributiva.

== Reparto de la seguridad social por expediente

#figure(
    align(center, etapa-ss()),
    caption: [Etapa de Seguridad Social: ficheros de entrada y salidas que produce.],
)

La seguridad social es la única magnitud que vive a nivel persona (no por expediente): la UJI cotiza una vez por cada empleado, atado a un único CCC. Eso plantea una dificultad cuando una persona tiene varios expedientes: ¿con qué reglas se reparte la SS si esos expedientes pertenecen a sectores diferentes? El modelo de CoAna lo resuelve en dos pasos, de modo que cada expediente acaba siendo autónomo en su circuito de reglas:

=== Paso 1 — Reparto persona → expediente

Por cada persona con al menos un expediente vivo en el año (con cobro retributivo), se calcula:

- *SS total persona* = SS cotizada (suma de #campo("importe") en líneas con #campo("aplicación") empezando por #val("12")) + SS calculada (de #ruta("auxiliares", "nóminas", "costes_sociales_calculados.parquet"), aplicable solo a PDI funcionario en clases pasivas).
- *Bruto por expediente* = suma del #campo("importe") en líneas no SS del expediente.
- *Cacho SS de cada expediente* = #campo("SS_total_persona") × #campo("bruto_expediente") / #campo("bruto_total_persona").

Se persiste #ruta("auxiliares", "nóminas", "ss_por_expediente.parquet") con (#campo("expediente"), #campo("per_id"), #campo("bruto"), #campo("ss_total_persona"), #campo("ss_expediente")) para auditar.

La atribución original de cada línea SS a un expediente concreto (cada línea de aplicación #val("12") en la nómina lleva un #campo("expediente") por comodidad administrativa) se IGNORA: el dinero se cotiza por la persona y se redistribuye entre sus expedientes proporcional al bruto retribuido. Esto es coherente con la realidad: la UJI cotiza una vez aunque la persona tenga dos expedientes.

=== Paso 2 — Reparto expediente → (actividad, centro de coste)

Cada expediente se trata de forma autónoma con las reglas de su sector. Para cada expediente se reúnen sus UC retributivas (las que llevan #campo("expediente") directo y, además, las que solo llevan #campo("per_id") pero corresponden al sector PDI/PVI — #ruta("cargos_uc.parquet") y #ruta("uc_reparto_regla_23.parquet") — atribuidas al expediente PDI/PVI de mayor bruto de la persona) y se reparte el cacho de SS entre los pares (#campo("actividad"), #campo("centro_de_coste")) proporcional al importe de las UC retributivas en cada par.

Si un expediente no tiene UC retributivas (p. ej. su servicio no está mapeado en #ruta("data", "entrada", "inventario", "servicios.xlsx")), su cacho de SS queda sin imputar y aflora como descuadre en la columna Δ de *Personal · PDI/PVI*. El visor permite identificar exactamente qué dato falta por catalogar para cerrarlo.

=== Elemento de coste de las UC de SS

Cada UC de SS lleva como #campo("id") un código seriado de la forma `SS-NNNNN`. El elemento de coste depende del *sector del expediente* (no del sector «principal» de la persona, ya obsoleto):

- #etqele("ss-ptgas") para expedientes PTGAS.
- #etqele("ss-pdi-func") para expedientes PDI cuya persona NO está en clases pasivas.
- #etqele("ss-pvi-otpersonal") para expedientes PVI.
- #etqele("prevsoc-funcs-pdi") cuando la persona del expediente PDI está en clases pasivas (PDI funcionario CU/TU/TEU/CEU sin aplicación #val("12")).

Como consecuencia de pasar a sectorial por expediente, ya no existe la idea de «sector principal» con prelación PTGAS > PVI > PDI > Otros. Cada expediente lleva la SS al elemento de coste que le corresponde directamente.

=== Compatibilidad con artefactos previos

Por compatibilidad con el visor y la fase 2, los outputs históricos se mantienen:

- #ruta("auxiliares", "nóminas", "persona_uc.parquet"): UC retributivas + UC de SS, ahora todas con #campo("expediente") explícito.
- #ruta("auxiliares", "nóminas", "persona_ss.parquet"): reparto de SS por (per_id, actividad, centro_de_coste). Se calcula agregando por per_id el reparto por expediente.

El visor *Personal · PDI/PVI* agrega los expedientes por per_id como overview. La columna Δ del master debería ser 0 para cada expediente; cuando una persona descuadra, el desglose por concepto en la pestaña *Resumen / Cuadre* señala el expediente concreto y la causa (típicamente un servicio sin mapeo en #ruta("servicios.xlsx") que impide repartir la SS del expediente).


= Fase de reparto de actividades

El modelo de contabilidad analítica exige que los costes de *gestión agregada* —las UC cuya actividad es de tipo #emph[dag] (identificador #val("dags") o que empieza por #val("dag-"): departamentos #etqact("dag-dfc"), vicerrectorados #etqact("dag-vi"), servicios generales #etqact("dag-sgc-…"), institutos, amortizaciones imputadas a #etqact("dags")…)— se repartan entre las *actividades finalistas* (no-dag) del mismo centro. Es una fase posterior e independiente de la fase 1, con su propio botón *Reparto actividades* en la barra lateral; toma como entrada el combinado #ruta("data", "fase1", "unidades de coste.xlsx") y produce sus propios artefactos y visores. Los informes de la fase 2 la consumirán más adelante. El módulo vive en #ruta("coana", "reparto").

== Modelo de reparto

El conjunto de UC de la fase 1 se conserva como entrada. El destino de cada actividad dag lo fija la *Tabla 1* (más abajo): cada actividad dag se reparte a las *actividades hoja* (finalistas) de su destino, que es *o bien* un centro *o bien* una actividad. Sobre ese conjunto de actividades hoja se precalcula el peso de cada una según su coste:

#campo("peso(hoja) = coste_no_dag(hoja) / Σ coste_no_dag(hojas del destino)")

Cada UC dag genera un *fragmento* por cada actividad hoja del destino, con #campo("importe = importe_dag × peso"). El fragmento recuerda su procedencia: #campo("origen") = #val("reparto-dag"), #campo("origen_id") = id de la UC dag original, #campo("origen_porción") = #campo("peso"). La UC dag original desaparece (no se duplica el coste).

Toda UC lleva además un campo nuevo, #campo("marca_dag"): #val("None") en las UC normales y la etiqueta dag de procedencia (p.ej. #etqact("dag-dfc")) en los fragmentos. Permitirá más adelante subtotalizar el coste de un centro separando lo no-dag de lo dag.

Es una sola pasada (la base no-dag se calcula del conjunto pre-reparto; no hay cascada dag→dag) y la conservación del importe total es exacta: #campo("Σ post-reparto = Σ entrada").

== Tabla 1: destino de cada actividad dag

La Tabla 1 es un *artefacto de diseño*: no proviene de la base de datos corporativa, sino que lo definimos al especificar el sistema. Por eso vive *en esta especificación* (y, reflejada, como literal en #ruta("coana", "reparto", "tabla_dag_centro.py")), no como hoja de cálculo. Tiene tres columnas: la actividad dag a repartir y su destino, que es *exactamente uno* de los otros dos: un *centro* (se reparte a sus actividades finalistas) o una *actividad* (se reparte a las hojas de su subárbol).

*Departamentos, facultades, institutos y grupos de investigación* siguen la *convención*: #etqact("dag-X") reparte a las hojas del centro #etqcen("X") (la parte tras #val("dag-")). Son muchos (los grupos, en particular) y comparten un patrón claro, así que no se enumeran aquí; podrán afinarse caso a caso en el futuro. Aparte, el nodo paraguas #etqact("dags") (amortizaciones genéricas) reparte a #etqact("principales").

*Servicios de la UJI.* Estos sí se enumeran por completo, porque su destino es una decisión de diseño que querremos afinar. La primera versión lleva como destino por defecto la actividad global #etqact("principales") (toda la actividad finalista de la UJI); cada fila puede cambiarse a un centro (col. 2) o a otra actividad (col. 3). El orden sigue la estructura organizativa del árbol de actividades.

#table(
    columns: (1.7fr, auto, auto),
    stroke: 0.5pt + luma(80%),
    inset: 5pt,
    table.header(table.hline(), [*actividad dag*], [*centro*], [*actividad*], table.hline()),
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Rectorado*]],
    [#etqact("dag-rectorado")], [—], [#etqact("principales")],
    [#etqact("dag-delegado")], [—], [#etqact("principales")],
    [#etqact("dag-síndico-agravios")], [—], [#etqact("principales")],
    [#etqact("dag-inspección-servicios")], [—], [#etqact("principales")],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Vicerrectorados*]],
    [#etqact("dag-vi")], [—], [#etqact("principales")],
    [#etqact("dag-vefp")], [—], [#etqact("principales")],
    [#etqact("dag-voap")], [—], [#etqact("principales")],
    [#etqact("dag-vevs")], [—], [#etqact("principales")],
    [#etqact("dag-vri")], [—], [#etqact("principales")],
    [#etqact("dag-vitdc")], [—], [#etqact("principales")],
    [#etqact("dag-vrspii")], [—], [#etqact("principales")],
    [#etqact("dag-vcls")], [—], [#etqact("principales")],
    [#etqact("dag-vis")], [—], [#etqact("principales")],
    [#etqact("dag-vpee")], [—], [#etqact("principales")],
    [#etqact("dag-org-vicerrectorados-tributos")], [—], [#etqact("principales")],
    [#etqact("dag-org-vicerrectorados-arrendamiento-bienes")], [—], [#etqact("principales")],
    [#etqact("dag-org-vicerrectorados-reparación-conservación")], [—], [#etqact("principales")],
    [#etqact("dag-org-vicerrectorados-suministros")], [—], [#etqact("principales")],
    [#etqact("dag-org-vicerrectorados-transportes-comunicaciones")], [—], [#etqact("principales")],
    [#etqact("dag-org-vicerrectorados-trabajos-realizados-otras-empresas")], [—], [#etqact("principales")],
    [#etqact("dag-org-vicerrectorados-primas-seguros")], [—], [#etqact("principales")],
    [#etqact("dag-org-vicerrectorados-material-oficina")], [—], [#etqact("principales")],
    [#etqact("dag-org-vicerrectorados-gastos-diversos")], [—], [#etqact("principales")],
    [#etqact("dag-org-vicerrectorados-gastos-financieros")], [—], [#etqact("principales")],
    [#etqact("dag-org-vicerrectorados-adquisiciones-bibliográficas")], [—], [#etqact("principales")],
    [#etqact("dag-org-vicerrectorados-indemnizaciones-razón-servicio")], [—], [#etqact("principales")],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Secretaría General*]],
    [#etqact("dag-secretaría-general")], [—], [#etqact("principales")],
    [#etqact("dag-junta-electoral")], [—], [#etqact("principales")],
    [#etqact("dag-asesoría-jurídica")], [—], [#etqact("principales")],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Gerencia*]],
    [#etqact("dag-gerencia")], [—], [#etqact("principales")],
    [#etqact("dag-org-gerencia-tributos")], [—], [#etqact("principales")],
    [#etqact("dag-org-gerencia-arrendamiento-bienes")], [—], [#etqact("principales")],
    [#etqact("dag-org-gerencia-reparación-conservación")], [—], [#etqact("principales")],
    [#etqact("dag-org-gerencia-suministros")], [—], [#etqact("principales")],
    [#etqact("dag-org-gerencia-transportes-comunicaciones")], [—], [#etqact("principales")],
    [#etqact("dag-org-gerencia-trabajos-realizados-otras-empresas")], [—], [#etqact("principales")],
    [#etqact("dag-org-gerencia-primas-seguros")], [—], [#etqact("principales")],
    [#etqact("dag-org-gerencia-material-oficina")], [—], [#etqact("principales")],
    [#etqact("dag-org-gerencia-gastos-diversos")], [—], [#etqact("principales")],
    [#etqact("dag-org-gerencia-gastos-financieros")], [—], [#etqact("principales")],
    [#etqact("dag-org-gerencia-adquisiciones-bibliográficas")], [—], [#etqact("principales")],
    [#etqact("dag-org-gerencia-indemnizaciones-razón-servicio")], [—], [#etqact("principales")],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Consejo Social y Consejo de estudiantes*]],
    [#etqact("dag-consejo-social")], [—], [#etqact("principales")],
    [#etqact("dag-consejo-estudiantes")], [—], [#etqact("principales")],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Servicios Generales y Centrales*]],
    [#etqact("dag-scag")], [—], [#etqact("principales")],
    [#etqact("dag-sci")], [—], [#etqact("principales")],
    [#etqact("dag-sge")], [—], [#etqact("principales")],
    [#etqact("dag-sic")], [—], [#etqact("principales")],
    [#etqact("dag-srh")], [—], [#etqact("principales")],
    [#etqact("dag-sgit")], [—], [#etqact("principales")],
    [#etqact("dag-gencisub")], [—], [#etqact("principales")],
    [#etqact("dag-conserjería-estce")], [—], [#etqact("principales")],
    [#etqact("dag-conserjería-fcje")], [—], [#etqact("principales")],
    [#etqact("dag-conserjería-fchs")], [—], [#etqact("principales")],
    [#etqact("dag-conserjería-fcs")], [—], [#etqact("principales")],
    [#etqact("dag-conserjería-consejo-social")], [—], [#etqact("principales")],
    [#etqact("dag-conserjería-rectorado")], [—], [#etqact("principales")],
    [#etqact("dag-conserjería-parque-tecnológico")], [—], [#etqact("principales")],
    [#etqact("dag-sgc-tributos")], [—], [#etqact("principales")],
    [#etqact("dag-sgc-arrendamiento-bienes")], [—], [#etqact("principales")],
    [#etqact("dag-sgc-reparación-conservación")], [—], [#etqact("principales")],
    [#etqact("dag-sgc-suministros")], [—], [#etqact("principales")],
    [#etqact("dag-sgc-transportes-comunicaciones")], [—], [#etqact("principales")],
    [#etqact("dag-sgc-trabajos-realizados-otras-empresas")], [—], [#etqact("principales")],
    [#etqact("dag-sgc-primas-seguros")], [—], [#etqact("principales")],
    [#etqact("dag-sgc-material-oficina")], [—], [#etqact("principales")],
    [#etqact("dag-sgc-gastos-diversos")], [—], [#etqact("principales")],
    [#etqact("dag-sgc-gastos-financieros")], [—], [#etqact("principales")],
    [#etqact("dag-sgc-adquisiciones-bibliográficas")], [—], [#etqact("principales")],
    [#etqact("dag-sgc-indemnizaciones-razón-servicio")], [—], [#etqact("principales")],
    [#etqact("dag-sgc-indemnizaciones-asistencias")], [—], [#etqact("principales")],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Otros servicios de soporte general*]],
    [#etqact("dag-otros-servicios-comunicación-publicaciones")], [—], [#etqact("principales")],
    [#etqact("dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico")], [—], [#etqact("principales")],
    [#etqact("dag-otros-servicios-prevención-gestión-medioambiental")], [—], [#etqact("principales")],
    [#etqact("dag-otros-servicios-ti")], [—], [#etqact("principales")],
    [#etqact("dag-otros-servicios-obras-proyectos")], [—], [#etqact("principales")],
    [#etqact("dag-otros-servicios-información-registro")], [—], [#etqact("principales")],
    [#etqact("dag-otros-servicios-promoción-evaluación-calidad")], [—], [#etqact("principales")],
    [#etqact("dag-otros-servicios-relaciones-internacionales")], [—], [#etqact("principales")],
    [#etqact("dag-otros-servicios-atención-diversidad-apoyo-educativo")], [—], [#etqact("principales")],
    [#etqact("dag-otros-servicios-promoción-fomento-igualdad")], [—], [#etqact("principales")],
    [#etqact("dag-convivencia")], [—], [#etqact("principales")],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Soporte a extensión universitaria*]],
    [#etqact("dag-deportes")], [—], [#etqact("principales")],
    [#etqact("dag-cultura")], [—], [#etqact("principales")],
    [#etqact("dag-cooperación")], [—], [#etqact("principales")],
    [#etqact("dag-apoyo-estudiantes")], [—], [#etqact("principales")],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Biblioteca*]],
    [#etqact("dag-biblioteca")], [—], [#etqact("principales")],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Apoyo a la docencia oficial*]],
    [#etqact("dag-cent")], [—], [#etqact("principales")],
    [#etqact("dag-ufie")], [—], [#etqact("principales")],
    [#etqact("dag-sgde")], [—], [#etqact("principales")],
    [#etqact("dag-oe")], [—], [#etqact("principales")],
    [#etqact("dag-oipep")], [—], [#etqact("principales")],
    [#etqact("dag-opp")], [—], [#etqact("principales")],
    [#etqact("dag-uo")], [—], [#etqact("principales")],
    [#etqact("dag-encargos-gestión")], [—], [#etqact("principales")],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Apoyo a la investigación*]],
    [#etqact("dag-scic")], [—], [#etqact("principales")],
    [#etqact("dag-sea")], [—], [#etqact("principales")],
    [#etqact("dag-encargos-proyectos-investigación-europeos")], [—], [#etqact("principales")],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Laboratorios de Docencia e Investigación*]],
    [#etqact("dag-labcom")], [—], [#etqact("principales")],
    [#etqact("dag-sala-disección")], [—], [#etqact("principales")],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Escuela de Doctorado*]],
    [#etqact("dag-escuela-doctorado")], [#etqcen("ed")], [—],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Apoyo a estudios propios*]],
    [#etqact("dag-encargos-gestión-estudios-propios")], [—], [#etqact("principales")],
    [#etqact("dag-encargos-gestión-microcredenciales")], [—], [#etqact("principales")],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Apoyo a la transferencia del conocimiento*]],
    [#etqact("dag-encargos-gestión-transferencia")], [—], [#etqact("principales")],
    [#etqact("dag-encargos-gestión-espaitec")], [—], [#etqact("principales")],
    [#etqact("dag-innovación-emprendeduría")], [—], [#etqact("principales")],
    [#etqact("dag-divulgación-científica")], [—], [#etqact("principales")],
    table.cell(colspan: 3)[#text(fill: luma(45%))[*Apoyo a proyectos internacionales*]],
    [#etqact("dag-encargos-gestión-proyectos-internacionales")], [—], [#etqact("principales")],
    table.hline(),
)

Esta tabla es el reflejo de #campo("SERVICIOS") en #ruta("coana", "reparto", "_servicios.py"); ambos se mantienen sincronizados.

== Anomalías del reparto

Con la regla de servicios centrales, prácticamente toda actividad dag tiene un destino con base. Una UC dag cuyo destino no tenga *ninguna* actividad hoja con coste *no se reparte*: se conserva intacta (para no perder coste) y se lista en el visor de anomalías para revisión. El visor permite además localizar las actividades dag que se han repartido a #etqact("principales") por la regla por defecto, para decidir si merecen una excepción más fina en la Tabla 1.

== Artefactos y visores

La fase escribe en #ruta("data", "fase1", "reparto"): #ruta("uc_post_reparto.parquet") (UC tras el reparto, con #campo("marca_dag")), #ruta("porcentajes_centro.parquet") (la tabla de % por centro) y #ruta("anomalias.parquet"). El bloque *Reparto de actividades* de la #app expone cuatro vistas —Resumen, UC tras reparto, Porcentajes por centro y Anomalías— y reutiliza el gestor de jobs y el panel terminal de la fase 1.


= Fase 2: Informes consolidados

La fase 2 produce, sobre el conjunto de UC generadas por la fase 1, una colección de informes normalizados que siguen la plantilla del modelo Crue 2024 (cuadros 10.1 a 10.7 del capítulo 10) más un *constructor de informes a la carta* que permite recorrer las UC en cualquier permutación de los tres ejes (centro de coste, actividad, elemento de coste). El módulo vive en #ruta("coana", "fase2") y se invoca con

#raw("uv run coana informes")

que regenera todos los cuadros normalizados como artefactos paralelos: un YAML estructurado (datos crudos), un XLSX con tipografía Calibri (lista para imprimir) y una sección dedicada en #ruta("documentación", "informes", "informes.typ") que se compila a PDF con #raw("uv run informes"). El documento Typst único carga los YAML con #raw("yaml(\"…\")") en tiempo de compilación, así que el flujo es: editar prosa a mano y reejecutar #raw("uv run informes") tras cada nueva pasada de fase 1.

== Estructura común de los cuadros normalizados

Cada cuadro 10.x se modela con una *plantilla SUE* hardcoded (lista fija de filas a mostrar, con código numérico oficial, slug del árbol y rótulo) y un *árbol* sobre el que se computan los importes. La función `importes_por_nodo` en #ruta("coana", "fase2", "calculo.py") agrega, para cada slug X del árbol, la suma `A(X) + B(X)` (lo asignado directamente a X más lo que cuelga de sus descendientes); el motor común `generar_cuadro_jerárquico` de #ruta("coana", "fase2", "_cuadro_jerarquico.py") aplica la plantilla y emite YAML+XLSX con tres columnas: importe, `% elemento` (sobre el grupo nivel-1 inmediato) y `% total` (solo en filas nivel-1).

Los slugs de la plantilla pueden coincidir o no con los del fichero #ruta("data", "entrada", "estructuras", "<árbol>.tree"). Los árboles que carga la fase 2 son los *enriquecidos* tras la fase 1 (#ruta("data", "fase1", "<árbol>.tree")), porque la fase 1 añade nodos dinámicamente (grupos de investigación, cátedras UNESCO, etc.) que un cuadro como el 10.4 necesita ver. Las UC cuyo slug cae fuera del subárbol de cualquier nodo nivel-1 de la plantilla aparecen al final del cuadro en una fila «Sin clasificar en plantilla SUE».

== Catálogo de cuadros normalizados

=== Cuadro 10.1 — Informe de elementos de coste

Lista plana del árbol de elementos de coste según la plantilla oficial (códigos 01, 01.01 … 09.03). Para cada nodo, importe absoluto + `% elemento` dentro de su grupo nivel-1 + `% total` sobre el total general. El #ruta("coana", "fase2", "cuadro_10_1.py") implementa la plantilla; usa la columna #campo("elemento_de_coste") de las UC.

Cifras 2025: total #val("152 779 410,82 €") repartidos como 01 Costes de personal #val("113,5 M") · 03 Bienes y servicios #val("10,4 M") · 04 Servicios exteriores #val("16,4 M") · 05 Tributos · 06 Costes financieros · 07 Amortizaciones #val("6,3 M") · 09 Transferencias #val("3,0 M"). Sin filas «sin clasificar» (toda la masa cae bajo algún nodo de la plantilla).

=== Cuadro 10.3 — Informe general de ingresos por actividades

Plantilla del SUE con cinco niveles jerárquicos. Mientras la fase 1 no procese los ingresos, esta vista emite la estructura completa con importes a #val("0,00 €") y un aviso visible *Estructura preliminar*. El #ruta("coana", "fase2", "cuadro_10_3.py") contiene la plantilla de 34 filas estables; los niveles dinámicos del PDF original (ámbito de conocimiento, grado N, máster N, programa de doctorado N) se generarán cuando entren los ingresos al pipeline.

=== Cuadro 10.4 — Informe de costes por centros de coste según su finalidad

Plantilla SUE sobre el árbol de centros de coste (8 nodos nivel-1: docencia, investigación, docencia e investigación, apoyo, extensión universitaria, soporte, anexos, agrupaciones). Para varios subgrupos se exigen filas nivel-3 con los hijos del árbol enriquecido (facultades, institutos, departamentos, áreas administrativas, etc.); el set `_EXPANDIR_NIVEL_3` en #ruta("cuadro_10_4.py") controla qué slugs se desglosan.

Cifras 2025: total #val("152,8 M") repartidos como 01 Docencia #val("30,3 M") · 02 Investigación #val("37,7 M") · 03 Docencia e investigación #val("25,9 M") · 04 Apoyo #val("11,0 M") · 05 Extensión #val("5,2 M") · 06 Soporte #val("33,1 M") · 07 Anexos · 08 Agrupaciones. *Sin clasificar* #val("≈ 9,5 M") en centros intermedios (edificios) y CC pendiente — saldrán de ese cubo cuando se implemente la fase 3 del modelo.

=== Cuadro 10.5 — Informe de costes primarios por centro de coste

Un sub-cuadro por cada centro nivel-1 del 10.4 (8 sub-tablas), con la misma plantilla de elementos de coste del 10.1 y tres columnas: *Directo* (UC con #campo("regla_cc") nula = el CC se conoce con exactitud del dato), *Indirecto* (asignación al CC por algoritmo de reparto) y *Primario (D+I)*. Cada sub-cuadro lleva al final tres filas adicionales:

- *Total coste primario*: la suma D+I obtenida en fase 1.
- *Centros superiores*: 0 € — pendiente de la fase 3.a del modelo (imputación de centros de nivel superior).
- *Actividades auxiliares*: 0 € — pendiente de la fase 3.d.
- *Total*: igual al primario mientras no se implementen las dos anteriores.

Cifras 2025: suma de primarios #val("143,2 M €"); ratio directo/indirecto: 115,4 M / 27,8 M.

=== Cuadro 10.7 — Composición del coste de las actividades finalistas

Matriz «actividad finalista × tipo de centro». Filas: jerarquía de actividades #etqact("principales") (docencia, investigación, extensión); columnas: agrupaciones de centros (Depts., Biblioteca, Laboratorios, Aulas, DAG, Otros, Total) definidas en `_COLUMNAS` de #ruta("cuadro_10_7.py"). Solo cuentan las UC cuya actividad cuelga del subárbol #etqact("principales"); las actividades DAG, anexas y de organización no aparecen en este cuadro.

Cifras 2025: total finalistas #val("80,5 M €") (≈ #val("53 %") del total general). Las columnas Biblioteca, Laboratorios y Aulas aparecen vacías porque sus UC tienen actividad DAG, no finalista; se llenarán cuando la fase 3 reimpute esos centros a actividades finalistas.

== Informes a la carta

El menú *Informes · A la carta* (#ruta("coana", "web", "routers", "informes_carta.py")) permite construir una vista jerárquica ad-hoc:

+ Seleccionar uno o más slugs en cada uno de los tres ejes (CC, actividades, EC). Selección vacía = «todos». Cada slug elegido incluye implícitamente su subárbol.
+ Elegir el orden de los tres niveles arrastrando tres fichas (CC / actividad / EC en cualquier permutación). Las fichas llevan un *handle rugoso* a la izquierda (#val("dots-grip")) y se reordenan con drag & drop nativo HTML5.
+ Pulsar *Generar*: la consulta agrega las UC filtradas y devuelve una tabla expandible con tres niveles, mostrando `n_ucs` e importe por nodo, subtotales y total.
+ Al pulsar el botón *UCs* de un nodo se abre un modal con las UC concretas que caen en esa combinación (ancestros acumulados desde la raíz).

*Configuraciones guardadas*. El usuario puede dar un nombre a la combinación actual y persistirla en #ruta("data", "informes", "carta_configs", "<nombre>.yaml"); reaparece en el desplegable «Cargar…» para reaplicarla con un clic. También admite eliminar configuraciones existentes. CRUD vía endpoints #raw("/api/informes-carta/configs[/{nombre}]") (GET / PUT / DELETE).

*Exportación*. Dos botones adicionales generan el mismo informe como descarga:

- *Descargar Excel*: `POST /api/informes-carta/excel` produce un XLSX con la jerarquía indentada (Calibri, sombreado por nivel).
- *Descargar PDF*: `POST /api/informes-carta/pdf` compila al vuelo un Typst inline a un PDF apaisado (#raw("typst compile --root <tmp>")).

== Activación desde el visor

El visor expone dos botones en el sidebar:

- *Cálculo de unidades de coste* — lanza la fase 1 en segundo plano (job tipo #raw("\"fase1\"") en el manager #ruta("coana", "web", "streaming.py")).
- *Generar informes* — lanza la fase 2 + compilación del PDF. NO abre el PDF automáticamente; un botón paralelo *Abrir PDF*, habilitado cuando el archivo existe en disco, llama a `POST /api/sistema/informes/abrir-pdf`. La salida en curso de ambos botones se canaliza al panel terminal global (#ruta("coana", "web", "frontend", "src", "lib", "terminalStore.ts")), que conserva el histórico acumulado entre ejecuciones y se autoculta a los 5 s tras pulsar el botón; un icono de terminal en el pie del sidebar permite mostrarlo/ocultarlo manualmente sin auto-hide.


= Resultados

En la #app, ha de haber un desplegable «Resultados» con las siguientes entradas:

- Actividades: se muestra una tabla con las actividades (incluyendo las que se han creado) y los costes asignados. La tabla tiene código, actividad, etiqueta, importe asignado por unidades de coste a partir de presupuesto (y número de uc), ídem por amortizaciones e ídem por nóminas.
- Centros de coste: lo mismo, pero con centros de coste.
- Elementos de coste: lo mismo, pero con elementos de coste.

Al pinchar en una actividad, se verán las unidades de coste de cada tipo que se le han asignado en una tabla adicional.


= Apéndice: tablas de mapeo críticas

Las siguientes tablas se mantienen en código (no en #ruta("data", "configuración.xlsx")) porque son mapeos «estructurales» del modelo que no varían año a año, sino con cambios de norma o de catálogo administrativo. Se documentan aquí en una única pasada para que un implementador pueda reconstruir la lógica sin tener que leer Python.

== Mapeo categoría → XXX del elemento de coste PTGAS

(constante `_PTGAS_CAT_XXX` en #ruta("coana", "fase1", "nóminas", "__init__.py"))

#table(
    columns: 3,
    align: (center, left, left),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*Categoría*], [*XXX*], [*Significado*], table.hline()),
    val("FC"), val("func"), [Funcionario de carrera],
    val("FI"), val("func"), [Funcionario interino (mismo subárbol que carrera)],
    val("E"), val("ev"), [Personal eventual],
    val("LF"), val("labfijo"), [Laboral fijo],
    val("LT"), val("labtemp"), [Laboral temporal],
    val("LE"), val("labtemp"), [Laboral eventual (comparte subárbol con LT)],
    table.hline(),
)

Excepción: cuando la categoría es #val("FC") y el #campo("per_id") coincide con el valor #campo("_PTGAS_PER_ID_DIR") (gerente actual: #val("65214")), XXX = #val("dir") en lugar de #val("func"). Es un caso singular que se actualiza con cada cambio de gerencia.

== Mapeo categoría → XXX del elemento de coste PDI

(constante `_PDI_CAT_XXX` en #ruta("coana", "fase1", "nóminas", "__init__.py"))

#table(
    columns: 3,
    align: (center, left, left),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*Categorías*], [*XXX*], [*Significado*], table.hline()),
    [#val("CU")], val("cu"), [Catedrático de universidad],
    [#val("TU"), #val("TUI")], val("tu"), [Titular de universidad (titular e interino)],
    [#val("CEU")], val("ceu"), [Catedrático de escuela universitaria],
    [#val("TEU")], val("teu"), [Titular de escuela universitaria],
    [#val("AJ"), #val("AJD"), #val("AJDII")], val("aj"), [Ayudante doctor (todas sus variantes)],
    [#val("PAA"), #val("PAL")], val("as"), [Profesor asociado (asistencial y normal)],
    [#val("PS")], val("ps"), [Profesor sustituto],
    [#val("PEME")], val("em"), [Profesor emérito],
    [#val("PPL"), #val("PPLV")], val("pl"), [Profesor permanente laboral],
    [#val("PVI")], val("pv"), [Profesor vinculado a la investigación],
    [#val("PD")], val("pd"), [Profesor distinguido],
    [#val("PCD")], val("pcd"), [Profesor contratado doctor],
    [#val("PC")], val("pc"), [Profesor contratado],
    table.hline(),
)

== Mapeo concepto_retributivo → YYY del elemento de coste

(constante `_PTGAS_CR_YYY`; aplica también al PDI y al PVI vía las funciones `_elemento_coste_*`)

#table(
    columns: 5,
    align: (center, center, center, center, center),
    stroke: 0.5pt + luma(80%),
    inset: 4pt,
    table.header(table.hline(), [*CR*], [*YYY*], [*CR*], [*YYY*], [*CR*], table.hline()),
    val("01"), val("sueldo"), val("32"), val("prod"), val("62"),
    val("03"), val("trienios"), val("34"), val("otfij"), val("64"),
    val("04"), val("paga-extra"), val("35"), val("otvars"), val("67"),
    val("05"), val("esp"), val("43"), val("otvars"), val("68"),
    val("06"), val("esp"), val("44"), val("trienios"), val("70"),
    val("10"), val("dst"), val("47"), val("otvars"), val("71"),
    val("12"), val("dst"), val("53"), val("otvars"), val("72"),
    val("13"), val("otvars"), val("55"), val("otvars"), val("75"),
    val("15"), val("esp"), val("56"), val("esp"), val("76"),
    val("17"), val("otfij"), val("57"), val("otfij"), val("77"),
    val("18"), val("esp"), val("59"), val("dst"), val("78"),
    val("19"), val("cargos"), val("---"), val("---"), val("80"),
    val("20"), val("quin"), val("---"), val("---"), val("82"),
    val("24"), val("dst"), val("---"), val("---"), val("83"),
    val("25"), val("otvars"), val("---"), val("---"), val("86"),
    val("26"), val("sexinv"), val("---"), val("---"), val("87"),
    val("30"), val("cargos"), val("---"), val("---"), val("90"),
    table.hline(),
)

Tabla resumida; el código fuente contiene la lista completa con todas las correspondencias. Los CR #val("47") (despidos), #val("48") (indemnizaciones por asistencias), #val("19")/#val("64") (cargos) y #val("30")/#val("87") (atrasos) tienen tratamiento especial fuera de la simple traducción a YYY.

== Mapeo sector en RR.HH. → sector canónico del modelo

(constante `_MAPEO_SECTOR` en #ruta("coana", "fase1", "nóminas", "__init__.py"))

#table(
    columns: 3,
    align: (center, center, left),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*Sector RR.HH.*], [*Sector canónico*], [*Notas*], table.hline()),
    val("PDI"), val("PDI"), [Personal docente e investigador.],
    val("PAS"),
    val("PTGAS"),
    [Personal de administración y servicios; el código canónico es PTGAS desde la reforma LOSU.],
    val("PI"), val("PVI"), [Personal investigador (vinculado a investigación).],
    table.hline(),
)

Cualquier otro código se canaliza a #val("Otros") (becarios, jubilados, etc.). La prelación por la que se elige el *sector principal* de una persona con varios expedientes es #val("PTGAS") > #val("PVI") > #val("PDI") > #val("Otros") (constante `_PRELACIÓN_SECTOR`).

== Reglas de tipo de anexo de proyecto → actividad y h/semana

(constante `_REGLAS` en #ruta("coana", "fase1", "regla23", "cargadores", "proyectos.py"))

La concatenación #campo("tipo_anexo") + #campo("subtipo_anexo") + #campo("microtipo_anexo") clasifica cada contrato de SGIT. Las reglas se aplican en orden de primera coincidencia. El símbolo #val("*") es un comodín.

#table(
    columns: 3,
    align: (center, left, right),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*Patrón*], [*Actividad*], [*h/sem*], table.hline()),
    val("2PE"), etqact("ai-internacional"), val("10"),
    val("2PN"), etqact("ai-nacional"), val("10"),
    val("2PV"), etqact("ai-regional"), val("10"),
    val("2PA"), etqact("ai-nacional"), val("10"),
    val("2PI"), etqact("ai-internacional"), val("10"),
    val("2PU"), etqact("ai-plan-propio"), val("3"),
    val("1CE"), etqact("cátedras-aulas-empresa"), val("2"),
    val("1AA"), etqact("transf"), val("1"),
    val("1**"), etqact("transf"), val("8"),
    table.hline(),
)

== Tipos del Real Decreto 1086/1989 (cargos académicos)

(catálogo en #ruta("data", "entrada", "nóminas", "cargos real decreto.xlsx"))

Los #val("8") tipos del RD 1086/1989 con su importe mensual de referencia en el año analizado. El campo #campo("cargo_asimilado") de #ruta("data", "entrada", "nóminas", "cargos.xlsx") asocia cada cargo institucional a uno de estos ocho tipos (o queda nulo si el cargo no está asimilable, en cuyo caso no se le imputan retribuciones por la regla de reparto). La cuantía mensual se usa como peso para repartir la masa CR 19/64 en proyecto general entre los cargos vigentes de cada persona.

#nota[El detalle de los importes y los nombres de los tipos vive en el Excel #ruta("cargos real decreto.xlsx"); aquí no se transcribe para evitar duplicar la fuente de verdad.]


= Apéndice: artefactos generados por la fase 1

Este apéndice cataloga todos los ficheros que la fase 1 escribe en #ruta("data", "fase1"). Para cada uno se indica una descripción de su contenido y los ficheros de entrada (de #ruta("data", "entrada")), reglas y otros artefactos previos de los que depende, de modo que pueda reconstruirse el grafo de dependencias sin leer el código.

Convenciones del apéndice:
- Las rutas son relativas a #ruta("data", "fase1") salvo cuando se citen explícitamente entradas de #ruta("data", "entrada").
- Cuando un artefacto solo se genera si hay datos que lo justifiquen (por ejemplo, anomalías), se indica de forma explícita.
- Las referencias a secciones (§) apuntan a la spec donde están las reglas que producen el artefacto.

== Presupuesto

/ #ruta(
        "uc presupuesto.parquet",
    ): unidades de coste generadas por el traductor de presupuesto, una por apunte clasificado (más expansiones de suministros distribuidos #val("SC001")). Producido a partir de #ruta("data", "entrada", "presupuesto", "apuntes presupuesto de gasto.xlsx") y los ficheros de catálogo asociados (centros, subcentros, proyectos, subproyectos, líneas de financiación, programas, aplicaciones, capítulos, artículos, conceptos, tipos de proyecto, tipos de línea), aplicando el filtro previo de §«Generación de UC a partir de presupuesto», las reglas de centro de coste, elemento de coste y actividad, y la traducción aplicación → elemento de coste de #ruta("data", "entrada", "presupuesto", "aplicaciones a elementos de coste.xlsx"). El reparto de los apuntes con #campo("centro") = #val("SC001") usa la matriz de #ruta("auxiliares", "amortizaciones", "inventario_enriquecido.parquet") (vía #emph[distribución de costes OTOP]).

/ #ruta(
        "presupuesto sin uc.parquet",
    ): apuntes presupuestarios que sobreviven al filtro previo pero no encajan con ninguna regla de actividad. Producido a partir de los apuntes filtrados.

/ #ruta(
        "auxiliares",
        "filtrados_presupuesto.parquet",
    ): apuntes descartados por el filtro previo, con la regla concreta que los descarta. Producido a partir de los apuntes y las reglas de §«Filtro previo».

/ #ruta(
        "auxiliares",
        "sin_clasificar_presupuesto.parquet",
    ): apuntes que pasaron el filtro pero no obtuvieron actividad (subconjunto enriquecido de #ruta("presupuesto sin uc.parquet"), con #campo("tipo_proyecto") añadido).

/ #ruta(
        "auxiliares",
        "conteo_reglas_presupuesto.parquet",
    ): conteo (n filas e importe) de cuántas veces se aplicó cada regla con nombre del traductor. Producido durante la traducción.

/ #ruta("auxiliares", "conteo_cc_presupuesto.parquet"): conteo (n filas e importe) por regla de centro de coste.

/ #ruta("auxiliares", "conteo_ec_presupuesto.parquet"): conteo (n filas e importe) por regla de elemento de coste.

/ #ruta(
        "auxiliares",
        "resumen.json",
    ): contadores agregados de la fase 1 (nº de UC y de filtrados por etapa, nº de nodos antes/después en cada árbol). Se reescribe al final con todas las cifras consolidadas.

== Inventario y amortizaciones

/ #ruta(
        "auxiliares",
        "amortizaciones",
        "inventario_enriquecido.parquet",
    ): registros de inventario que pasan los filtros previos, enriquecidos con #campo("años_amortización"), #campo("días_en_año") e #campo("importe") amortizado en el año. Es la tabla maestra que alimenta tanto las UC de amortizaciones como el reparto de suministros distribuidos. Producido a partir de #ruta("data", "entrada", "inventario", "inventario.xlsx") y #ruta("data", "entrada", "inventario", "años amortización por cuenta.xlsx"), aplicando las reglas de §«Cálculo del importe de amortización en el año analizado y reglas de filtrado».

/ #ruta(
        "auxiliares",
        "amortizaciones",
        "filtrados_estado.parquet",
    ): registros descartados por #campo("estado") = #val("B"). Origen: regla #nombre-regla[Supresión de elementos de baja].

/ #ruta(
        "auxiliares",
        "amortizaciones",
        "sin_cuenta.parquet",
    ): registros descartados por no tener #campo("cuenta"). Origen: regla #nombre-regla[Supresión por falta de cuenta contable].

/ #ruta(
        "auxiliares",
        "amortizaciones",
        "filtrados_cuenta.parquet",
    ): registros descartados por #campo("cuenta") con prefijo no aceptado. Origen: regla #nombre-regla[Supresión por cuentas contables].

/ #ruta(
        "auxiliares",
        "amortizaciones",
        "detalle_cuentas_filtradas.parquet",
    ): resumen agregado (n y valor inicial) por #campo("cuenta") de los registros descartados por prefijo. Producido a partir del anterior.

/ #ruta(
        "auxiliares",
        "amortizaciones",
        "sin_fecha_alta.parquet",
    ): registros sin #campo("fecha_alta"). Origen: regla #nombre-regla[Supresión por falta de información del alta].

/ #ruta(
        "auxiliares",
        "amortizaciones",
        "filtrados_fecha.parquet",
    ): registros con #campo("importe") = 0 o #campo("días_en_año") = 0 tras el enriquecimiento. Origen: regla #nombre-regla[Supresión de elementos que no se amortizan en el año].

/ #ruta(
        "uc amortizaciones.parquet",
    ): UC generadas a partir de los registros de inventario enriquecidos, asignando #campo("centro_de_coste") por #campo("id_ubicación") (con reparto a partes iguales si hay varios) o por reglas sobre #campo("descripción"). Producido a partir de #ruta("auxiliares", "amortizaciones", "inventario_enriquecido.parquet") y las reglas de §«Reglas para generar unidades de coste a partir de amortizaciones».

/ #ruta(
        "auxiliares",
        "amortizaciones",
        "sin_uc.parquet",
    ): registros enriquecidos que no recibieron #campo("centro_de_coste") (ni por ubicación ni por descripción). Producido a partir del enriquecido.

== Suministros (energía, agua, gas)

/ #ruta(
        "uc suministros.parquet",
    ): UC generadas por el reparto de gasto de energía, agua y gas según presencia de cada centro de coste en el complejo, edificio o zona. Producido a partir de #ruta("data", "entrada", "consumos", "energía.xlsx"), #ruta("data", "entrada", "consumos", "agua.xlsx") y #ruta("data", "entrada", "consumos", "gas.xlsx"), y las matrices de presencia derivadas de #ruta("data", "entrada", "superficies", "ubicaciones.xlsx") y #ruta("data", "entrada", "superficies", "ubicaciones a servicios.xlsx") según §«Asignación de metros a cada servicio». Las filas con prefijo sin match se descartan y se reportan en la #app.

== Nóminas: preprocesamiento

/ #ruta("auxiliares", "nóminas", "PDI.parquet"), #ruta("auxiliares", "nóminas", "PVI.parquet"), #ruta("auxiliares", "nóminas", "PTGAS.parquet"), #ruta("auxiliares", "nóminas", "Otros.parquet"): expedientes clasificados por sector tras el preprocesamiento de #ruta("data", "entrada", "nómina", "nóminas y seguridad social.xlsx"), con sus líneas y métricas agregadas. Origen: §«Preprocesamiento nóminas».

/ #ruta(
        "auxiliares",
        "nóminas",
        "multiexpediente.parquet",
    ): personas con expedientes en sectores distintos en el mismo año. Producido a partir de los cuatro parquets sectoriales.

/ #ruta(
        "auxiliares",
        "nóminas",
        "multiexpediente_actividad.parquet",
    ): información de actividad sobreescrita para esas personas (asignada al expediente principal).

/ #ruta(
        "auxiliares",
        "categoría_última_pdi_pvi.parquet",
    ): por #campo("per_id"), categoría RR.HH. más reciente observada en nóminas (PDI/PVI), usada por el cargador de cargos académicos para componer el elemento de coste `pdi-XXX-cargos` o `piyotper-XXX-cargos`. Producido durante el preprocesamiento.

/ #ruta(
        "auxiliares",
        "cargos_departamentos.parquet",
    ): mapeo de cargos a (centro, actividad) cuando el patrón de #ruta("data", "entrada", "nóminas", "cargos.xlsx") incluye placeholders dependientes del departamento del cargo. Producido durante el preprocesamiento de cargos académicos.

/ #ruta(
        "auxiliares",
        "nóminas",
        "costes_sociales_calculados.parquet",
    ): por persona del PDI funcionario en régimen de clases pasivas, el detalle del cálculo de su coste social simulado (base, contingencias comunes, MEI, formación profesional, cuotas de solidaridad por tramos y total). Producido por #campo("_generar_costes_sociales_calculados") con las constantes de SS de #ruta("data", "configuración.xlsx"). Es la entrada que se suma al SS cotizado en el reparto por persona.

/ #ruta(
        "auxiliares",
        "nóminas",
        "atrasos_no_vinculados.parquet",
    ): personas cuyas nóminas del año son exclusivamente atrasos (CR 30/87). Una fila por persona con #campo("per_id"), #campo("sectores"), #campo("expedientes"), #campo("n_meses"), #campo("n_líneas") e #campo("importe_total"). Sus líneas se filtran al inicio del preprocesamiento y NO entran a UC retributivas ni a la masa regla 23. Producido por #campo("_filtrar_atrasos_no_vinculados"). En 2025 son #val("≈ 110") personas con un total #val("≈ 8 800 €").

/ #ruta(
        "auxiliares",
        "nóminas",
        "nominas_aplicadas.parquet",
    ): nóminas del año tras los filtros y descuentos del preprocesamiento: se han quitado las líneas de personas con solo atrasos, y a las líneas CR 68 (paga adicional CE PDI) en proyecto general se les ha restado la extra estimada del cargo (para evitar duplicidad con #ruta("cargos_uc.parquet")). Es el insumo que usan #ruta("regla23", "uc_reparto_regla_23.parquet") y el visor *Personal · PDI/PVI* para que las masas reflejen exactamente lo que termina en UC.

== Nóminas: UC retributivas

/ #ruta(
        "auxiliares",
        "nóminas",
        "uc_ptgas.parquet",
    ): UC retributivas ordinarias del sector PTGAS, una por par #campo("elemento_de_coste") + servicio. Producido a partir de #ruta("auxiliares", "nóminas", "PTGAS.parquet") y las reglas de §«Tratamiento del PTGAS».

/ #ruta(
        "auxiliares",
        "nóminas",
        "uc_pvi.parquet",
    ): UC retributivas del sector PVI tras la #nombre-regla[Regla 23] de PVI. Producido a partir de #ruta("auxiliares", "nóminas", "PVI.parquet"), de los diccionarios de dedicación de la #nombre-regla[Regla 23] y de las reglas de §«Tratamiento del PVI y del PDI».

/ #ruta(
        "auxiliares",
        "nóminas",
        "uc_pdi.parquet",
    ): UC retributivas del sector PDI tras la #nombre-regla[Regla 23] de PDI. Mismo origen que el anterior, con la rama PDI.

/ #ruta(
        "auxiliares",
        "nóminas",
        "uc_despidos.parquet",
    ): UC de PDI/PVI por #campo("concepto_retributivo") de despido (categoría especial fuera de regla 23). Origen: §«Despidos».

/ #ruta(
        "auxiliares",
        "nóminas",
        "uc_indemnizaciones_asistencias.parquet",
    ): UC de PDI/PVI por #campo("concepto_retributivo") de indemnización por asistencias a tribunales y similares. Origen: §«Indemnizaciones por asistencias».

/ #ruta(
        "auxiliares",
        "nóminas",
        "uc_cargos.parquet",
    ): UC de PDI/PVI por #campo("concepto_retributivo") = #val("19") o #val("64") con proyecto identificado (cargos en proyectos específicos). Origen: §«Cargos académicos en proyectos».

/ #ruta(
        "auxiliares",
        "nóminas",
        "uc_presupuesto_en_nóminas.parquet",
    ): UC PTGAS «extra» de personas multiexpediente cuyo expediente principal está en otro sector (se reasignan al sector mayoritario en multiexpediente).

== Regla 23 (dedicación e información instrumental)

/ #ruta(
        "auxiliares",
        "nóminas",
        "regla_23_dedicación_docente.parquet",
    ): por expediente, dedicación en créditos a las distintas asignaturas del #ruta("data", "entrada", "docencia", "pod.xlsx"). Origen: §«Construcción del diccionario de registro de actividades reales».

/ #ruta(
        "auxiliares",
        "nóminas",
        "regla_23_pod_resuelto.parquet",
    ): pod con titulación efectiva resuelta (incluyendo desambiguación con #ruta("data", "entrada", "docencia", "pod másteres.xlsx")). Producido a partir de #ruta("data", "entrada", "docencia", "pod.xlsx") y los catálogos de titulaciones (grados, másteres, estudios propios, doctorados, microcredenciales).

/ #ruta(
        "auxiliares",
        "nóminas",
        "regla_23_dedicación_titulaciones.parquet",
    ): por expediente y titulación, créditos impartidos. Producido a partir del pod resuelto.

/ #ruta(
        "auxiliares",
        "nóminas",
        "regla_23_dedicación_estudios.parquet",
    ): por expediente y estudio, créditos impartidos (los estudios agrupan titulaciones equivalentes a través de varias ediciones). Producido a partir de la dedicación por titulaciones y del catálogo de estudios.

/ #ruta(
        "auxiliares",
        "nóminas",
        "regla_23_estructura_estudios.parquet",
    ): catálogo de titulaciones del año con créditos impartidos (activas o no). Origen: §«Información de dedicación a titulaciones».

/ #ruta(
        "auxiliares",
        "nóminas",
        "regla_23_horas_no_oficiales.parquet",
    ): horas dedicadas a estudios propios, microcredenciales, doctorado, etc. (todo lo que no se imputa a titulaciones oficiales). Origen: §«Horas no oficiales».

/ #ruta(
        "auxiliares",
        "nóminas",
        "regla_23_asignaturas_sin_titulación.parquet",
    ): asignaturas con créditos impartidos cuya titulación no está en ningún catálogo. Solo se genera si hay anomalías. Origen: §«Anomalías y depuración».

/ #ruta(
        "auxiliares",
        "nóminas",
        "regla_23_anomalías_resolución.parquet",
    ): filas de pod sin titulación efectiva resoluble. Solo se genera si hay anomalías.

/ #ruta(
        "auxiliares",
        "nóminas",
        "regla_23_múltiples_con_grado.parquet",
    ): asignaturas con varias titulaciones donde alguna no es máster (incumple la regla del catálogo de pod de másteres). Solo se genera si hay anomalías.

/ #ruta(
        "auxiliares",
        "nóminas",
        "regla_23_múltiples_oficiales.parquet",
    ): asignaturas con varias titulaciones oficiales (todas grados o todas másteres) sin información para desambiguar. Solo se genera si hay anomalías.

/ #ruta(
        "regla23",
        "dedicación_pdi.parquet",
    ): tabla maestra de la nueva regla 23: una fila por (#campo("per_id"), #campo("actividad"), #campo("centro_de_coste"), #campo("origen"), #campo("origen_id")) con #campo("horas"), #campo("factor"), #campo("método"), #campo("grupo") (docencia_oficial / docencia_no_oficial / gestión / investigación / extensión), #campo("detalle") y #campo("anomalía"). Producido por los cinco cargadores (POD, tesis, grupos, proyectos, cargos). Origen: §«Regla 23 — invariante #campo("dedicación_pdi")».

/ #ruta(
        "regla23",
        "dedicación_pdi_normalizada.parquet",
    ): tabla anterior tras aplicar las fases 5-7 de la regla 23. Misma granularidad más #campo("horas_iniciales") (= #campo("horas") × #campo("factor")), #campo("horas_finales") (normalizadas a la jornada anual del PDI), #campo("es_asociado") y #campo("sexenio_vivo"). Producido por #campo("coana.fase1.regla23.reparto"). Para los PDI cuyas horas iniciales no llegan a #campo("jornada_anual_pdi") y carecen de fila de investigación, se sintetiza una fila con #etqact("ai") y centro #etqcen("pendiente") (o el centro del grupo de investigación principal) para absorber la HND repercutida. Origen: §«Fase de reparto (fases 5-7 de la regla 23)».

/ #ruta(
        "regla23",
        "uc_reparto_regla_23.parquet",
    ): unidades de coste generadas al repartir la masa regla 23 por persona. Una fila por (#campo("per_id"), #campo("elemento_de_coste"), #campo("actividad"), #campo("centro_de_coste")) con #campo("importe"), #campo("origen") = #val("regla_23"), #campo("origen_id") (con per_id, ec, act y cc) y #campo("origen_porción") (peso del par actividad/centro sobre el total de horas finales de la persona). Origen: §«Reparto de la masa regla 23 → unidades de coste».

== Cargos académicos

/ #ruta(
        "auxiliares",
        "nóminas",
        "cargos_uc.parquet",
    ): una fila por (#campo("per_id"), #campo("cargo")) remunerado en el año analizado, con los días en el periodo de cobro, la cuantía mensual del RD asimilado, el peso del reparto, la parte ordinaria imputada, la parte extra estimada y la extra realmente aplicada, el importe UC total, y la propuesta de elemento de coste, centro de coste y actividad. Producido a partir de #ruta("data", "entrada", "nóminas", "personas cargos.xlsx"), #ruta("data", "entrada", "nóminas", "cargos.xlsx"), #ruta("data", "entrada", "nóminas", "cargos real decreto.xlsx"), #ruta("data", "entrada", "nóminas", "nóminas y seguridad social.xlsx") y #ruta("data", "entrada", "nóminas", "expedientes recursos humanos.xlsx"). Origen: §«Tratamiento de los cargos académicos».

/ #ruta(
        "auxiliares",
        "nóminas",
        "cargos_extras_aplicadas.parquet",
    ): por persona, extra estimada por cargos, masa CR 68 disponible en proyecto general, extra realmente aplicada y diferencia no aplicada (anomalía). Producido durante el preprocesamiento de nóminas, antes del reparto. Origen: §«Tratamiento de los cargos académicos / Parte extra del cargo».

== Seguridad social

/ #ruta(
        "auxiliares",
        "nóminas",
        "persona_uc.parquet",
    ): consolidado por persona de todas las UC retributivas (de nómina y de presupuesto vinculadas a un expediente suyo) con #campo("actividad") y #campo("centro_de_coste"). Es el insumo del reparto de SS. Incluye también las UC del reparto regla 23 (#ruta("fase1", "regla23", "uc_reparto_regla_23.parquet")), de modo que el peso de cada par (actividad, centro_de_coste) en el cálculo del porcentaje refleja el coste retributivo total de la persona, no solo el de los proyectos no generales.

/ #ruta(
        "auxiliares",
        "nóminas",
        "persona_ss.parquet",
    ): UC de seguridad social, una por persona y par (#campo("actividad"), #campo("centro_de_coste")). Producido a partir del anterior, repartiendo proporcionalmente los costes de SS. La inclusión de la masa regla 23 en la base de cálculo asegura que la SS de los PDI/PVI cuyas retribuciones van íntegramente por proyecto general también se reparta entre las actividades y centros donde se devenga (antes quedaba huérfana porque no había otras UC en las que apoyar el reparto). Origen: §«Tratamiento de las personas (mono o multiexpediente) para creación de unidades de coste de seguridad social».

== Resultados consolidados

/ #ruta(
        "unidades de coste.xlsx",
    ): fichero único Excel con todas las UC de la fase 1 (presupuesto, suministros, amortizaciones, nóminas en todas sus variantes, cargos, SS). Es la salida principal que consume la fase 2.

/ #ruta("actividades.tree"), #ruta("centros de coste.tree"), #ruta("elementos de coste.tree"): árboles finales tras aplicar las reglas que añaden nodos dinámicos (cátedras por proyecto, departamentos en categorías PDI/PVI, etc.). Se persisten para que la fase 2 los consuma con la misma estructura que la fase 1.

== Esquemas tipados de los parquets

Cada parquet listado arriba se persiste con un esquema determinado. Esta sección lista los esquemas de los parquets relevantes (las UC consumidas por la fase 2 y los artefactos intermedios más usados por la #app).

=== Esquema común de las UC

Todas las UC (cualquiera que sea su origen) cumplen el esquema mínimo:

#table(
    columns: (auto, auto, 1fr),
    align: (left, left, left),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*Columna*], [*Tipo*], [*Significado*], table.hline()),
    [#campo("id")],
    [String],
    [Identificador único de la UC, con prefijo que indica el origen (#val("P-…"), #val("A-…"), #val("S-…"), #val("T-…"), #val("D-…"), #val("V-…"), #val("CARGO-…"), #val("R23-…"), #val("SS-…")).],
    [#campo("elemento_de_coste")], [String], [Etiqueta del nodo del árbol de elementos de coste.],
    [#campo("centro_de_coste")], [String], [Etiqueta del nodo del árbol de centros de coste.],
    [#campo("actividad")], [String], [Etiqueta del nodo del árbol de actividades.],
    [#campo("importe")], [Float64], [Importe en euros (positivo o negativo).],
    [#campo("origen")],
    [String],
    [Categoría de procedencia (#val("presupuesto"), #val("amortización"), #val("nómina"), #val("regla_23"), etc.).],
    [#campo("origen_id")], [String], [Identificador del registro originario (apunte, expediente, contrato, persona…).],
    [#campo("origen_porción")],
    [Float64],
    [Fracción del registro originario que corresponde a esta UC (1.0 cuando la UC absorbe todo el registro).],
    table.hline(),
)

Cada origen añade columnas propias documentadas a continuación.

=== Esquemas específicos

==== #ruta("uc presupuesto.parquet")

Columnas adicionales a las comunes:

#table(
    columns: (auto, auto, 1fr),
    align: (left, left, left),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*Columna*], [*Tipo*], [*Significado*], table.hline()),
    [#campo("regla_actividad")], [String], [Nombre de la regla que asignó la actividad.],
    [#campo("regla_cc")], [String], [Nombre de la regla que asignó el centro.],
    [#campo("regla_ec")], [String], [Nombre de la regla que asignó el elemento de coste.],
    table.hline(),
)

==== #ruta("auxiliares", "nóminas", "uc_ptgas.parquet"), #ruta("uc_pdi.parquet"), #ruta("uc_pvi.parquet")

Columna adicional #campo("expediente") (Int64) que enlaza con #ruta("data", "entrada", "nóminas", "expedientes recursos humanos.xlsx").

==== #ruta("auxiliares", "nóminas", "uc_despidos.parquet"), #ruta("uc_indemnizaciones_asistencias.parquet"), #ruta("uc_cargos.parquet")

Mismas columnas comunes más:

#table(
    columns: (auto, auto, 1fr),
    align: (left, left, left),
    stroke: 0.5pt + luma(80%),
    inset: 6pt,
    table.header(table.hline(), [*Columna*], [*Tipo*], [*Significado*], table.hline()),
    [#campo("expediente")], [Int64], [Expediente origen.],
    [#campo("per_id")], [Int64], [Persona (clave de #ruta("data", "entrada", "nóminas", "personas.xlsx")).],
    [#campo("proyecto")], [String], [Proyecto presupuestario al que se imputó.],
    [#campo("tipo_proyecto")], [String], [Tipo del proyecto (mismo dominio que en presupuesto).],
    table.hline(),
)

==== #ruta("auxiliares", "nóminas", "cargos_uc.parquet")

Tabla maestra del reparto de cargos académicos. Una fila por (#campo("per_id"), #campo("cargo")) remunerado:

#table(
    columns: (auto, auto, 1fr),
    align: (left, left, left),
    stroke: 0.5pt + luma(80%),
    inset: 4pt,
    table.header(table.hline(), [*Columna*], [*Tipo*], [*Significado*], table.hline()),
    [#campo("id")], [String], [Identificador #val("CARGO-NNNN").],
    [#campo("per_id")], [Int64], [Persona.],
    [#campo("cargo")], [String], [Código del cargo (#ruta("cargos.xlsx")).],
    [#campo("nombre_cargo")], [String], [Nombre legible.],
    [#campo("cargo_asimilado")], [Int64], [Tipo del RD 1086/1989 (1-8).],
    [#campo("importe_rd")], [Float64], [Cuantía mensual del RD asimilado (€/mes).],
    [#campo("fecha_inicio_cobra"), #campo("fecha_fin_cobra")], [Date], [Periodo de cobro del cargo.],
    [#campo("días")], [Int64], [Días del periodo dentro del año analizado.],
    [#campo("peso")], [Float64], [Peso del cargo en el reparto: días × importe_rd.],
    [#campo("importe_uc_ord")], [Float64], [Parte ordinaria imputada al cargo.],
    [#campo("extra_estimada")], [Float64], [Parte extra estimada (2 pagas × importe_rd × días / 365).],
    [#campo("importe_uc_extra")], [Float64], [Parte extra realmente aplicada (acotada por CR 68 disponible).],
    [#campo("importe_uc")], [Float64], [#campo("importe_uc_ord") + #campo("importe_uc_extra").],
    [#campo("extra_no_aplicada")], [Float64], [Diferencia entre la extra estimada y la aplicada (anomalía).],
    [#campo("elemento_de_coste"), #campo("centro_de_coste"), #campo("actividad")],
    [String],
    [Resolución del patrón de #ruta("cargos.xlsx").],
    [#campo("_anomalía_patrón")], [String], [Texto explicativo cuando el patrón no se resuelve.],
    [#campo("categoría_última")], [String], [Categoría RR.HH. usada para el elemento de coste.],
    table.hline(),
)

==== #ruta("auxiliares", "nóminas", "persona_uc.parquet")

Consolidado de UC retributivas por persona:

#table(
    columns: (auto, auto, 1fr),
    align: (left, left, left),
    stroke: 0.5pt + luma(80%),
    inset: 4pt,
    table.header(table.hline(), [*Columna*], [*Tipo*], [*Significado*], table.hline()),
    [esquema común], [—], [Las 8 columnas comunes de UC.],
    [#campo("expediente")], [Int64], [Expediente origen (nullable para las UC sintéticas de regla 23 y SS).],
    [#campo("per_id")], [Int64], [Persona.],
    [#campo("proyecto"), #campo("tipo_proyecto")], [String], [Cuando proceda.],
    [#campo("tipo")], [String], [#val("retributiva") o #val("coste social").],
    table.hline(),
)

==== #ruta("auxiliares", "nóminas", "persona_ss.parquet")

UC de seguridad social: una fila por (#campo("per_id"), #campo("actividad"), #campo("centro_de_coste")):

#table(
    columns: (auto, auto, 1fr),
    align: (left, left, left),
    stroke: 0.5pt + luma(80%),
    inset: 4pt,
    table.header(table.hline(), [*Columna*], [*Tipo*], [*Significado*], table.hline()),
    [#campo("per_id")], [Int64], [Persona.],
    [#campo("actividad")], [String], [Actividad.],
    [#campo("centro_de_coste")], [String], [Centro de coste.],
    [#campo("importe_uc")], [Float64], [Suma de UC retributivas de esa persona en ese par (sirve de denominador).],
    [#campo("ss_total")], [Float64], [SS total cotizada + calculada de la persona (mismo valor en todas sus filas).],
    [#campo("pct")], [Float64], [Porcentaje del par sobre el total de la persona (0-100).],
    [#campo("ss_proporcional")], [Float64], [SS imputada a ese par. La suma sobre la persona = #campo("ss_total").],
    table.hline(),
)

==== #ruta("regla23", "dedicación_pdi.parquet")

Tabla maestra de la regla 23 antes del reparto:

#table(
    columns: (auto, auto, 1fr),
    align: (left, left, left),
    stroke: 0.5pt + luma(80%),
    inset: 4pt,
    table.header(table.hline(), [*Columna*], [*Tipo*], [*Significado*], table.hline()),
    [#campo("per_id")], [Int64], [Persona.],
    [#campo("actividad")], [String], [Actividad (o #val("pendiente")).],
    [#campo("centro_de_coste")], [String], [Centro de coste (o #val("pendiente")).],
    [#campo("horas")], [Float64], [Horas registradas sin factor ×2,5.],
    [#campo("método")],
    [String],
    [#val("md") medición directa · #val("ep") estimación porcentual · #val("et") estimación por tipología · #val("pr") peso relativo.],
    [#campo("factor")], [Float64], [#val("2.5") para impartición de docencia, #val("1.0") para el resto.],
    [#campo("grupo")],
    [String],
    [#val("docencia_oficial") · #val("docencia_no_oficial") · #val("gestión") · #val("investigación") · #val("extensión").],
    [#campo("origen")], [String], [#val("POD") · #val("tesis") · #val("cargo") · #val("proyecto") · #val("grupo").],
    [#campo("origen_id")], [String], [Identificador del registro origen.],
    [#campo("detalle")], [String], [Texto explicativo libre.],
    [#campo("anomalía")], [String], [Texto cuando hay dato pendiente o nulo (nullable).],
    table.hline(),
)

==== #ruta("regla23", "dedicación_pdi_normalizada.parquet")

Igual que el anterior, sin #campo("horas"), #campo("método") ni #campo("factor"), y añadiendo:

#table(
    columns: (auto, auto, 1fr),
    align: (left, left, left),
    stroke: 0.5pt + luma(80%),
    inset: 4pt,
    table.header(table.hline(), [*Columna*], [*Tipo*], [*Significado*], table.hline()),
    [#campo("horas_iniciales")], [Float64], [#campo("horas") × #campo("factor") en la tabla origen.],
    [#campo("horas_finales")],
    [Float64],
    [Horas tras las fases 5-7 de la regla 23. Suman #campo("jornada_anual_pdi") por persona (salvo casos anómalos).],
    [#campo("es_asociado")], [Boolean], [La persona es una figura puramente docente (associat PAA/PAL o substitut PS) en el año. El nombre de la columna se conserva por compatibilidad histórica.],
    [#campo("sexenio_vivo")],
    [Boolean],
    [La persona tiene un sexenio finalizado en los últimos #campo("sexenio_vivo_años") años.],
    table.hline(),
)

==== #ruta("regla23", "uc_reparto_regla_23.parquet")

UC generadas por reparto de la masa regla 23. Esquema común de UC más #campo("per_id") (Int64) para trazabilidad. #campo("origen") = #val("regla_23"); #campo("origen_id") codifica (per_id, ec, act, cc); #campo("origen_porción") es el peso del par sobre el total de horas finales de la persona.
