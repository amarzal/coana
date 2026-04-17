#import "preámbulo.typ": *
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

== Datos de entrada

La carpeta #ruta("datos", "entrada") contiene los datos que se van a procesar para generar las unidades de coste. Se organizan en siete grupos, cada uno con su propia carpeta:

En las siguientes secciones los describimos y describimos también algunos filtros y preprocesos sobre ellos, de modo que lleguen a la fase de generación de unidades de coste con los datos preparados. En algunos casos, se generan tablas intermedias que pueden ser útiles para depurar el proceso, y que se describen también en estas secciones.

Los filtros se expresan con reglas que son ítems de listas. Si empiezan con un texto entre corchetes, ese texto es el nombre o descripción de la regla, que se puede usar para identificar su aplicación en la #app.

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
            per_id_endosatario: [Identificador de la persona endosataria. Interesa a efectos de identtificar el perceptor de una nómina cuando la tratamos desde el presupuesto, y no desde directamente desde nóminas.],
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
            expediente: [Entero (ejemplo #val("5913")). Ver #ruta("expedientes recursos humanos.xlsx").],
            categoría: [Código de categoría. Ver #ruta("categorías recursos humanos.xlsx").],
            perceptor: [Código de perceptor. Ver #ruta("perceptores.xlsx").],
            provisión: [Código del sistema de provisión. Ver #ruta("provisiones.xlsx").],
            fecha: [Fecha retribución/pago.],
            importe: [Importe.],
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
        descripción: [Fichero con los cargos de recursos humanos],
        campos: (
            cargo: [Identificador (entero).],
            nombre: [],
            tipo_cargo: [Identificador en #ruta("tipos cargo")],
            cuantía: [Es la cuantía mensual que debería percibir quien ostenta el cargo.],
        ),
    ),
    "personas cargos.xlsx": (
        descripción: [Dice qué persona ocupa qué cargo de qué fecha a qué fecha.],
        campos: (
            per_id: [Identificador de la persona. Ver #ruta("personas.xlsx").],
            cargo: [Identificador del cargo. Ver #ruta("cargos.xlsx").],
            servicio: [Departamento, Facultad... Ver #ruta("servicios.xlsx").],
            titulación: [Titulación en la que desempeña el cargo (en el caso de los cargos docentes). Puede estar grado, máster o doctorado. Ver #ruta("grados.xlsx"), #ruta("másteres.xlsx"), #ruta("doctorados.xlsx").],
            fecha_inicio: [Fecha de nombramiento],
            fecha_fin: [Fecha de cese],
            fecha_inicio_cobra: [Fecha de efectos económicos],
            fecha_fin_cobra: [Fecha fin de efectos económicos],
        ),
    ),
    "tipos cargo.xlsx": (
        descripción: [Fichero con los tipos de cargo de recursos humanos],
        campos: (
            tipo_cargo: [Identificador (entero)],
            nombre: [Descripción del tipo de cargo],
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
    "docencia.xlsx": (
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
    "estudios.xlsx": (
        descripción: [Fichero con los estudios de grado y máster],
        campos: (
            estudio: [Identificador (entero, >90000).],
            nombre: [Nombre del estudio],
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
    "microcredenciales.xlsx": (
        descripción: [Fichero con las microcredenciales],
        campos: (
            PER_ID: [Identificador de persona],
            APUNTADOS: [Número de apuntados a la microcredencial],
            CURSO_ID: [Identificador de la microcredencial],
            NOMBRE_CURSO: [Nombre de la microcredencial],
            ANYO: [Año de impartición de la microcredencial],
            URL: [URL de la microcredencial],
            CREDITOS_ECTS: [Créditos ECTS de la microcredencial],
            EDIC: [Número de edición de la microcredencial],
            PROFES: [Nombre del profesor o profesores que imparten la microcredencial],
        ),
    ),
)

#tabula_ficheros_y_campos(ficheros_campos_docencia)

#nota[Los datos de doctorado deberían ir a investigación.]



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
    campo("importe"), [Un importe en euros],
    campo("origen"),
    [Presupuesto, energía, agua, gas, nómina, inventario, o unidad de coste (valores de un `Enum`), dependiendo de si el elemento procede de un apunte presupuestario, de un coste de energía, agua o gas, de un pago por nómina, de un registro de inventario o de otra unidad de coste.],

    campo("origen_id"),
    [Si el elemento viene de un apunte presupuestario, el identificador del apunte presupuestario del que procede, que es el valor de `asiento`. Si el elemento viene de un pago por nómina, el identificador del pago por nómina del que procede. Si el elemento viene de un registro de inventario, el identificador del registro de inventario del que procede. Si el elemento procede de otra unidad de coste, el identificador de la unidad de coste del que procede.],

    campo("origen_porción"),
    [Tanto por uno del importe de la unidad de coste del que procede que se ha asignado a este elemento de coste. Por ejemplo, si una unidad de coste de 100 euros se reparte al 50% entre otras dos unidades de coste, cada una de ellas tendrá un origen con `origen` = unidad, `origen_id` = identificador de la unidad original y `porción` = 0.5.],
    table.hline(),
))


== Proceso secuencial

El programa trabaja secuencialmente en varias fases y cada fase tiene una serie de tareas. El objetivo de cada fase es generar un conjunto de datos que, o bien son parte del producto final, o bien alimentan a otras fases:

- *Fase 1*: generación de unidades de coste a partir de datos extraídos de la base de datos corporativa y de otras fuentes de datos. Tiene tres etapas:
    - *Etapa 1*: filtrado de los registros presupuestarios y generación de unidades de coste a partir de apuntes presupuestarios de gasto mediante reglas y edición de las estructuras de elementos de coste, centros de coste y actividades si es necesario.
    - *Etapa 2*: filtrado de los registros de inventario, cálculo de la amortización anual de los bienes y generación de unidades de coste por amortizaciones mediante reglas a partir de registros de inventario y descripciones de espacios para el cálculo de amortizaciones, y edición de las estructuras de elementos de coste, centros de coste y actividades si es necesario.
    - *Etapa 3*: separación de los registros de nómina por sector del personal  y generación de unidades de coste por nómina mediante reglas a partir de registros de pago por nómina, y edición de las estructuras de elementos de coste, centros de coste y actividades si es necesario.

Los resultados de esta fase alimentarán a la fase 2, que es la generación de informes. El objetivo principal es generar lo que denominamos unidades de coste, que son registros que contienen información sobre el coste de una actividad o centro de coste en el período analizado.

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

Con "Etiquetado de actividades ejemplo" nos referimos al conjunto de reglas. Como hemos dicho, esa regla en realidad se compone de tres reglas. Cada una de ellas sería "Etiquetado de actividades ejemplo (1)", "Etiquetado de actividades ejemplo (2)" y "Etiquetado de actividades ejemplo (3)". Es decir, equivale a

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

En ese caso, la tercera regla se llamaría "Etiquetado de actividades ejemplo. Resto". En cualquier caso, lo importante es que cada regla tenga un nombre que permita identificarla en la #app. De ese modo, cuando se muestre el número de veces que se ha aplicado cada regla, el analista podrá identificarla y, al seleccionar la regla, podrá ver los apuntes a los que se ha aplicado esa regla y las unidades de coste que se han generado a partir de esos apuntes.


=== Reglas que modifican el mismo árbol del que asignan etiquetas

Los árboles de elementos de coste, centros de coste y actividades se pueden modificar a través de reglas, creando nuevos elementos de coste, centros de coste o actividades. Para eso usaremos el operador suma (+) y diremos cómo formar una etiqueta nueva y qué posición ha de ocupar en el árbol.

Una etiqueta con suma, como #etq("act-ejemplo", clave-color: "act") + #campo("proyecto") sobre un apunte cuyo proyecto es #val("28I000") genera una nueva etiqueta #etq("act-ejemplo-28I000", clave-color: "act"). Esa etiqueta se ha de crear en el árbol de actividades, como hijo de #etq("act-ejemplo", clave-color: "act"). En ese caso, no te preocupes de su código, porque el código se asigna automáticamente en función de la posición que ocupe en el árbol. Cuando vayas a poner su códido en el documento de especificación, si #etq("act-ejemplo", clave-color: "act") es el nodo #código("01.02") y #etq("act-ejemplo-28I000", clave-color: "act") es hijo suyo, usa algo como #código("01.02.01.XX"). En el documento basta con saber que es un hijo de #etq("act-ejemplo", clave-color: "act"), pero el código exacto se asigna automáticamente en función de la posición que ocupe en el árbol y ese es un subproducto de esta fase.

He aquí un ejemplo de regla con suma:

#reglas[
    - Si el #campo("programa") es #val("500-X") y el #campo("tipo de proyecto") es #val("99"), entonces la actividad es #etq("act-ejemplo", clave-color: "act") +  #campo("proyecto").
]




= Fase 1: Obtención de unidades de coste

En esta fase se generan unidades de coste a partir de datos extraídos de la base de datos corporativa (y, en ocasiones, de otras fuentes, como la lectura de contadores de OTOP o cálculos de distribución de costes por edificios).

Esta figura ilustra el proceso que parte de los datos de entrada (carpeta #ruta("data", "entrada"), que tiene carpetas dentro, una por cada grupo de datos hasta llegar a los de salida en la fase 1.

#align(center, image("img/fase1.drawio.pdf"))

La #app es la encargada de ejecutar el proceso que va desde los datos de entrada hasta los datos de salida, aplicando las reglas que se definen en esta especificación y mostrando los resultados intermedios y finales.

Queremos generar dos tablas internas:

- `unidades_de_coste_por_presupuesto`: con las unidades de coste generadas a partir de la estructura presupuestaria, con sus campos fijados en función de las reglas definidas
- `apuntes_presupuesto_sin_uc`: con los apuntes presupuestarios con los que no se ha podido generar una unidad de coste, para que el analista pueda revisar esos apuntes y refinar las reglas para asignarles un elemento de coste, centro de coste o actividad.

La #app ha de mostrar las dos tablas mediante opciones de un desplegable «Presupuesto» y permitir descargarlas en formato Excel. Además, se ha de mostrar un resumen de la información que contienen, con el número de filas y el importe total de cada una de ellas.

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

    - #nombre-regla[Formación permanente]
        en los siguientes casos, el #campo("tipo de proyecto") determina la actividad sumando el proyecto:

        #align(center, table(
            columns: 2,
            align: (left, left),

            table.header(table.hline(), campo("tipo de proyecto"), [actividad], table.hline()),
            table.hline(),
            val("07G"), [#etqact("doctorado") + #campo("proyecto")],
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
        y #campo("tipo de proyecto") es #val("OAT"),

        - y #campo("tipo de línea de financiación") es #val("00"), la actividad es #etqact("ait-financiación-propia")
        - en otro caso, la actividad es #etqact("ait-financiación-externa")

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
            val("I4"), etqact("dag-ocit"),
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
        y #campo("proyecto") es #val("8G022") (MANTENIMENT D'EQUIPS D'INVESTIGACIÓ), la actividad es #etq("dag-DEPARTAMENTO") usando la TABLA-TRADUCCIÓN-DEPARTAMENTOS.

]

== Preparación de un módulo para clasificar centros de coste

Del mismo modo que antes hemos usado información que puede estar en registros de nómina o de presupuesto para conocer la actividad, queremos hacer los mismo para obtener el centro de coste.

El árbol de centros de coste modificado por las reglas se ha de mostrar en la #app, con una opción de un desplegable #val("Presupuesto") para mostrarlo o descargarlo en formato `.tree`. Además, se ha de mostrar un resumen de la información que contiene, con el número de filas y el importe total de cada una de ellas. Los nodos añadidos se han de mostrar de un color distinto y se ha de indicar cuantos nodos se han añadido.

#reglas[
    - #nombre-regla[Gastos de servicios centrales en mantenimientos, limpieza y seguridad distribuidos por % OTOP]
        Los gastos del #campo("centro") #val("SC001"),

        - y #campo("aplicación") #val("2251"), #val("2252"), #val("2222"), #val("2223") o #val("2225") generan una unidad de coste para cada centro de coste según la tabla de #val("porcentaje de coste de suministros por centro de coste") y les corresponde el importe que corrresponde según el porcentaje.

    - #nombre-regla[Cátedras y aulas de empresa]
        Si #campo("centro") es #val("INVES"), el #campo("proyecto") determina que el centro de coste sea #etqcen("cátedras-investigación") + #campo("proyecto") en estos casos:

        #align(center, table(
            columns: 2,
            align: (left, left),

            table.header(table.hline(), campo("proyecto"), [Descripción], table.hline()),

            val("1I235"), [CÀTEDRES],
            val("12I327"), [AJUNTAMENT DE VILA-REAL - CÀTEDRA D'INNOVACIÓ CERÀMICA "CIUTAT DE VILA-REAL"],
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
            val("22I070"), [CATEDRA RTVE "CULTURA AUDIOVISUAL Y ALFABETIZACIÓN MEDIÁTICA"],
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
            columns: (.5fr, 1fr, 1fr),
            align: (left, left, left),
            table.header(table.hline(), [*Servicio*], [*Centro de coste*], [*Actividad*], table.hline()),
            [523], [#etqcen("asesoría-jurídica")], [#etqact("dag-asesoría-jurídica")],
            [660], [#etqcen("bibliotecas")], [#etqact("dag-biblioteca")],
            [640], [#etqcen("cent")], [#etqact("dag-cent")],
            [263], [#etqcen("consejo-social")], [#etqact("dag-consejo-social")],
            [2984], [#etqcen("consejo-estudiantes")], [#etqact("dag-consejo-estudiantes")],
            [1862], [#etqcen("cátedras-investigación-1I201")], [#etqact("otras-ait-financiación-propia-1I201")],
            [1662], [#etqcen("cátedras-investigación-1I235")], [#etqact("")],
            [4267], [#etqcen("delegado")], [#etqact("dag-delegado")],
            [101], [#etqcen("daem")], [#etqact("dag-daem")],
            [93], [#etqcen("deco")], [#etqact("dag-deco")],
            [3466], [#etqcen("dea")], [#etqact("dag-dede")],
            [2103], [#etqcen("dmc")], [#etqact("dag-dmc")],
            [81], [#etqcen("deq")], [#etqact("dag-deq")],
            [2102], [#etqcen("desid")], [#etqact("dag-desid")],
            [1442], [#etqcen("dicc")], [#etqact("dag-dicc")],
            [1882], [#etqcen("dea")], [#etqact("dag-dea")],
            [104], [#etqcen("dhga")], [#etqact("dag-dhga")],
            [4207], [#etqcen("dbbcn")], [#etqact("dag-dbbcn")],
            [2502], [#etqcen("dcc")], [#etqact("dag-dcc")],
            [90], [#etqcen("ddpub")], [#etqact("dag-ddpub")],
            [1883], [#etqcen("dfce")], [#etqact("dag-dfce")],
            [2503], [#etqcen("dfs")], [#etqact("dag-dfs")],
            [102], [#etqcen("dfc")], [#etqact("dag-dfc")],
            [2283], [#etqcen("dfis")], [#etqact("dag-dfis")],
            [1443], [#etqcen("dlsi")], [#etqact("dag-dlsi")],
            [92], [#etqcen("dmat")], [#etqact("dag-dmat")],
            [3465], [#etqcen("dpdcsll")], [#etqact("dag-dpdcsll")],
            [97], [#etqcen("dpbcp")], [#etqact("dag-dpbcp")],
            [96], [#etqcen("dpeesm")], [#etqact("dag-dpeesm")],
            [2284], [#etqcen("dqfa")], [#etqact("dag-dqfa")],
            [98], [#etqcen("dqio")], [#etqact("dag-dqio")],
            [99], [#etqcen("dtc")], [#etqact("dag-dtc")],
            [4], [#etqcen("estce")], [#etqact("dag-estce")],
            [3165], [#etqcen("ed")], [#etqact("dag-escuela-doctorado")],
            [2], [#etqcen("fchs")], [#etqact("dag-fchs")],
            [3], [#etqcen("fcje")], [#etqact("dag-fcje")],
            [2922], [#etqcen("fcs")], [#etqact("dag-fcs")],
            [3405], [#etqcen("rectorado")], [#etqact("dag-rectorado")],
            [261], [#etqcen("gerencia")], [#etqact("dag-gerencia")],
            [4907], [#etqcen("inspección-servicios")], [#etqact("dag-inspección-servicios")],
            [3145], [#etqcen("iidl")], [#etqact("dag-iidl")],
            [3285], [#etqcen("inam")], [#etqact("dag-inam")],
            [2603], [#etqcen("init")], [#etqact("dag-init")],
            [2022], [#etqcen("iupa")], [#etqact("dag-iupa")],
            [264], [#etqcen("iutc")], [#etqact("dag-iutc")],
            [1982], [#etqcen("labcom")], [#etqact("dag-labcom")],
            [4168], [#etqcen("ol")], [#etqact("dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico")],
            [364], [#etqcen("otop")], [#etqact("dag-otros-servicios-obras-proyectos")],
            [3408], [#etqcen("oe")], [#etqact("dag-oe")],
            [3406], [#etqcen("oir")], [#etqact("dag-otros-servicios-información-registro")],
            [3425], [#etqcen("oiati")], [#etqact("dag-otros-servicios-ti")],
            [2883], [#etqcen("oipep")], [#etqact("dag-oipep")],
            [1723], [#etqcen("ocds")], [#etqact("cooperación")],
            [242], [#etqcen("ocit")], [#etqact("dag-ocit")],
            [3847], [#etqcen("opp")], [#etqact("dag-opp")],
            [4567], [#etqcen("oppsm")], [#etqact("dag-otros-servicios-prevención-gestión-medioambiental")],
            [2882], [#etqcen("ori")], [#etqact("dag-otros-servicios-relaciones-internacionales")],
            [1722], [#etqcen("opaq")], [#etqact("dag-otros-servicios-promoción-evaluación-calidad")],
            // [368], [#etqcen("")], [#etqact("")],
            [311], [#etqcen("secretaría-general")], [#etqact("dag-secretaría-general")],
            [720], [#etqcen("scic")], [#etqact("dag-scic")],
            [251], [#etqcen("sasc")], [#etqact("cultura")],
            [760], [#etqcen("se")], [#etqact("deportes")],
            [3004], [#etqcen("sea")], [#etqact("dag-sea")],
            [1530], [#etqcen("sic")], [#etqact("dag-sic")],
            [366], [#etqcen("scp")], [#etqact("dag-otros-servicios-comunicación-publicaciones")],
            [1544], [#etqcen("scag")], [#etqact("dag-scag")],
            [1529], [#etqcen("sci")], [#etqact("dag-sci")],
            [1543], [#etqcen("sge")], [#etqact("dag-sge")],
            [361], [#etqcen("sgde")], [#etqact("dag-sgde")],
            [4887], [#etqcen("sgit")], [#etqact("dag-sgit")],
            [350], [#etqcen("slt")], [#etqact("dag-otros-servicios-promoción-lengua-asesoramiento-lingüístico")],
            [362], [#etqcen("srh")], [#etqact("dag-srh")],
            [2942], [#etqcen("upi")], [#etqact("dag-upi")],
            [95], [#etqcen("updtssee")], [#etqact("dag-updtssee")],
            [2943], [#etqcen("upm")], [#etqact("dag-upm")],
            [3427], [#etqcen("uadti")], [#etqact("dag-otros-servicios-ti")],
            [4167], [#etqcen("gencisub")], [#etqact("dag-gencisub")],
            [2822], [#etqcen("ui")], [#etqact("dag-otros-servicios-promoción-fomento-igualdad")],
            [218], [#etqcen("uiic")], [#etqact("dag-otros-servicios-ti")],
            [4487], [#etqcen("uo")], [#etqact("dag-uo")],
            [4687], [#etqcen("udpea")], [#etqact("otras-extensión-universitaria-refinamiento")],
            [4488], [#etqcen("udd")], [#etqact("dag-otros-servicios-atención-diversidad-apoyo-educativo")],
            [4489], [#etqcen("ufie")], [#etqact("dag-ufie")],
            [344], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [3409], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [3445], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [345], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [347], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [346], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [348], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [349], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [2263], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [4647], [#etqcen("sgit")], [#etqact("dag-sgit")],
            // UG
            [2342], [#etqcen("universidad-mayores")], [#etqact("universidad-mayores")],
            [4251], [#etqcen("vevs")], [#etqact("dag-vevs")],
            [4252], [#etqcen("vefp")], [#etqact("dag-vefp")],
            [4248], [#etqcen("vis")], [#etqact("dag-vis")],
            [4250], [#etqcen("vitdc")], [#etqact("dag-vitdc")],
            [4247], [#etqcen("vi")], [#etqact("dag-vi")],
            [2224], [#etqcen("voap")], [#etqact("dag-voap")],
            [4253], [#etqcen("vcls")], [#etqact("dag-vcls")],
            [4255], [#etqcen("vpee")], [#etqact("dag-vpee")],
            [4254], [#etqcen("vri")], [#etqact("dag-vri")],
            [4249], [#etqcen("vrspii")], [#etqact("dag-vrspii")],
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
        Si el #campo("centro") es #val("INVES"), el centro de coste es el mismo que el centro de origen.
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
            val("%/I4"), etqcen("ocit"),
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

            table.hline(),
        ))

]

== Generación de UC a partir de presupuesto

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

#align(center, image("img/filtro presupuesto.drawio.pdf"))

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
        La #campo("aplicación") presupuestaria determina el elemento de coste, según la siguiente tabla. (Hay alguna duda con #etqele("arrendamientos-instalaciones") y #etqele("arrendamientos-utillaje"), porque los dos están en #val("2213"). Ídem con conservación.)

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

En la #app debes informar de cuántas unidades de coste se han generado a partir de cada línea de cada uno de los ficheros (energía, agua, gas), y qué importe supone cada una de esas unidades de coste. Además, al final del proceso, se ha de informar del importe total que se ha asignado a cada centro de coste por cada uno de los suministros (energía, agua y gas), comprobando que la suma de las unidades generadas coincide con el importe total de cada uno de los suministros.



== Generación de unidades de coste a partir de información de amortizaciones

La #app tendrá un desplegable «Amortizaciones» con nuevas entradas para mostrar los diferentes elementos de esta etapa.

=== Cálculo del importe de amortización en el año analizado y reglas de filtrado

Lo primero que se hace es descartar línea del inventario aplicando estas reglas:

#reglas[
    - #nombre-regla[Supresión de elementos de baja]
        Se descartan las líneas con #campo("estado") = #val("B") (baja).

    - #nombre-regla[Supresión por cuentas contables]
        Solo han de pasar el filtro las cuentas contables que empiecen por uno de estos prefijos de 3 dígitos: #val("202"), #val("203"), #val("204"), #val("205"), #val("206"), #val("211"), #val("214"), #val("215"), #val("216"), #val("217"), #val("218").

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



== Generación de unidades de coste a partir de información de nóminas

=== Preprocesamiento nóminas

En primer lugar, vamos a agrupar todas la entradas de #ruta("nóminas y seguridad social.xlsx") por #campo("expediente"). Los expedientes se van a clasificar en una lista (o tabla) de PDI y PVI (el PVI está codificado como sector PI) y otra de PTGAS.

En la #app, quiero poder ver, por separado, los expedientes de cada uno de estos sectores. Si aparece algún expediente que no se pueda clasificar en ninguno de estos sectores, quiero poder verlo también para analizarlo.

También quiero ver en la #app la relación de personas que tiene más de un expediente de tipos distintos. En una entrada de menú aparecerá Multiexpediente y dentro, en un tabs, tendré: PTGAS + PDI, PTGAS + PVI, PDI + PVI, PTGAS + PDI + PDI. En cada uno de esos tabs, veré la relación de personas que tienen expedientes de ambos tipos, con el número de expedientes de cada tipo que tienen, para analizar si es correcto o no que tengan expedientes de ambos tipos. Al seleccionar la persona, veré los doce meses del año y en qué meses tenía activos que expedientes (número y colectivo al que pertenece).

Cada una de esas listas (PDI+PVI y PTGAS) se va a procesar de un modo distinto, lo que vamos a describir en los siguientes apartados.

==== Agrupamiento de los registros

Cada expediente del PTGAS, PDI o PVI tendrá varias tablas en las que se almacenan los registros de la nómina correspondientes:

- una, #campo("costes sociales"), con los registros asociados a la Seguridad Social (#campo("aplicación") que empieza por 12),
- otra, #campo("retribuciones ordinarias") con lo que es nómina ordinaria (cuando #campo("proyecto") es #val("1G019"), #val("23G019"), #val("02G041"), #val("11G006"), #val("1G046") o #val("00000"))
- otra, #campo("retribuciones extra"), con el resto.
- otra, #campo("unidades de coste"), con una lista de unidades de coste asociadas a este expediente. Estas unidades se pueden crear desde el presupuesto o desde las nóminas.

En la #app, al seleccionar un expediente, veré sus tres tablas y al pinchar en una fila de un de esas tablas veré el detalle de esa fila, con toda la información que tiene, para facilitar comprobaciones.

La #app mostrará los totales de cada tabla y comprobará que los totales de las tres primeras suman el total de la nómina del expediente, para detectar posibles errores en la clasificación de los registros en las tablas.

En la #app se han de mostrar también las unidad de coste que ya se han creado para cada expediente, para facilitar comprobaciones y evitar duplicidades.


=== Tabla para determinar parte del elemento de coste a partir del concepto retributivo

Esta tabla se va a usar para determinar parte del elemento de coste de las unidades de coste que se van a generar a partir de los registros de nómina. Es común a todos (PDI+PVI y PTGAS) La tabla es la siguiente:

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
    [#val("48")], [Indemnització per assistències], [#val("otvars")],
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

==== Creación de unidades de coste a partir de registros de nómina

#reglas[
    - En primer lugar hemos de agrupar los registros que hemos puesto en #campo("retribuciones ordinarias") por el par `elemento de coste`-`servicio` (una `persona`, desde un expediente, puede haber trabajado en más de un servicio a lo largo del año).

    El primer problema es determinar el elemento de coste, que no es trivial. Su etiqueta tiene la forma `ZZZ-XXX-YYY`, donde
    - `ZZZ` depende del sector:
        - el sector PTGAS se corresponde con `ptgas`
        - el sector PDI se corresponde con `pdi`
        - el sector PVI se corresponde con `piyotper`
    - `XXX` depende de la categoría u otros campos,
    - e `YYY` depende del tipo de retribución.

    Para determinar `XXX` y `YYY` hay que aplicar una serie de reglas.

    - Para determinar el valor de `XXX`
        - En el caso del PTGAS, miramos el campo #campo("categoría") del registro. Estas son las reglas:
            - Si el valor es #val("FC") y el #campo("per_id") es #val("65214") (AMV), `XXX` es #val("dir").
            - Si no, si el valor es #val("FC") o #val("FI"), `XXX` es #val("func").
            - Si no, si el valor es #val("E"), `XXX` es #val("ev").
            - Si no, si el valor es #val("LE"), #val("LF") o #val("LT"), `XXX` es #val("lab").
            - Si no, marca un error, porque no debería de pasar.
        - En el caso del PVI o PDI, los campos relevantes son #campo("categoría"), #campo("perceptor") y #campo("provisión"). Estas son las reglas (las celdas en blanco significan que no importa el valor de ese campo):
            #table(
                columns: 4,
                table.header(
                    table.hline(),
                    [#campo("categoría")], [#campo("perceptor")], [#campo("provisión")], [Valor de `XXX`],
                    table.hline(),
                ),
                table.cell(colspan: 4, align: center)[_PVI — se aplica la primera regla que encaja_],
                [], val("35"), [], val("act"),
                val("PREDO"), [], [], val("pif"),
                [], [], val("PD"), val("pif"),
                [], [], val("P2"), val("idi"),
                [], [], [], val("pid"),
                table.hline(),
                table.cell(colspan: 4, align: center)[_PDI — por #campo("categoría") exacta_],
                val("CU"), [], [], val("cu"),
                val("TU"), [], [], val("tu"),
                val("TUI"), [], [], val("tu"),
                val("CEU"), [], [], val("ceu"),
                val("TEU"), [], [], val("teu"),
                val("AJ"), [], [], val("aj"),
                val("AJD"), [], [], val("aj"),
                val("AJDII"), [], [], val("aj"),
                val("PAA"), [], [], val("as"),
                val("PAL"), [], [], val("as"),
                val("PS"), [], [], val("ps"),
                val("PEME"), [], [], val("em"),
                val("PPL"), [], [], val("pl"),
                val("PPLV"), [], [], val("pl"),
                val("PVI"), [], [], val("pv"),
                val("PD"), [], [], val("pd"),
                val("PCD"), [], [], val("pcd"),
                val("PC"), [], [], val("pc"),
                table.hline(),
            )

    - Para determinar el valor de `YYY` hay que mirar el campo #campo("concepto_retributivo") del registro y usar la tabla que hemos definido antes para determinar la etiqueta del elemento de coste a partir del concepto retributivo. Por ejemplo, si el concepto retributivo es #val("01"), el valor de `YYY` es #val("sueldo"). Si el concepto retributivo es #val("03"), el valor de `YYY` es #val("trienios"). Y así sucesivamente.

    Cuando generas un etiqueta `ZZZ-XXX-YYY` has de comprobar que existe en el árbol de elementos de coste. Si no existe, has de dar un error.

    Para cada par elemento-servicio, tomamos sus registros de #campo("retribuciones ordinarias") y hacemos la suma, porque de cada par elemento-servicio, para ese expediente, vamos a crear una unidad de coste.

    A continuación, tenemos que mapear cada servicio al centro de coste que le corresponde, y asignar el elemento de coste #etqele("retribuciones-ordinarias") y la actividad #etqact("dag-general-universidad").

    El importe de la unidad de coste que hemos creado con ese elemento de coste, centro de coste y actividad es el importe total de las retribuciones ordinarias del servicio.


    - La actividad se ha de determinar usando el módulo de clasificación de actividades (que ya has usado para el presupuesto).
]


=== Tratamiento del PVI y del PDI

El agrupamiento de registros es común al de PTGAS (véase la sección «Preprocesamiento nóminas / Agrupamiento de los registros»). La generación de unidades de coste para PVI y PDI sigue la misma estructura `ZZZ-XXX-YYY` descrita en el apartado anterior, con `ZZZ` = #val("piyotper") para PVI y #val("pdi") para PDI, y las reglas específicas de `XXX` de la tabla anterior.

#nota[Los detalles de la generación de unidades de coste para PVI y PDI (agrupación, cálculo de actividad y centro de coste) se definirán en un siguiente paso.]

==== Dedicación docente en créditos a las distintas titulaciones en las que tiene docencia

A partir del  #campo("per_id") del expediente hemos de ir a la tabla de #ruta("docencia") y averiguar las asignaturas (columna  #campo("asignatura")) en las que tiene docencia y cuántos créditos imparte (columna `créditos_impartidos`).

Si la asignatura está en la tabla #ruta("asignaturas grados") (columna  #campo("asignatura")) podemos averiguar su nombre ( #campo("nombre")) y el grado al que pertenece ( #campo("grado")). Para saber la titulación hay que ir a la tabla #ruta("grados") y ver si ese código tiene un valor en la columna  #campo("grado"). Si es así, la columna  #campo("estudio") nos da otro número. ¡Es número conduce, por fin, a la titulación con la columna  #campo("estudio") de la tabla  #campo("estudio"): es el que dice su columna  #campo("nombre").

Para los másteres hay que hacer lo mismo, pero con la tabla #ruta("asignaturas másteres") en lugar de #ruta("asignaturas grados").

La #app ha de mostrar la tabla con las asignaturas en las que tiene docencia, los créditos que imparte de cada una, el grado al que pertenece cada asignatura y la titulación a la que pertenece cada grado, sumarizando la información por titulación y el total. Todo eso en un desplegable «Docencia».

Para tener controlados los casos raros, quiero que haya una opción en «Personal» llamada «Anomalías PDI». Se recogerán las siguientes anomalías:

- Asignaturas sin titulación conocida (código y nombre), con todos los  #campo("per_id") (y nombre) de profesorado que tengan asignaturas sin titulación conocida, con el número de créditos que imparte en cada una de esas asignaturas. Se ha de mostrar, también el total de créditos anómales sobre el total de créditos impartidos por el profesorado, para tener una idea de la magnitud del problema.

#nota[Con el doctorado no sabemos qué hacer aún. ¿Asignaturas? Por otra parte, se considera actividad de investigación.]


== Tratamiento de las personas (mono o multiexpediente) para creación de unidades de coste de seguridad social

Ahora interesa considerar a cada persona tomando en cuenta todos los expedientes asociados.

Creamos una lista con todas las unidades de coste que se han asociado a algún expediente de la persona (vengan de nómina o de presupuesto). Clasificamos cada unidad de coste por su actividad y centro de coste. Con eso sabemos que retribuciones ha tenido cada par (actividad, centro de coste) y qué porcentaje suponen estas sobre el total percibido en el año. Para cada par (actividad, centro de coste) se crea una unidad de coste con su porcentaje de seguridad social. De este modo, se reparte la seguridad social entre las actividades y centros de coste a los que ha estado asociado cada persona a lo largo del año, teniendo en cuenta todos sus expedientes.

Para estas unidades de coste pondremos como `id` de la unidad de coste un un código seriado de la forma `SS-XXX`, donde `XXX` es un número que se va incrementando a medida que se van creando unidades de coste de seguridad social. El elemento de coste depende del sector del expediente principal de la persona:
- `ss-ptgas` para el sector PTGAS
- `ss-pdi-func` para el sector PDI #nota[REVISAR porque se distingue funcionario y laboral, y aún hemos de ver el tema de la previsión.]
- `ss-pvi-otpersonal` para el sector PVI

Quiero que la #app permita, en un menú llamado Persona, elegir una persona (de las que han tenido al menos un expediente vivo en el año) y me muestre toda esta información. En esa ficha, al mostrar el detalle de unidades de coste se verán todas las asociadas a esa persona: las retributivas y las de costes sociales. De cada una de esas unidades quiero ver toda la información asociada y al pinchar en una, su detalle máximo. Ojo, las unidades de coste que se visualizan son:
- todas las que ya había por nóminas o presupuesto vinculadas a algún expediente de la persona (y pueden ser muchas)
- las que se acaban de crear para la seguridad social (que serán pocas, porque hay una por cada par actividad-centro de coste, y no por cada unidad de coste retributiva).


= Resultados

En la #app, ha de haber un desplegable «Resultados» con las siguientes entradas:

- Actividades: se muestra una tabla con las actividades (incluyendo las que se han creado) y los costes asignados. La tabla tiene código, actividad, etiqueta, importe asignado por unidades de coste a partir de presupuesto (y número de uc), ídem por amortizaciones e ídem por nóminas.
- Centros de coste: lo mismo, pero con centros de coste.
- Elementos de coste: lo mismo, pero con elementos de coste.

Al pinchar en una actividad, se verán las unidades de coste de cada tipo que se le han asignado en una tabla adicional.
