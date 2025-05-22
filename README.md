# CoAna: Contabilidad Analítica

El proyecto se gestiona con uv (https://docs.astral.sh/uv/).

Los datos han de estar en un directorio con la estructura que determina un fichero en su raíz llamado manifesto.yaml.

Si ese directorio está, por ejemplo, en ../datos_coana/2024, el programa se ejecuta con

uv run coana uji ../datos_coana/2024

En el estado actual se limita a etoquetar apuntes y nóminas y generar un fichero de traza en Typst (https://typst.app/). El fichero, traza.typ, se genera en el directorio de datos, en una carpeta llamada traza.

Si se quiere generar el fichero traza.pdf, basta con llamar a `typst compile ../datos/2024/traza/traza.typ` una vez instalado Typst en local. Se necesita tener instalada la fuente Fira Sans (https://fonts.google.com/specimen/Fira+Sans).
