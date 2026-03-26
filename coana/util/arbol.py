"""Árboles de conceptos para la contabilidad analítica.

Lectura, escritura y consulta de ficheros .tree con jerarquías
indentadas de la forma::

    Descripción del nodo | identificador
        Descripción del hijo | id-hijo

Cada nodo recibe un código automático (01, 01.02, 01.02.03…) según
su posición en el árbol.  La raíz es un nodo virtual con
identificador ``UJI``.
"""

from pathlib import Path
from typing import Any, Self

from pydantic import GetCoreSchemaHandler, GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema, core_schema


class NodoÁrbol:
    """Nodo de un árbol de contabilidad analítica."""

    __slots__ = ("código", "descripción", "identificador", "padre", "hijos", "insertado")

    def __init__(
        self,
        código: str,
        descripción: str,
        identificador: str,
        padre: NodoÁrbol | None = None,
        *,
        insertado: bool = False,
    ) -> None:
        self.código = código
        self.descripción = descripción
        self.identificador = identificador
        self.padre = padre
        self.hijos: list[NodoÁrbol] = []
        self.insertado = insertado

    def __repr__(self) -> str:
        return f"NodoÁrbol({self.código!r}, {self.descripción!r}, {self.identificador!r})"


class Árbol:
    """Árbol de conceptos de contabilidad analítica.

    Puede usarse como tipo de campo en modelos Pydantic.
    """

    def __init__(self, raíz: NodoÁrbol) -> None:
        self.raíz = raíz
        self._por_código: dict[str, NodoÁrbol] = {}
        self._por_id: dict[str, NodoÁrbol] = {}
        self._indexar(raíz)

    # --- Indexación ---

    def _indexar(self, nodo: NodoÁrbol) -> None:
        if nodo.código:
            self._por_código[nodo.código] = nodo
        if nodo.identificador:
            self._por_id[nodo.identificador] = nodo
        for hijo in nodo.hijos:
            self._indexar(hijo)

    def _nodo(self, ref: str) -> NodoÁrbol:
        """Resuelve una referencia (identificador o código) a un nodo."""
        if ref in self._por_id:
            return self._por_id[ref]
        if ref in self._por_código:
            return self._por_código[ref]
        raise KeyError(f"Nodo no encontrado: {ref!r}")

    # --- Consultas ---

    def hijos(self, ref: str) -> list[NodoÁrbol]:
        """Hijos de un nodo dado su identificador o código."""
        return list(self._nodo(ref).hijos)

    def padre(self, ref: str) -> NodoÁrbol | None:
        """Padre de un nodo dado su identificador o código."""
        return self._nodo(ref).padre

    def código(self, ref: str) -> str:
        """Código de un nodo dado su identificador o código."""
        return self._nodo(ref).código

    def descripción(self, ref: str) -> str:
        """Descripción de un nodo dado su identificador o código."""
        return self._nodo(ref).descripción

    def identificador(self, ref: str) -> str:
        """Identificador de un nodo dado su código o identificador."""
        return self._nodo(ref).identificador

    def buscar(self, subcadena: str) -> list[str]:
        """Identificadores de nodos que contienen *subcadena* en su
        código, descripción o identificador."""
        sub = subcadena.lower()
        resultados: list[str] = []
        for nodo in self._por_id.values():
            if nodo is self.raíz:
                continue
            if (
                sub in nodo.código.lower()
                or sub in nodo.descripción.lower()
                or sub in nodo.identificador.lower()
            ):
                resultados.append(nodo.identificador)
        return resultados

    # --- Mutación ---

    def añadir_hijo(
        self, padre_id: str, descripción: str, sufijo_id: str
    ) -> NodoÁrbol:
        """Añade un hijo al nodo con identificador *padre_id*.

        El identificador del nuevo nodo es ``padre_id + "-" + sufijo_id``.
        Si ya existe un nodo con ese identificador y la misma descripción,
        no hace nada y devuelve el nodo existente.  Si la descripción
        difiere, lanza ``ValueError`` (colisión de identificadores).

        Los nodos insertados se colocan después de los hijos originales,
        en orden alfabético por descripción entre sí.
        """
        padre = self._nodo(padre_id)
        nuevo_id = f"{padre_id}-{sufijo_id}"

        if nuevo_id in self._por_id:
            existente = self._por_id[nuevo_id]
            if existente.descripción == descripción:
                return existente
            raise ValueError(
                f"Colisión de identificadores: {nuevo_id!r} ya existe con "
                f"descripción {existente.descripción!r}, pero se intentó "
                f"añadir con descripción {descripción!r}"
            )

        nodo = NodoÁrbol(
            código="",  # se asigna abajo con _reordenar_insertados
            descripción=descripción,
            identificador=nuevo_id,
            padre=padre,
            insertado=True,
        )
        padre.hijos.append(nodo)
        self._por_id[nuevo_id] = nodo
        self._reordenar_insertados(padre)
        return nodo

    def _reordenar_insertados(self, padre: NodoÁrbol) -> None:
        """Reordena los hijos insertados alfabéticamente y renumera códigos."""
        # Separar originales (mantienen orden) de insertados (ordenar)
        originales = [h for h in padre.hijos if not h.insertado]
        insertados = [h for h in padre.hijos if h.insertado]
        insertados.sort(key=lambda n: n.descripción.lower())
        padre.hijos[:] = originales + insertados

        # Renumerar todos los hijos insertados (y sus subárboles)
        for i, hijo in enumerate(padre.hijos, 1):
            if not hijo.insertado:
                continue
            if padre.código:
                nuevo_código = f"{padre.código}.{i:02d}"
            else:
                nuevo_código = f"{i:02d}"
            if hijo.código != nuevo_código:
                self._renumerar(hijo, nuevo_código)

    def _renumerar(self, nodo: NodoÁrbol, nuevo_código: str) -> None:
        """Actualiza recursivamente el código de un nodo y sus descendientes."""
        if nodo.código and nodo.código in self._por_código:
            del self._por_código[nodo.código]
        nodo.código = nuevo_código
        self._por_código[nuevo_código] = nodo
        for i, hijo in enumerate(nodo.hijos, 1):
            self._renumerar(hijo, f"{nuevo_código}.{i:02d}")

    # --- Lectura / Escritura ---

    @classmethod
    def from_str(cls, texto: str) -> Self:
        """Construye un árbol a partir de texto en formato ``.tree``."""
        raíz = NodoÁrbol(código="", descripción="", identificador="UJI")
        líneas: list[tuple[int, str]] = []
        for línea in texto.splitlines():
            stripped = línea.strip()
            if not stripped or stripped.startswith("#"):
                continue
            indent = len(línea) - len(línea.lstrip())
            líneas.append((indent, stripped))

        if not líneas:
            return cls(raíz)

        # Detectar unidad de indentación
        indents = sorted({ind for ind, _ in líneas if ind > 0})
        indent_unit = indents[0] if indents else 4

        pila: list[tuple[int, NodoÁrbol]] = [(-1, raíz)]

        for indent, contenido in líneas:
            nivel = indent // indent_unit if indent_unit else 0

            partes = contenido.rsplit("|", 1)
            desc = partes[0].strip()
            ident = partes[1].strip() if len(partes) > 1 else ""

            # Encontrar padre: retroceder en la pila hasta un nivel menor
            while pila[-1][0] >= nivel:
                pila.pop()
            padre_nodo = pila[-1][1]

            # Generar código
            num_hijo = len(padre_nodo.hijos) + 1
            if padre_nodo.código:
                código = f"{padre_nodo.código}.{num_hijo:02d}"
            else:
                código = f"{num_hijo:02d}"

            nodo = NodoÁrbol(
                código=código,
                descripción=desc,
                identificador=ident,
                padre=padre_nodo,
            )
            padre_nodo.hijos.append(nodo)
            pila.append((nivel, nodo))

        return cls(raíz)

    @classmethod
    def from_file(cls, path: str | Path) -> Self:
        """Carga un árbol desde un fichero ``.tree``."""
        return cls.from_str(Path(path).read_text(encoding="utf-8"))

    def to_str(self) -> str:
        """Serializa el árbol a formato ``.tree``."""
        líneas: list[str] = []

        def _rec(nodo: NodoÁrbol, nivel: int) -> None:
            for hijo in nodo.hijos:
                líneas.append(
                    f"{'    ' * nivel}{hijo.descripción} | {hijo.identificador}"
                )
                _rec(hijo, nivel + 1)

        _rec(self.raíz, 0)
        return "\n".join(líneas) + "\n"

    def to_file(self, path: str | Path) -> None:
        """Escribe el árbol en un fichero ``.tree``."""
        Path(path).write_text(self.to_str(), encoding="utf-8")

    # --- Pydantic 2 ---

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: Any,
        _handler: GetCoreSchemaHandler,
    ) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            cls._pydantic_validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                cls._pydantic_serialize,
                info_arg=False,
            ),
        )

    @classmethod
    def _pydantic_validate(cls, valor: Any) -> Árbol:
        if isinstance(valor, cls):
            return valor
        if isinstance(valor, str):
            return cls.from_str(valor)
        raise ValueError(f"No se puede convertir {type(valor).__name__} a Árbol")

    @staticmethod
    def _pydantic_serialize(árbol: Árbol) -> str:
        return árbol.to_str()

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        _schema: CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        return {
            "type": "string",
            "description": "Árbol en formato .tree (líneas indentadas)",
        }

    def __repr__(self) -> str:
        n = len(self._por_id) - 1  # sin contar la raíz
        return f"Árbol({n} nodos)"
