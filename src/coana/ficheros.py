import json
import pprint
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from re import A
from typing import cast

import yaml

from coana.misc.utils import Singleton

type DiccionarioDeDiccionarios = dict[str, None | DiccionarioDeDiccionarios]

@dataclass
class Ficheros(metaclass=Singleton):
    raíz_datos: Path = field(default_factory=lambda: Path(""))
    ficheros: dict[str, Path] = field(default_factory=dict)
    directorios: dict[str, Path] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.raíz_datos == Path(""):
            raise ValueError("No se ha definido la raíz de los datos")

        config = yaml.load(open(self.raíz_datos / "manifesto.yaml"), Loader=yaml.FullLoader)

        for key, value in config.items():
            path = Path.joinpath(*[self.directorios.get(word, Path(word)) for word in value.split("/")])
            if path.suffix == "":  # Es un directorio
                self.directorios[key] = path
            else:  # Es un fichero, pero nos lo pueden haber expresado con un glob
                self.ficheros[key] = path

        for directorio in self.directorios.values():
            con_raíz = self.raíz_datos / directorio
            if not con_raíz.exists():
                con_raíz.mkdir(parents=True)
        for clave, fichero in list(self.ficheros.items()):
            con_raíz = self.raíz_datos / fichero
            ficheros = sorted(Path(".").glob(str(con_raíz)))
            if ficheros:
                self.ficheros[clave] = ficheros[-1]
            else:
                self.ficheros[clave] = con_raíz

    def para_traza(self) -> str:
        def añade_directorio(piezas: tuple[str, ...], dónde: DiccionarioDeDiccionarios):
            if not piezas:
                return
            if piezas[0] not in dónde:
                dónde[piezas[0]] = {}
            añade_directorio(piezas[1:], cast(DiccionarioDeDiccionarios, dónde[piezas[0]]))

        def añade_fichero(piezas: tuple[str, ...], dónde: DiccionarioDeDiccionarios):
            if not piezas:
                return
            if len(piezas) == 1:
                dónde[piezas[0]] = None
            añade_fichero(piezas[1:], cast(DiccionarioDeDiccionarios, dónde[piezas[0]]))

        como_árbol: DiccionarioDeDiccionarios = {}
        for directorio in self.directorios.values():
            añade_directorio((self.raíz_datos / directorio).parts, como_árbol)
        for fichero in self.ficheros.values():
            añade_fichero(fichero.parts, como_árbol)


        s = StringIO()
        s.write("= Directorios y ficheros con datos\n")
        s.write(self._tree_to_directory_string(como_árbol))
        s.write("- Directorios\n")
        for clave in sorted(self.directorios):
            s.write(f"  - *{clave}*: `{self.__getattr__(clave)}`\n")
        s.write("- Ficheros\n")
        for clave in sorted(self.ficheros):
            s.write(f"  - *{clave}*: `{self.__getattr__(clave)}`\n")

        return s.getvalue()

    def __getattr__(self, name: str) -> Path:
        if name in self.ficheros:
            return self.ficheros[name]
        if name in self.directorios:
            return self.raíz_datos / self.directorios[name]
        raise AttributeError(f'El fichero manifesto.yaml en {self.raíz_datos} no ha definido "{name}"')


    def _tree_to_directory_string(self, tree):
        def go(tree, prefix="", is_last=True):
            if not tree:
                return ""

            result = []
            items = list(tree.items())

            for i, (name, subtree) in enumerate(items):
                is_last_item = i == len(items) - 1

                # Choose the appropriate tree character (skip for root level)
                if prefix == "":  # Root level
                    current_prefix = ""
                    # For root level, start with proper indentation for children
                    if is_last_item:
                        next_prefix = "    "
                    else:
                        next_prefix = "│   "
                elif is_last_item:
                    current_prefix = prefix + "└── "
                    next_prefix = prefix + "    "
                else:
                    current_prefix = prefix + "├── "
                    next_prefix = prefix + "│   "

                # Add current item
                result.append(current_prefix + name)

                # If it's a directory (not None), recursively add its contents
                if subtree is not None:
                    subtree_str = go(subtree, next_prefix, is_last_item)
                    if subtree_str:
                        result.append(subtree_str)

            result = [result[0]] + [line[4:] for line in result[1:]]

            return "\n".join(result)

        t = go(tree)
        s = StringIO()
        s.write("#grid(columns: 1, inset: (y: 0.4em),\n")
        for x in t.split("\n"):
            s.write(f"`{x}`,\n")
        s.write(")\n")
        return s.getvalue()
