from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path

import yaml

from coana.misc.utils import Singleton


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
        s = StringIO()
        s.write("= Directorios y ficheros con datos\n")
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
