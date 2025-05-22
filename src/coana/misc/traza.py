from dataclasses import dataclass
from io import StringIO

import coana.misc.typst as ty
from coana.ficheros import Ficheros
from coana.misc.utils import Singleton


@dataclass
class Traza(metaclass=Singleton):
    text = StringIO()

    def __call__(self, texto: str, end: str = "\n") -> None:
        self.text.write(texto + end)

    def __str__(self) -> str:
        return self.text.getvalue()

    def guarda(self):
        ficheros = Ficheros()
        with open(ficheros.traza, "w") as f:
            f.write(ty.preámbulo())
            f.write(self.text.getvalue())
