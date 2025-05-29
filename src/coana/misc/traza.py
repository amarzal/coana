from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import coana.misc.typst as ty


@dataclass
class Traza:
    ruta: Path | None
    text: StringIO = StringIO()

    def __init__(self, ruta: Path | None):
        self.ruta = ruta
        self.text = StringIO()

    def __call__(self, texto: str, end: str = "\n") -> None:
        if self.ruta is not None:
            self.text.write(texto + end)

    def __str__(self) -> str:
        return self.text.getvalue()

    def guarda(self):
        if self.ruta is not None:
            with open(self.ruta, "w") as f:
                f.write(ty.preámbulo())
                f.write(self.text.getvalue())
