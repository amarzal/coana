import math
from dataclasses import dataclass
from functools import total_ordering
from typing import Self


def _round_half_up(n, decimals=0):
    multiplier = 10**decimals
    return math.floor(n * multiplier + 0.5) / multiplier


@total_ordering
@dataclass(slots=True)
class E:
    céntimos: int

    def __init__(self, quantity: float | int | str | Self, céntimos=False):
        match quantity:
            case E():
                self.céntimos = quantity.céntimos
            case int():
                self.céntimos = int(quantity) if céntimos else int(quantity * 100)
            case str():
                quantity = quantity.split()[0].replace(".", "").replace(",", ".")
                self.céntimos = int(round(float(quantity) * 100))
            case float():
                self.céntimos = int(quantity) if céntimos else int(_round_half_up(quantity * 100))
            case _:
                try:
                    q = float(quantity)
                except ValueError:
                    try:
                        q = float(quantity.replace(".", "_").replace(",", "."))
                    except ValueError:
                        raise TypeError(f"No se puede convertir {quantity} de tipo {type(quantity)} a E")
                self.céntimos = int(q) if céntimos else int(round(q * 100))

    def __add__(self, other: Self | int | float) -> "E":
        match other:
            case int():
                return E(self.céntimos + 100 * other, céntimos=True)
            case float():
                return E(self.céntimos + int(round(100 * other)), céntimos=True)
            case _:
                return E(self.céntimos + other.céntimos, céntimos=True)

    def __radd__(self, other: Self | int | float) -> "E":
        return self + other

    def __sub__(self, other: Self | int | float) -> "E":
        match other:
            case int():
                return E(self.céntimos - 100 * other, céntimos=True)
            case float():
                return E(self.céntimos - int(round(100 * other)), céntimos=True)
            case _:
                return E(self.céntimos - other.céntimos, céntimos=True)

    def __rsub__(self, other: Self | int | float) -> "E":
        return -self + other

    def __rmul__(self, other: int | float) -> "E":
        return self * other

    def __mul__(self, other: int | float) -> "E":
        return E(int(self.céntimos * other), céntimos=True)

    def __truediv__(self, other: int | float | Self) -> float:
        match other:
            case E():
                return self.céntimos / other.céntimos
        return self.céntimos / other / 100

    def __floordiv__(self, other: int | float) -> "E":
        return E(self.céntimos / other, céntimos=True)

    def __neg__(self) -> "E":
        return E(-self.céntimos, céntimos=True)

    def __abs__(self) -> "E":
        return E(abs(self.céntimos), céntimos=True)

    def reparto(self, n: int) -> list["E"]:
        common = self.céntimos // n if self.céntimos >= 0 else -((-self.céntimos) // n)
        return [E(common, céntimos=True)] * (n - 1) + [E(self.céntimos - (n - 1) * common, céntimos=True)]

    def __float__(self) -> float:
        return self.céntimos / 100

    def is_zero(self) -> bool:
        return self.céntimos == 0

    def __eq__(self, other: object) -> bool:
        match other:
            case int():
                return self.céntimos == 100 * other
            case float():
                return self.céntimos == int(round(100 * other))
            case E():
                return self.céntimos == other.céntimos
            case _:
                return False

    def __gt__(self, other: Self | int | float) -> bool:
        match other:
            case int():
                return self.céntimos > 100 * other
            case float():
                return self.céntimos > int(round(100 * other))
            case _:
                return self.céntimos > other.céntimos

    @property
    def eu(self) -> str:
        return str(self) + " euros"

    @property
    def e(self) -> str:
        return str(self) + " €"

    def __repr__(self) -> str:
        return f"E({self.céntimos / 100})"

    def __str__(self) -> str:
        return f"{self.céntimos / 100:_.2f}".replace(".", ",").replace("_", ".")

    def __format__(self, fmt_spec: str) -> str:
        fmt_spec = fmt_spec.strip().lower()
        con_euro = "€" in fmt_spec
        con_euro_texto = "e" in fmt_spec
        en_kilos = "k" in fmt_spec
        en_megas = "m" in fmt_spec
        if en_kilos:
            número = f"{self.céntimos / 100_000:_.2f}".replace(".", ",").replace("_", ".")
            prefijo = "K"
        elif en_megas:
            número = f"{self.céntimos / 100_000_000:_.2f}".replace(".", ",").replace("_", ".")
            prefijo = "M"
        else:
            número = f"{self.céntimos / 100:_.2f}".replace(".", ",").replace("_", ".")
            prefijo = ""
        return número + prefijo + ("€" if con_euro else "euro" if con_euro_texto else "")


zero = E(0)
