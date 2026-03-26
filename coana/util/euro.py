"""Tipo monetario Euro sin errores de redondeo.

Internamente usa un entero que representa céntimos, evitando
cualquier problema de representación en coma flotante.
La impresión sigue convenios europeos: 1.234,56 €

Compatible con Pydantic 2: se puede usar como tipo de campo en BaseModel.
"""

from typing import Any, Self

from pydantic import GetCoreSchemaHandler, GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema, core_schema


class Euro:
    """Importe monetario en euros, precisión de céntimo."""

    __slots__ = ("_céntimos",)

    def __init__(self, valor: int | float | str | Euro = 0) -> None:
        if isinstance(valor, Euro):
            self._céntimos = valor._céntimos
        elif isinstance(valor, int):
            self._céntimos = valor * 100
        elif isinstance(valor, float):
            self._céntimos = round(valor * 100)
        elif isinstance(valor, str):
            self._céntimos = self._parse(valor)
        else:
            raise TypeError(f"No se puede crear Euro a partir de {type(valor).__name__}")

    @staticmethod
    def _parse(texto: str) -> int:
        """Interpreta cadenas en formato europeo o anglosajón."""
        t = texto.strip().replace("€", "").replace("\u00a0", "").strip()
        if not t:
            return 0
        # Formato europeo: 1.234,56 → separador decimal es coma
        if "," in t and ("." not in t or t.rindex(".") < t.rindex(",")):
            t = t.replace(".", "").replace(",", ".")
        # Formato anglosajón: 1,234.56 → separador decimal es punto
        elif "." in t and "," in t:
            t = t.replace(",", "")
        return round(float(t) * 100)

    @classmethod
    def desde_céntimos(cls, céntimos: int) -> Self:
        obj = object.__new__(cls)
        obj._céntimos = céntimos
        return obj

    @property
    def céntimos(self) -> int:
        return self._céntimos

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
    def _pydantic_validate(cls, valor: Any) -> Euro:
        if isinstance(valor, cls):
            return valor
        if isinstance(valor, (int, float, str)):
            return cls(valor)
        raise ValueError(f"No se puede convertir {type(valor).__name__} a Euro")

    @staticmethod
    def _pydantic_serialize(euro: Euro) -> float:
        return euro._céntimos / 100

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        _schema: CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        return {"type": "number", "description": "Importe en euros"}

    # --- Aritmética ---

    def __add__(self, otro: object) -> Euro:
        if isinstance(otro, Euro):
            return Euro.desde_céntimos(self._céntimos + otro._céntimos)
        if isinstance(otro, (int, float)):
            return self + Euro(otro)
        return NotImplemented

    def __radd__(self, otro: object) -> Euro:
        # Permite sum() que arranca con 0
        if isinstance(otro, (int, float)):
            return Euro(otro) + self
        return NotImplemented

    def __sub__(self, otro: object) -> Euro:
        if isinstance(otro, Euro):
            return Euro.desde_céntimos(self._céntimos - otro._céntimos)
        if isinstance(otro, (int, float)):
            return self - Euro(otro)
        return NotImplemented

    def __rsub__(self, otro: object) -> Euro:
        if isinstance(otro, (int, float)):
            return Euro(otro) - self
        return NotImplemented

    def __mul__(self, factor: object) -> Euro:
        if isinstance(factor, (int, float)):
            return Euro.desde_céntimos(round(self._céntimos * factor))
        return NotImplemented

    def __rmul__(self, factor: object) -> Euro:
        return self.__mul__(factor)

    def __truediv__(self, divisor: object) -> Euro | float:
        if isinstance(divisor, Euro):
            if divisor._céntimos == 0:
                raise ZeroDivisionError("División por cero euros")
            return self._céntimos / divisor._céntimos
        if isinstance(divisor, (int, float)):
            if divisor == 0:
                raise ZeroDivisionError("División por cero")
            return Euro.desde_céntimos(round(self._céntimos / divisor))
        return NotImplemented

    def __neg__(self) -> Euro:
        return Euro.desde_céntimos(-self._céntimos)

    def __abs__(self) -> Euro:
        return Euro.desde_céntimos(abs(self._céntimos))

    # --- Comparación ---

    def _cmp_value(self, otro: object) -> int | None:
        if isinstance(otro, Euro):
            return otro._céntimos
        if isinstance(otro, (int, float)):
            return round(otro * 100)
        return None

    def __eq__(self, otro: object) -> bool:
        v = self._cmp_value(otro)
        return self._céntimos == v if v is not None else NotImplemented

    def __lt__(self, otro: object) -> bool:
        v = self._cmp_value(otro)
        if v is None:
            return NotImplemented
        return self._céntimos < v

    def __le__(self, otro: object) -> bool:
        v = self._cmp_value(otro)
        if v is None:
            return NotImplemented
        return self._céntimos <= v

    def __gt__(self, otro: object) -> bool:
        v = self._cmp_value(otro)
        if v is None:
            return NotImplemented
        return self._céntimos > v

    def __ge__(self, otro: object) -> bool:
        v = self._cmp_value(otro)
        if v is None:
            return NotImplemented
        return self._céntimos >= v

    def __hash__(self) -> int:
        return hash(self._céntimos)

    # --- Conversión ---

    def __float__(self) -> float:
        return self._céntimos / 100

    def __int__(self) -> int:
        return self._céntimos // 100

    def __bool__(self) -> bool:
        return self._céntimos != 0

    # --- Representación ---

    def __repr__(self) -> str:
        return f"Euro({float(self)})"

    def __str__(self) -> str:
        return self.formato()

    def formato(self, *, con_símbolo: bool = True) -> str:
        """Formatea el importe según convenio europeo: 1.234,56 €"""
        negativo = self._céntimos < 0
        total = abs(self._céntimos)
        euros, cents = divmod(total, 100)

        # Agrupación de miles con punto
        parte_entera = f"{euros:,}".replace(",", ".")
        texto = f"{parte_entera},{cents:02d}"
        if negativo:
            texto = f"-{texto}"
        if con_símbolo:
            texto = f"{texto} €"
        return texto
