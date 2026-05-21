"""Diccionario de ficheros de entrada extraído de la especificación.

La especificación (Typst) declara bloques `#let ficheros_campos_X = (
"fichero.xlsx": (descripción: [...], campos: (campo: [...], ...))
...)` con la descripción de cada fichero y de cada uno de sus campos.

Este módulo parsea esos bloques con un escaneador balanceado simple y
devuelve un diccionario plano usable por el visor:

    { "fichero.xlsx": {"descripción": "…", "campos": {"campo": "…"}}, … }

Las funciones de formato Typst (#val, #ruta, #campo, #etq…) se reducen
a texto plano para que la API pueda servirlas sin más procesado.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path


_SPEC_PATH = Path("documentación/especificación.typ")


def _balancear(s: str, start: int, abrir: str, cerrar: str) -> int:
    """Devuelve el índice del cierre que balancea `s[start]`.
    `s[start]` debe ser `abrir`. Salta strings entre comillas."""
    if start >= len(s) or s[start] != abrir:
        return -1
    depth = 0
    i = start
    while i < len(s):
        c = s[i]
        if c == abrir:
            depth += 1
        elif c == cerrar:
            depth -= 1
            if depth == 0:
                return i
        elif c == '"':
            # saltar cadena
            i += 1
            while i < len(s) and s[i] != '"':
                if s[i] == "\\":
                    i += 1
                i += 1
        i += 1
    return -1


_RE_TAG_ARG = re.compile(
    r'#(val|campo|etqcen|etqact|etqele|cód|raw)\(\s*"([^"]*)"\s*\)'
)
_RE_RUTA = re.compile(r'#ruta\(([^)]*)\)')
_RE_EMPH = re.compile(r'#emph\[([^\[\]]*)\]')
_RE_NOMBRE_REGLA = re.compile(r'#nombre-regla\[([^\[\]]*)\]')


def _limpiar_typst(s: str) -> str:
    """Reduce las funciones de formato Typst a texto plano."""

    def _ruta_repl(m: re.Match) -> str:
        partes = re.findall(r'"([^"]*)"', m.group(1))
        return "/".join(partes)

    s = _RE_RUTA.sub(_ruta_repl, s)
    s = _RE_TAG_ARG.sub(lambda m: m.group(2), s)
    s = _RE_EMPH.sub(r"\1", s)
    s = _RE_NOMBRE_REGLA.sub(r"\1", s)
    s = s.replace("#app", "app")
    # Espacios + saltos de línea → un único espacio.
    return re.sub(r"\s+", " ", s).strip()


def _parsear_campos(texto: str) -> dict[str, str]:
    """Dentro del paréntesis de `campos: (...)`, extrae los pares
    `nombre: [valor]`. `nombre` puede ir entre comillas (nombres con
    espacios) o como identificador con `_` y guiones."""
    out: dict[str, str] = {}
    i = 0
    pat = re.compile(r'(?:"([^"]+)"|([\wáéíóúñÁÉÍÓÚÑ][\w_-]*))\s*:\s*\[')
    while i < len(texto):
        m = pat.search(texto, i)
        if m is None:
            break
        nombre = m.group(1) or m.group(2)
        b_open = m.end() - 1  # posición del '['
        b_close = _balancear(texto, b_open, "[", "]")
        if b_close < 0:
            break
        valor = _limpiar_typst(texto[b_open + 1 : b_close])
        out[nombre] = valor
        i = b_close + 1
    return out


def _parsear_ficha(contenido: str) -> dict:
    descripción = ""
    campos: dict[str, str] = {}

    m = re.search(r"descripción\s*:\s*\[", contenido)
    if m is not None:
        b_open = m.end() - 1
        b_close = _balancear(contenido, b_open, "[", "]")
        if b_close >= 0:
            descripción = _limpiar_typst(contenido[b_open + 1 : b_close])

    m = re.search(r"campos\s*:\s*\(", contenido)
    if m is not None:
        p_open = m.end() - 1
        p_close = _balancear(contenido, p_open, "(", ")")
        if p_close >= 0:
            campos = _parsear_campos(contenido[p_open + 1 : p_close])

    return {"descripción": descripción, "campos": campos}


def _parsear_bloque(bloque: str, out: dict[str, dict]) -> None:
    """Itera las entradas `"xxx.xlsx": (...)` dentro de un bloque."""
    pat = re.compile(r'"([^"]+\.xlsx)"\s*:\s*\(')
    i = 0
    while i < len(bloque):
        m = pat.search(bloque, i)
        if m is None:
            break
        p_open = m.end() - 1
        p_close = _balancear(bloque, p_open, "(", ")")
        if p_close < 0:
            break
        ficha = _parsear_ficha(bloque[p_open + 1 : p_close])
        out[m.group(1)] = ficha
        i = p_close + 1


@lru_cache(maxsize=2)
def _cargar_cached(path_str: str, mtime_ns: int) -> dict[str, dict]:
    del mtime_ns
    p = Path(path_str)
    if not p.exists():
        return {}
    text = p.read_text(encoding="utf-8")
    out: dict[str, dict] = {}
    for m in re.finditer(r"#let\s+ficheros_campos_\w+\s*=\s*\(", text):
        p_open = m.end() - 1
        p_close = _balancear(text, p_open, "(", ")")
        if p_close < 0:
            continue
        _parsear_bloque(text[p_open + 1 : p_close], out)
    return out


def cargar_diccionario(
    spec_path: Path = _SPEC_PATH,
) -> dict[str, dict]:
    """Devuelve `{nombre.xlsx: {descripción, campos: {campo: desc}}}`."""
    p = Path(spec_path)
    mtime = p.stat().st_mtime_ns if p.exists() else 0
    return _cargar_cached(str(p), mtime)


def ficha_para(nombre_fichero: str) -> dict | None:
    """Devuelve la ficha del fichero indicado (o None si no está)."""
    return cargar_diccionario().get(nombre_fichero)
