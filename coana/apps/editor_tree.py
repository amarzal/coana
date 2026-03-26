"""Editor gráfico de ficheros .tree con tkinter."""

import argparse
import platform
import re
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path


INDENT = "    "  # 4 espacios por nivel
FONT = ("Menlo", 12)
BG_DISABLED = "#e8e8e8"
BG_HIGHLIGHT = "#4a90d9"
FG_HIGHLIGHT = "#ffffff"
BG_ERROR = "#ffcccc"
COLOR_IDENT = "#1a5cc8"
COLOR_BUSCAR = "#1a7a2a"
COLOR_DUPLICADO = "#cc0000"


def _calcular_códigos(líneas: list[str]) -> list[str]:
    """Genera códigos (01, 01.02, …) a partir de las líneas del editor.

    Replica la lógica de Árbol.from_str en coana/arbol.py.
    """
    códigos: list[str] = []
    pila: list[tuple[int, str, int]] = [(-1, "", 0)]

    for línea in líneas:
        stripped = línea.strip()
        if not stripped:
            códigos.append("")
            continue
        parte_desc = línea.split("|")[0] if "|" in línea else línea
        indent = len(parte_desc) - len(parte_desc.lstrip())
        nivel = indent // 4

        while pila[-1][0] >= nivel:
            pila.pop()

        padre_nivel, padre_código, n_hijos = pila[-1]
        n_hijos += 1
        pila[-1] = (padre_nivel, padre_código, n_hijos)

        if padre_código:
            código = f"{padre_código}.{n_hijos:02d}"
        else:
            código = f"{n_hijos:02d}"

        códigos.append(código)
        pila.append((nivel, código, 0))

    return códigos


def _detectar_duplicados(líneas: list[str]) -> set[int]:
    """Devuelve el conjunto de números de línea (1-based) con identificador duplicado."""
    vistos: dict[str, list[int]] = {}
    for i, línea in enumerate(líneas, 1):
        if not línea.strip():
            continue
        partes = línea.rsplit("|", 1)
        if len(partes) < 2:
            continue
        ident = partes[1].strip()
        if not ident:
            continue
        vistos.setdefault(ident, []).append(i)

    duplicadas: set[int] = set()
    for nums in vistos.values():
        if len(nums) > 1:
            duplicadas.update(nums)
    return duplicadas


class PestañaTree(ttk.Frame):
    """Una pestaña del editor para un fichero .tree."""

    def __init__(self, parent: ttk.Notebook, path: Path, solo_lectura: bool = False) -> None:
        super().__init__(parent)
        self.path = path
        self.solo_lectura = solo_lectura
        self._syncing = False
        self._scroll_suppressed = False
        self._línea_destacada: int = -1
        self._líneas_coincidentes: list[int] = []
        self._idx_coincidencia: int = -1
        self._líneas_error: list[int] = []
        self._idx_error: int = -1
        self._mensajes_error: dict[int, str] = {}  # línea → mensaje

        self._crear_paneles()
        self._cargar()
        self._vincular_eventos()

    def _crear_paneles(self) -> None:
        # Barra de estado abajo
        self.lbl_estado = ttk.Label(self, text="", font=("Menlo", 11), foreground=COLOR_DUPLICADO)
        self.lbl_estado.pack(side="bottom", fill="x", padx=6, pady=(0, 2))

        # Scrollbar a la derecha
        self.scrollbar = ttk.Scrollbar(self, orient="vertical")
        self.scrollbar.pack(side="right", fill="y")

        # PanedWindow: líneas | códigos | editor
        self.paned = tk.PanedWindow(
            self, orient="horizontal", sashwidth=6, sashrelief="raised",
            bg="#cccccc",
        )
        self.paned.pack(fill="both", expand=True)

        # --- Panel números de línea (solo lectura) ---
        frame_líneas = tk.Frame(self.paned)
        tk.Label(frame_líneas, text="Línea", font=FONT, anchor="w").pack(
            fill="x", padx=4, pady=2,
        )
        self.txt_líneas = tk.Text(
            frame_líneas, width=5, font=FONT, wrap="none",
            state="disabled", bg=BG_DISABLED, cursor="arrow",
            borderwidth=1, relief="sunken",
        )
        self.txt_líneas.pack(fill="both", expand=True)
        self.paned.add(frame_líneas, minsize=50, width=60)

        # --- Panel códigos (solo lectura) ---
        frame_códigos = tk.Frame(self.paned)
        tk.Label(frame_códigos, text="Código", font=FONT, anchor="w").pack(
            fill="x", padx=4, pady=2,
        )
        self.txt_códigos = tk.Text(
            frame_códigos, width=14, font=FONT, wrap="none",
            state="disabled", bg=BG_DISABLED, cursor="arrow",
            borderwidth=1, relief="sunken",
        )
        self.txt_códigos.pack(fill="both", expand=True)
        self.paned.add(frame_códigos, minsize=80, width=140)

        # --- Panel editor (descripción | identificador) ---
        frame_editor = tk.Frame(self.paned)
        tk.Label(
            frame_editor, text="Descripción | Identificador", font=FONT, anchor="w",
        ).pack(fill="x", padx=4, pady=2)
        self.txt_editor = tk.Text(
            frame_editor, font=FONT, wrap="none", undo=True,
            borderwidth=1, relief="sunken",
            selectbackground="#ffcc80", selectforeground="black",
        )
        self.txt_editor.pack(fill="both", expand=True)
        self.paned.add(frame_editor, minsize=400)

        # Si es solo lectura, desactivar el editor
        if self.solo_lectura:
            self.txt_editor.config(state="disabled", bg=BG_DISABLED)
            self.lbl_estado.config(text="Solo lectura")

        # Paneles de solo lectura (para iterar cómodamente)
        self._paneles_ro = (self.txt_líneas, self.txt_códigos)
        self._todos = (self.txt_líneas, self.txt_códigos, self.txt_editor)

        # Tags
        self.txt_editor.tag_configure("ident", foreground=COLOR_IDENT)
        self.txt_editor.tag_configure("duplicado", foreground=COLOR_DUPLICADO)
        for txt in self._todos:
            txt.tag_configure("linea_actual", background=BG_HIGHLIGHT, foreground=FG_HIGHLIGHT)
            txt.tag_configure("error", background=BG_ERROR)
            txt.tag_configure("buscar", foreground=COLOR_BUSCAR, font=(*FONT, "bold"))
        # Prioridad: duplicado > buscar > ident; linea_actual encima, error encima de todo
        self.txt_editor.tag_raise("buscar", "ident")
        self.txt_editor.tag_raise("duplicado", "buscar")
        for txt in self._todos:
            txt.tag_raise("linea_actual")
            txt.tag_raise("error")

        # Scroll sincronizado
        self.scrollbar.config(command=self._on_scrollbar)
        for txt in self._todos:
            txt.config(yscrollcommand=self._on_text_scroll)

    # --- Scroll sincronizado ---

    def _on_scrollbar(self, *args) -> None:
        for txt in self._todos:
            txt.yview(*args)

    def _on_text_scroll(self, first, last) -> None:
        if self._scroll_suppressed:
            return
        self.scrollbar.set(first, last)
        for txt in self._todos:
            txt.yview("moveto", first)

    def _on_mousewheel(self, event: tk.Event) -> str:
        if platform.system() == "Darwin":
            delta = -event.delta
        else:
            delta = -event.delta // 120
        for txt in self._todos:
            txt.yview("scroll", delta, "units")
        return "break"

    # --- Colorear identificadores y duplicados ---

    def _colorear_identificadores(self, líneas: list[str]) -> None:
        """Aplica tags 'ident' y 'duplicado' a los identificadores."""
        self.txt_editor.tag_remove("ident", "1.0", "end")
        self.txt_editor.tag_remove("duplicado", "1.0", "end")

        duplicadas = _detectar_duplicados(líneas)

        n_dup = 0
        for i, línea in enumerate(líneas, 1):
            pos_pipe = línea.rfind("|")
            if pos_pipe < 0:
                continue
            if i in duplicadas:
                self.txt_editor.tag_add("duplicado", f"{i}.{pos_pipe}", f"{i}.end")
                n_dup += 1
            else:
                self.txt_editor.tag_add("ident", f"{i}.{pos_pipe}", f"{i}.end")

        if n_dup:
            self.lbl_estado.config(
                text=f"⚠ {n_dup} identificadores duplicados",
            )
        elif not self.solo_lectura:
            self.lbl_estado.config(text="")

    # --- Highlight de línea actual ---

    def _actualizar_highlight(self, _event: tk.Event | None = None) -> None:
        """Destaca la línea actual en ambos paneles."""
        foco = self.focus_get()
        if foco is not self.txt_editor:
            return

        línea = int(self.txt_editor.index("insert").split(".")[0])
        if línea == self._línea_destacada:
            return
        self._línea_destacada = línea

        for txt in self._paneles_ro:
            txt.tag_remove("linea_actual", "1.0", "end")
            txt.tag_add("linea_actual", f"{línea}.0", f"{línea}.0 lineend")
        self.txt_editor.tag_remove("linea_actual", "1.0", "end")

        # Mostrar mensaje de error si la línea tiene uno
        if línea in self._mensajes_error:
            self.lbl_estado.config(text=self._mensajes_error[línea])
        elif self._mensajes_error:
            self.lbl_estado.config(text="")


    # --- Eventos ---

    def _vincular_eventos(self) -> None:
        for txt in self._todos:
            txt.bind("<MouseWheel>", self._on_mousewheel)

        self.txt_editor.bind("<KeyRelease>", self._actualizar_highlight)
        self.txt_editor.bind("<ButtonRelease-1>", self._actualizar_highlight)

        if not self.solo_lectura:
            self.txt_editor.bind("<<Modified>>", self._on_modified)
            self.txt_editor.bind("<Tab>", self._on_tab)
            self.txt_editor.bind("<Shift-Tab>", self._on_shift_tab)
            self.txt_editor.bind("<<Cut>>", self._on_cortar)
            self.txt_editor.bind("<Control-x>", self._on_cortar)

        # Navegación estructural por el árbol
        mod = "Command" if platform.system() == "Darwin" else "Control"
        self.txt_editor.bind(f"<{mod}-Right>", self._on_hermano_siguiente)
        self.txt_editor.bind(f"<{mod}-Left>", self._on_hermano_anterior)
        self.txt_editor.bind(f"<{mod}-Up>", self._on_padre)
        self.txt_editor.bind(f"<{mod}-Down>", self._on_primer_hijo)

    # --- Navegación estructural ---

    @staticmethod
    def _nivel_línea(línea: str) -> int:
        """Devuelve el nivel de indentación (indent // 4) de una línea."""
        parte = línea.split("|")[0] if "|" in línea else línea
        indent = len(parte) - len(parte.lstrip())
        return indent // 4

    def _líneas_y_niveles(self) -> list[tuple[int, str]]:
        """Devuelve lista de (nivel, texto) para cada línea del editor."""
        return [
            (self._nivel_línea(l), l)
            for l in self.txt_editor.get("1.0", "end-1c").split("\n")
        ]

    def _ir_a_línea(self, n: int) -> str:
        """Mueve el cursor a la línea n (1-based) y actualiza highlight."""
        self.txt_editor.mark_set("insert", f"{n}.0")
        self.txt_editor.see(f"{n}.0")
        self._línea_destacada = -1
        self._actualizar_highlight()
        return "break"

    def _hermanos(self, líneas: list[tuple[int, str]], actual_1based: int) -> list[int]:
        """Devuelve las líneas (1-based) que son hermanos del nodo actual."""
        nivel = líneas[actual_1based - 1][0]
        hermanos: list[int] = [actual_1based]
        # Buscar hermanos hacia atrás
        for i in range(actual_1based - 2, -1, -1):
            if not líneas[i][1].strip():
                continue
            if líneas[i][0] == nivel:
                hermanos.insert(0, i + 1)
            elif líneas[i][0] < nivel:
                break
        # Buscar hermanos hacia adelante
        for i in range(actual_1based, len(líneas)):
            if not líneas[i][1].strip():
                continue
            if líneas[i][0] == nivel:
                hermanos.append(i + 1)
            elif líneas[i][0] < nivel:
                break
        return hermanos

    def _on_hermano_siguiente(self, _event: tk.Event) -> str:
        líneas = self._líneas_y_niveles()
        actual = int(self.txt_editor.index("insert").split(".")[0])
        if actual > len(líneas):
            return "break"
        hermanos = self._hermanos(líneas, actual)
        if len(hermanos) <= 1:
            return "break"
        idx = hermanos.index(actual)
        destino = hermanos[(idx + 1) % len(hermanos)]
        return self._ir_a_línea(destino)

    def _on_hermano_anterior(self, _event: tk.Event) -> str:
        líneas = self._líneas_y_niveles()
        actual = int(self.txt_editor.index("insert").split(".")[0])
        if actual > len(líneas):
            return "break"
        hermanos = self._hermanos(líneas, actual)
        if len(hermanos) <= 1:
            return "break"
        idx = hermanos.index(actual)
        destino = hermanos[(idx - 1) % len(hermanos)]
        return self._ir_a_línea(destino)

    def _on_padre(self, _event: tk.Event) -> str:
        líneas = self._líneas_y_niveles()
        actual = int(self.txt_editor.index("insert").split(".")[0])
        if actual <= 1:
            return "break"
        nivel_actual = líneas[actual - 1][0]
        if nivel_actual == 0:
            return "break"
        for i in range(actual - 2, -1, -1):
            if not líneas[i][1].strip():
                continue
            if líneas[i][0] < nivel_actual:
                return self._ir_a_línea(i + 1)
        return "break"

    def _on_primer_hijo(self, _event: tk.Event) -> str:
        líneas = self._líneas_y_niveles()
        actual = int(self.txt_editor.index("insert").split(".")[0])
        if actual >= len(líneas):
            return "break"
        nivel_actual = líneas[actual - 1][0]
        # El primer hijo es la siguiente línea no vacía con nivel + 1
        next_idx = actual  # 0-based
        if next_idx < len(líneas) and líneas[next_idx][1].strip():
            if líneas[next_idx][0] == nivel_actual + 1:
                return self._ir_a_línea(next_idx + 1)
        return "break"

    def _on_modified(self, _event: tk.Event) -> None:
        if self.txt_editor.edit_modified():
            self.txt_editor.edit_modified(False)
            self.after_idle(self._sincronizar)

    def _sincronizar(self) -> None:
        """Recalcula códigos y recolorea identificadores."""
        if self.solo_lectura:
            return
        if self._syncing:
            return
        self._syncing = True
        try:
            # Guardar posición del cursor y vista
            pos_cursor = self.txt_editor.index("insert")
            vista = self.txt_editor.yview()[0]

            líneas = self.txt_editor.get("1.0", "end-1c").split("\n")

            # Suprimir scroll sync mientras reescribimos paneles RO
            self._scroll_suppressed = True

            # Números de línea
            nums = "\n".join(str(i) for i in range(1, len(líneas) + 1))
            self.txt_líneas.config(state="normal")
            self.txt_líneas.delete("1.0", "end")
            self.txt_líneas.insert("1.0", nums)
            self.txt_líneas.config(state="disabled")

            # Códigos
            códigos = _calcular_códigos(líneas)
            self.txt_códigos.config(state="normal")
            self.txt_códigos.delete("1.0", "end")
            self.txt_códigos.insert("1.0", "\n".join(códigos))
            self.txt_códigos.config(state="disabled")

            self._scroll_suppressed = False

            # Recolorear identificadores y detectar duplicados
            self._colorear_identificadores(líneas)

            # Restaurar posición del cursor y vista
            self.txt_editor.mark_set("insert", pos_cursor)
            for txt in self._todos:
                txt.yview("moveto", vista)

            # Refrescar highlight
            self._línea_destacada = -1
            self._actualizar_highlight()
        finally:
            self._scroll_suppressed = False
            self._syncing = False

    def _on_cortar(self, _event: tk.Event) -> str:
        """Corta la selección y posiciona el cursor en la línea anterior."""
        try:
            sel = self.txt_editor.tag_ranges("sel")
            if not sel:
                return "break"
            texto = self.txt_editor.get(sel[0], sel[1])
            línea = int(str(sel[0]).split(".")[0])

            self.txt_editor.clipboard_clear()
            self.txt_editor.clipboard_append(texto)
            self.txt_editor.delete(sel[0], sel[1])

            if "\n" in texto:
                nueva = max(1, línea - 1)
                self.txt_editor.mark_set("insert", f"{nueva}.0")
                self.txt_editor.see(f"{nueva}.0")
        except tk.TclError:
            pass
        return "break"

    def _rango_líneas_selección(self) -> tuple[int, int]:
        """Devuelve (primera, última) líneas de la selección, o la línea del cursor."""
        try:
            sel = self.txt_editor.tag_ranges("sel")
            if sel:
                primera = int(str(sel[0]).split(".")[0])
                última = int(str(sel[1]).split(".")[0])
                # Si la selección termina al inicio de una línea, no incluirla
                if str(sel[1]).endswith(".0"):
                    última = max(primera, última - 1)
                return primera, última
        except tk.TclError:
            pass
        línea = int(self.txt_editor.index("insert").split(".")[0])
        return línea, línea

    def _on_tab(self, _event: tk.Event) -> str:
        primera, última = self._rango_líneas_selección()
        for n in range(primera, última + 1):
            self.txt_editor.insert(f"{n}.0", INDENT)
        return "break"

    def _on_shift_tab(self, _event: tk.Event) -> str:
        primera, última = self._rango_líneas_selección()
        for n in range(primera, última + 1):
            contenido = self.txt_editor.get(f"{n}.0", f"{n}.4")
            n_espacios = len(contenido) - len(contenido.lstrip(" "))
            n_borrar = min(n_espacios, 4)
            if n_borrar > 0:
                self.txt_editor.delete(f"{n}.0", f"{n}.{n_borrar}")
        return "break"

    # --- Carga / Guardado ---

    def _cargar(self) -> None:
        """Carga el fichero .tree en el editor."""
        texto = self.path.read_text(encoding="utf-8")
        líneas: list[str] = []
        for línea in texto.splitlines():
            stripped = línea.strip()
            if not stripped or stripped.startswith("#"):
                continue
            líneas.append(línea)

        if self.solo_lectura:
            self.txt_editor.config(state="normal")
        self.txt_editor.delete("1.0", "end")
        self.txt_editor.insert("1.0", "\n".join(líneas))
        self.txt_editor.edit_reset()
        if self.solo_lectura:
            self.txt_editor.config(state="disabled")

        # Números de línea
        nums = "\n".join(str(i) for i in range(1, len(líneas) + 1))
        self.txt_líneas.config(state="normal")
        self.txt_líneas.delete("1.0", "end")
        self.txt_líneas.insert("1.0", nums)
        self.txt_líneas.config(state="disabled")

        # Códigos
        códigos = _calcular_códigos(líneas)
        self.txt_códigos.config(state="normal")
        self.txt_códigos.delete("1.0", "end")
        self.txt_códigos.insert("1.0", "\n".join(códigos))
        self.txt_códigos.config(state="disabled")

        # Colorear identificadores y detectar duplicados
        self._colorear_identificadores(líneas)

    def limpiar_errores(self) -> None:
        """Elimina el resaltado de errores."""
        for txt in self._todos:
            txt.tag_remove("error", "1.0", "end")
        self._líneas_error = []
        self._idx_error = -1
        self._mensajes_error = {}

    def validar(self) -> list[int]:
        """Valida el contenido. Devuelve lista de líneas con error (1-based)."""
        self.limpiar_errores()
        nombre = self.path.stem
        líneas = self.txt_editor.get("1.0", "end-1c").split("\n")

        errores: dict[int, str] = {}  # línea → mensaje

        # 1) Sangrado módulo 4 y saltos de nivel
        nivel_anterior = 0
        for i, línea in enumerate(líneas, 1):
            stripped = línea.strip()
            if not stripped:
                continue
            parte_desc = línea.split("|")[0] if "|" in línea else línea
            indent = len(parte_desc) - len(parte_desc.lstrip())
            if indent % 4 != 0:
                errores[i] = f"Mala indentación ({indent} espacios)"
            else:
                nivel = indent // 4
                if nivel > nivel_anterior + 1:
                    errores[i] = f"Salto de nivel ({nivel_anterior} → {nivel})"
                nivel_anterior = nivel

        # 2) Identificadores duplicados
        vistos: dict[str, int] = {}
        for i, línea in enumerate(líneas, 1):
            stripped = línea.strip()
            if not stripped:
                continue
            partes = línea.rsplit("|", 1)
            if len(partes) < 2 or not partes[1].strip():
                continue
            ident = partes[1].strip()
            if ident in vistos:
                errores[i] = f"Identificador duplicado: '{ident}'"
                if vistos[ident] not in errores:
                    errores[vistos[ident]] = f"Identificador duplicado: '{ident}'"
            else:
                vistos[ident] = i

        # Ordenar por número de línea y almacenar
        self._líneas_error = sorted(errores.keys())
        self._mensajes_error = errores

        # Aplicar tag error
        for n in self._líneas_error:
            for txt in self._todos:
                txt.tag_add("error", f"{n}.0", f"{n}.0 lineend")

        return self._líneas_error

    def ir_a_error(self, idx: int) -> tuple[int, str]:
        """Navega al error nº idx. Devuelve (índice 0-based, mensaje)."""
        if not self._líneas_error:
            return -1, ""
        idx = idx % len(self._líneas_error)
        self._idx_error = idx
        línea = self._líneas_error[idx]
        self.txt_editor.focus_set()
        self.txt_editor.mark_set("insert", f"{línea}.0")
        for txt in self._todos:
            txt.see(f"{línea}.0")
        # No aplicar linea_actual — el tag error (rojo) es lo que debe verse
        self._línea_destacada = línea
        for txt in self._todos:
            txt.tag_remove("linea_actual", "1.0", "end")
        msg = self._mensajes_error.get(línea, "")
        self.lbl_estado.config(text=msg)
        return idx, msg

    def buscar(self, patrón: str, es_regex: bool) -> int:
        """Resalta líneas que coinciden con el patrón. Devuelve nº de coincidencias."""
        for txt in self._todos:
            txt.tag_remove("buscar", "1.0", "end")

        self._líneas_coincidentes = []
        self._idx_coincidencia = -1

        if not patrón:
            return 0

        try:
            if es_regex:
                rx = re.compile(patrón, re.IGNORECASE)
            else:
                rx = re.compile(re.escape(patrón), re.IGNORECASE)
        except re.error:
            return 0

        líneas_editor = self.txt_editor.get("1.0", "end-1c").split("\n")
        líneas_códigos = self.txt_códigos.get("1.0", "end-1c").split("\n")

        for i, (ed, cod) in enumerate(zip(líneas_editor, líneas_códigos), 1):
            texto_completo = f"{cod} {ed}"
            if rx.search(texto_completo):
                self._líneas_coincidentes.append(i)
                for txt in self._todos:
                    txt.tag_add("buscar", f"{i}.0", f"{i}.end")

        return len(self._líneas_coincidentes)

    def ir_a_coincidencia(self, idx: int) -> int:
        """Navega a la coincidencia nº idx. Devuelve el índice actual (0-based)."""
        if not self._líneas_coincidentes:
            return -1
        idx = idx % len(self._líneas_coincidentes)
        self._idx_coincidencia = idx
        línea = self._líneas_coincidentes[idx]
        self.txt_editor.focus_set()
        self.txt_editor.mark_set("insert", f"{línea}.0")
        for txt in self._todos:
            txt.see(f"{línea}.0")
        # Aplicar highlight solo en paneles RO (no en editor, para no tapar selección)
        self._línea_destacada = línea
        for txt in self._paneles_ro:
            txt.tag_remove("linea_actual", "1.0", "end")
            txt.tag_add("linea_actual", f"{línea}.0", f"{línea}.0 lineend")
        self.txt_editor.tag_remove("linea_actual", "1.0", "end")
        return idx

    def guardar(self) -> None:
        """Escribe el fichero .tree."""
        líneas = self.txt_editor.get("1.0", "end-1c").split("\n")
        salida: list[str] = []
        for línea in líneas:
            if línea.strip():
                salida.append(línea)
        self.path.write_text("\n".join(salida) + "\n", encoding="utf-8")


class EditorTree(tk.Tk):
    """Ventana principal del editor de ficheros .tree."""

    def __init__(self, ruta_base: Path = Path("data")) -> None:
        super().__init__()
        self.title("Editor de árboles")
        self.geometry("1200x700")

        self.ruta_estructuras = ruta_base / "entrada" / "estructuras"
        self.ruta_fase1 = ruta_base / "fase1"

        self._crear_barra()
        self._crear_notebook()
        self._cargar_pestañas()
        self._vincular_atajos()

    def _crear_barra(self) -> None:
        barra = ttk.Frame(self)
        barra.pack(fill="x", padx=6, pady=4)

        ttk.Button(barra, text="Guardar", command=self._guardar).pack(side="left", padx=4)
        ttk.Button(barra, text="Validar", command=self._validar).pack(side="left", padx=4)

        ttk.Button(barra, text="Actualizar", command=self._actualizar).pack(side="left", padx=4)

        ttk.Button(
            barra, text="Coherencia",
            command=self._abrir_diálogo_coherencia,
        ).pack(side="left", padx=4)

        # --- Navegación de errores (junto a Validar) ---
        ttk.Button(barra, text="\u25b2", width=2, command=self._error_anterior).pack(side="left")
        ttk.Button(barra, text="\u25bc", width=2, command=self._error_siguiente).pack(side="left")
        self.lbl_errores = ttk.Label(barra, text="", font=("Menlo", 11), foreground=COLOR_DUPLICADO)
        self.lbl_errores.pack(side="left", padx=(4, 12))

        # --- Búsqueda (lado derecho) ---
        self.lbl_resultados = ttk.Label(barra, text="", font=("Menlo", 11))
        self.lbl_resultados.pack(side="right", padx=(4, 8))

        ttk.Button(barra, text="\u25bc", width=2, command=self._ir_siguiente).pack(side="right")
        ttk.Button(barra, text="\u25b2", width=2, command=self._ir_anterior).pack(side="right")

        self.var_regex = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            barra, text="Regex", variable=self.var_regex,
            command=self._on_buscar,
        ).pack(side="right", padx=2)

        self.entrada_buscar = ttk.Entry(barra, width=30, font=("Menlo", 12))
        self.entrada_buscar.pack(side="right", padx=4)
        self.entrada_buscar.bind("<KeyRelease>", lambda e: self._on_buscar())
        self.entrada_buscar.bind("<Return>", lambda e: self._ir_siguiente())
        self.entrada_buscar.bind("<Shift-Return>", lambda e: self._ir_anterior())

        ttk.Label(barra, text="Buscar:", font=("Menlo", 11)).pack(side="right", padx=(8, 2))

    def _crear_notebook(self) -> None:
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=6, pady=(0, 6))

    def _cargar_pestañas(self) -> None:
        self.pestañas: list[PestañaTree] = []

        # Pestañas editables de data/entrada/estructuras/
        archivos = sorted(self.ruta_estructuras.glob("*.tree"))
        for path in archivos:
            pestaña = PestañaTree(self.notebook, path)
            self.notebook.add(pestaña, text=path.stem)
            self.pestañas.append(pestaña)

        # Pestañas de solo lectura de data/fase1/
        archivos_fase1 = sorted(self.ruta_fase1.glob("*.tree")) if self.ruta_fase1.is_dir() else []
        for path in archivos_fase1:
            pestaña = PestañaTree(self.notebook, path, solo_lectura=True)
            self.notebook.add(pestaña, text=f"[fase1] {path.stem}")
            self.pestañas.append(pestaña)

        if not archivos and not archivos_fase1:
            messagebox.showwarning(
                "Sin ficheros",
                f"No se encontraron ficheros .tree en:\n{self.ruta_estructuras}\nni en:\n{self.ruta_fase1}",
            )

    def _vincular_atajos(self) -> None:
        mod = "Command" if platform.system() == "Darwin" else "Control"
        self.bind(f"<{mod}-s>", lambda e: self._guardar())
        self.bind(f"<{mod}-S>", lambda e: self._guardar())
        self.bind(f"<{mod}-f>", lambda e: self.entrada_buscar.focus_set())
        self.bind(f"<{mod}-F>", lambda e: self.entrada_buscar.focus_set())
        self.notebook.bind("<<NotebookTabChanged>>", lambda e: self._on_buscar())

    def _pestaña_actual(self) -> PestañaTree | None:
        if not self.pestañas:
            return None
        try:
            idx = self.notebook.index(self.notebook.select())
            return self.pestañas[idx]
        except (tk.TclError, IndexError):
            return None

    def _on_buscar(self) -> None:
        patrón = self.entrada_buscar.get()
        es_regex = self.var_regex.get()
        total = 0
        for pestaña in self.pestañas:
            total += pestaña.buscar(patrón, es_regex)
        if patrón:
            self.lbl_resultados.config(text=f"{total} líneas")
        else:
            self.lbl_resultados.config(text="")

    def _actualizar_label_posición(self, pestaña: PestañaTree) -> None:
        n = len(pestaña._líneas_coincidentes)
        if n == 0:
            return
        self.lbl_resultados.config(
            text=f"{pestaña._idx_coincidencia + 1}/{n} líneas",
        )

    def _ir_siguiente(self) -> None:
        pestaña = self._pestaña_actual()
        if not pestaña or not pestaña._líneas_coincidentes:
            return
        nuevo = (pestaña._idx_coincidencia + 1) % len(pestaña._líneas_coincidentes)
        pestaña.ir_a_coincidencia(nuevo)
        self._actualizar_label_posición(pestaña)

    def _ir_anterior(self) -> None:
        pestaña = self._pestaña_actual()
        if not pestaña or not pestaña._líneas_coincidentes:
            return
        nuevo = (pestaña._idx_coincidencia - 1) % len(pestaña._líneas_coincidentes)
        pestaña.ir_a_coincidencia(nuevo)
        self._actualizar_label_posición(pestaña)

    def _seleccionar_pestaña(self, pestaña: PestañaTree) -> None:
        """Activa la pestaña dada en el notebook."""
        idx = self.pestañas.index(pestaña)
        self.notebook.select(idx)

    def _primera_pestaña_con_error(self) -> PestañaTree | None:
        """Devuelve la primera pestaña que tenga errores."""
        for p in self.pestañas:
            if p._líneas_error:
                return p
        return None

    def _validar(self) -> bool:
        editables = [p for p in self.pestañas if not p.solo_lectura]
        total_errores = 0
        for pestaña in editables:
            total_errores += len(pestaña.validar())

        if total_errores:
            # Ir a la primera pestaña con errores
            destino = self._pestaña_actual()
            if not destino or not destino._líneas_error:
                destino = self._primera_pestaña_con_error()
            if destino:
                self._seleccionar_pestaña(destino)
                idx, msg = destino.ir_a_error(0)
                self.lbl_errores.config(
                    text=f"{idx + 1}/{len(destino._líneas_error)}: {msg}",
                )
            return False
        else:
            self.lbl_errores.config(text="")
            messagebox.showinfo("Validación", "Todos los ficheros son válidos.")
            return True

    def _error_siguiente(self) -> None:
        pestaña = self._pestaña_actual()
        if not pestaña or not pestaña._líneas_error:
            pestaña = self._primera_pestaña_con_error()
            if not pestaña:
                return
            self._seleccionar_pestaña(pestaña)
        nuevo = (pestaña._idx_error + 1) % len(pestaña._líneas_error)
        idx, msg = pestaña.ir_a_error(nuevo)
        self.lbl_errores.config(text=f"{idx + 1}/{len(pestaña._líneas_error)}: {msg}")

    def _error_anterior(self) -> None:
        pestaña = self._pestaña_actual()
        if not pestaña or not pestaña._líneas_error:
            pestaña = self._primera_pestaña_con_error()
            if not pestaña:
                return
            self._seleccionar_pestaña(pestaña)
        nuevo = (pestaña._idx_error - 1) % len(pestaña._líneas_error)
        idx, msg = pestaña.ir_a_error(nuevo)
        self.lbl_errores.config(text=f"{idx + 1}/{len(pestaña._líneas_error)}: {msg}")

    def _guardar(self) -> None:
        editables = [p for p in self.pestañas if not p.solo_lectura]
        total_errores = 0
        for pestaña in editables:
            total_errores += len(pestaña.validar())

        if total_errores:
            destino = self._primera_pestaña_con_error()
            if destino:
                self._seleccionar_pestaña(destino)
                idx, msg = destino.ir_a_error(0)
                self.lbl_errores.config(
                    text=f"{idx + 1}/{len(destino._líneas_error)}: {msg}",
                )
            return

        self.lbl_errores.config(text="")
        for pestaña in editables:
            pestaña.guardar()
        messagebox.showinfo("Guardado", "Todos los ficheros se han guardado correctamente.")

    def _actualizar(self) -> None:
        """Recarga todos los árboles desde disco, manteniendo la proporción de scroll."""
        actual = self._pestaña_actual()
        proporción = actual.txt_editor.yview()[0] if actual else 0.0

        for pestaña in self.pestañas:
            pestaña._cargar()

        if actual:
            for txt in actual._todos:
                txt.yview("moveto", proporción)

    # --- Coherencia entre árboles ---

    def _pestaña_por_nombre(self, nombre: str) -> PestañaTree | None:
        for p in self.pestañas:
            if p.path.stem == nombre:
                return p
        return None

    @staticmethod
    def _extraer_identificadores(pestaña: PestañaTree) -> set[str]:
        """Extrae los identificadores (hojas) del editor de una pestaña."""
        ids: set[str] = set()
        líneas = pestaña.txt_editor.get("1.0", "end-1c").split("\n")
        for línea in líneas:
            if not línea.strip():
                continue
            partes = línea.rsplit("|", 1)
            if len(partes) == 2:
                ident = partes[1].strip()
                if ident:
                    ids.add(ident)
        return ids

    def _abrir_diálogo_coherencia(self) -> None:
        if len(self.pestañas) < 2:
            messagebox.showwarning("Coherencia", "Se necesitan al menos dos árboles.")
            return

        nombres = [p.path.stem for p in self.pestañas]

        dlg = tk.Toplevel(self)
        dlg.title("Coherencia entre árboles")
        dlg.geometry("850x550")
        dlg.transient(self)
        dlg.grab_set()

        # --- Fila de selección ---
        frame_sel = ttk.Frame(dlg)
        frame_sel.pack(fill="x", padx=8, pady=(8, 4))

        ttk.Label(frame_sel, text="Árbol A:", font=("Menlo", 11)).pack(side="left", padx=(0, 4))
        combo_a = ttk.Combobox(frame_sel, values=nombres, state="readonly", width=30, font=("Menlo", 11))
        combo_a.pack(side="left", padx=(0, 12))
        if len(nombres) >= 1:
            combo_a.current(0)

        ttk.Label(frame_sel, text="Árbol B:", font=("Menlo", 11)).pack(side="left", padx=(0, 4))
        combo_b = ttk.Combobox(frame_sel, values=nombres, state="readonly", width=30, font=("Menlo", 11))
        combo_b.pack(side="left", padx=(0, 12))
        if len(nombres) >= 2:
            combo_b.current(1)

        # --- Cabeceras de resultados ---
        frame_cab = ttk.Frame(dlg)
        frame_cab.pack(fill="x", padx=8, pady=(4, 0))
        lbl_cab_a = ttk.Label(
            frame_cab, text="", font=(*FONT, "bold"), foreground=COLOR_DUPLICADO,
        )
        lbl_cab_a.pack(side="left", expand=True, fill="x")
        lbl_cab_b = ttk.Label(
            frame_cab, text="", font=(*FONT, "bold"), foreground=COLOR_DUPLICADO,
        )
        lbl_cab_b.pack(side="left", expand=True, fill="x")

        # --- Área de resultados con scroll sincronizado ---
        frame_listas = ttk.Frame(dlg)
        frame_listas.pack(fill="both", expand=True, padx=8, pady=4)

        scrollbar = ttk.Scrollbar(frame_listas, orient="vertical")
        scrollbar.pack(side="right", fill="y")

        txt_a = tk.Text(
            frame_listas, font=FONT, wrap="none", state="disabled",
            borderwidth=1, relief="sunken",
        )
        txt_b = tk.Text(
            frame_listas, font=FONT, wrap="none", state="disabled",
            borderwidth=1, relief="sunken",
        )
        txt_a.pack(side="left", fill="both", expand=True, padx=(0, 2))
        txt_b.pack(side="left", fill="both", expand=True, padx=(2, 0))

        def sync_scroll(*args):
            txt_a.yview(*args)
            txt_b.yview(*args)

        def on_scroll(first, last):
            scrollbar.set(first, last)
            txt_a.yview("moveto", first)
            txt_b.yview("moveto", first)

        scrollbar.config(command=sync_scroll)
        txt_a.config(yscrollcommand=on_scroll)
        txt_b.config(yscrollcommand=on_scroll)

        def comparar():
            nombre_a = combo_a.get()
            nombre_b = combo_b.get()
            if not nombre_a or not nombre_b:
                return
            if nombre_a == nombre_b:
                messagebox.showwarning("Coherencia", "Selecciona dos árboles distintos.", parent=dlg)
                return

            pestaña_a = self._pestaña_por_nombre(nombre_a)
            pestaña_b = self._pestaña_por_nombre(nombre_b)
            if not pestaña_a or not pestaña_b:
                return

            ids_a = self._extraer_identificadores(pestaña_a)
            ids_b = self._extraer_identificadores(pestaña_b)

            solo_a = sorted(ids_a - ids_b)
            solo_b = sorted(ids_b - ids_a)

            lbl_cab_a.config(text=f"En '{nombre_a}' pero no en '{nombre_b}' ({len(solo_a)})")
            lbl_cab_b.config(text=f"En '{nombre_b}' pero no en '{nombre_a}' ({len(solo_b)})")

            txt_a.config(state="normal")
            txt_a.delete("1.0", "end")
            txt_a.insert("1.0", "\n".join(solo_a) if solo_a else "(ninguno)")
            txt_a.config(state="disabled")

            txt_b.config(state="normal")
            txt_b.delete("1.0", "end")
            txt_b.insert("1.0", "\n".join(solo_b) if solo_b else "(ninguno)")
            txt_b.config(state="disabled")

        ttk.Button(frame_sel, text="Comparar", command=comparar).pack(side="left", padx=4)

        # --- Botón cerrar ---
        ttk.Button(dlg, text="Cerrar", command=dlg.destroy).pack(pady=(0, 8))


def main() -> None:
    """Punto de entrada para el script ``editor_de_arboles``."""
    parser = argparse.ArgumentParser(description="Editor de ficheros .tree")
    parser.add_argument(
        "--ruta-base", type=Path, default=Path("data"),
        help="Ruta base de datos (por defecto: data)",
    )
    args = parser.parse_args()
    EditorTree(args.ruta_base).mainloop()


if __name__ == "__main__":
    main()
