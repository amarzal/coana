"""CoAna — Aplicación Streamlit principal.

Ejecutar con:
    uv run coana
"""

import importlib as _importlib
from pathlib import Path

import polars as pl
import streamlit as st
import streamlit_antd_components as sac

# Recargar módulos de biblioteca para que Streamlit recoja cambios de código
# sin necesidad de reiniciar el servidor.
import coana.util.excel_cache as _m_excel_cache
import coana.util as _m_util
_importlib.reload(_m_excel_cache)
_importlib.reload(_m_util)

# ============================================================
# Configuración de página
# ============================================================

st.set_page_config(
    page_title="CoAna",
    page_icon="📊",
    layout="wide",
)

DIR_ENTRADA = Path("data/entrada")

# Columnas de ubicaciones que identifican una zona.
_ZONA = ["área", "edificio"]

# ============================================================
# Helpers
# ============================================================


@st.cache_data
def _load_excel(path: str) -> pl.DataFrame:
    from coana.util import read_excel

    return read_excel(path)


def _fmt_m2(v: float) -> str:
    """Formatea metros cuadrados en notación europea."""
    return f"{v:,.2f} m²".replace(",", "X").replace(".", ",").replace("X", ".")


def _enriquecer_per_id(df: pl.DataFrame) -> pl.DataFrame:
    """Si *df* tiene columna ``per_id``, añade ``persona`` justo después."""
    if "per_id" not in df.columns:
        return df
    path = DIR_ENTRADA / "nóminas" / "personas.xlsx"
    if not path.exists():
        return df
    personas = _load_excel(str(path))
    personas = personas.select(
        pl.col("per_id"),
        pl.concat_str(
            [pl.col("nombre"), pl.col("apellido1"), pl.col("apellido2")],
            separator=" ",
            ignore_nulls=True,
        ).alias("persona"),
    )
    idx = df.columns.index("per_id") + 1
    df = df.join(personas, on="per_id", how="left")
    # Reordenar para que «persona» quede justo tras «per_id».
    cols = df.columns.copy()
    cols.remove("persona")
    cols.insert(idx, "persona")
    return df.select(cols)


def _enriquecer_titulación(df: pl.DataFrame) -> pl.DataFrame:
    """Si *df* tiene columna ``titulación``, añade ``nombre_titulación`` buscando en grados y másteres."""
    if "titulación" not in df.columns:
        return df
    partes: list[pl.DataFrame] = []
    for fichero, col_id in [
        ("docencia/grados.xlsx", "grado"),
        ("docencia/másteres.xlsx", "máster"),
    ]:
        path = DIR_ENTRADA / fichero
        if path.exists():
            t = _load_excel(str(path)).select(
                pl.col(col_id).alias("titulación"),
                pl.col("nombre").alias("nombre_titulación"),
            )
            partes.append(t)
    if not partes:
        return df
    lookup = pl.concat(partes).unique(subset=["titulación"], keep="first")
    idx = df.columns.index("titulación") + 1
    df = df.join(lookup, on="titulación", how="left")
    cols = df.columns.copy()
    cols.remove("nombre_titulación")
    cols.insert(idx, "nombre_titulación")
    return df.select(cols)


# ============================================================
# Lookups: campo → (fichero, columna_clave, columnas_extra)
# ============================================================

_LOOKUPS: dict[str, tuple[str, str, list[str]]] = {
    # Presupuesto
    "aplicación": ("presupuesto/aplicaciones de gasto.xlsx", "aplicación", ["nombre"]),
    "programa": ("presupuesto/programas presupuestarios.xlsx", "programa", ["nombre"]),
    "centro": ("presupuesto/centros.xlsx", "centro", ["nombre"]),
    "proyecto": ("presupuesto/proyectos.xlsx", "proyecto", ["nombre", "tipo"]),
    "línea": ("presupuesto/líneas de financiación.xlsx", "línea", ["nombre"]),
    "capítulo": ("presupuesto/capítulos de gasto.xlsx", "capítulo", ["nombre"]),
    "artículo": ("presupuesto/artículos de gasto.xlsx", "artículo", ["nombre"]),
    "concepto": ("presupuesto/conceptos de gasto.xlsx", "concepto", ["nombre"]),
    "tipo_proyecto": ("presupuesto/tipos de proyecto.xlsx", "tipo", ["nombre"]),
    "tipo_línea": ("presupuesto/tipos de línea.xlsx", "tipo", ["nombre"]),
    # Superficies
    "tipo_ubicación": ("superficies/tipos de ubicación.xlsx", "tipo_ubicación", ["descripción"]),
    "área": ("superficies/complejos.xlsx", "complejo", ["descripción"]),
    # Superficies
    "servicio": ("inventario/servicios.xlsx", "servicio", ["nombre", "centro"]),
    "centro_plaza": ("inventario/servicios.xlsx", "servicio", ["nombre", "centro"]),
    # Amortizaciones
    "cuenta": ("inventario/años amortización por cuenta.xlsx", "cuenta", ["nombre", "años_amortización"]),
    # Nóminas
    # Docencia
    "grado": ("docencia/grados.xlsx", "grado", ["nombre"]),
    "máster": ("docencia/másteres.xlsx", "máster", ["nombre"]),
    # Nóminas
    "concepto_retributivo": ("nóminas/conceptos retributivos.xlsx", "concepto_retributivo", ["nombre"]),
    "perceptor": ("nóminas/perceptores.xlsx", "perceptor", ["nombre"]),
    "provisión": ("nóminas/provisiones.xlsx", "provisión", ["nombre"]),
    "categoría": ("nóminas/categorías recursos humanos.xlsx", "categoría", ["nombre", "sector"]),
    "tipo_cargo": ("nóminas/tipos cargo.xlsx", "tipo_cargo", ["nombre"]),
    "per_id": ("nóminas/personas.xlsx", "per_id", ["nombre", "apellido1", "apellido2"]),
}

# Campos cuyas columnas extra se concatenan en un único valor «nombre».
_LOOKUP_CONCAT: dict[str, list[str]] = {
    "per_id": ["nombre", "apellido1", "apellido2"],
}

# Campos que se buscan secuencialmente en varias tablas.
# campo → lista de (fichero, columna_clave, columnas_extra)
_LOOKUP_MULTI: dict[str, list[tuple[str, str, list[str]]]] = {
    "titulación": [
        ("docencia/grados.xlsx", "grado", ["nombre"]),
        ("docencia/másteres.xlsx", "máster", ["nombre"]),
    ],
}


def _lookup(campo: str, valor) -> list[tuple[str, str]] | None:
    """Busca información adicional para un campo/valor en tablas de referencia.

    Devuelve lista de (nombre_campo, valor) o None.
    """
    if valor is None or str(valor).strip() == "":
        return None
    if campo in _LOOKUP_MULTI:
        for fichero, col_clave, cols_extra in _LOOKUP_MULTI[campo]:
            path = DIR_ENTRADA / fichero
            if not path.exists():
                continue
            try:
                df_ref = _load_excel(str(path))
                fila = df_ref.filter(pl.col(col_clave).cast(pl.Utf8) == str(valor))
                if not fila.is_empty():
                    partes = []
                    for col in cols_extra:
                        if col in fila.columns:
                            v = fila[col][0]
                            if v is not None:
                                partes.append((col, str(v)))
                    if partes:
                        return partes
            except Exception:
                continue
        return None
    if campo not in _LOOKUPS:
        return None
    fichero, col_clave, cols_extra = _LOOKUPS[campo]
    path = DIR_ENTRADA / fichero
    if not path.exists():
        return None
    try:
        df_ref = _load_excel(str(path))
        fila = df_ref.filter(pl.col(col_clave).cast(pl.Utf8) == str(valor))
        if fila.is_empty():
            return None
        if campo in _LOOKUP_CONCAT:
            trozos = []
            for col in _LOOKUP_CONCAT[campo]:
                if col in fila.columns:
                    v = fila[col][0]
                    if v is not None:
                        trozos.append(str(v))
            return [("nombre", " ".join(trozos))] if trozos else None
        partes = []
        for col in cols_extra:
            if col in fila.columns:
                v = fila[col][0]
                if v is not None:
                    partes.append((col, str(v)))
        return partes if partes else None
    except Exception:
        return None


# ============================================================
# Ayuda contextual por sección
# ============================================================

_AYUDA: dict[str, str] = {
    # -- Entradas --
    "Entradas": (
        "Permite inspeccionar los ficheros de datos de entrada almacenados en "
        "`data/entrada/`. Los ficheros se agrupan por subdirectorio y cada "
        "entrada abre la visualización del fichero correspondiente.\n\n"
        "Los ficheros `.tree` se muestran como árbol jerárquico desplegable. "
        "Los ficheros `.xlsx` se presentan como tablas con filtrado avanzado "
        "y ficha de registro al seleccionar una fila."
    ),
    # -- Presupuesto --
    "Presupuesto — Resumen": (
        "Panel de métricas global: UC generadas, importe total, "
        "apuntes filtrados, apuntes sin clasificar y nodos de actividades "
        "antes y después del procesamiento."
    ),
    "Presupuesto — Unidades de coste": (
        "UC generadas a partir del presupuesto. "
        "La tabla se puede filtrar por texto y por columna, ordenar por "
        "cualquier columna y permite seleccionar filas para ver el detalle."
    ),
    "Presupuesto — Sin clasificar": (
        "Apuntes presupuestarios que no han podido clasificarse como "
        "unidades de coste, con su conteo e importe total."
    ),
    "Presupuesto — Apuntes filtrados": (
        "Apuntes eliminados en el filtro previo, agrupados por motivo de "
        "exclusión. Resumen por motivo y tabla completa con filtrado."
    ),
    "Presupuesto — Suministros": (
        "UC generadas por distribución de suministros (energía, agua, gas). "
        "Desglose por tipo de suministro y resumen por centro de coste."
    ),
    "Presupuesto — Distribución mantenimientos OTOP": (
        "Distribución de los costes de mantenimiento OTOP entre centros de "
        "coste usando la presencia superficial de cada centro en las zonas, "
        "edificaciones y complejos del campus."
    ),
    "Presupuesto — Reglas de actividad": (
        "Efectividad de las reglas de clasificación de actividad. "
        "Para cada regla: nombre, número de apuntes clasificados e importe. "
        "Las reglas con cero coincidencias se resaltan en rojo."
    ),
    "Presupuesto — Reglas de centro de coste": (
        "Efectividad de las reglas de clasificación de centro de coste. "
        "Para cada regla: nombre, número de apuntes clasificados e importe. "
        "Las reglas con cero coincidencias se resaltan en rojo."
    ),
    "Presupuesto — Reglas de elemento de coste": (
        "Efectividad de las reglas de clasificación de elemento de coste. "
        "Para cada regla: nombre, número de apuntes clasificados e importe. "
        "Las reglas con cero coincidencias se resaltan en rojo."
    ),
    "Presupuesto — Árbol: Actividades": (
        "Comparación del árbol original de actividades con el modificado "
        "tras la aplicación de reglas. Los nodos nuevos se resaltan en verde."
    ),
    "Presupuesto — Árbol: Centros de coste": (
        "Comparación del árbol original de centros de coste con el modificado "
        "tras la aplicación de reglas. Los nodos nuevos se resaltan en verde."
    ),
    "Presupuesto — Árbol: Elementos de coste": (
        "Comparación del árbol original de elementos de coste con el "
        "modificado tras la aplicación de reglas. Los nodos nuevos se "
        "resaltan en verde."
    ),
    # -- Amortizaciones --
    "Amortizaciones — Resumen": (
        "Panel de métricas global: registros originales, enriquecidos, "
        "importe total de amortización y registros filtrados (por estado, "
        "cuenta, fecha, sin cuenta y sin fecha de alta)."
    ),
    "Amortizaciones — Inventario con amortización": (
        "Inventario enriquecido con los datos de amortización calculados, "
        "incluyendo un resumen de los motivos de exclusión."
    ),
    "Amortizaciones — Filtrados por estado": (
        "Registros de inventario excluidos por tener estado de baja."
    ),
    "Amortizaciones — Filtrados por cuenta": (
        "Registros de inventario excluidos por cuenta contable no válida. "
        "Permite seleccionar una cuenta concreta para ver el detalle."
    ),
    "Amortizaciones — Filtrados por fecha": (
        "Registros de inventario excluidos por estar fuera del período "
        "de amortización."
    ),
    "Amortizaciones — Sin cuenta": (
        "Registros de inventario que no tienen cuenta contable asignada."
    ),
    "Amortizaciones — Por cuenta": (
        "Vista jerárquica: resumen por cuenta → selección de cuenta → "
        "registros individuales → UC generadas a partir de ese registro."
    ),
    "Amortizaciones — UC generadas": (
        "Todas las UC de origen inventario. Al seleccionar una UC se muestra "
        "el registro de inventario del que procede."
    ),
    "Amortizaciones — Sin centro": (
        "Registros de inventario cuya UC no ha podido asignarse a un "
        "centro de coste."
    ),
    # -- Personal --
    "Personal — Resumen": (
        "Métricas globales de nóminas: expedientes totales e importe "
        "total, con desglose por categoría de personal (PDI, PTGAS, "
        "PVI, Otros)."
    ),
    "Personal — Expedientes PDI": (
        "Expedientes del Personal Docente e Investigador. "
        "Métricas, tabla filtrable, desglose por costes sociales, "
        "retribuciones (docencia, gestión, investigación, incentivos) "
        "y carga docente."
    ),
    "Personal — Expedientes PTGAS": (
        "Expedientes del Personal Técnico, de Gestión y de Administración "
        "y Servicios. Métricas, tabla filtrable y desglose por "
        "retribuciones ordinarias y extraordinarias."
    ),
    "Personal — Expedientes PVI": (
        "Expedientes del Personal Visitante Investigador. "
        "Métricas, tabla filtrable y desglose por fondos UJI frente "
        "a financiación afectada."
    ),
    "Personal — Expedientes otros": (
        "Expedientes de personal no clasificado en las categorías "
        "anteriores (PDI, PTGAS, PVI)."
    ),
    "Personal — Anomalías PDI": (
        "Asignaturas cuya titulación no se encuentra en las tablas de "
        "referencia (grados, másteres, estudios). Muestra el número de "
        "asignaturas afectadas, el profesorado implicado y el porcentaje "
        "de créditos sin titulación conocida."
    ),
    # -- Superficies --
    "Superficies — Resumen": (
        "Panel de métricas global: superficie total en m², número de "
        "complejos, edificaciones, zonas y ubicaciones del campus."
    ),
    "Superficies — Totales": (
        "Verificación de coherencia de las superficies a cuatro niveles "
        "jerárquicos: zonas, edificaciones, complejos y campus. "
        "Selectores en cascada con métricas de m² y tablas de desglose."
    ),
    "Superficies — Presencia centros": (
        "Presencia de cada centro de coste en el espacio físico. "
        "Dos modos: por centro (distribución en complejos, edificaciones "
        "y zonas) y por nivel (qué centros tienen presencia en un "
        "elemento concreto)."
    ),
    # -- Resultados --
    "Resultados — Resumen": (
        "Métricas globales de la Fase 1: total de unidades de coste e "
        "importe total, con desglose por origen (presupuesto, "
        "amortizaciones, nóminas, suministros)."
    ),
    "Resultados — Todas las UC": (
        "Visión consolidada de todas las unidades de coste generadas por "
        "la Fase 1, combinando presupuesto, amortizaciones, nóminas y "
        "suministros. Filtrado por texto, ordenación y detalle por fila."
    ),
    "Resultados — Actividades": (
        "Para cada nodo del árbol de actividades: importe desglosado por "
        "origen (presupuesto, amortizaciones, nóminas, suministros) y total. "
        "Selección de nodo para drill-down a las UC individuales."
    ),
    "Resultados — Centros de coste": (
        "Para cada nodo del árbol de centros de coste: importe desglosado "
        "por origen (presupuesto, amortizaciones, nóminas, suministros) "
        "y total. Selección de nodo para drill-down a las UC individuales."
    ),
    "Resultados — Elementos de coste": (
        "Para cada nodo del árbol de elementos de coste: importe desglosado "
        "por origen (presupuesto, amortizaciones, nóminas, suministros) "
        "y total. Selección de nodo para drill-down a las UC individuales."
    ),
    "Resultados — Anomalías UC": (
        "Comprobación de integridad referencial: UC que referencian nodos "
        "inexistentes en los árboles finales. Resumen por fuente y tipo "
        "de anomalía, con conteo e importe afectado."
    ),
}


def _título(texto: str, clave_ayuda: str | None = None) -> None:
    """Renderiza un título con popover de ayuda contextual opcional."""
    ayuda = _AYUDA.get(clave_ayuda or texto)
    if ayuda:
        _t, _h = st.columns([0.94, 0.06])
        with _t:
            st.title(texto)
        with _h:
            st.write("")
            with st.popover("?"):
                st.markdown(ayuda)
    else:
        st.title(texto)


# ============================================================
# Ficha de registro (dialog modal)
# ============================================================


def _ficha_registro(df: pl.DataFrame, row_idx: int, key_suffix: str = "") -> None:
    """Muestra todos los campos de una fila en formato ficha inline."""
    _BG = ["#f0f6ff", "#ffffff"]
    row = df.row(row_idx, named=True)

    st.divider()
    hdr1, hdr2 = st.columns([6, 1])
    hdr1.subheader(f"Registro {row_idx + 1}")
    with hdr2:
        if st.button("✕ Cerrar", key=f"ficha_cerrar{key_suffix}", type="tertiary"):
            st.session_state._df_version = (
                st.session_state.get("_df_version", 0) + 1
            )
            st.rerun()

    filas_html: list[str] = []
    i = 0
    for campo, valor in row.items():
        if valor is None:
            continue
        info = _lookup(campo, valor)
        bg = _BG[i % 2]
        if info:
            extras = " · ".join(
                f"<b>{k}</b>: {v}" for k, v in info
            )
            texto_valor = (
                f"{valor} <span style='color:#666;font-style:italic'>{extras}</span>"
            )
        else:
            texto_valor = str(valor)
        filas_html.append(
            f"<div style='display:flex;gap:1em;padding:3px 10px;"
            f"background:{bg};border-radius:3px;font-size:0.9em;line-height:1.4'>"
            f"<div style='min-width:25%;font-weight:600;color:#444'>{campo}</div>"
            f"<div>{texto_valor}</div>"
            f"</div>"
        )
        i += 1
    st.html("".join(filas_html))


# ============================================================
# Estado de navegación
# ============================================================

if "vista" not in st.session_state:
    st.session_state.vista = None
if "fichero_sel" not in st.session_state:
    st.session_state.fichero_sel = None
if "sup_seccion" not in st.session_state:
    st.session_state.sup_seccion = "Resumen"
if "amort_seccion" not in st.session_state:
    st.session_state.amort_seccion = "Resumen"
if "pres_seccion" not in st.session_state:
    st.session_state.pres_seccion = "Resumen"
if "personal_seccion" not in st.session_state:
    st.session_state.personal_seccion = "Resumen"
if "resultados_seccion" not in st.session_state:
    st.session_state.resultados_seccion = "Resumen"


def _ir_a(vista: str, fichero: str | None = None) -> None:
    st.session_state.vista = vista
    if fichero is not None:
        st.session_state.fichero_sel = fichero


def _ir_a_sup(seccion: str) -> None:
    st.session_state.vista = "superficies"
    st.session_state.sup_seccion = seccion


def _ir_a_amort(seccion: str) -> None:
    st.session_state.vista = "amortizaciones"
    st.session_state.amort_seccion = seccion


def _ir_a_pres(seccion: str) -> None:
    st.session_state.vista = "presupuesto"
    st.session_state.pres_seccion = seccion


def _ir_a_personal(seccion: str) -> None:
    st.session_state.vista = "personal"
    st.session_state.personal_seccion = seccion


def _ir_a_resultados(seccion: str) -> None:
    st.session_state.vista = "resultados"
    st.session_state.resultados_seccion = seccion


# ============================================================
# Sidebar
# ============================================================


def _scan_entradas() -> dict[str, list[Path]]:
    grupos: dict[str, list[Path]] = {}
    if not DIR_ENTRADA.exists():
        return grupos
    for subdir in sorted(DIR_ENTRADA.iterdir()):
        if not subdir.is_dir() or subdir.name.startswith("_"):
            continue
        ficheros = sorted(
            f
            for f in subdir.iterdir()
            if f.is_file()
            and not f.name.startswith("~$")
            and f.suffix in (".xlsx", ".tree")
        )
        if ficheros:
            grupos[subdir.name] = ficheros
    return grupos


# ── Definición de secciones de navegación ──────────────────
_PRES_SECCIONES = [
    "Resumen",
    "Unidades de coste", "Sin clasificar", "Apuntes filtrados",
    "Suministros", "Distribución mantenimientos OTOP",
    "Reglas de actividad", "Reglas de centro de coste", "Reglas de elemento de coste",
    "Árbol: Actividades", "Árbol: Centros de coste", "Árbol: Elementos de coste",
]
_AMORT_SECCIONES = [
    "Resumen",
    "Inventario con amortización",
    "Filtrados por estado",
    "Filtrados por cuenta",
    "Filtrados por fecha",
    "Sin cuenta",
    "Por cuenta",
    "UC generadas",
    "Sin centro",
]
_PERSONAL_SECCIONES = [
    "Resumen",
    "Expedientes PDI", "Expedientes PTGAS", "Expedientes PVI", "Expedientes otros",
    "Multiexpediente",
    "Persona",
    "Anomalías PDI",
]
_SUP_SECCIONES = ["Resumen", "Totales", "Presencia centros"]
_RESULTADOS_SECCIONES = [
    "Resumen",
    "Todas las UC",
    "Actividades", "Centros de coste", "Elementos de coste",
    "Anomalías UC",
]

# Construir un único árbol de navegación y el mapa índice → (vista, sección).
# ParseItems.multi() asigna índices secuenciales en DFS, incluyendo padres.
_NAV_SECTIONS: list[tuple[str, str, list[str], callable]] = [
    ("Presupuesto",      "presupuesto",    _PRES_SECCIONES,       _ir_a_pres),
    ("Amortizaciones",   "amortizaciones", _AMORT_SECCIONES,      _ir_a_amort),
    ("Personal",         "personal",       _PERSONAL_SECCIONES,   _ir_a_personal),
    ("Superficies",      "superficies",    _SUP_SECCIONES,        _ir_a_sup),
    ("Resultados Fase 1","resultados",     _RESULTADOS_SECCIONES, _ir_a_resultados),
]

_nav_items: list[sac.TreeItem] = []
_nav_idx_map: dict[int, callable] = {}   # índice → lambda que navega
_nav_open_indices: list[int] = []        # índices de padres a desplegar
_idx = 0

# ── Entradas (dinámico, escaneado del disco) ───────────────
_grupos_entrada = _scan_entradas()
if _grupos_entrada:
    _entradas_parent_idx = _idx
    _idx += 1
    _entradas_hijos: list[sac.TreeItem] = []
    for _subdir, _ficheros in _grupos_entrada.items():
        _subdir_idx = _idx
        _idx += 1
        _fichero_items: list[sac.TreeItem] = []
        for _f in _ficheros:
            _path = str(_f)
            _nav_idx_map[_idx] = lambda p=_path: _ir_a("entradas", p)
            _fichero_items.append(sac.TreeItem(_f.stem))
            _idx += 1
        _entradas_hijos.append(sac.TreeItem(_subdir, children=_fichero_items, disabled=True))
    _nav_items.append(sac.TreeItem("Entradas", children=_entradas_hijos, disabled=True))
    if st.session_state.get("vista") == "entradas":
        _nav_open_indices.append(_entradas_parent_idx)

# ── Secciones fijas ────────────────────────────────────────
for _grupo, _vista_id, _secciones, _navegar in _NAV_SECTIONS:
    _parent_idx = _idx
    _idx += 1
    hijos = []
    for _s in _secciones:
        _fn = _navegar
        _sec = _s
        _nav_idx_map[_idx] = lambda fn=_fn, sec=_sec: fn(sec)
        hijos.append(sac.TreeItem(_s))
        _idx += 1
    _nav_items.append(sac.TreeItem(_grupo, children=hijos, disabled=True))
    if st.session_state.get("vista") == _vista_id:
        _nav_open_indices.append(_parent_idx)

with st.sidebar:
    if st.button("Ejecutar Fase 1", use_container_width=True, type="primary"):
        st.session_state._ejecutar_fase1 = True

    # ── Navegación principal (un solo tree) ────────────────────
    sel_nav = sac.tree(
        items=_nav_items,
        return_index=True,
        open_index=_nav_open_indices or None,
        show_line=False,
        size="sm",
        key="tree_nav",
    )

    # Despacho: solo navegar cuando el índice cambia
    _nav_sel_idx = (sel_nav[0] if isinstance(sel_nav, list) else sel_nav) if sel_nav else None
    _nav_prev = st.session_state.get("_prev_nav_idx")
    if _nav_sel_idx is not None and _nav_sel_idx != _nav_prev and _nav_sel_idx in _nav_idx_map:
        _nav_idx_map[_nav_sel_idx]()
    st.session_state["_prev_nav_idx"] = _nav_sel_idx

# -- Ejecución de Fase 1 (fuera del sidebar) --
if st.session_state.get("_ejecutar_fase1"):
    st.session_state._ejecutar_fase1 = False
    import contextlib

    class _LiveLog:
        """File-like que actualiza un placeholder de Streamlit en cada write."""

        def __init__(self, placeholder, status):
            self._ph = placeholder
            self._status = status
            self._lines: list[str] = []

        def write(self, text: str) -> int:
            if not text or text == "\n":
                return len(text) if text else 0
            for line in text.splitlines():
                line = line.rstrip()
                if not line:
                    continue
                self._lines.append(line)
                # Actualizar etiqueta del status con la última línea principal
                if not line.startswith(" "):
                    self._status.update(label=line)
            self._ph.code("\n".join(self._lines), language=None)
            return len(text)

        def flush(self) -> None:
            pass

        def getvalue(self) -> str:
            return "\n".join(self._lines)

    with st.status("Ejecutando Fase 1…", expanded=True) as status:
        _log_placeholder = st.empty()
        _live = _LiveLog(_log_placeholder, status)
        try:
            # Recargar módulos para recoger cambios de código sin reiniciar Streamlit
            import importlib
            import coana.fase1.presupuesto.contexto as _m_ctx
            import coana.fase1.presupuesto.traductor as _m_trad
            import coana.fase1.presupuesto as _m_pres
            import coana.fase1.inventario.contexto as _m_inv_ctx
            import coana.fase1.inventario.procesamiento as _m_proc
            import coana.fase1.inventario as _m_inv
            import coana.fase1.suministros as _m_sum
            import coana.fase1.amortizaciones as _m_amort
            import coana.fase1.nóminas.contexto as _m_nom_ctx
            import coana.fase1.nóminas as _m_nom
            import coana.fase1 as _m_fase1
            for _m in [_m_ctx, _m_trad, _m_pres,
                       _m_inv_ctx, _m_proc, _m_inv,
                       _m_sum, _m_amort, _m_nom_ctx, _m_nom, _m_fase1]:
                importlib.reload(_m)
            ejecutar = _m_fase1.ejecutar
            with contextlib.redirect_stdout(_live):
                ejecutar()
            # Limpiar todas las cachés de Streamlit para que se recarguen
            # los datos generados por la fase 1.
            st.cache_data.clear()
            status.update(label="Fase 1 completada", state="complete", expanded=False)
            st.toast("Fase 1 ejecutada correctamente", icon="✅")
        except Exception as e:
            import traceback
            status.update(label="Error en Fase 1", state="error", expanded=True)
            st.error(f"Error: {e}")
            st.code(traceback.format_exc(), language=None)


# ============================================================
# Panel principal: Visor de ficheros .tree
# ============================================================


def _mostrar_arbol(path: Path) -> None:
    """Visualiza un fichero .tree como árbol interactivo o tabla filtrada."""
    from coana.util import Árbol, NodoÁrbol

    árbol = Árbol.from_file(path)

    grupo_sel = path.parent.name
    st.title(path.stem)
    st.caption(f"Grupo: {grupo_sel}  ·  {path.name}")

    # Métricas
    def _contar(nodo: NodoÁrbol) -> tuple[int, int]:
        """Devuelve (nº nodos, profundidad máxima) bajo este nodo."""
        if not nodo.hijos:
            return (1, 0)
        total = 1
        prof_max = 0
        for h in nodo.hijos:
            n, p = _contar(h)
            total += n
            prof_max = max(prof_max, p + 1)
        return total, prof_max

    n_nodos, prof_max = _contar(árbol.raíz)
    n_nodos -= 1  # sin contar la raíz virtual
    c1, c2 = st.columns(2)
    c1.metric("Nodos", f"{n_nodos:,}")
    c2.metric("Profundidad máxima", f"{prof_max}")

    # Filtros
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        filtro_código = st.text_input("Código", key="tree_filtro_código")
    with fc2:
        filtro_desc = st.text_input("Descripción", key="tree_filtro_desc")
    with fc3:
        filtro_etiqueta = st.text_input("Etiqueta", key="tree_filtro_etiqueta")

    hay_filtro = bool(filtro_código or filtro_desc or filtro_etiqueta)

    if hay_filtro:
        # Aplanar a tabla y filtrar
        filas: list[dict[str, str | int]] = []

        def _aplanar(nodo: NodoÁrbol) -> None:
            if nodo is not árbol.raíz:
                filas.append({
                    "código": nodo.código,
                    "descripción": nodo.descripción,
                    "identificador": nodo.identificador,
                    "nivel": nodo.código.count("."),
                })
            for h in nodo.hijos:
                _aplanar(h)

        _aplanar(árbol.raíz)

        df = pl.DataFrame(filas)
        if filtro_código:
            df = df.filter(
                pl.col("código").str.to_lowercase().str.contains(
                    filtro_código.lower(), literal=True
                )
            )
        if filtro_desc:
            df = df.filter(
                pl.col("descripción").str.to_lowercase().str.contains(
                    filtro_desc.lower(), literal=True
                )
            )
        if filtro_etiqueta:
            df = df.filter(
                pl.col("identificador").str.to_lowercase().str.contains(
                    filtro_etiqueta.lower(), literal=True
                )
            )

        st.caption(f"{len(df):,} nodos encontrados")
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        # Árbol HTML interactivo
        def _nodo_html(nodo: NodoÁrbol) -> str:
            código_span = (
                f"<span style='color:#888;font-family:monospace'>"
                f"{nodo.código}</span>"
            )
            etiqueta_span = (
                f"<span style='color:#c06;font-family:monospace;"
                f"font-size:0.9em'>{nodo.identificador}</span>"
            )
            label = f"{código_span} {nodo.descripción} {etiqueta_span}"

            if nodo.hijos:
                hijos_html = "".join(_nodo_html(h) for h in nodo.hijos)
                return (
                    f"<details open>"
                    f"<summary>{label}</summary>"
                    f"<div style='margin-left:1.2em'>{hijos_html}</div>"
                    f"</details>"
                )
            return (
                f"<div style='padding:2px 0'>{label}</div>"
            )

        css = (
            "<style>"
            "details { margin: 1px 0; }"
            "summary { cursor: pointer; padding: 2px 0; }"
            "summary:hover { background: #f0f4ff; }"
            "</style>"
        )
        cuerpo = "".join(_nodo_html(h) for h in árbol.raíz.hijos)
        st.html(f"{css}<div style='font-size:0.92em'>{cuerpo}</div>")


# ============================================================
# Panel principal: Entradas (visor de fichero)
# ============================================================


def _mostrar_entradas():
    sel = st.session_state.fichero_sel
    if sel is None:
        _título("Ficheros de entrada", "Entradas")
        st.info("Selecciona un fichero en el panel lateral.")
        return

    path_sel = Path(sel)
    if not path_sel.exists():
        st.error(f"Fichero no encontrado: `{sel}`")
        return

    if path_sel.suffix == ".tree":
        _mostrar_arbol(path_sel)
        return

    grupo_sel = path_sel.parent.name
    st.title(path_sel.stem)
    st.caption(f"Grupo: {grupo_sel}  ·  {path_sel.name}")

    df = _load_excel(sel)

    c1, c2 = st.columns(2)
    c1.metric("Filas", f"{len(df):,}")
    c2.metric("Columnas", f"{len(df.columns):,}")

    fc1, fc2, fc3, fc4 = st.columns([4, 0.3, 1, 1])
    with fc1:
        filtro = st.text_input(
            "Filtrar",
            key="filtro_texto_entradas",
        )
    with fc2:
        st.markdown("")  # alinear verticalmente
        st.button(
            "✕",
            key="filtro_clear",
            type="tertiary",
            on_click=lambda: st.session_state.__setitem__(
                "filtro_texto_entradas", ""
            ),
        )
    with fc3:
        col_filtro = st.selectbox(
            "Columna",
            options=["(todas)"] + df.columns,
            key="filtro_col",
        )
    with fc4:
        st.markdown("")  # alinear verticalmente
        opciones_filtro = sac.chip(
            items=[
                sac.ChipItem(icon="type"),
                sac.ChipItem(icon="braces-asterisk"),
                sac.ChipItem(icon="textarea-t"),
            ],
            multiple=True,
            return_index=True,
            size="xs",
            radius="sm",
            variant="outline",
            key="filtro_opts",
        )
    _sel = opciones_filtro or []
    opt_case = 0 in _sel
    opt_regex = 1 in _sel
    opt_palabra = 2 in _sel

    if filtro:
        if opt_regex:
            patrón = filtro if opt_case else f"(?i){filtro}"
        elif opt_palabra:
            esc = filtro.replace("\\", "\\\\").replace(".", "\\.").replace("*", "\\*")
            patrón = f"(?:^|\\W){esc}(?:\\W|$)"
            if not opt_case:
                patrón = f"(?i){patrón}"
        else:
            patrón = None

        cols_filtro = df.columns if col_filtro == "(todas)" else [col_filtro]
        mask = pl.lit(False)
        for col in cols_filtro:
            col_str = pl.col(col).cast(pl.Utf8)
            if patrón is not None:
                mask = mask | col_str.str.contains(patrón, literal=False)
            else:
                if not opt_case:
                    mask = mask | col_str.str.to_lowercase().str.contains(
                        filtro.lower(), literal=True
                    )
                else:
                    mask = mask | col_str.str.contains(filtro, literal=True)
        df = df.filter(mask)

    st.caption(f"Mostrando {len(df):,} filas")
    _v = st.session_state.get("_df_version", 0)
    sel = _st_df(
        df,
        on_select="rerun",
        selection_mode="single-row",
        key=f"entradas_df_{_v}",
    )
    if sel and sel.selection.rows and sel.selection.rows[0] < len(df):
        _ficha_registro(df, sel.selection.rows[0])


# ============================================================
# Panel principal: Superficies
# ============================================================


@st.cache_data
def _load_superficies() -> tuple[
    pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame,
    pl.DataFrame, pl.DataFrame, pl.DataFrame,
]:
    dir_sup = DIR_ENTRADA / "superficies"
    ubicaciones = _load_excel(str(dir_sup / "ubicaciones.xlsx"))
    complejos = _load_excel(str(dir_sup / "complejos.xlsx"))
    edificaciones = _load_excel(str(dir_sup / "edificaciones.xlsx"))
    zonas = _load_excel(str(dir_sup / "zonas.xlsx"))
    tipos = _load_excel(str(dir_sup / "tipos de ubicación.xlsx"))
    dir_inv = DIR_ENTRADA / "inventario"
    ubic_serv = _load_excel(str(dir_inv / "ubicaciones a servicios.xlsx"))
    servicios = _load_excel(str(dir_inv / "servicios.xlsx"))
    dir_consumos = DIR_ENTRADA / "consumos"
    dist_otop = _load_excel(
        str(dir_consumos / "distribución OTOP.xlsx")
    )
    return ubicaciones, complejos, edificaciones, zonas, tipos, ubic_serv, servicios, dist_otop


@st.cache_data
def _calcular_presencia(
    ubicaciones: pl.DataFrame,
    ubic_serv: pl.DataFrame,
    servicios: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, int, int, int]:
    """Calcula matrices de presencia centro x zona/edificación/complejo/UJI.

    Devuelve (presencia_zona, presencia_edificación, presencia_complejo,
    presencia_uji, n_zonas_sin, n_zonas_con, n_centros).
    """
    # Mapa servicio → centro (solo servicios vivos con centro asignado)
    mapa_centro = (
        servicios.filter(
            (pl.col("vivo") == 1) & pl.col("centro").is_not_null()
        )
        .select("servicio", "centro")
    )

    # Asignación directa de m² a centros
    n_serv_por_ubic = (
        ubic_serv.group_by("ubicación")
        .agg(pl.len().alias("n_servicios"))
    )

    ubic_centro = (
        ubicaciones.join(
            ubic_serv,
            left_on="id_ubicación",
            right_on="ubicación",
            how="inner",
        )
        .join(n_serv_por_ubic, left_on="id_ubicación", right_on="ubicación")
        .join(mapa_centro, on="servicio", how="inner")
        .with_columns(
            (pl.col("metros_cuadrados") / pl.col("n_servicios")).alias("m2_porción"),
        )
    )

    directo = ubic_centro.group_by(_ZONA + ["centro"]).agg(
        pl.col("m2_porción").sum().alias("m2_directo"),
    )

    # m² total y asignado por zona
    total_por_zona = ubicaciones.group_by(_ZONA).agg(
        pl.col("metros_cuadrados").sum().alias("m2_total"),
    )

    ubic_ids_con_servicio = ubic_serv.select("ubicación").unique()
    m2_asignado = (
        ubicaciones.filter(
            pl.col("id_ubicación").is_in(ubic_ids_con_servicio["ubicación"])
        )
        .group_by(_ZONA)
        .agg(pl.col("metros_cuadrados").sum().alias("m2_asignado"))
    )

    m2_zona = (
        total_por_zona.join(m2_asignado, on=_ZONA, how="left")
        .with_columns(pl.col("m2_asignado").fill_null(0))
        .with_columns(
            (pl.col("m2_total") - pl.col("m2_asignado")).alias("m2_sin_asignar"),
        )
    )

    # Redistribución intra-zona (espacios comunes)
    zonas_con_centros_df = directo.select(_ZONA).unique()

    total_directo_por_zona = directo.group_by(_ZONA).agg(
        pl.col("m2_directo").sum().alias("m2_directo_total"),
    )

    redistr_local = (
        directo.join(total_directo_por_zona, on=_ZONA)
        .join(m2_zona.select(_ZONA + ["m2_sin_asignar"]), on=_ZONA)
        .with_columns(
            pl.when(pl.col("m2_directo_total") > 0)
            .then(
                pl.col("m2_directo")
                / pl.col("m2_directo_total")
                * pl.col("m2_sin_asignar")
            )
            .otherwise(0.0)
            .alias("m2_redistribuido"),
        )
        .with_columns(
            (pl.col("m2_directo") + pl.col("m2_redistribuido")).alias(
                "metros_cuadrados"
            ),
        )
        .select(_ZONA + ["centro", "metros_cuadrados"])
    )

    # Redistribución de zonas sin ningún centro
    zonas_sin = m2_zona.join(
        zonas_con_centros_df, on=_ZONA, how="anti",
    )

    n_zonas_sin = len(zonas_sin)
    n_zonas_con = len(zonas_con_centros_df)

    if not zonas_sin.is_empty() and not directo.is_empty():
        total_global = directo.select(pl.col("m2_directo").sum()).item()
        presencia_global = (
            directo.group_by("centro")
            .agg(pl.col("m2_directo").sum().alias("m2_global"))
            .with_columns(
                (pl.col("m2_global") / total_global).alias("proporción"),
            )
        )

        redistr_global = (
            zonas_sin.select(_ZONA + ["m2_total"])
            .join(
                presencia_global.select("centro", "proporción"),
                how="cross",
            )
            .with_columns(
                (pl.col("m2_total") * pl.col("proporción")).alias("metros_cuadrados"),
            )
            .select(_ZONA + ["centro", "metros_cuadrados"])
        )

        centro_zona = pl.concat([redistr_local, redistr_global])
    else:
        centro_zona = redistr_local

    centro_zona = centro_zona.with_columns(
        pl.col("metros_cuadrados").fill_nan(0.0),
    ).filter(pl.col("metros_cuadrados") > 0)

    n_centros = centro_zona.select("centro").unique().height

    # 1. Presencia por zona
    total_zona = centro_zona.group_by(_ZONA).agg(
        pl.col("metros_cuadrados").sum().alias("m2_zona"),
    )
    presencia_zona = (
        centro_zona.join(total_zona, on=_ZONA)
        .with_columns(
            (pl.col("metros_cuadrados") / pl.col("m2_zona") * 100).alias("pct"),
        )
        .rename({"metros_cuadrados": "m2"})
        .select("área", "edificio", "centro", "m2", "pct")
        .sort("área", "edificio", "centro")
    )

    # 2. Presencia por edificación
    centro_edif = (
        centro_zona.with_columns(
            pl.col("edificio").str.slice(0, 1).alias("edificación"),
        )
        .group_by(["área", "edificación", "centro"])
        .agg(pl.col("metros_cuadrados").sum().alias("m2"))
    )
    total_edif = centro_edif.group_by(["área", "edificación"]).agg(
        pl.col("m2").sum().alias("m2_edif"),
    )
    presencia_edificación = (
        centro_edif.join(total_edif, on=["área", "edificación"])
        .with_columns(
            (pl.col("m2") / pl.col("m2_edif") * 100).alias("pct"),
        )
        .select("área", "edificación", "centro", "m2", "pct")
        .sort("área", "edificación", "centro")
    )

    # 3. Presencia por complejo
    centro_complejo = (
        centro_zona.group_by(["área", "centro"])
        .agg(pl.col("metros_cuadrados").sum().alias("m2"))
    )
    total_complejo = centro_complejo.group_by("área").agg(
        pl.col("m2").sum().alias("m2_complejo"),
    )
    presencia_complejo = (
        centro_complejo.join(total_complejo, on="área")
        .with_columns(
            (pl.col("m2") / pl.col("m2_complejo") * 100).alias("pct"),
        )
        .select("área", "centro", "m2", "pct")
        .sort("área", "centro")
    )

    # 4. Presencia en UJI
    centro_uji = (
        centro_zona.group_by("centro")
        .agg(pl.col("metros_cuadrados").sum().alias("m2"))
    )
    total_uji = centro_uji.select(pl.col("m2").sum()).item()
    presencia_uji = (
        centro_uji.with_columns(
            (pl.col("m2") / total_uji * 100).alias("pct"),
        )
        .sort("m2", descending=True)
    )

    return (
        presencia_zona, presencia_edificación, presencia_complejo, presencia_uji,
        n_zonas_sin, n_zonas_con, n_centros,
    )


@st.cache_data
def _calcular_distribución_otop(
    distribución: pl.DataFrame,
    presencia_zona: pl.DataFrame,
    presencia_edificación: pl.DataFrame,
    presencia_complejo: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, list, list]:
    """Calcula distribución de costes OTOP por centro.

    Devuelve (por_centro, detalle, prefijos_duplicados, prefijos_sin_match).
    """
    agregado = (
        distribución.group_by("prefijo")
        .agg(
            pl.col("porcentaje").sum(),
            pl.col("comentario").first(),
            pl.len().alias("n_filas"),
        )
    )
    prefijos_duplicados = [
        (str(row["prefijo"]), row["n_filas"], float(row["porcentaje"]))
        for row in agregado.filter(pl.col("n_filas") > 1).iter_rows(named=True)
    ]

    filas_detalle: list[dict] = []
    prefijos_sin_match: list[tuple[str, float, str]] = []

    for row in agregado.iter_rows(named=True):
        prefijo = str(row["prefijo"]).strip()
        pct_dist = float(row["porcentaje"])
        comentario = str(row.get("comentario") or "")

        if len(prefijo) >= 3:
            área = prefijo[0]
            edificio = prefijo[1:]
            centros_en = presencia_zona.filter(
                (pl.col("área") == área) & (pl.col("edificio") == edificio)
            )
        elif len(prefijo) == 2:
            área = prefijo[0]
            edificación = prefijo[1]
            centros_en = presencia_edificación.filter(
                (pl.col("área") == área) & (pl.col("edificación") == edificación)
            )
        elif len(prefijo) == 1:
            centros_en = presencia_complejo.filter(
                pl.col("área") == prefijo
            )
        else:
            continue

        if centros_en.is_empty():
            prefijos_sin_match.append((prefijo, pct_dist, comentario))
            continue

        for row_c in centros_en.iter_rows(named=True):
            pct_presencia = row_c["pct"]
            contribución = pct_dist * pct_presencia / 100
            filas_detalle.append({
                "centro": row_c["centro"],
                "prefijo": prefijo,
                "comentario": comentario,
                "pct_distribución": pct_dist,
                "pct_presencia": pct_presencia,
                "contribución": contribución,
            })

    if filas_detalle:
        detalle = pl.DataFrame(filas_detalle)
        por_centro = (
            detalle.group_by("centro")
            .agg(pl.col("contribución").sum().alias("porcentaje"))
            .sort("porcentaje", descending=True)
        )

        total_asignado = por_centro["porcentaje"].sum()
        pct_sin_asignar = 1.0 - total_asignado
        if pct_sin_asignar > 1e-9:
            por_centro = por_centro.with_columns(
                (
                    pl.col("porcentaje")
                    + pct_sin_asignar * pl.col("porcentaje") / total_asignado
                ).alias("porcentaje"),
            ).sort("porcentaje", descending=True)
    else:
        detalle = pl.DataFrame(
            schema={
                "centro": pl.Utf8, "prefijo": pl.Utf8, "comentario": pl.Utf8,
                "pct_distribución": pl.Float64, "pct_presencia": pl.Float64,
                "contribución": pl.Float64,
            }
        )
        por_centro = pl.DataFrame(
            schema={"centro": pl.Utf8, "porcentaje": pl.Float64}
        )

    return por_centro, detalle, prefijos_duplicados, prefijos_sin_match


def _mostrar_superficies():
    (
        ubicaciones, complejos_df, edificaciones_df, zonas_df, tipos_df,
        ubic_serv_df, servicios_df, dist_otop_df,
    ) = _load_superficies()

    ub = ubicaciones.with_columns(
        pl.col("área").alias("_complejo"),
        pl.col("edificio").str.slice(0, 1).alias("_edificación"),
        pl.col("edificio").str.slice(1, 1).alias("_zona"),
    )

    seccion = st.session_state.sup_seccion
    _título(f"Superficies — {seccion}")

    total_m2 = float(ub["metros_cuadrados"].sum() or 0)

    if seccion == "Resumen":
        mc1, mc2, mc3, mc4, mc5 = st.columns(5)
        mc1.metric("Total m²", _fmt_m2(total_m2))
        mc2.metric("Complejos", f"{complejos_df.height}")
        mc3.metric("Edificaciones", f"{edificaciones_df.height}")
        mc4.metric("Zonas", f"{zonas_df.height}")
        mc5.metric("Ubicaciones", f"{ub.height:,}")
        return

    # ── Totales: explorar + verificación ─────────────────────
    if seccion == "Totales":
        _sup_totales(ub, total_m2, complejos_df, edificaciones_df, zonas_df, tipos_df)

    # ── Presencia centros ─────────────────────────────────────
    elif seccion == "Presencia centros":
        _sup_presencia(
            ubicaciones, ubic_serv_df, servicios_df,
            complejos_df, edificaciones_df, zonas_df,
        )



def _sup_totales(ub, total_m2, complejos_df, edificaciones_df, zonas_df, tipos_df):
    # ── Verificación de totales ───────────────────────────────
    total_zonas = float(
        ub.group_by(_ZONA)
        .agg(pl.col("metros_cuadrados").sum())
        .select(pl.col("metros_cuadrados").sum())
        .item()
    )
    total_edifs = float(
        ub.with_columns(pl.col("edificio").str.slice(0, 1).alias("_ed"))
        .group_by(["_complejo", "_ed"])
        .agg(pl.col("metros_cuadrados").sum())
        .select(pl.col("metros_cuadrados").sum())
        .item()
    )
    total_compls = float(
        ub.group_by("_complejo")
        .agg(pl.col("metros_cuadrados").sum())
        .select(pl.col("metros_cuadrados").sum())
        .item()
    )

    resumen = pl.DataFrame({
        "nivel": ["Zonas", "Edificaciones", "Complejos", "Campus (total)"],
        "m²": [total_zonas, total_edifs, total_compls, total_m2],
    })
    st.dataframe(resumen, use_container_width=True, hide_index=True)

    todos_iguales = (
        abs(total_zonas - total_edifs) < 0.01
        and abs(total_edifs - total_compls) < 0.01
        and abs(total_compls - total_m2) < 0.01
    )
    if todos_iguales:
        st.success("Los totales coinciden en todos los niveles.")
    else:
        st.error("Los totales NO coinciden entre niveles.")

    st.divider()

    # ── Exploración por nivel (selectboxes en cascada) ────────
    col1, col2, col3 = st.columns(3)

    compl_opts_df = (
        ub.group_by("_complejo")
        .agg(pl.col("metros_cuadrados").sum().alias("m²"))
        .join(complejos_df, left_on="_complejo", right_on="complejo", how="left")
        .sort("_complejo")
    )
    compl_labels = {
        row["_complejo"]: (
            f"{row['_complejo']} — {row['descripción']}"
            f"  ({_fmt_m2(row['m²'])})"
        )
        for row in compl_opts_df.iter_rows(named=True)
    }
    with col1:
        compl_sel = st.selectbox(
            "Complejo",
            options=["(todos)"] + list(compl_labels.keys()),
            format_func=lambda x: x if x == "(todos)" else compl_labels.get(x, x),
            key="sup_complejo",
        )

    if compl_sel != "(todos)":
        ub_c = ub.filter(pl.col("_complejo") == compl_sel)
        edif_opts_df = (
            ub_c.group_by("_edificación")
            .agg(pl.col("metros_cuadrados").sum().alias("m²"))
            .join(
                edificaciones_df.filter(pl.col("complejo") == compl_sel),
                left_on="_edificación",
                right_on="edificación",
                how="left",
            )
            .sort("_edificación")
        )
        edif_labels = {
            row["_edificación"]: (
                f"{row['_edificación']} — {row['descripción'] or '?'}"
                f"  ({_fmt_m2(row['m²'])})"
            )
            for row in edif_opts_df.iter_rows(named=True)
        }
        with col2:
            edif_sel = st.selectbox(
                "Edificación",
                options=["(todas)"] + list(edif_labels.keys()),
                format_func=lambda x: x
                if x == "(todas)"
                else edif_labels.get(x, x),
                key="sup_edificacion",
            )
    else:
        edif_sel = "(todas)"
        with col2:
            st.selectbox(
                "Edificación", ["(todas)"], disabled=True, key="sup_edif_d"
            )

    if compl_sel != "(todos)" and edif_sel != "(todas)":
        ub_e = ub_c.filter(pl.col("_edificación") == edif_sel)
        zona_opts_df = (
            ub_e.group_by("_zona")
            .agg(pl.col("metros_cuadrados").sum().alias("m²"))
            .join(
                zonas_df.filter(
                    (pl.col("complejo") == compl_sel)
                    & (pl.col("edificación") == edif_sel)
                ),
                left_on="_zona",
                right_on="zona",
                how="left",
            )
            .sort("_zona")
        )
        zona_labels = {
            row["_zona"]: (
                f"{row['_zona']} — {row['descripción'] or '?'}"
                f"  ({_fmt_m2(row['m²'])})"
            )
            for row in zona_opts_df.iter_rows(named=True)
        }
        with col3:
            zona_sel = st.selectbox(
                "Zona",
                options=["(todas)"] + list(zona_labels.keys()),
                format_func=lambda x: x
                if x == "(todas)"
                else zona_labels.get(x, x),
                key="sup_zona",
            )
    else:
        zona_sel = "(todas)"
        with col3:
            st.selectbox("Zona", ["(todas)"], disabled=True, key="sup_zona_d")

    # ── Tabla según nivel seleccionado ────────────────────────

    if compl_sel == "(todos)":
        st.subheader("Complejos")
        tabla = (
            ub.group_by("_complejo")
            .agg(
                pl.col("metros_cuadrados").sum().round(2).alias("m²"),
                pl.col("_edificación").n_unique().alias("edificaciones"),
                (pl.col("_edificación") + pl.col("_zona"))
                .n_unique()
                .alias("zonas"),
                pl.len().alias("ubicaciones"),
            )
            .join(
                complejos_df, left_on="_complejo", right_on="complejo", how="left"
            )
            .select(
                "_complejo",
                "descripción",
                "m²",
                "edificaciones",
                "zonas",
                "ubicaciones",
            )
            .rename({"_complejo": "complejo"})
            .sort("complejo")
        )
        st.dataframe(tabla, use_container_width=True, hide_index=True)

    elif edif_sel == "(todas)":
        st.subheader(f"Edificaciones — complejo {compl_sel}")
        tabla = (
            ub_c.group_by("_edificación")
            .agg(
                pl.col("metros_cuadrados").sum().round(2).alias("m²"),
                pl.col("_zona").n_unique().alias("zonas"),
                pl.len().alias("ubicaciones"),
            )
            .join(
                edificaciones_df.filter(pl.col("complejo") == compl_sel),
                left_on="_edificación",
                right_on="edificación",
                how="left",
            )
            .select("_edificación", "descripción", "m²", "zonas", "ubicaciones")
            .rename({"_edificación": "edificación"})
            .sort("edificación")
        )
        st.dataframe(tabla, use_container_width=True, hide_index=True)

    elif zona_sel == "(todas)":
        st.subheader(f"Zonas — edificación {compl_sel}{edif_sel}")
        tabla = (
            ub_e.group_by("_zona")
            .agg(
                pl.col("metros_cuadrados").sum().round(2).alias("m²"),
                pl.len().alias("ubicaciones"),
            )
            .join(
                zonas_df.filter(
                    (pl.col("complejo") == compl_sel)
                    & (pl.col("edificación") == edif_sel)
                ),
                left_on="_zona",
                right_on="zona",
                how="left",
            )
            .select("_zona", "descripción", "m²", "ubicaciones")
            .rename({"_zona": "zona"})
            .sort("zona")
        )
        st.dataframe(tabla, use_container_width=True, hide_index=True)

    else:
        ub_z = ub_e.filter(pl.col("_zona") == zona_sel)
        st.subheader(f"Ubicaciones — zona {compl_sel}{edif_sel}{zona_sel}")

        total_zona = float(ub_z["metros_cuadrados"].sum() or 0)
        st.caption(f"{len(ub_z):,} ubicaciones  ·  {_fmt_m2(total_zona)}")

        tabla = (
            ub_z.join(tipos_df, on="tipo_ubicación", how="left", suffix="_tipo")
            .select(
                "id_ubicación",
                "planta",
                "dependencia",
                "tipo_ubicación",
                pl.col("descripción_tipo").alias("tipo"),
                "metros_cuadrados",
                "descripción",
            )
            .sort("planta", "dependencia")
        )
        st.dataframe(tabla, use_container_width=True, hide_index=True)


def _sup_presencia(
    ubicaciones, ubic_serv_df, servicios_df,
    complejos_df, edificaciones_df, zonas_df,
):
    (
        pres_zona, pres_edif, pres_complejo, pres_uji,
        n_zonas_sin, n_zonas_con, n_centros,
    ) = _calcular_presencia(ubicaciones, ubic_serv_df, servicios_df)

    # Lookups código → descripción
    _desc_compl = dict(zip(
        complejos_df["complejo"].to_list(),
        complejos_df["descripción"].to_list(),
    ))
    _desc_edif = {
        (r["complejo"], r["edificación"]): r["descripción"]
        for r in edificaciones_df.iter_rows(named=True)
    }
    _desc_zona = {
        (r["complejo"], r["edificación"], r["zona"]): r["descripción"]
        for r in zonas_df.iter_rows(named=True)
    }

    def _nombre_compl(área):
        return f"{área} — {_desc_compl.get(área, '?')}"

    def _nombre_edif(área, edif):
        desc = _desc_edif.get((área, edif), "?")
        compl = _desc_compl.get(área, "?")
        return f"{área}{edif} — {desc} ({compl})"

    def _nombre_zona(área, edificio):
        edif, zona = edificio[0], edificio[1:]
        desc = _desc_zona.get((área, edif, zona), "?")
        desc_e = _desc_edif.get((área, edif), "?")
        compl = _desc_compl.get(área, "?")
        return f"{área}{edificio} — {desc} ({desc_e}, {compl})"

    sub = st.radio(
        "Vista",
        ["Por centro", "Por nivel"],
        horizontal=True,
        key="sup_pres_vista",
    )

    if sub == "Por centro":
        nivel_centro = st.selectbox(
            "Nivel de detalle",
            ["UJI", "Complejo", "Edificación", "Zona"],
            key="sup_pres_centro_nivel",
        )

        if nivel_centro == "UJI":
            df_pivot = (
                pres_uji.sort("m2", descending=True)
                .select(
                    "centro",
                    pl.col("m2").round(2).alias("m²"),
                    pl.col("pct").round(4).alias("%"),
                )
            )
            st.subheader("Presencia de todos los centros en la UJI")
            st.dataframe(df_pivot, use_container_width=True, hide_index=True)

        elif nivel_centro == "Complejo":
            filas = []
            for row in pres_complejo.iter_rows(named=True):
                filas.append({
                    "centro": row["centro"],
                    "complejo": f"{row['área']} — {_desc_compl.get(row['área'], '?')}",
                    "m²": round(row["m2"], 2),
                    "%": round(row["pct"], 4),
                })
            if filas:
                df_all = pl.DataFrame(filas).sort(["centro", "complejo"])
                st.subheader("Presencia de todos los centros por complejo")
                st.dataframe(df_all, use_container_width=True, hide_index=True)

        elif nivel_centro == "Edificación":
            filas = []
            for row in pres_edif.iter_rows(named=True):
                código = f"{row['área']}{row['edificación']}"
                desc = _desc_edif.get((row["área"], row["edificación"]), "?")
                compl = _desc_compl.get(row["área"], "?")
                filas.append({
                    "centro": row["centro"],
                    "edificación": f"{código} — {desc} ({compl})",
                    "m²": round(row["m2"], 2),
                    "%": round(row["pct"], 4),
                })
            if filas:
                df_all = pl.DataFrame(filas).sort(["centro", "edificación"])
                st.subheader("Presencia de todos los centros por edificación")
                st.dataframe(df_all, use_container_width=True, hide_index=True)

        else:  # Zona
            filas = []
            for row in pres_zona.iter_rows(named=True):
                código = f"{row['área']}{row['edificio']}"
                ed = row["edificio"][0] if len(row["edificio"]) > 0 else ""
                zn = row["edificio"][1:] if len(row["edificio"]) > 1 else ""
                desc_z = _desc_zona.get((row["área"], ed, zn), "?")
                desc_e = _desc_edif.get((row["área"], ed), "?")
                desc_c = _desc_compl.get(row["área"], "?")
                filas.append({
                    "centro": row["centro"],
                    "zona": f"{código} — {desc_z} ({desc_e}, {desc_c})",
                    "m²": round(row["m2"], 2),
                    "%": round(row["pct"], 4),
                })
            if filas:
                df_all = pl.DataFrame(filas).sort(["centro", "zona"])
                st.subheader("Presencia de todos los centros por zona")
                st.dataframe(df_all, use_container_width=True, hide_index=True)

    else:  # Por nivel
        nivel_sel = st.selectbox(
            "Nivel",
            ["UJI", "Complejo", "Edificación", "Zona"],
            key="sup_pres_nivel",
        )

        # Mapa centro → "srv — nombre, ..." (servicios vivos)
        _serv_por_centro: dict[str, str] = {}
        for r in (
            servicios_df.filter(
                (pl.col("vivo") == 1) & pl.col("centro").is_not_null()
            )
            .sort("servicio")
            .iter_rows(named=True)
        ):
            entry = f"{r['servicio']} — {r['nombre']}"
            _serv_por_centro.setdefault(r["centro"], []).append(entry)
        _serv_txt = {c: ", ".join(ss) for c, ss in _serv_por_centro.items()}

        def _añadir_servicios(df: pl.DataFrame) -> pl.DataFrame:
            servicios_col = [_serv_txt.get(c, "") for c in df["centro"].to_list()]
            return df.with_columns(
                pl.Series("servicios", servicios_col),
                pl.col("m2").round(2),
                pl.col("pct").round(4),
            ).select("centro", "servicios", "m2", "pct")

        if nivel_sel == "UJI":
            st.subheader("Centros con presencia en la UJI")
            st.dataframe(
                _añadir_servicios(pres_uji.sort("m2", descending=True)),
                use_container_width=True,
                hide_index=True,
            )

        elif nivel_sel == "Complejo":
            áreas = sorted(pres_complejo["área"].unique().to_list())
            área_sel = st.selectbox(
                "Complejo", áreas,
                format_func=_nombre_compl,
                key="sup_pres_compl",
            )
            if área_sel:
                st.subheader(f"Centros en complejo {_nombre_compl(área_sel)}")
                df_c = (
                    pres_complejo.filter(pl.col("área") == área_sel)
                    .sort("m2", descending=True)
                )
                st.dataframe(
                    _añadir_servicios(df_c),
                    use_container_width=True, hide_index=True,
                )

        elif nivel_sel == "Edificación":
            edif_keys = (
                pres_edif.select(
                    (pl.col("área") + pl.col("edificación")).alias("código"),
                    "área",
                    "edificación",
                )
                .unique()
                .sort("código")
            )
            códigos = edif_keys["código"].to_list()
            _edif_fmt = {c: _nombre_edif(c[0], c[1]) for c in códigos}
            código_sel = st.selectbox(
                "Edificación", códigos,
                format_func=lambda x: _edif_fmt.get(x, x),
                key="sup_pres_edif",
            )
            if código_sel:
                a = código_sel[0]
                e = código_sel[1]
                st.subheader(f"Centros en edificación {_edif_fmt[código_sel]}")
                df_e = (
                    pres_edif.filter(
                        (pl.col("área") == a) & (pl.col("edificación") == e)
                    )
                    .sort("m2", descending=True)
                )
                st.dataframe(
                    _añadir_servicios(df_e),
                    use_container_width=True, hide_index=True,
                )

        else:  # Zona
            zona_keys = (
                pres_zona.select(
                    (pl.col("área") + pl.col("edificio")).alias("código"),
                    "área",
                    "edificio",
                )
                .unique()
                .sort("código")
            )
            códigos_z = zona_keys["código"].to_list()
            _zona_fmt = {c: _nombre_zona(c[0], c[1:]) for c in códigos_z}
            código_z_sel = st.selectbox(
                "Zona", códigos_z,
                format_func=lambda x: _zona_fmt.get(x, x),
                key="sup_pres_zona2",
            )
            if código_z_sel:
                a = código_z_sel[0]
                ed = código_z_sel[1:]
                st.subheader(f"Centros en zona {_zona_fmt[código_z_sel]}")
                df_z = (
                    pres_zona.filter(
                        (pl.col("área") == a) & (pl.col("edificio") == ed)
                    )
                    .sort("m2", descending=True)
                )
                st.dataframe(
                    _añadir_servicios(df_z),
                    use_container_width=True, hide_index=True,
                )


def _sup_otop(
    ubicaciones, ubic_serv_df, servicios_df, dist_otop_df,
    complejos_df, edificaciones_df, zonas_df,
):
    (
        pres_zona, pres_edif, pres_complejo, pres_uji,
        _nzs, _nzc, _nc,
    ) = _calcular_presencia(ubicaciones, ubic_serv_df, servicios_df)

    otop_centro, otop_detalle, otop_dup, otop_sin = _calcular_distribución_otop(
        dist_otop_df, pres_zona, pres_edif, pres_complejo,
    )

    # Lookup prefijo → nombre
    _desc_compl = dict(zip(
        complejos_df["complejo"].to_list(),
        complejos_df["descripción"].to_list(),
    ))
    _desc_edif = {
        (r["complejo"], r["edificación"]): r["descripción"]
        for r in edificaciones_df.iter_rows(named=True)
    }
    _desc_zona = {
        (r["complejo"], r["edificación"], r["zona"]): r["descripción"]
        for r in zonas_df.iter_rows(named=True)
    }

    def _nombre_prefijo(p: str) -> str:
        if len(p) >= 3:
            desc = _desc_zona.get((p[0], p[1], p[2:]), p)
            desc_e = _desc_edif.get((p[0], p[1]), "?")
            desc_c = _desc_compl.get(p[0], "?")
            return f"{desc} ({desc_e}, {desc_c})"
        elif len(p) == 2:
            desc = _desc_edif.get((p[0], p[1]), p)
            desc_c = _desc_compl.get(p[0], "?")
            return f"{desc} ({desc_c})"
        elif len(p) == 1:
            return _desc_compl.get(p, p)
        return p

    if otop_dup:
        with st.expander("Prefijos duplicados (agregados)", expanded=False):
            dup_df = pl.DataFrame(
                [{"prefijo": p, "nombre": _nombre_prefijo(p),
                  "filas": n, "% total": round(pct, 4)}
                 for p, n, pct in otop_dup]
            )
            st.dataframe(dup_df, use_container_width=True, hide_index=True)

    if otop_sin:
        with st.expander("Prefijos sin match (redistribuidos)", expanded=False):
            sin_df = pl.DataFrame(
                [{"prefijo": p, "nombre": _nombre_prefijo(p),
                  "%": round(pct, 4), "comentario": c}
                 for p, pct, c in otop_sin]
            )
            st.dataframe(sin_df, use_container_width=True, hide_index=True)

    st.subheader("Porcentaje por centro de coste")
    if otop_centro.is_empty():
        st.info("No hay datos de distribución OTOP.")
    else:
        vista_centro = otop_centro.with_columns(
            (pl.col("porcentaje") * 100).round(4).alias("% coste"),
        ).select("centro", "% coste")
        st.dataframe(vista_centro, use_container_width=True, hide_index=True)

        st.subheader("Detalle por centro")
        centros_otop = otop_centro["centro"].to_list()
        centro_otop_sel = st.selectbox(
            "Centro", centros_otop, key="sup_otop_centro"
        )
        if centro_otop_sel:
            det_df = otop_detalle.filter(pl.col("centro") == centro_otop_sel)
            nombres = [
                _nombre_prefijo(p) for p in det_df["prefijo"].to_list()
            ]
            det = (
                det_df.with_columns(
                    pl.Series("lugar", nombres),
                    (pl.col("pct_distribución") * 100).round(4).alias("% distribución"),
                    pl.col("pct_presencia").round(4).alias("% presencia"),
                    (pl.col("contribución") * 100).round(6).alias("contribución %"),
                )
                .select("prefijo", "lugar", "comentario", "% distribución", "% presencia", "contribución %")
                .sort("contribución %", descending=True)
            )
            st.dataframe(det, use_container_width=True, hide_index=True)
            total_contrib = det["contribución %"].sum()
            st.caption(f"Total contribución: {total_contrib:.4f} %")


# ============================================================
# Panel principal: Presupuesto
# ============================================================

DIR_FASE1 = Path("data/fase1")


def _fmt_euro(v: float) -> str:
    """Formatea euros en notación europea."""
    return f"{v:,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")


def _mostrar_presupuesto():
    seccion = st.session_state.pres_seccion
    _título(f"Presupuesto — {seccion}")

    resumen_path = DIR_FASE1 / "auxiliares" / "resumen.json"
    if not resumen_path.exists():
        st.warning("No se han encontrado datos de fase 1. Ejecuta `uv run fase1` primero.")
        return

    import json
    with open(resumen_path) as f:
        resumen = json.load(f)

    _PRES_FICHERO_REGLAS = {
        "Reglas de actividad": "conteo_reglas_presupuesto.parquet",
        "Reglas de centro de coste": "conteo_cc_presupuesto.parquet",
        "Reglas de elemento de coste": "conteo_ec_presupuesto.parquet",
    }

    _PRES_ARBOLES = {
        "Árbol: Actividades": "actividades",
        "Árbol: Centros de coste": "centros de coste",
        "Árbol: Elementos de coste": "elementos de coste",
    }

    if seccion == "Resumen":
        mc1, mc2, mc3, mc4, mc5 = st.columns([1, 1.5, 1, 1, 1.5])
        mc1.metric("UC generadas", f"{resumen['n_uc_presupuesto']:,}")
        mc2.metric("Importe UC", _fmt_euro(resumen["importe_uc_presupuesto"]))
        mc3.metric("Filtrados", f"{resumen.get('n_filtrados_presupuesto', 0):,}")
        mc4.metric("Sin clasificar", f"{resumen['n_sin_presupuesto']:,}")
        mc5.metric("Actividades", f"{resumen['n_actividades_antes']} → {resumen['n_actividades_despues']}")
        return

    if seccion == "Unidades de coste":
        _pres_uc()
    elif seccion == "Sin clasificar":
        _pres_sin_clasificar()
    elif seccion == "Apuntes filtrados":
        _pres_filtrados()
    elif seccion == "Suministros":
        _pres_suministros()
    elif seccion == "Distribución mantenimientos OTOP":
        _pres_otop()
    elif seccion in _PRES_FICHERO_REGLAS:
        _pres_reglas(_PRES_FICHERO_REGLAS[seccion])
    elif seccion in _PRES_ARBOLES:
        _pres_arbol_modificado(_PRES_ARBOLES[seccion])


def _pres_uc():
    path = DIR_FASE1 / "uc presupuesto.parquet"
    if not path.exists():
        st.info("No hay fichero de UC de presupuesto.")
        return
    df = pl.read_parquet(path)
    st.caption(f"{len(df):,} unidades de coste")

    fc1, fc2 = st.columns([4, 1])
    with fc1:
        filtro = st.text_input("Filtrar", key="pres_uc_filtro")
    with fc2:
        col_filtro = st.selectbox(
            "Columna",
            options=["(todas)"] + df.columns,
            key="pres_uc_col",
        )

    if filtro:
        cols = df.columns if col_filtro == "(todas)" else [col_filtro]
        mask = pl.lit(False)
        for col in cols:
            mask = mask | pl.col(col).cast(pl.Utf8).str.to_lowercase().str.contains(
                filtro.lower(), literal=True
            )
        df = df.filter(mask)
        st.caption(f"Mostrando {len(df):,} filas")

    _st_df(df)


def _pres_sin_clasificar():
    path = DIR_FASE1 / "auxiliares" / "sin_clasificar_presupuesto.parquet"
    if not path.exists():
        st.info("No hay fichero de apuntes sin clasificar.")
        return
    df = pl.read_parquet(path)
    if df.is_empty():
        st.success("No hay apuntes sin clasificar.")
    else:
        st.warning(f"{len(df):,} apuntes sin clasificar")
        _st_df(df)


def _pres_filtrados():
    path = DIR_FASE1 / "auxiliares" / "filtrados_presupuesto.parquet"
    if not path.exists():
        st.info("No hay fichero de apuntes filtrados.")
        return
    df = pl.read_parquet(path)
    if df.is_empty():
        st.success("No hay apuntes filtrados.")
        return

    # Resumen por motivo
    resumen = (
        df.group_by("motivo")
        .agg(
            pl.len().alias("apuntes"),
            pl.col("importe").sum().alias("importe"),
        )
        .sort("importe", descending=True)
    )
    _st_df(resumen, totales=False)
    st.caption(f"Total: {len(df):,} apuntes filtrados · {_fmt_euro(float(df['importe'].sum()))}")
    st.divider()

    # Tabla completa filtrable
    df_f = _filtro_tabla(df, "pres_filtrados")
    _st_df(df_f)


def _pres_suministros():
    """Muestra las UC de suministros (energía, agua, gas)."""
    path = DIR_FASE1 / "uc suministros.parquet"
    if not path.exists():
        st.info("No hay UC de suministros. Ejecuta `uv run fase1` primero.")
        return
    df = pl.read_parquet(path)
    if df.is_empty():
        st.success("No se generaron UC de suministros.")
        return

    # Resumen por origen (energía, agua, gas)
    resumen = (
        df.group_by("origen")
        .agg(
            pl.len().alias("n"),
            pl.col("importe").sum().alias("importe"),
        )
        .sort("importe", descending=True)
    )
    _st_df(resumen, totales=False)

    total = float(df["importe"].sum())
    st.caption(f"Total: {len(df):,} UC · {_fmt_euro(total)}")
    st.divider()

    # Resumen por centro de coste
    por_centro = (
        df.group_by("centro_de_coste")
        .agg(
            pl.len().alias("n"),
            pl.col("importe").sum().alias("importe"),
        )
        .sort("importe", descending=True)
    )
    st.subheader(f"Por centro de coste ({len(por_centro):,})")
    _st_df(por_centro)

    # Tabla completa filtrable
    st.divider()
    st.subheader("Detalle")
    df_f = _filtro_tabla(df, "pres_suministros")
    _st_df(df_f)


def _pres_otop():
    """Muestra la distribución de mantenimientos OTOP (desde datos de superficies)."""
    try:
        (
            ubicaciones, _complejos_df, edificaciones_df, zonas_df, _tipos_df,
            ubic_serv_df, servicios_df, dist_otop_df,
        ) = _load_superficies()
    except Exception:
        st.warning("No se pudieron cargar los datos de superficies.")
        return

    _sup_otop(
        ubicaciones, ubic_serv_df, servicios_df, dist_otop_df,
        _complejos_df, edificaciones_df, zonas_df,
    )


_REGLA_COL = {
    "conteo_reglas_presupuesto.parquet": "regla_actividad",
    "conteo_cc_presupuesto.parquet": "regla_cc",
    "conteo_ec_presupuesto.parquet": "regla_ec",
}


def _pres_reglas(fichero: str):
    path = DIR_FASE1 / "auxiliares" / fichero
    if not path.exists():
        st.info("No hay datos de reglas.")
        return
    df_reglas = pl.read_parquet(path)

    # Tabla de reglas con selección por fila
    ev_regla = _st_df(
        df_reglas.with_columns(pl.col("importe").round(2)),
        on_select="rerun",
        selection_mode="single-row",
        key=f"pres_reglas_df_{fichero}",
    )

    # Columna de UC que corresponde a este fichero de reglas
    col_regla = _REGLA_COL.get(fichero)
    if col_regla is None:
        return

    uc_path = DIR_FASE1 / "uc presupuesto.parquet"
    if not uc_path.exists():
        return

    # Comprobar si hay fila seleccionada
    filas_sel = ev_regla.selection.rows if ev_regla.selection else []
    if not filas_sel:
        return

    regla_sel = df_reglas["regla"][filas_sel[0]]

    # Filtrar UC por regla seleccionada
    uc = pl.read_parquet(uc_path)
    uc_regla = uc.filter(pl.col(col_regla) == regla_sel)
    if uc_regla.is_empty():
        st.info("No hay UC para esta regla.")
        return

    # Obtener asientos únicos y cargar apuntes
    asientos = uc_regla["origen_id"].unique().to_list()
    apuntes_path = Path("data/entrada/presupuesto/_parquet/apuntes presupuesto de gasto.parquet")
    if apuntes_path.exists():
        apuntes = pl.read_parquet(apuntes_path).with_columns(
            pl.col("asiento").cast(pl.Utf8)
        )
        apuntes_regla = apuntes.filter(pl.col("asiento").is_in(asientos))
    else:
        apuntes_regla = pl.DataFrame()

    st.divider()
    st.subheader(f"Apuntes de «{regla_sel}» ({len(apuntes_regla):,})")
    if apuntes_regla.is_empty():
        st.info("No se encontraron apuntes.")
        return

    fc1, fc2 = st.columns([4, 1])
    with fc1:
        filtro_ap = st.text_input("Filtrar apuntes", key=f"pres_ap_filtro_{fichero}")
    with fc2:
        col_filtro_ap = st.selectbox(
            "Columna",
            options=["(todas)"] + apuntes_regla.columns,
            key=f"pres_ap_col_{fichero}",
        )

    if filtro_ap:
        cols = apuntes_regla.columns if col_filtro_ap == "(todas)" else [col_filtro_ap]
        mask = pl.lit(False)
        for col in cols:
            mask = mask | pl.col(col).cast(pl.Utf8).str.to_lowercase().str.contains(
                filtro_ap.lower(), literal=True
            )
        apuntes_regla = apuntes_regla.filter(mask)
        st.caption(f"Mostrando {len(apuntes_regla):,} apuntes")

    ev_apunte = _st_df(
        apuntes_regla,
        on_select="rerun",
        selection_mode="single-row",
        key=f"pres_apuntes_df_{fichero}",
    )

    # Comprobar si hay apunte seleccionado
    filas_ap = ev_apunte.selection.rows if ev_apunte.selection else []
    if not filas_ap:
        return

    apunte_sel = str(apuntes_regla["asiento"][filas_ap[0]])
    uc_apunte = uc.filter(pl.col("origen_id") == apunte_sel)
    st.divider()
    st.subheader(f"UC del apunte {apunte_sel} ({len(uc_apunte):,})")
    _st_df(uc_apunte.drop("regla_actividad", "regla_cc", "regla_ec"))


# ============================================================
# Panel principal: Árboles modificados por reglas
# ============================================================


def _pres_arbol_modificado(nombre: str) -> None:
    """Visualiza un árbol tras aplicar reglas, destacando nodos nuevos."""
    from coana.util import Árbol, NodoÁrbol

    path_original = DIR_ENTRADA / "estructuras" / f"{nombre}.tree"
    path_modificado = DIR_FASE1 / f"{nombre}.tree"

    if not path_modificado.exists():
        st.info(f"No se ha encontrado `{path_modificado}`. Ejecuta `uv run fase1` primero.")
        return

    árbol = Árbol.from_file(path_modificado)

    # Recoger identificadores originales para detectar nodos nuevos
    ids_originales: set[str] = set()
    if path_original.exists():
        original = Árbol.from_file(path_original)
        def _recoger_ids(nodo: NodoÁrbol) -> None:
            if nodo.identificador:
                ids_originales.add(nodo.identificador)
            for h in nodo.hijos:
                _recoger_ids(h)
        _recoger_ids(original.raíz)

    def _es_nuevo(nodo: NodoÁrbol) -> bool:
        return bool(ids_originales) and nodo.identificador not in ids_originales

    # Métricas
    n_total = 0
    n_nuevos = 0
    prof_max = 0

    def _contar(nodo: NodoÁrbol, nivel: int) -> None:
        nonlocal n_total, n_nuevos, prof_max
        if nodo is not árbol.raíz:
            n_total += 1
            if _es_nuevo(nodo):
                n_nuevos += 1
            prof_max = max(prof_max, nivel)
        for h in nodo.hijos:
            _contar(h, nivel + 1)

    _contar(árbol.raíz, 0)

    c1, c2, c3 = st.columns(3)
    c1.metric("Nodos", f"{n_total:,}")
    c2.metric("Nuevos", f"{n_nuevos:,}")
    c3.metric("Profundidad", f"{prof_max}")

    # Filtros
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        filtro_código = st.text_input("Código", key=f"pres_tree_código_{nombre}")
    with fc2:
        filtro_desc = st.text_input("Descripción", key=f"pres_tree_desc_{nombre}")
    with fc3:
        filtro_etiqueta = st.text_input("Etiqueta", key=f"pres_tree_etiq_{nombre}")

    hay_filtro = bool(filtro_código or filtro_desc or filtro_etiqueta)

    if hay_filtro:
        filas: list[dict] = []

        def _aplanar(nodo: NodoÁrbol) -> None:
            if nodo is not árbol.raíz:
                filas.append({
                    "código": nodo.código,
                    "descripción": nodo.descripción,
                    "identificador": nodo.identificador,
                    "nuevo": _es_nuevo(nodo),
                    "nivel": nodo.código.count("."),
                })
            for h in nodo.hijos:
                _aplanar(h)

        _aplanar(árbol.raíz)

        df = pl.DataFrame(filas)
        if filtro_código:
            df = df.filter(
                pl.col("código").str.to_lowercase().str.contains(
                    filtro_código.lower(), literal=True
                )
            )
        if filtro_desc:
            df = df.filter(
                pl.col("descripción").str.to_lowercase().str.contains(
                    filtro_desc.lower(), literal=True
                )
            )
        if filtro_etiqueta:
            df = df.filter(
                pl.col("identificador").str.to_lowercase().str.contains(
                    filtro_etiqueta.lower(), literal=True
                )
            )

        st.caption(f"{len(df):,} nodos encontrados")
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        # Árbol HTML interactivo con nodos nuevos destacados
        _COLOR_NUEVO = "#2a7"  # verde para nodos nuevos

        def _nodo_html(nodo: NodoÁrbol) -> str:
            nuevo = _es_nuevo(nodo)
            color_desc = _COLOR_NUEVO if nuevo else "inherit"
            peso = "600" if nuevo else "normal"
            código_span = (
                f"<span style='color:#888;font-family:monospace'>"
                f"{nodo.código}</span>"
            )
            etiqueta_span = (
                f"<span style='color:#c06;font-family:monospace;"
                f"font-size:0.9em'>{nodo.identificador}</span>"
            )
            desc_span = (
                f"<span style='color:{color_desc};font-weight:{peso}'>"
                f"{nodo.descripción}</span>"
            )
            label = f"{código_span} {desc_span} {etiqueta_span}"

            if nodo.hijos:
                hijos_html = "".join(_nodo_html(h) for h in nodo.hijos)
                return (
                    f"<details open>"
                    f"<summary>{label}</summary>"
                    f"<div style='margin-left:1.2em'>{hijos_html}</div>"
                    f"</details>"
                )
            return f"<div style='padding:2px 0'>{label}</div>"

        css = (
            "<style>"
            "details { margin: 1px 0; }"
            "summary { cursor: pointer; padding: 2px 0; }"
            "summary:hover { background: #f0f4ff; }"
            "</style>"
        )
        leyenda = (
            f"<div style='margin-bottom:8px;font-size:0.85em'>"
            f"<span style='color:{_COLOR_NUEVO};font-weight:600'>verde</span>"
            f" = nodo añadido por las reglas"
            f"</div>"
        )
        cuerpo = "".join(_nodo_html(h) for h in árbol.raíz.hijos)
        st.html(f"{css}{leyenda}<div style='font-size:0.92em'>{cuerpo}</div>")


# ============================================================
# Panel principal: Amortizaciones
# ============================================================

DIR_AMORT = DIR_FASE1 / "auxiliares" / "amortizaciones"


def _filtro_tabla(df: pl.DataFrame, key_prefix: str) -> pl.DataFrame:
    """Widget de filtro genérico: texto + columna + ordenación. Devuelve el df filtrado y ordenado."""
    fc1, fc2, fc3, fc4 = st.columns([4, 1, 1, 0.5])
    with fc1:
        filtro = st.text_input("Filtrar", key=f"{key_prefix}_filtro")
    with fc2:
        col_filtro = st.selectbox(
            "Columna",
            options=["(todas)"] + df.columns,
            key=f"{key_prefix}_col",
        )
    with fc3:
        col_orden = st.selectbox(
            "Ordenar por",
            options=["(ninguna)"] + df.columns,
            key=f"{key_prefix}_sort",
        )
    with fc4:
        desc = st.toggle("Desc", value=True, key=f"{key_prefix}_desc")
    if filtro:
        cols = df.columns if col_filtro == "(todas)" else [col_filtro]
        busca_null = filtro.lower() in ("none", "null")
        mask = pl.lit(False)
        for col in cols:
            if busca_null:
                mask = mask | pl.col(col).is_null()
            mask = mask | pl.col(col).cast(pl.Utf8).str.to_lowercase().str.contains(
                filtro.lower(), literal=True
            )
        df = df.filter(mask)
    if col_orden != "(ninguna)":
        df = df.sort(col_orden, descending=desc)
    st.caption(f"Mostrando {len(df):,} filas")
    return df


_COLS_IMPORTE = {"valor_inicial", "importe", "coste", "pto €", "amort €", "nóm €", "sumin €", "total"}


def _fmt_euro_series(s: pl.Series) -> pl.Series:
    """Convierte una serie numérica a texto en notación europea (1.234,56)."""
    return s.map_elements(
        lambda v: f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        return_dtype=pl.Utf8,
    )


def _col_config_euros(df: pl.DataFrame) -> dict:
    """Devuelve column_config para columnas de importe (ya formateadas a texto)."""
    return {
        c: st.column_config.TextColumn()
        for c in df.columns
        if c in _COLS_IMPORTE
    }


def _totales_importe(df: pl.DataFrame) -> None:
    """Muestra la suma de las columnas de importe presentes en el DataFrame.

    Funciona tanto con columnas numéricas como con columnas ya formateadas
    a texto (en cuyo caso parsea el valor europeo de vuelta a float).
    """
    cols = [c for c in df.columns if c in _COLS_IMPORTE]
    if not cols:
        return
    partes: list[str] = []
    for c in cols:
        if df[c].dtype == pl.Utf8:
            total = df[c].str.replace_all(r"\.", "").str.replace(",", ".").cast(pl.Float64, strict=False).sum()
        else:
            total = df[c].sum()
        partes.append(f"**{c}**: {_fmt_euro(float(total))}")
    st.caption("Totales: " + "  ·  ".join(partes))


def _aplicar_fmt_euros(df: pl.DataFrame) -> pl.DataFrame:
    """Formatea columnas de importe como texto en notación europea.

    Rellena con *figure spaces* (U+2007, mismo ancho que un dígito)
    a la izquierda para que los valores queden alineados a la derecha.
    """
    for c in df.columns:
        if c in _COLS_IMPORTE and df[c].dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32):
            formatted = _fmt_euro_series(df[c])
            max_len = formatted.str.len_chars().max() or 0
            # str.pad_start usa espacios; reemplazamos por figure space
            padded = formatted.str.pad_start(max_len)
            df = df.with_columns(padded.str.replace_all(" ", "\u2007").alias(c))
    return df


def _st_df(df: pl.DataFrame, *, totales: bool = True, **kwargs) -> object:
    """Muestra un DataFrame con columnas de importe en notación europea.

    Calcula totales sobre datos numéricos, formatea las columnas de
    importe a texto ``1.234,56`` y llama a ``st.dataframe``.
    Devuelve el objeto de evento de ``st.dataframe`` (útil para selección).

    Pasa cualquier kwarg extra directamente a ``st.dataframe``
    (p.ej. ``on_select``, ``selection_mode``, ``key``).
    """
    if totales:
        _totales_importe(df)
    df_fmt = _aplicar_fmt_euros(df)
    kwargs.setdefault("use_container_width", True)
    kwargs.setdefault("hide_index", True)
    kwargs["column_config"] = {
        **_col_config_euros(df_fmt),
        **kwargs.get("column_config", {}),
    }
    return st.dataframe(df_fmt, **kwargs)


def _mostrar_amortizaciones():
    seccion = st.session_state.amort_seccion
    _título(f"Amortizaciones — {seccion}")

    resumen_path = DIR_FASE1 / "auxiliares" / "resumen.json"
    if not resumen_path.exists():
        st.warning("No se han encontrado datos de fase 1. Ejecuta `uv run fase1` primero.")
        return

    import json
    with open(resumen_path) as f:
        resumen = json.load(f)

    if seccion == "Resumen":
        n_orig = resumen.get("amort_n_original", 0)
        n_enriq = resumen.get("amort_n_enriquecidos", 0)
        importe = resumen.get("amort_importe_total", 0.0)

        mc1, mc2, mc3, mc4 = st.columns([1, 1, 1.5, 1])
        mc1.metric("Registros originales", f"{n_orig:,}")
        mc2.metric("Enriquecidos", f"{n_enriq:,}")
        mc3.metric("Importe total", _fmt_euro(importe))
        mc4.metric("Filtrados",
                   f"{resumen.get('amort_n_filtrados_estado', 0) + resumen.get('amort_n_filtrados_cuenta', 0) + resumen.get('amort_n_filtrados_fecha', 0) + resumen.get('amort_n_sin_cuenta', 0) + resumen.get('amort_n_sin_fecha_alta', 0):,}")
        return

    if seccion == "Inventario con amortización":
        _amort_inventario_enriquecido()
    elif seccion == "Filtrados por estado":
        _amort_filtrados_estado()
    elif seccion == "Filtrados por cuenta":
        _amort_filtrados_cuenta()
    elif seccion == "Filtrados por fecha":
        _amort_filtrados_fecha()
    elif seccion == "Sin cuenta":
        _amort_sin_cuenta()
    elif seccion == "Por cuenta":
        _amort_por_cuenta()
    elif seccion == "UC generadas":
        _amort_uc_generadas()
    elif seccion == "Sin centro":
        _amort_sin_centro()


def _amort_inventario_enriquecido():
    path = DIR_AMORT / "inventario_enriquecido.parquet"
    if not path.exists():
        st.info("No hay datos. Ejecuta `uv run fase1` primero.")
        return
    df = pl.read_parquet(path)
    st.subheader(f"Enriquecidos ({len(df):,})")
    df_filt = _filtro_tabla(df, "amort_enriq")
    _st_df(df_filt)

    # Tabla de no enriquecidos con motivo
    motivos: list[tuple[str, str]] = [
        ("filtrados_estado.parquet", "Estado = B (baja)"),
        ("sin_cuenta.parquet", "Sin cuenta contable"),
        ("filtrados_cuenta.parquet", "Prefijo de cuenta inválido"),
        ("sin_fecha_alta.parquet", "Sin fecha de alta"),
        ("filtrados_fecha.parquet", "Fuera del período de amortización"),
    ]
    partes: list[pl.DataFrame] = []
    cols_comunes: list[str] | None = None
    for fichero, motivo in motivos:
        p = DIR_AMORT / fichero
        if p.exists():
            parte = pl.read_parquet(p)
            if not parte.is_empty():
                if cols_comunes is None:
                    cols_comunes = parte.columns
                else:
                    cols_comunes = [c for c in cols_comunes if c in parte.columns]
                partes.append((parte, motivo))

    if partes and cols_comunes:
        dfs = []
        for parte, motivo in partes:
            dfs.append(
                parte.select(cols_comunes).with_columns(
                    pl.lit(motivo).alias("motivo"),
                )
            )
        no_enriq = pl.concat(dfs)
        st.divider()
        st.subheader(f"No enriquecidos ({len(no_enriq):,})")
        no_enriq_filt = _filtro_tabla(no_enriq, "amort_no_enriq")
        _st_df(no_enriq_filt)


def _amort_filtrados_estado():
    path = DIR_AMORT / "filtrados_estado.parquet"
    if not path.exists():
        st.info("No hay datos. Ejecuta `uv run fase1` primero.")
        return
    df = pl.read_parquet(path)
    st.info(f"{len(df):,} registros eliminados por estado = 'B' (baja)")
    df = _filtro_tabla(df, "amort_fest")
    _st_df(df)


def _amort_filtrados_cuenta():
    path = DIR_AMORT / "filtrados_cuenta.parquet"
    detalle_path = DIR_AMORT / "detalle_cuentas_filtradas.parquet"
    if not path.exists():
        st.info("No hay datos. Ejecuta `uv run fase1` primero.")
        return
    df = pl.read_parquet(path)
    st.info(f"{len(df):,} registros eliminados por prefijo de cuenta inválido")

    if detalle_path.exists():
        detalle = pl.read_parquet(detalle_path)
        ref_path = DIR_ENTRADA / "inventario" / "años amortización por cuenta.xlsx"
        if ref_path.exists():
            ref = _load_excel(str(ref_path))
            detalle = detalle.join(
                ref.select("cuenta", "nombre", "años_amortización"),
                on="cuenta",
                how="left",
            ).select("cuenta", "nombre", "años_amortización", "n", "valor_inicial")
        st.subheader("Resumen por cuenta")
        ev_detalle = _st_df(detalle, totales=False,
            on_select="rerun",
            selection_mode="single-row",
            key="amort_fcta_resumen",
        )

        filas_sel = ev_detalle.selection.rows if ev_detalle.selection else []
        if filas_sel:
            cuenta_sel = str(detalle["cuenta"][filas_sel[0]])
            df = df.filter(pl.col("cuenta") == cuenta_sel)
            st.subheader(f"Detalle — cuenta {cuenta_sel}")
        else:
            st.subheader("Detalle")

    else:
        st.subheader("Detalle")

    df = _filtro_tabla(df, "amort_fcta")
    _st_df(df)


def _amort_filtrados_fecha():
    path_fecha = DIR_AMORT / "filtrados_fecha.parquet"
    path_sin_fecha = DIR_AMORT / "sin_fecha_alta.parquet"
    if not path_fecha.exists() and not path_sin_fecha.exists():
        st.info("No hay datos. Ejecuta `uv run fase1` primero.")
        return

    if path_sin_fecha.exists():
        df_sin = pl.read_parquet(path_sin_fecha)
        st.subheader(f"Sin fecha de alta ({len(df_sin):,})")
        if not df_sin.is_empty():
            df_sin = _filtro_tabla(df_sin, "amort_sfecha")
            _st_df(df_sin)
        else:
            st.success("No hay registros sin fecha de alta.")

    if path_fecha.exists():
        df = pl.read_parquet(path_fecha)
        st.subheader(f"Fuera del período de amortización ({len(df):,})")
        if not df.is_empty():
            df = _filtro_tabla(df, "amort_ffecha")
            _st_df(df)
        else:
            st.success("No hay registros filtrados por fecha.")


def _amort_sin_cuenta():
    path = DIR_AMORT / "sin_cuenta.parquet"
    if not path.exists():
        st.info("No hay datos. Ejecuta `uv run fase1` primero.")
        return
    df = pl.read_parquet(path)
    st.info(f"{len(df):,} registros sin cuenta contable")
    if not df.is_empty():
        df = _filtro_tabla(df, "amort_scta")
        _st_df(df)
    else:
        st.success("No hay registros sin cuenta.")


def _amort_por_cuenta():
    path = DIR_AMORT / "inventario_enriquecido.parquet"
    if not path.exists():
        st.info("No hay datos. Ejecuta `uv run fase1` primero.")
        return
    df = pl.read_parquet(path)
    if "cuenta" not in df.columns:
        st.error("El inventario enriquecido no tiene columna 'cuenta'.")
        return

    # Resumen por cuenta con nombre y años
    resumen = (
        df.group_by("cuenta")
        .agg(
            pl.len().alias("n"),
            pl.col("valor_inicial").sum(),
            pl.col("importe").sum(),
        )
        .sort("cuenta")
    )
    ref_path = DIR_ENTRADA / "inventario" / "años amortización por cuenta.xlsx"
    if ref_path.exists():
        ref = _load_excel(str(ref_path))
        resumen = resumen.join(
            ref.select("cuenta", "nombre", "años_amortización"),
            on="cuenta",
            how="left",
        ).select("cuenta", "nombre", "años_amortización", "n", "valor_inicial", "importe")

    st.subheader("Cuentas")
    ev = _st_df(resumen,
        on_select="rerun",
        selection_mode="single-row",
        key="amort_por_cuenta_resumen",
    )

    filas_sel = ev.selection.rows if ev.selection else []
    if not filas_sel:
        return

    cuenta_sel = str(resumen["cuenta"][filas_sel[0]])
    df_c = df.filter(pl.col("cuenta") == cuenta_sel)
    nombre = resumen["nombre"][filas_sel[0]] if "nombre" in resumen.columns else ""
    st.subheader(f"Detalle — {cuenta_sel} {nombre or ''}")
    ev_det = _st_df(df_c,
        on_select="rerun",
        selection_mode="single-row",
        key="amort_por_cuenta_detalle",
    )

    filas_det = ev_det.selection.rows if ev_det.selection else []
    if not filas_det:
        return

    inv_id = str(df_c["id"][filas_det[0]])
    uc_path = DIR_FASE1 / "uc amortizaciones.parquet"
    if not uc_path.exists():
        return
    uc = pl.read_parquet(uc_path)
    uc_inv = uc.filter(pl.col("origen_id") == inv_id)

    st.divider()
    st.subheader(f"UC generadas a partir del registro {inv_id}")
    if uc_inv.is_empty():
        st.info("No se generaron UC para este registro.")
    else:
        _st_df(uc_inv)


def _amort_uc_generadas():
    path = DIR_FASE1 / "uc amortizaciones.parquet"
    if not path.exists():
        st.info("No hay UC de amortizaciones. Ejecuta `uv run fase1` primero.")
        return
    df = pl.read_parquet(path)
    df_filt = _filtro_tabla(df, "amort_uc")
    ev = _st_df(df_filt,
        on_select="rerun",
        selection_mode="single-row",
        key="amort_uc_df",
    )

    filas_sel = ev.selection.rows if ev.selection else []
    if not filas_sel:
        return

    uc_sel = df_filt.row(filas_sel[0], named=True)
    origen_id = uc_sel["origen_id"]

    # Buscar líneas de inventario con ese id
    inv_path = DIR_AMORT / "inventario_enriquecido.parquet"
    if not inv_path.exists():
        return

    inv = pl.read_parquet(inv_path)
    # origen_id es str, inv.id es int
    lineas = inv.filter(pl.col("id").cast(pl.Utf8) == str(origen_id))

    st.divider()
    st.subheader(f"Líneas de inventario de UC {uc_sel['id']}")
    st.caption(f"origen_id: {origen_id}  ·  origen_porción: {uc_sel['origen_porción']}")
    if lineas.is_empty():
        st.info("No se encontraron líneas de inventario.")
    else:
        _st_df(lineas)


def _amort_sin_centro():
    path = Path("data/fase1/auxiliares/amortizaciones/sin_uc.parquet")
    if not path.exists():
        st.info("No hay fichero de amortizaciones sin UC.")
        return
    df = pl.read_parquet(path)
    st.info(f"{len(df):,} registros sin centro de coste asignado")
    if not df.is_empty():
        df = _filtro_tabla(df, "amort_sincc")
        _st_df(df)
    else:
        st.success("Todos los registros tienen centro de coste.")


_PERSONAL_PARQUETS: dict[str, str] = {
    "Expedientes PDI": "PDI.parquet",
    "Expedientes PTGAS": "PTGAS.parquet",
    "Expedientes PVI": "PVI.parquet",
    "Expedientes otros": "Otros.parquet",
}

DIR_NOMINAS = DIR_FASE1 / "auxiliares" / "nóminas"


def _mostrar_anomalias_pdi():
    """Muestra anomalías detectadas en datos de PDI."""
    _título("Personal — Anomalías PDI")

    from coana.util import read_excel as _read_xl

    dir_doc = DIR_ENTRADA / "docencia"
    doc_path = dir_doc / "docencia.xlsx"
    ag_path = dir_doc / "asignaturas grados.xlsx"
    am_path = dir_doc / "asignaturas másteres.xlsx"
    gr_path = dir_doc / "grados.xlsx"
    mr_path = dir_doc / "másteres.xlsx"
    est_path = dir_doc / "estudios.xlsx"

    for p in [doc_path, ag_path, gr_path, est_path]:
        pq = p.parent / "_parquet" / (p.stem + ".parquet")
        if not p.exists() and not pq.exists():
            st.warning(f"Fichero no encontrado: `{p}`")
            return

    docencia_df = _read_xl(str(doc_path))
    ag = _read_xl(str(ag_path))
    gr = _read_xl(str(gr_path))
    est = _read_xl(str(est_path))

    # Asignaturas con titulación conocida vía grados
    asig_con_tit_g = (
        ag.select("asignatura", "grado")
        .join(gr.select("grado", "estudio"), on="grado", how="left")
        .filter(pl.col("estudio").is_not_null())
        .join(est.select("estudio"), on="estudio", how="inner")
        .select("asignatura")
    )

    # Asignaturas con titulación conocida vía másteres
    asig_con_tit_m = pl.DataFrame(schema={"asignatura": pl.Utf8})
    if am_path.exists() and mr_path.exists():
        am = _read_xl(str(am_path))
        mr = _read_xl(str(mr_path))
        asig_con_tit_m = (
            am.select("asignatura", "máster")
            .join(mr.select("máster", "estudio"), on="máster", how="left")
            .filter(pl.col("estudio").is_not_null())
            .join(est.select("estudio"), on="estudio", how="inner")
            .select("asignatura")
        )

    asig_con_tit = pl.concat([asig_con_tit_g, asig_con_tit_m]).unique()

    # Asignaturas sin titulación: las de docencia que no están en asig_con_tit
    todas_asig = docencia_df.select("asignatura").unique()
    asig_sin_tit = todas_asig.join(asig_con_tit, on="asignatura", how="anti")

    # Enriquecer con nombre de asignatura (de grados o másteres)
    nombres_asig = pl.DataFrame(schema={"asignatura": pl.Utf8, "nombre_asig": pl.Utf8})
    if not ag.is_empty():
        nombres_g = ag.select("asignatura", pl.col("nombre").alias("nombre_asig"))
        nombres_asig = pl.concat([nombres_asig, nombres_g])
    if am_path.exists():
        am = _read_xl(str(am_path))
        if not am.is_empty():
            nombres_m = am.select("asignatura", pl.col("nombre").alias("nombre_asig"))
            nombres_asig = pl.concat([nombres_asig, nombres_m])
    nombres_asig = nombres_asig.unique(subset=["asignatura"])

    st.subheader("Asignaturas sin titulación conocida")

    if asig_sin_tit.is_empty():
        st.success("No hay asignaturas sin titulación conocida.")
        return

    # Enriquecer con per_id y nombre de persona
    personas_path = DIR_ENTRADA / "nóminas" / "personas.xlsx"
    personas_ref = _read_xl(str(personas_path)) if personas_path.exists() else pl.DataFrame()

    detalle = (
        docencia_df
        .join(asig_sin_tit, on="asignatura", how="inner")
        .select("asignatura", "per_id", "créditos_impartidos")
        .join(nombres_asig, on="asignatura", how="left")
        .with_columns(
            pl.col("nombre_asig").fill_null(pl.col("asignatura")).alias("nombre_asignatura"),
        )
    )

    if not personas_ref.is_empty() and "per_id" in personas_ref.columns:
        cols_nombre = [c for c in ["nombre", "apellido1", "apellido2"] if c in personas_ref.columns]
        if cols_nombre:
            per_ref = personas_ref.select("per_id", *cols_nombre)
            detalle = detalle.join(per_ref, on="per_id", how="left")
            detalle = detalle.with_columns(
                pl.concat_str([pl.col(c) for c in cols_nombre], separator=" ", ignore_nulls=True).alias("persona"),
            ).drop(cols_nombre)

    detalle = detalle.drop("nombre_asig").sort("asignatura", "per_id")

    n_asig = detalle["asignatura"].n_unique()
    n_prof = detalle["per_id"].n_unique()
    cred_anomalos = detalle["créditos_impartidos"].sum() or 0.0
    cred_totales = docencia_df["créditos_impartidos"].sum() or 0.0
    pct = (cred_anomalos / cred_totales * 100) if cred_totales > 0 else 0.0
    c1, c2, c3 = st.columns(3)
    c1.metric("Asignaturas sin titulación", f"{n_asig:,}")
    c2.metric("Profesorado afectado", f"{n_prof:,}")
    c3.metric("Créditos anómalos / totales", f"{cred_anomalos:,.1f} / {cred_totales:,.1f} ({pct:.1f}%)")

    detalle_f = _filtro_tabla(detalle, "anomalias_pdi_sin_tit")
    st.dataframe(detalle_f, use_container_width=True, hide_index=True, key="anomalias_pdi_df")


def _mostrar_uc_ptgas_por_servicio():
    """Muestra las UC generadas a partir de retribuciones ordinarias PTGAS, agrupadas por servicio."""
    _título("Personal — UC PTGAS por servicio")

    path = DIR_NOMINAS / "uc_ptgas.parquet"
    if not path.exists():
        st.info("No hay datos. Ejecuta la Fase 1 primero.")
        return

    uc = pl.read_parquet(path)
    if uc.is_empty():
        st.info("No se generaron UC de PTGAS.")
        return

    # Enriquecer con nombre del servicio
    srv_path = DIR_ENTRADA / "inventario" / "servicios.xlsx"
    if srv_path.exists():
        srv_ref = _load_excel(str(srv_path))
        if "servicio" in srv_ref.columns and "nombre" in srv_ref.columns:
            # Extraer servicio del origen_id (formato "PTGAS-srv-XXX" o "PTGAS-srv-368-cp-YYY")
            uc = uc.with_columns(
                pl.col("origen_id").str.replace("PTGAS-srv-", "")
                .str.split("-").list.first()
                .alias("_servicio"),
            )
            uc = uc.join(
                srv_ref.select(
                    pl.col("servicio").cast(pl.Utf8),
                    pl.col("nombre").alias("nombre_servicio"),
                ),
                left_on="_servicio",
                right_on="servicio",
                how="left",
            ).drop("_servicio")

    # Métricas generales
    c1, c2 = st.columns(2)
    c1.metric("UC generadas", f"{len(uc):,}")
    c2.metric("Importe total", _fmt_euro(float(uc["importe"].sum())))

    st.divider()

    # Tabla completa filtrable
    uc_f = _filtro_tabla(uc, "uc_ptgas_srv")
    ev = _st_df(
        uc_f,
        on_select="rerun",
        selection_mode="single-row",
        key="uc_ptgas_srv_df",
    )

    filas_sel = ev.selection.rows if ev.selection else []
    if filas_sel and filas_sel[0] < len(uc_f):
        _ficha_registro(uc_f, filas_sel[0], key_suffix="_uc_ptgas_srv")


def _mostrar_multiexpediente():
    """Muestra personas con expedientes en sectores distintos."""
    _título("Personal — Multiexpediente")

    multi_path = DIR_NOMINAS / "multiexpediente.parquet"
    activ_path = DIR_NOMINAS / "multiexpediente_actividad.parquet"
    if not multi_path.exists():
        st.warning("Fichero no encontrado. Ejecuta la Fase 1 primero.")
        return

    multi = pl.read_parquet(multi_path)
    if multi.is_empty():
        st.info("No hay personas con expedientes en sectores distintos.")
        return

    actividad = pl.read_parquet(activ_path) if activ_path.exists() else None

    _COMBINACIONES = [
        ("PTGAS + PDI", {"PTGAS", "PDI"}),
        ("PTGAS + PVI", {"PTGAS", "PVI"}),
        ("PDI + PVI", {"PDI", "PVI"}),
        ("PTGAS + PDI + PVI", {"PTGAS", "PDI", "PVI"}),
    ]

    tab_labels = []
    tab_dfs = []
    for label, combo in _COMBINACIONES:
        # Filtrar personas cuyo conjunto de sectores contenga exactamente combo
        df_combo = multi.filter(
            pl.col("sectores").list.eval(
                pl.element().is_in(list(combo))
            ).list.sum() == len(combo)
        )
        tab_labels.append(f"{label} ({len(df_combo):,})")
        tab_dfs.append((combo, df_combo))

    tabs = st.tabs(tab_labels)
    for tab, (combo, df_combo) in zip(tabs, tab_dfs):
        with tab:
            if df_combo.is_empty():
                st.info("No hay personas en esta combinación.")
                continue

            # Mostrar columnas relevantes: per_id, persona, n_PTGAS, n_PDI, n_PVI
            cols_mostrar = ["per_id"]
            for s in sorted(combo):
                col = f"n_{s}"
                if col in df_combo.columns:
                    cols_mostrar.append(col)
            df_vista = df_combo.select(cols_mostrar)
            df_vista = _enriquecer_per_id(df_vista)

            combo_slug = "_".join(sorted(combo)).lower()
            df_vista = _filtro_tabla(df_vista, f"multi_{combo_slug}")
            ev = _st_df(
                df_vista,
                on_select="rerun",
                selection_mode="single-row",
                key=f"multi_df_{combo_slug}",
            )

            # Detalle de la persona seleccionada: 12 meses
            filas_sel = ev.selection.rows if ev.selection else []
            if filas_sel and actividad is not None:
                fila = df_vista.row(filas_sel[0], named=True)
                per_id = fila["per_id"]
                persona = fila.get("persona", "")
                _lbl = f"{per_id} ({persona})" if persona else str(per_id)
                st.divider()
                st.subheader(f"Actividad mensual de {_lbl}")

                # Expedientes de esta persona
                act_persona = actividad.filter(pl.col("per_id") == per_id)
                if act_persona.is_empty():
                    st.info("Sin actividad registrada.")
                    continue

                # Cargar nóminas para obtener importes mensuales
                nom_path = DIR_ENTRADA / "nóminas" / "nóminas y seguridad social.xlsx"
                exps_persona = act_persona.select("expediente").unique()["expediente"]
                nom_mensual = None
                if nom_path.exists():
                    nom_raw = _load_excel(str(nom_path)).filter(
                        pl.col("expediente").is_in(exps_persona)
                    )
                    nom_mensual = (
                        nom_raw
                        .with_columns(pl.col("fecha").dt.month().alias("mes"))
                        .group_by("expediente", "mes")
                        .agg(pl.col("importe").sum().alias("importe"))
                    )

                # Tabla: filas = expedientes, columnas = meses 1..12 con importe
                _MESES = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
                           "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
                exps = (
                    act_persona.select("expediente", "sector_final")
                    .unique()
                    .sort("expediente")
                )
                filas_tabla = []
                for row in exps.iter_rows(named=True):
                    exp_id = row["expediente"]
                    sector = row["sector_final"]
                    fila_dict: dict = {"expediente": exp_id, "sector": sector}
                    for m in range(1, 13):
                        imp = 0.0
                        if nom_mensual is not None:
                            vals = nom_mensual.filter(
                                (pl.col("expediente") == exp_id) & (pl.col("mes") == m)
                            )
                            if not vals.is_empty():
                                imp = float(vals["importe"][0])
                        fila_dict[_MESES[m - 1]] = imp if imp != 0.0 else None
                    filas_tabla.append(fila_dict)

                df_meses = pl.DataFrame(filas_tabla)
                ev_meses = _st_df(
                    df_meses, totales=False,
                    on_select="rerun",
                    selection_mode="single-row",
                    key=f"multi_meses_{combo_slug}_{per_id}",
                )

                # Detalle del expediente seleccionado
                filas_exp = ev_meses.selection.rows if ev_meses.selection else []
                if filas_exp:
                    exp_sel = df_meses.row(filas_exp[0], named=True)
                    exp_id = exp_sel["expediente"]
                    sector_exp = exp_sel["sector"]
                    st.divider()
                    st.subheader(f"Expediente {exp_id} ({sector_exp})")

                    nom_path = DIR_ENTRADA / "nóminas" / "nóminas y seguridad social.xlsx"
                    if not nom_path.exists():
                        st.warning(f"Fichero no encontrado: `{nom_path}`")
                    else:
                        detalle = _load_excel(str(nom_path)).filter(
                            pl.col("expediente") == exp_id
                        )
                        if detalle.is_empty():
                            st.info("Sin registros de nómina para este expediente.")
                        else:
                            total = float(detalle["importe"].sum())
                            st.metric("Total cobrado", _fmt_euro(total))
                            _st_df(
                                detalle,
                                key=f"multi_det_{combo_slug}_{per_id}_{exp_id}",
                            )


def _mostrar_persona():
    """Muestra el reparto de SS por persona (mono o multiexpediente)."""
    _título("Personal — Persona")

    ss_path = DIR_NOMINAS / "persona_ss.parquet"
    uc_path = DIR_NOMINAS / "persona_uc.parquet"
    if not ss_path.exists():
        st.warning("Fichero no encontrado. Ejecuta la Fase 1 primero.")
        return

    reparto = pl.read_parquet(ss_path)
    if reparto.is_empty():
        st.info("No hay datos de reparto de SS.")
        return

    # Lista de personas con al menos una fila de reparto
    personas_ids = reparto["per_id"].unique().sort().to_list()

    # Construir tabla de personas para el selector
    personas_path = DIR_ENTRADA / "nóminas" / "personas.xlsx"
    if personas_path.exists():
        personas_ref = _load_excel(str(personas_path)).select(
            pl.col("per_id"),
            pl.concat_str(
                [pl.col("nombre"), pl.col("apellido1"), pl.col("apellido2")],
                separator=" ", ignore_nulls=True,
            ).alias("persona"),
        )
        personas_df = (
            pl.DataFrame({"per_id": personas_ids})
            .join(personas_ref, on="per_id", how="left")
        )
    else:
        personas_df = pl.DataFrame({
            "per_id": personas_ids,
            "persona": [str(p) for p in personas_ids],
        })

    # Selector con búsqueda
    opciones = {
        row["per_id"]: f"{row['per_id']} — {row['persona']}"
        for row in personas_df.iter_rows(named=True)
    }
    seleccion = st.selectbox(
        "Selecciona persona",
        options=list(opciones.keys()),
        format_func=lambda x: opciones[x],
        key="persona_selector",
    )
    if seleccion is None:
        return

    per_id = seleccion
    nombre = opciones[per_id]
    st.subheader(nombre)

    # Expedientes de esta persona
    exp_path = DIR_ENTRADA / "nóminas" / "expedientes recursos humanos.xlsx"
    if exp_path.exists():
        exp_df = _load_excel(str(exp_path)).filter(pl.col("per_id") == per_id)
        if not exp_df.is_empty():
            st.caption(f"Expedientes: {', '.join(str(e) for e in exp_df['expediente'].to_list())}")

    # Datos de reparto de esta persona
    datos = reparto.filter(pl.col("per_id") == per_id).drop("per_id")

    if datos.is_empty():
        st.info("Sin UC asociadas a esta persona.")
        return

    # Métricas
    total_uc = float(datos["importe_uc"].sum())
    ss_total = float(datos["ss_total"].max()) if "ss_total" in datos.columns else 0.0
    c1, c2 = st.columns(2)
    c1.metric("Total retribuciones (UC)", _fmt_euro(total_uc))
    c2.metric("Total seguridad social", _fmt_euro(ss_total))

    # Tabla de reparto por (actividad, centro_de_coste)
    st.divider()
    st.subheader("Reparto por actividad y centro de coste")
    vista = datos.select(
        "actividad", "centro_de_coste", "importe_uc", "pct", "ss_proporcional",
    ).rename({
        "importe_uc": "importe retributivo",
        "pct": "% del total",
        "ss_proporcional": "SS proporcional",
    })
    _st_df(vista, key="persona_reparto")

    # UC detalladas de esta persona (retributivas + costes sociales)
    if uc_path.exists():
        uc_persona = pl.read_parquet(uc_path).filter(pl.col("per_id") == per_id).drop("per_id")
        if not uc_persona.is_empty():
            st.divider()
            st.subheader("Unidades de coste asociadas")

            # Columnas a mostrar (las que tengan datos)
            cols_mostrar = [
                c for c in [
                    "tipo", "id", "expediente", "elemento_de_coste",
                    "centro_de_coste", "actividad", "importe",
                    "origen", "origen_id", "origen_porción",
                    "regla_actividad", "regla_cc", "regla_ec",
                ] if c in uc_persona.columns
            ]
            uc_vista = uc_persona.select(cols_mostrar)
            uc_vista = _filtro_tabla(uc_vista, "persona_uc_filtro")
            ev_uc = _st_df(
                uc_vista,
                on_select="rerun",
                selection_mode="single-row",
                key="persona_uc_detalle",
            )

            filas_sel = ev_uc.selection.rows if ev_uc.selection else []
            if filas_sel and filas_sel[0] < len(uc_vista):
                _ficha_registro(uc_vista, filas_sel[0], key_suffix="_persona_uc")


def _mostrar_personal():
    seccion = st.session_state.personal_seccion
    if seccion == "Anomalías PDI":
        _mostrar_anomalias_pdi()
        return
    if seccion == "Multiexpediente":
        _mostrar_multiexpediente()
        return
    if seccion == "Persona":
        _mostrar_persona()
        return
    _título(f"Personal — {seccion}")

    if seccion == "Resumen":
        total_exp = 0
        total_importe = 0.0
        filas: list[tuple[str, int, float]] = []
        for label, fichero in _PERSONAL_PARQUETS.items():
            path = DIR_NOMINAS / fichero
            if path.exists():
                df = pl.read_parquet(path)
                n = len(df)
                imp = float(df["importe"].sum()) if not df.is_empty() else 0.0
                total_exp += n
                total_importe += imp
                filas.append((label, n, imp))
        c1, c2 = st.columns(2)
        c1.metric("Expedientes totales", f"{total_exp:,}")
        c2.metric("Importe total", _fmt_euro(total_importe))
        if filas:
            st.divider()
            resumen_df = pl.DataFrame(
                {"categoría": [f[0] for f in filas],
                 "expedientes": [f[1] for f in filas],
                 "importe": [f[2] for f in filas]},
            )
            _st_df(resumen_df)
        return

    fichero = _PERSONAL_PARQUETS.get(seccion)
    if fichero is None:
        st.warning(f"Sección desconocida: {seccion}")
        return

    path = DIR_NOMINAS / fichero
    if not path.exists():
        st.warning(f"Fichero no encontrado: `{path}`. Ejecuta `uv run fase1` primero.")
        return

    df = pl.read_parquet(path)
    df = _enriquecer_per_id(df)
    df = _filtro_tabla(df, f"personal_{seccion}")
    ev = _st_df(df,
        on_select="rerun",
        selection_mode="single-row",
        key=f"personal_df_{seccion}",
    )

    # Detalle: líneas de nómina del expediente seleccionado
    filas_sel = ev.selection.rows if ev.selection else []
    if filas_sel:
        fila = df.row(filas_sel[0], named=True)
        expediente = fila["expediente"]
        persona = fila.get("persona", "")
        _lbl_exp = f"{expediente} ({persona})" if persona else str(expediente)
        st.divider()
        nom_path = DIR_ENTRADA / "nóminas" / "nóminas y seguridad social.xlsx"
        if not nom_path.exists():
            st.warning(f"Fichero no encontrado: `{nom_path}`")
        elif seccion in ("Expedientes PTGAS", "Expedientes PDI", "Expedientes PVI"):
            # Desglose en 3 tablas
            nominas = _load_excel(str(nom_path))
            detalle = nominas.filter(pl.col("expediente") == expediente)

            solo_filtradas = st.toggle(
                "Solo registros que pasan el filtro de nóminas",
                value=True,
                key="nom_toggle_filtro",
            )
            _PROYECTOS_NÓMINA = ["1G019", "23G019", "02G041", "11G006", "1G046", "00000"]
            if solo_filtradas:
                es_ss = pl.col("aplicación").cast(pl.Utf8) == "1211"
                es_proyecto = pl.col("proyecto").cast(pl.Utf8).is_in(_PROYECTOS_NÓMINA)
                detalle = detalle.filter(es_ss | es_proyecto)

            st.subheader(f"Líneas de nómina del expediente {_lbl_exp}")

            sec_slug = {"Expedientes PTGAS": "ptgas", "Expedientes PDI": "pdi", "Expedientes PVI": "pvi"}[seccion]
            es_coste_social = pl.col("aplicación").cast(pl.Utf8).str.starts_with("12")

            if seccion in ("Expedientes PVI", "Expedientes PDI"):
                # Enriquecer con tipo_línea
                lineas_path = DIR_ENTRADA / "presupuesto" / "líneas de financiación.xlsx"
                if lineas_path.exists():
                    lineas_ref = _load_excel(str(lineas_path)).select(
                        pl.col("línea"), pl.col("tipo").alias("tipo_línea"),
                    )
                    detalle = detalle.join(lineas_ref, on="línea", how="left")

            if seccion == "Expedientes PVI":
                es_fondos_uji = pl.col("tipo_línea") == "00"
                grupos = [
                    ("Costes sociales", detalle.filter(es_coste_social)),
                    ("Retribuciones con cargo a fondos UJI", detalle.filter(~es_coste_social & es_fondos_uji)),
                    ("Retribuciones con cargo a financiación afectada", detalle.filter(~es_coste_social & ~es_fondos_uji)),
                ]
            elif seccion == "Expedientes PDI":
                no_cs = detalle.filter(~es_coste_social)
                finalista = no_cs.filter(pl.col("tipo_línea") != "00")
                resto = no_cs.filter(pl.col("tipo_línea") == "00")
                docencia = resto.filter(
                    pl.col("proyecto").is_in(["1G019", "23G019"])
                    & pl.col("concepto_retributivo").cast(pl.Utf8).is_in(["17", "20", "24", "44", "86", "99"])
                )
                gestion = resto.filter(
                    pl.col("proyecto").is_in(["02G041", "11G006", "1G019", "23G019"])
                    & pl.col("concepto_retributivo").cast(pl.Utf8).is_in(["19", "30"])
                )
                investigacion = resto.filter(
                    pl.col("proyecto").is_in(["1G019", "23G019"])
                    & pl.col("concepto_retributivo").cast(pl.Utf8).is_in(["26", "77"])
                )
                incentivos = resto.filter(
                    pl.col("concepto_retributivo").cast(pl.Utf8).is_in(["13", "67"])
                )
                # Regla 23: lo que queda en resto tras quitar los grupos anteriores
                ids_clasificados = pl.concat([docencia, gestion, investigacion, incentivos]).select("id")
                regla_23 = resto.join(ids_clasificados, on="id", how="anti")
                grupos = [
                    ("Costes sociales", detalle.filter(es_coste_social)),
                    ("Retribuciones con financiación finalista", finalista),
                    ("Retribuciones ordinarias docencia", docencia),
                    ("Retribuciones ordinarias gestión", gestion),
                    ("Retribuciones ordinarias investigación", investigacion),
                    ("Retribuciones por incentivos", incentivos),
                    ("Retribuciones para regla 23", regla_23),
                ]
            else:
                # PTGAS
                es_proyecto_ord = pl.col("proyecto").is_in(["1G019", "23G019"])
                grupos = [
                    ("Costes sociales", detalle.filter(es_coste_social)),
                    ("Retribuciones ordinarias", detalle.filter(~es_coste_social & es_proyecto_ord)),
                    ("Retribuciones extra", detalle.filter(~es_coste_social & ~es_proyecto_ord)),
                ]
                # UC generadas a partir de retribuciones ordinarias PTGAS
                uc_ptgas_path = DIR_FASE1 / "auxiliares" / "nóminas" / "uc_ptgas.parquet"
                uc_ptgas_exp = pl.DataFrame()
                if uc_ptgas_path.exists():
                    uc_ptgas_all = pl.read_parquet(uc_ptgas_path)
                    uc_ptgas_exp = uc_ptgas_all.filter(pl.col("expediente") == expediente).drop("expediente")
                    if not uc_ptgas_exp.is_empty():
                        grupos.append(("UC retrib. ordinarias", uc_ptgas_exp))
                # UC inyectadas desde presupuesto
                uc_iny_path = DIR_FASE1 / "auxiliares" / "nóminas" / "uc_presupuesto_en_nóminas.parquet"
                uc_iny_exp = pl.DataFrame()
                if uc_iny_path.exists():
                    uc_iny_all = pl.read_parquet(uc_iny_path)
                    uc_iny_exp = uc_iny_all.filter(pl.col("expediente") == expediente).drop("expediente")
                    if not uc_iny_exp.is_empty():
                        grupos.append(("UC desde presupuesto", uc_iny_exp))

                # Agrupación por actividades: todas las UC del expediente
                _uc_partes = [
                    df_uc for df_uc in [uc_ptgas_exp, uc_iny_exp]
                    if not df_uc.is_empty() and "actividad" in df_uc.columns
                ]
                if _uc_partes:
                    todas_uc_exp = pl.concat(_uc_partes, how="diagonal")
                    total_uc = float(todas_uc_exp["importe"].sum())
                    total_ss = float(detalle.filter(es_coste_social)["importe"].sum())
                    agrup_act = (
                        todas_uc_exp
                        .group_by("actividad")
                        .agg(pl.col("importe").sum().alias("importe"))
                        .sort("importe", descending=True)
                        .with_columns([
                            (pl.col("importe") / total_uc * 100).round(2).alias("% del total")
                            if total_uc else pl.lit(0.0).alias("% del total"),
                            (pl.col("importe") / total_uc * total_ss).round(2).alias("SS proporcional")
                            if total_uc else pl.lit(0.0).alias("SS proporcional"),
                        ])
                    )
                    grupos.append(("Agrupación por actividades", agrup_act))

            # Verificación cruzada de totales (excluir UC generadas)
            grupos_nómina = [g for g in grupos if not g[0].startswith("UC ")]
            importe_subtablas = sum(float(g[1]["importe"].sum()) for g in grupos_nómina)
            total_expediente = float(detalle["importe"].sum())
            diferencia = abs(importe_subtablas - total_expediente)
            if diferencia < 0.01:
                st.success("Los totales coinciden.")
            else:
                st.warning(
                    f"Diferencia de {_fmt_euro(diferencia)} entre subtablas "
                    f"({_fmt_euro(importe_subtablas)}) y total ({_fmt_euro(total_expediente)})"
                )

            # Tabs: una por grupo (con registros y importe en el nombre)
            grupos_no_vacíos = [
                (nombre, df_g) for nombre, df_g in grupos if not df_g.is_empty()
            ]
            if not grupos_no_vacíos:
                st.info("Sin registros.")
                return

            tab_labels = []
            for nombre, df_g in grupos_no_vacíos:
                n_reg = len(df_g)
                imp = float(df_g["importe"].sum()) if "importe" in df_g.columns else 0.0
                tab_labels.append(f"{nombre} ({n_reg:,} · {_fmt_euro(imp)})")

            tabs = st.tabs(tab_labels)
            for tab, (nombre_grupo, df_grupo) in zip(tabs, grupos_no_vacíos):
                n_reg = len(df_grupo)
                importe_grupo = float(df_grupo["importe"].sum()) if "importe" in df_grupo.columns else 0.0
                slug = nombre_grupo.lower().replace(" ", "_")

                with tab:
                    df_grupo_f = _filtro_tabla(df_grupo, f"{sec_slug}_det_{slug}")
                    ev_g = _st_df(df_grupo_f,
                        on_select="rerun",
                        selection_mode="single-row",
                        key=f"{sec_slug}_det_df_{slug}",
                    )

                    # Resumen por concepto retributivo
                    if "concepto_retributivo" in df_grupo.columns and "importe" in df_grupo.columns:
                        resumen_cr = (
                            df_grupo
                            .group_by("concepto_retributivo")
                            .agg(
                                pl.col("importe").sum().alias("importe"),
                                pl.len().alias("registros"),
                            )
                            .sort("concepto_retributivo")
                        )
                        # Enriquecer con nombre del concepto retributivo
                        cr_path = DIR_ENTRADA / "nóminas" / "conceptos retributivos.xlsx"
                        if cr_path.exists():
                            cr_ref = _load_excel(str(cr_path))
                            resumen_cr = resumen_cr.join(
                                cr_ref.select("concepto_retributivo", "nombre"),
                                on="concepto_retributivo",
                                how="left",
                            )
                        st.caption("Resumen por concepto retributivo")
                        _st_df(resumen_cr, totales=False,
                            key=f"{sec_slug}_resumen_cr_{slug}",
                        )

                    # Alerta si retribuciones ordinarias tiene proyectos inesperados
                    if nombre_grupo == "Retribuciones ordinarias" and "proyecto" in df_grupo.columns:
                        proyectos_otros = (
                            df_grupo
                            .filter(~pl.col("proyecto").is_in(["1G019", "23G019"]))
                            .get_column("proyecto")
                            .unique()
                            .to_list()
                        )
                        if proyectos_otros:
                            st.error(
                                f"Proyectos inesperados en retribuciones ordinarias: "
                                f"{', '.join(str(p) for p in proyectos_otros)}"
                            )

                    filas_g = ev_g.selection.rows if ev_g.selection else []
                    if filas_g and filas_g[0] < len(df_grupo_f):
                        _ficha_registro(df_grupo_f, filas_g[0], key_suffix=f"_{sec_slug}_{slug}")

            # Docencia del PDI
            if seccion == "Expedientes PDI" and "per_id" in fila:
                per_id = fila["per_id"]
                dir_doc = DIR_ENTRADA / "docencia"
                doc_path = dir_doc / "docencia.xlsx"
                doc_pq = dir_doc / "_parquet" / "docencia.parquet"
                if doc_path.exists() or doc_pq.exists():
                    # Usar read_excel directamente (sin @st.cache_data) para evitar
                    # problemas de caché de streamlit con las tablas de referencia.
                    from coana.util import read_excel as _read_xl
                    docencia_df = _read_xl(str(doc_path))
                    doc_per = docencia_df.filter(pl.col("per_id") == per_id)
                    if not doc_per.is_empty():
                        df_doc = doc_per.select("asignatura", "créditos_impartidos")

                        # Cadena de joins: asignatura → grado/máster → estudio → titulación
                        # Vía grados
                        _tit_grado = pl.DataFrame(schema={"asignatura": pl.Utf8, "nombre_asig": pl.Utf8, "titulación": pl.Utf8})
                        ag_path = dir_doc / "asignaturas grados.xlsx"
                        gr_path = dir_doc / "grados.xlsx"
                        est_path = dir_doc / "estudios.xlsx"
                        if ag_path.exists() and gr_path.exists() and est_path.exists():
                            ag = _read_xl(str(ag_path))
                            gr = _read_xl(str(gr_path))
                            est = _read_xl(str(est_path))
                            _tit_grado = (
                                ag.select(pl.col("asignatura"), pl.col("nombre").alias("nombre_asig"), pl.col("grado"))
                                .join(gr.select("grado", "estudio"), on="grado", how="left")
                                .join(est.select("estudio", pl.col("nombre").alias("titulación")), on="estudio", how="left")
                                .select("asignatura", "nombre_asig", "titulación")
                            )

                        # Vía másteres
                        _tit_master = pl.DataFrame(schema={"asignatura": pl.Utf8, "nombre_asig": pl.Utf8, "titulación": pl.Utf8})
                        am_path = dir_doc / "asignaturas másteres.xlsx"
                        mr_path = dir_doc / "másteres.xlsx"
                        if am_path.exists() and mr_path.exists() and est_path.exists():
                            am = _read_xl(str(am_path))
                            mr = _read_xl(str(mr_path))
                            est = _read_xl(str(est_path))
                            _tit_master = (
                                am.select(pl.col("asignatura"), pl.col("nombre").alias("nombre_asig"), pl.col("máster"))
                                .join(mr.select("máster", "estudio"), on="máster", how="left")
                                .join(est.select("estudio", pl.col("nombre").alias("titulación")), on="estudio", how="left")
                                .select("asignatura", "nombre_asig", "titulación")
                            )

                        # Unir ambas tablas de resolución
                        tit_ref = pl.concat([_tit_grado, _tit_master]).unique(subset=["asignatura"])

                        # Join con las asignaturas del docente
                        df_docencia = (
                            df_doc
                            .join(tit_ref, on="asignatura", how="left")
                            .with_columns(
                                pl.col("nombre_asig").fill_null(pl.col("asignatura")).alias("nombre"),
                                pl.col("titulación").fill_null("Desconocida"),
                                pl.col("créditos_impartidos").fill_null(0.0).alias("créditos"),
                            )
                            .select("asignatura", "nombre", "créditos", "titulación")
                        )

                        total_creditos = df_docencia["créditos"].sum()

                        # Resumen por titulación
                        resumen_tit = (
                            df_docencia
                            .group_by("titulación")
                            .agg(pl.col("créditos").sum())
                            .sort("créditos", descending=True)
                        )

                        with st.expander(f"Docencia — {len(df_docencia)} asignaturas · {total_creditos:.1f} créditos"):
                            st.caption("Resumen por titulación")
                            st.dataframe(resumen_tit, use_container_width=True, hide_index=True, key="pdi_doc_resumen_tit")
                            st.caption("Detalle por asignatura")
                            st.dataframe(df_docencia, use_container_width=True, hide_index=True, key="pdi_doc_detalle")

        else:
            st.subheader(f"Líneas de nómina del expediente {_lbl_exp}")
            nominas = _load_excel(str(nom_path))
            detalle = nominas.filter(pl.col("expediente") == expediente)
            st.caption(f"{len(detalle):,} registros")
            detalle = _filtro_tabla(detalle, f"personal_detalle_{seccion}")
            ev_det = _st_df(detalle,
                on_select="rerun",
                selection_mode="single-row",
                key=f"personal_detalle_df_{seccion}",
            )

            # Ficha del registro seleccionado
            filas_det = ev_det.selection.rows if ev_det.selection else []
            if filas_det and filas_det[0] < len(detalle):
                _ficha_registro(detalle, filas_det[0])


# ============================================================
# Resultados
# ============================================================


def _cargar_todas_uc() -> pl.DataFrame:
    """Carga y concatena todas las UC disponibles con columnas comunes."""
    cols = ["id", "elemento_de_coste", "centro_de_coste", "actividad", "importe", "origen"]
    frames: list[pl.DataFrame] = []
    for nombre in ["uc presupuesto", "uc amortizaciones", "uc suministros"]:
        path = DIR_FASE1 / f"{nombre}.parquet"
        if path.exists():
            df = pl.read_parquet(path)
            df = df.select([c for c in cols if c in df.columns])
            frames.append(df)
    if not frames:
        return pl.DataFrame(schema={c: pl.Utf8 if c != "importe" else pl.Float64 for c in cols})
    return pl.concat(frames, how="diagonal_relaxed")


def _mostrar_anomalias_uc():
    """Muestra UC cuya actividad, centro de coste o elemento de coste no existe en el árbol."""
    from coana.util import Árbol

    _título("Anomalías UC", "Resultados — Anomalías UC")
    st.caption("Unidades de coste que referencian nodos inexistentes en los árboles de fase 1.")

    # Cargar árboles
    _ARBOLES = {
        "actividades": "actividad",
        "centros de coste": "centro_de_coste",
        "elementos de coste": "elemento_de_coste",
    }
    arboles: dict[str, set[str]] = {}
    for nombre_tree, _ in _ARBOLES.items():
        tree_path = DIR_FASE1 / f"{nombre_tree}.tree"
        if tree_path.exists():
            arbol = Árbol.from_file(tree_path)
            arboles[nombre_tree] = set(arbol._por_id.keys())
        else:
            arboles[nombre_tree] = set()

    # Cargar UC por fuente
    _FUENTES = [
        ("presupuesto", "uc presupuesto"),
        ("amortizaciones", "uc amortizaciones"),
        ("suministros", "uc suministros"),
    ]

    hay_anomalias = False

    for nombre_fuente, nombre_fichero in _FUENTES:
        path = DIR_FASE1 / f"{nombre_fichero}.parquet"
        if not path.exists():
            continue
        df = pl.read_parquet(path)
        if df.is_empty():
            continue

        anomalias_fuente: list[tuple[str, str, pl.DataFrame]] = []

        for nombre_tree, col_uc in _ARBOLES.items():
            if col_uc not in df.columns:
                continue
            ids_arbol = arboles.get(nombre_tree, set())
            if not ids_arbol:
                continue
            # UC con valor no nulo que no está en el árbol
            huerfanos = df.filter(
                pl.col(col_uc).is_not_null()
                & ~pl.col(col_uc).is_in(list(ids_arbol))
            )
            if not huerfanos.is_empty():
                anomalias_fuente.append((nombre_tree, col_uc, huerfanos))

        if not anomalias_fuente:
            continue

        hay_anomalias = True
        st.header(f"Fuente: {nombre_fuente}")

        for nombre_tree, col_uc, huerfanos in anomalias_fuente:
            n_uc = len(huerfanos)
            importe = float(huerfanos["importe"].sum()) if "importe" in huerfanos.columns else 0.0
            ids_faltantes = huerfanos[col_uc].unique().sort().to_list()

            with st.expander(
                f"{nombre_tree.capitalize()}: {n_uc:,} UC con nodos inexistentes — {_fmt_euro(importe)}",
                expanded=True,
            ):
                st.markdown(f"**{len(ids_faltantes)}** identificadores no encontrados en `{nombre_tree}.tree`:")

                # Tabla resumen por identificador faltante
                resumen = (
                    huerfanos
                    .group_by(col_uc)
                    .agg(
                        pl.len().alias("nº UC"),
                        pl.col("importe").sum().alias("importe"),
                    )
                    .sort(pl.col("importe").abs(), descending=True)
                    .rename({col_uc: "identificador"})
                )
                _st_df(resumen)

                # Detalle: todas las UC afectadas
                with st.expander("Ver todas las UC afectadas"):
                    cols_mostrar = [c for c in huerfanos.columns if not c.startswith("_")]
                    detalle = huerfanos.select(cols_mostrar)
                    detalle_f = _filtro_tabla(detalle, f"anom_{nombre_fuente}_{col_uc}")
                    _st_df(detalle_f)

    if not hay_anomalias:
        st.success("No se han encontrado anomalías: todas las UC referencian nodos existentes en los árboles.")


def _mostrar_resultados():
    from coana.util import Árbol

    seccion = st.session_state.resultados_seccion

    if seccion == "Anomalías UC":
        _mostrar_anomalias_uc()
        return

    _título(f"Resultados — {seccion}")

    uc = _cargar_todas_uc()
    if uc.is_empty():
        st.warning("No hay unidades de coste. Ejecuta `uv run fase1` primero.")
        return

    if seccion == "Resumen":
        n_total = len(uc)
        importe_total = float(uc["importe"].sum())
        c1, c2 = st.columns(2)
        c1.metric("Total de UC", f"{n_total:,}")
        c2.metric("Importe total", _fmt_euro(importe_total))
        if "origen" in uc.columns:
            st.divider()
            por_origen = (
                uc.group_by("origen")
                .agg(pl.len().alias("n"), pl.col("importe").sum().alias("importe"))
                .sort("origen")
            )
            _st_df(por_origen)
        return

    if seccion == "Todas las UC":
        uc_f = _filtro_tabla(uc, "resultados_todas_uc")
        ev = _st_df(
            uc_f,
            on_select="rerun",
            selection_mode="single-row",
            key="resultados_todas_uc_df",
        )
        filas_sel = ev.selection.rows if ev.selection else []
        if filas_sel and filas_sel[0] < len(uc_f):
            _ficha_registro(uc_f, filas_sel[0], key_suffix="_res_todas")
        return

    # Mapeo sección → columna de agrupación y fichero tree
    _CONF = {
        "Actividades": ("actividad", "actividades"),
        "Centros de coste": ("centro_de_coste", "centros de coste"),
        "Elementos de coste": ("elemento_de_coste", "elementos de coste"),
    }
    col_grupo, nombre_tree = _CONF[seccion]

    # Cargar árbol y construir lista ordenada de nodos (DFS)
    tree_path = DIR_FASE1 / f"{nombre_tree}.tree"
    nodos_ordenados: list[tuple[str, str, str]] = []  # (identificador, código, descripción)
    if tree_path.exists():
        arbol = Árbol.from_file(tree_path)
        def _dfs(nodo):
            if nodo.identificador:  # saltar raíz sin identificador
                nodos_ordenados.append((nodo.identificador, nodo.código, nodo.descripción))
            for hijo in nodo.hijos:
                _dfs(hijo)
        _dfs(arbol.raíz)

    # Nombre de la columna de agrupación en la tabla final
    _COL_LABEL = {
        "actividad": "actividad",
        "centro_de_coste": "centro de coste",
        "elemento_de_coste": "elemento de coste",
    }
    col_label = _COL_LABEL.get(col_grupo, col_grupo)

    # Agregar por grupo y origen
    _ORIGEN_COLS = {
        "presupuesto": "pto €",
        "inventario": "amort €",
        "nómina": "nóm €",
    }

    resumen = (
        uc
        .group_by(col_grupo, "origen")
        .agg(pl.col("importe").sum().alias("importe"))
    )

    # Base: todos los nodos del árbol, en orden
    if nodos_ordenados:
        tabla = pl.DataFrame({
            "_slug": [n[0] for n in nodos_ordenados],
            "nº": list(range(1, len(nodos_ordenados) + 1)),
            "código": [n[1] for n in nodos_ordenados],
            col_label: [n[2] for n in nodos_ordenados],
            "etq": [n[0] for n in nodos_ordenados],
        })
    else:
        # Sin árbol: usar solo los slugs presentes en las UC
        slugs = uc.select(col_grupo).unique().sort(col_grupo).to_series().to_list()
        tabla = pl.DataFrame({
            "_slug": slugs,
            "nº": list(range(1, len(slugs) + 1)),
            "código": [""] * len(slugs),
            col_label: slugs,
            "etq": slugs,
        })

    for origen, col_eur in _ORIGEN_COLS.items():
        sub = resumen.filter(pl.col("origen") == origen).select(
            pl.col(col_grupo).alias("_slug"),
            pl.col("importe").alias(col_eur),
        )
        tabla = tabla.join(sub, on="_slug", how="left")

    # Suministros: agrupar energía+agua+gas en "sumin €"
    _ORÍGENES_SUMINISTRO = ("energía", "agua", "gas")
    sub_sumin = (
        resumen.filter(pl.col("origen").is_in(_ORÍGENES_SUMINISTRO))
        .group_by(col_grupo)
        .agg(pl.col("importe").sum().alias("sumin €"))
        .select(pl.col(col_grupo).alias("_slug"), "sumin €")
    )
    tabla = tabla.join(sub_sumin, on="_slug", how="left")

    # Rellenar nulos con 0
    for col_eur in list(_ORIGEN_COLS.values()) + ["sumin €"]:
        if col_eur in tabla.columns:
            tabla = tabla.with_columns(pl.col(col_eur).fill_null(0.0))

    # Total
    cols_eur = [c for c in list(_ORIGEN_COLS.values()) + ["sumin €"] if c in tabla.columns]
    tabla = tabla.with_columns(
        pl.sum_horizontal(*[pl.col(c) for c in cols_eur]).alias("total"),
    )

    # Ordenar columnas y eliminar _slug interno
    orden = ["nº", "código", col_label, "etq"] + cols_eur + ["total"]
    orden = [c for c in orden if c in tabla.columns]
    tabla = tabla.select(orden)

    tabla_f = _filtro_tabla(tabla, f"resultados_{col_grupo}")
    ev = _st_df(tabla_f,
        on_select="rerun",
        selection_mode="single-row",
        key=f"resultados_df_{col_grupo}",
    )

    # Detalle al seleccionar una fila
    filas_sel = ev.selection.rows if ev.selection else []
    if filas_sel:
        fila = tabla_f.row(filas_sel[0], named=True)
        slug = fila.get("etq", "")
        desc = fila.get(col_label, slug)
        st.divider()
        st.subheader(f"Unidades de coste: {slug}" + (f" — {desc}" if desc else ""))

        # Importe del nodo y acumulado con descendientes
        uc_detalle = uc.filter(pl.col(col_grupo) == slug)
        importe_nodo = float(uc_detalle["importe"].sum()) if not uc_detalle.is_empty() else 0.0

        # Recopilar descendientes del nodo en el árbol
        ids_descendientes = {slug}
        if tree_path.exists():
            def _recoger_ids(nodo):
                for hijo in nodo.hijos:
                    ids_descendientes.add(hijo.identificador)
                    _recoger_ids(hijo)
            nodo_sel = arbol._por_id.get(slug)
            if nodo_sel:
                _recoger_ids(nodo_sel)

        uc_acumulado = uc.filter(pl.col(col_grupo).is_in(list(ids_descendientes)))
        importe_acumulado = float(uc_acumulado["importe"].sum()) if not uc_acumulado.is_empty() else 0.0

        # Desglose por origen para nodo y acumulado
        def _desglose_origen(df_uc):
            if df_uc.is_empty():
                return {}
            return dict(
                df_uc.group_by("origen")
                .agg(pl.col("importe").sum())
                .iter_rows()
            )

        desglose_nodo = _desglose_origen(uc_detalle)
        desglose_acum = _desglose_origen(uc_acumulado)
        _ETIQUETAS_ORIGEN = {
            "presupuesto": "Presupuesto",
            "inventario": "Amortizaciones",
            "nómina": "Nóminas",
            "energía": "Energía",
            "agua": "Agua",
            "gas": "Gas",
        }
        orígenes_presentes = sorted(
            set(desglose_nodo.keys()) | set(desglose_acum.keys()),
            key=lambda o: list(_ETIQUETAS_ORIGEN.keys()).index(o) if o in _ETIQUETAS_ORIGEN else 99,
        )

        filas_desglose = []
        for origen in orígenes_presentes:
            filas_desglose.append({
                "origen": _ETIQUETAS_ORIGEN.get(origen, origen),
                "nodo €": round(desglose_nodo.get(origen, 0.0), 2),
                "acumulado €": round(desglose_acum.get(origen, 0.0), 2),
            })
        filas_desglose.append({
            "origen": "TOTAL",
            "nodo €": round(importe_nodo, 2),
            "acumulado €": round(importe_acumulado, 2),
        })

        mc1, mc2 = st.columns(2)
        mc1.metric("UC nodo", f"{len(uc_detalle):,}")
        mc2.metric("UC acumuladas (con descendientes)", f"{len(uc_acumulado):,}")
        st.dataframe(
            pl.DataFrame(filas_desglose),
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"{len(uc_detalle):,} unidades de coste")
        uc_detalle_f = _filtro_tabla(uc_detalle, f"resultados_det_{col_grupo}")
        ev_det = _st_df(uc_detalle_f,
            on_select="rerun",
            selection_mode="single-row",
            key=f"resultados_det_df_{col_grupo}",
        )
        filas_det = ev_det.selection.rows if ev_det.selection else []
        if filas_det and filas_det[0] < len(uc_detalle_f):
            _ficha_registro(uc_detalle_f, filas_det[0], key_suffix=f"_res_{col_grupo}")


# ============================================================
# Despacho de vista
# ============================================================

vista = st.session_state.vista

if vista == "superficies":
    _mostrar_superficies()
elif vista == "amortizaciones":
    _mostrar_amortizaciones()
elif vista == "presupuesto":
    _mostrar_presupuesto()
elif vista == "entradas":
    _mostrar_entradas()
elif vista == "personal":
    _mostrar_personal()
elif vista == "resultados":
    _mostrar_resultados()
else:
    st.title("CoAna")
    st.info("Selecciona una sección en el panel lateral.")
