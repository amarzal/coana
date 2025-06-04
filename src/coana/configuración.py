import textwrap
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from re import I
from typing import Any, TypedDict, cast

import polars as pl
import yaml
from loguru import logger
from typing_extensions import Literal

from coana.misc.traza import Traza
from coana.misc.utils import Singleton
from coana.árbol import Árbol

type DiccionarioDeDiccionarios = dict[str, None | DiccionarioDeDiccionarios]


@dataclass
class Fichero:
    ruta: Path
    cols: dict[str, tuple[str, str]]
    tipo: Literal["excel", "csv", "parquet", "tree", "typst"] = field(init=False)

    def __init__(self, path: Path, cols: dict[str, str]) -> None:
        self.ruta = path
        if self.ruta.suffix == ".xlsx":
            self.tipo = "excel"
        elif self.ruta.suffix == ".csv":
            self.tipo = "csv"
        elif self.ruta.suffix == ".parquet":
            self.tipo = "parquet"
        elif self.ruta.suffix == ".tree":
            self.tipo = "tree"
        elif self.ruta.suffix == ".typ":
            self.tipo = "typst"
        else:
            raise ValueError(f"Formato de fichero no soportado: {self.ruta}")
        self.cols = {}
        for key, value in cols.items():
            value = value.strip()
            (columna_excel, tipo) = value.split(" ") if " " in value else (value, "str")
            self.cols[key] = (columna_excel, tipo)

    def __str__(self) -> str:
        return str(self.ruta)

    def carga_dataframe(self) -> pl.DataFrame:
        if self.tipo not in ("excel", "csv", "parquet"):
            raise ValueError(f"Formato de fichero no soportado: {self.ruta}")
        if not self.ruta.exists():
            raise FileNotFoundError(f"El fichero {self.ruta} no existe")

        parquet_path = self.ruta.with_suffix(".parquet")
        if self.tipo in ("excel", "csv"):
            if not parquet_path.exists() or self.ruta.stat().st_mtime > parquet_path.stat().st_mtime:
                logger.trace(f"Convirtiendo {self.ruta} a {parquet_path} para carga rápida")
                if self.tipo == "excel":
                    df = pl.read_excel(self.ruta)
                else:
                    df = pl.read_csv(self.ruta)
                df.write_parquet(parquet_path)
                self.ruta = parquet_path
                self.tipo = "parquet"
                logger.trace(f"Se ha convertido {self.ruta} en {parquet_path}: {df.shape} filas")

        df = pl.read_parquet(parquet_path)
        for columna, (columna_excel, tipo) in self.cols.items():
            match tipo:
                case "str":
                    df = df.with_columns(pl.col(columna_excel).cast(pl.Utf8).alias(columna))
                case "int":
                    df = df.with_columns(pl.col(columna_excel).cast(pl.Int64).alias(columna))
                case "float":
                    df = df.with_columns(pl.col(columna_excel).cast(pl.Float64).alias(columna))
                case "euro":
                    df = df.with_columns(pl.col(columna_excel).round(2).cast(pl.Decimal(scale=2)).alias(columna))
                case "date":
                    df = df.with_columns(pl.col(columna_excel).cast(pl.Date).alias(columna))
                case _:
                    df = df.with_columns(pl.col(columna_excel).cast(pl.Utf8).alias(columna))
        if self.cols:
            df = df.select(list(self.cols.keys()))
        return df

    def carga_árbol(self, indent_spaces: int = 4) -> "Árbol":
        if self.tipo != "tree":
            raise ValueError(f"El fichero {self.ruta} no es un árbol")
        logger.trace(f"Cargando árbol de {self.ruta}")
        parent_dir = self.ruta.parent
        tree = StringIO()
        with open(self.ruta, "r") as f:
            for línea in f.readlines():
                indent = len(línea) - len(línea.lstrip(" "))
                if indent % indent_spaces != 0:
                    raise ValueError(f"Indentación incorrecta en {self.ruta}: {línea}")
                indent //= indent_spaces
                if "|" not in línea:
                    raise ValueError(f"No se encuentra '|' en {self.ruta}: {línea}")
                desc, label = map(str.strip, línea.split("|"))
                children_filename = parent_dir / f"{label.lower()}.tree"
                tree.write(f"{' ' * indent}{desc} | {label}\n")
                if children_filename.exists():
                    logger.trace(f"  Cargando hijos de {label} desde {children_filename}")
                    with open(children_filename, "r") as fc:
                        for child_line in fc.readlines():
                            child_indent = len(child_line) - len(child_line.lstrip(" "))
                            if child_indent % indent_spaces != 0:
                                raise ValueError(f"Indentación incorrecta en {children_filename}: {child_line}")
                            child_indent = indent + 1 + child_indent // indent_spaces
                            if "|" not in child_line:
                                raise ValueError(f"No se encuentra '|' en {children_filename}: {child_line}")
                            desc, label = map(str.strip, child_line.split("|"))
                            tree.write(f"{' ' * child_indent}{desc} | {label}\n")
        árbol = Árbol.desde_texto_sangrado(tree.getvalue())
        árbol.fichero = self.ruta
        return árbol


@dataclass
class Directorio:
    ruta: Path
    desc: str


@dataclass
class ConfiguraciónPrevisiónSocialDeFuncionarios:
    porcentaje_previsión_social: float
    base_máxima_de_cotización: float


@dataclass
class ConfiguraciónTiposDeProyectoConvertiblesEnActividad:
    investigación_regional: list[str]
    investigación_nacional: list[str]
    investigación_internacional: list[str]
    transferencia: list[str]
    formación_propia: list[str]


@dataclass
class Configuración(metaclass=Singleton):
    raíz_datos: Path = field()
    ficheros: dict[str, Fichero] = field(default_factory=dict)
    directorios: dict[str, Directorio] = field(default_factory=dict)
    traza: Traza = field(init=False)
    cfg: dict[str, Any] = field(init=False)

    def __init__(self, raíz_datos: Path) -> None:
        self.raíz_datos = raíz_datos
        if self.raíz_datos == Path(""):
            raise ValueError("No se ha definido la raíz de los datos")

        self.cfg = yaml.load(open(self.raíz_datos / "configuracion.yaml"), Loader=yaml.FullLoader)

        # Los paths del manifesto pueden utilizar claves de directorios y hay que formar bien las rutas
        ficheros_y_directorios = cast(dict[str, Any], self.cfg.get("ficheros"))
        self.ficheros, self.directorios = {}, {}
        for key, value in ficheros_y_directorios.items():
            if "path" not in value:
                raise ValueError(f"El fichero configuración.yaml en {self.raíz_datos} no ha definido la ruta de {key}")
            path = Path("")
            for part in Path(value["path"]).parts:
                path = path / (self.directorios[part].ruta if part in self.directorios else Path(part))
            if path.suffix == "":  # Es un directorio
                self.directorios[key] = Directorio(path, value.get("desc", ""))
            else:  # Es un fichero, pero nos lo pueden haber expresado con un glob
                self.ficheros[key] = Fichero(path, cols=value.get("cols", {}))

        # Todas las rutas son relativas a la raíz de los datos
        for key, value in list(self.ficheros.items()):
            self.ficheros[key].ruta = self.raíz_datos / value.ruta
        for key, value in list(self.directorios.items()):
            self.directorios[key].ruta = self.raíz_datos / value.ruta

        # Los directorios se crean si no existen
        for directorio in self.directorios.values():
            if not directorio.ruta.exists():
                directorio.ruta.mkdir(parents=True)

        # El fichero se puede expresar con un glob y hay que encontrar el fichero que coincide. Si hay varios, se toma el último
        for clave, fichero in list(self.ficheros.items()):
            ficheros = sorted(Path(".").glob(str(fichero.ruta)))
            if ficheros:
                self.ficheros[clave].ruta = ficheros[-1]

        ruta_traza = self.ficheros.get("traza", None)
        self.traza = Traza(ruta_traza.ruta if ruta_traza is not None else None)
        self._traza()

    @property
    def año(self) -> int:
        return cast(int, self.cfg.get("año"))

    @property
    def previsión_social_funcionarios(self) -> ConfiguraciónPrevisiónSocialDeFuncionarios:
        psf = self.cfg.get("previsión-social-funcionarios")
        if psf is None:
            raise ValueError("No se ha definido la previsión social de funcionarios")
        return ConfiguraciónPrevisiónSocialDeFuncionarios(
            porcentaje_previsión_social=psf.get("porcentaje-previsión-social"),
            base_máxima_de_cotización=psf.get("base-máxima-de-cotización"),
        )

    @property
    def tipos_de_proyecto_convertibles_en_actividad(self) -> ConfiguraciónTiposDeProyectoConvertiblesEnActividad:
        tpc = self.cfg.get("tipos-de-proyecto-convertibles-en-actividad")
        if tpc is None:
            raise ValueError("No se han definido los tipos de proyecto convertibles en actividad")
        return ConfiguraciónTiposDeProyectoConvertiblesEnActividad(
            investigación_regional=tpc.get("investigación-regional").split(),
            investigación_nacional=tpc.get("investigación-nacional").split() ,
            investigación_internacional=tpc.get("investigación-internacional").split(),
            transferencia=tpc.get("transferencia").split(),
            formación_propia=tpc.get("formación-propia").split(),
        )

    def _traza(self) -> None:
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
            añade_directorio(directorio.ruta.parts, como_árbol)
        for fichero in self.ficheros.values():
            añade_fichero(fichero.ruta.parts, como_árbol)

        s = StringIO()
        s.write(f"= Directorios y ficheros con datos (`{self.raíz_datos}/configuración.yaml`)\n")
        s.write(self._tree_to_directory_string(como_árbol))
        s.write("- Directorios\n")
        for clave in sorted(self.directorios):
            s.write(f"  - `{clave}`\n")
            s.write(f"    - path: `{self.directorios[clave].ruta}`\n")
            if self.directorios[clave].desc:
                s.write("    - desc:\n")
                desc = textwrap.dedent(self.directorios[clave].desc)
                for línea in desc.split("\n"):
                    s.write(f"      {línea}\n")
        s.write("- Ficheros\n")
        for clave in sorted(self.ficheros):
            s.write(f"  - `{clave}`\n")
            s.write(f"    - path: `{self.ficheros[clave].ruta}`\n")
            if self.ficheros[clave].cols:
                s.write("    - cols:\n")
                s.write("#align(center, table(columns: 3, align: left, inset: (y: 0.3em), stroke: none,\n")
                s.write("table.header(table.hline(), [*Columna*], [*Columna Excel*], [*Tipo*], table.hline()),\n")
                for columna, (columna_excel, tipo) in self.ficheros[clave].cols.items():
                    s.write(f"[`{columna}`], [`{columna_excel}`], [`{tipo}`],\n")
                s.write("table.hline()\n")
                s.write("    ))\n")

        assert self.traza is not None
        self.traza(s.getvalue())

    def fichero(self, name: str) -> Fichero:
        if name not in self.ficheros:
            raise ValueError(f'El fichero configuración.yaml en {self.raíz_datos} no ha definido "{name}"')
        return self.ficheros[name]

    def directorio(self, name: str) -> Directorio:
        if name not in self.directorios:
            raise ValueError(f'El fichero configuración.yaml en {self.raíz_datos} no ha definido "{name}"')
        return self.directorios[name]

    def _tree_to_directory_string(self, tree):
        iconos = {
            "directory": "folder",
            "file": "folder",
            ".txt": "file-type-txt",
            ".pdf": "file-type-pdf",
            ".doc": "file-word",
            ".docx": "file-word",
            ".xls": "file-type-xls",
            ".xlsx": "file-type-xls",
            ".ppt": "file-type-ppt",
            ".tree": "binary-tree",
            ".typ": "file-typography",
        }

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
                sufijo = Path(name).suffix
                if sufijo != "":
                    icono = f'#tabler-icon("{iconos.get(sufijo, "file")}")'
                else:
                    icono = '#tabler-icon("folder")'
                if current_prefix != "":
                    current_prefix = f"`{current_prefix}`"
                result.append(f"[{current_prefix}{icono}`{name}`]")

                # If it's a directory (not None), recursively add its contents
                if subtree is not None:
                    subtree_str = go(subtree, next_prefix, is_last_item)
                    if subtree_str:
                        result.append(subtree_str)

            # result = [result[0]] + [line[4:] for line in result[1:]]

            return "\n".join(result)

        t = go(tree)
        s = StringIO()
        s.write("#{set text(size: 7pt);show raw: set text(size: 7pt)\n")
        s.write("grid(columns: 1, inset: (y: 0.15em),\n")
        for x in t.split("\n"):
            s.write(f"{x},\n")
        s.write(")}\n")
        return s.getvalue()
