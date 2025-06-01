import calendar
from dataclasses import dataclass, field
from datetime import date

from dateutil.relativedelta import relativedelta

from coana.configuración import Configuración
from coana.misc.euro import E
from coana.misc.utils import num


@dataclass(slots=True)
class PeríodoAmortización:
    cuenta: str = field(init=False)
    nombre_cuenta: str = field(init=False)
    años: float = field(init=False)
    fecha_olvido: date = field(init=False)  # Todo lo que sea anterior a esta fecha se considera amortizado

    def __init__(self, cuenta: str, nombre_cuenta: str, años: float, año_actual: int):
        self.cuenta = cuenta
        self.nombre_cuenta = nombre_cuenta
        self.años = años
        años_enteros = int(años)
        meses_enteros = int((años - años_enteros) * 12)
        días_enteros = int((años - años_enteros - meses_enteros / 12) * 365)
        self.fecha_olvido = date(año_actual, 1, 1) - relativedelta(
            years=años_enteros, months=meses_enteros, days=días_enteros
        )


@dataclass(slots=True)
class PeríodosAmortización:
    períodos: dict[str, PeríodoAmortización] = field()

    def __init__(self, cfg: Configuración):
        año_actual = cfg.año
        df = cfg.fichero("períodos-amortización").carga_dataframe()
        self.períodos = {}
        for row in df.iter_rows(named=True):
            cuenta = row["cuenta"]
            self.períodos[cuenta] = PeríodoAmortización(
                cuenta=cuenta, nombre_cuenta=row["nombre_cuenta"], años=row["años"], año_actual=año_actual
            )

    def __getitem__(self, key: str) -> PeríodoAmortización:
        return self.períodos[key]


@dataclass(slots=True)
class CostePorAmortización:
    cuenta: str = field()
    importe: E = field()
    importe_adquisición: E = field()
    fecha_adquisición: date = field()
    fecha_baja: date | None = field()
    id_inventario: str = field()
    proyectos: list[str] = field()
    subproyectos: list[str] = field()
    ubicación: str | None = field()
    elemento_de_coste: str | None = field(default=None)
    centro_de_coste: str | None = field(default=None)
    subproyecto: str | None = field(default=None)


@dataclass
class CostesPorAmortizaciones:
    amortizaciones: dict[str, list[CostePorAmortización]]

    def __init__(self, cfg: Configuración) -> None:
        año_actual = cfg.año
        días_del_año_actual = 366 if calendar.isleap(año_actual) else 365
        inicio_del_año_actual = date(año_actual, 1, 1)
        fin_del_año_actual = date(año_actual, 12, 31)

        períodos = PeríodosAmortización(cfg)

        inventario = cfg.fichero("inventario").carga_dataframe()
        self.amortizaciones = {}
        c_dados_de_baja = 0
        c_dados_de_alta_después = 0
        c_totalmente_amortizados = 0
        c_con_importe_cero = 0
        for row in inventario.iter_rows(named=True):
            cuenta = row["cuenta"]
            fecha_alta: date = row["fecha_alta"]
            fecha_baja: date | None = row["fecha_baja"]

            período = períodos[cuenta]

            if fecha_baja is not None and fecha_baja < inicio_del_año_actual:
                c_dados_de_baja += 1
                continue
            if fecha_alta > fin_del_año_actual:
                c_dados_de_alta_después += 1
                continue
            if fecha_alta < período.fecha_olvido:
                c_totalmente_amortizados += 1
                continue

            importe_adquisición = E(row["importe_adquisición"])
            importe_por_año = importe_adquisición / período.años
            fracción = fracción_del_año_actual_con_elemento_activo(
                fecha_alta,
                fecha_baja,
                período.fecha_olvido,
                inicio_del_año_actual,
                fin_del_año_actual,
                días_del_año_actual,
            )
            importe = importe_por_año * fracción
            id = row["id"]
            proyectos = row["proyectos"].split(";") if row["proyectos"] else []
            subproyectos = row["subproyectos"].split(";") if row["subproyectos"] else []
            ubicación = row["ubicación"] if row["ubicación"] else None

            if importe > 0:
                self.amortizaciones.setdefault(cuenta, []).append(
                    CostePorAmortización(
                        cuenta=cuenta,
                        importe=E(importe),
                        importe_adquisición=importe_adquisición,
                        fecha_adquisición=fecha_alta,
                        fecha_baja=fecha_baja,
                        id_inventario=id,
                        proyectos=proyectos,
                        subproyectos=subproyectos,
                        ubicación=ubicación,
                    )
                )
            else:
                c_con_importe_cero += 1

        traza = cfg.traza
        traza("= Costes por amortizaciones")
        traza(f"- Líneas de inventario: {num(inventario.shape[0])}")
        traza(f"  - Elementos ya dados de baja antes de {inicio_del_año_actual}: {num(c_dados_de_baja)}")
        traza(f"  - Elementos totalmente amortizados antes de {inicio_del_año_actual}: {num(c_totalmente_amortizados)}")
        traza(f"  - Elementos dados de alta después de {fin_del_año_actual}: {num(c_dados_de_alta_después)}")
        traza(f"  - Con importe cero: {num(c_con_importe_cero)}")
        traza("""
            #align(
              center,
              table(
                columns: 3,
                align: (left, right, right),
                stroke: none,
                table.header(
                  table.hline(),
                  [*Cuenta*], [*Líneas*], [*Importe*],
                  table.hline()
                ),
              """)
        for cuenta, amortizaciones in sorted(self.amortizaciones.items()):
            traza(f"  [{cuenta}], [{num(len(amortizaciones))}], [{sum(x.importe for x in amortizaciones)}],")
        traza("table.hline(),")
        traza(
            "[*Total*],"
            + f" [*{num(sum(len(x) for x in self.amortizaciones.values()))}*],"
            + f" [*{sum(sum(x.importe for x in x) for x in self.amortizaciones.values())}*],"
        )
        traza("table.hline(),")
        traza("""
                )
            )
            """)


def fracción_del_año_actual_con_elemento_activo(
    fecha_alta: date,
    fecha_baja: date | None,
    fecha_olvido: date,
    inicio_del_año_actual: date,
    fin_del_año_actual: date,
    días_del_año_actual: int,
) -> float:
    if fecha_alta < fecha_olvido:  # El ítem ya está amortizado
        return 0.0

    fecha_alta = max(fecha_alta, inicio_del_año_actual)
    fecha_baja = min(fin_del_año_actual if fecha_baja is None else fecha_baja, fin_del_año_actual)
    días = (fecha_baja - fecha_alta).days
    return días / días_del_año_actual
