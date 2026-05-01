import { NavLink } from "react-router";
import { cn } from "@/lib/cn";

type Item = { label: string; to: string };
type Group = { label: string; items: Item[] };

// Va creciendo a medida que se construyen los bloques.
const GROUPS: Group[] = [
    {
        label: "Inicio",
        items: [{ label: "Estado del backend", to: "/" }],
    },
    {
        label: "Entradas",
        items: [{ label: "Catálogo", to: "/entradas" }],
    },
    {
        label: "Presupuesto",
        items: [
            { label: "Resumen", to: "/presupuesto/resumen" },
            { label: "Unidades de coste", to: "/presupuesto/uc" },
            { label: "Sin clasificar", to: "/presupuesto/sin-clasificar" },
            { label: "Apuntes filtrados", to: "/presupuesto/filtrados" },
            { label: "Suministros", to: "/presupuesto/suministros" },
            { label: "Distribución mantenimientos OTOP", to: "/presupuesto/otop" },
            { label: "Reglas de actividad", to: "/presupuesto/reglas/actividad" },
            { label: "Reglas de centro de coste", to: "/presupuesto/reglas/cc" },
            { label: "Reglas de elemento de coste", to: "/presupuesto/reglas/ec" },
            { label: "Árbol: actividades", to: "/presupuesto/arbol/actividades" },
            { label: "Árbol: centros de coste", to: "/presupuesto/arbol/cc" },
            { label: "Árbol: elementos de coste", to: "/presupuesto/arbol/ec" },
        ],
    },
    {
        label: "Amortizaciones",
        items: [
            { label: "Resumen", to: "/amortizaciones/resumen" },
            { label: "Inventario con amortización", to: "/amortizaciones/enriquecido" },
            { label: "Filtrados por estado", to: "/amortizaciones/filtrados-estado" },
            { label: "Filtrados por cuenta", to: "/amortizaciones/filtrados-cuenta" },
            { label: "Filtrados por fecha", to: "/amortizaciones/filtrados-fecha" },
            { label: "Sin cuenta", to: "/amortizaciones/sin-cuenta" },
            { label: "Sin fecha de alta", to: "/amortizaciones/sin-fecha-alta" },
            { label: "UC generadas", to: "/amortizaciones/uc" },
            { label: "Sin centro", to: "/amortizaciones/sin-centro" },
        ],
    },
    {
        label: "Personal",
        items: [
            { label: "Resumen", to: "/personal/resumen" },
            { label: "Expedientes PDI", to: "/personal/pdi" },
            { label: "Expedientes PTGAS", to: "/personal/ptgas" },
            { label: "Expedientes PVI", to: "/personal/pvi" },
            { label: "Expedientes otros", to: "/personal/otros" },
            { label: "Multiexpediente", to: "/personal/multiexpediente" },
            { label: "Persona", to: "/personal/persona" },
            { label: "Anomalías PDI", to: "/personal/anomalias" },
        ],
    },
    {
        label: "Regla 23",
        items: [
            { label: "Dedicación docente", to: "/regla23/dedicacion" },
            { label: "Docencia no oficial", to: "/regla23/no-oficial" },
            { label: "Estructura estudios", to: "/regla23/estructura" },
            { label: "Bolsa de atrasos", to: "/regla23/atrasos" },
            { label: "Despidos", to: "/regla23/despidos" },
            { label: "Indemnizaciones asistencias", to: "/regla23/indemnizaciones" },
            { label: "Cargos", to: "/regla23/cargos" },
            { label: "Expedientes apartados", to: "/regla23/apartados" },
            { label: "Asignaturas sin titulación", to: "/regla23/sin-titulacion" },
            { label: "Anomalías", to: "/regla23/anomalias" },
        ],
    },
    {
        label: "Cargos académicos",
        items: [
            { label: "Categoría PDI/PVI", to: "/cargos/categoria" },
            { label: "Departamentos", to: "/cargos/departamentos" },
        ],
    },
    {
        label: "Superficies",
        items: [
            { label: "Resumen", to: "/superficies/resumen" },
            { label: "Totales", to: "/superficies/totales" },
            { label: "Presencia centros", to: "/superficies/presencia" },
        ],
    },
    {
        label: "Resultados Fase 1",
        items: [
            { label: "Resumen", to: "/resultados/resumen" },
            { label: "Todas las UC", to: "/resultados/uc" },
            { label: "Actividades", to: "/resultados/actividades" },
            { label: "Centros de coste", to: "/resultados/centros" },
            { label: "Elementos de coste", to: "/resultados/elementos" },
            { label: "Anomalías UC", to: "/resultados/anomalias" },
        ],
    },
];

export function MainNav() {
    return (
        <nav className="flex flex-col gap-4 text-sm">
            {GROUPS.map((g) => (
                <div key={g.label}>
                    <div className="mb-1 px-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                        {g.label}
                    </div>
                    <ul className="flex flex-col">
                        {g.items.map((it) => (
                            <li key={it.to}>
                                <NavLink
                                    to={it.to}
                                    end
                                    className={({ isActive }) =>
                                        cn(
                                            "block rounded-md px-2 py-1.5 text-slate-700 hover:bg-slate-100",
                                            isActive && "bg-slate-200 font-medium",
                                        )
                                    }
                                >
                                    {it.label}
                                </NavLink>
                            </li>
                        ))}
                    </ul>
                </div>
            ))}
        </nav>
    );
}
