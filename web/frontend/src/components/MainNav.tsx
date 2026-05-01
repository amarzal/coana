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
        items: [{ label: "Unidades de coste", to: "/presupuesto/uc" }],
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
