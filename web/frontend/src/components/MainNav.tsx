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
