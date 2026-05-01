import { NavLink } from "react-router";
import { cn } from "@/lib/cn";

type Item = { label: string; to: string };
type Group = { label: string; items: Item[] };

// Estructura mínima de la fase 1 — irá creciendo en fases sucesivas.
const GROUPS: Group[] = [
    {
        label: "Inicio",
        items: [{ label: "Estado del backend", to: "/" }],
    },
    {
        label: "Presupuesto",
        items: [{ label: "Unidades de coste", to: "/presupuesto/uc" }],
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
