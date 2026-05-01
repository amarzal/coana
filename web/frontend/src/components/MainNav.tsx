import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { NavLink, useLocation } from "react-router";
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
            { label: "Resumen", to: "/regla23/resumen" },
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
            { label: "Resumen", to: "/cargos/resumen" },
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

function _grupoActivo(grupo: Group, ruta: string): boolean {
    return grupo.items.some((it) => it.to === ruta);
}

// ----------------------------------------------------------------------
// Menú dinámico de Entradas
// ----------------------------------------------------------------------

type Fichero = {
    nombre: string;
    stem: string;
    extension: string;
    ruta_relativa: string;
    tamaño_bytes: number;
};
type SubGrupo = { subdirectorio: string; ficheros: Fichero[] };
type Catalogo = { grupos: SubGrupo[] };

function CabeceraColapsable({
    label, abierto, onToggle, level = 0,
}: {
    label: string;
    abierto: boolean;
    onToggle: () => void;
    level?: number;
}) {
    return (
        <button
            type="button"
            onClick={onToggle}
            aria-expanded={abierto}
            className={cn(
                "flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-slate-500 hover:bg-slate-100",
                level === 0
                    ? "text-xs font-semibold uppercase tracking-wide"
                    : "text-xs",
            )}
        >
            <span className="w-3 text-slate-400">{abierto ? "▾" : "▸"}</span>
            <span>{label}</span>
        </button>
    );
}

function EntradasMenu() {
    const { pathname } = useLocation();
    // Subdirectorio activo: el primer segmento tras /entradas/.
    const subdirActivo = pathname.startsWith("/entradas/")
        ? pathname.slice("/entradas/".length).split("/")[0]
        : null;

    const [abiertoRaiz, setAbiertoRaiz] = useState(
        () => pathname.startsWith("/entradas"),
    );
    const [subAbiertos, setSubAbiertos] = useState<Set<string>>(
        () => (subdirActivo ? new Set([subdirActivo]) : new Set()),
    );

    const { data, isLoading, isError } = useQuery({
        queryKey: ["entradas:catalogo"],
        queryFn: async (): Promise<Catalogo> => {
            const r = await fetch("/api/entradas/");
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return r.json() as Promise<Catalogo>;
        },
    });

    const toggleSub = (label: string) => {
        setSubAbiertos((prev) => {
            const next = new Set(prev);
            if (next.has(label)) next.delete(label);
            else next.add(label);
            return next;
        });
    };

    return (
        <div>
            <CabeceraColapsable
                label="Entradas"
                abierto={abiertoRaiz}
                onToggle={() => setAbiertoRaiz(!abiertoRaiz)}
            />
            {abiertoRaiz && (
                <div className="ml-4 flex flex-col border-l border-slate-200">
                    {isLoading && (
                        <div className="px-2 py-1 text-xs text-slate-500">Cargando…</div>
                    )}
                    {isError && (
                        <div className="px-2 py-1 text-xs text-red-700">Error</div>
                    )}
                    {data?.grupos.map((sg) => {
                        const sgOpen = subAbiertos.has(sg.subdirectorio);
                        return (
                            <div key={sg.subdirectorio}>
                                <CabeceraColapsable
                                    label={sg.subdirectorio}
                                    abierto={sgOpen}
                                    onToggle={() => toggleSub(sg.subdirectorio)}
                                    level={1}
                                />
                                {sgOpen && (
                                    <ul className="ml-4 flex flex-col border-l border-slate-200">
                                        {sg.ficheros.map((f) => (
                                            <li key={f.ruta_relativa}>
                                                <NavLink
                                                    to={`/entradas/${f.ruta_relativa}`}
                                                    className={({ isActive }) =>
                                                        cn(
                                                            "flex items-baseline gap-2 rounded-md px-2 py-1 text-sm text-slate-700 hover:bg-slate-100",
                                                            isActive && "bg-slate-200 font-medium",
                                                        )
                                                    }
                                                    title={f.ruta_relativa}
                                                >
                                                    <span className="text-xs text-slate-400">
                                                        {f.extension === ".tree" ? "▤" : "▦"}
                                                    </span>
                                                    <span className="truncate">{f.stem}</span>
                                                </NavLink>
                                            </li>
                                        ))}
                                    </ul>
                                )}
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
}

export function MainNav() {
    const { pathname } = useLocation();
    // Estado inicial: solo se despliega el grupo que contiene la ruta actual.
    const [abiertos, setAbiertos] = useState<Set<string>>(
        () => new Set(GROUPS.filter((g) => _grupoActivo(g, pathname)).map((g) => g.label)),
    );

    const toggle = (label: string) => {
        setAbiertos((prev) => {
            const next = new Set(prev);
            if (next.has(label)) next.delete(label);
            else next.add(label);
            return next;
        });
    };

    const renderGrupo = (g: Group) => {
        const open = abiertos.has(g.label);
        return (
            <div key={g.label}>
                <button
                    type="button"
                    onClick={() => toggle(g.label)}
                    aria-expanded={open}
                    className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-xs font-semibold uppercase tracking-wide text-slate-500 hover:bg-slate-100"
                >
                    <span className="w-3 text-slate-400">{open ? "▾" : "▸"}</span>
                    <span>{g.label}</span>
                </button>
                {open && (
                    <ul className="ml-4 flex flex-col border-l border-slate-200">
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
                )}
            </div>
        );
    };

    return (
        <nav className="flex flex-col gap-1 text-sm">
            {/* «Inicio» primero */}
            {GROUPS.filter((g) => g.label === "Inicio").map(renderGrupo)}
            {/* «Entradas» como menú dinámico (3 niveles: Entradas → subdir → fichero) */}
            <EntradasMenu />
            {/* El resto de bloques estáticos */}
            {GROUPS.filter((g) => g.label !== "Inicio").map(renderGrupo)}
        </nav>
    );
}
