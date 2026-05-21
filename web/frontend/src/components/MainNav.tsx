import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { NavLink, useLocation } from "react-router";
import {
    Wallet,
    Boxes,
    Users,
    Users2,
    Scale,
    GraduationCap,
    Building2,
    Sigma,
    FileText,
    ClipboardList,
    Filter,
    ListChecks,
    Inbox,
    Folder,
    Sheet,
    ListTree,
    Microscope,
    ChevronDown,
    ChevronRight,
    type LucideIcon,
} from "lucide-react";
import { cn } from "@/lib/cn";
import { GenerarInformes } from "@/components/GenerarInformes";

type IconCmp = LucideIcon;

type Leaf = { label: string; to: string; icon?: IconCmp };
type SubGrupo = { label: string; items: Leaf[]; icon?: IconCmp };
type Item = Leaf | SubGrupo;
type Group = { label: string; items: Item[]; icon: IconCmp };

function esSubgrupo(it: Item): it is SubGrupo {
    return (it as SubGrupo).items !== undefined;
}

// Va creciendo a medida que se construyen los bloques.
const GROUPS: Group[] = [
    {
        label: "Presupuesto",
        icon: Wallet,
        items: [
            { label: "Resumen", to: "/presupuesto/resumen" },
            { label: "Unidades de coste", to: "/presupuesto/uc" },
            { label: "Sin clasificar", to: "/presupuesto/sin-clasificar" },
            { label: "Apuntes filtrados", to: "/presupuesto/filtrados" },
            { label: "Suministros", to: "/presupuesto/suministros" },
            { label: "Distribución mantenimientos OTOP", to: "/presupuesto/otop" },
            {
                label: "Reglas",
                icon: ListChecks,
                items: [
                    { label: "Actividad", to: "/presupuesto/reglas/actividad" },
                    { label: "Centros de coste", to: "/presupuesto/reglas/cc" },
                    { label: "Elementos de coste", to: "/presupuesto/reglas/ec" },
                ],
            },
            {
                label: "Estructuras",
                icon: ListTree,
                items: [
                    { label: "Actividades", to: "/presupuesto/arbol/actividades" },
                    { label: "Centros de coste", to: "/presupuesto/arbol/cc" },
                    { label: "Elementos de coste", to: "/presupuesto/arbol/ec" },
                ],
            },
        ],
    },
    {
        label: "Amortizaciones",
        icon: Boxes,
        items: [
            { label: "Resumen", to: "/amortizaciones/resumen" },
            { label: "Inventario con amortización", to: "/amortizaciones/enriquecido" },
            {
                label: "Descartados",
                icon: Filter,
                items: [
                    { label: "Filtrados por estado", to: "/amortizaciones/filtrados-estado" },
                    { label: "Filtrados por cuenta", to: "/amortizaciones/filtrados-cuenta" },
                    { label: "Filtrados por fecha", to: "/amortizaciones/filtrados-fecha" },
                    { label: "Sin cuenta", to: "/amortizaciones/sin-cuenta" },
                    { label: "Sin fecha de alta", to: "/amortizaciones/sin-fecha-alta" },
                ],
            },
            { label: "UC generadas", to: "/amortizaciones/uc" },
            { label: "Sin centro", to: "/amortizaciones/sin-centro" },
        ],
    },
    {
        label: "Personal",
        icon: Users,
        items: [
            { label: "Resumen", to: "/personal/resumen" },
            {
                label: "Colectivos",
                icon: Users2,
                items: [
                    { label: "PDI", to: "/personal/pdi" },
                    { label: "PVI", to: "/personal/pvi" },
                    { label: "PTGAS", to: "/personal/ptgas" },
                    { label: "Otros", to: "/personal/otros" },
                ],
            },
            { label: "Multiexpediente", to: "/personal/multiexpediente" },
            { label: "Costes sociales calculados", to: "/personal/costes-sociales-calculados" },
            { label: "Atrasos a no vinculados", to: "/personal/atrasos-no-vinculados" },
            { label: "Despidos", to: "/personal/despidos" },
            { label: "Indemnizaciones asistencias", to: "/personal/indemnizaciones" },
            { label: "Anomalías PDI", to: "/personal/anomalias" },
            {
                label: "Cargos académicos",
                icon: GraduationCap,
                items: [
                    { label: "Resumen", to: "/cargos/resumen" },
                    { label: "Por persona", to: "/cargos/personas-remuneradas" },
                    { label: "Personas cargos", to: "/cargos/personas-cargos" },
                    { label: "Catálogo de cargos", to: "/cargos/cargos" },
                ],
            },
            {
                label: "Regla 23",
                icon: Scale,
                items: [
                    { label: "Resumen", to: "/regla23/resumen" },
                    { label: "Dedicación docente", to: "/regla23/dedicacion" },
                    { label: "Docencia no oficial", to: "/regla23/no-oficial" },
                    { label: "Estructura estudios", to: "/regla23/estructura" },
                    { label: "Cargos", to: "/regla23/cargos" },
                    { label: "Asignaturas sin titulación", to: "/regla23/sin-titulacion" },
                    { label: "Anomalías", to: "/regla23/anomalias" },
                ],
            },
        ],
    },
    {
        label: "Investigación",
        icon: Microscope,
        items: [
            { label: "Grupos", to: "/investigacion/grupos" },
        ],
    },
    {
        label: "Superficies",
        icon: Building2,
        items: [
            { label: "Resumen", to: "/superficies/resumen" },
            { label: "Totales", to: "/superficies/totales" },
            { label: "Presencia centros", to: "/superficies/presencia" },
        ],
    },
    {
        label: "Resultados Fase 1",
        icon: Sigma,
        items: [
            { label: "Resumen", to: "/resultados/resumen" },
            { label: "Todas las UC", to: "/resultados/uc" },
            { label: "Actividades", to: "/resultados/actividades" },
            { label: "Centros de coste", to: "/resultados/centros-de-coste" },
            { label: "Elementos de coste", to: "/resultados/elementos-de-coste" },
            { label: "Anomalías UC", to: "/resultados/anomalias" },
        ],
    },
    {
        label: "Informes",
        icon: FileText,
        items: [
            {
                label: "Normalizados",
                icon: ClipboardList,
                items: [
                    { label: "10.1 — Elementos de coste",            to: "/informes/cuadro_10_1" },
                    { label: "10.3 — Ingresos por actividades",      to: "/informes/cuadro_10_3" },
                    { label: "10.4 — Centros por finalidad",         to: "/informes/cuadro_10_4" },
                    { label: "10.5 — Costes primarios por centro",   to: "/informes/cuadro_10_5" },
                    { label: "10.7 — Coste de actividades finalistas", to: "/informes/cuadro_10_7" },
                ],
            },
            { label: "A la carta", to: "/informes-carta" },
        ],
    },
];

function _itemActivo(it: Item, ruta: string): boolean {
    if (esSubgrupo(it)) return it.items.some((leaf) => leaf.to === ruta);
    return it.to === ruta;
}

function _grupoActivo(grupo: Group, ruta: string): boolean {
    return grupo.items.some((it) => _itemActivo(it, ruta));
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
type SubgrupoEntradas = { subdirectorio: string; ficheros: Fichero[] };
type Catalogo = { grupos: SubgrupoEntradas[] };

function CabeceraColapsable({
    label, abierto, onToggle, level = 0, Icon,
}: {
    label: string;
    abierto: boolean;
    onToggle: () => void;
    level?: number;
    Icon?: IconCmp;
}) {
    const Chevron = abierto ? ChevronDown : ChevronRight;
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
            <Chevron size={12} className="shrink-0 text-slate-400" />
            {Icon && <Icon size={14} className="shrink-0" />}
            <span>{label}</span>
        </button>
    );
}

export function EntradasMenu() {
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
                Icon={Inbox}
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
                                    Icon={Folder}
                                />
                                {sgOpen && (
                                    <ul className="ml-4 flex flex-col border-l border-slate-200">
                                        {sg.ficheros.map((f) => {
                                            const FileIcon =
                                                f.extension === ".tree" ? ListTree : Sheet;
                                            return (
                                                <li key={f.ruta_relativa}>
                                                    <NavLink
                                                        to={`/entradas/${f.ruta_relativa}`}
                                                        className={({ isActive }) =>
                                                            cn(
                                                                "flex items-center gap-2 rounded-md px-2 py-1 text-sm text-slate-700 hover:bg-slate-100",
                                                                isActive && "bg-slate-200 font-medium",
                                                            )
                                                        }
                                                        title={f.ruta_relativa}
                                                    >
                                                        <FileIcon
                                                            size={14}
                                                            className="shrink-0 text-slate-400"
                                                        />
                                                        <span className="truncate">{f.stem}</span>
                                                    </NavLink>
                                                </li>
                                            );
                                        })}
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
    // Estado inicial: solo se despliega el grupo que contiene la ruta
    // actual, y dentro de él los sub-grupos cuyo contenido coincida.
    const [abiertos, setAbiertos] = useState<Set<string>>(() => {
        const open = new Set<string>();
        GROUPS.forEach((g) => {
            if (_grupoActivo(g, pathname)) {
                open.add(g.label);
                g.items.forEach((it) => {
                    if (esSubgrupo(it) && _itemActivo(it, pathname)) {
                        open.add(`${g.label}/${it.label}`);
                    }
                });
            }
        });
        return open;
    });

    const toggle = (label: string) => {
        setAbiertos((prev) => {
            const next = new Set(prev);
            if (next.has(label)) next.delete(label);
            else next.add(label);
            return next;
        });
    };

    const navClasses = ({ isActive }: { isActive: boolean }) =>
        cn(
            "block rounded-md px-2 py-1.5 text-slate-700 hover:bg-slate-100",
            isActive && "bg-slate-200 font-medium",
        );

    const renderItem = (grupoLabel: string, it: Item) => {
        if (!esSubgrupo(it)) {
            return (
                <li key={it.to}>
                    <NavLink to={it.to} end className={navClasses}>
                        {it.label}
                    </NavLink>
                </li>
            );
        }
        const key = `${grupoLabel}/${it.label}`;
        const open = abiertos.has(key);
        const Chevron = open ? ChevronDown : ChevronRight;
        const SubIcon = it.icon;
        return (
            <li key={key}>
                <button
                    type="button"
                    onClick={() => toggle(key)}
                    aria-expanded={open}
                    className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-slate-700 hover:bg-slate-100"
                >
                    <Chevron size={12} className="shrink-0 text-slate-400" />
                    {SubIcon && <SubIcon size={14} className="shrink-0 text-slate-500" />}
                    <span>{it.label}</span>
                </button>
                {open && (
                    <ul className="ml-4 flex flex-col border-l border-slate-200">
                        {it.items.map((leaf) => (
                            <li key={leaf.to}>
                                <NavLink to={leaf.to} end className={navClasses}>
                                    {leaf.label}
                                </NavLink>
                            </li>
                        ))}
                    </ul>
                )}
            </li>
        );
    };

    const renderGrupo = (g: Group) => {
        const open = abiertos.has(g.label);
        const Chevron = open ? ChevronDown : ChevronRight;
        const Icon = g.icon;
        return (
            <div key={g.label}>
                <button
                    type="button"
                    onClick={() => toggle(g.label)}
                    aria-expanded={open}
                    className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-xs font-semibold uppercase tracking-wide text-slate-500 hover:bg-slate-100"
                >
                    <Chevron size={12} className="shrink-0 text-slate-400" />
                    <Icon size={14} className="shrink-0" />
                    <span>{g.label}</span>
                </button>
                {open && (
                    <ul className="ml-4 flex flex-col border-l border-slate-200">
                        {g.items.map((it) => renderItem(g.label, it))}
                    </ul>
                )}
            </div>
        );
    };

    return (
        <nav className="flex flex-col gap-1 text-sm">
            {GROUPS.map((g) => (
                <div key={g.label}>
                    {g.label === "Informes" && (
                        <div className="my-2">
                            <GenerarInformes />
                        </div>
                    )}
                    {renderGrupo(g)}
                </div>
            ))}
        </nav>
    );
}
