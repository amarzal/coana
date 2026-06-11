import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { NavLink } from "react-router";
import { DataTable } from "@/components/DataTable";
import { RecordCard } from "@/components/RecordCard";
import { formatEuro, formatInt } from "@/lib/format";
import { cn } from "@/lib/cn";

const KPI = "/api/resultados/_resumen";
const QK_RESUMEN = "resultados:resumen";

function Cabecera({ title, subtitle }: { title: string; subtitle?: string }) {
    return (
        <div>
            <h1 className="text-2xl font-semibold">{title}</h1>
            {subtitle && <p className="text-sm text-slate-500">{subtitle}</p>}
        </div>
    );
}

/** Modal con la ficha de una UC: campos completos + sección con el
 * registro de origen (apunte presupuestario, bien inventariable…). */
function UcDetalleModal({
    origen, ucId, onClose, anomalia,
}: {
    origen: string; ucId: string; onClose: () => void;
    anomalia?: { campo?: string; valor?: string };
}) {
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === "Escape") onClose();
        };
        window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [onClose]);

    return (
        <div
            className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-6"
            onClick={onClose}
            role="dialog"
            aria-modal="true"
        >
            <div
                className="flex max-h-[90vh] w-full max-w-3xl flex-col overflow-hidden rounded-lg border border-slate-200 bg-white shadow-xl"
                onClick={(e) => e.stopPropagation()}
            >
                <div className="flex items-center justify-between border-b border-slate-200 px-4 py-2">
                    <h2 className="text-sm font-medium uppercase tracking-wide text-slate-500">
                        UC {ucId} · {origen}
                    </h2>
                    <button
                        type="button"
                        onClick={onClose}
                        className="rounded px-2 py-1 text-sm text-slate-500 hover:bg-slate-100 hover:text-slate-800"
                        aria-label="Cerrar"
                    >
                        ✕ Cerrar
                    </button>
                </div>
                <div className="overflow-auto p-4 flex flex-col gap-3">
                    {anomalia?.campo && (
                        <aside className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">
                            <span className="font-medium">Anomalía de integridad referencial.</span>{" "}
                            {anomalia.valor
                                ? <>El campo <span className="font-mono">{anomalia.campo}</span> referencia
                                    el identificador <span className="font-mono">«{anomalia.valor}»</span>,
                                    que no existe en el árbol final.</>
                                : <>El campo <span className="font-mono">{anomalia.campo}</span> está vacío:
                                    la UC no está clasificada en ese eje.</>}
                            {" "}Más abajo, en <em>Información relacionada</em>, tienes el detalle (si el
                            identificador existía en el árbol original y posibles identificadores correctos).
                        </aside>
                    )}
                    <RecordCard
                        endpoint={`/api/resultados/uc/${encodeURIComponent(origen)}/{id}`}
                        id={ucId}
                        queryKey={`resultados:uc:${origen}:${ucId}`}
                    />
                </div>
            </div>
        </div>
    );
}

function Lista({
    title, subtitle, endpoint, queryKey, rowKey = "id",
    reorderImportes,
}: {
    title: string; subtitle?: string; endpoint: string; queryKey: string;
    rowKey?: string;
    reorderImportes?: boolean;
}) {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera title={title} subtitle={subtitle} />
            <DataTable
                endpoint={endpoint}
                queryKey={queryKey}
                rowKey={rowKey}
                reorderImportes={reorderImportes}
            />
        </div>
    );
}

type Kpi = {
    label: string;
    value: number | string | null;
    format: string;
    hint?: string | null;
};
type KpiPanelData = { kpis: Kpi[] };

// Agrupación temática de las fuentes que produce el resumen. Cada
// grupo lleva un color para que la composición sea visualmente legible.
const CATEGORIAS: Array<{
    título: string;
    color: string;
    fuentes: string[];
}> = [
    { título: "Presupuesto", color: "bg-blue-500",  fuentes: ["presupuesto"] },
    { título: "Inventario",  color: "bg-amber-500", fuentes: ["inventario"] },
    { título: "Suministros", color: "bg-teal-500",  fuentes: ["energía", "agua", "gas"] },
    { título: "Nóminas",     color: "bg-emerald-600", fuentes: ["nómina"] },
    { título: "Regla 23",    color: "bg-purple-500", fuentes: ["regla_23"] },
];

function categoríaDe(fuente: string): { título: string; color: string } | null {
    for (const c of CATEGORIAS) {
        if (c.fuentes.includes(fuente)) return { título: c.título, color: c.color };
    }
    return null;
}

function CajaTotal({
    etiqueta, valor, sub,
}: { etiqueta: string; valor: string; sub?: string }) {
    return (
        <div className="rounded-lg border border-slate-200 bg-white px-5 py-4 shadow-sm">
            <div className="text-xs uppercase tracking-wide text-slate-500">{etiqueta}</div>
            <div className="mt-1 text-3xl font-semibold tabular-nums text-slate-900">{valor}</div>
            {sub && <div className="mt-1 text-xs text-slate-500">{sub}</div>}
        </div>
    );
}

function FilaFuente({
    fuente, importe, nUcs, total, color,
}: {
    fuente: string;
    importe: number;
    nUcs: number | null;
    total: number;
    color: string;
}) {
    const pct = total > 0 ? (100 * importe) / total : 0;
    return (
        <div className="flex items-center gap-3 py-1.5">
            <div className="w-32 text-sm font-medium text-slate-700">{fuente}</div>
            <div className="flex-1">
                <div className="relative h-2 overflow-hidden rounded-full bg-slate-100">
                    <div
                        className={cn("absolute inset-y-0 left-0", color)}
                        style={{ width: `${pct.toFixed(2)}%` }}
                    />
                </div>
            </div>
            <div className="w-28 text-right text-sm font-semibold tabular-nums text-slate-900">
                {formatEuro(importe)}
            </div>
            <div className="w-16 text-right text-xs tabular-nums text-slate-500">
                {pct.toFixed(1)} %
            </div>
            <div className="w-20 text-right text-xs tabular-nums text-slate-500">
                {nUcs !== null ? `${formatInt(nUcs)} UC` : ""}
            </div>
        </div>
    );
}

function Atajo({ to, título, descripción }: { to: string; título: string; descripción: string }) {
    return (
        <NavLink
            to={to}
            className="block rounded-md border border-slate-200 bg-white px-3 py-2 hover:border-slate-400 hover:bg-slate-50"
        >
            <div className="text-sm font-medium text-slate-800">{título}</div>
            <div className="text-xs text-slate-500">{descripción}</div>
        </NavLink>
    );
}

function parseHintN(hint: string | null | undefined): number | null {
    // Hints tipo "1.234 UC · 12,3 % del total" → 1234
    if (!hint) return null;
    const m = hint.match(/([\d.,]+)\s+UC/);
    if (!m) return null;
    const limpio = m[1].replace(/\./g, "").replace(",", ".");
    const n = Number(limpio);
    return Number.isFinite(n) ? Math.round(n) : null;
}

export function ResultadosResumen() {
    const { data, isLoading, isError, error } = useQuery({
        queryKey: [QK_RESUMEN],
        queryFn: async (): Promise<KpiPanelData> => {
            const r = await fetch(KPI);
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return (await r.json()) as KpiPanelData;
        },
        staleTime: 30_000,
    });

    if (isError) {
        return (
            <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">
                {error instanceof Error ? error.message : String(error)}
            </div>
        );
    }
    if (isLoading || !data) {
        return <div className="text-sm text-slate-500">Cargando…</div>;
    }

    // Extraer KPIs principales del listado plano.
    const kpiUcTotales = data.kpis.find((k) => k.label === "UC totales");
    const kpiImporteTotal = data.kpis.find((k) => k.label === "Importe total");
    const fuentes = data.kpis.filter(
        (k) => k.label !== "UC totales" && k.label !== "Importe total",
    );
    const totalImp = typeof kpiImporteTotal?.value === "number" ? kpiImporteTotal.value : 0;
    const totalN = typeof kpiUcTotales?.value === "number" ? kpiUcTotales.value : 0;

    // Agrupar fuentes por categoría temática.
    const fuentesPorCat = new Map<string, { color: string; items: Kpi[] }>();
    const sinCat: Kpi[] = [];
    for (const k of fuentes) {
        const cat = categoríaDe(k.label);
        if (cat) {
            const prev = fuentesPorCat.get(cat.título) ?? { color: cat.color, items: [] };
            prev.items.push(k);
            fuentesPorCat.set(cat.título, prev);
        } else {
            sinCat.push(k);
        }
    }

    return (
        <div className="flex flex-col gap-8">
            <Cabecera
                title="Resultados Fase 1 · Resumen"
                subtitle="Visión consolidada de las UC generadas, con desglose por origen."
            />

            {/* Sección 1: totales destacados */}
            <section>
                <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">
                    Totales del ejercicio
                </h2>
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                    <CajaTotal
                        etiqueta="Unidades de coste"
                        valor={formatInt(totalN)}
                        sub={`Importe total ${formatEuro(totalImp)}`}
                    />
                    <CajaTotal
                        etiqueta="Coste total imputado"
                        valor={formatEuro(totalImp)}
                        sub={`${formatInt(totalN)} unidades de coste`}
                    />
                </div>
            </section>

            {/* Sección 2: composición por fuente */}
            <section>
                <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">
                    Composición por fuente
                </h2>
                <div className="space-y-5 rounded-lg border border-slate-200 bg-white p-4">
                    {CATEGORIAS.map((cat) => {
                        const grupo = fuentesPorCat.get(cat.título);
                        if (!grupo || grupo.items.length === 0) return null;
                        const subtotalImp = grupo.items.reduce(
                            (acc, k) => acc + (typeof k.value === "number" ? k.value : 0),
                            0,
                        );
                        return (
                            <div key={cat.título}>
                                <div className="mb-1.5 flex items-baseline justify-between">
                                    <div className="flex items-center gap-2">
                                        <span className={cn("inline-block h-3 w-3 rounded-sm", cat.color)} />
                                        <span className="text-sm font-semibold text-slate-700">{cat.título}</span>
                                    </div>
                                    <div className="text-xs text-slate-500 tabular-nums">
                                        {formatEuro(subtotalImp)} · {((subtotalImp / totalImp) * 100).toFixed(1)} %
                                    </div>
                                </div>
                                {grupo.items.map((k) => (
                                    <FilaFuente
                                        key={k.label}
                                        fuente={k.label}
                                        importe={typeof k.value === "number" ? k.value : 0}
                                        nUcs={parseHintN(k.hint)}
                                        total={totalImp}
                                        color={cat.color}
                                    />
                                ))}
                            </div>
                        );
                    })}
                    {sinCat.length > 0 && (
                        <div>
                            <div className="mb-1.5 flex items-center gap-2">
                                <span className="inline-block h-3 w-3 rounded-sm bg-slate-400" />
                                <span className="text-sm font-semibold text-slate-700">Otros</span>
                            </div>
                            {sinCat.map((k) => (
                                <FilaFuente
                                    key={k.label}
                                    fuente={k.label}
                                    importe={typeof k.value === "number" ? k.value : 0}
                                    nUcs={parseHintN(k.hint)}
                                    total={totalImp}
                                    color="bg-slate-400"
                                />
                            ))}
                        </div>
                    )}
                </div>
            </section>

            {/* Sección 3: navegación a vistas relacionadas */}
            <section>
                <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">
                    Explorar
                </h2>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
                    <Atajo
                        to="/resultados/uc"
                        título="Todas las UC"
                        descripción="Listado consolidado con búsqueda y filtro."
                    />
                    <Atajo
                        to="/resultados/actividades"
                        título="Por actividad"
                        descripción="Árbol de actividades con importe por origen."
                    />
                    <Atajo
                        to="/resultados/centros-de-coste"
                        título="Por centro de coste"
                        descripción="Árbol de centros con importe por origen."
                    />
                    <Atajo
                        to="/resultados/elementos-de-coste"
                        título="Por elemento de coste"
                        descripción="Árbol de elementos con importe por origen."
                    />
                    <Atajo
                        to="/resultados/anomalias"
                        título="Anomalías UC"
                        descripción="Integridad referencial frente a los árboles finales."
                    />
                    <Atajo
                        to="/informes/cuadro_10_1"
                        título="Informes normalizados"
                        descripción="Cuadros 10.1, 10.4, 10.5, 10.7 según el modelo SUE."
                    />
                    <Atajo
                        to="/informes-carta"
                        título="Informes a la carta"
                        descripción="Combinación libre de CC · actividades · EC."
                    />
                </div>
            </section>
        </div>
    );
}

export function ResultadosTodasUc() {
    const [sel, setSel] = useState<{ origen: string; id: string } | null>(null);

    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Resultados Fase 1 · Todas las UC"
                subtitle="Listado consolidado de todas las unidades de coste de la fase 1: presupuesto, suministros, amortizaciones, nóminas, SS, despidos, indemnizaciones y cargos."
            />
            <DataTable
                endpoint="/api/resultados/uc"
                queryKey="resultados:uc"
                rowKey="id"
                onRowSelect={(row) => {
                    const id = row.id;
                    const origen = row._origen;
                    if (id == null || origen == null) return;
                    setSel({ origen: String(origen), id: String(id) });
                }}
            />
            {sel && (
                <UcDetalleModal
                    origen={sel.origen}
                    ucId={sel.id}
                    onClose={() => setSel(null)}
                />
            )}
        </div>
    );
}

export function ResultadosActividades() {
    return (
        <Lista
            title="Resultados Fase 1 · Actividades"
            subtitle="Para cada nodo del árbol de actividades, importe desglosado por origen y total."
            endpoint="/api/resultados/actividades"
            queryKey="resultados:actividades"
            rowKey="identificador"
            reorderImportes={false}
        />
    );
}

export function ResultadosCentros() {
    return (
        <Lista
            title="Resultados Fase 1 · Centros de coste"
            subtitle="Para cada nodo del árbol de centros de coste, importe desglosado por origen y total."
            endpoint="/api/resultados/centros-de-coste"
            queryKey="resultados:centros"
            rowKey="identificador"
            reorderImportes={false}
        />
    );
}

export function ResultadosElementos() {
    return (
        <Lista
            title="Resultados Fase 1 · Elementos de coste"
            subtitle="Para cada nodo del árbol de elementos de coste, importe desglosado por origen y total."
            endpoint="/api/resultados/elementos-de-coste"
            queryKey="resultados:elementos"
            rowKey="identificador"
            reorderImportes={false}
        />
    );
}

function AnomaliasUnicosModal({ onClose }: { onClose: () => void }) {
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === "Escape") onClose();
        };
        window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [onClose]);

    return (
        <div
            className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-6"
            onClick={onClose}
            role="dialog"
            aria-modal="true"
        >
            <div
                className="flex max-h-[90vh] w-full max-w-4xl flex-col overflow-hidden rounded-lg border border-slate-200 bg-white shadow-xl"
                onClick={(e) => e.stopPropagation()}
            >
                <div className="flex items-center justify-between border-b border-slate-200 px-4 py-2">
                    <h2 className="text-sm font-medium uppercase tracking-wide text-slate-500">
                        Identificadores inexistentes (sin repeticiones)
                    </h2>
                    <button
                        type="button"
                        onClick={onClose}
                        className="rounded px-2 py-1 text-sm text-slate-500 hover:bg-slate-100 hover:text-slate-800"
                        aria-label="Cerrar"
                    >
                        ✕ Cerrar
                    </button>
                </div>
                <div className="overflow-auto p-4">
                    <DataTable
                        endpoint="/api/resultados/anomalias-unicos"
                        queryKey="resultados:anomalias-unicos"
                        rowKey="valor_inexistente"
                        pageSize={100}
                    />
                </div>
            </div>
        </div>
    );
}

export function ResultadosAnomalias() {
    const [verUnicos, setVerUnicos] = useState(false);
    const [sel, setSel] = useState<
        { origen: string; id: string; campo?: string; valor?: string } | null
    >(null);

    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Resultados Fase 1 · Anomalías UC"
                subtitle="Comprobación de integridad referencial: UC que referencian nodos inexistentes en los árboles finales. Pincha una fila para ver la ficha completa de la UC y el detalle de la anomalía."
            />
            <div>
                <button
                    type="button"
                    onClick={() => setVerUnicos(true)}
                    className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-100"
                >
                    Ver identificadores únicos
                </button>
            </div>
            <DataTable
                endpoint="/api/resultados/anomalias"
                queryKey="resultados:anomalias"
                rowKey="id"
                onRowSelect={(row) => {
                    const id = row.id;
                    const origen = row._origen;
                    if (id == null || origen == null) return;
                    setSel({
                        origen: String(origen),
                        id: String(id),
                        campo: row.campo == null ? undefined : String(row.campo),
                        valor: row.valor_inexistente == null
                            ? undefined : String(row.valor_inexistente),
                    });
                }}
            />
            {sel && (
                <UcDetalleModal
                    origen={sel.origen}
                    ucId={sel.id}
                    anomalia={{ campo: sel.campo, valor: sel.valor }}
                    onClose={() => setSel(null)}
                />
            )}
            {verUnicos && <AnomaliasUnicosModal onClose={() => setVerUnicos(false)} />}
        </div>
    );
}
