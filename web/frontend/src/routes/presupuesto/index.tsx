import { useQuery } from "@tanstack/react-query";
import { NavLink } from "react-router";
import { DataTable } from "@/components/DataTable";
import { TreeView } from "@/components/TreeView";
import { formatEuro, formatInt } from "@/lib/format";
import { cn } from "@/lib/cn";

const KPI = "/api/presupuesto/_resumen";
const QK_RESUMEN = "presupuesto:resumen";

function Cabecera({ title, subtitle }: { title: string; subtitle?: string }) {
    return (
        <div>
            <h1 className="text-2xl font-semibold">{title}</h1>
            {subtitle && <p className="text-sm text-slate-500">{subtitle}</p>}
        </div>
    );
}

function Lista({
    title, subtitle, endpoint, queryKey, rowKey = "id",
}: {
    title: string; subtitle?: string; endpoint: string; queryKey: string;
    rowKey?: string;
}) {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera title={title} subtitle={subtitle} />
            <DataTable
                endpoint={endpoint}
                queryKey={queryKey}
                rowKey={rowKey}
                showPopoverOnRowClick
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

function getN(data: KpiPanelData, label: string): number {
    const k = data.kpis.find((x) => x.label === label);
    return typeof k?.value === "number" ? k.value : 0;
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

function FilaCategoria({
    etiqueta, n, importe, total, color,
}: {
    etiqueta: string;
    n: number;
    importe: number;
    total: number;
    color: string;
}) {
    const pct = total > 0 ? (100 * importe) / total : 0;
    return (
        <div className="flex items-center gap-3 py-1.5">
            <div className="w-44 text-sm font-medium text-slate-700">{etiqueta}</div>
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
            <div className="w-28 text-right text-xs tabular-nums text-slate-500">
                {formatInt(n)} apuntes
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

export function PresupuestoResumen() {
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

    const nUc = getN(data, "UC generadas");
    const impUc = getN(data, "Importe UC");
    const nSin = getN(data, "Apuntes sin UC");
    const impSin = getN(data, "Importe sin UC");
    const nFilt = getN(data, "Apuntes filtrados");
    const impFilt = getN(data, "Importe filtrado");
    const nTotal = nUc + nSin + nFilt;
    const impTotal = impUc + impSin + impFilt;

    return (
        <div className="flex flex-col gap-8">
            <Cabecera
                title="Presupuesto · Resumen"
                subtitle="Apuntes presupuestarios procesados por el traductor: cuántos se convierten en UC, cuántos quedan sin clasificar y cuántos se filtran como no imputables."
            />

            <section>
                <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">
                    Procesamiento de apuntes
                </h2>
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                    <CajaTotal
                        etiqueta="Apuntes procesados"
                        valor={formatInt(nTotal)}
                        sub={`Importe total ${formatEuro(impTotal)}`}
                    />
                    <CajaTotal
                        etiqueta="Unidades de coste"
                        valor={formatInt(nUc)}
                        sub={`${formatEuro(impUc)} · ${impTotal > 0 ? ((impUc / impTotal) * 100).toFixed(1) : "0"} % del importe`}
                    />
                    <CajaTotal
                        etiqueta="Apuntes no convertidos"
                        valor={formatInt(nSin + nFilt)}
                        sub={`${formatEuro(impSin + impFilt)} · ${impTotal > 0 ? (((impSin + impFilt) / impTotal) * 100).toFixed(1) : "0"} %`}
                    />
                </div>
            </section>

            <section>
                <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">
                    Distribución de apuntes
                </h2>
                <div className="rounded-lg border border-slate-200 bg-white p-4">
                    <FilaCategoria
                        etiqueta="UC generadas"
                        n={nUc}
                        importe={impUc}
                        total={impTotal}
                        color="bg-emerald-600"
                    />
                    <FilaCategoria
                        etiqueta="Sin clasificar"
                        n={nSin}
                        importe={impSin}
                        total={impTotal}
                        color="bg-amber-500"
                    />
                    <FilaCategoria
                        etiqueta="Filtrados"
                        n={nFilt}
                        importe={impFilt}
                        total={impTotal}
                        color="bg-slate-400"
                    />
                </div>
            </section>

            <section>
                <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">
                    Explorar
                </h2>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                    <Atajo
                        to="/presupuesto/uc"
                        título="Unidades de coste"
                        descripción="UC generadas por el traductor a partir de los apuntes."
                    />
                    <Atajo
                        to="/presupuesto/sin-clasificar"
                        título="Sin clasificar"
                        descripción="Apuntes válidos pero sin actividad asignable por ninguna regla."
                    />
                    <Atajo
                        to="/presupuesto/filtrados"
                        título="Apuntes filtrados"
                        descripción="Apuntes descartados por el filtro previo, agrupados por motivo."
                    />
                    <Atajo
                        to="/presupuesto/suministros"
                        título="Suministros"
                        descripción="UC de energía, agua y gas repartidas por presencia en zonas."
                    />
                    <Atajo
                        to="/presupuesto/reglas/actividad"
                        título="Reglas aplicadas"
                        descripción="Estadísticas por regla de actividad / CC / EC."
                    />
                    <Atajo
                        to="/presupuesto/arbol/actividades"
                        título="Estructuras finales"
                        descripción="Árboles de actividades, CC y EC tras aplicar las reglas."
                    />
                </div>
            </section>
        </div>
    );
}

export function PresupuestoUc() {
    return (
        <Lista
            title="Presupuesto · Unidades de coste"
            subtitle="UC generadas por el traductor de presupuesto a partir de los apuntes."
            endpoint="/api/presupuesto/uc"
            queryKey="presupuesto:uc"
        />
    );
}

export function PresupuestoSinClasificar() {
    return (
        <Lista
            title="Presupuesto · Sin clasificar"
            subtitle="Apuntes que pasaron el filtro previo pero no obtuvieron actividad por ninguna regla."
            endpoint="/api/presupuesto/sin-clasificar"
            queryKey="presupuesto:sin-clasificar"
            rowKey="asiento"
        />
    );
}

export function PresupuestoFiltrados() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Presupuesto · Apuntes filtrados"
                subtitle="Apuntes descartados por el filtro previo, agrupados por motivo y con la lista completa."
            />
            <div className="rounded-md border border-slate-200 bg-white p-4">
                <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                    Resumen por motivo
                </h2>
                <DataTable
                    endpoint="/api/presupuesto/filtrados-por-motivo"
                    queryKey="presupuesto:filtrados:resumen"
                    rowKey="motivo"
                    showPopoverOnRowClick
                />
            </div>
            <div className="rounded-md border border-slate-200 bg-white p-4">
                <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                    Apuntes descartados (todos)
                </h2>
                <DataTable
                    endpoint="/api/presupuesto/filtrados"
                    queryKey="presupuesto:filtrados:todos"
                    rowKey="asiento"
                    showPopoverOnRowClick
                />
            </div>
        </div>
    );
}

export function PresupuestoSuministros() {
    return (
        <Lista
            title="Presupuesto · Suministros"
            subtitle="UC generadas por reparto de los gastos de energía, agua y gas según presencia de centros en zonas, edificios y complejos."
            endpoint="/api/presupuesto/uc-suministros"
            queryKey="presupuesto:uc-suministros"
            rowKey="id"
        />
    );
}

export function PresupuestoDistribucionOTOP() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Presupuesto · Distribución mantenimientos OTOP"
                subtitle="Reparto de los costes centrales de mantenimiento, limpieza y seguridad entre centros, según la presencia superficial calculada por OTOP."
            />
            <div className="rounded-md border border-slate-200 bg-white p-4">
                <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                    Resumen por centro y elemento de coste
                </h2>
                <DataTable
                    endpoint="/api/presupuesto/otop-resumen"
                    queryKey="presupuesto:otop:resumen"
                    rowKey="_centro_de_coste"
                    showPopoverOnRowClick
                />
            </div>
            <div className="rounded-md border border-slate-200 bg-white p-4">
                <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                    Detalle del centro con mayor importe
                </h2>
                <DataTable
                    endpoint="/api/presupuesto/otop-detalle"
                    queryKey="presupuesto:otop:detalle"
                    rowKey="asiento"
                    showPopoverOnRowClick
                />
            </div>
        </div>
    );
}

export function PresupuestoReglasActividad() {
    return (
        <Lista
            title="Presupuesto · Reglas de actividad"
            subtitle="Para cada regla del traductor de presupuesto que asigna actividad, número de apuntes e importe."
            endpoint="/api/presupuesto/reglas/actividad"
            queryKey="presupuesto:reglas:act"
            rowKey="regla"
        />
    );
}

export function PresupuestoReglasCC() {
    return (
        <Lista
            title="Presupuesto · Reglas de centro de coste"
            subtitle="Para cada regla del traductor que asigna centro de coste, número de apuntes e importe."
            endpoint="/api/presupuesto/reglas/cc"
            queryKey="presupuesto:reglas:cc"
            rowKey="regla"
        />
    );
}

export function PresupuestoReglasEC() {
    return (
        <Lista
            title="Presupuesto · Reglas de elemento de coste"
            subtitle="Para cada regla del traductor que asigna elemento de coste, número de apuntes e importe."
            endpoint="/api/presupuesto/reglas/ec"
            queryKey="presupuesto:reglas:ec"
            rowKey="regla"
        />
    );
}

function ArbolFinal({
    title, ruta,
}: { title: string; ruta: string }) {
    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">{title}</h1>
                <p className="text-sm text-slate-500">
                    Árbol final tras aplicar las reglas. Los nodos creados
                    dinámicamente quedan en su sitio del árbol original.
                </p>
            </div>
            <TreeView
                endpoint={`/api/entradas/tree?ruta=${encodeURIComponent(ruta)}`}
                queryKey={`presupuesto:arbol:${ruta}`}
            />
        </div>
    );
}

// Los árboles finales se sirven desde data/fase1/ pero el endpoint
// /api/entradas/tree solo lee de data/entrada/. Necesitamos un nuevo
// endpoint para leer de data/fase1/.
export function PresupuestoArbolActividades() {
    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">Presupuesto · Árbol de actividades (final)</h1>
                <p className="text-sm text-slate-500">
                    Árbol de actividades tras aplicar las reglas del traductor de presupuesto.
                </p>
            </div>
            <TreeView
                endpoint="/api/resultados/arbol/actividades"
                queryKey="presupuesto:arbol:actividades"
            />
        </div>
    );
}

export function PresupuestoArbolCentros() {
    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">Presupuesto · Árbol de centros de coste (final)</h1>
                <p className="text-sm text-slate-500">
                    Árbol de centros de coste tras aplicar las reglas.
                </p>
            </div>
            <TreeView
                endpoint="/api/resultados/arbol/centros-de-coste"
                queryKey="presupuesto:arbol:centros"
            />
        </div>
    );
}

export function PresupuestoArbolElementos() {
    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">Presupuesto · Árbol de elementos de coste (final)</h1>
                <p className="text-sm text-slate-500">
                    Árbol de elementos de coste tras aplicar las reglas.
                </p>
            </div>
            <TreeView
                endpoint="/api/resultados/arbol/elementos-de-coste"
                queryKey="presupuesto:arbol:elementos"
            />
        </div>
    );
}

export { ArbolFinal };
