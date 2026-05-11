import { DataTable } from "@/components/DataTable";
import { KpiPanel } from "@/components/KpiPanel";
import { TreeView } from "@/components/TreeView";

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

export function PresupuestoResumen() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Presupuesto · Resumen"
                subtitle="Métricas globales del traductor de presupuesto."
            />
            <KpiPanel endpoint={KPI} queryKey={QK_RESUMEN} />
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
                subtitle="Distribución de los costes centrales de mantenimiento, limpieza y seguridad entre centros, según presencia superficial."
            />
            <div className="rounded-md border border-amber-200 bg-amber-50 p-4 text-sm text-amber-900">
                <strong>Pendiente.</strong> El reparto se calcula al vuelo
                durante la fase 1 pero no se persiste a parquet. Esta vista
                aparecerá cuando las matrices de presencia se materialicen
                (igual que «Superficies / Presencia centros»).
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
