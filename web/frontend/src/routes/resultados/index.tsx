import { useEffect, useState } from "react";
import { DataTable } from "@/components/DataTable";
import { KpiPanel } from "@/components/KpiPanel";
import { RecordCard } from "@/components/RecordCard";

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
    origen, ucId, onClose,
}: { origen: string; ucId: string; onClose: () => void }) {
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
                <div className="overflow-auto p-4">
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

export function ResultadosResumen() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Resultados Fase 1 · Resumen"
                subtitle="Visión consolidada de las UC generadas, con desglose por origen."
            />
            <KpiPanel endpoint={KPI} queryKey={QK_RESUMEN} />
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

    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Resultados Fase 1 · Anomalías UC"
                subtitle="Comprobación de integridad referencial: UC que referencian nodos inexistentes en los árboles finales."
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
            />
            {verUnicos && <AnomaliasUnicosModal onClose={() => setVerUnicos(false)} />}
        </div>
    );
}
