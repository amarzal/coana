import { useEffect, useState } from "react";
import { DataTable } from "@/components/DataTable";
import { RecordCard } from "@/components/RecordCard";

function KalendasDetalleModal({
    perId, contrato, persona, onClose,
}: { perId: string; contrato: string; persona: string; onClose: () => void }) {
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === "Escape") onClose();
        };
        window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [onClose]);

    const clave = `${perId}:${contrato}`;
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
                        Kalendas · per_id {perId} · contrato {contrato}
                        {persona ? ` · ${persona}` : ""}
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
                        endpoint="/api/investigacion/horas-kalendas/{id}"
                        id={clave}
                        queryKey={`investigacion:kalendas:${clave}`}
                    />
                </div>
            </div>
        </div>
    );
}

export function InvestigacionHorasKalendas() {
    const [sel, setSel] = useState<{ perId: string; contrato: string; persona: string } | null>(null);

    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">
                    Investigación · Horas Kalendas
                </h1>
                <p className="text-sm text-slate-500">
                    Suma de las horas declaradas y validadas en Kalendas
                    (solo «Proyecto de investigacion») por cada investigador
                    (<code>per_id</code>) en cada contrato del SGIT. Pincha una
                    fila para ver todo su detalle.
                </p>
            </div>
            <DataTable
                endpoint="/api/investigacion/horas-kalendas"
                queryKey="investigacion:horas-kalendas"
                rowKey="per_id"
                reorderImportes={false}
                onRowSelect={(row) => {
                    const perId = row.per_id;
                    const contrato = row.contrato;
                    if (perId == null || contrato == null) return;
                    setSel({
                        perId: String(perId),
                        contrato: String(contrato),
                        persona: String(row.persona ?? ""),
                    });
                }}
            />
            {sel && (
                <KalendasDetalleModal
                    perId={sel.perId}
                    contrato={sel.contrato}
                    persona={sel.persona}
                    onClose={() => setSel(null)}
                />
            )}
        </div>
    );
}
