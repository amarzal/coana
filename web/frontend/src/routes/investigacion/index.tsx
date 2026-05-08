import { useState } from "react";
import { DataTable } from "@/components/DataTable";
import { KpiPanel } from "@/components/KpiPanel";
import { RecordCard } from "@/components/RecordCard";

const KPI = "/api/investigacion/_resumen";
const QK_RESUMEN = "investigacion:resumen";

function Cabecera({ title, subtitle }: { title: string; subtitle?: string }) {
    return (
        <div>
            <h1 className="text-2xl font-semibold">{title}</h1>
            {subtitle && <p className="text-sm text-slate-500">{subtitle}</p>}
        </div>
    );
}

export function InvestigacionResumen() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Investigación · Resumen"
                subtitle="Indicadores de dedicación a investigación (grupos, tesis, proyectos)."
            />
            <KpiPanel endpoint={KPI} queryKey={QK_RESUMEN} />
        </div>
    );
}

export function InvestigacionPersonas() {
    const [perId, setPerId] = useState<string | null>(null);

    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Investigación · Dedicación por persona"
                subtitle="Horas de investigación calculadas a partir de coordinación de grupos, dirección de tesis y participación en proyectos."
            />
            <DataTable
                endpoint="/api/investigacion/personas"
                queryKey="investigacion:personas"
                rowKey="per_id"
                onRowSelect={(row) => {
                    const v = row.per_id;
                    setPerId(v == null ? null : String(v));
                }}
            />
            {perId && (
                <>
                    <RecordCard
                        endpoint="/api/investigacion/personas/{id}"
                        id={perId}
                        queryKey="investigacion:persona:detail"
                    />
                    <div className="rounded-md border border-slate-200 bg-white p-4">
                        <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-slate-500">
                            Detalle de registros (per_id {perId})
                        </h2>
                        <DataTable
                            endpoint={`/api/investigacion/personas/${encodeURIComponent(perId)}/detalle`}
                            queryKey={`investigacion:detalle:${perId}`}
                            rowKey="identificador"
                            showPopoverOnRowClick
                        />
                    </div>
                </>
            )}
        </div>
    );
}
