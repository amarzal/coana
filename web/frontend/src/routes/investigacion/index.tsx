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

export function InvestigacionUC() {
    const [sel, setSel] = useState<{ perId: string; actividad: string } | null>(
        null,
    );

    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Investigación · Unidades de coste"
                subtitle="Distribución porcentual de las horas de investigación por (persona, actividad). En esta anualidad solo se calculan porcentajes; el importe en euros se añadirá al integrar la regla 23."
            />
            <DataTable
                endpoint="/api/investigacion/uc"
                queryKey="investigacion:uc"
                rowKey="actividad"
                onRowSelect={(row) => {
                    const per = row.per_id;
                    const act = row.actividad;
                    if (per == null || act == null) {
                        setSel(null);
                    } else {
                        setSel({ perId: String(per), actividad: String(act) });
                    }
                }}
            />
            {sel && (
                <div className="rounded-md border border-slate-200 bg-white p-4">
                    <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-slate-500">
                        Registros que contribuyen a{" "}
                        <span className="font-mono text-slate-700">{sel.actividad}</span>{" "}
                        para per_id {sel.perId}
                    </h2>
                    <DataTable
                        endpoint={`/api/investigacion/uc/${encodeURIComponent(sel.perId)}/${encodeURIComponent(sel.actividad)}/detalle`}
                        queryKey={`investigacion:uc:detalle:${sel.perId}:${sel.actividad}`}
                        rowKey="identificador"
                        showPopoverOnRowClick
                    />
                </div>
            )}
        </div>
    );
}
