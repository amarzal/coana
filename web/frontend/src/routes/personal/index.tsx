import { useState } from "react";
import { ResourceView } from "@/components/ResourceView";
import { DataTable } from "@/components/DataTable";
import { KpiPanel } from "@/components/KpiPanel";
import { RecordCard } from "@/components/RecordCard";

const KPI = "/api/personal/_resumen";
const QK_RESUMEN = "personal:resumen";

function Cabecera({ title, subtitle }: { title: string; subtitle?: string }) {
    return (
        <div>
            <h1 className="text-2xl font-semibold">{title}</h1>
            {subtitle && <p className="text-sm text-slate-500">{subtitle}</p>}
        </div>
    );
}

export function PersonalResumen() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Personal · Resumen"
                subtitle="Expedientes e importes agregados por sector."
            />
            <KpiPanel endpoint={KPI} queryKey={QK_RESUMEN} />
        </div>
    );
}

function ExpedientesPorSector({
    sector,
    descripcion,
}: { sector: string; descripcion: string }) {
    return (
        <ResourceView
            title={`Personal · Expedientes ${sector}`}
            subtitle={descripcion}
            listEndpoint={`/api/personal/expedientes/${sector}`}
            recordEndpoint={`/api/personal/expedientes/${sector}/{id}`}
            rowKey="expediente"
            queryKey={`personal:exp:${sector}`}
        />
    );
}

export function ExpedientesPDI() {
    return <ExpedientesPorSector sector="PDI" descripcion="Personal Docente e Investigador." />;
}

export function ExpedientesPTGAS() {
    return <ExpedientesPorSector sector="PTGAS" descripcion="Personal Técnico, de Gestión y de Administración y Servicios." />;
}

export function ExpedientesPVI() {
    return <ExpedientesPorSector sector="PVI" descripcion="Personal Visitante Investigador." />;
}

export function ExpedientesOtros() {
    return <ExpedientesPorSector sector="Otros" descripcion="Personal no clasificado en las categorías anteriores." />;
}

export function PersonalMultiexpediente() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Personal · Multiexpediente"
                subtitle="Personas con expedientes en sectores distintos en el año analizado."
            />
            <DataTable
                endpoint="/api/personal/multiexpediente"
                queryKey="personal:multiexpediente"
                rowKey="per_id"
            />
        </div>
    );
}

export function PersonalPersona() {
    const [perId, setPerId] = useState<string | null>(null);

    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Personal · Persona"
                subtitle="Vista por persona del reparto de SS y de todas sus UC retributivas."
            />
            <DataTable
                endpoint="/api/personal/persona"
                queryKey="personal:persona"
                rowKey="per_id"
                onRowSelect={(row) => {
                    const v = row.per_id;
                    setPerId(v == null ? null : String(v));
                }}
            />
            {perId && (
                <>
                    <RecordCard
                        endpoint="/api/personal/persona/{id}"
                        id={perId}
                        queryKey="personal:persona:detail"
                    />
                    <div className="rounded-md border border-slate-200 bg-white p-4">
                        <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-slate-500">
                            UC asociadas (retributivas + SS)
                        </h2>
                        <DataTable
                            endpoint={`/api/personal/persona/${encodeURIComponent(perId)}/uc`}
                            queryKey={`personal:persona:uc:${perId}`}
                            rowKey="id"
                        />
                    </div>
                </>
            )}
        </div>
    );
}

export function PersonalAnomaliasPdi() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Personal · Anomalías PDI"
                subtitle="Asignaturas con créditos impartidos cuya titulación no aparece en ningún catálogo de referencia."
            />
            <DataTable
                endpoint="/api/personal/anomalias-pdi"
                queryKey="personal:anomalias-pdi"
                rowKey="asignatura"
            />
        </div>
    );
}
