import { ResourceView } from "@/components/ResourceView";
import { DataTable } from "@/components/DataTable";
import { KpiPanel } from "@/components/KpiPanel";

const KPI = "/api/personal/_resumen";
const QK_RESUMEN = "personal:resumen";

function Cabecera({ title, subtitle }: { title: string; subtitle?: string }) {
    return (
        <>
            <div>
                <h1 className="text-2xl font-semibold">{title}</h1>
                {subtitle && <p className="text-sm text-slate-500">{subtitle}</p>}
            </div>
            <KpiPanel endpoint={KPI} queryKey={QK_RESUMEN} />
        </>
    );
}

export function PersonalResumen() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Personal · Resumen"
                subtitle="Expedientes e importes agregados por sector."
            />
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
            kpiEndpoint={KPI}
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
    return (
        <ResourceView
            title="Personal · Persona"
            subtitle="Vista por persona del reparto de SS y de todas sus UC retributivas."
            kpiEndpoint={KPI}
            listEndpoint="/api/personal/persona"
            recordEndpoint="/api/personal/persona/{id}"
            rowKey="per_id"
            queryKey="personal:persona"
        />
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
