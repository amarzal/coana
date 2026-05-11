import { useState } from "react";
import { ResourceView } from "@/components/ResourceView";
import { DataTable } from "@/components/DataTable";
import { KpiPanel } from "@/components/KpiPanel";
import { Tabs } from "@/components/Tabs";

const KPI = "/api/regla23/_resumen";
const QK_RESUMEN = "regla23:resumen";

function Cabecera({ title, subtitle }: { title: string; subtitle?: string }) {
    return (
        <div>
            <h1 className="text-2xl font-semibold">{title}</h1>
            {subtitle && <p className="text-sm text-slate-500">{subtitle}</p>}
        </div>
    );
}

export function Regla23Resumen() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Regla 23 · Resumen"
                subtitle="Métricas del reparto de la masa retributiva indiferenciada del PDI/PVI."
            />
            <KpiPanel endpoint={KPI} queryKey={QK_RESUMEN} />
        </div>
    );
}

function Lista({
    title, subtitle, endpoint, queryKey, rowKey = "expediente",
}: {
    title: string; subtitle: string; endpoint: string; queryKey: string;
    rowKey?: string;
}) {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera title={title} subtitle={subtitle} />
            <DataTable endpoint={endpoint} queryKey={queryKey} rowKey={rowKey} />
        </div>
    );
}

// 1. Dedicación docente — 3 sub-pestañas
const DED_TABS = [
    { key: "asignaturas", label: "Por asignatura" },
    { key: "titulaciones", label: "Por titulación" },
    { key: "estudios", label: "Por estudio" },
];
const DED_EP: Record<string, string> = {
    asignaturas: "/api/regla23/dedicacion/asignaturas",
    titulaciones: "/api/regla23/dedicacion/titulaciones",
    estudios: "/api/regla23/dedicacion/estudios",
};

export function Regla23DedicacionDocente() {
    const [active, setActive] = useState("asignaturas");
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Regla 23 · Dedicación docente"
                subtitle="Dedicación en créditos por expediente, agrupada por asignatura, titulación o estudio."
            />
            <Tabs tabs={DED_TABS} active={active} onChange={setActive} />
            <DataTable
                endpoint={DED_EP[active]}
                queryKey={`regla23:dedicacion:${active}`}
                rowKey="expediente"
                showPopoverOnRowClick
            />
        </div>
    );
}

// 2-7. Listas simples
export function Regla23DocenciaNoOficial() {
    return (
        <Lista
            title="Regla 23 · Docencia no oficial"
            subtitle="Horas y dedicación a estudios propios, microcredenciales, doctorado y otras actividades no asociadas a titulaciones oficiales."
            endpoint="/api/regla23/horas-no-oficiales"
            queryKey="regla23:horas"
            rowKey="perid"
        />
    );
}

export function Regla23EstructuraEstudios() {
    return (
        <Lista
            title="Regla 23 · Estructura estudios"
            subtitle="Catálogo de titulaciones del año (activas o sin créditos), agrupadas por estudio."
            endpoint="/api/regla23/estructura-estudios"
            queryKey="regla23:estructura"
            rowKey="titulación"
        />
    );
}

export function Regla23BolsaAtrasos() {
    return (
        <Lista
            title="Regla 23 · Bolsa de atrasos"
            subtitle="Líneas de PDI/PVI con concepto retributivo de atrasos (30 o 87), apartadas del reparto del año."
            endpoint="/api/regla23/atrasos"
            queryKey="regla23:atrasos"
            rowKey="id"
        />
    );
}

export function Regla23ExpedientesApartados() {
    return (
        <Lista
            title="Regla 23 · Expedientes apartados"
            subtitle="Expedientes que quedan sin ingresos reales en el año tras separar la bolsa de atrasos."
            endpoint="/api/regla23/apartados"
            queryKey="regla23:apartados"
            rowKey="expediente"
        />
    );
}

// 8-10. UC especiales — con ResourceView (lista + ficha)
export function Regla23Despidos() {
    return (
        <ResourceView
            title="Regla 23 · Despidos"
            subtitle="UC de PDI/PVI por concepto retributivo de despido."
            
            listEndpoint="/api/regla23/despidos"
            recordEndpoint="/api/regla23/despidos/{id}"
            rowKey="id"
            queryKey="regla23:despidos"
        />
    );
}

export function Regla23IndemnizacionesAsistencias() {
    return (
        <ResourceView
            title="Regla 23 · Indemnizaciones por asistencias"
            subtitle="UC de PDI/PVI por indemnizaciones por asistencias a tribunales y similares."
            
            listEndpoint="/api/regla23/indemnizaciones"
            recordEndpoint="/api/regla23/indemnizaciones/{id}"
            rowKey="id"
            queryKey="regla23:indemnizaciones"
        />
    );
}

export function Regla23Cargos() {
    return (
        <ResourceView
            title="Regla 23 · Cargos en proyectos"
            subtitle="UC de PDI/PVI por cargos académicos asociados a un proyecto específico (concepto 19/64 fuera de TABLA-PROYECTOS-GENERALES)."
            
            listEndpoint="/api/regla23/cargos"
            recordEndpoint="/api/regla23/cargos/{id}"
            rowKey="id"
            queryKey="regla23:cargos"
        />
    );
}

// 11. Asignaturas sin titulación
export function Regla23AsignaturasSinTitulacion() {
    return (
        <Lista
            title="Regla 23 · Asignaturas sin titulación"
            subtitle="Asignaturas con créditos impartidos cuya titulación no aparece en ningún catálogo de referencia."
            endpoint="/api/regla23/sin-titulacion"
            queryKey="regla23:sin-titulacion"
            rowKey="asignatura"
        />
    );
}

// 12. Anomalías — 2 pestañas
const ANOM_TABS = [
    { key: "resolucion", label: "Pod sin titulación efectiva" },
    { key: "multiples", label: "Múltiples con grado" },
];
const ANOM_EP: Record<string, string> = {
    resolucion: "/api/regla23/anomalias/resolucion",
    multiples: "/api/regla23/anomalias/multiples-grado",
};
const ANOM_KEY: Record<string, string> = {
    resolucion: "asignatura",
    multiples: "asignatura",
};

export function Regla23Anomalias() {
    const [active, setActive] = useState("resolucion");
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Regla 23 · Anomalías"
                subtitle="Filas de pod sin titulación efectiva resoluble, y asignaturas con varias titulaciones donde alguna no es máster."
            />
            <Tabs tabs={ANOM_TABS} active={active} onChange={setActive} />
            <DataTable
                endpoint={ANOM_EP[active]}
                queryKey={`regla23:anomalias:${active}`}
                rowKey={ANOM_KEY[active]}
            />
        </div>
    );
}
