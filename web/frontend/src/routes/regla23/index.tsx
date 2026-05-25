import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ResourceView } from "@/components/ResourceView";
import { DataTable } from "@/components/DataTable";
import { Atajo, Cabecera, CajaTotal, SeccionTitulo } from "@/components/Dashboard";
import { Tabs } from "@/components/Tabs";
import { formatEuro, formatFloat, formatInt } from "@/lib/format";

export { Regla23DedicacionPdi } from "./DedicacionPdi";

const KPI = "/api/regla23/_resumen";
const QK_RESUMEN = "regla23:resumen";

type Kpi = { label: string; value: number | string | null; format: string };
type KpiData = { kpis: Kpi[] };

function getN(d: KpiData, label: string): number {
    const k = d.kpis.find((x) => x.label === label);
    return typeof k?.value === "number" ? k.value : 0;
}

export function Regla23Resumen() {
    const { data, isLoading, isError, error } = useQuery({
        queryKey: [QK_RESUMEN],
        queryFn: async (): Promise<KpiData> => {
            const r = await fetch(KPI);
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return (await r.json()) as KpiData;
        },
        staleTime: 30_000,
    });
    if (isError) return <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">{error instanceof Error ? error.message : String(error)}</div>;
    if (isLoading || !data) return <div className="text-sm text-slate-500">Cargando…</div>;
    const nDed = getN(data, "Expedientes con dedicación");
    const cred = getN(data, "Créditos impartidos (total)");
    const nHND = getN(data, "Horas no oficiales");
    const impAtrasos = getN(data, "Bolsa de atrasos");
    const nApart = getN(data, "Expedientes apartados");
    const nDesp = getN(data, "UC despidos");
    const impDesp = getN(data, "Importe despidos");
    const nIndem = getN(data, "UC indemnizaciones");
    const nCargos = getN(data, "UC cargos en proyectos");
    const impCargos = getN(data, "Importe cargos");

    return (
        <div className="flex flex-col gap-8">
            <Cabecera
                title="Regla 23 · Resumen"
                subtitle="Reparto de la masa retributiva indiferenciada del PDI/PVI entre actividades y centros, ponderando por la dedicación horaria de cada persona."
            />

            <section>
                <SeccionTitulo>Dedicación docente</SeccionTitulo>
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                    <CajaTotal etiqueta="Expedientes con dedicación" valor={formatInt(nDed)} sub="PDI/PVI con al menos una hora registrada" />
                    <CajaTotal etiqueta="Créditos impartidos" valor={formatFloat(cred)} sub="POD oficial agregado del ejercicio" />
                    <CajaTotal etiqueta="Horas no oficiales" valor={formatInt(nHND)} sub="Estimaciones de docencia propia" />
                </div>
            </section>

            <section>
                <SeccionTitulo>Tratamientos especiales</SeccionTitulo>
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
                    <CajaTotal etiqueta="Bolsa de atrasos" valor={formatEuro(impAtrasos)} sub="CR 30/87 que se distribuyen por dedicación" />
                    <CajaTotal etiqueta="Expedientes apartados" valor={formatInt(nApart)} sub="Quedan fuera del reparto general" />
                    <CajaTotal etiqueta="UC despidos" valor={formatInt(nDesp)} sub={formatEuro(impDesp)} />
                    <CajaTotal etiqueta="UC cargos en proyecto específico" valor={formatInt(nCargos)} sub={formatEuro(impCargos)} />
                </div>
            </section>

            <section>
                <SeccionTitulo>Explorar</SeccionTitulo>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                    <Atajo to="/regla23/dedicacion" título="Dedicación docente" descripción="POD por expediente y asignatura." />
                    <Atajo to="/regla23/no-oficial" título="Docencia no oficial" descripción="Estimación de horas en proyectos propios." />
                    <Atajo to="/regla23/estructura" título="Estructura estudios" descripción="Titulaciones, asignaturas y centros." />
                    <Atajo to="/regla23/cargos" título="Cargos" descripción="Asimilación del CR 19/64 a cargos del RD." />
                    <Atajo to="/regla23/sin-titulacion" título="Asignaturas sin titulación" descripción="Casos pendientes de mapeo." />
                    <Atajo to="/regla23/anomalias" título="Anomalías" descripción="Datos a revisar antes de cerrar el reparto." />
                </div>
            </section>
            <div className="hidden">{formatInt(nIndem)}</div>
        </div>
    );
}

function Lista({
    title, subtitle, endpoint, queryKey, rowKey = "expediente",
    showPopoverOnRowClick = false,
}: {
    title: string; subtitle: string; endpoint: string; queryKey: string;
    rowKey?: string;
    showPopoverOnRowClick?: boolean;
}) {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera title={title} subtitle={subtitle} />
            <DataTable
                endpoint={endpoint}
                queryKey={queryKey}
                rowKey={rowKey}
                showPopoverOnRowClick={showPopoverOnRowClick}
            />
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
            subtitle="Horas y dedicación a estudios propios, microcredenciales, doctorado y otras actividades no asociadas a titulaciones oficiales. Pincha una fila para ver el detalle completo."
            endpoint="/api/regla23/horas-no-oficiales"
            queryKey="regla23:horas"
            rowKey="gre_id"
            showPopoverOnRowClick
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
