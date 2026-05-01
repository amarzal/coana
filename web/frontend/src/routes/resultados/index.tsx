import { DataTable } from "@/components/DataTable";
import { KpiPanel } from "@/components/KpiPanel";

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

function Lista({
    title, subtitle, endpoint, queryKey, rowKey = "id",
}: {
    title: string; subtitle?: string; endpoint: string; queryKey: string;
    rowKey?: string;
}) {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera title={title} subtitle={subtitle} />
            <DataTable endpoint={endpoint} queryKey={queryKey} rowKey={rowKey} />
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
    return (
        <Lista
            title="Resultados Fase 1 · Todas las UC"
            subtitle="Listado consolidado de todas las unidades de coste de la fase 1: presupuesto, suministros, amortizaciones, nóminas, SS, despidos, indemnizaciones y cargos."
            endpoint="/api/resultados/uc"
            queryKey="resultados:uc"
            rowKey="id"
        />
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
        />
    );
}

export function ResultadosAnomalias() {
    return (
        <Lista
            title="Resultados Fase 1 · Anomalías UC"
            subtitle="Comprobación de integridad referencial: UC que referencian nodos inexistentes en los árboles finales."
            endpoint="/api/resultados/anomalias"
            queryKey="resultados:anomalias"
            rowKey="id"
        />
    );
}
