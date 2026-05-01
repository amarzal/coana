import { ResourceView } from "@/components/ResourceView";
import { DataTable } from "@/components/DataTable";
import { KpiPanel } from "@/components/KpiPanel";

const KPI = "/api/amortizaciones/_resumen";
const QK_RESUMEN = "amortizaciones:resumen";

/** Cabecera con título + KPI panel común a todas las subvistas. */
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

export function AmortResumen() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Amortizaciones · Resumen"
                subtitle="Métricas globales del procesamiento del inventario."
            />
        </div>
    );
}

export function AmortEnriquecido() {
    return (
        <ResourceView
            title="Amortizaciones · Inventario con amortización"
            subtitle="Registros del inventario que sobreviven a los filtros y reciben importe de amortización para el año analizado."
            kpiEndpoint={KPI}
            listEndpoint="/api/amortizaciones/enriquecido"
            recordEndpoint="/api/amortizaciones/enriquecido/{id}"
            rowKey="id"
            queryKey="amort:enriquecido"
        />
    );
}

function Lista({
    title, subtitle, endpoint, queryKey,
}: { title: string; subtitle: string; endpoint: string; queryKey: string }) {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera title={title} subtitle={subtitle} />
            <DataTable
                endpoint={endpoint}
                queryKey={queryKey}
                rowKey="id"
            />
        </div>
    );
}

export function AmortFiltradosEstado() {
    return (
        <Lista
            title="Amortizaciones · Filtrados por estado"
            subtitle="Registros con estado B (baja) que se descartan antes del enriquecimiento."
            endpoint="/api/amortizaciones/filtrados/estado"
            queryKey="amort:filtrados-estado"
        />
    );
}

export function AmortFiltradosCuenta() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Amortizaciones · Filtrados por cuenta"
                subtitle="Registros descartados por tener una cuenta contable cuyo prefijo no está en la lista de aceptados."
            />
            <div className="rounded-md border border-slate-200 bg-white p-4">
                <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                    Detalle por cuenta filtrada
                </h2>
                <DataTable
                    endpoint="/api/amortizaciones/detalle-cuentas"
                    queryKey="amort:detalle-cuentas"
                    rowKey="cuenta"
                />
            </div>
            <div className="rounded-md border border-slate-200 bg-white p-4">
                <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                    Registros descartados (todos)
                </h2>
                <DataTable
                    endpoint="/api/amortizaciones/filtrados/cuenta"
                    queryKey="amort:filtrados-cuenta"
                    rowKey="id"
                />
            </div>
        </div>
    );
}

export function AmortFiltradosFecha() {
    return (
        <Lista
            title="Amortizaciones · Filtrados por fecha"
            subtitle="Registros con importe de amortización igual a 0 o sin días en el año analizado."
            endpoint="/api/amortizaciones/filtrados/fecha"
            queryKey="amort:filtrados-fecha"
        />
    );
}

export function AmortSinCuenta() {
    return (
        <Lista
            title="Amortizaciones · Sin cuenta"
            subtitle="Registros del inventario que no tienen cuenta contable; se descartan."
            endpoint="/api/amortizaciones/sin-cuenta"
            queryKey="amort:sin-cuenta"
        />
    );
}

export function AmortSinFechaAlta() {
    return (
        <Lista
            title="Amortizaciones · Sin fecha de alta"
            subtitle="Registros sin fecha de alta; no se puede calcular su período de amortización."
            endpoint="/api/amortizaciones/sin-fecha-alta"
            queryKey="amort:sin-fecha-alta"
        />
    );
}

export function AmortUc() {
    return (
        <ResourceView
            title="Amortizaciones · UC generadas"
            subtitle="Unidades de coste creadas a partir de los registros de inventario enriquecidos."
            kpiEndpoint={KPI}
            listEndpoint="/api/amortizaciones/uc"
            recordEndpoint="/api/amortizaciones/uc/{id}"
            rowKey="id"
            queryKey="amort:uc"
        />
    );
}

export function AmortSinCentro() {
    return (
        <Lista
            title="Amortizaciones · Sin centro de coste"
            subtitle="Registros enriquecidos que no han recibido centro de coste (ni por ubicación ni por descripción) y no generan UC."
            endpoint="/api/amortizaciones/sin-uc"
            queryKey="amort:sin-uc"
        />
    );
}
