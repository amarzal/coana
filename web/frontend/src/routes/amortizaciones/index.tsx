import { useQuery } from "@tanstack/react-query";
import { ResourceView } from "@/components/ResourceView";
import { DataTable } from "@/components/DataTable";
import { Atajo, Cabecera, CajaTotal, FilaBarra, SeccionTitulo } from "@/components/Dashboard";
import { formatEuro, formatInt } from "@/lib/format";

const KPI = "/api/amortizaciones/_resumen";
const QK_RESUMEN = "amortizaciones:resumen";

type Kpi = { label: string; value: number | string | null; format: string };
type KpiData = { kpis: Kpi[] };

function getN(d: KpiData, label: string): number {
    const k = d.kpis.find((x) => x.label === label);
    return typeof k?.value === "number" ? k.value : 0;
}

export function AmortResumen() {
    const { data, isLoading, isError, error } = useQuery({
        queryKey: [QK_RESUMEN],
        queryFn: async (): Promise<KpiData> => {
            const r = await fetch(KPI);
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return (await r.json()) as KpiData;
        },
        staleTime: 30_000,
    });
    if (isError) {
        return (
            <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">
                {error instanceof Error ? error.message : String(error)}
            </div>
        );
    }
    if (isLoading || !data) {
        return <div className="text-sm text-slate-500">Cargando…</div>;
    }
    const nEnriq = getN(data, "Registros enriquecidos");
    const impEnriq = getN(data, "Importe amortización año");
    const nUc = getN(data, "UC generadas");
    const impUc = getN(data, "Importe UC");
    const nFiltEstado = getN(data, "Filtrados (estado B)");
    const nFiltCuenta = getN(data, "Filtrados (cuenta no válida)");
    const nFiltFecha = getN(data, "Filtrados (fecha)");
    const nSinCuenta = getN(data, "Sin cuenta");
    const nSinFecha = getN(data, "Sin fecha de alta");
    const nSinCentro = getN(data, "Sin centro de coste");
    const nFilt = nFiltEstado + nFiltCuenta + nFiltFecha;
    const nIncidencias = nSinCuenta + nSinFecha + nSinCentro;

    return (
        <div className="flex flex-col gap-8">
            <Cabecera
                title="Amortizaciones · Resumen"
                subtitle="Procesamiento del inventario de bienes para imputar la cuota anual de amortización a cada centro de coste."
            />

            <section>
                <SeccionTitulo>Totales del ejercicio</SeccionTitulo>
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                    <CajaTotal
                        etiqueta="Registros enriquecidos"
                        valor={formatInt(nEnriq)}
                        sub={`Importe amortización ${formatEuro(impEnriq)}`}
                    />
                    <CajaTotal
                        etiqueta="Unidades de coste"
                        valor={formatInt(nUc)}
                        sub={formatEuro(impUc)}
                    />
                    <CajaTotal
                        etiqueta="Incidencias"
                        valor={formatInt(nIncidencias)}
                        sub={`${formatInt(nFilt)} filtrados · ${formatInt(nSinCentro)} sin CC`}
                    />
                </div>
            </section>

            <section>
                <SeccionTitulo>Bienes descartados por motivo</SeccionTitulo>
                <div className="rounded-lg border border-slate-200 bg-white p-4">
                    <FilaBarra
                        etiqueta="Estado de baja"
                        importe={nFiltEstado}
                        total={Math.max(1, nFilt + nIncidencias)}
                        color="bg-slate-400"
                        extra={`${formatInt(nFiltEstado)} bienes`}
                    />
                    <FilaBarra
                        etiqueta="Cuenta no válida"
                        importe={nFiltCuenta}
                        total={Math.max(1, nFilt + nIncidencias)}
                        color="bg-slate-400"
                        extra={`${formatInt(nFiltCuenta)} bienes`}
                    />
                    <FilaBarra
                        etiqueta="Fecha fuera de rango"
                        importe={nFiltFecha}
                        total={Math.max(1, nFilt + nIncidencias)}
                        color="bg-slate-400"
                        extra={`${formatInt(nFiltFecha)} bienes`}
                    />
                    <FilaBarra
                        etiqueta="Sin cuenta"
                        importe={nSinCuenta}
                        total={Math.max(1, nFilt + nIncidencias)}
                        color="bg-amber-500"
                        extra={`${formatInt(nSinCuenta)} bienes`}
                    />
                    <FilaBarra
                        etiqueta="Sin fecha de alta"
                        importe={nSinFecha}
                        total={Math.max(1, nFilt + nIncidencias)}
                        color="bg-amber-500"
                        extra={`${formatInt(nSinFecha)} bienes`}
                    />
                    <FilaBarra
                        etiqueta="Sin centro de coste"
                        importe={nSinCentro}
                        total={Math.max(1, nFilt + nIncidencias)}
                        color="bg-rose-500"
                        extra={`${formatInt(nSinCentro)} bienes`}
                    />
                </div>
            </section>

            <section>
                <SeccionTitulo>Explorar</SeccionTitulo>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                    <Atajo to="/amortizaciones/enriquecido" título="Inventario con amortización" descripción="Bienes con su cuota anual calculada." />
                    <Atajo to="/amortizaciones/uc" título="UC generadas" descripción="Unidades de coste de amortización por bien y centro." />
                    <Atajo to="/amortizaciones/sin-centro" título="Sin centro de coste" descripción="Bienes que no se han podido imputar a un CC." />
                    <Atajo to="/amortizaciones/filtrados-estado" título="Filtrados por estado" descripción="Bienes en estado B (de baja)." />
                    <Atajo to="/amortizaciones/filtrados-cuenta" título="Filtrados por cuenta" descripción="Cuenta contable no válida." />
                    <Atajo to="/amortizaciones/filtrados-fecha" título="Filtrados por fecha" descripción="Fecha de alta fuera del rango procesable." />
                    <Atajo to="/amortizaciones/sin-cuenta" título="Sin cuenta" descripción="Bienes sin cuenta contable informada." />
                    <Atajo to="/amortizaciones/sin-fecha-alta" título="Sin fecha de alta" descripción="Bienes sin fecha de alta informada." />
                </div>
            </section>
        </div>
    );
}

export function AmortEnriquecido() {
    return (
        <ResourceView
            title="Amortizaciones · Inventario con amortización"
            subtitle="Registros del inventario que sobreviven a los filtros y reciben importe de amortización para el año analizado."
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
