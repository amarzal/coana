import { useQuery } from "@tanstack/react-query";
import { Atajo, Cabecera, CajaTotal, SeccionTitulo } from "@/components/Dashboard";
import { formatFloat, formatInt } from "@/lib/format";

type Kpi = { label: string; value: number | string | null; format: string };
type KpiData = { kpis: Kpi[] };

function getN(d: KpiData, label: string): number {
    const k = d.kpis.find((x) => x.label === label);
    return typeof k?.value === "number" ? k.value : 0;
}

export function SuperficiesResumen() {
    const { data, isLoading, isError, error } = useQuery({
        queryKey: ["superficies:resumen"],
        queryFn: async (): Promise<KpiData> => {
            const r = await fetch("/api/superficies/_resumen");
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return (await r.json()) as KpiData;
        },
        staleTime: 30_000,
    });
    if (isError) return <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">{error instanceof Error ? error.message : String(error)}</div>;
    if (isLoading || !data) return <div className="text-sm text-slate-500">Cargando…</div>;
    const m2 = getN(data, "Superficie total");
    const nComplejos = getN(data, "Complejos");
    const nEdif = getN(data, "Edificaciones");
    const nZonas = getN(data, "Zonas");
    const nUbic = getN(data, "Ubicaciones");

    return (
        <div className="flex flex-col gap-8">
            <Cabecera
                title="Superficies · Resumen"
                subtitle="Inventario de espacios del campus organizados en complejo → edificación → zona → ubicación."
            />
            <section>
                <SeccionTitulo>Totales del campus</SeccionTitulo>
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                    <CajaTotal etiqueta="Superficie total" valor={`${formatFloat(m2)} m²`} sub={`${formatInt(nUbic)} ubicaciones inventariadas`} />
                    <CajaTotal etiqueta="Estructura física" valor={`${formatInt(nComplejos)} / ${formatInt(nEdif)} / ${formatInt(nZonas)}`} sub="Complejos · Edificaciones · Zonas" />
                    <CajaTotal etiqueta="Ubicaciones" valor={formatInt(nUbic)} sub="Granularidad más fina del inventario" />
                </div>
            </section>
            <section>
                <SeccionTitulo>Explorar</SeccionTitulo>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                    <Atajo to="/superficies/totales" título="Totales" descripción="Vista plana con métricas por complejo/edificio/zona." />
                    <Atajo to="/superficies/presencia" título="Presencia centros" descripción="Matriz de presencia de centros en cada zona." />
                </div>
            </section>
        </div>
    );
}
