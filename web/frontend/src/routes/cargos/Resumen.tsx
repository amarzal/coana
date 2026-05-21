import { useQuery } from "@tanstack/react-query";
import { Atajo, Cabecera, CajaTotal, SeccionTitulo } from "@/components/Dashboard";
import { formatEuro, formatInt } from "@/lib/format";

type Kpi = { label: string; value: number | string | null; format: string };
type KpiData = { kpis: Kpi[] };

function getN(d: KpiData, label: string): number {
    const k = d.kpis.find((x) => x.label === label);
    return typeof k?.value === "number" ? k.value : 0;
}

export function CargosResumen() {
    const { data, isLoading, isError, error } = useQuery({
        queryKey: ["cargos:resumen"],
        queryFn: async (): Promise<KpiData> => {
            const r = await fetch("/api/cargos/_resumen");
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return (await r.json()) as KpiData;
        },
        staleTime: 30_000,
    });
    if (isError) {
        return <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">{error instanceof Error ? error.message : String(error)}</div>;
    }
    if (isLoading || !data) {
        return <div className="text-sm text-slate-500">Cargando…</div>;
    }
    const nPersonas = getN(data, "Personas con UC de cargos");
    const nUc = getN(data, "UC de cargos académicos");
    const imp = getN(data, "Importe imputado");

    return (
        <div className="flex flex-col gap-8">
            <Cabecera
                title="Cargos académicos · Resumen"
                subtitle="Reparto del CR 19/64 en proyecto general entre los cargos asimilados al RD 1086/1989, ponderando por días cobrados × importe mensual del cargo."
            />

            <section>
                <SeccionTitulo>Totales del ejercicio</SeccionTitulo>
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                    <CajaTotal etiqueta="Personas con cargo remunerado" valor={formatInt(nPersonas)} sub={`${formatInt(nUc)} UC generadas`} />
                    <CajaTotal etiqueta="UC de cargos académicos" valor={formatInt(nUc)} sub={formatEuro(imp)} />
                    <CajaTotal etiqueta="Importe total" valor={formatEuro(imp)} sub="Suma de retribución ordinaria + parte extra estimada del CE" />
                </div>
            </section>

            <section>
                <SeccionTitulo>Explorar</SeccionTitulo>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
                    <Atajo to="/cargos/personas-remuneradas" título="Por persona" descripción="Personas con cargo remunerado y sub-tabla por cargo." />
                    <Atajo to="/cargos/personas-cargos" título="Personas cargos" descripción="Histórico de personas × cargos con fechas." />
                    <Atajo to="/cargos/cargos" título="Catálogo de cargos" descripción="Cargos UJI con su asimilación al RD." />
                </div>
            </section>
        </div>
    );
}
