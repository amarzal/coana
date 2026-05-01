import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import { formatValue, type ColumnFormat } from "@/lib/format";

type Kpi = {
    label: string;
    value: number | string | null;
    format: ColumnFormat;
    hint?: string | null;
};

type KpiPanelData = { kpis: Kpi[] };

type Props = {
    /** Endpoint relativo al panel KPI. Ej: "/api/presupuesto/_resumen". */
    endpoint: string;
    queryKey: string;
};

export function KpiPanel({ endpoint, queryKey }: Props) {
    const { data, isLoading, isError, error } = useQuery({
        queryKey: [queryKey],
        queryFn: async (): Promise<KpiPanelData> => {
            const res = await api.GET(endpoint as never, {} as never);
            const r = res as unknown as { data?: KpiPanelData; error?: unknown };
            if (r.error) throw new Error(JSON.stringify(r.error));
            if (!r.data) throw new Error("Respuesta vacía");
            return r.data;
        },
        staleTime: 30_000,
    });

    if (isError) {
        return (
            <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">
                KPIs: {error instanceof Error ? error.message : String(error)}
            </div>
        );
    }
    if (isLoading || !data) {
        return <div className="text-sm text-slate-500">Cargando KPIs…</div>;
    }

    return (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
            {data.kpis.map((k) => (
                <div
                    key={k.label}
                    className="rounded-md border border-slate-200 bg-white px-3 py-2"
                    title={k.hint ?? undefined}
                >
                    <div className="text-xs uppercase tracking-wide text-slate-500">
                        {k.label}
                    </div>
                    <div className="mt-0.5 truncate text-lg font-semibold tabular-nums">
                        {formatValue(k.value, k.format)}
                    </div>
                </div>
            ))}
        </div>
    );
}
