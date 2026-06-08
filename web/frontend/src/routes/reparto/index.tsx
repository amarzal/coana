import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { DataTable } from "@/components/DataTable";
import { formatEuro, formatInt } from "@/lib/format";

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
    title: string; subtitle?: string; endpoint: string; queryKey: string; rowKey?: string;
}) {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera title={title} subtitle={subtitle} />
            <DataTable endpoint={endpoint} queryKey={queryKey} rowKey={rowKey} reorderImportes={false} />
        </div>
    );
}

type Kpi = { label: string; value: number | string | null; format: string; hint?: string | null };
type KpiPanelData = { kpis: Kpi[] };

function CajaKpi({ k }: { k: Kpi }) {
    const valor =
        typeof k.value === "number"
            ? (k.format === "euro" ? formatEuro(k.value) : formatInt(k.value))
            : String(k.value ?? "—");
    return (
        <div className="rounded-lg border border-slate-200 bg-white px-5 py-4 shadow-sm">
            <div className="text-xs uppercase tracking-wide text-slate-500">{k.label}</div>
            <div className="mt-1 text-3xl font-semibold tabular-nums text-slate-900">{valor}</div>
            {k.hint && <div className="mt-1 text-xs text-slate-500">{k.hint}</div>}
        </div>
    );
}

export function RepartoResumen() {
    const { data, isLoading, isError, error } = useQuery({
        queryKey: ["reparto:resumen"],
        queryFn: async (): Promise<KpiPanelData> => {
            const r = await fetch("/api/reparto/_resumen");
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return (await r.json()) as KpiPanelData;
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
    if (isLoading || !data) return <div className="text-sm text-slate-500">Cargando…</div>;

    return (
        <div className="flex flex-col gap-8">
            <Cabecera
                title="Reparto de actividades · Resumen"
                subtitle="Reparto de los costes dag entre las actividades finalistas (no-dag) de cada centro. Pulsa «Reparto actividades» en la barra lateral para recalcular."
            />
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
                {data.kpis.map((k) => <CajaKpi key={k.label} k={k} />)}
            </div>
        </div>
    );
}

export function RepartoUc() {
    return (
        <Lista
            title="Reparto de actividades · UC tras reparto"
            subtitle="Conjunto de UC tras repartir los costes dag: UC no-dag intactas, fragmentos (origen «reparto-dag», con su marca dag de procedencia) y UC dag anómalas sin repartir."
            endpoint="/api/reparto/uc"
            queryKey="reparto:uc"
            rowKey="id"
        />
    );
}

export function RepartoPorcentajes() {
    return (
        <Lista
            title="Reparto de actividades · Porcentajes por centro"
            subtitle="Para cada centro, peso de cada actividad no-dag sobre el total no-dag del centro. Es la clave de reparto aplicada a las UC dag de ese centro."
            endpoint="/api/reparto/porcentajes"
            queryKey="reparto:porcentajes"
            rowKey="clave"
        />
    );
}

export function RepartoDag() {
    const [dag, setDag] = useState<string | null>(null);
    const [dagLabel, setDagLabel] = useState<string>("");
    const [destino, setDestino] = useState<{ centro: string; actividad: string; label: string } | null>(null);
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Reparto de actividades · Por actividad dag"
                subtitle="Cada fila es una actividad dag que se repartió. Pincha una para ver a qué actividades finalistas (y centros) fue a parar su coste; pincha un destino para ver los fragmentos individuales (una UC por elemento de coste)."
            />
            <DataTable
                endpoint="/api/reparto/dag"
                queryKey="reparto:dag"
                rowKey="marca_dag"
                reorderImportes={false}
                onRowSelect={(row) => {
                    setDag(String(row.marca_dag ?? ""));
                    setDagLabel(
                        `${row.código ?? ""} · ${row.descripción ?? row.marca_dag ?? ""}`,
                    );
                    setDestino(null);
                }}
            />
            {dag && (
                <div className="rounded-md border border-slate-200 bg-white p-4">
                    <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                        Reparto de {dagLabel}
                    </h2>
                    <DataTable
                        key={dag}
                        endpoint={`/api/reparto/dag/${encodeURIComponent(dag)}`}
                        queryKey={`reparto:dag:detalle:${dag}`}
                        reorderImportes={false}
                        onRowSelect={(row) => {
                            const centro = String(row.centro_de_coste ?? "");
                            const actividad = String(row.actividad ?? "");
                            setDestino({
                                centro,
                                actividad,
                                label: `${row.código ?? ""} · ${row.descripción ?? actividad} (${centro})`,
                            });
                        }}
                    />
                </div>
            )}
            {dag && destino && (
                <div className="rounded-md border border-slate-200 bg-white p-4">
                    <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                        Fragmentos · {destino.label}
                    </h2>
                    <DataTable
                        key={`${dag}:${destino.centro}:${destino.actividad}`}
                        endpoint={`/api/reparto/dag/${encodeURIComponent(dag)}/fragmentos`}
                        queryKey={`reparto:dag:fragmentos:${dag}:${destino.centro}:${destino.actividad}`}
                        extraParams={{ centro: destino.centro, actividad: destino.actividad }}
                        reorderImportes={false}
                        showPopoverOnRowClick
                    />
                </div>
            )}
        </div>
    );
}

export function RepartoAnomalias() {
    return (
        <Lista
            title="Reparto de actividades · Anomalías"
            subtitle="UC dag que NO se han repartido: centros sin base no-dag (servicios/edificios sin actividad finalista) o centro distinto del esperado por la tabla dag→centro."
            endpoint="/api/reparto/anomalias"
            queryKey="reparto:anomalias"
            rowKey="id"
        />
    );
}
