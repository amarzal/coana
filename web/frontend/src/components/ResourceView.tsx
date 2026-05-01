import { useState } from "react";
import { DataTable } from "@/components/DataTable";
import { KpiPanel } from "@/components/KpiPanel";
import { RecordCard } from "@/components/RecordCard";

type Props = {
    title: string;
    subtitle?: string;
    /** Endpoint de KPIs. Si se omite, no se muestra el panel. Ej: "/api/presupuesto/_resumen" */
    kpiEndpoint?: string;
    /** Endpoint de listado. Ej: "/api/presupuesto/uc" */
    listEndpoint: string;
    /** Endpoint de registro con `{id}` interpolable. Ej: "/api/presupuesto/uc/{id}" */
    recordEndpoint: string;
    /** Identificador único de fila (por defecto "id"). */
    rowKey?: string;
    /** Prefijo de cache key para react-query. */
    queryKey: string;
};

/**
 * Vista canónica de un recurso: título, KPIs, tabla y ficha al seleccionar.
 *
 * Acopla los tres componentes reutilizables (DataTable, KpiPanel,
 * RecordCard) en el patrón que se repite en todos los bloques del visor.
 */
export function ResourceView({
    title,
    subtitle,
    kpiEndpoint,
    listEndpoint,
    recordEndpoint,
    rowKey = "id",
    queryKey,
}: Props) {
    const [selectedId, setSelectedId] = useState<string | null>(null);

    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">{title}</h1>
                {subtitle && (
                    <p className="text-sm text-slate-500">{subtitle}</p>
                )}
            </div>

            {kpiEndpoint && (
                <KpiPanel
                    endpoint={kpiEndpoint}
                    queryKey={`${queryKey}:resumen`}
                />
            )}

            <DataTable
                endpoint={listEndpoint}
                queryKey={`${queryKey}:list`}
                rowKey={rowKey}
                onRowSelect={(row) => {
                    const v = row[rowKey];
                    setSelectedId(v == null ? null : String(v));
                }}
            />

            <RecordCard
                endpoint={recordEndpoint}
                id={selectedId}
                queryKey={`${queryKey}:detail`}
            />
        </div>
    );
}
