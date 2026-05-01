import { useState } from "react";
import { DataTable } from "@/components/DataTable";
import { KpiPanel } from "@/components/KpiPanel";
import { RecordCard } from "@/components/RecordCard";

export function PresupuestoUc() {
    const [selectedId, setSelectedId] = useState<string | null>(null);

    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">Presupuesto · Unidades de coste</h1>
                <p className="text-sm text-slate-500">
                    UC generadas por el traductor de presupuesto a partir de los apuntes.
                </p>
            </div>

            <KpiPanel
                endpoint="/api/presupuesto/_resumen"
                queryKey="presupuesto:resumen"
            />

            <DataTable
                endpoint="/api/presupuesto/uc"
                queryKey="presupuesto:uc"
                onRowSelect={(row) => setSelectedId(String(row.id))}
            />

            <RecordCard
                endpoint="/api/presupuesto/uc/{id}"
                id={selectedId}
                queryKey="presupuesto:uc:detail"
            />
        </div>
    );
}
