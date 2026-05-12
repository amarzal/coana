import { useState } from "react";
import { DataTable } from "@/components/DataTable";

export function CargosPersonasRemuneradas() {
    const [perId, setPerId] = useState<number | null>(null);
    const [personaSel, setPersonaSel] = useState<string | null>(null);
    const [totalSel, setTotalSel] = useState<number | null>(null);

    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">
                    Cargos académicos · Por persona
                </h1>
                <p className="text-sm text-slate-500">
                    Personas con al menos un cargo remunerado (con asimilación
                    a RD 1086/1989) activo en 2025. Pincha una fila para ver
                    todos sus cargos —remunerados y anomalías— y las unidades
                    de coste tentativas que se generan.
                </p>
            </div>
            <DataTable
                endpoint="/api/cargos/personas-remuneradas"
                queryKey="cargos:personas-remuneradas"
                rowKey="per_id"
                onRowSelect={(row) => {
                    const pid = Number(row.per_id);
                    setPerId(Number.isFinite(pid) ? pid : null);
                    setPersonaSel(String(row.persona ?? ""));
                    setTotalSel(
                        typeof row.total_cr19_64 === "number"
                            ? row.total_cr19_64
                            : null,
                    );
                }}
            />
            {perId !== null && (
                <div className="rounded-md border border-slate-200 bg-white p-4">
                    <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                        Cargos de {personaSel || `per_id ${perId}`}
                        {totalSel !== null
                            ? ` · Total CR 19/64: ${totalSel.toLocaleString("es-ES", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} €`
                            : ""}
                    </h2>
                    <DataTable
                        key={perId}
                        endpoint={`/api/cargos/personas-remuneradas/${perId}/cargos`}
                        queryKey={`cargos:persona:${perId}:cargos`}
                        rowKey="id"
                        showPopoverOnRowClick
                        reorderImportes={false}
                    />
                </div>
            )}
        </div>
    );
}
