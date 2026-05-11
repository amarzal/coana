import { useState } from "react";
import { DataTable } from "@/components/DataTable";

export function CargosCargos() {
    const [cargoSel, setCargoSel] = useState<string | null>(null);
    const [nombreSel, setNombreSel] = useState<string | null>(null);

    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">
                    Cargos académicos · Catálogo de cargos
                </h1>
                <p className="text-sm text-slate-500">
                    Catálogo de cargos (data/entrada/nóminas/cargos.xlsx) con
                    su nombre, cuantía mensual, tipo y, para el año analizado,
                    el número de personas distintas que lo han ocupado al menos
                    un día por sector principal (PDI · PVI · PTGAS · Otros) y
                    el TOTAL. Pincha una fila para ver las personas concretas.
                </p>
            </div>
            <DataTable
                endpoint="/api/cargos/cargos"
                queryKey="cargos:cargos"
                rowKey="cargo"
                onRowSelect={(row) => {
                    setCargoSel(String(row.cargo ?? ""));
                    setNombreSel(String(row.nombre ?? ""));
                }}
            />
            {cargoSel && (
                <div className="rounded-md border border-slate-200 bg-white p-4">
                    <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                        Personas que ocupan el cargo {cargoSel}
                        {nombreSel ? ` — ${nombreSel}` : ""}
                    </h2>
                    <DataTable
                        key={cargoSel}
                        endpoint={`/api/cargos/cargos/${encodeURIComponent(cargoSel)}/personas`}
                        queryKey={`cargos:cargos:${cargoSel}:personas`}
                        rowKey="per_id"
                        showPopoverOnRowClick
                    />
                </div>
            )}
        </div>
    );
}
