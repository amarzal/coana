import { useState } from "react";
import { DataTable } from "@/components/DataTable";

export function InvestigacionGrupos() {
    const [idGrupo, setIdGrupo] = useState<string | null>(null);
    const [nombreSel, setNombreSel] = useState<string | null>(null);

    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">
                    Investigación · Grupos
                </h1>
                <p className="text-sm text-slate-500">
                    Listado de grupos de investigación con al menos una
                    persona activa en el año. Pincha una fila para ver sus
                    miembros: primero el interlocutor, luego los
                    coordinadores y luego el resto.
                </p>
            </div>
            <DataTable
                endpoint="/api/investigacion/grupos"
                queryKey="investigacion:grupos"
                rowKey="id_grupo"
                onRowSelect={(row) => {
                    const v = row.id_grupo;
                    setIdGrupo(v == null ? null : String(v));
                    setNombreSel(String(row.nombre_grupo ?? ""));
                }}
            />
            {idGrupo && (
                <div className="rounded-md border border-slate-200 bg-white p-4">
                    <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                        Personas del grupo {idGrupo}
                        {nombreSel ? ` · ${nombreSel}` : ""}
                    </h2>
                    <DataTable
                        key={idGrupo}
                        endpoint={`/api/investigacion/grupos/${encodeURIComponent(idGrupo)}/personas`}
                        queryKey={`investigacion:grupo:${idGrupo}:personas`}
                        rowKey="per_id"
                        reorderImportes={false}
                    />
                </div>
            )}
        </div>
    );
}
