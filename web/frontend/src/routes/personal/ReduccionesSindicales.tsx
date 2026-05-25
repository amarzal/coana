import { useState } from "react";
import { Cabecera } from "@/components/Dashboard";
import { Tabs } from "@/components/Tabs";
import { DataTable } from "@/components/DataTable";

const TABS = [
    { key: "pdi", label: "PDI" },
    { key: "ptgas", label: "PTGAS" },
];

const ENDPOINT: Record<string, string> = {
    pdi: "/api/reducciones-sindicales/pdi",
    ptgas: "/api/reducciones-sindicales/ptgas",
};

const ROW_KEY: Record<string, string> = {
    pdi: "per_id",
    ptgas: "expediente",
};

/**
 * Reducciones por representación sindical, en dos pestañas:
 *
 * - PDI: tipos 37-40 de `reducciones docentes.xlsx`, en créditos
 *   traducidos a fracción de jornada por la fase 1.
 * - PTGAS: tipo 8 de `reducciones laborales.xlsx`, en días y porcentaje
 *   de jornada trabajada.
 */
export function ReduccionesSindicales() {
    const [active, setActive] = useState("pdi");
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Reducciones sindicales"
                subtitle="Dedicación a la representación sindical. El PDI se mide en créditos de reducción docente (tipos 37-40) traducidos a fracción de la jornada anual; el PTGAS, en días y porcentaje de jornada trabajada (tipo 8). Son dos mecanismos independientes."
            />
            <Tabs tabs={TABS} active={active} onChange={setActive} />
            <DataTable
                endpoint={ENDPOINT[active]}
                queryKey={`reducciones-sindicales:${active}`}
                rowKey={ROW_KEY[active]}
                showPopoverOnRowClick
            />
        </div>
    );
}
