import { useState } from "react";
import { DataTable } from "@/components/DataTable";
import { Tabs } from "@/components/Tabs";

const TABS = [
    { key: "complejos", label: "Complejos" },
    { key: "edificaciones", label: "Edificaciones" },
    { key: "zonas", label: "Zonas" },
];

const ENDPOINTS: Record<string, string> = {
    complejos: "/api/superficies/totales/complejos",
    edificaciones: "/api/superficies/totales/edificaciones",
    zonas: "/api/superficies/totales/zonas",
};

const ROW_KEYS: Record<string, string> = {
    complejos: "complejo",
    edificaciones: "edificación",
    zonas: "zona",
};

export function SuperficiesTotales() {
    const [active, setActive] = useState<string>("complejos");

    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">Superficies · Totales</h1>
                <p className="text-sm text-slate-500">
                    Suma de m² agregada a tres niveles jerárquicos. Los totales de
                    cada nivel inferior coinciden con la agregación del nivel superior.
                </p>
            </div>

            <Tabs tabs={TABS} active={active} onChange={setActive} />

            <DataTable
                endpoint={ENDPOINTS[active]}
                queryKey={`superficies:totales:${active}`}
                rowKey={ROW_KEYS[active]}
                pageSize={50}
            />
        </div>
    );
}
