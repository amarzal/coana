import { useState } from "react";
import { DataTable } from "@/components/DataTable";
import { KpiPanel } from "@/components/KpiPanel";
import { Tabs } from "@/components/Tabs";

type Sector = "PDI" | "PVI";

const DESCRIPCIONES: Record<Sector, string> = {
    PDI: (
        "Vista 360º del Personal Docente e Investigador: cobrado, "
        + "cotizado, dedicación a actividades y unidades de coste generadas. "
        + "La columna Δ cuadre permite identificar al instante personas "
        + "cuyos importes no terminan correctamente convertidos a UC."
    ),
    PVI: (
        "Vista 360º del Personal Vinculado a la Investigación: cobrado, "
        + "cotizado, dedicación y UC. Mismo formato que la vista PDI."
    ),
};

type TabKey =
    | "resumen"
    | "laboral"
    | "nomina"
    | "regla23"
    | "uc";

const TABS: { key: TabKey; label: string }[] = [
    { key: "resumen", label: "Resumen / Cuadre" },
    { key: "laboral", label: "Relación laboral" },
    { key: "nomina", label: "Nómina" },
    { key: "regla23", label: "Dedicación regla 23" },
    { key: "uc", label: "UC generadas" },
];

export function PersonaPdiPvi({ sector }: { sector: Sector }) {
    const [perId, setPerId] = useState<number | null>(null);
    const [personaSel, setPersonaSel] = useState<string | null>(null);
    const [activeTab, setActiveTab] = useState<TabKey>("resumen");

    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">Personal · {sector}</h1>
                <p className="text-sm text-slate-500">{DESCRIPCIONES[sector]}</p>
            </div>
            <DataTable
                endpoint={`/api/persona360/${sector}/personas`}
                queryKey={`persona360:${sector}:master`}
                rowKey="per_id"
                onRowSelect={(row) => {
                    const pid = Number(row.per_id);
                    setPerId(Number.isFinite(pid) ? pid : null);
                    setPersonaSel(String(row.persona ?? ""));
                    setActiveTab("resumen");
                }}
            />
            {perId !== null && (
                <div className="rounded-md border border-slate-200 bg-white p-4">
                    <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                        {personaSel || `per_id ${perId}`}
                    </h2>
                    <Tabs
                        tabs={TABS.map((t) => ({ key: t.key, label: t.label }))}
                        active={activeTab}
                        onChange={(k) => setActiveTab(k as TabKey)}
                    />
                    <div className="mt-4">
                        {activeTab === "resumen" && (
                            <ResumenTab sector={sector} perId={perId} />
                        )}
                        {activeTab === "laboral" && (
                            <LaboralTab sector={sector} perId={perId} />
                        )}
                        {activeTab === "nomina" && (
                            <NominaTab sector={sector} perId={perId} />
                        )}
                        {activeTab === "regla23" && (
                            <Regla23Tab sector={sector} perId={perId} />
                        )}
                        {activeTab === "uc" && (
                            <UCTab sector={sector} perId={perId} />
                        )}
                    </div>
                </div>
            )}
        </div>
    );
}

function ResumenTab({ sector, perId }: { sector: Sector; perId: number }) {
    return (
        <div className="flex flex-col gap-6">
            <KpiPanel
                endpoint={`/api/persona360/${sector}/personas/${perId}/resumen`}
                queryKey={`persona360:${sector}:${perId}:resumen`}
            />
            <div>
                <h3 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                    Cuadre por concepto
                </h3>
                <p className="mb-3 text-xs text-slate-500">
                    Una fila por flujo de retribución. Δ = cobrado − UC.
                    En condiciones normales todas las celdas Δ son 0; cualquier
                    valor distinto indica un descuadre puntual entre nómina y
                    UC para ese concepto.
                </p>
                <DataTable
                    key={`cuadre-${perId}`}
                    endpoint={`/api/persona360/${sector}/personas/${perId}/cuadre`}
                    queryKey={`persona360:${sector}:${perId}:cuadre`}
                    rowKey="concepto"
                    reorderImportes={false}
                />
            </div>
        </div>
    );
}

function LaboralTab({ sector, perId }: { sector: Sector; perId: number }) {
    return (
        <div className="flex flex-col gap-3">
            <p className="text-xs text-slate-500">
                Períodos observados en las nóminas del año: una fila por
                combinación (expediente, categoría de plaza, categoría
                RR.HH.) con el primer y último mes de pago y nº de meses.
            </p>
            <DataTable
                key={`laboral-${perId}`}
                endpoint={`/api/persona360/${sector}/personas/${perId}/laboral`}
                queryKey={`persona360:${sector}:${perId}:laboral`}
                rowKey="expediente"
                reorderImportes={false}
            />
        </div>
    );
}

function NominaTab({ sector, perId }: { sector: Sector; perId: number }) {
    return (
        <div className="flex flex-col gap-3">
            <p className="text-xs text-slate-500">
                Detalle línea a línea de la nómina del año (todos los
                expedientes de la persona). Cada fila incluye el concepto
                retributivo, el tipo de coste y el <em>flujo</em> que canaliza
                ese importe hacia su UC (la misma clasificación del cuadre).
            </p>
            <DataTable
                key={`nomina-${perId}`}
                endpoint={`/api/persona360/${sector}/personas/${perId}/nomina`}
                queryKey={`persona360:${sector}:${perId}:nomina`}
                rowKey="concepto_retributivo"
                reorderImportes={false}
            />
        </div>
    );
}

function Regla23Tab({ sector, perId }: { sector: Sector; perId: number }) {
    const [sub, setSub] = useState<"resumen" | "totales" | "detalle">("resumen");
    return (
        <div className="flex flex-col gap-4">
            <Tabs
                tabs={[
                    { key: "resumen", label: "Reparto por grupo y origen" },
                    { key: "totales", label: "Totales por actividad/centro" },
                    { key: "detalle", label: "Detalle por actividad" },
                ]}
                active={sub}
                onChange={(k) => setSub(k as typeof sub)}
            />
            {sub === "resumen" && (
                <DataTable
                    key={`r23-resumen-${perId}`}
                    endpoint={`/api/persona360/${sector}/personas/${perId}/regla23/resumen`}
                    queryKey={`persona360:${sector}:${perId}:r23:resumen`}
                    rowKey="grupo"
                    reorderImportes={false}
                />
            )}
            {sub === "totales" && (
                <DataTable
                    key={`r23-totales-${perId}`}
                    endpoint={`/api/persona360/${sector}/personas/${perId}/regla23/totales`}
                    queryKey={`persona360:${sector}:${perId}:r23:totales`}
                    rowKey="actividad"
                    reorderImportes={false}
                />
            )}
            {sub === "detalle" && (
                <DataTable
                    key={`r23-detalle-${perId}`}
                    endpoint={`/api/persona360/${sector}/personas/${perId}/regla23/detalle`}
                    queryKey={`persona360:${sector}:${perId}:r23:detalle`}
                    rowKey="origen_id"
                    showPopoverOnRowClick
                    reorderImportes={false}
                />
            )}
        </div>
    );
}

function UCTab({ sector, perId }: { sector: Sector; perId: number }) {
    return (
        <div className="flex flex-col gap-3">
            <p className="text-xs text-slate-500">
                Todas las unidades de coste vinculadas a la persona: retributivas
                (extras, despidos, indemnizaciones, cargos, reparto regla 23) y
                de seguridad social. La cabecera muestra el importe total.
            </p>
            <DataTable
                key={`uc-${perId}`}
                endpoint={`/api/persona360/${sector}/personas/${perId}/uc`}
                queryKey={`persona360:${sector}:${perId}:uc`}
                rowKey="id"
                reorderImportes={false}
            />
        </div>
    );
}

export function PersonaPdi() {
    return <PersonaPdiPvi sector="PDI" />;
}

export function PersonaPvi() {
    return <PersonaPdiPvi sector="PVI" />;
}
