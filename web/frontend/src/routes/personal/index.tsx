import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { DataTable } from "@/components/DataTable";
import { KpiPanel } from "@/components/KpiPanel";
import { RecordCard } from "@/components/RecordCard";
import { Tabs } from "@/components/Tabs";
import { formatEuro } from "@/lib/format";

type GrupoLineas = { label: string; n: number; importe: number };
type GruposResponse = { grupos: GrupoLineas[] };

/** Tabla de líneas de nómina de un expediente, organizada en pestañas
 * por grupo (Costes sociales, Retribuciones ordinarias, etc.). */
function LineasExpedienteTabs({
    sector, expediente,
}: { sector: string; expediente: string }) {
    const { data, isLoading } = useQuery({
        queryKey: ["personal:exp-grupos", sector, expediente],
        queryFn: async (): Promise<GruposResponse> => {
            const r = await fetch(
                `/api/personal/expedientes/${encodeURIComponent(sector)}/${encodeURIComponent(expediente)}/grupos`,
            );
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return r.json() as Promise<GruposResponse>;
        },
    });
    const grupos = data?.grupos ?? [];
    const [activo, setActivo] = useState<string | null>(null);
    const activoLabel = activo ?? grupos[0]?.label ?? null;

    if (isLoading) return <div className="text-sm text-slate-500">Cargando…</div>;
    if (grupos.length === 0)
        return <div className="text-sm text-slate-500">Sin líneas para este expediente.</div>;

    const tabs = grupos.map((g) => ({
        key: g.label,
        label: (
            <span>
                {g.label}{" "}
                <span className="ml-1 text-xs text-slate-400 tabular-nums">
                    {g.n} · {formatEuro(g.importe)}
                </span>
            </span>
        ),
    }));

    return (
        <div className="flex flex-col gap-6">
            <div className="flex flex-col gap-3">
                <Tabs
                    tabs={tabs}
                    active={activoLabel ?? grupos[0].label}
                    onChange={setActivo}
                />
                {activoLabel && (
                    <DataTable
                        endpoint={`/api/personal/expedientes/${sector}/${encodeURIComponent(expediente)}/lineas`}
                        queryKey={`personal:exp:${sector}:${expediente}:grupo:${activoLabel}`}
                        rowKey="id"
                        extraParams={{ grupo: activoLabel }}
                        showPopoverOnRowClick
                        reorderImportes={activoLabel !== "Cargos"}
                    />
                )}
            </div>
            <div className="rounded-md border border-slate-200 bg-white p-4">
                <h3 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                    Unidades de coste generadas
                </h3>
                <DataTable
                    endpoint={`/api/personal/expedientes/${sector}/${encodeURIComponent(expediente)}/uc`}
                    queryKey={`personal:exp:${sector}:${expediente}:uc`}
                    rowKey="id"
                    showPopoverOnRowClick
                />
            </div>
        </div>
    );
}

/** Modal con un sub-DataTable de las líneas de nómina de un expediente. */
function LineasExpedienteModal({
    sector,
    expediente,
    onClose,
}: {
    sector: string;
    expediente: string;
    onClose: () => void;
}) {
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === "Escape") onClose();
        };
        window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [onClose]);

    return (
        <div
            className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-6"
            onClick={onClose}
            role="dialog"
            aria-modal="true"
        >
            <div
                className="flex max-h-[90vh] w-full max-w-6xl flex-col overflow-hidden rounded-lg border border-slate-200 bg-white shadow-xl"
                onClick={(e) => e.stopPropagation()}
            >
                <div className="flex items-center justify-between border-b border-slate-200 px-4 py-2">
                    <h2 className="text-sm font-medium uppercase tracking-wide text-slate-500">
                        Expediente {expediente} ({sector}) · líneas de nómina
                    </h2>
                    <button
                        type="button"
                        onClick={onClose}
                        className="rounded px-2 py-1 text-sm text-slate-500 hover:bg-slate-100 hover:text-slate-800"
                        aria-label="Cerrar"
                    >
                        ✕ Cerrar
                    </button>
                </div>
                <div className="flex flex-col gap-4 overflow-auto p-4">
                    <RecordCard
                        endpoint={`/api/personal/expedientes/${sector}/{id}`}
                        id={expediente}
                        queryKey={`personal:multiexp:exp-record:${sector}:${expediente}`}
                    />
                    <LineasExpedienteTabs sector={sector} expediente={expediente} />
                </div>
            </div>
        </div>
    );
}

const KPI = "/api/personal/_resumen";
const QK_RESUMEN = "personal:resumen";

function Cabecera({ title, subtitle }: { title: string; subtitle?: string }) {
    return (
        <div>
            <h1 className="text-2xl font-semibold">{title}</h1>
            {subtitle && <p className="text-sm text-slate-500">{subtitle}</p>}
        </div>
    );
}

type KpiRem = { label: string; value: number | string | null; format: string };
type KpiData = { kpis: KpiRem[] };

function getN(d: KpiData, label: string): number {
    const k = d.kpis.find((x) => x.label === label);
    return typeof k?.value === "number" ? k.value : 0;
}

const _COL_SECTOR: Record<string, string> = {
    PDI: "bg-blue-600",
    PTGAS: "bg-emerald-600",
    PVI: "bg-purple-500",
    Otros: "bg-slate-400",
};

import { Atajo as _Atajo, CajaTotal as _CajaTotal, FilaBarra as _FilaBarra, SeccionTitulo as _SecTit } from "@/components/Dashboard";
import { formatInt as _fmtInt } from "@/lib/format";

export function PersonalResumen() {
    const { data, isLoading, isError, error } = useQuery({
        queryKey: [QK_RESUMEN],
        queryFn: async (): Promise<KpiData> => {
            const r = await fetch(KPI);
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return (await r.json()) as KpiData;
        },
        staleTime: 30_000,
    });
    if (isError) {
        return <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">{error instanceof Error ? error.message : String(error)}</div>;
    }
    if (isLoading || !data) {
        return <div className="text-sm text-slate-500">Cargando…</div>;
    }
    const sectores: Array<{ nombre: string; n: number; imp: number }> = [];
    for (const sec of ["PDI", "PTGAS", "PVI", "Otros"]) {
        const n = getN(data, `Expedientes ${sec}`);
        const imp = getN(data, `Importe ${sec}`);
        if (n > 0 || imp > 0) sectores.push({ nombre: sec, n, imp });
    }
    const totalExp = getN(data, "Total expedientes");
    const totalImp = getN(data, "Importe total");
    const nMulti = getN(data, "Personas multiexpediente");

    return (
        <div className="flex flex-col gap-8">
            <Cabecera
                title="Personal · Resumen"
                subtitle="Expedientes, importes brutos y costes sociales agregados por sector. Pulsa una tarjeta de Explorar para ver el detalle."
            />

            <section>
                <_SecTit>Totales del ejercicio</_SecTit>
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                    <_CajaTotal etiqueta="Expedientes con cobro" valor={_fmtInt(totalExp)} sub={`Importe total ${formatEuro(totalImp)}`} />
                    <_CajaTotal etiqueta="Importe total nómina" valor={formatEuro(totalImp)} sub={`${_fmtInt(totalExp)} expedientes`} />
                    <_CajaTotal etiqueta="Multiexpediente" valor={_fmtInt(nMulti)} sub="Personas con expedientes en más de un sector" />
                </div>
            </section>

            <section>
                <_SecTit>Distribución por sector</_SecTit>
                <div className="rounded-lg border border-slate-200 bg-white p-4">
                    {sectores.map((s) => (
                        <_FilaBarra
                            key={s.nombre}
                            etiqueta={s.nombre}
                            importe={s.imp}
                            total={totalImp || 1}
                            color={_COL_SECTOR[s.nombre] ?? "bg-slate-500"}
                            extra={`${_fmtInt(s.n)} expedientes`}
                        />
                    ))}
                </div>
            </section>

            <section>
                <_SecTit>Explorar</_SecTit>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                    <_Atajo to="/personal/pdi" título="PDI" descripción="Personal docente e investigador." />
                    <_Atajo to="/personal/pvi" título="PVI" descripción="Personal investigador y otro personal." />
                    <_Atajo to="/personal/ptgas" título="PTGAS" descripción="Personal técnico, de gestión y servicios." />
                    <_Atajo to="/personal/otros" título="Otros" descripción="Becas, jubilados y otros sectores residuales." />
                    <_Atajo to="/personal/multiexpediente" título="Multiexpediente" descripción="Personas con varios expedientes." />
                    <_Atajo to="/personal/costes-sociales-calculados" título="Costes sociales calculados" descripción="Clases pasivas de PDI funcionario." />
                    <_Atajo to="/personal/atrasos-no-vinculados" título="Atrasos a no vinculados" descripción="Personal que ya no trabaja en la UJI." />
                    <_Atajo to="/personal/despidos" título="Despidos" descripción="CR 47 en proyecto general." />
                    <_Atajo to="/personal/indemnizaciones" título="Indemnizaciones asistencias" descripción="Tribunales y similares (CR 48)." />
                    <_Atajo to="/personal/anomalias" título="Anomalías PDI" descripción="Casos a revisar." />
                </div>
            </section>
        </div>
    );
}

function ExpedientesPorSector({
    sector,
    descripcion,
}: { sector: string; descripcion: string }) {
    const [expediente, setExpediente] = useState<string | null>(null);

    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title={`Personal · Expedientes ${sector}`}
                subtitle={descripcion}
            />
            <DataTable
                endpoint={`/api/personal/expedientes/${sector}`}
                queryKey={`personal:exp:${sector}`}
                rowKey="expediente"
                onRowSelect={(row) => {
                    const v = row.expediente;
                    setExpediente(v == null ? null : String(v));
                }}
            />
            {expediente && (
                <div className="rounded-md border border-slate-200 bg-white p-4">
                    <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-slate-500">
                        Líneas de nómina del expediente {expediente}
                    </h2>
                    <LineasExpedienteTabs sector={sector} expediente={expediente} />
                </div>
            )}
        </div>
    );
}

export function ExpedientesPDI() {
    return <ExpedientesPorSector sector="PDI" descripcion="Personal Docente e Investigador." />;
}

export function ExpedientesPTGAS() {
    return <ExpedientesPorSector sector="PTGAS" descripcion="Personal Técnico, de Gestión y de Administración y Servicios." />;
}

export function ExpedientesPVI() {
    return <ExpedientesPorSector sector="PVI" descripcion="Personal Visitante Investigador." />;
}

export function ExpedientesOtros() {
    return <ExpedientesPorSector sector="Otros" descripcion="Personal no clasificado en las categorías anteriores." />;
}

export function PersonalMultiexpediente() {
    const [perId, setPerId] = useState<string | null>(null);
    const [expSel, setExpSel] = useState<{ sector: string; expediente: string } | null>(null);

    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Personal · Multiexpediente"
                subtitle="Personas con expedientes en sectores distintos en el año analizado."
            />
            <DataTable
                endpoint="/api/personal/multiexpediente"
                queryKey="personal:multiexpediente"
                rowKey="per_id"
                onRowSelect={(row) => {
                    const v = row.per_id;
                    setPerId(v == null ? null : String(v));
                    setExpSel(null);
                }}
            />
            {perId && (
                <div className="rounded-md border border-slate-200 bg-white p-4">
                    <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-slate-500">
                        Matriz mensual de importes (per_id {perId})
                    </h2>
                    <DataTable
                        endpoint={`/api/personal/multiexpediente/${encodeURIComponent(perId)}/matriz`}
                        queryKey={`personal:multiexp:matriz:${perId}`}
                        rowKey="expediente"
                        pageSize={50}
                        onRowSelect={(row) => {
                            const exp = row.expediente;
                            const sec = row.sector;
                            if (exp == null || sec == null) return;
                            setExpSel({
                                sector: String(sec),
                                expediente: String(exp),
                            });
                        }}
                    />
                </div>
            )}
            {expSel && (
                <LineasExpedienteModal
                    sector={expSel.sector}
                    expediente={expSel.expediente}
                    onClose={() => setExpSel(null)}
                />
            )}
        </div>
    );
}

export function PersonalPersona() {
    const [perId, setPerId] = useState<string | null>(null);

    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Personal · Persona"
                subtitle="Vista por persona del reparto de SS y de todas sus UC retributivas."
            />
            <DataTable
                endpoint="/api/personal/persona"
                queryKey="personal:persona"
                rowKey="per_id"
                onRowSelect={(row) => {
                    const v = row.per_id;
                    setPerId(v == null ? null : String(v));
                }}
            />
            {perId && (
                <>
                    <RecordCard
                        endpoint="/api/personal/persona/{id}"
                        id={perId}
                        queryKey="personal:persona:detail"
                    />
                    <div className="rounded-md border border-slate-200 bg-white p-4">
                        <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-slate-500">
                            UC asociadas (retributivas + SS)
                        </h2>
                        <DataTable
                            endpoint={`/api/personal/persona/${encodeURIComponent(perId)}/uc`}
                            queryKey={`personal:persona:uc:${perId}`}
                            rowKey="id"
                        />
                    </div>
                </>
            )}
        </div>
    );
}

export function PersonalAnomaliasPdi() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Personal · Anomalías PDI"
                subtitle="Asignaturas con créditos impartidos cuya titulación no aparece en ningún catálogo de referencia."
            />
            <DataTable
                endpoint="/api/personal/anomalias-pdi"
                queryKey="personal:anomalias-pdi"
                rowKey="asignatura"
                showPopoverOnRowClick
            />
        </div>
    );
}

export function PersonalCostesSocialesCalculados() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Personal · Costes sociales calculados"
                subtitle="PDI funcionario en régimen de clases pasivas: importe simulado de cotización social que se reparte como si fuera SS cotizada."
            />
            <DataTable
                endpoint="/api/personal/costes-sociales-calculados"
                queryKey="personal:costes-sociales-calculados"
                rowKey="per_id"
                showPopoverOnRowClick
            />
        </div>
    );
}

export function PersonalAbsentismo() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Personal · Absentismo"
                subtitle="Meses con bonificación de seguridad social (tipo «BS» en la aplicación 1211): bajas tan largas que la SS las bonifica. Todo lo percibido y cotizado ese mes se desvía a una única UC imputada al centro raíz UJI y a la actividad «absentismo», con el elemento de coste de la categoría de la persona."
            />
            <KpiPanel
                endpoint="/api/personal/absentismo/_resumen"
                queryKey="personal:absentismo:resumen"
            />
            <DataTable
                endpoint="/api/personal/absentismo"
                queryKey="personal:absentismo"
                rowKey="id"
                showPopoverOnRowClick
            />
        </div>
    );
}

export function PersonalAtrasosNoVinculados() {
    return (
        <div className="flex flex-col gap-6">
            <Cabecera
                title="Personal · Atrasos a no vinculados"
                subtitle="Personas que en el año analizado solo cobran atrasos (CR 30/87) sin tener relación laboral activa con la UJI. Su importe queda fuera del reparto de costes analíticos."
            />
            <DataTable
                endpoint="/api/personal/atrasos-no-vinculados"
                queryKey="personal:atrasos-no-vinculados"
                rowKey="per_id"
            />
        </div>
    );
}
