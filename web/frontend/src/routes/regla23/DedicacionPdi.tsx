import { useState } from "react";
import { DataTable } from "@/components/DataTable";

export function Regla23DedicacionPdi() {
    const [perId, setPerId] = useState<number | null>(null);
    const [personaSel, setPersonaSel] = useState<string | null>(null);
    const [horasSel, setHorasSel] = useState<number | null>(null);

    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">
                    Regla 23 · Dedicación PDI
                </h1>
                <p className="text-sm text-slate-500">
                    Personas con horas registradas en 2025 a partir de las
                    fuentes disponibles (POD, tesis…). Pincha una fila para
                    ver el detalle de actividades. Las horas son las brutas;
                    el factor ×2,5 se aplicará en el cálculo final de la
                    regla 23.
                </p>
            </div>
            <DataTable
                endpoint="/api/regla23/dedicacion-pdi/personas"
                queryKey="regla23:dedicacion-pdi:personas"
                rowKey="per_id"
                onRowSelect={(row) => {
                    const pid = Number(row.per_id);
                    setPerId(Number.isFinite(pid) ? pid : null);
                    setPersonaSel(String(row.persona ?? ""));
                    setHorasSel(
                        typeof row.horas_total === "number"
                            ? row.horas_total
                            : null,
                    );
                }}
            />
            {perId !== null && (
                <>
                    <div className="rounded-md border border-slate-200 bg-white p-4">
                        <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                            Relación laboral de {personaSel || `per_id ${perId}`}
                        </h2>
                        <p className="mb-3 text-xs text-slate-500">
                            Períodos observados en las nóminas del año
                            analizado: una fila por cada combinación
                            (expediente, categoría plaza, categoría
                            RR.HH.) con el primer y último mes de pago en
                            esa categoría y el nº de meses cobrados.
                        </p>
                        <DataTable
                            key={`laboral-${perId}`}
                            endpoint={`/api/regla23/dedicacion-pdi/${perId}/laboral`}
                            queryKey={`regla23:dedicacion-pdi:laboral:${perId}`}
                            rowKey="expediente"
                            reorderImportes={false}
                        />
                    </div>
                    <div className="rounded-md border border-slate-200 bg-white p-4">
                        <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                            Reparto por grupo de {personaSel || `per_id ${perId}`}
                            {horasSel !== null
                                ? ` · ${horasSel.toLocaleString("es-ES", { minimumFractionDigits: 1, maximumFractionDigits: 1 })} h registradas (sin factor)`
                                : ""}
                        </h2>
                        <p className="mb-3 text-xs text-slate-500">
                            Las horas de impartición docente se multiplican por
                            ×2,5 (factor de la regla 23). El porcentaje se
                            calcula sobre la <em>jornada efectiva</em> de la
                            persona: la jornada anual de 1 642 h ajustada al
                            período realmente contratado en el año (quien se
                            jubila o se incorpora a mitad de año tiene una
                            jornada proporcional). La suma de horas finales de
                            la cabecera es esa jornada efectiva y los
                            porcentajes suman el 100 %.
                        </p>
                        <DataTable
                            key={`resumen-${perId}`}
                            endpoint={`/api/regla23/dedicacion-pdi/${perId}/resumen`}
                            queryKey={`regla23:dedicacion-pdi:resumen:${perId}`}
                            rowKey="grupo"
                            reorderImportes={false}
                        />
                    </div>
                    <div className="rounded-md border border-slate-200 bg-white p-4">
                        <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                            Totales por actividad y centro
                        </h2>
                        <p className="mb-3 text-xs text-slate-500">
                            Una fila por cada par (actividad, centro de
                            coste) con las horas finales agregadas (suma
                            de todos los orígenes) y el porcentaje sobre
                            la jornada efectiva de la persona (1 642 h
                            ajustadas al período contratado). Estos
                            porcentajes son los que se usarán para
                            repartir la masa retributiva regla 23 entre
                            actividades y centros.
                        </p>
                        <DataTable
                            key={`totales-${perId}`}
                            endpoint={`/api/regla23/dedicacion-pdi/${perId}/totales`}
                            queryKey={`regla23:dedicacion-pdi:totales:${perId}`}
                            rowKey="actividad"
                            reorderImportes={false}
                        />
                    </div>
                    <div className="rounded-md border border-slate-200 bg-white p-4">
                        <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                            Unidades de coste regla 23
                        </h2>
                        <p className="mb-3 text-xs text-slate-500">
                            UC generadas por reparto de la masa regla 23
                            (retribuciones ordinarias en proyecto general
                            de PDI/PVI, excluyendo CR 19, 64, 47 y 48).
                            Cada UC se obtiene multiplicando el importe
                            total por elemento de coste por el peso
                            <em> horas_finales / Σ horas_finales</em> de
                            la persona.
                        </p>
                        <DataTable
                            key={`uc-reparto-${perId}`}
                            endpoint={`/api/regla23/dedicacion-pdi/${perId}/uc-reparto`}
                            queryKey={`regla23:dedicacion-pdi:uc-reparto:${perId}`}
                            rowKey="id"
                            reorderImportes={false}
                        />
                    </div>
                    <div className="rounded-md border border-slate-200 bg-white p-4">
                        <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                            Detalle por actividad
                        </h2>
                        <DataTable
                            key={perId}
                            endpoint={`/api/regla23/dedicacion-pdi/${perId}`}
                            queryKey={`regla23:dedicacion-pdi:persona:${perId}`}
                            rowKey="origen_id"
                            showPopoverOnRowClick
                            reorderImportes={false}
                        />
                    </div>
                </>
            )}
        </div>
    );
}
