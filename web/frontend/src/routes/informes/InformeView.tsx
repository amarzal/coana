import { useQuery } from "@tanstack/react-query";
import { useParams } from "react-router";
import { formatEuro, formatFloat } from "@/lib/format";
import { cn } from "@/lib/cn";

type Meta = { id: string; título: string; tipo: string };

type FilaJerarquica = {
    código: string;
    slug?: string;
    nombre: string;
    nivel: number;
    importe: number;
    pct_elemento: number | null;
    pct_total: number | null;
};

type CuadroJerarquico = {
    id: string;
    título: string;
    encabezado_concepto?: string;
    estado?: string;
    total: number;
    filas: FilaJerarquica[];
    _meta: Meta;
};

type DIP = { directo: number; indirecto: number; primario: number };
type FilaDIP = {
    código: string;
    slug?: string;
    nombre: string;
    nivel: number;
    directo: number;
    indirecto: number;
    primario: number;
};

type CentroDIP = {
    código_sue: string;
    slug: string;
    nombre: string;
    filas: FilaDIP[];
    total_coste_primario: DIP;
    centros_superiores: DIP & { pendiente?: boolean };
    actividades_auxiliares: DIP & { pendiente?: boolean };
    total: DIP;
};

type Cuadro105 = {
    id: string;
    título: string;
    centros: CentroDIP[];
    _meta: Meta;
};

type FilaMatriz = {
    código: string;
    slug?: string;
    nombre: string;
    nivel: number;
    valores: Record<string, number>;
};
type ColMeta = { id: string; nombre: string };
type CuadroMatriz = {
    id: string;
    título: string;
    columnas: ColMeta[];
    filas: FilaMatriz[];
    total: Record<string, number>;
    _meta: Meta;
};

function formatPct(p: number | null): string {
    if (p === null || p === undefined || !Number.isFinite(p)) return "";
    return `${formatFloat(p)} %`;
}

export function InformeView() {
    const { cuadroId } = useParams<{ cuadroId: string }>();
    const q = useQuery({
        queryKey: ["informe", cuadroId],
        queryFn: async () => {
            const r = await fetch(`/api/informes/${cuadroId}`);
            if (!r.ok) {
                const txt = await r.text();
                throw new Error(`HTTP ${r.status}: ${txt}`);
            }
            return (await r.json()) as
                | CuadroJerarquico
                | Cuadro105
                | CuadroMatriz;
        },
        enabled: !!cuadroId,
    });

    if (q.isLoading) {
        return <div className="text-sm text-slate-500">Cargando…</div>;
    }
    if (q.isError) {
        return (
            <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm text-red-700">
                {String(q.error instanceof Error ? q.error.message : q.error)}
                <div className="mt-2 text-xs">
                    Si el cuadro aún no se ha generado, pulsa
                    <em> Generar informes</em> en la barra lateral.
                </div>
            </div>
        );
    }
    const datos = q.data!;
    const tipo = datos._meta.tipo;

    return (
        <div className="space-y-4">
            <div>
                <h1 className="text-xl font-semibold">{datos.título}</h1>
                {"estado" in datos && datos.estado && (
                    <div className="mt-1 inline-block rounded bg-amber-100 px-2 py-0.5 text-xs text-amber-800">
                        {datos.estado}
                    </div>
                )}
            </div>
            {tipo === "jerárquico" && (
                <TablaJerarquica datos={datos as CuadroJerarquico} />
            )}
            {tipo === "centros_d_i_p" && (
                <TablaCentrosDIP datos={datos as Cuadro105} />
            )}
            {tipo === "matriz" && (
                <TablaMatriz datos={datos as CuadroMatriz} />
            )}
        </div>
    );
}

function TablaJerarquica({ datos }: { datos: CuadroJerarquico }) {
    const enc = datos.encabezado_concepto ?? "Concepto";
    return (
        <table className="w-full border-collapse text-sm">
            <thead>
                <tr className="border-b-2 border-slate-700 text-slate-700">
                    <th className="px-2 py-1 text-left font-semibold">{enc}</th>
                    <th className="px-2 py-1 text-right font-semibold">Importe&nbsp;(€)</th>
                    <th className="px-2 py-1 text-right font-semibold">% elemento</th>
                    <th className="px-2 py-1 text-right font-semibold">% total</th>
                </tr>
            </thead>
            <tbody>
                {datos.filas.map((fila, i) => {
                    const esGrupo = fila.nivel === 1;
                    const prev = i > 0 ? datos.filas[i - 1] : null;
                    const separa = esGrupo && prev && prev.nivel !== 1;
                    return (
                        <tr
                            key={`${fila.código}-${i}`}
                            className={cn(
                                "border-b border-slate-100",
                                separa && "border-t-2 border-t-slate-700",
                                esGrupo
                                    ? "bg-slate-50 font-semibold"
                                    : i % 2 === 1
                                      ? "bg-slate-50/60"
                                      : "",
                            )}
                        >
                            <td
                                className="px-2 py-1"
                                style={{ paddingLeft: `${0.5 + (fila.nivel - 1) * 1.2}rem` }}
                            >
                                <span className="mr-2 text-slate-500 tabular-nums">{fila.código}</span>
                                {fila.nombre}
                            </td>
                            <td className="px-2 py-1 text-right tabular-nums">{formatEuro(fila.importe)}</td>
                            <td className="px-2 py-1 text-right tabular-nums">{formatPct(fila.pct_elemento)}</td>
                            <td className="px-2 py-1 text-right tabular-nums">{formatPct(fila.pct_total)}</td>
                        </tr>
                    );
                })}
                <tr className="border-t-2 border-slate-700 font-semibold">
                    <td className="px-2 py-1">Total</td>
                    <td className="px-2 py-1 text-right tabular-nums">{formatEuro(datos.total)}</td>
                    <td className="px-2 py-1 text-right"></td>
                    <td className="px-2 py-1 text-right tabular-nums">100,00&nbsp;%</td>
                </tr>
            </tbody>
        </table>
    );
}

function TablaCentrosDIP({ datos }: { datos: Cuadro105 }) {
    return (
        <div className="space-y-8">
            {datos.centros.map((centro) => (
                <section key={centro.código_sue}>
                    <h2 className="mb-2 text-base font-semibold">
                        {centro.código_sue} — {centro.nombre}
                    </h2>
                    <table className="w-full border-collapse text-sm">
                        <thead>
                            <tr className="border-b-2 border-slate-700 text-slate-700">
                                <th className="px-2 py-1 text-left font-semibold">Elemento de coste</th>
                                <th className="px-2 py-1 text-right font-semibold">Directo&nbsp;(€)</th>
                                <th className="px-2 py-1 text-right font-semibold">Indirecto&nbsp;(€)</th>
                                <th className="px-2 py-1 text-right font-semibold">Primario&nbsp;(D+I)&nbsp;(€)</th>
                            </tr>
                        </thead>
                        <tbody>
                            {centro.filas.map((fila, i) => {
                                const esGrupo = fila.nivel === 1;
                                const prev = i > 0 ? centro.filas[i - 1] : null;
                                const separa = esGrupo && prev && prev.nivel !== 1;
                                return (
                                    <tr
                                        key={`${fila.código}-${i}`}
                                        className={cn(
                                            "border-b border-slate-100",
                                            separa && "border-t-2 border-t-slate-700",
                                            esGrupo
                                                ? "bg-slate-50 font-semibold"
                                                : i % 2 === 1
                                                  ? "bg-slate-50/60"
                                                  : "",
                                        )}
                                    >
                                        <td
                                            className="px-2 py-1"
                                            style={{ paddingLeft: `${0.5 + (fila.nivel - 1) * 1.2}rem` }}
                                        >
                                            <span className="mr-2 text-slate-500 tabular-nums">{fila.código}</span>
                                            {fila.nombre}
                                        </td>
                                        <td className="px-2 py-1 text-right tabular-nums">{formatEuro(fila.directo)}</td>
                                        <td className="px-2 py-1 text-right tabular-nums">{formatEuro(fila.indirecto)}</td>
                                        <td className="px-2 py-1 text-right tabular-nums">{formatEuro(fila.primario)}</td>
                                    </tr>
                                );
                            })}
                            <FilaResumen
                                etiqueta="Total coste primario"
                                v={centro.total_coste_primario}
                            />
                            <FilaResumen
                                etiqueta="Centros superiores"
                                v={centro.centros_superiores}
                                pendiente="Fase 3.a pendiente"
                            />
                            <FilaResumen
                                etiqueta="Actividades auxiliares"
                                v={centro.actividades_auxiliares}
                                pendiente="Fase 3.d pendiente"
                            />
                            <tr className="border-t-2 border-slate-700 font-semibold">
                                <td className="px-2 py-1">Total</td>
                                <td className="px-2 py-1 text-right tabular-nums">{formatEuro(centro.total.directo)}</td>
                                <td className="px-2 py-1 text-right tabular-nums">{formatEuro(centro.total.indirecto)}</td>
                                <td className="px-2 py-1 text-right tabular-nums">{formatEuro(centro.total.primario)}</td>
                            </tr>
                        </tbody>
                    </table>
                </section>
            ))}
        </div>
    );
}

function FilaResumen({
    etiqueta,
    v,
    pendiente,
}: {
    etiqueta: string;
    v: DIP;
    pendiente?: string;
}) {
    const muted = !!pendiente;
    return (
        <tr
            className={cn(
                "border-t border-slate-300 font-semibold",
                muted && "text-slate-500",
            )}
        >
            <td className="px-2 py-1">
                {etiqueta}
                {pendiente && (
                    <span className="ml-2 text-xs font-normal italic text-slate-500">
                        ({pendiente})
                    </span>
                )}
            </td>
            <td className="px-2 py-1 text-right tabular-nums">{formatEuro(v.directo)}</td>
            <td className="px-2 py-1 text-right tabular-nums">{formatEuro(v.indirecto)}</td>
            <td className="px-2 py-1 text-right tabular-nums">{formatEuro(v.primario)}</td>
        </tr>
    );
}

function TablaMatriz({ datos }: { datos: CuadroMatriz }) {
    const cols = datos.columnas;
    return (
        <table className="w-full border-collapse text-sm">
            <thead>
                <tr className="border-b-2 border-slate-700 text-slate-700">
                    <th className="px-2 py-1 text-left font-semibold">Actividad</th>
                    {cols.map((c) => (
                        <th key={c.id} className="px-2 py-1 text-right font-semibold">
                            {c.nombre}&nbsp;(€)
                        </th>
                    ))}
                </tr>
            </thead>
            <tbody>
                {datos.filas.map((fila, i) => {
                    const esGrupo = fila.nivel === 1;
                    const prev = i > 0 ? datos.filas[i - 1] : null;
                    const separa = esGrupo && prev && prev.nivel !== 1;
                    return (
                        <tr
                            key={`${fila.código}-${i}`}
                            className={cn(
                                "border-b border-slate-100",
                                separa && "border-t-2 border-t-slate-700",
                                esGrupo
                                    ? "bg-slate-50 font-semibold"
                                    : i % 2 === 1
                                      ? "bg-slate-50/60"
                                      : "",
                            )}
                        >
                            <td
                                className="px-2 py-1"
                                style={{ paddingLeft: `${0.5 + (fila.nivel - 1) * 1.2}rem` }}
                            >
                                <span className="mr-2 text-slate-500 tabular-nums">{fila.código}</span>
                                {fila.nombre}
                            </td>
                            {cols.map((c) => (
                                <td key={c.id} className="px-2 py-1 text-right tabular-nums">
                                    {formatEuro(fila.valores[c.id] ?? 0)}
                                </td>
                            ))}
                        </tr>
                    );
                })}
                <tr className="border-t-2 border-slate-700 font-semibold">
                    <td className="px-2 py-1">Total</td>
                    {cols.map((c) => (
                        <td key={c.id} className="px-2 py-1 text-right tabular-nums">
                            {formatEuro(datos.total[c.id] ?? 0)}
                        </td>
                    ))}
                </tr>
            </tbody>
        </table>
    );
}
