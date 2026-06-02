import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { formatEuro, formatInt } from "@/lib/format";
import { cn } from "@/lib/cn";

type Opcion = { slug: string; código: string; descripción: string };
type Opciones = {
    centros_de_coste: Opcion[];
    actividades: Opcion[];
    elementos_de_coste: Opcion[];
};

type Eje = "cc" | "act" | "ec";

type Filtro = {
    centros_de_coste: string[];
    actividades: string[];
    elementos_de_coste: string[];
    orden: Eje[];
    // Por eje: true = agregado (un solo importe), false = detalle (valor a valor).
    agregado: Record<Eje, boolean>;
};

const AGREGADO_DEFECTO: Record<Eje, boolean> = { cc: true, act: true, ec: true };

type Nodo = {
    nivel: number;
    eje: Eje;
    slug: string;
    código: string;
    descripción: string;
    n_ucs: number;
    importe: number;
    hijos: Nodo[];
};

type Resultado = {
    orden: Eje[];
    n_ucs: number;
    importe: number;
    raíces: Nodo[];
};

const EJE_LABEL: Record<Eje, string> = {
    cc: "Centro de coste",
    act: "Actividad",
    ec: "Elemento de coste",
};

function ToggleAgregado({
    agregado,
    onChange,
}: {
    agregado: boolean;
    onChange: (v: boolean) => void;
}) {
    return (
        <div
            className="inline-flex overflow-hidden rounded border border-slate-300 text-[10px]"
            role="group"
            aria-label="Nivel de agregación"
        >
            <button
                type="button"
                onClick={() => onChange(true)}
                className={cn(
                    "px-2 py-0.5",
                    agregado ? "bg-slate-800 text-white" : "bg-white text-slate-600 hover:bg-slate-100",
                )}
                title="Un único importe agregado para este campo"
            >
                Agregado
            </button>
            <button
                type="button"
                onClick={() => onChange(false)}
                className={cn(
                    "px-2 py-0.5",
                    !agregado ? "bg-slate-800 text-white" : "bg-white text-slate-600 hover:bg-slate-100",
                )}
                title="Desglose fino, valor a valor"
            >
                Detalle
            </button>
        </div>
    );
}

function MultiSelect({
    label,
    opciones,
    seleccionados,
    onChange,
    agregado,
    onAgregadoChange,
}: {
    label: string;
    opciones: Opcion[];
    seleccionados: string[];
    onChange: (next: string[]) => void;
    agregado: boolean;
    onAgregadoChange: (v: boolean) => void;
}) {
    const [busqueda, setBusqueda] = useState("");
    const [abierto, setAbierto] = useState(false);
    const filtradas = useMemo(() => {
        if (!busqueda.trim()) return opciones.slice(0, 200);
        const b = busqueda.toLowerCase();
        return opciones
            .filter(
                (o) =>
                    o.slug.toLowerCase().includes(b) ||
                    o.código.toLowerCase().includes(b) ||
                    o.descripción.toLowerCase().includes(b),
            )
            .slice(0, 200);
    }, [opciones, busqueda]);
    const setSel = new Set(seleccionados);
    const opcDict = useMemo(() => {
        const m = new Map<string, Opcion>();
        opciones.forEach((o) => m.set(o.slug, o));
        return m;
    }, [opciones]);
    return (
        <div className="rounded border border-slate-200 bg-white p-2">
            <div className="mb-1 flex items-center justify-between gap-2">
                <span className="text-xs font-semibold text-slate-600">{label}</span>
                <ToggleAgregado agregado={agregado} onChange={onAgregadoChange} />
            </div>
            {seleccionados.length > 0 && (
                <div className="mb-1 flex flex-wrap gap-1">
                    {seleccionados.map((s) => {
                        const o = opcDict.get(s);
                        return (
                            <span
                                key={s}
                                className="inline-flex items-center gap-1 rounded bg-slate-800 px-2 py-0.5 text-xs text-white"
                            >
                                <span className="text-slate-300">{o?.código || ""}</span>
                                <span>{o?.descripción || s}</span>
                                <button
                                    type="button"
                                    onClick={() => onChange(seleccionados.filter((x) => x !== s))}
                                    className="ml-1 hover:text-rose-300"
                                    aria-label="Quitar"
                                >
                                    ×
                                </button>
                            </span>
                        );
                    })}
                </div>
            )}
            <input
                type="text"
                value={busqueda}
                onChange={(e) => {
                    setBusqueda(e.target.value);
                    setAbierto(true);
                }}
                onFocus={() => setAbierto(true)}
                onBlur={() => setTimeout(() => setAbierto(false), 200)}
                placeholder="Buscar (código o descripción)…"
                className="w-full rounded border border-slate-300 px-2 py-1 text-sm"
            />
            {abierto && filtradas.length > 0 && (
                <div className="mt-1 max-h-64 overflow-y-auto rounded border border-slate-200 bg-white">
                    {filtradas.map((o) => {
                        const marc = setSel.has(o.slug);
                        return (
                            <button
                                key={o.slug}
                                type="button"
                                onMouseDown={(e) => e.preventDefault()}
                                onClick={() => {
                                    if (marc) onChange(seleccionados.filter((x) => x !== o.slug));
                                    else onChange([...seleccionados, o.slug]);
                                }}
                                className={cn(
                                    "flex w-full items-center gap-2 border-b border-slate-100 px-2 py-1 text-left text-xs hover:bg-slate-100",
                                    marc && "bg-blue-50",
                                )}
                            >
                                <input type="checkbox" checked={marc} readOnly className="pointer-events-none" />
                                <span className="text-slate-500 tabular-nums">{o.código}</span>
                                <span className="flex-1">{o.descripción}</span>
                                <span className="text-[10px] text-slate-400">{o.slug}</span>
                            </button>
                        );
                    })}
                </div>
            )}
        </div>
    );
}

function NodoRow({
    nodo,
    depth = 0,
    contexto = {},
}: {
    nodo: Nodo;
    depth?: number;
    contexto?: Partial<Record<Eje, string>>;
}) {
    const [abierto, setAbierto] = useState(depth < 1);
    const [verUcs, setVerUcs] = useState(false);
    const ctxAqui = { ...contexto, [nodo.eje]: nodo.slug };
    return (
        <>
            <tr className={cn(depth === 0 && "border-t-2 border-slate-700 font-semibold")}>
                <td className="px-2 py-1" style={{ paddingLeft: `${0.5 + depth * 1.2}rem` }}>
                    {nodo.hijos.length > 0 && (
                        <button
                            type="button"
                            onClick={() => setAbierto(!abierto)}
                            className="mr-1 inline-block w-4 text-slate-500"
                        >
                            {abierto ? "▾" : "▸"}
                        </button>
                    )}
                    <span className="mr-2 text-[10px] uppercase text-slate-400">{nodo.eje}</span>
                    <span className="mr-2 text-slate-500 tabular-nums">{nodo.código}</span>
                    <span>{nodo.descripción}</span>
                    <button
                        type="button"
                        onClick={() => setVerUcs(true)}
                        className="ml-2 rounded border border-slate-300 px-1.5 py-0.5 text-[10px] text-slate-600 hover:bg-slate-50"
                    >
                        UCs
                    </button>
                </td>
                <td className="px-2 py-1 text-right tabular-nums">{formatInt(nodo.n_ucs)}</td>
                <td className="px-2 py-1 text-right tabular-nums">{formatEuro(nodo.importe)}</td>
            </tr>
            {abierto && nodo.hijos.map((h, i) => (
                <NodoRow key={`${h.eje}-${h.slug}-${i}`} nodo={h} depth={depth + 1} contexto={ctxAqui} />
            ))}
            {verUcs && <UcsModal nodo={nodo} contexto={ctxAqui} onClose={() => setVerUcs(false)} />}
        </>
    );
}

function UcsModal({
    nodo,
    contexto,
    onClose,
}: {
    nodo: Nodo;
    contexto: Partial<Record<Eje, string>>;
    onClose: () => void;
}) {
    // Construir filtro de UC: ancestros acumulados (CC, ACT y EC) hasta
    // este nodo inclusive. Si un eje no aparece en el contexto, no se
    // filtra por él.
    const cuerpo = useMemo(() => {
        return {
            centros_de_coste: contexto.cc ? [contexto.cc] : [],
            actividades: contexto.act ? [contexto.act] : [],
            elementos_de_coste: contexto.ec ? [contexto.ec] : [],
            limit: 2000,
        };
    }, [contexto]);
    const q = useQuery({
        queryKey: ["informes-carta-uc", cuerpo],
        queryFn: async () => {
            const r = await fetch("/api/informes-carta/uc", {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify(cuerpo),
            });
            return (await r.json()) as { n_total: number; n_devueltas: number; filas: Record<string, unknown>[] };
        },
    });
    return (
        <tr>
            <td colSpan={3}>
                <div
                    className="fixed inset-0 z-40 flex items-center justify-center bg-black/40 p-6"
                    onClick={onClose}
                >
                    <div
                        className="relative max-h-[90vh] w-[95vw] overflow-auto rounded-lg bg-white p-4 shadow-xl"
                        onClick={(e) => e.stopPropagation()}
                    >
                        <div className="mb-2 flex items-center justify-between">
                            <h2 className="text-base font-semibold">
                                {nodo.código} — {nodo.descripción}
                                <span className="ml-2 text-xs text-slate-500">
                                    ({nodo.eje}, {formatInt(nodo.n_ucs)} UC, {formatEuro(nodo.importe)})
                                </span>
                            </h2>
                            <button
                                type="button"
                                onClick={onClose}
                                className="rounded border border-slate-300 px-2 py-0.5 text-sm hover:bg-slate-100"
                            >
                                Cerrar
                            </button>
                        </div>
                        {q.isLoading && <div className="text-sm text-slate-500">Cargando…</div>}
                        {q.data && (
                            <>
                                <div className="mb-2 text-xs text-slate-500">
                                    Mostrando {formatInt(q.data.n_devueltas)} de {formatInt(q.data.n_total)} UC
                                </div>
                                <table className="w-full border-collapse text-xs">
                                    <thead>
                                        <tr className="border-b-2 border-slate-700 text-slate-700">
                                            <th className="px-2 py-1 text-left font-semibold">id</th>
                                            <th className="px-2 py-1 text-left font-semibold">EC</th>
                                            <th className="px-2 py-1 text-left font-semibold">CC</th>
                                            <th className="px-2 py-1 text-left font-semibold">Actividad</th>
                                            <th className="px-2 py-1 text-left font-semibold">Origen</th>
                                            <th className="px-2 py-1 text-left font-semibold">Actividad dag</th>
                                            <th className="px-2 py-1 text-right font-semibold">Importe</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {q.data.filas.map((f, i) => (
                                            <tr key={`${f.id}-${i}`} className={cn(i % 2 === 1 && "bg-slate-50/60")}>
                                                <td className="px-2 py-0.5 font-mono">{String(f.id ?? "")}</td>
                                                <td className="px-2 py-0.5">{String(f.elemento_de_coste ?? "")}</td>
                                                <td className="px-2 py-0.5">{String(f.centro_de_coste ?? "")}</td>
                                                <td className="px-2 py-0.5">{String(f.actividad ?? "")}</td>
                                                <td className="px-2 py-0.5">{String(f.origen ?? "")}</td>
                                                <td className="px-2 py-0.5">
                                                    {f.marca_dag
                                                        ? String(f.marca_dag)
                                                        : <span className="text-slate-300">—</span>}
                                                </td>
                                                <td className="px-2 py-0.5 text-right tabular-nums">
                                                    {formatEuro(Number(f.importe ?? 0))}
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </>
                        )}
                    </div>
                </div>
            </td>
        </tr>
    );
}

function OrdenDragDrop({
    orden,
    onChange,
}: {
    orden: Eje[];
    onChange: (next: Eje[]) => void;
}) {
    const [arrastrado, setArrastrado] = useState<number | null>(null);
    const [destino, setDestino] = useState<number | null>(null);

    function onDragStart(idx: number) {
        return (e: React.DragEvent) => {
            setArrastrado(idx);
            e.dataTransfer.effectAllowed = "move";
        };
    }
    function onDragOver(idx: number) {
        return (e: React.DragEvent) => {
            e.preventDefault();
            e.dataTransfer.dropEffect = "move";
            setDestino(idx);
        };
    }
    function onDrop(idx: number) {
        return (e: React.DragEvent) => {
            e.preventDefault();
            if (arrastrado === null || arrastrado === idx) {
                setArrastrado(null);
                setDestino(null);
                return;
            }
            const nuevo = [...orden];
            const [m] = nuevo.splice(arrastrado, 1);
            nuevo.splice(idx, 0, m);
            onChange(nuevo);
            setArrastrado(null);
            setDestino(null);
        };
    }
    function onDragEnd() {
        setArrastrado(null);
        setDestino(null);
    }

    return (
        <div className="flex items-center gap-2">
            {orden.map((eje, idx) => {
                const dragging = arrastrado === idx;
                const isOver = destino === idx && arrastrado !== null && arrastrado !== idx;
                return (
                    <div
                        key={eje}
                        draggable
                        onDragStart={onDragStart(idx)}
                        onDragOver={onDragOver(idx)}
                        onDrop={onDrop(idx)}
                        onDragEnd={onDragEnd}
                        className={cn(
                            "flex items-center gap-2 rounded border bg-white px-2 py-1.5 shadow-sm select-none cursor-grab active:cursor-grabbing",
                            "border-slate-300",
                            dragging && "opacity-40",
                            isOver && "ring-2 ring-blue-500",
                        )}
                        title="Arrastra para reordenar"
                    >
                        {/* Handle "rugoso": dos columnas de tres puntos */}
                        <span
                            aria-hidden="true"
                            className="grid grid-cols-2 gap-x-0.5 gap-y-0.5 text-slate-500"
                        >
                            {Array.from({ length: 6 }).map((_, i) => (
                                <span
                                    key={i}
                                    className="inline-block h-1 w-1 rounded-full bg-slate-400"
                                />
                            ))}
                        </span>
                        <span className="text-[10px] font-mono text-slate-400">{idx + 1}.</span>
                        <span className="font-medium">{EJE_LABEL[eje]}</span>
                    </div>
                );
            })}
        </div>
    );
}


export function InformesACarta() {
    const [filtro, setFiltro] = useState<Filtro>({
        centros_de_coste: [],
        actividades: [],
        elementos_de_coste: [],
        orden: ["cc", "act", "ec"],
        agregado: { ...AGREGADO_DEFECTO },
    });
    const [consultaActiva, setConsultaActiva] = useState<Filtro | null>(null);
    const [nombreConfig, setNombreConfig] = useState("");

    const opcionesQ = useQuery({
        queryKey: ["informes-carta-opciones"],
        queryFn: async () => {
            const r = await fetch("/api/informes-carta/opciones");
            return (await r.json()) as Opciones;
        },
    });
    const configsQ = useQuery({
        queryKey: ["informes-carta-configs"],
        queryFn: async () => {
            const r = await fetch("/api/informes-carta/configs");
            return (await r.json()) as { nombre: string }[];
        },
    });
    const resQ = useQuery({
        queryKey: ["informes-carta-consulta", consultaActiva],
        queryFn: async () => {
            const r = await fetch("/api/informes-carta/consulta", {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify(consultaActiva),
            });
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return (await r.json()) as Resultado;
        },
        enabled: !!consultaActiva,
    });

    const opc = opcionesQ.data;

    async function guardar() {
        const nombre = nombreConfig.trim();
        if (!nombre) return;
        await fetch(`/api/informes-carta/configs/${encodeURIComponent(nombre)}`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(filtro),
        });
        configsQ.refetch();
    }
    async function cargar(nombre: string) {
        if (!nombre) return;
        const r = await fetch(`/api/informes-carta/configs/${encodeURIComponent(nombre)}`);
        if (!r.ok) return;
        const f = (await r.json()) as Filtro;
        // Configs guardadas antes del toggle no traen `agregado`.
        setFiltro({ ...f, agregado: { ...AGREGADO_DEFECTO, ...(f.agregado ?? {}) } });
        setNombreConfig(nombre);
    }
    async function borrar(nombre: string) {
        if (!nombre) return;
        if (!window.confirm(`¿Borrar configuración "${nombre}"?`)) return;
        await fetch(`/api/informes-carta/configs/${encodeURIComponent(nombre)}`, {
            method: "DELETE",
        });
        if (nombreConfig === nombre) setNombreConfig("");
        configsQ.refetch();
    }

    function reordenar(orden: Eje[]) {
        setFiltro({ ...filtro, orden });
    }

    async function descargar(formato: "excel" | "pdf") {
        if (!consultaActiva) return;
        const r = await fetch(`/api/informes-carta/${formato}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(consultaActiva),
        });
        if (!r.ok) {
            alert(`Error: ${r.status} ${await r.text()}`);
            return;
        }
        const blob = await r.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download =
            formato === "excel" ? "informe_a_la_carta.xlsx" : "informe_a_la_carta.pdf";
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
    }

    return (
        <div className="space-y-4">
            <h1 className="text-xl font-semibold">Informes a la carta</h1>

            {/* Configuraciones guardadas */}
            <div className="rounded border border-slate-200 bg-slate-50 p-3">
                <div className="mb-2 text-sm font-semibold">Configuración</div>
                <div className="flex flex-wrap items-center gap-2">
                    <input
                        type="text"
                        value={nombreConfig}
                        onChange={(e) => setNombreConfig(e.target.value)}
                        placeholder="Nombre de la configuración"
                        className="rounded border border-slate-300 px-2 py-1 text-sm"
                    />
                    <button
                        type="button"
                        onClick={guardar}
                        disabled={!nombreConfig.trim()}
                        className="rounded bg-emerald-700 px-3 py-1 text-sm text-white hover:bg-emerald-600 disabled:bg-slate-300"
                    >
                        Guardar
                    </button>
                    <select
                        onChange={(e) => cargar(e.target.value)}
                        value=""
                        className="rounded border border-slate-300 px-2 py-1 text-sm"
                    >
                        <option value="">Cargar…</option>
                        {(configsQ.data ?? []).map((c) => (
                            <option key={c.nombre} value={c.nombre}>
                                {c.nombre}
                            </option>
                        ))}
                    </select>
                    <button
                        type="button"
                        onClick={() => borrar(nombreConfig)}
                        disabled={!nombreConfig.trim()}
                        className="rounded border border-rose-300 bg-white px-3 py-1 text-sm text-rose-700 hover:bg-rose-50 disabled:opacity-40"
                    >
                        Borrar
                    </button>
                </div>
            </div>

            {/* Selectores */}
            {opc && (
                <div className="grid grid-cols-1 gap-3 lg:grid-cols-3">
                    <MultiSelect
                        label="Centros de coste (vacío = todos)"
                        opciones={opc.centros_de_coste}
                        seleccionados={filtro.centros_de_coste}
                        onChange={(v) => setFiltro({ ...filtro, centros_de_coste: v })}
                        agregado={filtro.agregado.cc}
                        onAgregadoChange={(v) =>
                            setFiltro({ ...filtro, agregado: { ...filtro.agregado, cc: v } })
                        }
                    />
                    <MultiSelect
                        label="Actividades (vacío = todas)"
                        opciones={opc.actividades}
                        seleccionados={filtro.actividades}
                        onChange={(v) => setFiltro({ ...filtro, actividades: v })}
                        agregado={filtro.agregado.act}
                        onAgregadoChange={(v) =>
                            setFiltro({ ...filtro, agregado: { ...filtro.agregado, act: v } })
                        }
                    />
                    <MultiSelect
                        label="Elementos de coste (vacío = todos)"
                        opciones={opc.elementos_de_coste}
                        seleccionados={filtro.elementos_de_coste}
                        onChange={(v) => setFiltro({ ...filtro, elementos_de_coste: v })}
                        agregado={filtro.agregado.ec}
                        onAgregadoChange={(v) =>
                            setFiltro({ ...filtro, agregado: { ...filtro.agregado, ec: v } })
                        }
                    />
                </div>
            )}

            {/* Orden jerárquico (drag & drop) */}
            <div className="flex flex-wrap items-center gap-3 text-sm">
                <span className="text-slate-700">Orden jerárquico:</span>
                <OrdenDragDrop orden={filtro.orden} onChange={reordenar} />
                <div className="ml-auto flex flex-wrap items-center gap-2">
                    <button
                        type="button"
                        onClick={() => setConsultaActiva({ ...filtro })}
                        className="rounded bg-slate-800 px-4 py-1.5 text-white hover:bg-slate-700"
                    >
                        Generar
                    </button>
                    <button
                        type="button"
                        onClick={() => descargar("excel")}
                        disabled={!resQ.data}
                        className="rounded border border-emerald-700 bg-white px-3 py-1.5 text-emerald-700 hover:bg-emerald-50 disabled:opacity-40"
                    >
                        Descargar Excel
                    </button>
                    <button
                        type="button"
                        onClick={() => descargar("pdf")}
                        disabled={!resQ.data}
                        className="rounded border border-rose-700 bg-white px-3 py-1.5 text-rose-700 hover:bg-rose-50 disabled:opacity-40"
                    >
                        Descargar PDF
                    </button>
                </div>
            </div>

            {/* Resultados */}
            {resQ.isLoading && <div className="text-sm text-slate-500">Consultando…</div>}
            {resQ.error && <div className="text-sm text-rose-700">{String(resQ.error)}</div>}
            {resQ.data && (
                <div className="rounded border border-slate-200 bg-white">
                    <div className="border-b border-slate-200 bg-slate-50 px-3 py-1.5 text-sm">
                        <strong>{formatInt(resQ.data.n_ucs)}</strong> unidades de coste ·{" "}
                        <strong>{formatEuro(resQ.data.importe)}</strong>
                    </div>
                    <table className="w-full border-collapse text-sm">
                        <thead>
                            <tr className="border-b-2 border-slate-700 text-slate-700">
                                <th className="px-2 py-1 text-left font-semibold">Concepto</th>
                                <th className="px-2 py-1 text-right font-semibold">UCs</th>
                                <th className="px-2 py-1 text-right font-semibold">Importe</th>
                            </tr>
                        </thead>
                        <tbody>
                            {resQ.data.raíces.map((r, i) => (
                                <NodoRow key={`${r.eje}-${r.slug}-${i}`} nodo={r} />
                            ))}
                            <tr className="border-t-2 border-slate-700 font-semibold">
                                <td className="px-2 py-1">Total</td>
                                <td className="px-2 py-1 text-right tabular-nums">{formatInt(resQ.data.n_ucs)}</td>
                                <td className="px-2 py-1 text-right tabular-nums">{formatEuro(resQ.data.importe)}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    );
}
