import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { formatFloat, formatInt } from "@/lib/format";
import { cn } from "@/lib/cn";
import { TreeSelect } from "@/components/TreeSelect";

type Eje = "cc" | "act" | "ec";

type Filtro = {
    // Eje que vertebra el informe (árbol monográfico). Los otros dos ejes son
    // solo filtros; la selección del eje de estructura es solo foco.
    estructura: Eje;
    centros_de_coste: string[];
    actividades: string[];
    elementos_de_coste: string[];
};

type Nodo = {
    nivel: number;
    eje: Eje;
    slug: string;
    código: string;
    descripción: string;
    n_ucs: number;
    importe: number;            // subárbol: directo (a) + descendientes (b)
    n_ucs_directo: number;
    importe_directo: number;    // a
    importe_ancestros: number;  // c: roll-down de ancestros
    n_ucs_ancestros: number;
    hijos: Nodo[];
};

type Modo = "directo" | "descendientes" | "ancestros" | "total";

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

/** True si el subárbol de `nodo` (incluido él) contiene algún slug de foco. */
function subtreeTieneFoco(nodo: Nodo, foco: Set<string>): boolean {
    if (foco.has(nodo.slug)) return true;
    return nodo.hijos.some((h) => subtreeTieneFoco(h, foco));
}

function NodoRow({
    nodo,
    depth = 0,
    filtroBase,
}: {
    nodo: Nodo;
    depth?: number;
    filtroBase: Filtro;
}) {
    // Foco = selección del eje de estructura. Si lo hay, abrimos de entrada cada
    // rama que conduce a un nodo de foco (y el propio foco); si no, solo el nivel 0.
    const focoSlugs =
        filtroBase.estructura === "cc" ? filtroBase.centros_de_coste
            : filtroBase.estructura === "act" ? filtroBase.actividades
                : filtroBase.elementos_de_coste;
    const [abierto, setAbierto] = useState(
        focoSlugs.length > 0
            ? subtreeTieneFoco(nodo, new Set(focoSlugs))
            : depth < 1,
    );
    const [modal, setModal] = useState<null | Modo>(null);
    const descendientes = Math.round((nodo.importe - nodo.importe_directo) * 100) / 100;
    const total = Math.round((nodo.importe + nodo.importe_ancestros) * 100) / 100;
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
                </td>
                {/* a) Directo (clicable si hay UC directas) */}
                <td className="px-2 py-1 text-right tabular-nums">
                    {nodo.n_ucs_directo > 0 ? (
                        <button
                            type="button"
                            onClick={() => setModal("directo")}
                            className="text-slate-700 underline decoration-dotted underline-offset-2 hover:text-blue-700"
                            title={`${formatInt(nodo.n_ucs_directo)} UC directas`}
                        >
                            {formatFloat(nodo.importe_directo)}
                        </button>
                    ) : (
                        <span className="text-slate-300">—</span>
                    )}
                </td>
                {/* b) Descendientes, clicable */}
                <td className="px-2 py-1 text-right tabular-nums">
                    {descendientes !== 0 ? (
                        <button
                            type="button"
                            onClick={() => setModal("descendientes")}
                            className="text-slate-500 underline decoration-dotted underline-offset-2 hover:text-blue-700"
                            title={`${formatInt(nodo.n_ucs - nodo.n_ucs_directo)} UC aportadas por descendientes`}
                        >
                            {formatFloat(descendientes)}
                        </button>
                    ) : (
                        <span className="text-slate-300">—</span>
                    )}
                </td>
                {/* c) Ancestros (roll-down), clicable */}
                <td className="px-2 py-1 text-right tabular-nums">
                    {nodo.importe_ancestros !== 0 ? (
                        <button
                            type="button"
                            onClick={() => setModal("ancestros")}
                            className="text-slate-500 underline decoration-dotted underline-offset-2 hover:text-blue-700"
                            title={`Fracción de ${formatInt(nodo.n_ucs_ancestros)} UC de ancestros`}
                        >
                            {formatFloat(nodo.importe_ancestros)}
                        </button>
                    ) : (
                        <span className="text-slate-300">—</span>
                    )}
                </td>
                {/* Total = a + b + c (clicable: muestra el subárbol; los ancestros se ven en su columna) */}
                <td className="px-2 py-1 text-right tabular-nums">
                    <button
                        type="button"
                        onClick={() => setModal("total")}
                        className="font-medium underline decoration-dotted underline-offset-2 hover:text-blue-700"
                        title={`${formatInt(nodo.n_ucs)} UC en el subárbol + fracción de ancestros`}
                    >
                        {formatFloat(total)}
                    </button>
                </td>
            </tr>
            {abierto && nodo.hijos.map((h, i) => (
                <NodoRow key={`${h.eje}-${h.slug}-${i}`} nodo={h} depth={depth + 1} filtroBase={filtroBase} />
            ))}
            {modal && (
                <UcsModal
                    nodo={nodo}
                    modo={modal}
                    filtroBase={filtroBase}
                    onClose={() => setModal(null)}
                />
            )}
        </>
    );
}

function UcsModal({
    nodo,
    modo,
    filtroBase,
    onClose,
}: {
    nodo: Nodo;
    modo: Modo;
    filtroBase: Filtro;
    onClose: () => void;
}) {
    const esAncestros = modo === "ancestros";
    const ejeArray: Record<Eje, "centros_de_coste" | "actividades" | "elementos_de_coste"> = {
        cc: "centros_de_coste",
        act: "actividades",
        ec: "elementos_de_coste",
    };
    // Informe monográfico: los OTROS ejes se filtran por la selección del informe
    // (filtroBase); el eje de la estructura se filtra por el nodo pinchado, con
    // modo exacto (directo) / subárbol sin slug (descendientes) / subárbol (total).
    // En ancestros, el eje de estructura aporta el nodo y se ignora su ámbito.
    const cuerpo = useMemo(() => {
        const base = {
            centros_de_coste: [...filtroBase.centros_de_coste],
            actividades: [...filtroBase.actividades],
            elementos_de_coste: [...filtroBase.elementos_de_coste],
        };
        base[ejeArray[nodo.eje]] = [nodo.slug];
        if (esAncestros) {
            return { ...base, eje: nodo.eje, slug: nodo.slug, scope_slugs: [], limit: 2000 };
        }
        return {
            ...base,
            exacto_eje: modo === "directo" ? nodo.eje : null,
            indirecto_eje: modo === "descendientes" ? nodo.eje : null,
            limit: 2000,
        };
    }, [filtroBase, modo, nodo.eje, nodo.slug, esAncestros]);

    // Ámbito (para cabecera) y base del porcentaje de aportación al nodo.
    const ámbito = {
        directo: { lbl: "directas", n: nodo.n_ucs_directo, imp: nodo.importe_directo },
        descendientes: {
            lbl: "de descendientes",
            n: nodo.n_ucs - nodo.n_ucs_directo,
            imp: Math.round((nodo.importe - nodo.importe_directo) * 100) / 100,
        },
        ancestros: {
            lbl: "de ancestros (fracción)",
            n: nodo.n_ucs_ancestros,
            imp: nodo.importe_ancestros,
        },
        total: { lbl: "subárbol", n: nodo.n_ucs, imp: nodo.importe },
    }[modo];
    const q = useQuery({
        queryKey: ["informes-carta-uc", esAncestros, cuerpo],
        queryFn: async () => {
            const url = esAncestros
                ? "/api/informes-carta/uc-ancestros"
                : "/api/informes-carta/uc";
            const r = await fetch(url, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify(cuerpo),
            });
            return (await r.json()) as { n_total: number; n_devueltas: number; filas: Record<string, unknown>[] };
        },
    });

    type Fila = Record<string, unknown>;
    // Métrica relevante por fila: en ancestros, lo aportado (importe × fracción);
    // en el resto, el importe.
    const métrica = (f: Fila) =>
        Number(f.importe ?? 0) * (esAncestros ? Number(f._fraccion ?? 0) : 1);
    const sumMétrica = (fs: Fila[]) => fs.reduce((s, f) => s + métrica(f), 0);

    const tabla = (filas: Fila[], offset: number) => (
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
                    <th
                        className="px-2 py-1 text-right font-semibold"
                        title={esAncestros
                            ? "Fracción de la UC del ancestro asignada a este nodo"
                            : "Aportación de la UC al total del nodo pinchado"}
                    >
                        {esAncestros ? "% asignado" : "% nodo"}
                    </th>
                    {esAncestros && (
                        <th
                            className="px-2 py-1 text-right font-semibold"
                            title="Importe × fracción: lo que esta UC aporta al nodo"
                        >
                            Aporta
                        </th>
                    )}
                </tr>
            </thead>
            <tbody>
                {filas.map((f, i) => {
                    const imp = Number(f.importe ?? 0);
                    const fraccion = esAncestros ? Number(f._fraccion ?? 0) : 0;
                    const pct = esAncestros
                        ? 100 * fraccion
                        : nodo.importe ? (100 * imp) / nodo.importe : 0;
                    return (
                        <tr key={`${f.id}-${offset + i}`} className={cn((offset + i) % 2 === 1 && "bg-slate-50/60")}>
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
                            <td className="px-2 py-0.5 text-right tabular-nums">{formatFloat(imp)}</td>
                            <td className="px-2 py-0.5 text-right tabular-nums text-slate-500">
                                {pct.toLocaleString("es-ES", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%
                            </td>
                            {esAncestros && (
                                <td className="px-2 py-0.5 text-right tabular-nums">
                                    {formatFloat(imp * fraccion)}
                                </td>
                            )}
                        </tr>
                    );
                })}
            </tbody>
        </table>
    );
    return (
        <tr>
            <td colSpan={5}>
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
                                    ({nodo.eje} · {ámbito.lbl}: {formatInt(ámbito.n)} UC,{" "}
                                    {formatFloat(ámbito.imp)})
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
                        {q.data && (() => {
                            const filas = q.data.filas;
                            const dag = filas.filter((f) => !!f.marca_dag);
                            const noDag = filas.filter((f) => !f.marca_dag);
                            const lblMet = esAncestros ? "aportado" : "importe";
                            return (
                                <>
                                    {/* Estadísticas globales */}
                                    <div className="mb-3 grid grid-cols-1 gap-2 text-xs sm:grid-cols-3">
                                        <div className="rounded border border-slate-200 bg-slate-50 px-2 py-1">
                                            <div className="text-slate-500">Total ({lblMet})</div>
                                            <div className="font-semibold">
                                                {formatInt(filas.length)} UC · {formatFloat(sumMétrica(filas))}
                                            </div>
                                            {q.data.n_total > q.data.n_devueltas && (
                                                <div className="text-[10px] text-amber-600">
                                                    mostrando {formatInt(q.data.n_devueltas)} de {formatInt(q.data.n_total)}
                                                </div>
                                            )}
                                        </div>
                                        <div className="rounded border border-slate-200 bg-white px-2 py-1">
                                            <div className="text-slate-500">No dag</div>
                                            <div className="font-semibold">
                                                {formatInt(noDag.length)} UC · {formatFloat(sumMétrica(noDag))}
                                            </div>
                                        </div>
                                        <div className="rounded border border-slate-200 bg-white px-2 py-1">
                                            <div className="text-slate-500">Procedentes de dag</div>
                                            <div className="font-semibold">
                                                {formatInt(dag.length)} UC · {formatFloat(sumMétrica(dag))}
                                            </div>
                                        </div>
                                    </div>

                                    {/* Bloque 1: UC que NO provienen de dag */}
                                    <div className="mb-1 text-sm font-semibold text-slate-700">
                                        UC propias (no proceden de reparto dag)
                                    </div>
                                    {noDag.length > 0
                                        ? tabla(noDag, 0)
                                        : <div className="mb-3 text-xs text-slate-400">— ninguna —</div>}

                                    {/* Bloque 2: UC procedentes de reparto dag */}
                                    <div className="mb-1 mt-4 text-sm font-semibold text-slate-700">
                                        UC procedentes de reparto dag
                                    </div>
                                    {dag.length > 0
                                        ? tabla(dag, noDag.length)
                                        : <div className="text-xs text-slate-400">— ninguna —</div>}
                                </>
                            );
                        })()}
                    </div>
                </div>
            </td>
        </tr>
    );
}

export function InformesACarta() {
    const [filtro, setFiltro] = useState<Filtro>({
        estructura: "act",
        centros_de_coste: [],
        actividades: [],
        elementos_de_coste: [],
    });
    const [consultaActiva, setConsultaActiva] = useState<Filtro | null>(null);
    const [nombreConfig, setNombreConfig] = useState("");

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
        const f = (await r.json()) as Partial<Filtro>;
        // Configs antiguas (multi-eje) pueden no traer `estructura`.
        setFiltro({
            estructura: f.estructura ?? "act",
            centros_de_coste: f.centros_de_coste ?? [],
            actividades: f.actividades ?? [],
            elementos_de_coste: f.elementos_de_coste ?? [],
        });
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

            {/* Estructura del informe (monográfico: un solo eje vertebra el árbol) */}
            <div className="flex flex-wrap items-center gap-3 text-sm">
                <span className="font-semibold text-slate-700">Estructura del informe:</span>
                <div className="inline-flex overflow-hidden rounded border border-slate-300" role="group">
                    {(["cc", "act", "ec"] as Eje[]).map((e) => (
                        <button
                            key={e}
                            type="button"
                            onClick={() => setFiltro({ ...filtro, estructura: e })}
                            className={cn(
                                "px-3 py-1",
                                filtro.estructura === e
                                    ? "bg-slate-800 text-white"
                                    : "bg-white text-slate-600 hover:bg-slate-100",
                            )}
                        >
                            {EJE_LABEL[e]}
                        </button>
                    ))}
                </div>
                <span className="text-xs text-slate-500">
                    El árbol es del eje elegido; los otros dos solo filtran.
                </span>
            </div>

            {/* Selectores: el del eje de estructura es FOCO; los otros, FILTRO */}
            <div className="grid grid-cols-1 gap-3 lg:grid-cols-3">
                <TreeSelect
                    label={filtro.estructura === "cc"
                        ? "Centros de coste · foco (vacío = todo el árbol)"
                        : "Centros de coste · filtro (vacío = todos)"}
                    eje="cc"
                    seleccionados={filtro.centros_de_coste}
                    onChange={(v) => setFiltro({ ...filtro, centros_de_coste: v })}
                />
                <TreeSelect
                    label={filtro.estructura === "act"
                        ? "Actividades · foco (vacío = todo el árbol)"
                        : "Actividades · filtro (vacío = todas)"}
                    eje="act"
                    seleccionados={filtro.actividades}
                    onChange={(v) => setFiltro({ ...filtro, actividades: v })}
                />
                <TreeSelect
                    label={filtro.estructura === "ec"
                        ? "Elementos de coste · foco (vacío = todo el árbol)"
                        : "Elementos de coste · filtro (vacío = todos)"}
                    eje="ec"
                    seleccionados={filtro.elementos_de_coste}
                    onChange={(v) => setFiltro({ ...filtro, elementos_de_coste: v })}
                />
            </div>

            {/* Acciones */}
            <div className="flex flex-wrap items-center gap-3 text-sm">
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
                        <strong>{formatFloat(resQ.data.importe)}</strong>
                    </div>
                    <table className="w-full border-collapse text-sm">
                        <thead>
                            <tr className="border-b-2 border-slate-700 text-slate-700">
                                <th className="px-2 py-1 text-left font-semibold">Concepto</th>
                                <th className="px-2 py-1 text-right font-semibold" title="a) Importe asignado directamente al nodo">Directo</th>
                                <th className="px-2 py-1 text-right font-semibold" title="b) Importe que el nodo recibe de sus descendientes">Descendientes</th>
                                <th className="px-2 py-1 text-right font-semibold" title="c) Fracción del coste de los ancestros (infraestructura) que le corresponde">Ancestros</th>
                                <th className="px-2 py-1 text-right font-semibold" title="a + b + c: coste totalmente cargado del nodo">Total</th>
                            </tr>
                        </thead>
                        <tbody>
                            {resQ.data.raíces.map((r, i) => (
                                <NodoRow key={`${r.eje}-${r.slug}-${i}`} nodo={r} filtroBase={consultaActiva ?? filtro} />
                            ))}
                            <tr className="border-t-2 border-slate-700 font-semibold">
                                <td className="px-2 py-1">Total ({formatInt(resQ.data.n_ucs)} UC)</td>
                                <td className="px-2 py-1" />
                                <td className="px-2 py-1" />
                                <td className="px-2 py-1" />
                                <td className="px-2 py-1 text-right tabular-nums">{formatFloat(resQ.data.importe)}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    );
}
