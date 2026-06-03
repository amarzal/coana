import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { cn } from "@/lib/cn";
import type { NodoTree } from "@/components/TreeView";
import {
    normalizar,
    nodoCoincide,
    subarbolCoincide,
    Highlight,
} from "@/components/TreeView";

export type Eje = "cc" | "act" | "ec";

const ÁRBOL_NOMBRE: Record<Eje, string> = {
    cc: "centros-de-coste",
    act: "actividades",
    ec: "elementos-de-coste",
};

// ---------------------------------------------------------------------------

function indexar(n: NodoTree, m: Map<string, NodoTree>) {
    if (n.identificador) m.set(n.identificador, n);
    n.hijos.forEach((h) => indexar(h, m));
}

/** Ids del subárbol de `n` (inclusive). */
function idsSubárbol(n: NodoTree, out: Set<string>) {
    if (n.identificador) out.add(n.identificador);
    n.hijos.forEach((h) => idsSubárbol(h, out));
}

/** Marca en `out` los ids que tienen ALGÚN descendiente en `sel` (para el
 * estado «indeterminado» del checkbox). Devuelve si el subárbol bajo `n`
 * (excluyendo `n`) contiene un seleccionado. */
function marcarIndeterminados(
    n: NodoTree, sel: Set<string>, out: Set<string>,
): boolean {
    let abajo = false;
    for (const h of n.hijos) {
        const b = marcarIndeterminados(h, sel, out);
        if (sel.has(h.identificador) || b) abajo = true;
    }
    if (abajo) out.add(n.identificador);
    return abajo;
}

function Checkbox({
    checked, indeterminate, disabled, onChange,
}: {
    checked: boolean;
    indeterminate: boolean;
    disabled: boolean;
    onChange: () => void;
}) {
    const ref = useRef<HTMLInputElement>(null);
    useEffect(() => {
        if (ref.current) ref.current.indeterminate = !checked && indeterminate;
    }, [checked, indeterminate]);
    return (
        <input
            ref={ref}
            type="checkbox"
            checked={checked}
            disabled={disabled}
            onChange={onChange}
            onClick={(e) => e.stopPropagation()}
            className="shrink-0 accent-slate-700 disabled:opacity-50"
        />
    );
}

function NodoSel({
    nodo, nivel, query, sel, indet, ancestroSel, onToggle,
}: {
    nodo: NodoTree;
    nivel: number;
    query: string;
    sel: Set<string>;
    indet: Set<string>;
    ancestroSel: boolean;
    onToggle: (n: NodoTree) => void;
}) {
    const tieneHijos = nodo.hijos.length > 0;
    const qn = normalizar(query);
    const subC = qn ? subarbolCoincide(nodo, qn) : false;
    const coincide = qn ? nodoCoincide(nodo, qn) : false;
    const [abiertoU, setAbiertoU] = useState<boolean | null>(null);
    // Por defecto colapsado (para no renderizar miles de nodos); con búsqueda,
    // se abren los subárboles con coincidencias.
    const abierto = abiertoU ?? (qn ? subC : false);

    const propio = sel.has(nodo.identificador);
    const checked = ancestroSel || propio;
    const indeterminate = !checked && indet.has(nodo.identificador);

    if (qn && !subC) return null; // ocultar ramas sin coincidencia al buscar

    return (
        <div>
            <div
                className="flex items-center gap-1.5 rounded py-0.5 hover:bg-slate-50"
                style={{ paddingLeft: `${nivel * 1.0}rem` }}
            >
                <button
                    type="button"
                    onClick={() => tieneHijos && setAbiertoU(!abierto)}
                    className={cn(
                        "w-4 shrink-0 select-none text-center text-xs",
                        tieneHijos
                            ? "cursor-pointer text-slate-500 hover:text-slate-800"
                            : "text-transparent",
                    )}
                    aria-label={abierto ? "colapsar" : "expandir"}
                >
                    {tieneHijos ? (abierto ? "▾" : "▸") : "·"}
                </button>
                <Checkbox
                    checked={checked}
                    indeterminate={indeterminate}
                    disabled={ancestroSel}
                    onChange={() => onToggle(nodo)}
                />
                <button
                    type="button"
                    onClick={() => !ancestroSel && onToggle(nodo)}
                    className="flex min-w-0 flex-1 items-baseline gap-1.5 text-left"
                    title={ancestroSel ? "Incluido por un ancestro seleccionado" : undefined}
                >
                    {nodo.código && (
                        <span
                            className={cn(
                                "shrink-0 font-mono text-[11px]",
                                coincide ? "font-bold text-red-600"
                                    : nodo.nuevo ? "text-emerald-600" : "text-slate-400",
                            )}
                        >
                            <Highlight texto={nodo.código} query={query} />
                        </span>
                    )}
                    <span className={cn("truncate text-sm", nodo.nuevo && "text-emerald-700")}>
                        {nodo.descripción
                            ? <Highlight texto={nodo.descripción} query={query} />
                            : "(todos)"}
                    </span>
                </button>
            </div>
            {abierto && tieneHijos && (
                <div>
                    {nodo.hijos.map((h) => (
                        <NodoSel
                            key={h.identificador || h.código}
                            nodo={h}
                            nivel={nivel + 1}
                            query={query}
                            sel={sel}
                            indet={indet}
                            ancestroSel={checked}
                            onToggle={onToggle}
                        />
                    ))}
                </div>
            )}
        </div>
    );
}

// ---------------------------------------------------------------------------

export function TreeSelect({
    label, eje, seleccionados, onChange,
}: {
    label: string;
    eje: Eje;
    seleccionados: string[];
    onChange: (next: string[]) => void;
}) {
    const [abierto, setAbierto] = useState(false);
    const [query, setQuery] = useState("");

    const { data: árbol } = useQuery({
        queryKey: ["informes-carta-arbol", eje],
        queryFn: async (): Promise<NodoTree> => {
            const r = await fetch(`/api/resultados/arbol/${ÁRBOL_NOMBRE[eje]}`);
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return (await r.json()) as NodoTree;
        },
        staleTime: 5 * 60_000,
    });

    const índice = useMemo(() => {
        const m = new Map<string, NodoTree>();
        if (árbol) indexar(árbol, m);
        return m;
    }, [árbol]);

    const sel = useMemo(() => new Set(seleccionados), [seleccionados]);
    const indet = useMemo(() => {
        const out = new Set<string>();
        if (árbol) marcarIndeterminados(árbol, sel, out);
        return out;
    }, [árbol, sel]);

    function toggle(nodo: NodoTree) {
        const id = nodo.identificador;
        const next = new Set(seleccionados);
        if (next.has(id)) {
            next.delete(id);
        } else {
            // Al seleccionar un nodo, sus descendientes quedan cubiertos:
            // quitamos selecciones redundantes.
            const desc = new Set<string>();
            idsSubárbol(nodo, desc);
            for (const s of seleccionados) if (desc.has(s)) next.delete(s);
            next.add(id);
        }
        onChange(Array.from(next));
    }

    return (
        <div className="rounded border border-slate-200 bg-white p-2">
            <div className="mb-1 flex items-center justify-between gap-2">
                <span className="text-xs font-semibold text-slate-600">{label}</span>
            </div>

            {/* Chips de lo seleccionado */}
            {seleccionados.length > 0 && (
                <div className="mb-1 flex flex-wrap gap-1">
                    {seleccionados.map((s) => {
                        const n = índice.get(s);
                        return (
                            <span
                                key={s}
                                className="inline-flex items-center gap-1 rounded bg-slate-800 px-2 py-0.5 text-xs text-white"
                            >
                                <span className="text-slate-300">{n?.código ?? ""}</span>
                                <span>{n?.descripción ?? s}</span>
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

            <button
                type="button"
                onClick={() => setAbierto((v) => !v)}
                className="w-full rounded border border-slate-300 px-2 py-1 text-left text-sm text-slate-600 hover:bg-slate-50"
            >
                {seleccionados.length === 0
                    ? "Seleccionar en el árbol… (vacío = todos)"
                    : `${seleccionados.length} selección(es) · editar en el árbol…`}
            </button>

            {abierto && (
                <>
                    <div
                        className="fixed inset-0 z-30"
                        onClick={() => setAbierto(false)}
                    />
                    <div className="relative z-40">
                        <div className="absolute left-0 right-0 mt-1 rounded-md border border-slate-300 bg-white shadow-xl">
                            <div className="flex items-center gap-2 border-b border-slate-200 p-2">
                                <input
                                    type="text"
                                    value={query}
                                    autoFocus
                                    onChange={(e) => setQuery(e.target.value)}
                                    placeholder="Buscar (código o descripción)…"
                                    className="w-full rounded border border-slate-300 px-2 py-1 text-sm"
                                />
                                {seleccionados.length > 0 && (
                                    <button
                                        type="button"
                                        onClick={() => onChange([])}
                                        className="shrink-0 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100"
                                    >
                                        Limpiar
                                    </button>
                                )}
                                <button
                                    type="button"
                                    onClick={() => setAbierto(false)}
                                    className="shrink-0 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100"
                                >
                                    Cerrar
                                </button>
                            </div>
                            <div className="max-h-[60vh] overflow-auto p-2">
                                {!árbol && <div className="text-sm text-slate-500">Cargando árbol…</div>}
                                {árbol && (
                                    <NodoSel
                                        nodo={árbol}
                                        nivel={0}
                                        query={query}
                                        sel={sel}
                                        indet={indet}
                                        ancestroSel={false}
                                        onToggle={toggle}
                                    />
                                )}
                            </div>
                        </div>
                    </div>
                </>
            )}
        </div>
    );
}
