import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import { cn } from "@/lib/cn";
import { DataTable } from "@/components/DataTable";
import { RecordCard } from "@/components/RecordCard";
import { formatEuro } from "@/lib/format";

type NodoConTotales = {
    código: string;
    descripción: string;
    identificador: string;
    nuevo?: boolean;
    n_uc_directo: number;
    importe_directo: number;
    n_uc_subárbol: number;
    importe_subárbol: number;
    hijos: NodoConTotales[];
};

type Slug = "actividades" | "centros-de-coste" | "elementos-de-coste";

const TÍTULOS: Record<Slug, { titulo: string; subtitulo: string }> = {
    "actividades": {
        titulo: "Resultados Fase 1 · Actividades",
        subtitulo:
            "Árbol jerárquico de actividades enriquecido con totales de UC. "
            + "Selecciona un nodo para ver sus UC y el reparto entre hijos.",
    },
    "centros-de-coste": {
        titulo: "Resultados Fase 1 · Centros de coste",
        subtitulo:
            "Árbol jerárquico de centros de coste enriquecido con totales de UC. "
            + "Selecciona un nodo para ver sus UC y el reparto entre hijos.",
    },
    "elementos-de-coste": {
        titulo: "Resultados Fase 1 · Elementos de coste",
        subtitulo:
            "Árbol jerárquico de elementos de coste enriquecido con totales de UC. "
            + "Selecciona un nodo para ver sus UC y el reparto entre hijos.",
    },
};

function normalizar(s: string): string {
    return s.normalize("NFKD").replace(/\p{Diacritic}/gu, "").toLowerCase();
}

function Highlight({ texto, query }: { texto: string; query: string }) {
    if (!query) return <>{texto}</>;
    const tn = normalizar(texto);
    const qn = normalizar(query);
    if (!qn || !tn.includes(qn)) return <>{texto}</>;
    const partes: React.ReactNode[] = [];
    let i = 0;
    while (i < tn.length) {
        const j = tn.indexOf(qn, i);
        if (j < 0) { partes.push(texto.slice(i)); break; }
        if (j > i) partes.push(texto.slice(i, j));
        partes.push(
            <mark
                key={j}
                className="rounded bg-yellow-300 px-0.5 text-slate-900"
            >
                {texto.slice(j, j + qn.length)}
            </mark>,
        );
        i = j + qn.length;
    }
    return <>{partes}</>;
}

function nodoCoincide(n: NodoConTotales, qn: string): boolean {
    if (!qn) return false;
    return (
        normalizar(n.código).includes(qn) ||
        normalizar(n.descripción).includes(qn) ||
        normalizar(n.identificador).includes(qn)
    );
}

function subarbolCoincide(n: NodoConTotales, qn: string): boolean {
    if (nodoCoincide(n, qn)) return true;
    return n.hijos.some((h) => subarbolCoincide(h, qn));
}

function NodoFila({
    nodo,
    nivel,
    query,
    seleccionado,
    onSeleccionar,
}: {
    nodo: NodoConTotales;
    nivel: number;
    query: string;
    seleccionado: string | null;
    onSeleccionar: (n: NodoConTotales) => void;
}) {
    const qn = normalizar(query);
    const subCoincide = qn ? subarbolCoincide(nodo, qn) : false;
    const coincide = qn ? nodoCoincide(nodo, qn) : false;
    const [abiertoUsuario, setAbiertoUsuario] = useState<boolean | null>(null);
    const tieneHijos = nodo.hijos.length > 0;
    // Por defecto: abierto si hay búsqueda (y subárbol coincide) o si es la raíz.
    const abiertoPorDefecto = qn ? subCoincide : nivel < 1;
    const abierto = abiertoUsuario ?? abiertoPorDefecto;
    const esSeleccionado = seleccionado === nodo.identificador;
    const sinUc = nodo.n_uc_subárbol === 0;

    return (
        <div>
            <div
                className={cn(
                    "flex items-baseline gap-2 py-0.5 hover:bg-slate-50 cursor-pointer",
                    esSeleccionado && "bg-blue-50 hover:bg-blue-50",
                )}
                style={{ paddingLeft: `${nivel * 1.25}rem` }}
                onClick={() => onSeleccionar(nodo)}
            >
                <button
                    type="button"
                    onClick={(e) => {
                        e.stopPropagation();
                        if (tieneHijos) setAbiertoUsuario(!abierto);
                    }}
                    className={cn(
                        "w-6 shrink-0 select-none text-center text-sm",
                        tieneHijos
                            ? "cursor-pointer font-semibold text-slate-700 hover:text-slate-900"
                            : "text-transparent",
                    )}
                    aria-label={abierto ? "colapsar" : "expandir"}
                    title={tieneHijos ? (abierto ? "Colapsar" : "Expandir") : undefined}
                >
                    {tieneHijos ? (abierto ? "▾" : "▸") : "·"}
                </button>
                {nodo.código && (
                    <span
                        className={cn(
                            "shrink-0 font-mono text-xs",
                            coincide
                                ? "font-bold text-red-600"
                                : nodo.nuevo
                                    ? "text-emerald-600"
                                    : sinUc ? "text-slate-300" : "text-slate-400",
                        )}
                    >
                        <Highlight texto={nodo.código} query={query} />
                    </span>
                )}
                <span
                    className={cn(
                        "text-sm flex-1",
                        nodo.nuevo && "font-medium text-emerald-700",
                        sinUc && !nodo.nuevo && "text-slate-400",
                    )}
                    title={nodo.nuevo ? "Nodo añadido por las reglas de la fase 1" : undefined}
                >
                    {nodo.descripción
                        ? <Highlight texto={nodo.descripción} query={query} />
                        : "(raíz)"}
                </span>
                {nodo.identificador && (
                    <span
                        className={cn(
                            "shrink-0 font-mono text-xs",
                            coincide ? "text-red-500"
                                : nodo.nuevo ? "text-emerald-500"
                                    : sinUc ? "text-slate-300" : "text-slate-400",
                        )}
                    >
                        <Highlight texto={nodo.identificador} query={query} />
                    </span>
                )}
                <span
                    className={cn(
                        "ml-3 w-28 shrink-0 text-right tabular-nums text-xs",
                        nodo.n_uc_directo > 0 ? "font-semibold text-slate-800" : "text-slate-300",
                    )}
                    title="UCs directas del nodo (no incluye descendientes)"
                >
                    {nodo.n_uc_directo > 0
                        ? `${nodo.n_uc_directo.toLocaleString("es-ES")} UC`
                        : "—"}
                </span>
                <span
                    className={cn(
                        "ml-1 w-32 shrink-0 text-right tabular-nums text-xs",
                        nodo.importe_directo > 0 ? "font-semibold text-slate-800" : "text-slate-300",
                    )}
                    title="Importe directo del nodo (no incluye descendientes)"
                >
                    {nodo.importe_directo > 0 ? formatEuro(nodo.importe_directo) : "—"}
                </span>
                <span
                    className={cn(
                        "ml-3 w-28 shrink-0 text-right tabular-nums text-xs",
                        sinUc ? "text-slate-300" : "text-slate-500",
                    )}
                    title="UCs del nodo + descendientes"
                >
                    {tieneHijos
                        ? `(${nodo.n_uc_subárbol.toLocaleString("es-ES")} UC)`
                        : ""}
                </span>
                <span
                    className={cn(
                        "ml-1 w-32 shrink-0 text-right tabular-nums text-xs",
                        sinUc ? "text-slate-300" : "text-slate-500",
                    )}
                    title="Importe del nodo + descendientes"
                >
                    {tieneHijos ? `(${formatEuro(nodo.importe_subárbol)})` : ""}
                </span>
            </div>
            {abierto && tieneHijos && (
                <div>
                    {nodo.hijos.map((h) => (
                        <NodoFila
                            key={h.identificador || h.código}
                            nodo={h}
                            nivel={nivel + 1}
                            query={query}
                            seleccionado={seleccionado}
                            onSeleccionar={onSeleccionar}
                        />
                    ))}
                </div>
            )}
        </div>
    );
}

export function ArbolConUcs({ slug }: { slug: Slug }) {
    const { titulo, subtitulo } = TÍTULOS[slug];
    const [seleccionado, setSeleccionado] = useState<string | null>(null);
    const [seleccionadoLabel, setSeleccionadoLabel] = useState<string | null>(null);
    const [query, setQuery] = useState("");
    const [ucModal, setUcModal] = useState<{ origen: string; id: string } | null>(null);

    const { data, isLoading, isError, error } = useQuery({
        queryKey: ["resultados:arbol-totales", slug],
        queryFn: async (): Promise<NodoConTotales> => {
            const res = await api.GET(
                `/api/resultados/arbol/${slug}/con-totales` as never,
                {} as never,
            );
            const r = res as unknown as { data?: NodoConTotales; error?: unknown };
            if (r.error) throw new Error(JSON.stringify(r.error));
            if (!r.data) throw new Error("Respuesta vacía");
            return r.data;
        },
    });

    const nMatches = useMemo(() => {
        if (!data || !query) return 0;
        const qn = normalizar(query);
        let n = 0;
        const recorrer = (nodo: NodoConTotales) => {
            if (nodoCoincide(nodo, qn)) n++;
            nodo.hijos.forEach(recorrer);
        };
        recorrer(data);
        return n;
    }, [data, query]);

    // Selección por defecto: raíz.
    useEffect(() => {
        if (!seleccionado && data) {
            setSeleccionado("_raíz");
            setSeleccionadoLabel(`${data.descripción || "(raíz)"}`);
        }
    }, [data, seleccionado]);

    if (isError)
        return (
            <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">
                {error instanceof Error ? error.message : String(error)}
            </div>
        );
    if (isLoading || !data)
        return <div className="text-sm text-slate-500">Cargando árbol…</div>;

    const seleccionarNodo = (n: NodoConTotales) => {
        // Para listar UC del nodo usamos su identificador; para los
        // hijos de la raíz usamos el placeholder "_raíz".
        const isRoot = n === data;
        setSeleccionado(isRoot ? "_raíz" : n.identificador);
        setSeleccionadoLabel(
            isRoot ? (data.descripción || "(raíz)") : `${n.descripción} · ${n.identificador}`,
        );
    };

    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">{titulo}</h1>
                <p className="text-sm text-slate-500">{subtitulo}</p>
            </div>

            <div className="rounded-md border border-slate-200 bg-white p-4">
                <div className="mb-3 flex items-end gap-3">
                    <label className="flex flex-col gap-1 text-xs text-slate-500">
                        Buscar
                        <input
                            type="text"
                            value={query}
                            onChange={(e) => setQuery(e.target.value)}
                            placeholder="Texto a destacar (insensible a tildes/mayúsculas)…"
                            className="w-96 rounded-md border border-slate-300 bg-white px-2 py-1 text-sm focus:border-slate-500 focus:outline-none"
                        />
                    </label>
                    {query && (
                        <span className="pb-1 text-xs text-slate-500">
                            {nMatches.toLocaleString("es-ES")} {nMatches === 1 ? "nodo" : "nodos"}
                        </span>
                    )}
                </div>
                <div className="max-h-[50vh] overflow-auto text-sm">
                    <NodoFila
                        nodo={data}
                        nivel={0}
                        query={query}
                        seleccionado={seleccionado}
                        onSeleccionar={seleccionarNodo}
                    />
                </div>
            </div>

            {seleccionado && (
                <>
                    <div className="rounded-md border border-slate-200 bg-white p-4">
                        <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                            UC del nodo seleccionado · {seleccionadoLabel}
                        </h2>
                        <p className="mb-3 text-xs text-slate-500">
                            UCs imputadas directamente a este nodo (sin descendientes).
                        </p>
                        <DataTable
                            key={`uc-${slug}-${seleccionado}`}
                            endpoint={`/api/resultados/arbol/${slug}/${encodeURIComponent(seleccionado)}/uc`}
                            queryKey={`resultados:arbol:${slug}:uc:${seleccionado}`}
                            rowKey="id"
                            onRowSelect={(row) => {
                                const id = row.id;
                                const origen = row._origen;
                                if (id == null || origen == null) return;
                                setUcModal({
                                    origen: String(origen),
                                    id: String(id),
                                });
                            }}
                        />
                    </div>
                    <div className="rounded-md border border-slate-200 bg-white p-4">
                        <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                            Hijos del nodo
                        </h2>
                        <p className="mb-3 text-xs text-slate-500">
                            Número de UC y total imputado en cada hijo (agregando todos sus
                            descendientes), con el peso porcentual de cada uno entre sus
                            hermanos.
                        </p>
                        <DataTable
                            key={`hijos-${slug}-${seleccionado}`}
                            endpoint={`/api/resultados/arbol/${slug}/${encodeURIComponent(seleccionado)}/hijos`}
                            queryKey={`resultados:arbol:${slug}:hijos:${seleccionado}`}
                            rowKey="identificador"
                            onRowSelect={(row) => {
                                const ident = row.identificador as string;
                                const desc = row.descripción as string;
                                if (ident) {
                                    setSeleccionado(ident);
                                    setSeleccionadoLabel(`${desc} · ${ident}`);
                                }
                            }}
                            reorderImportes={false}
                        />
                    </div>
                </>
            )}

            {ucModal && (
                <UcDetalleModal
                    origen={ucModal.origen}
                    ucId={ucModal.id}
                    onClose={() => setUcModal(null)}
                />
            )}
        </div>
    );
}

function UcDetalleModal({
    origen, ucId, onClose,
}: { origen: string; ucId: string; onClose: () => void }) {
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
                className="flex max-h-[90vh] w-full max-w-3xl flex-col overflow-hidden rounded-lg border border-slate-200 bg-white shadow-xl"
                onClick={(e) => e.stopPropagation()}
            >
                <div className="flex items-center justify-between border-b border-slate-200 px-4 py-2">
                    <h2 className="text-sm font-medium uppercase tracking-wide text-slate-500">
                        UC {ucId} · {origen}
                    </h2>
                    <button
                        type="button"
                        onClick={onClose}
                        className="rounded px-2 py-1 text-sm text-slate-500 hover:bg-slate-100 hover:text-slate-800"
                    >
                        ✕ Cerrar
                    </button>
                </div>
                <div className="overflow-auto p-4">
                    <RecordCard
                        endpoint={`/api/resultados/uc/${encodeURIComponent(origen)}/{id}`}
                        id={ucId}
                        queryKey={`resultados:uc:${origen}:${ucId}`}
                    />
                </div>
            </div>
        </div>
    );
}

export function ResultadosArbolActividades() {
    return <ArbolConUcs slug="actividades" />;
}
export function ResultadosArbolCentros() {
    return <ArbolConUcs slug="centros-de-coste" />;
}
export function ResultadosArbolElementos() {
    return <ArbolConUcs slug="elementos-de-coste" />;
}
