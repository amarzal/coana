import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { api } from "@/api/client";
import { cn } from "@/lib/cn";

export type NodoTree = {
    código: string;
    descripción: string;
    identificador: string;
    /** Nodo añadido por las reglas de la fase 1; no estaba en el árbol original. */
    nuevo?: boolean;
    hijos: NodoTree[];
};

type Props = {
    /** Endpoint que devuelve un NodoTree raíz. Ej: "/api/entradas/tree?ruta=…" */
    endpoint: string;
    queryKey: string;
};

/** Normaliza un texto: minúsculas y sin tildes. Equivalente al `sin_tildes`
 * del backend para que la búsqueda sea consistente. */
export function normalizar(s: string): string {
    return s.normalize("NFKD").replace(/\p{Diacritic}/gu, "").toLowerCase();
}

/** Resalta en `<mark>` cada ocurrencia de `query` en `texto`. Si la query
 * está vacía, devuelve el texto tal cual. La búsqueda es insensible a
 * tildes y mayúsculas; el highlight respeta el casing original. */
export function Highlight({ texto, query }: { texto: string; query: string }) {
    if (!query) return <>{texto}</>;
    const tn = normalizar(texto);
    const qn = normalizar(query);
    if (!qn || !tn.includes(qn)) return <>{texto}</>;
    // Recorremos `tn` buscando `qn`; los offsets normalizados se
    // corresponden 1-a-1 con los de `texto` siempre que NFKD no
    // descomponga caracteres en varios codepoints visibles. Para cubrir
    // ese caso (raro en nuestro corpus) trabajamos con substrings de
    // `texto` en los mismos índices.
    const partes: React.ReactNode[] = [];
    let i = 0;
    while (i < tn.length) {
        const j = tn.indexOf(qn, i);
        if (j < 0) {
            partes.push(texto.slice(i));
            break;
        }
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

/** Devuelve true si el nodo (cualquier campo: código, descripción,
 * identificador) contiene la query normalizada. */
export function nodoCoincide(n: NodoTree, q: string): boolean {
    if (!q) return false;
    return (
        normalizar(n.código).includes(q) ||
        normalizar(n.descripción).includes(q) ||
        normalizar(n.identificador).includes(q)
    );
}

/** True si el subárbol enraizado en `n` contiene algún nodo coincidente. */
export function subarbolCoincide(n: NodoTree, q: string): boolean {
    if (nodoCoincide(n, q)) return true;
    return n.hijos.some((h) => subarbolCoincide(h, q));
}

/** Render recursivo de un nodo con expand/collapse. */
function Nodo({
    nodo,
    nivel,
    query,
}: {
    nodo: NodoTree;
    nivel: number;
    query: string;
}) {
    // Si hay búsqueda, abrimos automáticamente los nodos cuyo subárbol
    // contenga una coincidencia. Si no, dejamos abierto por defecto
    // (comportamiento previo).
    const qn = normalizar(query);
    const subCoincide = qn ? subarbolCoincide(nodo, qn) : false;
    const coincide = qn ? nodoCoincide(nodo, qn) : false;
    const [abiertoUsuario, setAbiertoUsuario] = useState<boolean | null>(null);
    const tieneHijos = nodo.hijos.length > 0;
    const abiertoPorDefecto = qn ? subCoincide : true;
    const abierto = abiertoUsuario ?? abiertoPorDefecto;

    return (
        <div>
            <div
                className={cn(
                    "flex items-baseline gap-2 py-0.5 hover:bg-slate-50",
                    "border-l border-slate-100",
                )}
                style={{ paddingLeft: `${nivel * 1.25}rem` }}
            >
                <button
                    type="button"
                    onClick={() => tieneHijos && setAbiertoUsuario(!abierto)}
                    className={cn(
                        "w-5 shrink-0 select-none text-center text-xs",
                        tieneHijos
                            ? "cursor-pointer text-slate-500 hover:text-slate-800"
                            : "text-transparent",
                    )}
                    aria-label={abierto ? "colapsar" : "expandir"}
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
                                    : "text-slate-400",
                        )}
                    >
                        <Highlight texto={nodo.código} query={query} />
                    </span>
                )}
                <span
                    className={cn(
                        "text-sm",
                        nodo.nuevo && "font-medium text-emerald-700",
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
                            "ml-2 shrink-0 font-mono text-xs",
                            coincide
                                ? "text-red-500"
                                : nodo.nuevo
                                    ? "text-emerald-500"
                                    : "text-slate-400",
                        )}
                    >
                        <Highlight texto={nodo.identificador} query={query} />
                    </span>
                )}
            </div>
            {abierto && tieneHijos && (
                <div>
                    {nodo.hijos.map((h) => (
                        <Nodo
                            key={h.identificador || h.código}
                            nodo={h}
                            nivel={nivel + 1}
                            query={query}
                        />
                    ))}
                </div>
            )}
        </div>
    );
}

export function TreeView({ endpoint, queryKey }: Props) {
    const [query, setQuery] = useState("");

    const { data, isLoading, isError, error } = useQuery({
        queryKey: [queryKey],
        queryFn: async (): Promise<NodoTree> => {
            const res = await api.GET(endpoint as never, {} as never);
            const r = res as unknown as { data?: NodoTree; error?: unknown };
            if (r.error) throw new Error(JSON.stringify(r.error));
            if (!r.data) throw new Error("Respuesta vacía");
            return r.data;
        },
    });

    // Conteo de coincidencias para feedback al usuario.
    const nMatches = useMemo(() => {
        if (!data || !query) return 0;
        const qn = normalizar(query);
        let n = 0;
        const recorrer = (nodo: NodoTree) => {
            if (nodoCoincide(nodo, qn)) n++;
            nodo.hijos.forEach(recorrer);
        };
        recorrer(data);
        return n;
    }, [data, query]);

    if (isError)
        return (
            <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">
                {error instanceof Error ? error.message : String(error)}
            </div>
        );
    if (isLoading || !data)
        return <div className="text-sm text-slate-500">Cargando árbol…</div>;

    return (
        <div className="flex flex-col gap-3">
            <div className="flex items-end gap-3">
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
            <div className="rounded-md border border-slate-200 bg-white p-3">
                <Nodo nodo={data} nivel={0} query={query} />
            </div>
        </div>
    );
}
