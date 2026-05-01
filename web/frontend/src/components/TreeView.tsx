import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { api } from "@/api/client";
import { cn } from "@/lib/cn";

type NodoTree = {
    código: string;
    descripción: string;
    identificador: string;
    hijos: NodoTree[];
};

type Props = {
    /** Endpoint que devuelve un NodoTree raíz. Ej: "/api/entradas/tree?ruta=…" */
    endpoint: string;
    queryKey: string;
};

/** Render recursivo de un nodo con expand/collapse. */
function Nodo({ nodo, nivel }: { nodo: NodoTree; nivel: number }) {
    const [abierto, setAbierto] = useState(nivel < 1);
    const tieneHijos = nodo.hijos.length > 0;
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
                    onClick={() => tieneHijos && setAbierto(!abierto)}
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
                    <span className="shrink-0 font-mono text-xs text-slate-400">
                        {nodo.código}
                    </span>
                )}
                <span className="text-sm">{nodo.descripción || "(raíz)"}</span>
                {nodo.identificador && (
                    <span className="ml-2 shrink-0 font-mono text-xs text-slate-400">
                        {nodo.identificador}
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
                        />
                    ))}
                </div>
            )}
        </div>
    );
}

export function TreeView({ endpoint, queryKey }: Props) {
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

    if (isError)
        return (
            <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">
                {error instanceof Error ? error.message : String(error)}
            </div>
        );
    if (isLoading || !data)
        return <div className="text-sm text-slate-500">Cargando árbol…</div>;

    return (
        <div className="rounded-md border border-slate-200 bg-white p-3">
            <Nodo nodo={data} nivel={0} />
        </div>
    );
}
