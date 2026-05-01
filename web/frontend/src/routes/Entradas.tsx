import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import { cn } from "@/lib/cn";
import { DataTable } from "@/components/DataTable";
import { TreeView } from "@/components/TreeView";

type Fichero = {
    nombre: string;
    stem: string;
    extension: string;
    ruta_relativa: string;
    tamaño_bytes: number;
};

type Grupo = { subdirectorio: string; ficheros: Fichero[] };
type Catalogo = { grupos: Grupo[] };

function formatBytes(n: number) {
    if (n < 1024) return `${n} B`;
    if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
    return `${(n / 1024 ** 2).toFixed(1)} MB`;
}

export function Entradas() {
    const [seleccionada, setSeleccionada] = useState<Fichero | null>(null);

    const { data, isLoading, isError, error } = useQuery({
        queryKey: ["entradas:catalogo"],
        queryFn: async (): Promise<Catalogo> => {
            const res = await api.GET("/api/entradas/" as never, {} as never);
            const r = res as unknown as { data?: Catalogo; error?: unknown };
            if (r.error) throw new Error(JSON.stringify(r.error));
            if (!r.data) throw new Error("Respuesta vacía");
            return r.data;
        },
    });

    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">Entradas</h1>
                <p className="text-sm text-slate-500">
                    Ficheros disponibles en
                    <span className="font-mono"> data/entrada/</span>. Selecciona
                    uno para verlo.
                </p>
            </div>

            <div className="grid grid-cols-[16rem_1fr] gap-6">
                <aside className="rounded-md border border-slate-200 bg-white p-3 text-sm">
                    {isError && (
                        <div className="text-red-700">
                            {error instanceof Error ? error.message : String(error)}
                        </div>
                    )}
                    {isLoading && <div className="text-slate-500">Cargando…</div>}
                    {data?.grupos.map((g) => (
                        <div key={g.subdirectorio} className="mb-4">
                            <div className="mb-1 px-1 text-xs font-semibold uppercase tracking-wide text-slate-500">
                                {g.subdirectorio}
                            </div>
                            <ul className="flex flex-col">
                                {g.ficheros.map((f) => {
                                    const activo =
                                        seleccionada?.ruta_relativa === f.ruta_relativa;
                                    return (
                                        <li key={f.ruta_relativa}>
                                            <button
                                                type="button"
                                                onClick={() => setSeleccionada(f)}
                                                className={cn(
                                                    "flex w-full items-baseline justify-between gap-2 rounded px-2 py-1 text-left hover:bg-slate-100",
                                                    activo && "bg-slate-200 font-medium",
                                                )}
                                                title={f.ruta_relativa}
                                            >
                                                <span className="flex items-baseline gap-2 truncate">
                                                    <span className="text-xs text-slate-400">
                                                        {f.extension === ".tree" ? "▤" : "▦"}
                                                    </span>
                                                    <span className="truncate">{f.stem}</span>
                                                </span>
                                                <span className="shrink-0 font-mono text-xs text-slate-400">
                                                    {formatBytes(f.tamaño_bytes)}
                                                </span>
                                            </button>
                                        </li>
                                    );
                                })}
                            </ul>
                        </div>
                    ))}
                </aside>

                <section>
                    {seleccionada ? (
                        <div className="flex flex-col gap-3">
                            <div className="flex items-baseline gap-3">
                                <h2 className="text-lg font-medium">
                                    {seleccionada.nombre}
                                </h2>
                                <span className="font-mono text-xs text-slate-500">
                                    data/entrada/{seleccionada.ruta_relativa}
                                </span>
                            </div>
                            {seleccionada.extension === ".xlsx" ? (
                                <DataTable
                                    endpoint={
                                        `/api/entradas/xlsx?ruta=${encodeURIComponent(seleccionada.ruta_relativa)}`
                                    }
                                    queryKey={`entradas:xlsx:${seleccionada.ruta_relativa}`}
                                    rowKey={"__row_key_no_existe__"}
                                />
                            ) : (
                                <TreeView
                                    endpoint={
                                        `/api/entradas/tree?ruta=${encodeURIComponent(seleccionada.ruta_relativa)}`
                                    }
                                    queryKey={`entradas:tree:${seleccionada.ruta_relativa}`}
                                />
                            )}
                        </div>
                    ) : (
                        <div className="rounded-md border border-dashed border-slate-300 bg-white p-6 text-sm text-slate-500">
                            Selecciona un fichero del panel izquierdo.
                        </div>
                    )}
                </section>
            </div>
        </div>
    );
}
