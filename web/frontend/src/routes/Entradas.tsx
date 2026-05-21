import { useState } from "react";
import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { ChevronDown, ChevronRight, BookOpen } from "lucide-react";
import { DataTable } from "@/components/DataTable";
import { TreeView } from "@/components/TreeView";
import { cn } from "@/lib/cn";

type Ficha = { descripción: string; campos: Record<string, string> };

function FichaDescriptiva({ ruta }: { ruta: string }) {
    const [abierto, setAbierto] = useState(false);
    const { data, isLoading } = useQuery({
        queryKey: ["entradas:ficha", ruta],
        queryFn: async (): Promise<Ficha> => {
            const r = await fetch(
                `/api/entradas/ficha?ruta=${encodeURIComponent(ruta)}`,
            );
            if (!r.ok) return { descripción: "", campos: {} };
            return (await r.json()) as Ficha;
        },
        staleTime: 60_000,
    });
    if (isLoading) return null;
    const hayInfo =
        !!data &&
        (data.descripción.trim().length > 0 || Object.keys(data.campos).length > 0);
    if (!hayInfo) return null;
    const Chevron = abierto ? ChevronDown : ChevronRight;
    return (
        <div className="rounded-md border border-slate-200 bg-white">
            <button
                type="button"
                onClick={() => setAbierto(!abierto)}
                aria-expanded={abierto}
                className="flex w-full items-center gap-2 border-b border-slate-100 px-3 py-2 text-left text-sm hover:bg-slate-50"
            >
                <Chevron size={14} className="text-slate-400" />
                <BookOpen size={14} className="text-slate-500" />
                <span className="font-medium text-slate-700">Ficha descriptiva</span>
                <span className="text-xs text-slate-400">
                    (de la especificación)
                </span>
            </button>
            {abierto && (
                <div className="space-y-3 p-3">
                    {data!.descripción && (
                        <p className="text-sm text-slate-700">{data!.descripción}</p>
                    )}
                    {Object.keys(data!.campos).length > 0 && (
                        <div>
                            <div className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-500">
                                Campos
                            </div>
                            <dl className="grid grid-cols-1 gap-x-4 gap-y-2 lg:grid-cols-2">
                                {Object.entries(data!.campos).map(([campo, desc]) => (
                                    <div
                                        key={campo}
                                        className={cn(
                                            "flex flex-col rounded border border-slate-100 bg-slate-50 px-2 py-1.5",
                                        )}
                                    >
                                        <dt className="font-mono text-xs font-semibold text-slate-700">
                                            {campo}
                                        </dt>
                                        <dd className="text-xs text-slate-600">{desc}</dd>
                                    </div>
                                ))}
                            </dl>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}

/**
 * Visor de un fichero de ``data/entrada/`` cuya ruta relativa viene como
 * splat en la URL (``/entradas/<subdir>/<fichero>``). Se selecciona desde
 * la sidebar (componente :func:`MainNav` / :func:`EntradasMenu`).
 */
export function Entradas() {
    const params = useParams();
    const ruta = params["*"] ?? "";

    if (!ruta) {
        return (
            <div className="flex flex-col gap-6">
                <div>
                    <h1 className="text-2xl font-semibold">Entradas</h1>
                    <p className="text-sm text-slate-500">
                        Ficheros disponibles en{" "}
                        <span className="font-mono">data/entrada/</span>.
                    </p>
                </div>
                <div className="rounded-md border border-dashed border-slate-300 bg-white p-6 text-sm text-slate-500">
                    Selecciona un fichero del menú lateral.
                </div>
            </div>
        );
    }

    const esTree = ruta.endsWith(".tree");
    const esXlsx = ruta.endsWith(".xlsx");
    const nombre = ruta.split("/").pop() ?? ruta;

    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">{nombre}</h1>
                <p className="font-mono text-xs text-slate-500">
                    data/entrada/{ruta}
                </p>
            </div>
            {esXlsx && <FichaDescriptiva ruta={ruta} />}
            {esTree ? (
                <TreeView
                    endpoint={`/api/entradas/tree?ruta=${encodeURIComponent(ruta)}`}
                    queryKey={`entradas:tree:${ruta}`}
                />
            ) : (
                <DataTable
                    endpoint="/api/entradas/xlsx"
                    queryKey={`entradas:xlsx:${ruta}`}
                    rowKey="__row_key_no_existe__"
                    extraParams={{ ruta }}
                    showPopoverOnRowClick
                />
            )}
        </div>
    );
}
