import { useParams } from "react-router";
import { DataTable } from "@/components/DataTable";
import { TreeView } from "@/components/TreeView";

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
    const nombre = ruta.split("/").pop() ?? ruta;

    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">{nombre}</h1>
                <p className="font-mono text-xs text-slate-500">
                    data/entrada/{ruta}
                </p>
            </div>
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
