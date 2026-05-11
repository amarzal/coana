import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { formatValue, type ColumnFormat } from "@/lib/format";

type ColumnSpec = {
    name: string;
    label: string;
    format: ColumnFormat;
};

type Props = {
    row: Record<string, unknown>;
    columns: ColumnSpec[];
    onClose: () => void;
};

type EnrichResponse = {
    enriquecimientos: Record<string, Record<string, string>>;
};

/** Modal centrado que muestra todos los campos de una fila, enriquecidos. */
export function RowDetailPopover({ row, columns, onClose }: Props) {
    // Cierra con Escape.
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === "Escape") onClose();
        };
        window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [onClose]);

    // Pide enriquecimiento al backend (caché por contenido del row).
    const { data: enrich } = useQuery({
        queryKey: ["lookups:enrich-row", row],
        queryFn: async (): Promise<EnrichResponse> => {
            const r = await fetch("/api/lookups/enrich-row", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ row }),
            });
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return r.json() as Promise<EnrichResponse>;
        },
    });

    const enriquecimientos = enrich?.enriquecimientos ?? {};

    return (
        <div
            className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-6"
            onClick={onClose}
            role="dialog"
            aria-modal="true"
        >
            <div
                className="max-h-[80vh] w-full max-w-2xl overflow-y-auto rounded-lg border border-slate-200 bg-white shadow-xl"
                onClick={(e) => e.stopPropagation()}
            >
                <div className="flex items-center justify-between border-b border-slate-200 px-4 py-2">
                    <h2 className="text-sm font-medium uppercase tracking-wide text-slate-500">
                        Detalle de la fila
                    </h2>
                    <button
                        type="button"
                        onClick={onClose}
                        className="rounded px-2 py-1 text-sm text-slate-500 hover:bg-slate-100 hover:text-slate-800"
                        aria-label="Cerrar"
                    >
                        ✕ Cerrar
                    </button>
                </div>
                <dl className="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 px-4 py-3 text-sm">
                    {columns.map((c) => {
                        const extra = enriquecimientos[c.name];
                        const extraEntries = extra ? Object.entries(extra) : [];
                        return (
                            <div key={c.name} className="contents">
                                <dt className="font-mono text-xs text-slate-500">
                                    {c.label}
                                </dt>
                                <dd className="font-mono break-words">
                                    {formatValue(row[c.name], c.format)}
                                    {extraEntries.length > 0 && (
                                        <table className="mt-1 border-collapse text-xs text-slate-500">
                                            <tbody>
                                                {extraEntries.map(([k, v]) => (
                                                    <tr key={k} className="align-top">
                                                        <td className="pr-3 text-slate-400">
                                                            {k}
                                                        </td>
                                                        <td className="italic">
                                                            {v || "—"}
                                                        </td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    )}
                                </dd>
                            </div>
                        );
                    })}
                </dl>
            </div>
        </div>
    );
}
