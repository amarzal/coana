import { useEffect } from "react";
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

/** Modal centrado que muestra todos los campos de una fila. */
export function RowDetailPopover({ row, columns, onClose }: Props) {
    // Cierra con Escape.
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
                <dl className="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 px-4 py-3 text-sm">
                    {columns.map((c) => (
                        <div key={c.name} className="contents">
                            <dt className="font-mono text-xs text-slate-500">
                                {c.label}
                            </dt>
                            <dd className="font-mono break-words">
                                {formatValue(row[c.name], c.format)}
                            </dd>
                        </div>
                    ))}
                </dl>
            </div>
        </div>
    );
}
