import { useEffect, useRef } from "react";
import { terminalStore, useTerminalStore } from "@/lib/terminalStore";

const KIND_LABEL: Record<string, string> = {
    fase1: "Unidades de coste",
    informes: "Informes",
};

/** Panel inferior fijo que muestra el log activo del store global. */
export function TerminalPanel() {
    const { shown, lines, status, error, jobId, kind } = useTerminalStore();
    const logRef = useRef<HTMLDivElement | null>(null);

    useEffect(() => {
        const el = logRef.current;
        if (el) el.scrollTop = el.scrollHeight;
    }, [lines]);

    if (!shown) return null;

    const titulo = (kind && KIND_LABEL[kind]) || "Terminal";

    return (
        <div className="fixed bottom-0 left-0 right-0 z-30 border-t border-slate-300 bg-slate-900 text-slate-100 shadow-lg">
            <div className="flex items-center gap-3 border-b border-slate-700 px-4 py-1.5 text-xs">
                <button
                    type="button"
                    onClick={() => terminalStore.setShown(false)}
                    className="rounded px-2 py-0.5 text-slate-400 hover:bg-slate-700 hover:text-white"
                    aria-label="Cerrar terminal"
                    title="Cerrar terminal"
                >
                    ✕
                </button>
                <span className="font-mono">
                    {titulo}
                    {jobId && ` · job ${jobId}`} · {status}
                    {status === "running" && " ⏳"}
                    {status === "done" && " ✓"}
                    {status === "error" && " ✗"}
                    {error && ` · ${error}`}
                </span>
            </div>
            <div
                ref={logRef}
                className="max-h-64 overflow-y-auto px-4 py-2 font-mono text-xs leading-snug"
            >
                {lines.length === 0 ? (
                    <div className="text-slate-500">Esperando salida…</div>
                ) : (
                    lines.map((l, i) => (
                        <div key={i} className="whitespace-pre-wrap">
                            {l}
                        </div>
                    ))
                )}
            </div>
        </div>
    );
}
