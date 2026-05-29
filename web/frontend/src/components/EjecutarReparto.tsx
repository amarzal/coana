import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { cn } from "@/lib/cn";
import { terminalStore, useTerminalStore } from "@/lib/terminalStore";

type JobInfo = {
    id: string;
    status: "running" | "done" | "error";
    started_at: number;
    finished_at: number | null;
    n_lines: number;
    error: string | null;
    kind?: string;
};

/**
 * Botón que arranca la fase de reparto de actividades (costes dag) y
 * empuja el log al store global del terminal. Reutiliza el stream SSE
 * genérico del job manager.
 */
export function EjecutarReparto() {
    const queryClient = useQueryClient();
    const t = useTerminalStore();
    const corriendo = t.kind === "reparto" && t.status === "running";

    useEffect(() => {
        let cancelado = false;
        fetch("/api/sistema/reparto/current")
            .then((r) => (r.ok ? (r.json() as Promise<JobInfo | null>) : null))
            .then((info) => {
                if (cancelado || !info) return;
                if (info.status === "running") {
                    terminalStore.start({ kind: "reparto", jobId: info.id });
                    abrirStream(info.id);
                }
            })
            .catch(() => { });
        return () => { cancelado = true; };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    function abrirStream(id: string) {
        const src = new EventSource(`/api/sistema/fase1/${id}/stream`);
        const onLog = (ev: Event) => {
            const e = ev as MessageEvent;
            try {
                const linea = JSON.parse(e.data) as string;
                terminalStore.pushLine(linea);
            } catch {
                terminalStore.pushLine(String(e.data));
            }
        };
        const onDone = () => {
            terminalStore.setDone();
            src.close();
            queryClient.invalidateQueries();
        };
        const onError = (ev: Event) => {
            const e = ev as MessageEvent;
            let msg = "Conexión interrumpida";
            try {
                const data = JSON.parse(e.data ?? "{}") as { error?: string };
                msg = data.error ?? msg;
            } catch { /* ignore */ }
            terminalStore.setError(msg);
            src.close();
        };
        src.addEventListener("log", onLog);
        src.addEventListener("done", onDone);
        src.addEventListener("error", onError);
    }

    async function ejecutar() {
        terminalStore.start({ kind: "reparto", jobId: null });
        try {
            const res = await fetch("/api/sistema/reparto/run", { method: "POST" });
            if (!res.ok) {
                const txt = await res.text();
                throw new Error(`HTTP ${res.status}: ${txt}`);
            }
            const job = (await res.json()) as JobInfo;
            abrirStream(job.id);
        } catch (e: unknown) {
            terminalStore.setError(e instanceof Error ? e.message : String(e));
        }
    }

    return (
        <button
            type="button"
            onClick={ejecutar}
            disabled={corriendo}
            className={cn(
                "w-full rounded-md px-3 py-2 text-sm font-medium",
                corriendo
                    ? "bg-slate-300 text-slate-600"
                    : "bg-slate-800 text-white hover:bg-slate-700",
            )}
            title={
                corriendo
                    ? "Hay una ejecución en curso"
                    : "Repartir los costes dag entre actividades no-dag y ver el log en vivo"
            }
        >
            {corriendo ? "Repartiendo…" : "Reparto actividades"}
        </button>
    );
}
