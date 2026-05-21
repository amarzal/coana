import { useEffect, useState } from "react";
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
 * Botón que arranca la Fase 2 (generación de informes), empuja el log
 * al store global del terminal y deja un botón paralelo «Abrir PDF»
 * disponible una vez el PDF está en disco.
 */
export function GenerarInformes() {
    const t = useTerminalStore();
    const corriendo = t.kind === "informes" && t.status === "running";
    const [pdfExiste, setPdfExiste] = useState(false);

    const refrescarPdfExiste = () => {
        fetch("/api/sistema/informes/pdf-existe")
            .then((r) => r.ok ? r.json() : { existe: false })
            .then((d) => setPdfExiste(Boolean(d.existe)))
            .catch(() => { });
    };
    useEffect(refrescarPdfExiste, []);

    useEffect(() => {
        let cancelado = false;
        fetch("/api/sistema/informes/current")
            .then((r) => (r.ok ? (r.json() as Promise<JobInfo | null>) : null))
            .then((info) => {
                if (cancelado || !info) return;
                if (info.status === "running") {
                    terminalStore.start({ kind: "informes", jobId: info.id });
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
            refrescarPdfExiste();
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
        terminalStore.start({ kind: "informes", jobId: null });
        try {
            const res = await fetch("/api/sistema/informes/run", { method: "POST" });
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

    async function abrirPdf() {
        try {
            const r = await fetch("/api/sistema/informes/abrir-pdf", { method: "POST" });
            if (!r.ok) {
                const txt = await r.text();
                alert(`No se pudo abrir el PDF: ${txt}`);
            }
        } catch (e) {
            alert(`No se pudo abrir el PDF: ${e instanceof Error ? e.message : String(e)}`);
        }
    }

    return (
        <div className="flex gap-2">
            <button
                type="button"
                onClick={ejecutar}
                disabled={corriendo}
                className={cn(
                    "flex-1 rounded-md px-3 py-2 text-sm font-medium",
                    corriendo
                        ? "bg-slate-300 text-slate-600"
                        : "bg-emerald-700 text-white hover:bg-emerald-600",
                )}
                title={
                    corriendo
                        ? "Generando informes…"
                        : "Generar el PDF de informes"
                }
            >
                {corriendo ? "Generando…" : "Generar informes"}
            </button>
            <button
                type="button"
                onClick={abrirPdf}
                disabled={!pdfExiste || corriendo}
                className={cn(
                    "rounded-md px-3 py-2 text-sm font-medium",
                    pdfExiste && !corriendo
                        ? "border border-emerald-700 text-emerald-700 hover:bg-emerald-50"
                        : "border border-slate-200 text-slate-400",
                )}
                title={pdfExiste ? "Abrir el PDF en el visor del sistema" : "No hay PDF aún"}
            >
                Abrir PDF
            </button>
        </div>
    );
}
