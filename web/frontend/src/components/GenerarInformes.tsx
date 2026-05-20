import { useEffect, useRef, useState } from "react";
import { cn } from "@/lib/cn";

type Status = "idle" | "running" | "done" | "error";

type JobInfo = {
    id: string;
    status: "running" | "done" | "error";
    started_at: number;
    finished_at: number | null;
    n_lines: number;
    error: string | null;
    kind: string;
};

/**
 * Botón que arranca la Fase 2 (generación de informes), compila el PDF
 * Typst y lo abre en el visor de PDF del sistema. Muestra el log en
 * vivo en el panel inferior compartido con la Fase 1.
 */
export function GenerarInformes() {
    const [status, setStatus] = useState<Status>("idle");
    const [lines, setLines] = useState<string[]>([]);
    const [error, setError] = useState<string | null>(null);
    const [jobId, setJobId] = useState<string | null>(null);
    const [shown, setShown] = useState(false);
    const eventSrc = useRef<EventSource | null>(null);
    const logRef = useRef<HTMLDivElement | null>(null);

    useEffect(() => {
        const el = logRef.current;
        if (el) el.scrollTop = el.scrollHeight;
    }, [lines]);

    useEffect(() => {
        return () => {
            eventSrc.current?.close();
        };
    }, []);

    useEffect(() => {
        let cancelado = false;
        fetch("/api/sistema/informes/current")
            .then((r) => (r.ok ? (r.json() as Promise<JobInfo | null>) : null))
            .then((info) => {
                if (cancelado || !info) return;
                if (info.status === "running") {
                    setJobId(info.id);
                    setStatus("running");
                    setShown(true);
                    abrirStream(info.id);
                }
            })
            .catch(() => {});
        return () => { cancelado = true; };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    function abrirStream(id: string) {
        eventSrc.current?.close();
        const src = new EventSource(`/api/sistema/fase1/${id}/stream`);
        eventSrc.current = src;

        const onLog = (ev: Event) => {
            const e = ev as MessageEvent;
            try {
                const linea = JSON.parse(e.data) as string;
                setLines((prev) => [...prev, linea]);
            } catch {
                setLines((prev) => [...prev, String(e.data)]);
            }
        };
        const onDone = () => {
            setStatus("done");
            src.close();
        };
        const onError = (ev: Event) => {
            const e = ev as MessageEvent;
            try {
                const data = JSON.parse(e.data ?? "{}") as { error?: string };
                setError(data.error ?? "Error desconocido");
            } catch {
                setError("Conexión interrumpida");
            }
            setStatus("error");
            src.close();
        };

        src.addEventListener("log", onLog);
        src.addEventListener("done", onDone);
        src.addEventListener("error", onError);
    }

    async function ejecutar() {
        setLines([]);
        setError(null);
        setStatus("running");
        setShown(true);
        try {
            const res = await fetch("/api/sistema/informes/run", { method: "POST" });
            if (!res.ok) {
                const txt = await res.text();
                throw new Error(`HTTP ${res.status}: ${txt}`);
            }
            const job = (await res.json()) as JobInfo;
            setJobId(job.id);
            abrirStream(job.id);
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : String(e));
            setStatus("error");
        }
    }

    return (
        <>
            <button
                type="button"
                onClick={ejecutar}
                disabled={status === "running"}
                className={cn(
                    "w-full rounded-md px-3 py-2 text-sm font-medium",
                    status === "running"
                        ? "bg-slate-300 text-slate-600"
                        : "bg-emerald-700 text-white hover:bg-emerald-600",
                )}
                title={
                    status === "running"
                        ? "Generando informes…"
                        : "Generar el PDF de informes y abrirlo en el visor del sistema"
                }
            >
                {status === "running" ? "Generando…" : "Generar informes"}
            </button>

            {shown && (
                <div className="fixed bottom-0 left-0 right-0 z-30 border-t border-slate-300 bg-slate-900 text-slate-100 shadow-lg">
                    <div className="flex items-center justify-between border-b border-slate-700 px-4 py-1.5 text-xs">
                        <span className="font-mono">
                            Informes{jobId && ` · job ${jobId}`} · {status}
                            {status === "running" && " ⏳"}
                            {status === "done" && " ✓"}
                            {status === "error" && " ✗"}
                            {error && ` · ${error}`}
                        </span>
                        <button
                            type="button"
                            onClick={() => {
                                eventSrc.current?.close();
                                setShown(false);
                            }}
                            className="rounded px-2 py-0.5 text-slate-400 hover:bg-slate-700 hover:text-white"
                            aria-label="Cerrar log"
                        >
                            ✕
                        </button>
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
            )}
        </>
    );
}
