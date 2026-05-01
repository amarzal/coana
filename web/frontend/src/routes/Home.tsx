import { useEffect, useState } from "react";

type Health = {
    status: string;
    version: string;
    entrada_existe: boolean;
    fase1_existe: boolean;
};

export function Home() {
    const [health, setHealth] = useState<Health | null>(null);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        fetch("/api/sistema/health")
            .then((r) => {
                if (!r.ok) throw new Error(`HTTP ${r.status}`);
                return r.json() as Promise<Health>;
            })
            .then(setHealth)
            .catch((e: unknown) =>
                setError(e instanceof Error ? e.message : String(e)),
            );
    }, []);

    return (
        <div>
            <h1 className="mb-3 text-2xl font-semibold">Inicio</h1>
            <p className="mb-4 text-sm text-slate-500">
                Gemelo del visor en construcción · Fase 1 (walking skeleton).
            </p>
            <h2 className="mb-2 text-lg font-medium">Estado del backend</h2>
            {error ? (
                <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">
                    Error: {error}
                </div>
            ) : !health ? (
                <div className="text-sm text-slate-500">Cargando…</div>
            ) : (
                <dl className="grid w-fit grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-sm">
                    <dt className="text-slate-500">status</dt>
                    <dd>{health.status}</dd>
                    <dt className="text-slate-500">version</dt>
                    <dd className="font-mono">{health.version}</dd>
                    <dt className="text-slate-500">entrada existe</dt>
                    <dd>{health.entrada_existe ? "sí" : "no"}</dd>
                    <dt className="text-slate-500">fase1 existe</dt>
                    <dd>{health.fase1_existe ? "sí" : "no"}</dd>
                </dl>
            )}
        </div>
    );
}
