import { useEffect, useState } from "react";

type Health = {
    status: string;
    version: string;
    entrada_existe: boolean;
    fase1_existe: boolean;
};

export function App() {
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
                setError(e instanceof Error ? e.message : String(e))
            );
    }, []);

    return (
        <div className="min-h-screen bg-slate-50 text-slate-900">
            <header className="border-b border-slate-200 bg-white px-6 py-4">
                <h1 className="text-xl font-semibold">CoAna — gemelo web</h1>
                <p className="text-sm text-slate-500">
                    Andamiaje (Fase 0) · React + Vite + FastAPI
                </p>
            </header>
            <main className="mx-auto max-w-3xl px-6 py-8">
                <h2 className="mb-3 text-lg font-medium">Estado del backend</h2>
                {error ? (
                    <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">
                        Error: {error}
                    </div>
                ) : !health ? (
                    <div className="text-sm text-slate-500">Cargando…</div>
                ) : (
                    <dl className="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-sm">
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
            </main>
        </div>
    );
}
