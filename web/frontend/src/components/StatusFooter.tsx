import { useQuery } from "@tanstack/react-query";
import { cn } from "@/lib/cn";

type Health = {
    status: string;
    version: string;
    entrada_existe: boolean;
    fase1_existe: boolean;
};

/** Pilotos de estado en el pie del sidebar. Refresca cada 5 s. */
export function StatusFooter() {
    const { data, isError } = useQuery({
        queryKey: ["sistema:health"],
        queryFn: async (): Promise<Health> => {
            const r = await fetch("/api/sistema/health");
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return r.json() as Promise<Health>;
        },
        refetchInterval: 5_000,
        retry: false,
    });

    const apiOk = !isError && data?.status === "ok";
    const items: { label: string; ok: boolean | null; tip?: string }[] = [
        {
            label: "Backend",
            ok: apiOk ? true : (isError ? false : null),
            tip: data?.version ? `v${data.version}` : undefined,
        },
        {
            label: "data/entrada/",
            ok: apiOk ? !!data?.entrada_existe : null,
        },
        {
            label: "data/fase1/",
            ok: apiOk ? !!data?.fase1_existe : null,
            tip: apiOk && !data?.fase1_existe ? "Aún no se ha ejecutado la Fase 1" : undefined,
        },
    ];

    return (
        <div className="mt-auto border-t border-slate-200 pt-3 text-xs">
            <ul className="flex flex-col gap-1">
                {items.map((it) => (
                    <li key={it.label} className="flex items-center gap-2">
                        <span
                            className={cn(
                                "inline-block size-2.5 shrink-0 rounded-full",
                                it.ok === true && "bg-emerald-500",
                                it.ok === false && "bg-red-500",
                                it.ok === null && "bg-slate-300",
                            )}
                            aria-label={
                                it.ok === true ? "ok" :
                                it.ok === false ? "error" : "comprobando"
                            }
                        />
                        <span className="font-mono text-slate-600">{it.label}</span>
                        {it.tip && (
                            <span className="ml-auto truncate text-slate-400" title={it.tip}>
                                {it.tip}
                            </span>
                        )}
                    </li>
                ))}
            </ul>
        </div>
    );
}
