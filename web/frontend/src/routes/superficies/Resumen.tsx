import { KpiPanel } from "@/components/KpiPanel";

export function SuperficiesResumen() {
    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">Superficies · Resumen</h1>
                <p className="text-sm text-slate-500">
                    Totales globales del campus a partir de
                    <span className="font-mono"> data/entrada/superficies/</span>.
                </p>
            </div>
            <KpiPanel
                endpoint="/api/superficies/_resumen"
                queryKey="superficies:resumen"
            />
        </div>
    );
}
