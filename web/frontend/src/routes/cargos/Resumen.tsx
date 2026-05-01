import { KpiPanel } from "@/components/KpiPanel";

export function CargosResumen() {
    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">Cargos académicos · Resumen</h1>
                <p className="text-sm text-slate-500">
                    Métricas globales de la categorización de PDI/PVI y de los cargos
                    académicos por departamento.
                </p>
            </div>
            <KpiPanel endpoint="/api/cargos/_resumen" queryKey="cargos:resumen" />
        </div>
    );
}
