import { ResourceView } from "@/components/ResourceView";

export function PresupuestoUc() {
    return (
        <ResourceView
            title="Presupuesto · Unidades de coste"
            subtitle="UC generadas por el traductor de presupuesto a partir de los apuntes."
            kpiEndpoint="/api/presupuesto/_resumen"
            listEndpoint="/api/presupuesto/uc"
            recordEndpoint="/api/presupuesto/uc/{id}"
            queryKey="presupuesto:uc"
        />
    );
}
