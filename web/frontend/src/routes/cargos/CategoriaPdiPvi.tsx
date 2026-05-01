import { ResourceView } from "@/components/ResourceView";

export function CargosCategoria() {
    return (
        <ResourceView
            title="Cargos académicos · Categoría PDI/PVI"
            subtitle="Categoría de cada PDI/PVI tras su último cobro de los conceptos retributivos 19 o 64."
            listEndpoint="/api/cargos/categoria_pdi_pvi"
            recordEndpoint="/api/cargos/categoria_pdi_pvi/{id}"
            rowKey="per_id"
            queryKey="cargos:categoria"
        />
    );
}
