import { ResourceView } from "@/components/ResourceView";

export function CargosDepartamentos() {
    return (
        <ResourceView
            title="Cargos académicos · Departamentos"
            subtitle="Cargos académicos asociados a departamentos (cuantía > 0, al menos un día activo en el año)."
            listEndpoint="/api/cargos/departamentos"
            recordEndpoint="/api/cargos/departamentos/{id}"
            rowKey="idx"
            queryKey="cargos:departamentos"
        />
    );
}
