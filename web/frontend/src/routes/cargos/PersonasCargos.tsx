import { DataTable } from "@/components/DataTable";

export function CargosPersonasCargos() {
    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">
                    Cargos académicos · Personas cargos
                </h1>
                <p className="text-sm text-slate-500">
                    Fichero bruto de cargos académicos (data/entrada/nóminas/personas
                    cargos.xlsx) con todos los periodos históricos. Al pinchar una fila se muestra,
                    si la persona ha percibido por concepto retributivo 19 o 64 (cargos académicos,
                    excluidos los atrasos), el desglose de importes por mes en el año analizado.
                </p>
            </div>
            <DataTable
                endpoint="/api/cargos/personas-cargos"
                queryKey="cargos:personas-cargos"
                rowKey="per_id"
                showPopoverOnRowClick
            />
        </div>
    );
}
