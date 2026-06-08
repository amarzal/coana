import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";

type Etapa = {
    clave: string;
    nombre: string;
    existe: boolean;
    mtime: number | null;
    obsoleta: boolean;
    motivo: string;
};
type EstadoPipeline = { etapas: Etapa[]; hay_obsoletas: boolean };

/**
 * Banner global que avisa cuando una etapa derivada (Reparto, Fase 2)
 * está obsoleta respecto a la Fase 1. Evita ver números incoherentes
 * entre pantallas sin darse cuenta (p. ej. tras reejecutar la Fase 1
 * sin recalcular el reparto).
 */
export function PipelineStaleBanner() {
    const { data } = useQuery({
        queryKey: ["estado:pipeline"],
        queryFn: async (): Promise<EstadoPipeline> => {
            const res = await api.GET("/api/estado/pipeline" as never, {} as never);
            const r = res as unknown as { data?: EstadoPipeline; error?: unknown };
            if (r.error) throw new Error(JSON.stringify(r.error));
            if (!r.data) throw new Error("Respuesta vacía");
            return r.data;
        },
        refetchInterval: 15_000,
        staleTime: 10_000,
    });

    if (!data || !data.hay_obsoletas) return null;
    const obsoletas = data.etapas.filter((e) => e.obsoleta);

    return (
        <div className="mb-4 rounded-md border border-amber-300 bg-amber-50 px-4 py-3 text-sm text-amber-900">
            <div className="font-medium">⚠ Datos derivados desactualizados</div>
            <ul className="mt-1 list-disc pl-5">
                {obsoletas.map((e) => (
                    <li key={e.clave}>
                        <span className="font-medium">{e.nombre}:</span> {e.motivo}
                    </li>
                ))}
            </ul>
        </div>
    );
}
