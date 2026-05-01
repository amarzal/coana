import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import { formatValue, type ColumnFormat } from "@/lib/format";

type FieldValue = {
    name: string;
    label: string;
    value: unknown;
    format: ColumnFormat;
};

type RecordSection = {
    label: string;
    fields: FieldValue[];
};

type RecordResponseData = {
    main: FieldValue[];
    sections: RecordSection[];
};

type Props = {
    /** Endpoint relativo. Ej: "/api/presupuesto/uc/{id}". */
    endpoint: string;
    /** Identificador interpolado en el endpoint. Si null, no se hace fetch. */
    id: string | null;
    /** Identificador para la caché. */
    queryKey: string;
};

function FieldList({ fields }: { fields: FieldValue[] }) {
    return (
        <dl className="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-sm">
            {fields.map((f) => (
                <div key={f.name} className="contents">
                    <dt className="font-mono text-xs text-slate-500">{f.label}</dt>
                    <dd className="font-mono">{formatValue(f.value, f.format)}</dd>
                </div>
            ))}
        </dl>
    );
}

export function RecordCard({ endpoint, id, queryKey }: Props) {
    const { data, isLoading, isError, error } = useQuery({
        queryKey: [queryKey, id],
        queryFn: async (): Promise<RecordResponseData> => {
            if (id === null) throw new Error("id null");
            const url = endpoint.replace("{id}", encodeURIComponent(id));
            const res = await api.GET(url as never, {} as never);
            const r = res as unknown as { data?: RecordResponseData; error?: unknown };
            if (r.error) throw new Error(JSON.stringify(r.error));
            if (!r.data) throw new Error("Respuesta vacía");
            return r.data;
        },
        enabled: id !== null,
        staleTime: 60_000,
    });

    if (id === null) return null;

    if (isError) {
        return (
            <aside className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">
                Ficha: {error instanceof Error ? error.message : String(error)}
            </aside>
        );
    }
    if (isLoading || !data) {
        return (
            <aside className="rounded-md border border-slate-200 bg-white p-4 text-sm text-slate-500">
                Cargando ficha…
            </aside>
        );
    }

    return (
        <aside className="flex flex-col gap-4 rounded-md border border-slate-200 bg-white p-4">
            <div>
                <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-slate-500">
                    Ficha
                </h2>
                <FieldList fields={data.main} />
            </div>
            {data.sections.map((s) => (
                <div key={s.label}>
                    <h3 className="mb-2 text-xs font-medium uppercase tracking-wide text-slate-500">
                        {s.label}
                    </h3>
                    <FieldList fields={s.fields} />
                </div>
            ))}
        </aside>
    );
}
