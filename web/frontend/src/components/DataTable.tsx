import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
    flexRender,
    getCoreRowModel,
    useReactTable,
    type ColumnDef,
    type SortingState,
} from "@tanstack/react-table";
import { api } from "@/api/client";
import { cn } from "@/lib/cn";
import { formatValue, type ColumnFormat } from "@/lib/format";

type ColumnSpec = {
    name: string;
    label: string;
    format: ColumnFormat;
    sortable: boolean;
};

type ListResponse = {
    columns: ColumnSpec[];
    rows: Record<string, unknown>[];
    total: number;
};

type Props = {
    /** Endpoint relativo, sin prefijo /api. Ej: "/presupuesto/uc". */
    endpoint: string;
    /** Identificador para la caché de react-query. */
    queryKey: string;
    /** Disparado al pinchar una fila. Recibe la fila completa. */
    onRowSelect?: (row: Record<string, unknown>) => void;
    /** Identificador único de la fila (por defecto "id"). */
    rowKey?: string;
    /** Tamaño de página inicial. */
    pageSize?: number;
};

const PAGE_SIZES = [25, 50, 100, 250, 500];

export function DataTable({
    endpoint,
    queryKey,
    onRowSelect,
    rowKey = "id",
    pageSize: initialPageSize = 50,
}: Props) {
    const [q, setQ] = useState("");
    const [columnFilter, setColumnFilter] = useState<string>(""); // "" = todas
    const [sorting, setSorting] = useState<SortingState>([]);
    const [pageSize, setPageSize] = useState(initialPageSize);
    const [pageIndex, setPageIndex] = useState(0);
    const [selectedKey, setSelectedKey] = useState<string | null>(null);

    const sortBy = sorting[0]?.id;
    const desc = sorting[0]?.desc ?? false;

    const params = {
        q: q || undefined,
        column: columnFilter || undefined,
        sort_by: sortBy,
        desc,
        offset: pageIndex * pageSize,
        limit: pageSize,
    };

    const { data, isLoading, isError, error } = useQuery({
        queryKey: [queryKey, params],
        queryFn: async (): Promise<ListResponse> => {
            const res = await api.GET(endpoint as never, {
                params: { query: params },
            } as never);
            // openapi-fetch v0.17 expone { data, error }; el tipado de
            // endpoints dinámicos requiere cast.
            const r = res as unknown as { data?: ListResponse; error?: unknown };
            if (r.error) throw new Error(JSON.stringify(r.error));
            if (!r.data) throw new Error("Respuesta vacía");
            return r.data;
        },
        staleTime: 5_000,
    });

    const columns: ColumnSpec[] = data?.columns ?? [];
    const rows: Record<string, unknown>[] = data?.rows ?? [];
    const total = data?.total ?? 0;

    const tableColumns = useMemo<ColumnDef<Record<string, unknown>>[]>(
        () =>
            columns.map((c) => ({
                id: c.name,
                accessorKey: c.name,
                header: c.label,
                enableSorting: c.sortable,
                cell: ({ getValue }) => {
                    const v = getValue();
                    const isNum = c.format === "euro" || c.format === "int" ||
                        c.format === "float" || c.format === "m2";
                    return (
                        <span
                            className={cn(
                                "block truncate",
                                isNum && "text-right tabular-nums",
                            )}
                            title={String(v ?? "")}
                        >
                            {formatValue(v, c.format)}
                        </span>
                    );
                },
            })),
        [columns],
    );

    const table = useReactTable({
        data: rows,
        columns: tableColumns,
        state: { sorting },
        onSortingChange: (updater) => {
            setSorting(updater);
            setPageIndex(0); // resetear a la primera página al cambiar el sort
        },
        manualSorting: true,
        manualPagination: true,
        getCoreRowModel: getCoreRowModel(),
    });

    const totalPages = Math.max(1, Math.ceil(total / pageSize));

    return (
        <div className="flex flex-col gap-3">
            {/* Barra de controles */}
            <div className="flex flex-wrap items-end gap-3">
                <label className="flex flex-col gap-1 text-xs text-slate-500">
                    Filtrar
                    <input
                        type="text"
                        value={q}
                        onChange={(e) => {
                            setQ(e.target.value);
                            setPageIndex(0);
                        }}
                        placeholder="Buscar (insensible a tildes/mayúsculas)…"
                        className="w-72 rounded-md border border-slate-300 bg-white px-2 py-1 text-sm focus:border-slate-500 focus:outline-none"
                    />
                </label>
                <label className="flex flex-col gap-1 text-xs text-slate-500">
                    Columna
                    <select
                        value={columnFilter}
                        onChange={(e) => {
                            setColumnFilter(e.target.value);
                            setPageIndex(0);
                        }}
                        className="rounded-md border border-slate-300 bg-white px-2 py-1 text-sm"
                    >
                        <option value="">— todas —</option>
                        {columns.map((c) => (
                            <option key={c.name} value={c.name}>
                                {c.label}
                            </option>
                        ))}
                    </select>
                </label>
                <div className="ml-auto text-xs text-slate-500">
                    {isLoading
                        ? "Cargando…"
                        : `${total.toLocaleString("es-ES")} filas`}
                </div>
            </div>

            {isError && (
                <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">
                    Error: {error instanceof Error ? error.message : String(error)}
                </div>
            )}

            {/* Tabla */}
            <div className="overflow-x-auto rounded-md border border-slate-200 bg-white">
                <table className="w-full text-sm">
                    <thead className="bg-slate-50">
                        {table.getHeaderGroups().map((hg) => (
                            <tr key={hg.id}>
                                {hg.headers.map((h) => {
                                    const sort = h.column.getIsSorted();
                                    return (
                                        <th
                                            key={h.id}
                                            className={cn(
                                                "border-b border-slate-200 px-3 py-2 text-left font-medium text-slate-700",
                                                h.column.getCanSort() &&
                                                    "cursor-pointer select-none hover:bg-slate-100",
                                            )}
                                            onClick={h.column.getToggleSortingHandler()}
                                        >
                                            <span className="inline-flex items-center gap-1">
                                                {flexRender(
                                                    h.column.columnDef.header,
                                                    h.getContext(),
                                                )}
                                                {sort === "asc" && (
                                                    <span className="text-slate-400">▲</span>
                                                )}
                                                {sort === "desc" && (
                                                    <span className="text-slate-400">▼</span>
                                                )}
                                            </span>
                                        </th>
                                    );
                                })}
                            </tr>
                        ))}
                    </thead>
                    <tbody>
                        {table.getRowModel().rows.map((row) => {
                            const id = String(row.original[rowKey] ?? "");
                            const selected = id === selectedKey;
                            return (
                                <tr
                                    key={id || row.id}
                                    className={cn(
                                        "cursor-pointer border-b border-slate-100 hover:bg-slate-50",
                                        selected && "bg-blue-50 hover:bg-blue-50",
                                    )}
                                    onClick={() => {
                                        setSelectedKey(id);
                                        onRowSelect?.(row.original);
                                    }}
                                >
                                    {row.getVisibleCells().map((cell) => (
                                        <td
                                            key={cell.id}
                                            className="max-w-[28ch] truncate px-3 py-1.5"
                                        >
                                            {flexRender(
                                                cell.column.columnDef.cell,
                                                cell.getContext(),
                                            )}
                                        </td>
                                    ))}
                                </tr>
                            );
                        })}
                        {!isLoading && rows.length === 0 && (
                            <tr>
                                <td
                                    colSpan={columns.length || 1}
                                    className="px-3 py-6 text-center text-sm text-slate-500"
                                >
                                    Sin resultados.
                                </td>
                            </tr>
                        )}
                    </tbody>
                </table>
            </div>

            {/* Paginación */}
            <div className="flex flex-wrap items-center gap-3 text-sm">
                <button
                    type="button"
                    onClick={() => setPageIndex(0)}
                    disabled={pageIndex === 0}
                    className="rounded-md border border-slate-300 px-2 py-1 disabled:opacity-40"
                >
                    «
                </button>
                <button
                    type="button"
                    onClick={() => setPageIndex((p) => Math.max(0, p - 1))}
                    disabled={pageIndex === 0}
                    className="rounded-md border border-slate-300 px-2 py-1 disabled:opacity-40"
                >
                    ‹
                </button>
                <span className="text-slate-600">
                    Página {pageIndex + 1} de {totalPages}
                </span>
                <button
                    type="button"
                    onClick={() => setPageIndex((p) => Math.min(totalPages - 1, p + 1))}
                    disabled={pageIndex >= totalPages - 1}
                    className="rounded-md border border-slate-300 px-2 py-1 disabled:opacity-40"
                >
                    ›
                </button>
                <button
                    type="button"
                    onClick={() => setPageIndex(totalPages - 1)}
                    disabled={pageIndex >= totalPages - 1}
                    className="rounded-md border border-slate-300 px-2 py-1 disabled:opacity-40"
                >
                    »
                </button>
                <label className="ml-auto flex items-center gap-2 text-slate-600">
                    Por página
                    <select
                        value={pageSize}
                        onChange={(e) => {
                            setPageSize(Number(e.target.value));
                            setPageIndex(0);
                        }}
                        className="rounded-md border border-slate-300 bg-white px-2 py-1"
                    >
                        {PAGE_SIZES.map((n) => (
                            <option key={n} value={n}>
                                {n}
                            </option>
                        ))}
                    </select>
                </label>
            </div>
        </div>
    );
}
