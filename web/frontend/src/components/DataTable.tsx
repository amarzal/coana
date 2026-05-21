import { useEffect, useMemo, useState } from "react";
import { useQuery, keepPreviousData } from "@tanstack/react-query";
import {
    flexRender,
    getCoreRowModel,
    useReactTable,
    type ColumnDef,
    type SortingState,
} from "@tanstack/react-table";
import { Funnel } from "lucide-react";
import { api } from "@/api/client";
import { cn } from "@/lib/cn";
import { formatValue, type ColumnFormat } from "@/lib/format";
import { RowDetailPopover } from "@/components/RowDetailPopover";

type ColumnSpec = {
    name: string;
    label: string;
    format: ColumnFormat;
    sortable: boolean;
};

type ColumnStats = {
    total: number;
    count: number;
    min: number | null;
    max: number | null;
    bins: number[];
};

type ListResponse = {
    columns: ColumnSpec[];
    rows: Record<string, unknown>[];
    total: number;
    column_stats?: Record<string, ColumnStats>;
};

function isStatFormat(fmt: ColumnFormat): boolean {
    // Mostramos total + histograma para columnas que representan
    // cantidades. `id` queda fuera (los enteros del visor genérico de
    // xlsx se sirven como id por defecto).
    return fmt === "euro" || fmt === "m2" || fmt === "float" || fmt === "int";
}

/** Sparkline SVG con 20 bins; ocupa 84×16 px. */
function Sparkline({ bins }: { bins: number[] }) {
    if (bins.length === 0) return null;
    const max = Math.max(...bins, 1);
    const W = 84;
    const H = 16;
    const barW = W / bins.length;
    return (
        <svg
            viewBox={`0 0 ${W} ${H}`}
            width={W}
            height={H}
            preserveAspectRatio="none"
            className="block"
            aria-hidden="true"
        >
            {bins.map((v, i) => {
                const h = (v / max) * H;
                return (
                    <rect
                        key={i}
                        x={i * barW + 0.5}
                        y={H - h}
                        width={Math.max(0, barW - 1)}
                        height={h}
                        className="fill-slate-400"
                    />
                );
            })}
        </svg>
    );
}

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
    /** Parámetros de query adicionales que se mezclan con los estándar. */
    extraParams?: Record<string, string | number | boolean | undefined>;
    /**
     * Si es true, al pinchar una fila se abre un popover con todos los
     * campos formateados. Útil para tablas genéricas (visor de xlsx)
     * que no tienen un endpoint de ficha enriquecida.
     */
    showPopoverOnRowClick?: boolean;
    /**
     * Por defecto, las columnas euro/m²/float se traen a la izquierda
     * justo detrás de la primera columna. Si la vista quiere mantener
     * el orden tal y como llega del backend, pasar `false`.
     */
    reorderImportes?: boolean;
    /**
     * Función opcional que recibe la fila y devuelve clases CSS extra
     * para esa fila (p. ej. negrita o cursiva según un campo de rol).
     */
    rowClassName?: (row: Record<string, unknown>) => string | undefined;
};

const PAGE_SIZES = [10, 25, 50, 100, 250, 500];

export function DataTable({
    endpoint,
    queryKey,
    onRowSelect,
    rowKey = "id",
    pageSize: initialPageSize = 10,
    extraParams,
    showPopoverOnRowClick = false,
    reorderImportes = true,
    rowClassName,
}: Props) {
    const [q, setQ] = useState("");
    const [columnFilter, setColumnFilter] = useState<string>(""); // "" = todas
    const [sorting, setSorting] = useState<SortingState>([]);
    const [pageSize, setPageSize] = useState(initialPageSize);
    const [pageIndex, setPageIndex] = useState(0);
    const [selectedKey, setSelectedKey] = useState<string | null>(null);
    const [popoverRow, setPopoverRow] = useState<Record<string, unknown> | null>(null);

    // Si cambia el endpoint o los parámetros extra (otra ruta, otro
    // fichero en el visor xlsx, otra persona en sub-tablas, etc.), la
    // tabla pasa a una colección distinta: no tiene sentido mantener
    // el slider/paginación donde estaba — podríamos quedar fuera de
    // rango. Reseteamos a la página 1 y limpiamos la selección.
    const extraParamsKey = JSON.stringify(extraParams ?? {});
    useEffect(() => {
        setPageIndex(0);
        setSelectedKey(null);
    }, [endpoint, extraParamsKey]);

    const sortBy = sorting[0]?.id;
    const desc = sorting[0]?.desc ?? false;

    const params = {
        ...(extraParams ?? {}),
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
        placeholderData: keepPreviousData,
    });

    const rawColumns: ColumnSpec[] = data?.columns ?? [];
    const rows: Record<string, unknown>[] = data?.rows ?? [];
    const total = data?.total ?? 0;
    const columnStats: Record<string, ColumnStats> = data?.column_stats ?? {};

    // Los importes (euro/m²/float) son los datos más informativos; los
    // traemos a la izquierda — justo detrás de la primera columna (que
    // suele ser el identificador/clave). `int` se queda donde está
    // porque suele ser un contador (n_uc, n_personas…), no una cantidad.
    // Para tablas sin importes no cambia nada. La vista puede pedir
    // que respete el orden del backend con `reorderImportes={false}`.
    const columns = useMemo<ColumnSpec[]>(() => {
        if (!reorderImportes || rawColumns.length <= 1) return rawColumns;
        const isImporte = (f: ColumnFormat) =>
            f === "euro" || f === "m2" || f === "float";
        const head = rawColumns[0];
        const importes = rawColumns.slice(1).filter((c) => isImporte(c.format));
        if (importes.length === 0) return rawColumns;
        const resto = rawColumns.slice(1).filter((c) => !isImporte(c.format));
        return [head, ...importes, ...resto];
    }, [rawColumns, reorderImportes]);

    // Una fila de stats (total + sparkline) solo si alguna columna numérica
    // mostrable tiene datos. En el visor genérico (entradas xlsx) los
    // enteros se sirven como "id" y por tanto no entran aquí.
    const statColumns = columns.filter(
        (c) => isStatFormat(c.format) && columnStats[c.name]?.count,
    );
    const showStatsRow = statColumns.length > 0;

    const tableColumns = useMemo<ColumnDef<Record<string, unknown>>[]>(
        () =>
            columns.map((c) => ({
                id: c.name,
                accessorKey: c.name,
                header: c.label,
                enableSorting: c.sortable,
                cell: ({ getValue }) => {
                    const v = getValue();
                    // `id` es un entero pero se trata como cadena
                    // identificadora: alineado a la izquierda, sin
                    // tabular-nums.
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
                    <span className="inline-flex items-center gap-1">
                        <Funnel size={11} strokeWidth={2.25} aria-hidden="true" />
                        Filtrar
                    </span>
                    <input
                        type="text"
                        value={q}
                        onChange={(e) => {
                            setQ(e.target.value);
                            setPageIndex(0);
                        }}
                        placeholder={
                            columnFilter
                                ? `Buscar en «${
                                    columns.find((c) => c.name === columnFilter)?.label ?? columnFilter
                                }»`
                                : "Buscar en todas las columnas (insensible a tildes/mayúsculas)…"
                        }
                        className="w-96 rounded-md border border-slate-300 bg-white px-2 py-1 text-sm focus:border-slate-500 focus:outline-none"
                    />
                </label>
                <div className="ml-auto text-xs text-slate-500">
                    {columnFilter && (
                        <span className="mr-3">
                            ámbito: <span className="font-mono">{columnFilter}</span>{" "}
                            <button
                                type="button"
                                onClick={() => setColumnFilter("")}
                                className="text-slate-400 hover:text-slate-700"
                                title="Buscar en todas las columnas"
                            >
                                ✕
                            </button>
                        </span>
                    )}
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
                <table className="w-full text-xs">
                    <thead className="bg-slate-50">
                        {table.getHeaderGroups().map((hg) => (
                            <tr key={hg.id}>
                                {hg.headers.map((h) => {
                                    const sort = h.column.getIsSorted();
                                    const colName = h.column.id;
                                    const filtroActivo = columnFilter === colName;
                                    const colSpec = columns.find((c) => c.name === colName);
                                    const isNum = colSpec
                                        ? (colSpec.format === "euro" || colSpec.format === "int"
                                            || colSpec.format === "float" || colSpec.format === "m2")
                                        : false;
                                    return (
                                        <th
                                            key={h.id}
                                            className={cn(
                                                "border-b border-slate-200 px-2 py-1.5 font-medium text-slate-700",
                                                isNum ? "text-right" : "text-left",
                                                h.column.getCanSort() &&
                                                    "cursor-pointer select-none hover:bg-slate-100",
                                            )}
                                            onClick={h.column.getToggleSortingHandler()}
                                        >
                                            <span
                                                className={cn(
                                                    "inline-flex items-center gap-1",
                                                    isNum && "flex-row-reverse",
                                                )}
                                            >
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
                                                <button
                                                    type="button"
                                                    onClick={(e) => {
                                                        e.stopPropagation();
                                                        setColumnFilter((prev) =>
                                                            prev === colName ? "" : colName,
                                                        );
                                                        setPageIndex(0);
                                                    }}
                                                    title={
                                                        filtroActivo
                                                            ? "Quitar ámbito de columna (filtro en todas)"
                                                            : `Filtrar solo en «${
                                                                columns.find((c) => c.name === colName)?.label ?? colName
                                                            }»`
                                                    }
                                                    aria-label="Filtrar por esta columna"
                                                    className={cn(
                                                        "inline-flex items-center rounded p-0.5",
                                                        filtroActivo
                                                            ? "bg-slate-200 text-slate-800"
                                                            : "text-slate-300 hover:bg-slate-100 hover:text-slate-600",
                                                    )}
                                                >
                                                    <Funnel size={11} strokeWidth={2.25} />
                                                </button>
                                            </span>
                                        </th>
                                    );
                                })}
                            </tr>
                        ))}
                        {showStatsRow && (
                            <tr className="bg-slate-50/60">
                                {columns.map((c) => {
                                    const s = columnStats[c.name];
                                    if (!s || !s.count || !isStatFormat(c.format)) {
                                        return (
                                            <th
                                                key={`stat-${c.name}`}
                                                className="border-b border-slate-200 px-2 py-1"
                                            />
                                        );
                                    }
                                    return (
                                        <th
                                            key={`stat-${c.name}`}
                                            className="border-b border-slate-200 px-2 py-1 text-right align-top font-normal"
                                            title={
                                                `Total: ${formatValue(s.total, c.format)}\n` +
                                                `Min: ${formatValue(s.min, c.format)} · ` +
                                                `Max: ${formatValue(s.max, c.format)}`
                                            }
                                        >
                                            <div className="flex flex-col items-end gap-0.5">
                                                <span className="tabular-nums text-[11px] text-slate-700">
                                                    Σ {formatValue(s.total, c.format)}
                                                </span>
                                                <Sparkline bins={s.bins} />
                                            </div>
                                        </th>
                                    );
                                })}
                            </tr>
                        )}
                    </thead>
                    <tbody>
                        {table.getRowModel().rows.map((row) => {
                            const id = String(row.original[rowKey] ?? "");
                            const selected = id === selectedKey;
                            const extraClass = rowClassName?.(row.original);
                            return (
                                <tr
                                    key={id || row.id}
                                    className={cn(
                                        "cursor-pointer border-b border-slate-100 hover:bg-slate-50",
                                        selected && "bg-blue-50 hover:bg-blue-50",
                                        extraClass,
                                    )}
                                    onClick={() => {
                                        setSelectedKey(id);
                                        if (showPopoverOnRowClick) {
                                            setPopoverRow(row.original);
                                        }
                                        onRowSelect?.(row.original);
                                    }}
                                >
                                    {row.getVisibleCells().map((cell) => (
                                        <td
                                            key={cell.id}
                                            className="max-w-[28ch] truncate px-2 py-1"
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

            {/* Slider rápido (oculto si solo hay una página) */}
            {totalPages > 1 && (
                <input
                    type="range"
                    min={0}
                    max={totalPages - 1}
                    value={pageIndex}
                    onChange={(e) => setPageIndex(Number(e.target.value))}
                    aria-label={`Página ${pageIndex + 1} de ${totalPages}`}
                    className="w-full accent-slate-700"
                />
            )}

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

            {popoverRow && (
                <RowDetailPopover
                    row={popoverRow}
                    columns={columns}
                    onClose={() => setPopoverRow(null)}
                />
            )}
        </div>
    );
}
