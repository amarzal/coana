import { NavLink } from "react-router";
import { cn } from "@/lib/cn";

/** Encabezado de página: título + subtítulo. */
export function Cabecera({ title, subtitle }: { title: string; subtitle?: string }) {
    return (
        <div>
            <h1 className="text-2xl font-semibold">{title}</h1>
            {subtitle && <p className="text-sm text-slate-500">{subtitle}</p>}
        </div>
    );
}

/** Caja grande con un total destacado. */
export function CajaTotal({
    etiqueta, valor, sub,
}: { etiqueta: string; valor: string; sub?: string }) {
    return (
        <div className="rounded-lg border border-slate-200 bg-white px-5 py-4 shadow-sm">
            <div className="text-xs uppercase tracking-wide text-slate-500">{etiqueta}</div>
            <div className="mt-1 text-3xl font-semibold tabular-nums text-slate-900">{valor}</div>
            {sub && <div className="mt-1 text-xs text-slate-500">{sub}</div>}
        </div>
    );
}

/** Fila con etiqueta a la izquierda, barra de progreso y dos métricas a la derecha. */
export function FilaBarra({
    etiqueta, importe, total, color, extra,
}: {
    etiqueta: string;
    importe: number;
    total: number;
    color: string;
    /** Texto adicional a la derecha (p. ej. "1.234 UC"). */
    extra?: string;
}) {
    const pct = total > 0 ? (100 * importe) / total : 0;
    return (
        <div className="flex items-center gap-3 py-1.5">
            <div className="w-44 text-sm font-medium text-slate-700">{etiqueta}</div>
            <div className="flex-1">
                <div className="relative h-2 overflow-hidden rounded-full bg-slate-100">
                    <div
                        className={cn("absolute inset-y-0 left-0", color)}
                        style={{ width: `${Math.max(0, Math.min(pct, 100)).toFixed(2)}%` }}
                    />
                </div>
            </div>
            <div className="w-28 text-right text-sm font-semibold tabular-nums text-slate-900">
                {importe.toLocaleString("es-ES", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            </div>
            <div className="w-16 text-right text-xs tabular-nums text-slate-500">
                {pct.toFixed(1)} %
            </div>
            {extra !== undefined && (
                <div className="w-28 text-right text-xs tabular-nums text-slate-500">{extra}</div>
            )}
        </div>
    );
}

/** Atajo de navegación (tarjeta clicable). */
export function Atajo({
    to, título, descripción,
}: { to: string; título: string; descripción: string }) {
    return (
        <NavLink
            to={to}
            className="block rounded-md border border-slate-200 bg-white px-3 py-2 hover:border-slate-400 hover:bg-slate-50"
        >
            <div className="text-sm font-medium text-slate-800">{título}</div>
            <div className="text-xs text-slate-500">{descripción}</div>
        </NavLink>
    );
}

/** Título de sección uniforme: uppercase pequeño y gris. */
export function SeccionTitulo({ children }: { children: React.ReactNode }) {
    return (
        <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">
            {children}
        </h2>
    );
}
