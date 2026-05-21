import { Terminal } from "lucide-react";
import { terminalStore, useTerminalStore } from "@/lib/terminalStore";
import { cn } from "@/lib/cn";

/** Botón de icono que muestra/oculta el panel terminal global. */
export function TerminalToggle() {
    const { shown, status, lines } = useTerminalStore();
    const hayContenido = lines.length > 0 || status !== "idle";
    return (
        <button
            type="button"
            onClick={() => terminalStore.toggleShown()}
            disabled={!hayContenido}
            title={
                !hayContenido
                    ? "No hay log que mostrar"
                    : shown
                        ? "Ocultar terminal"
                        : "Mostrar terminal"
            }
            aria-label={shown ? "Ocultar terminal" : "Mostrar terminal"}
            className={cn(
                "flex h-8 w-8 items-center justify-center rounded-md border",
                hayContenido
                    ? shown
                        ? "border-slate-700 bg-slate-800 text-white hover:bg-slate-700"
                        : "border-slate-300 bg-white text-slate-700 hover:bg-slate-100"
                    : "border-slate-200 bg-slate-50 text-slate-300",
            )}
        >
            <Terminal size={16} aria-hidden="true" />
        </button>
    );
}
