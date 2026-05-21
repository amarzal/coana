/**
 * Store global del «terminal» de logs del visor.
 *
 * Comparte estado entre las dos acciones de ejecución (Cálculo de
 * unidades de coste y Generar informes) y un único panel inferior:
 * - `start({kind, jobId})` arranca un nuevo log; muestra el panel y
 *   programa un auto-hide a 5 s (que se cancela si el usuario
 *   interactúa con el toggle).
 * - `pushLine` / `pushLines` añaden líneas durante el job.
 * - `setDone` / `setError` cambian el estado final.
 * - `toggleShown` / `setShown` controlan la visibilidad explícita por
 *   el botón del sidebar; cancelan el auto-hide.
 */

import { useSyncExternalStore } from "react";

export type Status = "idle" | "running" | "done" | "error";
export type Kind = "fase1" | "informes" | null;

export type TerminalState = {
    lines: string[];
    status: Status;
    error: string | null;
    jobId: string | null;
    kind: Kind;
    shown: boolean;
};

const _initial: TerminalState = {
    lines: [],
    status: "idle",
    error: null,
    jobId: null,
    kind: null,
    shown: false,
};
let _state: TerminalState = _initial;
const _listeners = new Set<() => void>();
let _hideTimer: number | null = null;
const AUTO_HIDE_MS = 5000;

function _emit() {
    _listeners.forEach((fn) => fn());
}

function _cancelHide() {
    if (_hideTimer !== null) {
        window.clearTimeout(_hideTimer);
        _hideTimer = null;
    }
}

function _programaAutoHide() {
    _cancelHide();
    _hideTimer = window.setTimeout(() => {
        _state = { ..._state, shown: false };
        _hideTimer = null;
        _emit();
    }, AUTO_HIDE_MS);
}

export const terminalStore = {
    subscribe(fn: () => void): () => void {
        _listeners.add(fn);
        return () => _listeners.delete(fn);
    },
    getSnapshot(): TerminalState {
        return _state;
    },
    start(args: { kind: Exclude<Kind, null>; jobId: string | null }): void {
        // Conservamos el histórico acumulado y añadimos una cabecera
        // separadora al inicio de cada ejecución.
        const titulos: Record<Exclude<Kind, null>, string> = {
            fase1: "Cálculo de unidades de coste",
            informes: "Generar informes",
        };
        const ahora = new Date();
        const hh = String(ahora.getHours()).padStart(2, "0");
        const mm = String(ahora.getMinutes()).padStart(2, "0");
        const ss = String(ahora.getSeconds()).padStart(2, "0");
        const sep = "─".repeat(80);
        const cabecera =
            _state.lines.length > 0
                ? ["", sep, `▶ ${titulos[args.kind]} · ${hh}:${mm}:${ss}`, sep]
                : [sep, `▶ ${titulos[args.kind]} · ${hh}:${mm}:${ss}`, sep];
        _state = {
            lines: [..._state.lines, ...cabecera],
            status: "running",
            error: null,
            jobId: args.jobId,
            kind: args.kind,
            shown: true,
        };
        _emit();
        _programaAutoHide();
    },
    pushLine(line: string): void {
        _state = { ..._state, lines: [..._state.lines, line] };
        _emit();
    },
    pushLines(lines: string[]): void {
        if (lines.length === 0) return;
        _state = { ..._state, lines: [..._state.lines, ...lines] };
        _emit();
    },
    setDone(): void {
        _state = { ..._state, status: "done" };
        _emit();
    },
    setError(err: string): void {
        _state = { ..._state, status: "error", error: err };
        _emit();
    },
    toggleShown(): void {
        _cancelHide();
        _state = { ..._state, shown: !_state.shown };
        _emit();
    },
    setShown(v: boolean): void {
        _cancelHide();
        _state = { ..._state, shown: v };
        _emit();
    },
};

export function useTerminalStore(): TerminalState {
    return useSyncExternalStore(terminalStore.subscribe, terminalStore.getSnapshot);
}
