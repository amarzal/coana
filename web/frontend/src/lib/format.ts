/**
 * Formateadores comunes alineados con la convención del visor Streamlit:
 * importes en notación europea (1.234,56 €), m² con dos decimales, etc.
 *
 * Tomamos como locale 'es-ES' por consistencia con el resto del proyecto.
 */

// useGrouping=true + minimumGroupingDigits=1 fuerza el separador de
// miles incluso para números de 4 dígitos (1.234,56 en vez del
// 1234,56 que es el default del CLDR es-ES, que solo agrupa a partir
// de 10000).
const _OPTS: Intl.NumberFormatOptions = {
    useGrouping: true,
    minimumGroupingDigits: 1,
};

const numEs = new Intl.NumberFormat("es-ES", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
    ..._OPTS,
});

const numEsInt = new Intl.NumberFormat("es-ES", _OPTS);

/** "1.234,56 €". Devuelve cadena vacía si el valor no es finito. */
export function formatEuro(value: unknown): string {
    if (typeof value !== "number" || !Number.isFinite(value)) return "";
    return `${numEs.format(value)} €`;
}

/** "1.234,56 m²". */
export function formatM2(value: unknown): string {
    if (typeof value !== "number" || !Number.isFinite(value)) return "";
    return `${numEs.format(value)} m²`;
}

/** Entero con separador de miles ("1.234"). */
export function formatInt(value: unknown): string {
    if (typeof value !== "number" || !Number.isFinite(value)) return "";
    return numEsInt.format(value);
}

/** Float con dos decimales ("1.234,56"). */
export function formatFloat(value: unknown): string {
    if (typeof value !== "number" || !Number.isFinite(value)) return "";
    return numEs.format(value);
}

/** Convierte un valor a texto vacío→"—", booleano→sí/no, resto→String. */
export function formatText(value: unknown): string {
    if (value === null || value === undefined || value === "") return "—";
    if (typeof value === "boolean") return value ? "sí" : "no";
    return String(value);
}

export type ColumnFormat =
    | "text"
    | "int"
    | "float"
    | "euro"
    | "m2"
    | "date"
    | "bool";

/** Aplica el formateador adecuado según el tipo declarado en ColumnSpec. */
export function formatValue(value: unknown, fmt: ColumnFormat): string {
    switch (fmt) {
        case "euro":
            return formatEuro(value);
        case "m2":
            return formatM2(value);
        case "int":
            return formatInt(value);
        case "float":
            return formatFloat(value);
        case "bool":
            return typeof value === "boolean" ? (value ? "sí" : "no") : formatText(value);
        case "date":
        case "text":
        default:
            return formatText(value);
    }
}
