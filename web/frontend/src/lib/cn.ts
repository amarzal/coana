/**
 * Mini util para concatenar clases CSS condicionalmente.
 * Aceptamos string | false | null | undefined; descartamos vacíos y unimos
 * con un espacio.
 */
export function cn(...parts: (string | false | null | undefined)[]): string {
    return parts.filter(Boolean).join(" ");
}
