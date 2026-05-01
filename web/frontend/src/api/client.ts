import createClient from "openapi-fetch";
import type { paths } from "./schema";

/**
 * Cliente HTTP tipado contra la API de CoAna.
 *
 * En desarrollo Vite proxia /api al backend en :8765; en producción
 * frontend y backend comparten origen. Ambos casos funcionan con baseUrl
 * vacío (rutas relativas).
 */
export const api = createClient<paths>({ baseUrl: "" });
