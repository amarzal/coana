import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "node:path";

// El backend FastAPI corre por defecto en :8765 (ver `coana web` CLI).
// En desarrollo, Vite lo proxia bajo /api y /events para que el cliente
// hable con un solo origen.
const BACKEND = process.env.COANA_BACKEND ?? "http://127.0.0.1:8765";

export default defineConfig({
    plugins: [react(), tailwindcss()],
    resolve: {
        alias: {
            "@": path.resolve(__dirname, "src"),
        },
    },
    // El build se emite directamente al directorio que FastAPI sirve
    // como estático en producción.
    build: {
        outDir: path.resolve(__dirname, "../../coana/web/dist"),
        emptyOutDir: true,
    },
    server: {
        port: 5173,
        proxy: {
            "/api": { target: BACKEND, changeOrigin: true },
            "/events": { target: BACKEND, changeOrigin: true, ws: true },
        },
    },
});
