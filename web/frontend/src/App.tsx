import { BrowserRouter, Outlet, Route, Routes } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MainNav } from "@/components/MainNav";
import { Home } from "@/routes/Home";
import { PresupuestoUc } from "@/routes/PresupuestoUc";
import { CargosCategoria } from "@/routes/cargos/CategoriaPdiPvi";
import { CargosDepartamentos } from "@/routes/cargos/Departamentos";
import { SuperficiesResumen } from "@/routes/superficies/Resumen";
import { SuperficiesTotales } from "@/routes/superficies/Totales";
import { SuperficiesPresencia } from "@/routes/superficies/PresenciaCentros";
import { Entradas } from "@/routes/Entradas";

const queryClient = new QueryClient({
    defaultOptions: {
        queries: { retry: false },
    },
});

function Layout() {
    return (
        <div className="grid min-h-screen grid-cols-[16rem_1fr] bg-slate-50 text-slate-900">
            <aside className="border-r border-slate-200 bg-white p-4">
                <div className="mb-4 px-2">
                    <div className="text-sm font-semibold">CoAna</div>
                    <div className="text-xs text-slate-500">gemelo web</div>
                </div>
                <MainNav />
            </aside>
            <main className="overflow-x-auto p-6">
                <Outlet />
            </main>
        </div>
    );
}

export function App() {
    return (
        <QueryClientProvider client={queryClient}>
            <BrowserRouter>
                <Routes>
                    <Route element={<Layout />}>
                        <Route index element={<Home />} />
                        <Route path="entradas" element={<Entradas />} />
                        <Route path="presupuesto/uc" element={<PresupuestoUc />} />
                        <Route path="cargos/categoria" element={<CargosCategoria />} />
                        <Route path="cargos/departamentos" element={<CargosDepartamentos />} />
                        <Route path="superficies/resumen" element={<SuperficiesResumen />} />
                        <Route path="superficies/totales" element={<SuperficiesTotales />} />
                        <Route path="superficies/presencia" element={<SuperficiesPresencia />} />
                    </Route>
                </Routes>
            </BrowserRouter>
        </QueryClientProvider>
    );
}
