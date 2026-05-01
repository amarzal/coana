import { BrowserRouter, Outlet, Route, Routes } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MainNav } from "@/components/MainNav";
import { Home } from "@/routes/Home";
import { PresupuestoUc } from "@/routes/PresupuestoUc";

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
                        <Route path="presupuesto/uc" element={<PresupuestoUc />} />
                    </Route>
                </Routes>
            </BrowserRouter>
        </QueryClientProvider>
    );
}
