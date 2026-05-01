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
import {
    AmortResumen,
    AmortEnriquecido,
    AmortFiltradosEstado,
    AmortFiltradosCuenta,
    AmortFiltradosFecha,
    AmortSinCuenta,
    AmortSinFechaAlta,
    AmortUc,
    AmortSinCentro,
} from "@/routes/amortizaciones";
import {
    PersonalResumen,
    ExpedientesPDI,
    ExpedientesPTGAS,
    ExpedientesPVI,
    ExpedientesOtros,
    PersonalMultiexpediente,
    PersonalPersona,
    PersonalAnomaliasPdi,
} from "@/routes/personal";
import {
    Regla23DedicacionDocente,
    Regla23DocenciaNoOficial,
    Regla23EstructuraEstudios,
    Regla23BolsaAtrasos,
    Regla23ExpedientesApartados,
    Regla23Despidos,
    Regla23IndemnizacionesAsistencias,
    Regla23Cargos,
    Regla23AsignaturasSinTitulacion,
    Regla23Anomalias,
} from "@/routes/regla23";
import {
    ResultadosResumen,
    ResultadosTodasUc,
    ResultadosActividades,
    ResultadosCentros,
    ResultadosElementos,
    ResultadosAnomalias,
} from "@/routes/resultados";

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
                        <Route path="amortizaciones/resumen" element={<AmortResumen />} />
                        <Route path="amortizaciones/enriquecido" element={<AmortEnriquecido />} />
                        <Route path="amortizaciones/filtrados-estado" element={<AmortFiltradosEstado />} />
                        <Route path="amortizaciones/filtrados-cuenta" element={<AmortFiltradosCuenta />} />
                        <Route path="amortizaciones/filtrados-fecha" element={<AmortFiltradosFecha />} />
                        <Route path="amortizaciones/sin-cuenta" element={<AmortSinCuenta />} />
                        <Route path="amortizaciones/sin-fecha-alta" element={<AmortSinFechaAlta />} />
                        <Route path="amortizaciones/uc" element={<AmortUc />} />
                        <Route path="amortizaciones/sin-centro" element={<AmortSinCentro />} />
                        <Route path="personal/resumen" element={<PersonalResumen />} />
                        <Route path="personal/pdi" element={<ExpedientesPDI />} />
                        <Route path="personal/ptgas" element={<ExpedientesPTGAS />} />
                        <Route path="personal/pvi" element={<ExpedientesPVI />} />
                        <Route path="personal/otros" element={<ExpedientesOtros />} />
                        <Route path="personal/multiexpediente" element={<PersonalMultiexpediente />} />
                        <Route path="personal/persona" element={<PersonalPersona />} />
                        <Route path="personal/anomalias" element={<PersonalAnomaliasPdi />} />
                        <Route path="regla23/dedicacion" element={<Regla23DedicacionDocente />} />
                        <Route path="regla23/no-oficial" element={<Regla23DocenciaNoOficial />} />
                        <Route path="regla23/estructura" element={<Regla23EstructuraEstudios />} />
                        <Route path="regla23/atrasos" element={<Regla23BolsaAtrasos />} />
                        <Route path="regla23/despidos" element={<Regla23Despidos />} />
                        <Route path="regla23/indemnizaciones" element={<Regla23IndemnizacionesAsistencias />} />
                        <Route path="regla23/cargos" element={<Regla23Cargos />} />
                        <Route path="regla23/apartados" element={<Regla23ExpedientesApartados />} />
                        <Route path="regla23/sin-titulacion" element={<Regla23AsignaturasSinTitulacion />} />
                        <Route path="regla23/anomalias" element={<Regla23Anomalias />} />
                        <Route path="resultados/resumen" element={<ResultadosResumen />} />
                        <Route path="resultados/uc" element={<ResultadosTodasUc />} />
                        <Route path="resultados/actividades" element={<ResultadosActividades />} />
                        <Route path="resultados/centros" element={<ResultadosCentros />} />
                        <Route path="resultados/elementos" element={<ResultadosElementos />} />
                        <Route path="resultados/anomalias" element={<ResultadosAnomalias />} />
                    </Route>
                </Routes>
            </BrowserRouter>
        </QueryClientProvider>
    );
}
