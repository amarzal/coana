import { BrowserRouter, Navigate, Outlet, Route, Routes } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MainNav } from "@/components/MainNav";
import { EjecutarFase1 } from "@/components/EjecutarFase1";
import { StatusFooter } from "@/components/StatusFooter";
import {
    PresupuestoResumen,
    PresupuestoUc,
    PresupuestoSinClasificar,
    PresupuestoFiltrados,
    PresupuestoSuministros,
    PresupuestoDistribucionOTOP,
    PresupuestoReglasActividad,
    PresupuestoReglasCC,
    PresupuestoReglasEC,
    PresupuestoArbolActividades,
    PresupuestoArbolCentros,
    PresupuestoArbolElementos,
} from "@/routes/presupuesto";
import { CargosCargos } from "@/routes/cargos/Cargos";
import { CargosPersonasCargos } from "@/routes/cargos/PersonasCargos";
import { CargosPersonasRemuneradas } from "@/routes/cargos/PersonasRemuneradas";
import { CargosResumen } from "@/routes/cargos/Resumen";
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
    PersonalCostesSocialesCalculados,
    PersonalAtrasosNoVinculados,
} from "@/routes/personal";
import { PersonaPdi, PersonaPvi } from "@/routes/personal/PersonaPdiPvi";
import {
    Regla23Resumen,
    Regla23DedicacionDocente,
    Regla23DocenciaNoOficial,
    Regla23EstructuraEstudios,
    Regla23Despidos,
    Regla23IndemnizacionesAsistencias,
    Regla23Cargos,
    Regla23AsignaturasSinTitulacion,
    Regla23Anomalias,
} from "@/routes/regla23";
import { InvestigacionGrupos } from "@/routes/investigacion";
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

function HomePlaceholder() {
    return (
        <div className="text-sm text-slate-500">
            Selecciona una sección del menú lateral.
        </div>
    );
}

function Layout() {
    return (
        <div className="grid h-screen grid-cols-[16rem_1fr] overflow-hidden bg-slate-50 text-slate-900">
            <aside className="flex flex-col gap-4 overflow-y-auto border-r border-slate-200 bg-white p-4">
                <div className="px-2">
                    <div className="text-sm font-semibold">CoAna</div>
                    <div className="text-xs text-slate-500">UJI 2025</div>
                </div>
                <EjecutarFase1 />
                <MainNav />
                <StatusFooter />
            </aside>
            <main className="overflow-auto p-6">
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
                        <Route index element={<HomePlaceholder />} />
                        <Route path="entradas/*" element={<Entradas />} />
                        <Route path="presupuesto/resumen" element={<PresupuestoResumen />} />
                        <Route path="presupuesto/uc" element={<PresupuestoUc />} />
                        <Route path="presupuesto/sin-clasificar" element={<PresupuestoSinClasificar />} />
                        <Route path="presupuesto/filtrados" element={<PresupuestoFiltrados />} />
                        <Route path="presupuesto/suministros" element={<PresupuestoSuministros />} />
                        <Route path="presupuesto/otop" element={<PresupuestoDistribucionOTOP />} />
                        <Route path="presupuesto/reglas/actividad" element={<PresupuestoReglasActividad />} />
                        <Route path="presupuesto/reglas/cc" element={<PresupuestoReglasCC />} />
                        <Route path="presupuesto/reglas/ec" element={<PresupuestoReglasEC />} />
                        <Route path="presupuesto/arbol/actividades" element={<PresupuestoArbolActividades />} />
                        <Route path="presupuesto/arbol/cc" element={<PresupuestoArbolCentros />} />
                        <Route path="presupuesto/arbol/ec" element={<PresupuestoArbolElementos />} />
                        <Route path="cargos/resumen" element={<CargosResumen />} />
                        <Route path="cargos/personas-cargos" element={<CargosPersonasCargos />} />
                        <Route path="cargos/personas-remuneradas" element={<CargosPersonasRemuneradas />} />
                        <Route path="cargos/cargos" element={<CargosCargos />} />
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
                        <Route path="personal/pdi" element={<PersonaPdi />} />
                        <Route path="personal/pvi" element={<PersonaPvi />} />
                        <Route path="personal/pdi-expedientes" element={<ExpedientesPDI />} />
                        <Route path="personal/pvi-expedientes" element={<ExpedientesPVI />} />
                        <Route path="personal/ptgas" element={<ExpedientesPTGAS />} />
                        <Route path="personal/otros" element={<ExpedientesOtros />} />
                        <Route path="personal/multiexpediente" element={<PersonalMultiexpediente />} />
                        <Route path="personal/persona" element={<PersonalPersona />} />
                        <Route path="personal/atrasos-no-vinculados" element={<PersonalAtrasosNoVinculados />} />
                    <Route path="personal/costes-sociales-calculados" element={<PersonalCostesSocialesCalculados />} />
                        <Route path="personal/anomalias" element={<PersonalAnomaliasPdi />} />
                        <Route path="personal/despidos" element={<Regla23Despidos />} />
                        <Route path="personal/indemnizaciones" element={<Regla23IndemnizacionesAsistencias />} />
                        <Route path="regla23/resumen" element={<Regla23Resumen />} />
                        <Route path="regla23/dedicacion-pdi" element={<Navigate to="/personal/pdi" replace />} />
                        <Route path="regla23/dedicacion" element={<Regla23DedicacionDocente />} />
                        <Route path="regla23/no-oficial" element={<Regla23DocenciaNoOficial />} />
                        <Route path="regla23/estructura" element={<Regla23EstructuraEstudios />} />
                        <Route path="regla23/cargos" element={<Regla23Cargos />} />
                        <Route path="regla23/sin-titulacion" element={<Regla23AsignaturasSinTitulacion />} />
                        <Route path="regla23/anomalias" element={<Regla23Anomalias />} />
                        <Route path="investigacion/grupos" element={<InvestigacionGrupos />} />
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
