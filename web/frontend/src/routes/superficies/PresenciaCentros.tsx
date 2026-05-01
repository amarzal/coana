export function SuperficiesPresencia() {
    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-2xl font-semibold">
                    Superficies · Presencia centros
                </h1>
                <p className="text-sm text-slate-500">
                    Matriz centro de coste × espacio físico (zonas, edificaciones,
                    complejos).
                </p>
            </div>

            <div className="rounded-md border border-amber-200 bg-amber-50 p-4 text-sm text-amber-900">
                <strong>Pendiente.</strong> Esta vista necesita las matrices de
                presencia que el procesamiento de inventario calcula al ejecutar
                la fase 1, pero que actualmente no se persisten en disco. Se
                materializará cuando el botón «Ejecutar fase 1» del visor esté
                operativo (Fase 5 del plan de migración) o se persistan las
                matrices a parquet en la fase 1.
            </div>
        </div>
    );
}
