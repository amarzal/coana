from coana.configuración import Configuración


def carga_ámbitos_de_conocimiento(cfg: Configuración) -> dict[str, str]:
    df = cfg.fichero('ámbitos-de-conocimiento').carga_dataframe()
    ámbito = {}
    for row in df.iter_rows(named=True):
        ámbito[row['código']] = row['nombre']
    return ámbito
