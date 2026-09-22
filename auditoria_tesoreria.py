"""
auditoria_tesoreria.py — Auditoría independiente de la conciliación

No decide si algo matchea o no: eso ya lo resolvió logica_tesoreria.py.
Este módulo toma el resultado ya cerrado y lo interroga desde afuera,
buscando lo que la propia lógica de matching no está en condiciones de
ver sobre sí misma:

  1. Reconciliación de montos: que la plata, no sólo la cantidad de
     filas, cierre entre el archivo de origen y el conjunto de salidas.
  2. Materialidad: entre los matches que no cerraron exactos, cuáles
     representan una diferencia real de pesos grande, más allá de si
     pasaron la tolerancia porcentual. Una diferencia de 0,001% en una
     línea de 300 millones son 3.000 pesos reales que hoy no se
     distinguen de un match exacto en ningún lado del reporte.
  3. Asignaciones ambiguas: pares del cruce uno a uno donde existía otro
     candidato casi tan bueno como el elegido -mismo importe, fecha
     parecida-, así que la elección entre ellos fue prácticamente un
     empate resuelto por la matemática, no una coincidencia inequívoca.
  4. Patrones repetidos: la misma diferencia de importe apareciendo más
     de una vez para el mismo tercero o concepto, candidata a ser un
     cargo sistemático no contemplado (una comisión, un redondeo de tipo
     de cambio) en vez de ruido aislado.

Produce una única tabla de hallazgos ordenada por severidad, pensada
para revisarse en minutos en vez de releer todo el cruce.
"""

import pandas as pd


def verificar_totales(resultado, total_unif_original, total_michu_original, tolerancia=1):
    """
    Compara la suma de Monto de todas las salidas de cada lado contra el
    total del archivo de origen de ese lado. Devuelve una lista de
    strings con cualquier descuadre; vacía si todo cierra.

    Es el chequeo más simple y el más importante: si esto falla, nada de
    lo demás importa, porque significa que alguna fila se perdió o se
    contó dos veces en el camino.
    """
    errores = []

    suma_unif = (
        resultado["match_caja_unificada"]["Monto"].sum()
        + resultado["falta_unificada"]["Monto"].sum()
        + resultado["neteos"]["Monto"].sum()
        + resultado["revisar"]["Monto Contabilidad"].sum()
    )
    dif_unif = round(suma_unif - total_unif_original, 2)
    if abs(dif_unif) > tolerancia:
        errores.append(
            f"Contabilidad no cierra: archivo de origen {total_unif_original:,.2f} vs "
            f"suma de salidas {suma_unif:,.2f} (diferencia {dif_unif:,.2f})"
        )

    suma_michu = (
        resultado["match_tesoreria"]["Monto"].sum()
        + resultado["falta_tesoreria"]["Monto"].sum()
        + resultado["comisiones"]["Monto"].sum()
        + resultado["diferencia_arqueo"]["Monto"].sum()
        + resultado["revisar"]["Monto Tesorería"].sum()
    )
    dif_michu = round(suma_michu - total_michu_original, 2)
    if abs(dif_michu) > tolerancia:
        errores.append(
            f"Tesorería no cierra: archivo de origen {total_michu_original:,.2f} vs "
            f"suma de salidas {suma_michu:,.2f} (diferencia {dif_michu:,.2f})"
        )

    return errores


def _materialidad_con_concepto(resultado):
    """
    Base compartida de rankear_materialidad y detectar_patrones_repetidos:
    calcula, para cada id de match, la diferencia real de pesos entre lo
    que sumó contabilidad y lo que sumó tesorería (0 si cerró exacto), y
    guarda además el tercero como "_concepto" -para agrupar hace falta el
    tercero, no el comentario: la mayoría de las filas de contabilidad
    dicen sólo "Pago S/F", que no identifica a nadie y agruparía por
    casualidad-.
    """
    mc, mt = resultado["match_caja_unificada"], resultado["match_tesoreria"]
    filas = []
    for i in mc["id"].unique():
        u, t = mc[mc["id"] == i], mt[mt["id"] == i]
        diferencia = round(u["Monto"].sum() - t["Monto"].sum(), 2)
        if diferencia == 0:
            continue
        if "TERCERO" in u.columns:
            tercero = u["TERCERO"].dropna().astype(str).unique()
            concepto = tercero[0] if len(tercero) else ""
        else:
            concepto = ""
        filas.append({
            "id": i,
            "Tipo Match": u["Tipo Match"].iloc[0],
            "Fecha": u["Fecha"].iloc[0],
            "Contabilidad": " / ".join(str(x) for x in u["Comentario"].astype(str).unique()[:2]),
            "Tesorería": " / ".join(str(x) for x in t["Detalle"].astype(str).unique()[:2]),
            "Diferencia": diferencia,
            "_concepto": concepto,
        })
    return pd.DataFrame(filas)


def rankear_materialidad(resultado, umbral_pesos=1000):
    """
    Devuelve la tabla de diferencias de importe por match, ordenada de
    mayor a menor diferencia absoluta, marcando "ALERTA" a las que
    superan umbral_pesos y "revisar" al resto -incluye también las
    diferencias chicas porque un patrón entre varias diferencias chicas
    es lo que busca detectar_patrones_repetidos-.
    """
    df = _materialidad_con_concepto(resultado)
    if df.empty:
        return df

    df["Severidad"] = "revisar"
    df.loc[df["Diferencia"].abs() >= umbral_pesos, "Severidad"] = "ALERTA"
    orden = df["Diferencia"].abs().sort_values(ascending=False).index
    return df.loc[orden].drop(columns="_concepto").reset_index(drop=True)


def listar_asignaciones_ambiguas(resultado, margen_maximo=0.5):
    """
    De los matches uno a uno (los que llevan "Margen Asignación"), lista
    los que tenían otro candidato casi tan bueno como el elegido: uno
    cuya distancia de costo al elegido es menor o igual a margen_maximo
    -en la práctica, mismo importe y una fecha a menos de medio día de
    diferencia entre las dos alternativas-.

    En esos casos la asignación es matemáticamente óptima -minimiza el
    costo del conjunto, no inventa un cruce inválido- pero la elección
    puntual entre los candidatos empatados fue prácticamente arbitraria,
    y vale la pena que alguien confirme que el nombre corresponde al
    pago correcto.
    """
    mc = resultado["match_caja_unificada"]
    if "Margen Asignación" not in mc.columns:
        return pd.DataFrame()

    ambiguos = mc[mc["Margen Asignación"].notna() & (mc["Margen Asignación"] <= margen_maximo)]
    if ambiguos.empty:
        return pd.DataFrame()

    mt = resultado["match_tesoreria"]
    filas = []
    for _, fila_u in ambiguos.iterrows():
        i = fila_u["id"]
        fila_t = mt[mt["id"] == i].iloc[0]
        filas.append({
            "id": i,
            "Margen": fila_u["Margen Asignación"],
            "Fecha Contabilidad": fila_u["Fecha"],
            "Tercero": fila_u.get("TERCERO"),
            "Comentario": fila_u.get("Comentario"),
            "Monto": fila_u["Monto"],
            "Fecha Tesorería": fila_t["Fecha"],
            "Detalle": fila_t["Detalle"],
        })

    return pd.DataFrame(filas).sort_values("Margen").reset_index(drop=True)


def detectar_patrones_repetidos(resultado, minimo_ocurrencias=2, diferencia_minima=1):
    """
    Agrupa las diferencias de rankear_materialidad por (tercero,
    diferencia redondeada al peso) y se queda con los grupos que
    aparecen minimo_ocurrencias veces o más.

    Exige un tercero identificado -sin eso no hay concepto real que
    repetir, "Pago S/F" no identifica a nadie- y una diferencia de al
    menos diferencia_minima pesos -por debajo de eso es ruido de
    redondeo de la tolerancia porcentual, no un patrón-.

    La misma diferencia repitiéndose para el mismo tercero no es ruido
    aleatorio -el ruido no se repite igual-, es candidato a ser un
    cargo sistemático no contemplado en el cruce: una comisión que no
    se está restando, un redondeo de tipo de cambio, una tasa fija.
    """
    materialidad = _materialidad_con_concepto(resultado)
    if materialidad.empty:
        return pd.DataFrame()

    candidatos = materialidad[
        (materialidad["_concepto"] != "") & (materialidad["Diferencia"].abs() >= diferencia_minima)
    ]
    if candidatos.empty:
        return pd.DataFrame()

    clave = candidatos["_concepto"] + "|" + candidatos["Diferencia"].round(0).astype(str)
    repetido = clave.map(clave.value_counts()) >= minimo_ocurrencias
    salida = candidatos[repetido].assign(_clave=clave[repetido]).sort_values("_clave")
    return (
        salida.drop(columns="_clave")
        .rename(columns={"_concepto": "Tercero"})
        .reset_index(drop=True)
    )


def auditar(
    resultado,
    total_unif_original,
    total_michu_original,
    umbral_materialidad=1000,
    margen_ambiguedad=0.5,
    minimo_ocurrencias_patron=2,
):
    """
    Corre las cuatro verificaciones y arma una única tabla de hallazgos,
    ordenada por severidad (ERROR primero, después ALERTA, después
    revisar), pensada para agregarse como hoja al Excel de salida.
    """
    hallazgos = []

    for error in verificar_totales(resultado, total_unif_original, total_michu_original):
        hallazgos.append({"Severidad": "ERROR", "Categoría": "Totales", "Detalle": error})

    materialidad = rankear_materialidad(resultado, umbral_materialidad)
    for _, f in materialidad.iterrows():
        hallazgos.append({
            "Severidad": f["Severidad"],
            "Categoría": "Materialidad",
            "Detalle": (
                f"id={f['id']} [{f['Tipo Match']}] {f['Fecha']:%d/%m/%Y} "
                f"{f['Contabilidad']} <-> {f['Tesorería']}: diferencia {f['Diferencia']:,.2f}"
            ),
        })

    ambiguas = listar_asignaciones_ambiguas(resultado, margen_ambiguedad)
    for _, f in ambiguas.iterrows():
        hallazgos.append({
            "Severidad": "revisar",
            "Categoría": "Asignación ambigua",
            "Detalle": (
                f"id={f['id']} {f['Fecha Contabilidad']:%d/%m/%Y} "
                f"{f.get('Tercero') or f.get('Comentario')} {f['Monto']:,.2f} <-> "
                f"{f['Fecha Tesorería']:%d/%m/%Y} {f['Detalle']} "
                f"(otro candidato a {f['Margen']:.2f} días de distancia)"
            ),
        })

    repetidos = detectar_patrones_repetidos(resultado, minimo_ocurrencias_patron)
    if not repetidos.empty:
        for (tercero, _), grupo in repetidos.groupby(["Tercero", repetidos["Diferencia"].round(0)]):
            hallazgos.append({
                "Severidad": "ALERTA",
                "Categoría": "Patrón repetido",
                "Detalle": (
                    f"{tercero}: {len(grupo)} matches con diferencia "
                    f"~{grupo['Diferencia'].mean():,.2f} cada uno (ids "
                    f"{', '.join(str(x) for x in grupo['id'])})"
                ),
            })

    orden_severidad = {"ERROR": 0, "ALERTA": 1, "revisar": 2}
    df = pd.DataFrame(hallazgos, columns=["Severidad", "Categoría", "Detalle"])
    if df.empty:
        return df
    df["_orden"] = df["Severidad"].map(orden_severidad)
    return df.sort_values("_orden", kind="stable").drop(columns="_orden").reset_index(drop=True)
