"""
logica_tesoreria.py — Conciliación Tesorería (Caja Central vs Contabilidad)

Cruza los movimientos de la Caja Central (un Excel con una hoja por día,
cargado por Tesorería) contra el mayor contable unificado del sistema:
  1. Carga la Caja Central hoja por hoja, ubicando SALDO INICIAL, encabezado
     Detalle/Monto, SALDO FINAL y la fila de DIFERENCIA de arqueo.
  2. Depura la Caja Unificada del sistema (Monto = Debe - Haber).
  3. Aparta lo que nunca tiene contrapartida del otro lado: comisiones y
     diferencias de arqueo (tesorería), y los pares pago/devolución del
     mismo tercero que se anulan entre sí (contabilidad).
  4. Cruza ambos lados: primero los pasos que exigen coincidencia exacta
     (ingresos agrupados por mes, agrupamiento por nombre, uno a uno por
     monto+fecha, suma por día, combinaciones) y recién al final el que
     afloja la tolerancia de importe, repitiendo el ciclo hasta que no
     aparezcan matches nuevos.
  5. Exporta el resultado (match, faltantes de cada lado y lo apartado) a
     un Excel en memoria, con formato numérico/fecha y ancho de columna
     autoajustado.
"""

import datetime
import re
import unicodedata
from io import BytesIO
from itertools import combinations

import openpyxl
import pandas as pd
from rapidfuzz import fuzz
from openpyxl.utils import get_column_letter


# ─────────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────────

def cargar_caja_central(archivo) -> pd.DataFrame:
    """
    Lee el Excel de Caja Central (una hoja por día) y arma un único
    DataFrame con columnas: Fecha, Detalle, Monto.

    Por hoja:
      - Busca la celda "SALDO INICIAL" y toma la fecha de esa misma fila.
      - Busca el encabezado "Detalle" / "Monto" para saber las columnas.
      - Ubica la fila de "DIFERENCIA" de arqueo tomando la ÚLTIMA fila que
        menciona esa palabra (siempre al final, tras SALDO FINAL y ARQUEO),
        para no confundirla con un movimiento real (ej. "Diferencia de
        sueldos").
      - Toma como movimientos las filas entre el encabezado y "SALDO
        FINAL", excluyendo por número de fila la de "DIFERENCIA" ya
        identificada, y las que digan SALDO o ARQUEO.
      - Si esa fila de diferencia tiene un monto distinto de cero, agrega
        una fila extra "Diferencia Arqueo" con ese valor.
    """
    wb = openpyxl.load_workbook(archivo, data_only=True)
    registros = []

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        max_row = ws.max_row
        max_col = ws.max_column

        fecha_inicial = None
        header_row = None
        col_detalle = None
        col_monto = None
        saldo_final_row = None
        diferencia_row = None

        for r in range(1, max_row + 1):
            for c in range(1, max_col + 1):
                val = ws.cell(row=r, column=c).value
                if not isinstance(val, str):
                    continue
                texto = val.strip().upper()

                if fecha_inicial is None and "SALDO INICIAL" in texto:
                    for cc in range(c + 1, max_col + 1):
                        v2 = ws.cell(row=r, column=cc).value
                        if isinstance(v2, (datetime.datetime, datetime.date)):
                            fecha_inicial = v2
                            break

                elif header_row is None and texto == "DETALLE":
                    header_row = r
                    col_detalle = c
                    for cc in range(c + 1, max_col + 1):
                        v2 = ws.cell(row=r, column=cc).value
                        if isinstance(v2, str) and v2.strip().upper() == "MONTO":
                            col_monto = cc
                            break

                elif saldo_final_row is None and "SALDO FINAL" in texto:
                    saldo_final_row = r

                elif "DIFERENCIA" in texto:
                    diferencia_row = r  # se queda con la ÚLTIMA ocurrencia

        if fecha_inicial is None or header_row is None or col_monto is None:
            continue

        fin = saldo_final_row if saldo_final_row is not None else max_row

        for r in range(header_row + 1, fin):
            if r == diferencia_row:
                continue

            detalle = ws.cell(row=r, column=col_detalle).value
            monto = ws.cell(row=r, column=col_monto).value

            if not isinstance(detalle, str) or monto is None:
                continue
            if not isinstance(monto, (int, float)):
                continue

            texto_up = detalle.strip().upper()
            if texto_up == "" or "SALDO" in texto_up or "ARQUEO" in texto_up:
                continue

            registros.append({
                "Fecha": fecha_inicial,
                "Detalle": detalle.strip(),
                "Monto": monto,
            })

        if diferencia_row is not None:
            monto_dif = ws.cell(row=diferencia_row, column=col_monto).value
            if monto_dif is not None and monto_dif != 0:
                registros.append({
                    "Fecha": fecha_inicial,
                    "Detalle": "Diferencia Arqueo",
                    "Monto": monto_dif,
                })

    df = pd.DataFrame(registros, columns=["Fecha", "Detalle", "Monto"])
    df["Fecha"] = pd.to_datetime(df["Fecha"])
    df = df.sort_values("Fecha", kind="stable").reset_index(drop=True)
    return df


def cargar_caja_central_multiple(archivos) -> pd.DataFrame:
    """
    Igual que cargar_caja_central pero acepta varios archivos (uno por
    mes) y devuelve un único DataFrame con todos los meses juntos,
    ordenado por Fecha.
    """
    if not isinstance(archivos, (list, tuple)):
        archivos = [archivos]

    dfs = [cargar_caja_central(archivo) for archivo in archivos]
    df = pd.concat(dfs, ignore_index=True)
    df = df.sort_values("Fecha", kind="stable").reset_index(drop=True)
    return df


def load_excel_file(archivo) -> pd.DataFrame:
    return pd.read_excel(archivo)


# ─────────────────────────────────────────────
# DEPURACIÓN
# ─────────────────────────────────────────────

def depurar_caja_unificada(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "Monto" = Debe - Haber (NaN/vacío se toman como 0).
    """
    df = df.copy()

    def a_numero(serie):
        if serie.dtype == object:
            serie = (
                serie.astype(str)
                .str.strip()
                .str.replace(".", "", regex=False)   # separador de miles
                .str.replace(",", ".", regex=False)  # coma decimal -> punto
            )
            serie = serie.replace({"": None, "nan": None, "None": None})
        return pd.to_numeric(serie, errors="coerce")

    debe = a_numero(df["Debe"]).fillna(0)
    haber = a_numero(df["Haber"]).fillna(0)

    df["Monto"] = (debe - haber).astype("float64").round(2)

    return df


# ─────────────────────────────────────────────
# CRUCE
# ─────────────────────────────────────────────

PALABRAS_EXCLUIDAS_NOMBRE = ("pago", "devolucion", "ingreso")


def _normalizar_texto(texto):
    if not isinstance(texto, str):
        return ""
    t = unicodedata.normalize("NFKD", texto)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.lower()
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _es_nombre(texto_normalizado):
    if texto_normalizado == "":
        return False
    return not any(p in texto_normalizado for p in PALABRAS_EXCLUIDAS_NOMBRE)


def _tolerancia_dinamica(monto, tolerancia_pesos, tolerancia_pct):
    return max(tolerancia_pesos, abs(monto) * tolerancia_pct / 100)


def _buscar_combinacion(monto_objetivo, fecha_objetivo, pool_df, max_combinacion,
                         ventana_dias, tolerancia_pesos, tolerancia_pct):
    """
    Busca en pool_df un subconjunto de filas (2 a max_combinacion) cuya
    suma coincida con monto_objetivo (con tolerancia), restringido a filas
    a lo sumo a ventana_dias de fecha_objetivo. Devuelve los índices de la
    PRIMER combinación válida (se prueban primero las más chicas), o None.
    """
    candidatos = pool_df[
        pool_df["_fecha_norm"].apply(lambda f: abs((f - fecha_objetivo).days) <= ventana_dias)
    ]
    if len(candidatos) < 2:
        return None

    tol = _tolerancia_dinamica(monto_objetivo, tolerancia_pesos, tolerancia_pct)
    indices = candidatos.index.tolist()

    for k in range(2, max_combinacion + 1):
        if k > len(indices):
            break
        for combo in combinations(indices, k):
            suma = candidatos.loc[list(combo), "_monto_norm"].sum()
            if abs(suma - monto_objetivo) <= tol:
                return list(combo)
    return None


# ─────────────────────────────────────────────
# SEPARACIONES PREVIAS AL CRUCE
# ─────────────────────────────────────────────

def _separar_por_texto(df, col_detalle, palabra):
    """
    Parte df en (resto, apartados) según si el detalle normalizado
    contiene `palabra`. Se usa para sacar del cruce lo que nunca tiene
    contrapartida del otro lado.
    """
    mask = df[col_detalle].apply(lambda t: palabra in _normalizar_texto(t))
    return df[~mask].reset_index(drop=True), df[mask].reset_index(drop=True)


def _separar_neteos(unif, col_fecha, col_monto, col_tercero,
                    tolerancia_pesos, tolerancia_dias):
    """
    Saca de contabilidad los pares que se anulan entre sí (un pago y su
    devolución): mismo tercero, importes opuestos y fechas dentro de
    tolerancia_dias. Esas filas nunca van a tener contrapartida en
    tesorería porque la operación se revirtió, así que ensucian el cruce
    y el listado de faltantes.

    Devuelve (resto, neteos). Si el archivo no trae la columna de tercero,
    no netea nada: sin tercero el criterio sería demasiado laxo.
    """
    if col_tercero not in unif.columns:
        return unif.reset_index(drop=True), unif.iloc[0:0].reset_index(drop=True)

    fechas = pd.to_datetime(unif[col_fecha]).dt.date
    montos = unif[col_monto].astype(float).round(2)
    terceros = unif[col_tercero].apply(_normalizar_texto)

    usados = set()
    for i in unif.index:
        if i in usados or montos[i] >= 0 or terceros[i] == "":
            continue
        for j in unif.index:
            if j in usados or j == i or montos[j] <= 0:
                continue
            if terceros[j] != terceros[i]:
                continue
            if abs(montos[i] + montos[j]) > tolerancia_pesos:
                continue
            if abs((fechas[i] - fechas[j]).days) > tolerancia_dias:
                continue
            usados.update({i, j})
            break

    neteos = unif.loc[sorted(usados)].reset_index(drop=True)
    resto = unif.drop(index=usados).reset_index(drop=True)
    return resto, neteos


# ─────────────────────────────────────────────
# CLAVES DE AGRUPAMIENTO
# ─────────────────────────────────────────────

def _claves_por_tercero(df, col_tercero):
    """Clave de agrupamiento = el tercero normalizado. Vacío = no agrupa."""
    if col_tercero not in df.columns:
        return pd.Series("", index=df.index)
    return df[col_tercero].apply(_normalizar_texto)


def _claves_por_similitud(df, col_detalle, score_minimo):
    """
    Agrupa las filas de un mismo lado por parecido de texto: dos detalles
    caen en el mismo grupo si su versión normalizada (sin mayúsculas, sin
    acentos, sin puntuación y sin espacios de más) llega al score mínimo.

    Hace falta en tesorería, donde el mismo concepto se escribe distinto
    de un día para otro ("Iron Driver" / "IronDriver"). El recorrido es
    greedy: la primera fila de cada grupo le presta su texto como clave a
    todas las que se le parecen.
    """
    textos = df[col_detalle].apply(_normalizar_texto)
    claves = pd.Series("", index=df.index, dtype=object)

    libres = [i for i in df.index if textos[i] != ""]
    while libres:
        cabeza = libres.pop(0)
        claves[cabeza] = textos[cabeza]
        for otro in list(libres):
            if fuzz.token_sort_ratio(textos[cabeza], textos[otro]) >= score_minimo:
                claves[otro] = textos[cabeza]
                libres.remove(otro)
    return claves


def _clusters_por_fecha(sub, max_gap):
    """
    Parte un grupo en tramos donde el salto entre fechas consecutivas no
    supera max_gap. Sin esto, agrupar por tercero sumaría movimientos de
    meses distintos que no tienen nada que ver entre sí.
    """
    sub = sub.sort_values("_fecha_norm")
    tramos, actual = [], []
    for idx, fila in sub.iterrows():
        if actual and (fila["_fecha_norm"] - sub.loc[actual[-1], "_fecha_norm"]).days > max_gap:
            tramos.append(actual)
            actual = []
        actual.append(idx)
    if actual:
        tramos.append(actual)
    return tramos


# ─────────────────────────────────────────────
# PASOS DE MATCH
# ─────────────────────────────────────────────

def _marcar(df, indices, id_match):
    df.loc[indices, "_matched"] = True
    df.loc[indices, "id"] = id_match


def _paso_uno_a_uno(unif, michu, tipo_por_id, contador,
                    tolerancia_pesos, tolerancia_pct, tolerancia_dias,
                    etiqueta=None, ignorar_fecha=False):
    """
    Cruza línea contra línea por monto (con tolerancia) y fecha (con
    tolerancia_dias), eligiendo siempre el candidato más cercano en fecha
    y después en importe.

    Con `ignorar_fecha` la fecha deja de filtrar y queda sólo como
    criterio de desempate: entre varios candidatos del mismo importe se
    elige el más cercano en el tiempo, pero ninguno se descarta por
    lejano. Es la última pasada de todas: cuando un movimiento ya no
    cruzó por ningún otro camino, que el importe coincida exacto es
    señal suficiente, y fijar una ventana de días sería arbitrario.

    Si `etiqueta` viene dada, todos los matches se taggean con ese texto
    (se usa en las pasadas finales, donde lo que importa es que el match
    se revise, no el detalle de qué se relajó). Si no, se clasifica como
    Exacto / Tolerancia Importe / Tolerancia Fecha / Tolerancia Importe
    y Fecha, como siempre.
    """
    restante_michu = michu[~michu["_matched"]].copy()

    for _, fila_u in unif[~unif["_matched"]].sort_values("_fecha_norm").iterrows():
        if restante_michu.empty:
            break

        monto_u = fila_u["_monto_norm"]
        fecha_u = fila_u["_fecha_norm"]
        tol = _tolerancia_dinamica(monto_u, tolerancia_pesos, tolerancia_pct)

        candidatos = restante_michu[(restante_michu["_monto_norm"] - monto_u).abs() <= tol]
        if candidatos.empty:
            continue

        if not ignorar_fecha:
            dif_dias = candidatos["_fecha_norm"].apply(lambda f: abs((f - fecha_u).days))
            candidatos = candidatos[dif_dias <= tolerancia_dias]
            if candidatos.empty:
                continue

        dif_dias = candidatos["_fecha_norm"].apply(lambda f: abs((f - fecha_u).days))
        dif_monto = (candidatos["_monto_norm"] - monto_u).abs()
        orden = pd.DataFrame({"dif_dias": dif_dias, "dif_monto": dif_monto}).sort_values(
            ["dif_dias", "dif_monto"]
        )
        fila_m = candidatos.loc[orden.index[0]]
        id_michu_elegido = fila_m["_id"]

        if etiqueta is not None:
            tipo = etiqueta
        else:
            dm = round(abs(monto_u - fila_m["_monto_norm"]), 2)
            dd = abs((fecha_u - fila_m["_fecha_norm"]).days)
            if dm == 0 and dd == 0:
                tipo = "Exacto"
            elif dm > 0 and dd == 0:
                tipo = "Tolerancia Importe"
            elif dm == 0 and dd > 0:
                tipo = "Tolerancia Fecha"
            else:
                tipo = "Tolerancia Importe y Fecha"

        _marcar(unif, unif["_id"] == fila_u["_id"], contador)
        _marcar(michu, michu["_id"] == id_michu_elegido, contador)
        tipo_por_id[contador] = tipo
        contador += 1

        restante_michu = restante_michu[restante_michu["_id"] != id_michu_elegido]

    return contador


def _paso_suma_por_dia(unif, michu, tipo_por_id, contador,
                       tolerancia_pesos, tolerancia_pct, tolerancia_dias):
    """
    Suma el remanente de tesorería de cada día y lo compara contra UNA
    línea suelta del sistema.

    Sirve para los gastos que contabilidad carga en una sola línea (un
    tercero, un importe) y tesorería registra desagregados en varios
    pagos del mismo día. Es más barato y más explicable que buscar
    combinaciones: no explora subconjuntos, toma el día entero.

    Por eso mismo depende de que el día esté limpio: si quedó una línea
    del día que en realidad cruza contra otra cosa, la suma no cierra.
    De ahí que el cruce se repita después de la pasada de tolerancia
    ampliada (ver `cruzar_caja`).
    """
    for dia, grupo in michu[~michu["_matched"]].groupby("_fecha_norm"):
        if len(grupo) < 2:
            continue

        suma = round(grupo["_monto_norm"].sum(), 2)
        tol = _tolerancia_dinamica(suma, tolerancia_pesos, tolerancia_pct)

        candidatos = unif[(~unif["_matched"]) & ((unif["_monto_norm"] - suma).abs() <= tol)]
        if candidatos.empty:
            continue

        dif_dias = candidatos["_fecha_norm"].apply(lambda f: abs((f - dia).days))
        candidatos = candidatos[dif_dias <= tolerancia_dias]
        if candidatos.empty:
            continue

        dif_dias = candidatos["_fecha_norm"].apply(lambda f: abs((f - dia).days))
        dif_monto = (candidatos["_monto_norm"] - suma).abs()
        orden = pd.DataFrame({"dif_dias": dif_dias, "dif_monto": dif_monto}).sort_values(
            ["dif_dias", "dif_monto"]
        )
        idx_unif = orden.index[0]

        _marcar(unif, [idx_unif], contador)
        _marcar(michu, grupo.index, contador)
        tipo_por_id[contador] = "Agrupado (Día)"
        contador += 1

    return contador


def _paso_suma_por_clave(origen, destino, claves, etiqueta,
                         tipo_por_id, contador,
                         tolerancia_pesos, tolerancia_pct, tolerancia_dias):
    """
    Agrupa el remanente de `origen` por una clave (el tercero en
    contabilidad, el detalle parecido en tesorería), suma cada grupo y
    busca UNA línea suelta de `destino` que coincida con ese total.

    Es el mismo mecanismo que la suma por día, con otra clave: sirve para
    cuando un lado carga el movimiento consolidado y el otro lo tiene
    abierto en varias líneas del mismo proveedor o concepto.

    Cada grupo se parte además por cercanía de fechas (`tolerancia_dias`
    de salto máximo entre líneas consecutivas). Sin ese corte, un tercero
    con movimientos todos los meses sumaría el período entero.
    """
    restante = origen[~origen["_matched"]]

    for clave, grupo in restante[claves.reindex(restante.index) != ""].groupby(
        claves.reindex(restante.index)
    ):
        for tramo in _clusters_por_fecha(grupo, tolerancia_dias):
            if len(tramo) < 2:
                continue

            suma = round(origen.loc[tramo, "_monto_norm"].sum(), 2)
            tol = _tolerancia_dinamica(suma, tolerancia_pesos, tolerancia_pct)
            fechas_tramo = origen.loc[tramo, "_fecha_norm"]

            candidatos = destino[(~destino["_matched"]) &
                                 ((destino["_monto_norm"] - suma).abs() <= tol)]
            if candidatos.empty:
                continue

            dif_dias = candidatos["_fecha_norm"].apply(
                lambda f: min(abs((f - g).days) for g in fechas_tramo)
            )
            candidatos = candidatos[dif_dias <= tolerancia_dias]
            if candidatos.empty:
                continue

            dif_dias = candidatos["_fecha_norm"].apply(
                lambda f: min(abs((f - g).days) for g in fechas_tramo)
            )
            dif_monto = (candidatos["_monto_norm"] - suma).abs()
            orden = pd.DataFrame({"dif_dias": dif_dias, "dif_monto": dif_monto}).sort_values(
                ["dif_dias", "dif_monto"]
            )

            _marcar(destino, [orden.index[0]], contador)
            _marcar(origen, tramo, contador)
            tipo_por_id[contador] = etiqueta
            contador += 1

    return contador


def _armar_grupos(df, claves, tolerancia_dias):
    """
    Arma los grupos de un lado: agrupa el remanente por clave y parte cada
    grupo por cercanía de fechas. Devuelve sólo los grupos de 2 o más
    líneas, porque los de una sola ya los cubre el cruce uno a uno.
    """
    restante = df[~df["_matched"]]
    claves_restante = claves.reindex(restante.index)

    grupos = []
    for clave, grupo in restante[claves_restante != ""].groupby(
        claves_restante[claves_restante != ""]
    ):
        for tramo in _clusters_por_fecha(grupo, tolerancia_dias):
            if len(tramo) >= 2:
                grupos.append((clave, tramo))
    return grupos


def _paso_grupo_contra_grupo(unif, michu, claves_unif, claves_michu, etiqueta,
                             tipo_por_id, contador,
                             tolerancia_pesos, tolerancia_pct, tolerancia_dias):
    """
    Cruza un grupo contra otro grupo: contabilidad agrupada por tercero
    contra tesorería agrupada por detalle parecido, comparando las sumas
    de los dos lados.

    Los pasos de suma por clave anteriores resuelven N contra 1 -varias
    líneas de un lado contra una sola del otro-. Este resuelve N contra
    M: el caso en que los DOS lados tienen el movimiento abierto, pero
    abierto distinto (contabilidad en tres pagos al mismo proveedor,
    tesorería en cinco entregas del mismo concepto). Por eso exige 2 o
    más líneas de cada lado: lo de una sola línea ya pasó por los pasos
    anteriores.

    Como cada grupo ya viene acotado por fecha, para cruzarlos alcanza
    con que alguna fecha de un lado caiga a tolerancia_dias de alguna del
    otro.
    """
    grupos_unif = _armar_grupos(unif, claves_unif, tolerancia_dias)
    grupos_michu = _armar_grupos(michu, claves_michu, tolerancia_dias)

    for _, tramo_u in grupos_unif:
        if unif.loc[tramo_u, "_matched"].any():
            continue

        suma_u = round(unif.loc[tramo_u, "_monto_norm"].sum(), 2)
        tol = _tolerancia_dinamica(suma_u, tolerancia_pesos, tolerancia_pct)
        fechas_u = unif.loc[tramo_u, "_fecha_norm"]

        for _, tramo_m in grupos_michu:
            if michu.loc[tramo_m, "_matched"].any():
                continue

            suma_m = round(michu.loc[tramo_m, "_monto_norm"].sum(), 2)
            if abs(suma_u - suma_m) > tol:
                continue

            fechas_m = michu.loc[tramo_m, "_fecha_norm"]
            cerca = min(abs((fu - fm).days) for fu in fechas_u for fm in fechas_m)
            if cerca > tolerancia_dias:
                continue

            _marcar(unif, tramo_u, contador)
            _marcar(michu, tramo_m, contador)
            tipo_por_id[contador] = etiqueta
            contador += 1
            break

    return contador


def _paso_combinaciones(unif, michu, tipo_por_id, contador,
                        tolerancia_pesos, tolerancia_pct,
                        max_combinacion, ventana_dias_combinacion):
    """
    Para lo que sigue sin matchear, busca si VARIAS líneas de un lado
    (hasta max_combinacion) suman el monto de UNA línea del otro lado,
    dentro de ventana_dias_combinacion días. Se corre en las dos
    direcciones. Es greedy y más propenso a falsos positivos que los
    pasos anteriores, por eso queda taggeado aparte.
    """
    for ancla, pool in ((unif, michu), (michu, unif)):
        for _, fila in ancla[~ancla["_matched"]].sort_values("_fecha_norm").iterrows():
            if ancla.loc[ancla["_id"] == fila["_id"], "_matched"].iloc[0]:
                continue

            combo_idx = _buscar_combinacion(
                fila["_monto_norm"], fila["_fecha_norm"], pool[~pool["_matched"]],
                max_combinacion, ventana_dias_combinacion,
                tolerancia_pesos, tolerancia_pct,
            )
            if combo_idx is None:
                continue

            _marcar(ancla, ancla["_id"] == fila["_id"], contador)
            _marcar(pool, combo_idx, contador)
            tipo_por_id[contador] = "Agrupado (Combinación)"
            contador += 1

    return contador


def cruzar_caja(
    df_caja_unificada,
    df_caja_michu,
    col_fecha="Fecha",
    col_monto="Monto",
    col_detalle_unificada="Comentario",
    col_detalle_michu="Detalle",
    col_tercero_unificada="TERCERO",
    tolerancia_pesos=5,
    tolerancia_pct=0.001,
    tolerancia_pct_amplia=0.05,
    tolerancia_dias=3,
    max_combinacion=4,
    ventana_dias_combinacion=5,
    score_similitud=80,
):
    """
    Cruza df_caja_unificada (sistema) contra df_caja_michu (tesorería).

    SEPARACIONES PREVIAS (no participan del cruce, van a su propia salida)
      - Comisiones: sólo existen en tesorería.
      - Diferencia de arqueo: es el descuadre del conteo diario, nunca
        tiene contrapartida contable.
      - Neteos: pares pago/devolución del mismo tercero dentro de
        contabilidad, que se anulan entre sí.

    PASOS FIJOS
      1. Ingresos agrupados por mes: suma de "ingreso" (tesorería) vs
         "ingreso efectivo" (sistema).
      2. Agrupamiento por nombre: filas cuyo detalle no dice "pago",
         "devolucion" ni "ingreso" se agrupan por nombre normalizado de
         cada lado y se cruzan sus sumas.
      3. Uno a uno por monto + fecha, con la tolerancia estricta.

    CICLO (se repite hasta que una vuelta completa no agregue nada)
      4. Suma por día del remanente de tesorería vs una línea suelta del
         sistema.
      5. Suma por tercero del remanente de contabilidad vs una línea
         suelta de tesorería.
      6. Suma por detalle parecido (score_similitud) del remanente de
         tesorería vs una línea suelta del sistema.
      7. Grupo contra grupo: contabilidad agrupada por tercero contra
         tesorería agrupada por detalle parecido, comparando las dos
         sumas (los pasos 5 y 6 resuelven N contra 1; éste, N contra M).
      8. Combinaciones: varias líneas de un lado suman una del otro.
      9. Uno a uno con tolerancia de importe ampliada, dentro de la
         ventana de fechas -> "Diferencia de Importe".
     10. Uno a uno por importe, ya SIN ventana de fechas: la fecha queda
         sólo como desempate entre candidatos del mismo importe ->
         "Coincidencia de Importe".

    Los pasos 5 y 6 parten cada grupo por cercanía de fechas, para no
    sumar movimientos de meses distintos del mismo tercero o concepto.

    El orden importa: los pasos que exigen coincidencia exacta corren
    antes que el que afloja la tolerancia, para que un match flojo no se
    robe una línea que pertenecía a un grupo que cerraba exacto. Pero el
    paso 4 a veces sólo puede ver su grupo después de que el paso 6 se
    llevó una línea ajena que caía el mismo día, así que en vez de elegir
    un orden se itera hasta que la cosa se estabiliza.

    Returns
    -------
    dict con match_caja_unificada, match_tesoreria, falta_unificada,
    falta_tesoreria, comisiones, diferencia_arqueo, neteos y warnings.
    """

    unif = df_caja_unificada.copy().reset_index(drop=True)
    michu = df_caja_michu.copy().reset_index(drop=True)

    # ------------------------------------------------------------------
    # Separaciones previas
    # ------------------------------------------------------------------
    michu, comisiones = _separar_por_texto(michu, col_detalle_michu, "comision")
    michu, diferencia_arqueo = _separar_por_texto(michu, col_detalle_michu, "diferencia arqueo")
    unif, neteos = _separar_neteos(
        unif, col_fecha, col_monto, col_tercero_unificada,
        tolerancia_pesos, tolerancia_dias,
    )

    unif["_id"] = unif.index
    michu["_id"] = michu.index

    unif["_matched"] = False
    michu["_matched"] = False
    unif["id"] = pd.NA
    michu["id"] = pd.NA

    unif["_fecha_norm"] = pd.to_datetime(unif[col_fecha]).dt.date
    michu["_fecha_norm"] = pd.to_datetime(michu[col_fecha]).dt.date
    unif["_monto_norm"] = unif[col_monto].astype(float).round(2)
    michu["_monto_norm"] = michu[col_monto].astype(float).round(2)

    claves_tercero = _claves_por_tercero(unif, col_tercero_unificada)
    claves_detalle = _claves_por_similitud(michu, col_detalle_michu, score_similitud)

    contador = 1
    tipo_por_id = {}
    warnings = []

    # ------------------------------------------------------------------
    # PASO 1: bloque de "ingresos" agrupados, por mes
    #
    # Cuando el detalle de tesorería contiene "ingreso" y más de un número
    # (ej. varios comprobantes en una misma línea), se agrupan y suman
    # junto con el resto de "ingreso" del mismo lado que caigan en el
    # mismo mes, y se comparan contra la suma de "ingreso efectivo" del
    # sistema de ese mismo mes (en vez de una única suma global), para no
    # perder matches cuando distintos meses no cuadran entre sí.
    # ------------------------------------------------------------------
    def tiene_ingreso_y_mas_de_un_numero(texto):
        if not isinstance(texto, str):
            return False
        if "ingreso" not in texto.lower():
            return False
        numeros = re.findall(r"\d+", texto)
        return len(numeros) > 1

    mask_michu_ingreso = michu[col_detalle_michu].apply(tiene_ingreso_y_mas_de_un_numero)
    mask_unif_ingreso_efectivo = (
        unif[col_detalle_unificada].astype(str).str.lower().str.contains("ingreso efectivo", na=False)
    )

    grupo_michu_total = michu[mask_michu_ingreso]
    grupo_unif_total = unif[mask_unif_ingreso_efectivo]

    meses = sorted(set(
        grupo_michu_total["_fecha_norm"].apply(lambda f: (f.year, f.month))
    ) | set(
        grupo_unif_total["_fecha_norm"].apply(lambda f: (f.year, f.month))
    ))

    for mes in meses:
        grupo_michu = grupo_michu_total[
            grupo_michu_total["_fecha_norm"].apply(lambda f: (f.year, f.month)) == mes
        ]
        grupo_unif = grupo_unif_total[
            grupo_unif_total["_fecha_norm"].apply(lambda f: (f.year, f.month)) == mes
        ]

        suma_michu = grupo_michu["_monto_norm"].sum()
        suma_unif = grupo_unif["_monto_norm"].sum()

        if len(grupo_michu) > 0 and len(grupo_unif) > 0:
            tol = _tolerancia_dinamica(max(abs(suma_michu), abs(suma_unif)), tolerancia_pesos, tolerancia_pct)
            if abs(suma_michu - suma_unif) <= tol:
                _marcar(michu, grupo_michu.index, contador)
                _marcar(unif, grupo_unif.index, contador)
                tipo_por_id[contador] = "Agrupado (Ingreso)"
                contador += 1
            else:
                warnings.append(
                    f"Ingresos no cuadran en {mes[0]}-{mes[1]:02d}: tesorería suma {suma_michu:.2f} vs "
                    f"sistema 'Ingreso efectivo' suma {suma_unif:.2f} (diferencia "
                    f"{suma_michu - suma_unif:.2f}, tolerancia {tol:.2f}). No se marcaron como matcheados."
                )
        elif len(grupo_michu) > 0:
            warnings.append(
                f"Ingresos sin contrapartida en {mes[0]}-{mes[1]:02d}: tesorería tiene "
                f"{len(grupo_michu)} línea(s) de ingreso por {suma_michu:.2f} y el sistema no trae "
                "ninguna línea 'Ingreso efectivo' ese mes. Puede faltar una cuenta en la "
                "exportación del mayor."
            )
        elif len(grupo_unif) > 0:
            warnings.append(
                f"Ingresos sin contrapartida en {mes[0]}-{mes[1]:02d}: el sistema trae "
                f"{len(grupo_unif)} línea(s) de 'Ingreso efectivo' por {suma_unif:.2f} y tesorería "
                "no registra ingresos ese mes."
            )

    # ------------------------------------------------------------------
    # PASO 2: agrupamiento por nombre, en ambos lados
    # ------------------------------------------------------------------
    unif["_nombre_norm"] = unif[col_detalle_unificada].apply(_normalizar_texto)
    michu["_nombre_norm"] = michu[col_detalle_michu].apply(_normalizar_texto)

    restante_unif = unif[~unif["_matched"]]
    restante_michu = michu[~michu["_matched"]]

    nombres_unif = set(
        restante_unif.loc[restante_unif["_nombre_norm"].apply(_es_nombre), "_nombre_norm"]
    )
    nombres_michu = set(
        restante_michu.loc[restante_michu["_nombre_norm"].apply(_es_nombre), "_nombre_norm"]
    )
    nombres_comunes = nombres_unif & nombres_michu

    for nombre in nombres_comunes:
        grupo_u = unif[(~unif["_matched"]) & (unif["_nombre_norm"] == nombre)]
        grupo_m = michu[(~michu["_matched"]) & (michu["_nombre_norm"] == nombre)]
        if grupo_u.empty or grupo_m.empty:
            continue

        suma_u = grupo_u["_monto_norm"].sum()
        suma_m = grupo_m["_monto_norm"].sum()
        tol = _tolerancia_dinamica(max(abs(suma_u), abs(suma_m)), tolerancia_pesos, tolerancia_pct)
        if abs(suma_u - suma_m) > tol:
            continue

        fechas = pd.concat([grupo_u["_fecha_norm"], grupo_m["_fecha_norm"]])
        rango_dias = (fechas.max() - fechas.min()).days
        if rango_dias > tolerancia_dias:
            continue

        _marcar(unif, grupo_u.index, contador)
        _marcar(michu, grupo_m.index, contador)
        tipo_por_id[contador] = "Agrupado (Nombre)"
        contador += 1

    # ------------------------------------------------------------------
    # PASO 3: uno a uno con la tolerancia estricta
    # ------------------------------------------------------------------
    contador = _paso_uno_a_uno(
        unif, michu, tipo_por_id, contador,
        tolerancia_pesos, tolerancia_pct, tolerancia_dias,
    )

    # ------------------------------------------------------------------
    # PASOS 4 a 10, en ciclo hasta estabilizar
    # ------------------------------------------------------------------
    while True:
        antes = contador

        contador = _paso_suma_por_dia(
            unif, michu, tipo_por_id, contador,
            tolerancia_pesos, tolerancia_pct, tolerancia_dias,
        )
        contador = _paso_suma_por_clave(
            unif, michu, claves_tercero, "Agrupado (Tercero)",
            tipo_por_id, contador,
            tolerancia_pesos, tolerancia_pct, tolerancia_dias,
        )
        contador = _paso_suma_por_clave(
            michu, unif, claves_detalle, "Agrupado (Detalle)",
            tipo_por_id, contador,
            tolerancia_pesos, tolerancia_pct, tolerancia_dias,
        )
        contador = _paso_grupo_contra_grupo(
            unif, michu, claves_tercero, claves_detalle,
            "Agrupado (Tercero vs Detalle)",
            tipo_por_id, contador,
            tolerancia_pesos, tolerancia_pct, tolerancia_dias,
        )
        contador = _paso_combinaciones(
            unif, michu, tipo_por_id, contador,
            tolerancia_pesos, tolerancia_pct,
            max_combinacion, ventana_dias_combinacion,
        )
        contador = _paso_uno_a_uno(
            unif, michu, tipo_por_id, contador,
            tolerancia_pesos, tolerancia_pct_amplia, tolerancia_dias,
            etiqueta="Diferencia de Importe",
        )
        contador = _paso_uno_a_uno(
            unif, michu, tipo_por_id, contador,
            tolerancia_pesos, tolerancia_pct, tolerancia_dias,
            etiqueta="Coincidencia de Importe", ignorar_fecha=True,
        )

        if contador == antes:
            break

    unif["Tipo Match"] = unif["id"].map(tipo_por_id)
    michu["Tipo Match"] = michu["id"].map(tipo_por_id)

    # ------------------------------------------------------------------
    # Armar resultados finales
    # ------------------------------------------------------------------
    cols_unif = list(df_caja_unificada.columns)
    cols_michu = list(df_caja_michu.columns)

    match_caja_unificada = unif[unif["_matched"]][cols_unif + ["id", "Tipo Match"]].sort_values("id").reset_index(drop=True)
    match_tesoreria = michu[michu["_matched"]][cols_michu + ["id", "Tipo Match"]].sort_values("id").reset_index(drop=True)
    falta_unificada = unif[~unif["_matched"]][cols_unif + ["id"]].reset_index(drop=True)
    falta_tesoreria = michu[~michu["_matched"]][cols_michu + ["id"]].reset_index(drop=True)

    return {
        "match_caja_unificada": match_caja_unificada,
        "match_tesoreria": match_tesoreria,
        "falta_unificada": falta_unificada,
        "falta_tesoreria": falta_tesoreria,
        "comisiones": comisiones,
        "diferencia_arqueo": diferencia_arqueo,
        "neteos": neteos,
        "warnings": warnings,
    }


# ─────────────────────────────────────────────
# EXPORTAR EN MEMORIA
# ─────────────────────────────────────────────

def generar_excel_en_memoria_tesoreria(match_caja_unificada, match_tesoreria,
                                        falta_unificada, falta_tesoreria,
                                        comisiones, diferencia_arqueo=None,
                                        neteos=None) -> bytes:
    """
    Exporta los DataFrames del cruce a un Excel en memoria, con:
      - columnas float con formato numérico (separador de miles, 2 decimales)
      - columnas de fecha con formato dd/mm/aaaa
      - ancho de columna autoajustado al contenido
    """
    buf = BytesIO()

    hojas = {
        "Match Contabilidad": match_caja_unificada,
        "Match Tesoreria": match_tesoreria,
        "Falta Contabilidad": falta_unificada,
        "Falta Tesoreria": falta_tesoreria,
        "Comisiones": comisiones,
    }
    if diferencia_arqueo is not None:
        hojas["Diferencia Arqueo"] = diferencia_arqueo
    if neteos is not None:
        hojas["Neteos Contabilidad"] = neteos

    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for nombre, df in hojas.items():
            df.to_excel(writer, sheet_name=nombre, index=False)

        workbook = writer.book

        for nombre, df in hojas.items():
            ws = workbook[nombre]

            for col_idx, col_name in enumerate(df.columns, start=1):
                letra = get_column_letter(col_idx)
                serie = df[col_name]

                if pd.api.types.is_float_dtype(serie):
                    formato = "#,##0.00"
                elif pd.api.types.is_datetime64_any_dtype(serie):
                    formato = "dd/mm/yyyy"
                else:
                    formato = None

                if formato:
                    for celda in ws[letra][1:]:  # saltea el encabezado
                        celda.number_format = formato

                largos = [len(str(col_name))]
                if len(serie):
                    largos += [len(str(v)) for v in serie.dropna()]
                ws.column_dimensions[letra].width = max(largos) + 3

    return buf.getvalue()


# ─────────────────────────────────────────────
# PIPELINE COMPLETO
# ─────────────────────────────────────────────

def correr_conciliacion_tesoreria(
    archivos_caja_central,
    archivo_caja_unificada,
    col_detalle_unificada="Comentario",
    tolerancia_pesos=5,
    tolerancia_pct=0.001,
    tolerancia_pct_amplia=0.05,
    tolerancia_dias=3,
):
    """
    archivos_caja_central: uno o varios archivos de Caja Central (uno por
    mes, cada uno con una hoja por día). archivo_caja_unificada: un único
    archivo del sistema que ya puede traer varios meses juntos.
    """
    df_caja_michu = cargar_caja_central_multiple(archivos_caja_central)

    df_caja_unificada = load_excel_file(archivo_caja_unificada)
    df_caja_unificada = depurar_caja_unificada(df_caja_unificada)

    resultado = cruzar_caja(
        df_caja_unificada,
        df_caja_michu,
        col_detalle_unificada=col_detalle_unificada,
        col_detalle_michu="Detalle",
        tolerancia_pesos=tolerancia_pesos,
        tolerancia_pct=tolerancia_pct,
        tolerancia_pct_amplia=tolerancia_pct_amplia,
        tolerancia_dias=tolerancia_dias,
    )

    buf = generar_excel_en_memoria_tesoreria(
        resultado["match_caja_unificada"],
        resultado["match_tesoreria"],
        resultado["falta_unificada"],
        resultado["falta_tesoreria"],
        resultado["comisiones"],
        resultado["diferencia_arqueo"],
        resultado["neteos"],
    )

    stats = {
        "match": len(resultado["match_caja_unificada"]),
        "falta_contabilidad": len(resultado["falta_unificada"]),
        "falta_tesoreria": len(resultado["falta_tesoreria"]),
        "comisiones": len(resultado["comisiones"]),
        "diferencia_arqueo": len(resultado["diferencia_arqueo"]),
        "neteos": len(resultado["neteos"]),
        "warnings": resultado["warnings"],
    }

    return buf, stats
