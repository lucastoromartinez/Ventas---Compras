"""
logica_conciliacion_rappi_ronda.py
==================================
Conciliación Rappi — submódulo RONDA.

Conciliación mensual de la cuenta de recaudación Rappi de la sociedad RONDA.

Toma el pendientes del mes anterior y lo actualiza al mes que se cierra:
  1. Ventas: Rappi vs HIO (por localizador y por Fecha + Local + monto) y
     Rappi vs Atalaya (lógica de logica_atalaya.py).
  2. Facturas: cruce de las facturas del cuadro de conceptos y de las facturas
     pendientes contra el mayor (por período, por número, contra créditos
     agrupados y preliminares por importe).
  3. Acreditaciones: liquidaciones cobradas en el mes + pendientes del mes
     anterior contra 'Acreditaciones Rappi mm/aaaa' del mayor. Las liquidaciones
     con 'Valor total a transferir' negativo no se cruzan: van directo al
     pendientes como 'Acreditaciones Negativas'.
  4. Arma el pendientes nuevo y recalcula la cadena de totales
     (Saldo a conciliar -> Total Saldo del mes -> Diferencia para llegar al
     saldo -> Diferencia de centavo, que cierra en cero).

Diferencias con EASA (misma idea, distinto formato del cuadro manual):
  - No hay Dean: hay Atalaya, con su propio sistema y reporte.
  - Los conceptos del pendientes van AGRUPADOS por concepto y en orden alfabético.
  - El mes va en una sola línea: 'Gastos Faltan Registrar <Mes> <Año>'.
  - El bloque de gastos de las liquidaciones que se cobran el mes siguiente se
    llama 'Gastos Faltan Registrar ' y, al cerrar el mes siguiente, sube
    renombrado a 'Gastos Faltan Registrar Acreditaciones de <Mes> que están en <Mes sig>'.
  - El saldo a acreditar va dentro del encabezado de acreditaciones:
    'Acreditaciones de <Mes> Registradas en <Mes sig> ( 31.587.789,98 )'.
  - Las liquidaciones se escriben 'Liquidación aaaa-mm-dd al aaaa-mm-dd'.
  - En el mayor las acreditaciones dicen 'Acreditaciones Rappi mm/aaaa'.

Función principal: correr_conciliacion_rappi_ronda(...) -> (Excel, stats, pendientes)
"""

import io
import re
import itertools
import calendar
import unicodedata
from collections import Counter

import openpyxl
import pandas as pd
from openpyxl.styles import Font, PatternFill

from logica_atalaya import depurar_rappi, depurar_atalaya, cruzar_rappi_atalaya
# Mismos validadores de archivos que EASA: verifican que cada archivo sea el del
# casillero que lo recibió y toleran filas de título arriba de los encabezados.
from logica_conciliacion_rappi import _leer_reporte, _leer_pendientes


MESES_ES = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
            "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]

TOL_FACTURAS = 0.01
TOL_ACREDITACIONES = 5

PAT_LIQ_CUADRO = re.compile(r'^Liquidacion (\d{2})/(\d{2})/(\d{4}) a (\d{2})/(\d{2})/(\d{4})$')
PAT_FECHA_PAGO = re.compile(r'^Fecha de Pago (\d{2})/(\d{2})/(\d{4})$')
PAT_FACT_NUM = re.compile(r'^A-\d{4}-\d{8}$')
PAT_FACT_PRELIM = re.compile(r'^[A-Za-z]+\d+$')
PAT_LIQ_PENDIENTES = re.compile(
    r'^Liquidaci[oó]n\s+(?:(\d{2})/(\d{2})/(\d{4})\s+a\s+(\d{2})/(\d{2})/(\d{4})'
    r'|(\d{4})-(\d{2})-(\d{2})\s+al\s+(\d{4})-(\d{2})-(\d{2}))',
    re.IGNORECASE
)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

# Las búsquedas por combinación son exponenciales. Con pocas filas se prueban
# todas; arriba de este límite se acota el tamaño de la combinación para que la
# app no quede colgada, y queda avisado.
MAX_FILAS_COMBINACION_COMPLETA = 20
MAX_TAM_COMBINACION_RECORTADA = 4


def _avisar(avisos, mensaje):
    if avisos is not None:
        avisos.append(mensaje)


def _texto(df):
    """Columna de textos del pendientes (columna B), limpia."""
    return df.iloc[:, 1].apply(lambda x: "" if pd.isna(x) else str(x).strip())


def _filas_vacias(df, k):
    return pd.DataFrame([[None] * df.shape[1]] * k, columns=df.columns).astype(object)


def _bloque(df, items):
    """items: lista de (texto, monto). Devuelve filas con el formato del pendientes."""
    b = pd.DataFrame([[None] * df.shape[1] for _ in items], columns=df.columns).astype(object)
    b.iloc[:, 1] = [t for t, _ in items]
    b.iloc[:, 3] = [m for _, m in items]
    return b


def _fmt_ar(x):
    """1234567.8 -> '1.234.567,80'"""
    return f"{x:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")


def _normalizar(texto):
    return unicodedata.normalize("NFKD", str(texto)).encode("ascii", "ignore").decode().lower()


def clave_periodo(texto):
    """Clave día+mes de un período ('ddmm_ddmm'). Tolera los formatos del mayor:
    'Periodo 01-08-2026 al 02-08-20' (año truncado), 'DEL 27/07 AL 31/07',
    'Periodo del 29-06 al 30-06' y '2026-08-03 al 2026-08-09'."""
    t = str(texto)
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})\s*al\s*(\d{4})-(\d{2})-(\d{2})', t)
    if m:
        return f'{m.group(3)}{m.group(2)}_{m.group(6)}{m.group(5)}'
    f = re.findall(r'(\d{1,2})[/-](\d{1,2})(?:[/-]\d{2,4})?', t)
    if len(f) < 2:
        return None
    (d1, m1), (d2, m2) = f[0], f[1]
    return f'{int(d1):02d}{int(m1):02d}_{int(d2):02d}{int(m2):02d}'


def homologar_factura(nro):
    """'A-0002-01921352' / 'FC 2-1919486' / '0002-01921352' -> '0002-01921352'.
    Las preliminares ('VARACC21023015') devuelven None."""
    t = str(nro).strip()
    m = re.match(r'^A-(\d{4})-(\d{8})', t)
    if m:
        return f'{m.group(1)}-{m.group(2)}'
    m = re.match(r'^(?:FC\s*)?(\d{1,4})-(\d{1,8})$', t, re.IGNORECASE)
    if m:
        return f'{int(m.group(1)):04d}-{int(m.group(2)):08d}'
    return None


def liquidacion_iso(concepto):
    """'Liquidacion 17/08/2026 a 23/08/2026' -> 'Liquidación 2026-08-17 al 2026-08-23'."""
    m = PAT_LIQ_PENDIENTES.match(str(concepto).strip())
    if not m:
        return concepto
    g = m.groups()
    if g[0]:
        return f'Liquidación {g[2]}-{g[1]}-{g[0]} al {g[5]}-{g[4]}-{g[3]}'
    return f'Liquidación {g[6]}-{g[7]}-{g[8]} al {g[9]}-{g[10]}-{g[11]}'


def _subset(df, objetivo, col="Importe", tol=TOL_FACTURAS, avisos=None):
    """Combinación más chica de filas cuyo 'col' suma 'objetivo' (valor absoluto)."""
    regs = list(df.itertuples())
    obj = round(abs(objetivo), 2)

    tam_max = len(regs)
    if tam_max > MAX_FILAS_COMBINACION_COMPLETA:
        tam_max = MAX_TAM_COMBINACION_RECORTADA
        _avisar(avisos, f"Hay {len(regs)} filas para explicar {obj:,.2f}: se probaron "
                        f"combinaciones de hasta {tam_max} filas para no colgar el proceso.")

    for tam in range(1, tam_max + 1):
        for c in itertools.combinations(regs, tam):
            if abs(round(sum(getattr(r, col) for r in c), 2) - obj) <= tol:
                return [r.Index for r in c]
    return None


# ─────────────────────────────────────────────
# LIQUIDACIONES
# ─────────────────────────────────────────────

def importar_liquidaciones_ronda(archivos_liq):
    """De cada liquidación toma el período y la fecha de pago (hoja 'Resumen') y
    las órdenes (hoja 'Detalle', encabezados en fila 2, Tipo de transacción = ORDEN).
    Devuelve (info_liquidaciones, df_venta_rappi)."""
    info, dfs = [], []
    for archivo in archivos_liq:
        nombre = getattr(archivo, "name", "liquidación")
        if hasattr(archivo, "seek"):
            archivo.seek(0)
        wb = openpyxl.load_workbook(archivo, data_only=True, read_only=True)
        if "Resumen" not in wb.sheetnames:
            wb.close()
            raise ValueError(
                f"El archivo '{nombre}' no tiene la hoja 'Resumen': no parece una liquidación "
                f"de Rappi (hojas que trae: {', '.join(wb.sheetnames)})."
            )
        ws = wb["Resumen"]
        filas = list(ws.iter_rows(min_row=1, max_row=7, values_only=True))
        info.append({
            "inicio": pd.Timestamp(filas[2][3]),
            "fin": pd.Timestamp(filas[3][3]),
            "fecha_pago": pd.Timestamp(filas[4][3]),
            "id_pago": filas[6][3],
        })
        wb.close()
        if hasattr(archivo, "seek"):
            archivo.seek(0)
        try:
            det = pd.read_excel(archivo, sheet_name="Detalle", header=1)
        except ValueError as e:
            raise ValueError(f"El archivo '{nombre}' no tiene la hoja 'Detalle' con las órdenes.") from e
        if "Tipo de transacción" not in det.columns:
            raise ValueError(
                f"La hoja 'Detalle' de '{nombre}' no tiene la columna 'Tipo de transacción'."
            )
        det = det[det["Tipo de transacción"].astype(str).str.strip().str.upper() == "ORDEN"]
        dfs.append(det)
    df_venta_rappi = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    return pd.DataFrame(info), df_venta_rappi


def agregar_fecha_pago_cuadro(cuadro_conceptos, info_liquidaciones):
    """Si el cuadro de conceptos no trae las líneas 'Fecha de Pago dd/mm/aaaa', las
    inserta debajo de cada 'Liquidacion ...' tomándolas de las liquidaciones."""
    textos = cuadro_conceptos["Concepto"].apply(lambda x: "" if pd.isna(x) else str(x).strip())
    if textos.str.startswith("Fecha de Pago").any():
        return cuadro_conceptos

    mapa = {(r.inicio.date(), r.fin.date()): r.fecha_pago for r in info_liquidaciones.itertuples()}
    filas = []
    for _, row in cuadro_conceptos.iterrows():
        filas.append(row.to_dict())
        m = PAT_LIQ_CUADRO.match(str(row["Concepto"]).strip())
        if m:
            d1, m1, a1, d2, m2, a2 = m.groups()
            fp = mapa.get((pd.Timestamp(int(a1), int(m1), int(d1)).date(),
                           pd.Timestamp(int(a2), int(m2), int(d2)).date()))
            if fp is not None:
                filas.append({"Concepto": f"Fecha de Pago {fp:%d/%m/%Y}", "Monto": None})
    return pd.DataFrame(filas, columns=cuadro_conceptos.columns)


def detectar_mes_cuadro_conceptos(cuadro_conceptos):
    """Mes y año más frecuente entre los inicios de las liquidaciones del cuadro."""
    meses = []
    for t in cuadro_conceptos["Concepto"].apply(lambda x: "" if pd.isna(x) else str(x).strip()):
        m = PAT_LIQ_CUADRO.match(t)
        if m:
            meses.append((int(m.group(3)), int(m.group(2))))
    if not meses:
        return None, None
    (anio, mes), _ = Counter(meses).most_common(1)[0]
    return anio, mes


# ─────────────────────────────────────────────
# EXTRACCIÓN DEL CUADRO DE CONCEPTOS
# ─────────────────────────────────────────────

def _recorrer_cuadro(cuadro_conceptos):
    """Recorre el cuadro arrastrando tienda, liquidación y fecha de pago."""
    tienda = liq = fecha_pago = None
    for idx, row in cuadro_conceptos.iterrows():
        texto = str(row["Concepto"]).strip() if pd.notna(row["Concepto"]) else ""
        monto = row["Monto"]
        if texto == "Facturas no asignadas":
            tienda = liq = fecha_pago = None
            yield idx, "no_asignadas", texto, monto, tienda, liq, fecha_pago
            continue
        if not texto:
            continue
        m = PAT_LIQ_CUADRO.match(texto)
        if m:
            liq = texto
            continue
        m = PAT_FECHA_PAGO.match(texto)
        if m:
            d, mm, a = m.groups()
            fecha_pago = pd.Timestamp(int(a), int(mm), int(d))
            continue
        if texto == "Valor total a transferir":
            yield idx, "valor_total", texto, monto, tienda, liq, fecha_pago
            continue
        if PAT_FACT_NUM.match(texto) or PAT_FACT_PRELIM.match(texto):
            yield idx, "factura", texto, monto, tienda, liq, fecha_pago
            continue
        if pd.isna(monto):
            tienda = texto
            continue
        yield idx, "concepto", texto, monto, tienda, liq, fecha_pago


def _periodo_de(liq):
    m = PAT_LIQ_CUADRO.match(str(liq)) if liq else None
    if not m:
        return None
    d1, m1, a1, d2, m2, a2 = m.groups()
    return f'Periodo {d1}-{m1}-{a1} al {d2}-{m2}-{a2}'


def extraer_facturas_cuadro_conceptos(cuadro_conceptos):
    filas = [{
        "Orden": idx, "Nro Factura": texto, "Importe": round(float(monto), 2),
        "Formato": "numerico" if PAT_FACT_NUM.match(texto) else "cuentas_al_dia",
        "Periodo": _periodo_de(liq), "Fecha de Pago": fp,
    } for idx, tipo, texto, monto, _, liq, fp in _recorrer_cuadro(cuadro_conceptos) if tipo == "factura"]
    return pd.DataFrame(filas, columns=["Orden", "Nro Factura", "Importe", "Formato", "Periodo", "Fecha de Pago"])


def extraer_conceptos_cuadro_conceptos(cuadro_conceptos):
    filas = [{
        "Orden": idx, "Concepto": texto, "Monto": round(float(monto), 2),
        "Tienda": tienda, "Liquidacion": liq, "Fecha de Pago": fp,
    } for idx, tipo, texto, monto, tienda, liq, fp in _recorrer_cuadro(cuadro_conceptos) if tipo == "concepto"]
    return pd.DataFrame(filas, columns=["Orden", "Concepto", "Monto", "Tienda", "Liquidacion", "Fecha de Pago"])


def extraer_liquidaciones_cuadro_conceptos(cuadro_conceptos):
    filas = [{
        "Orden": idx, "Tienda": tienda, "Concepto": liq, "Fecha de Pago": fp,
        "Importe": round(float(monto), 2),
    } for idx, tipo, texto, monto, tienda, liq, fp in _recorrer_cuadro(cuadro_conceptos) if tipo == "valor_total"]
    return pd.DataFrame(filas, columns=["Orden", "Tienda", "Concepto", "Fecha de Pago", "Importe"])


# ─────────────────────────────────────────────
# EXTRACCIÓN DEL PENDIENTES DEL MES ANTERIOR
# ─────────────────────────────────────────────

def extraer_facturas_pendientes(pendientes):
    """Facturas pendientes del mes anterior. Acepta 'A-0002-01903030',
    'FC 2-1919486', preliminares ('VARACC21023015'), con o sin
    'Periodo dd-mm-aaaa al dd-mm-aaaa' pegado. Las líneas que empiezan con
    'Factura ...' o 'Facturas saldadas ...' son históricas y no se toman."""
    patron = re.compile(
        r'^(A-\d{4}-\d{8}|FC\s*\d{1,4}-\d{1,8}|[A-Za-z]+\d+)'
        r'(?:\s+(Periodo\s+(?:del\s+)?\d{2}-\d{2}-\d{4}\s+al\s+\d{2}-\d{2}-\d{4}))?$',
        re.IGNORECASE
    )
    texto = _texto(pendientes)
    monto = pd.to_numeric(pendientes.iloc[:, 3], errors="coerce")
    filas = []
    for i, t in texto.items():
        m = patron.match(t)
        if m and pd.notna(monto[i]):
            periodo = re.sub(r'\bdel\s+', '', m.group(2)) if m.group(2) else None
            filas.append({
                "Fila": i, "Nro Factura": m.group(1), "Importe": round(abs(monto[i]), 2),
                "Formato": "cuentas_al_dia" if PAT_FACT_PRELIM.match(m.group(1)) else "numerico",
                "Periodo": periodo,
            })
    return pd.DataFrame(filas, columns=["Fila", "Nro Factura", "Importe", "Formato", "Periodo"])


def extraer_acreditaciones_pendientes(pendientes):
    """Liquidaciones del bloque 'Acreditaciones de <Mes> Registradas en <Mes sig>'.
    Los viejos 'Acreditaciones Faltan Registrar en Mayor ...' NO se toman (tienen
    el mismo formato de línea y romperían el cruce)."""
    texto = _texto(pendientes)
    monto = pd.to_numeric(pendientes.iloc[:, 3], errors="coerce")
    encabezados = [i for i, t in texto.items() if re.match(r'^Acreditaciones de \w+\s+Registradas en', t, re.I)]
    if not encabezados:
        return pd.DataFrame(columns=["Fila", "Concepto", "Importe"])
    filas, i = [], encabezados[-1] + 1
    while i < len(pendientes):
        t = texto[i]
        if t and not PAT_LIQ_PENDIENTES.match(t):
            break
        if t:
            filas.append({"Fila": i, "Concepto": t, "Importe": round(abs(monto[i]), 2)})
        i += 1
    return pd.DataFrame(filas, columns=["Fila", "Concepto", "Importe"])


# ─────────────────────────────────────────────
# VENTAS
# ─────────────────────────────────────────────

_MESES_ABR = {'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may': '05', 'jun': '06',
              'jul': '07', 'ago': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12'}


def _parsear_fecha_rappi(texto):
    if pd.isna(texto):
        return pd.NaT
    if isinstance(texto, pd.Timestamp):
        return texto
    t = str(texto).strip().lower()
    p = re.search(r'\d', t)
    if not p:
        return pd.NaT
    t = t[p.start():]
    m = re.match(r'(\d{1,2})\s+([a-zñáéíóú]+)\.\s+(\d{4}),\s+(\d{1,2}):(\d{2}):(\d{2})\s*(a|p)\.\s*m\.', t)
    if not m:
        return pd.to_datetime(t, errors="coerce")
    d, mb, a, h, mi, s, per = m.groups()
    h = int(h)
    if per == 'p' and h != 12:
        h += 12
    if per == 'a' and h == 12:
        h = 0
    return pd.Timestamp(f'{a}-{_MESES_ABR[mb[:3]]}-{d.zfill(2)} {h:02d}:{mi}:{s}')


def depurar_venta_rappi(df_venta_rappi):
    """Fecha / Hora, IDs como texto y 'Venta Neta' = Venta Bruta + Descuento de
    Producto asumido por el aliado (el descuento viene NEGATIVO en el reporte)."""
    df = df_venta_rappi.copy()
    fh = df["Fecha de creación orden"].apply(_parsear_fecha_rappi)
    df["Fecha"] = fh.dt.normalize()
    df["Hora"] = fh.dt.strftime("%H:%M:%S")
    for col in ["ID de la órden", "ID del paidlot", "ID de la tienda"]:
        if col in df:
            df[col] = df[col].apply(lambda x: str(int(x)) if pd.notna(x) else None)
    df["Venta Neta"] = (df["Venta Bruta"] + df["Descuento de Producto asumido por el aliado"]).round(2)
    for col in df.select_dtypes(include="float64").columns:
        df[col] = df[col].round(2)
    return df


def mapear_local(texto):
    """Local a partir de 'Nombre de la tienda' (Rappi) o 'Establecimiento' (HIO)."""
    t = _normalizar(texto)
    for clave, local in [("paolo", "Paolo"), ("orno", "Orno"), ("nacha", "Nacha"), ("koko", "Koko"),
                         ("crep", "Creperie"), ("carne", "Carne"), ("quotidiano", "Quotidiano"),
                         ("antiche", "Antiche"), ("guapaletas", "Guapaletas"), ("atalaya", "Atalaya")]:
        if clave in t:
            return local
    return None


def armar_df_hio(hio_documento, hio_metodo):
    """Documentos de HIO pagados con RAPPI: documento ∩ método (uno a uno por Serie / Número)."""
    col_doc = [c for c in hio_documento.columns if "Serie" in str(c)][0]
    col_met = [c for c in hio_metodo.columns if "Serie" in str(c)][0]
    doc = hio_documento.copy()
    met = hio_metodo[hio_metodo["Medio Pago"].astype(str).str.upper().str.contains("RAPPI", na=False)].copy()
    doc["_serie"] = doc[col_doc].astype(str).str.strip()
    met["_serie"] = met[col_met].astype(str).str.strip()
    doc = doc[doc["_serie"].isin(set(met["_serie"]))]
    doc["_rk"] = doc.groupby("_serie").cumcount()
    met["_rk"] = met.groupby("_serie").cumcount()
    df_hio = doc.merge(met[["_serie", "_rk"]], on=["_serie", "_rk"], how="inner").drop(columns=["_serie", "_rk"])
    df_hio["Fecha"] = pd.to_datetime(df_hio["Fecha"]).dt.normalize()
    df_hio["Local"] = df_hio["Establecimiento"].apply(mapear_local)
    return df_hio


def cruzar_hio(df_venta_rappi_dep, df_hio):
    """Cruce 1: 'ID de la órden' vs 'Localizador'. Cruce 2 (lo que no tiene
    localizador): Fecha + Local + monto, desempatando por la hora más cercana."""
    def _id(x):
        try:
            return str(int(float(x)))
        except (TypeError, ValueError):
            return None

    def _td(x):
        try:
            return pd.to_timedelta(str(x))
        except (TypeError, ValueError):
            return pd.NaT

    rappi = df_venta_rappi_dep[~df_venta_rappi_dep["Nombre de la tienda"].astype(str)
                               .str.contains("atalaya|dean", case=False, na=False)].copy()
    rappi["Local"] = rappi["Nombre de la tienda"].apply(mapear_local)
    hio = df_hio.copy()
    rappi["_id"] = rappi["ID de la órden"].apply(_id)
    hio["_id"] = hio["Localizador"].apply(_id)

    ventas_hio = hio.dropna(subset=["_id"]).drop_duplicates("_id").set_index("_id")["Venta"]
    en_ambos = rappi["_id"].isin(ventas_hio.index)
    match_hio_rappi1 = rappi[en_ambos].copy()
    match_hio_rappi1["Importe Hio"] = match_hio_rappi1["_id"].map(ventas_hio)
    match_hio_rappi1["Diferencia"] = (match_hio_rappi1["Importe Hio"] - match_hio_rappi1["Venta Neta"]).round(2)
    match_hio = hio[hio["_id"].isin(set(match_hio_rappi1["_id"]))].copy()

    falta_hio1 = rappi[~en_ambos].copy()
    falta_rappi_hio1 = hio[~hio["_id"].isin(set(match_hio_rappi1["_id"]))].copy()

    falta_hio1["_h"] = falta_hio1["Hora"].apply(_td)
    falta_rappi_hio1["_h"] = falta_rappi_hio1["Hora"].apply(_td)
    falta_hio1["_m"] = False
    falta_rappi_hio1["_m"] = False
    for i, r in falta_rappi_hio1.iterrows():
        c = falta_hio1[(falta_hio1["Fecha"] == r["Fecha"]) & (falta_hio1["Local"] == r["Local"]) &
                       (falta_hio1["Venta Neta"].round(2) == round(r["Venta"], 2)) & (~falta_hio1["_m"])]
        if c.empty:
            continue
        j = c.index[0] if len(c) == 1 else (c["_h"] - r["_h"]).abs().idxmin()
        falta_rappi_hio1.at[i, "_m"] = True
        falta_hio1.at[j, "_m"] = True

    aux = ["_id", "_h", "_m"]
    match_hio2 = falta_rappi_hio1[falta_rappi_hio1["_m"]].drop(columns=aux)
    match_hio_rappi2 = falta_hio1[falta_hio1["_m"]].drop(columns=aux)
    falta_rappi_hio2 = falta_rappi_hio1[~falta_rappi_hio1["_m"]].drop(columns=aux)
    falta_hio2 = falta_hio1[~falta_hio1["_m"]].drop(columns=aux)
    match_hio_rappi1 = match_hio_rappi1.drop(columns=["_id"])
    match_hio = match_hio.drop(columns=["_id"])

    total_rappi = round(rappi["Venta Neta"].sum(), 2)
    total_hio = round(df_hio["Venta"].sum(), 2)
    return {
        "match_hio": match_hio, "match_hio_rappi1": match_hio_rappi1,
        "match_hio2": match_hio2, "match_hio_rappi2": match_hio_rappi2,
        "falta_rappi_hio2": falta_rappi_hio2, "falta_hio2": falta_hio2,
        "venta_rappi": total_rappi, "venta_sistema": total_hio,
        "diferencia": round(total_rappi - total_hio, 2),
    }


def cruzar_atalaya(df_venta_rappi, reporte_atalaya):
    """Usa logica_atalaya. La fila de total del reporte (sin Fecha) se descarta."""
    rappi = df_venta_rappi[df_venta_rappi["Nombre de la tienda"].astype(str)
                           .str.contains("atalaya", case=False, na=False)].copy()
    atalaya = reporte_atalaya.copy()
    # El cruce empareja por fecha exacta contra la fecha de Rappi (sin hora), así
    # que la del reporte se normaliza: si ya viene como fecha, no cambia nada.
    atalaya["Fecha"] = pd.to_datetime(atalaya["Fecha"], errors="coerce").dt.normalize()
    atalaya = atalaya[atalaya["Fecha"].notna()].copy()
    rappi_dep = depurar_rappi(rappi)
    atalaya_dep = depurar_atalaya(atalaya)
    match_atalaya, match_rappi, falta_rappi, falta_atalaya = cruzar_rappi_atalaya(rappi_dep, atalaya_dep)
    total_rappi = round(rappi_dep["Ventas Totales"].sum(), 2)
    total_sistema = round(atalaya_dep.loc[atalaya_dep["Medio Pago"].astype(str)
                                          .str.contains("RAPPI", na=False), "CON IVA"].sum(), 2)
    return {
        "match_atalaya": match_atalaya, "match_rappi_atalaya": match_rappi,
        "falta_rappi_atalaya": falta_rappi, "falta_atalaya": falta_atalaya,
        "venta_rappi": total_rappi, "venta_sistema": total_sistema,
        "diferencia": round(total_rappi - total_sistema, 2),
    }


# ─────────────────────────────────────────────
# FACTURAS vs MAYOR
# ─────────────────────────────────────────────

def preparar_mayor(recaudacion):
    """Filas de facturas del mayor (sin saldo anterior ni acreditaciones)."""
    df = recaudacion.copy()
    comentario = df["Comentario"].astype(str)
    es_factura = ~comentario.str.contains(r'acreditaci|saldo anterior|^ventas', case=False, regex=True)
    df = df[es_factura & (df["Haber"].fillna(0) != 0)].copy()
    df["_clave"] = df["Comentario"].apply(clave_periodo)
    df["_sf"] = df["Su factura"].apply(lambda x: homologar_factura(x) if pd.notna(x) else None)
    df["Haber"] = df["Haber"].round(2)
    return df


def cruzar_facturas(mayor, facturas, consumidas=None, etiqueta="", avisos=None):
    """
    Cruza facturas contra el mayor sin reutilizar filas ya consumidas.
      Etapa 1: por período (suma de facturas vs suma del mayor del período).
               Si el mayor tiene de menos, busca qué factura(s) explican la diferencia.
      Etapa 2: por número de factura ('Su factura').
      Etapa 3: contra créditos agrupados sin 'Su factura' (combinación de facturas).
      Etapa 4: preliminares por importe contra filas con número desconocido.
    Devuelve match, falta, diferencias y el set de filas del mayor consumidas.
    """
    consumidas = set() if consumidas is None else set(consumidas)
    fac = facturas.reset_index(drop=True).copy()
    if "Periodo" not in fac:
        fac["Periodo"] = None
    fac["_clave"] = fac["Periodo"].apply(lambda p: clave_periodo(p) if isinstance(p, str) and p else None)
    fac["_sf"] = fac["Nro Factura"].apply(homologar_factura)
    fac["_estado"] = None
    fac["_etapa"] = None
    fac["Factura real"] = None
    diferencias = []

    def disponibles():
        return mayor[~mayor.index.isin(consumidas)]

    for clave, grupo in fac[fac["_clave"].notna()].groupby("_clave"):
        filas = disponibles()[disponibles()["_clave"] == clave]
        if filas.empty:
            continue
        d = round(filas["Haber"].sum() - grupo["Importe"].sum(), 2)
        if abs(d) <= TOL_FACTURAS:
            fac.loc[grupo.index, ["_estado", "_etapa"]] = ["match", 1]
            consumidas |= set(filas.index)
        elif d < 0:
            idx = _subset(grupo, d, avisos=avisos)
            if idx is not None:
                fac.loc[[i for i in grupo.index if i not in idx], ["_estado", "_etapa"]] = ["match", 1]
                consumidas |= set(filas.index)

    for i, r in fac[fac["_estado"].isna() & fac["_sf"].notna()].iterrows():
        filas = disponibles()[disponibles()["_sf"] == r["_sf"]]
        if filas.empty:
            continue
        d = round(filas["Haber"].sum() - r["Importe"], 2)
        fac.loc[i, ["_estado", "_etapa"]] = ["match" if abs(d) <= TOL_FACTURAS else "diferencia", 2]
        if abs(d) > TOL_FACTURAS:
            diferencias.append({"Periodo": r["Periodo"], "Nro Factura": r["Nro Factura"], "Diferencia": d})
        consumidas |= set(filas.index)

    agrupados = disponibles()[disponibles()["_sf"].isna() & disponibles()["_clave"].notna()]
    for clave, filas in agrupados.groupby("_clave"):
        pool = fac[fac["_estado"].isna()]
        if pool.empty:
            break
        mismo = pool[pool["_clave"] == clave]
        idx = _subset(mismo if len(mismo) else pool, filas["Haber"].sum(), avisos=avisos)
        if idx is None and len(mismo):
            idx = _subset(pool, filas["Haber"].sum(), avisos=avisos)
        if idx is not None:
            fac.loc[idx, ["_estado", "_etapa"]] = ["match", 3]
            consumidas |= set(filas.index)

    conocidas = set(fac["_sf"].dropna())
    desconocidas = disponibles()[disponibles()["_sf"].notna() & ~disponibles()["_sf"].isin(conocidas)]
    for i, r in fac[fac["_estado"].isna() & (fac["_sf"].isna())].iterrows():
        cand = desconocidas[~desconocidas.index.isin(consumidas) &
                            ((desconocidas["Haber"] - r["Importe"]).abs() <= TOL_FACTURAS)]
        if len(cand) > 1 and r["_clave"]:
            cand = cand[cand["_clave"] == r["_clave"]]
        if len(cand) == 1:
            j = cand.index[0]
            fac.loc[i, ["_estado", "_etapa", "Factura real"]] = ["match", 4, cand.at[j, "_sf"]]
            consumidas.add(j)

    match = fac[fac["_estado"] == "match"]
    falta = fac[fac["_estado"].isna()]
    return match, falta, pd.DataFrame(diferencias, columns=["Periodo", "Nro Factura", "Diferencia"]), consumidas


def facturas_saldadas_no_cruzadas(mayor, falta_pendientes, consumidas):
    """
    Lo que quedó en el mayor sin consumir después de los dos cruces son facturas
    registradas que no estaban en el pendientes ni en el cuadro.
      - Créditos agrupados que contienen facturas pendientes más una factura de
        más: se asignan las pendientes y el sobrante va como no cruzada.
      - Filas con 'Su factura' desconocida: van con su número.
    Devuelve (lista de (texto, monto) para el pendientes, filas de falta que se
    dieron por pagadas).
    """
    sobrantes, pagadas = [], []
    consumidas = set(consumidas)
    restantes = mayor[~mayor.index.isin(consumidas)]

    for clave, filas in restantes[restantes["_sf"].isna() & restantes["_clave"].notna()].groupby("_clave"):
        pool = falta_pendientes[~falta_pendientes.index.isin(pagadas)]
        mismo = pool[pool["_clave"] == clave]
        pool = mismo if len(mismo) else pool[pool["_clave"].isna()]
        total = round(filas["Haber"].sum(), 2)
        mejor = None
        if len(pool) <= 20:
            for tam in range(len(pool), 0, -1):
                for c in itertools.combinations(list(pool.itertuples()), tam):
                    s = round(sum(r.Importe for r in c), 2)
                    if s <= total + TOL_FACTURAS and (mejor is None or s > mejor[0]):
                        mejor = (s, [r.Index for r in c])
                if mejor:
                    break
        asignado = mejor[0] if mejor else 0
        if mejor:
            pagadas += mejor[1]
        consumidas |= set(filas.index)
        sobrante = round(total - asignado, 2)
        if sobrante > TOL_FACTURAS:
            d1, m1, d2, m2 = clave[0:2], clave[2:4], clave[5:7], clave[7:9]
            sobrantes.append((f"Sin identificar Periodo {d1}-{m1} al {d2}-{m2}", sobrante))

    for _, r in mayor[~mayor.index.isin(consumidas) & mayor["_sf"].notna()].iterrows():
        sobrantes.append((f"A-{r['_sf']}", round(r["Haber"], 2)))

    return sobrantes, pagadas


# ─────────────────────────────────────────────
# ACREDITACIONES
# ─────────────────────────────────────────────

def cruzar_acreditaciones(recaudacion, liquidaciones, acred_mes_anterior, anio, mes, avisos=None):
    """
    Liquidaciones cobradas en el mes (Fecha de Pago <= fin de mes) + pendientes
    del mes anterior vs 'Acreditaciones Rappi mm/aaaa' del mayor (tolera $5).
    Las liquidaciones con Valor total negativo no se cruzan (no se registran):
    van aparte como acreditaciones negativas.
    Si no cierra, busca qué liquidación(es) explican la diferencia -> faltante.
    """
    patron = re.compile(rf'acreditaci(?:o|ó)n(?:es)?\s+rappi\s+{mes:02d}[-/]{anio}', re.IGNORECASE)
    total_mayor = round(recaudacion.loc[recaudacion["Comentario"].astype(str)
                                        .str.contains(patron, na=False), "Haber"].sum(), 2)
    fin_mes = pd.Timestamp(anio, mes, calendar.monthrange(anio, mes)[1])

    negativas = liquidaciones[liquidaciones["Importe"] < 0]
    positivas = liquidaciones[liquidaciones["Importe"] >= 0]
    corriente = positivas[positivas["Fecha de Pago"] <= fin_mes]
    siguiente = positivas[positivas["Fecha de Pago"] > fin_mes]
    anterior = acred_mes_anterior.reset_index(drop=True)

    esperado = round(corriente["Importe"].sum() + anterior["Importe"].sum(), 2)
    diferencia = round(total_mayor - esperado, 2)

    faltantes = pd.DataFrame(columns=["Concepto", "Importe"])
    if abs(diferencia) > TOL_ACREDITACIONES:
        pool = pd.concat([anterior[["Concepto", "Importe"]], corriente[["Concepto", "Importe"]]], ignore_index=True)
        idx = _subset(pool, diferencia, tol=TOL_ACREDITACIONES, avisos=avisos)
        if idx is not None:
            faltantes = pool.loc[idx]
        else:
            _avisar(avisos, f"No se encontró qué liquidación explica la diferencia de "
                            f"acreditaciones de {diferencia:,.2f}.")

    return {"corriente": corriente, "siguiente": siguiente, "faltantes": faltantes,
            "negativas": negativas, "total_mayor": total_mayor, "esperado": esperado,
            "diferencia": diferencia}


# ─────────────────────────────────────────────
# ARMADO DEL PENDIENTES
# ─────────────────────────────────────────────

def _posicion(textos_en_minuscula, prefijo, nombre_humano):
    """Índice de la primera línea del cuadro que empieza con el prefijo."""
    coincidencias = textos_en_minuscula[textos_en_minuscula.str.startswith(prefijo)]
    if coincidencias.empty:
        raise ValueError(
            f"El cuadro de pendientes no tiene la línea '{nombre_humano}': sin ella no se "
            "puede ubicar dónde van los bloques del mes."
        )
    return coincidencias.index[0]


def agrupar_conceptos(conceptos):
    """Suma por concepto, orden alfabético (sin distinguir mayúsculas), signo invertido."""
    if conceptos.empty:
        return []
    g = conceptos.groupby("Concepto", as_index=False)["Monto"].sum()
    g["Monto"] = g["Monto"].round(2)
    g = g.iloc[sorted(range(len(g)), key=lambda i: g["Concepto"].iloc[i].lower())]
    return [(r.Concepto, -round(r.Monto, 2)) for r in g.itertuples() if round(r.Monto, 2) != 0]


def _texto_factura(fila):
    return f"{fila['Nro Factura']} {fila['Periodo']}" if fila.get("Periodo") else str(fila["Nro Factura"])


def armar_pendientes(pendientes_mes_anterior, anio, mes, filas_a_sacar, sobrantes,
                     conceptos, falta_facturas_mes, acreditaciones, ventas_sistema,
                     diferencias_venta, recaudacion, avisos=None):
    """Genera el pendientes del mes a partir del del mes anterior."""
    df = pendientes_mes_anterior.reset_index(drop=True).copy()
    mes_nombre = MESES_ES[mes]
    mes_ant = MESES_ES[12] if mes == 1 else MESES_ES[mes - 1]
    mes_sig = MESES_ES[1] if mes == 12 else MESES_ES[mes + 1]
    etiqueta = f"{mes_nombre} {anio}"
    fin_mes = pd.Timestamp(anio, mes, calendar.monthrange(anio, mes)[1])

    if _texto(df).eq(f"Gastos Faltan Registrar {etiqueta}").any():
        _avisar(avisos, f"{etiqueta} ya estaba en el cuadro de pendientes, no se volvió a agregar.")
        return df

    # Fecha de cierre (primera celda con fecha del encabezado)
    for i in range(min(5, len(df))):
        v = df.iat[i, 1]
        if isinstance(v, (pd.Timestamp,)) or (hasattr(v, "year") and not isinstance(v, str)):
            df.iat[i, 1] = fin_mes.to_pydatetime()
            break

    # Facturas y acreditaciones del mes anterior que ya se resolvieron
    df = df.drop(index=[i for i in filas_a_sacar if i in df.index]).reset_index(drop=True)
    texto = _texto(df)
    bajo = texto.str.lower()
    pos_total = _posicion(bajo, "total saldo del mes", "Total Saldo del mes")
    pos_dif = _posicion(bajo, "diferencia para llegar al saldo", "Diferencia para llegar al saldo")

    # Bloque 'Gastos Faltan Registrar ' del mes anterior: sube renombrado
    viejo = None
    ini = next((i for i in range(pos_total, pos_dif) if bajo[i].startswith("gastos faltan registrar")), None)
    if ini is not None:
        fin = ini + 1
        while fin < pos_dif and texto[fin]:
            fin += 1
        viejo = df.iloc[ini:fin].copy()
        viejo.iat[0, 1] = f"Gastos Faltan Registrar Acreditaciones de {mes_ant} que están en {mes_nombre}"

    conc_mes = conceptos[conceptos["Fecha de Pago"] <= fin_mes]
    conc_sig = conceptos[conceptos["Fecha de Pago"] > fin_mes]
    ff = falta_facturas_mes
    fact_mes = [(_texto_factura(r), -r["Importe"]) for _, r in ff[ff["Fecha de Pago"] <= fin_mes].iterrows()]
    fact_sig = [(_texto_factura(r), -r["Importe"]) for _, r in ff[ff["Fecha de Pago"] > fin_mes].iterrows()]

    antes = df.iloc[:pos_total].copy()
    while len(antes) and antes.iloc[-1].isna().all():
        antes = antes.iloc[:-1]

    partes = [antes]
    if viejo is not None:
        partes += [_filas_vacias(df, 1), viejo]
    if sobrantes:
        partes += [_filas_vacias(df, 1), _bloque(df, [("Factura saldada no cruzada", None)] + sobrantes)]
    partes += [_filas_vacias(df, 2),
               _bloque(df, [(f"Gastos Faltan Registrar {etiqueta}", None)] + agrupar_conceptos(conc_mes) + fact_mes)]
    negativas = acreditaciones["negativas"]
    if len(negativas):
        partes += [_filas_vacias(df, 1), _bloque(df, [(f"Acreditaciones Negativas {etiqueta}", None)] + [
            (f"Acreditaciones Negativas {liquidacion_iso(r.Concepto)}", -r.Importe) for r in negativas.itertuples()])]
    partes += [_filas_vacias(df, 3), df.iloc[[pos_total]].copy(), _filas_vacias(df, 2)]

    liqs = ([(liquidacion_iso(r.Concepto), -r.Importe) for r in acreditaciones["faltantes"].itertuples()] +
            [(liquidacion_iso(r.Concepto), -r.Importe) for r in acreditaciones["siguiente"].itertuples()])
    gastos_sig = agrupar_conceptos(conc_sig) + fact_sig
    saldo_acreditar = round(-sum(m for _, m in liqs) - sum(m for _, m in gastos_sig), 2)
    partes += [
        _bloque(df, [(f"Acreditaciones de {mes_nombre} Registradas en {mes_sig} ( {_fmt_ar(saldo_acreditar)} )", None)]),
        _filas_vacias(df, 1), _bloque(df, liqs), _filas_vacias(df, 2),
        _bloque(df, [("Gastos Faltan Registrar ", None)] + gastos_sig), _filas_vacias(df, 2),
    ]

    # Desde 'Diferencia para llegar al saldo': bloque de diferencias de venta del mes
    resto = df.iloc[pos_dif:].reset_index(drop=True)
    resto.iat[0, 1] = "Diferencia para llegar al saldo"
    bajo_r = _texto(resto).str.lower()
    ult_cent = [i for i, v in bajo_r.items() if v.startswith("diferencia de centavo")]
    corte = ult_cent[-1] + 2 if ult_cent else len(resto)
    bloque_venta = _bloque(resto, [(etiqueta, None)] + list(diferencias_venta) + [("Diferencia de centavo", 0)])
    resto = pd.concat([resto.iloc[:corte], bloque_venta, resto.iloc[corte:]], ignore_index=True)

    out = pd.concat(partes + [resto], ignore_index=True)

    # Saldo según mayor + ventas del mes si no están registradas
    bajo = _texto(out).str.lower()
    pos_saldo = _posicion(bajo, "saldo según mayor contable", "Saldo según Mayor Contable")
    out.iat[pos_saldo, 1] = f"Saldo según Mayor Contable al {fin_mes:%d/%m/%Y}"
    out.iat[pos_saldo, 3] = round(float(recaudacion["Saldo"].dropna().iloc[-1]), 2)
    comentario = recaudacion["Comentario"].astype(str)
    lineas = []
    for nombre, importe in ventas_sistema:
        etq = f"Ventas {mes:02d}-{anio} {nombre}"
        if not comentario.str.contains(re.escape(etq), case=False).any():
            lineas.append((etq, round(importe, 2)))
    if lineas:
        out = pd.concat([out.iloc[:pos_saldo + 1], _bloque(out, lineas), out.iloc[pos_saldo + 1:]], ignore_index=True)

    return recalcular_totales(out)


def recalcular_totales(out):
    """Saldo a conciliar -> Total Saldo del mes -> Diferencia para llegar al saldo
    -> Diferencia de centavo (plug) -> checksum final (queda en ~0)."""
    bajo = _texto(out).str.lower()
    valores = lambda: pd.to_numeric(out.iloc[:, 3], errors="coerce")

    def pos(prefijo, ultima=False):
        l = [i for i, v in bajo.items() if v.startswith(prefijo)]
        return (l[-1] if ultima else l[0]) if l else None

    p_saldo = pos("saldo según mayor contable")
    p_conciliar = pos("saldo a conciliar")
    p_total = pos("total saldo del mes")
    p_dif = pos("diferencia para llegar al saldo")
    p_cent = pos("diferencia de centavo", ultima=True)

    if p_conciliar is not None:
        out.iat[p_conciliar, 3] = round(valores()[p_saldo:p_conciliar].sum(), 2)
        desde_total = p_conciliar
    else:
        desde_total = p_saldo
    out.iat[p_total, 3] = round(valores()[desde_total:p_total].sum(), 2)
    out.iat[p_dif, 3] = round(valores()[p_total:p_dif].sum(), 2)
    if p_cent is not None and p_cent > p_dif:
        out.iat[p_cent, 3] = round(-valores()[p_dif:p_cent].sum(), 2)
    ultima = out.index[-1]
    if not _texto(out).iloc[-1]:
        out.iat[ultima, 3] = round(valores()[p_dif:ultima].sum(), 6)
    return out


# ─────────────────────────────────────────────
# EXPORTACIÓN
# ─────────────────────────────────────────────

def _es_titulo(texto):
    if not isinstance(texto, str):
        return hasattr(texto, "year")
    t = texto.strip().lower()
    if re.match(rf"^({'|'.join(m.lower() for m in MESES_ES[1:])})(\s+\d{{4}})?$", t):
        return True
    return t.startswith(("gastos faltan registrar", "acreditaciones", "total saldo del mes",
                         "diferencia de venta y extracto", "gastos no pertenecientes"))


def exportar_conciliacion(pendientes, hojas_datos):
    """Libro con 'Conciliación' primero (títulos en negrita, Total Saldo del mes en
    amarillo) y después las hojas de datos, en el orden recibido."""
    amarillo = PatternFill(start_color="FFFFFF00", end_color="FFFFFF00", fill_type="solid")
    negrita = Font(bold=True)

    def nombre_valido(n):
        return re.sub(r'[:\\/?*\[\]]', '-', n).strip()[:31]

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pendientes.to_excel(writer, sheet_name="Conciliación", index=False, header=False)
        for nombre, df in hojas_datos:
            df.to_excel(writer, sheet_name=nombre_valido(nombre), index=False)
            ws = writer.sheets[nombre_valido(nombre)]
            for celda in ws[1]:
                celda.font = negrita
            ws.freeze_panes = "A2"

        ws = writer.sheets["Conciliación"]
        for fila in range(1, ws.max_row + 1):
            valor = ws.cell(fila, 2).value
            if valor is not None and _es_titulo(valor):
                ws.cell(fila, 2).font = negrita
            if isinstance(valor, str) and valor.strip().lower().startswith("total saldo del mes"):
                for col in (2, 4):
                    ws.cell(fila, col).fill = amarillo
            if isinstance(ws.cell(fila, 4).value, (int, float)):
                ws.cell(fila, 4).number_format = "#,##0.00"
        ws.column_dimensions["B"].width = 80
        ws.column_dimensions["D"].width = 18
    buf.seek(0)
    return buf


# ─────────────────────────────────────────────
# FUNCIÓN PRINCIPAL
# ─────────────────────────────────────────────

def correr_conciliacion_rappi_ronda(archivos_liq, archivo_cuadro_conceptos, archivo_recaudacion,
                                    archivo_hio_documento, archivo_hio_metodo,
                                    archivo_reporte_atalaya, archivo_pendientes_mes_anterior):
    """
    Corre la conciliación completa de Ronda.
    Devuelve (buffer del Excel, stats, pendientes nuevo). El Excel trae la hoja
    'Conciliación' (el pendientes nuevo) más el mayor, los reportes y los cruces.
    """
    avisos = []

    info_liq, df_venta_rappi = importar_liquidaciones_ronda(archivos_liq)
    if df_venta_rappi.empty:
        raise ValueError("Las liquidaciones no traen órdenes en la hoja 'Detalle'.")

    cuadro = _leer_reporte(archivo_cuadro_conceptos, 'Cuadro de conceptos',
                           [('concepto',), ('monto',)])
    cuadro = agregar_fecha_pago_cuadro(cuadro, info_liq)
    anio, mes = detectar_mes_cuadro_conceptos(cuadro)
    if anio is None:
        raise ValueError("No se pudo detectar el mes: el cuadro de conceptos no tiene liquidaciones.")

    recaudacion = _leer_reporte(archivo_recaudacion, 'Cuenta recaudación Rappi (mayor)',
                                [('comentario',), ('haber',), ('saldo',), ('sufactura',)])
    recaudacion.columns = [str(c).strip() for c in recaudacion.columns]
    pendientes = _leer_pendientes(archivo_pendientes_mes_anterior,
                                  'Cuadro de pendientes del mes anterior')

    # Ventas
    df_venta_rappi_dep = depurar_venta_rappi(df_venta_rappi)
    df_hio = armar_df_hio(
        _leer_reporte(archivo_hio_documento, 'Reporte HIO por documento',
                      [('serie',), ('establecimiento',), ('localizador',), ('venta',), ('fecha',)]),
        _leer_reporte(archivo_hio_metodo, 'Reporte HIO por método de pago',
                      [('serie',), ('medio', 'pago')]),
    )
    reporte_atalaya = _leer_reporte(archivo_reporte_atalaya, 'Reporte Atalaya',
                                    [('fecha',), ('coniva',), ('medio', 'pago')])

    hio = cruzar_hio(df_venta_rappi_dep, df_hio)
    atalaya = cruzar_atalaya(df_venta_rappi, reporte_atalaya)

    sin_local = sorted({str(t) for t in df_hio.loc[df_hio["Local"].isna(), "Establecimiento"]})
    if sin_local:
        _avisar(avisos, "Locales de HIO sin equivalencia en Rappi (no entran al segundo cruce): "
                        + ", ".join(sin_local) + ".")

    # Facturas
    facturas_mes = extraer_facturas_cuadro_conceptos(cuadro)
    facturas_pend = extraer_facturas_pendientes(pendientes)
    mayor = preparar_mayor(recaudacion)
    match_mes, falta_mes, dif_mes, consumidas = cruzar_facturas(
        mayor, facturas_mes, etiqueta="FACTURAS MES", avisos=avisos)
    match_pend, falta_pend, dif_pend, consumidas = cruzar_facturas(
        mayor, facturas_pend, consumidas, etiqueta="FACTURAS PENDIENTES", avisos=avisos)
    sobrantes, pagadas = facturas_saldadas_no_cruzadas(mayor, falta_pend, consumidas)
    falta_pend = falta_pend.drop(index=pagadas)
    filas_a_sacar = set(facturas_pend["Fila"]) - set(falta_pend["Fila"])

    # Acreditaciones
    acred_ant = extraer_acreditaciones_pendientes(pendientes)
    liquidaciones = extraer_liquidaciones_cuadro_conceptos(cuadro)
    acred = cruzar_acreditaciones(recaudacion, liquidaciones, acred_ant, anio, mes, avisos=avisos)
    filas_a_sacar |= set(acred_ant["Fila"])  # el bloque se rehace entero (las faltantes se vuelven a escribir)

    # Pendientes
    conceptos = extraer_conceptos_cuadro_conceptos(cuadro)
    pendientes_nuevo = armar_pendientes(
        pendientes, anio, mes, filas_a_sacar, sobrantes, conceptos, falta_mes, acred,
        ventas_sistema=[("HIO", hio["venta_sistema"]), ("Atalaya", atalaya["venta_sistema"])],
        diferencias_venta=[
            ("Diferencia en Venta Atalaya", atalaya["diferencia"]),
            ("Diferencia en Venta HIO (ver Resumen de Extracto REPORTE  HIO)", hio["diferencia"]),
        ],
        recaudacion=recaudacion,
        avisos=avisos,
    )

    hojas = [
        ("Mayor", recaudacion),
        ("Atalaya", reporte_atalaya),
        ("HIO Combinado", df_hio),
        ("Rappi", df_venta_rappi_dep),
        ("Match Atalaya", atalaya["match_rappi_atalaya"]),
        ("Falta Rappi (sobra Atalaya)", atalaya["falta_rappi_atalaya"]),
        ("Falta Atalaya (sobra Rappi)", atalaya["falta_atalaya"]),
        ("Match HIO (Localizador)", hio["match_hio_rappi1"]),
        ("Match HIO - HIO (sin Localiz)", hio["match_hio2"]),
        ("Match HIO - Rappi (sin Localiz)", hio["match_hio_rappi2"]),
        ("Falta Rappi (sobra HIO)", hio["falta_rappi_hio2"]),
        ("Falta HIO (sobra Rappi)", hio["falta_hio2"]),
    ]
    buf = exportar_conciliacion(pendientes_nuevo, hojas)

    stats = {
        "mes": f"{MESES_ES[mes]} {anio}",
        "mes_numero": mes,
        "anio": anio,
        "advertencias": avisos,
        "venta_rappi_hio": hio["venta_rappi"],
        "venta_sistema_hio": hio["venta_sistema"],
        "venta_rappi_atalaya": atalaya["venta_rappi"],
        "venta_sistema_atalaya": atalaya["venta_sistema"],
        "match_hio": len(hio["match_hio_rappi1"]) + len(hio["match_hio2"]),
        "falta_hio": len(hio["falta_rappi_hio2"]) + len(hio["falta_hio2"]),
        "match_atalaya": len(atalaya["match_rappi_atalaya"]),
        "falta_atalaya": len(atalaya["falta_rappi_atalaya"]) + len(atalaya["falta_atalaya"]),
        "diferencia_hio": hio["diferencia"],
        "diferencia_atalaya": atalaya["diferencia"],
        "facturas_mes": {"match": len(match_mes), "falta": len(falta_mes), "diferencias": len(dif_mes)},
        "facturas_pendientes": {"match": len(match_pend) + len(pagadas), "falta": len(falta_pend),
                                "diferencias": len(dif_pend)},
        "facturas_saldadas_no_cruzadas": sobrantes,
        "acreditaciones": {"mayor": acred["total_mayor"], "esperado": acred["esperado"],
                           "diferencia": acred["diferencia"],
                           "faltantes": len(acred["faltantes"]), "negativas": len(acred["negativas"]),
                           "mes_siguiente": len(acred["siguiente"])},
    }
    return buf, stats, pendientes_nuevo
