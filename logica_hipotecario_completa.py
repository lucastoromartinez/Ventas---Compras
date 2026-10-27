"""
logica_hipotecario_completa.py — Conciliación completa Banco Hipotecario

Versión nueva, en prueba. Convive con logica_hipotecario.py (cruce original), que no
se modifica hasta validar este módulo. Sigue la misma estructura y genera las mismas
hojas que logica_galicia_completa.py:

  CC, Control, Pendientes anteriores, Cruces pend. anteriores, Faltante Mayor,
  Faltante Extracto, Match Mayor, Match Extracto, Extracto_cat, Neteos Mayor,
  Neteos Extracto y Foto mayor (oculta), más el libro aparte de pendientes por persona.

Contiene todo el proceso:
  1. Cruce mayor contra extracto (match_tipo 0 a 5, propios de Hipotecario, y 9 a 19
     sobre los faltantes, los mismos de Galicia).
  2. Conciliación completa (hoja CC) a partir del cruce y de la conciliación anterior:
     pendientes anteriores, control de saldos y asientos tocados con fecha ya conciliada.

Cambios respecto de logica_hipotecario.py:
  - Categorías unificadas entre mayor y extracto ("Acred. Prisma", "Acred. Cabal",
    "Acred. Fiserv", "Acred. Debin"): antes el extracto decía "Prisma"/"Cabal" y el cruce
    por categoría nunca los juntaba con las acreditaciones del mayor.
  - "IVA" solo como palabra (antes "PRIVADA", "ACTIVA", "NATIVA" caían en gastos bancarios).
  - Débitos automáticos (PAGO SERV DEBITO AUTOM) se clasifican antes que gastos
    bancarios: seguros, AFIP o proveedores según el ente.
  - Mayor: "tc" solo como palabra (antes cualquier comentario con "tc" era acreditación de tarjeta) y
    Cabal/Prisma/Debin se evalúan antes que TC; "Pago" al Banco Hipotecario con serie
    A?CG o centro de costo GB es gasto bancario aunque el comentario no diga "GB";
    se quitó "dif cambio" → Seguros.
  - TEF DATANET de terceros (sin CUIT propio) ya no se toma como transferencia entre
    cuentas: es cobranza (N/C) o pago (N/D).
  - Gastos bancarios y préstamos se cruzan por mes de referencia (el mes del comentario,
    "GB 09-26"), no por mes de la fecha del asiento.
  - El cruce con tolerancia corre sobre lo que dejó el exacto (antes corría sobre todo y
    un renglón podía quedar apareado dos veces).
  - Acreditaciones (Debin, TC, Cabal, Prisma) con línea resumen de lo pendiente, como en
    Galicia, en lugar de pasar todo el detalle del banco a Faltante Mayor.

Entrada para la app:
  correr_conciliacion_hipotecario(archivo_mayor, archivo_extracto,
                                  archivo_conciliacion_anterior=None, ...)
"""

import re
import itertools
from io import BytesIO
from datetime import date, timedelta

import numpy as np
import pandas as pd
from rapidfuzz import fuzz
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter


# ─────────────────────────────────────────────────────────────────────────────
# PARÁMETROS GENERALES
# ─────────────────────────────────────────────────────────────────────────────

BANCO = "Hipotecario"

# CUIT de las sociedades del grupo: si aparecen en la descripción del banco, el
# movimiento es una transferencia entre cuentas propias.
CUITS_PROPIOS = ["30716161591", "30718257510", "30716981076", "30719170699", "30717516288"]

COL_DEB, COL_CRED, COL_SALDO = "debito en $", "credito en $", "saldo en $"

# Acreditaciones: Debin, Fiserv (ex First Data, antes "Acred. TC"), Cabal y Prisma (Payway)
# "Cobranzas/Señas" (solo EASA): las transferencias recibidas de particulares pueden ser señas
# (registradas en el mayor en una "Acreditación Señas" mensual por el total) o cobranzas
# (registradas una por una con "Cobro N/F"); con la leyenda del banco no se puede distinguir.
# Primero se cruzan 1 a 1 contra los Cobro N/F (fecha e importe exactos) y el resto se
# concilia por total contra las "Acreditación Señas", como una acreditación.
COBRANZAS_SENAS = "Cobranzas/Señas"
SOCIEDADES_COBRANZAS_SENAS = ["EASA"]
CATEGORIAS_ACRED = ["Acred. Debin", "Acred. Fiserv", "Acred. Cabal", "Acred. Prisma", COBRANZAS_SENAS]
CATEGORIAS_MASIVAS = ["Gastos Bancarios"] + CATEGORIAS_ACRED
# Categorías que se cruzan por mes de referencia (el mes del comentario del asiento)
CATEGORIAS_POR_MES = ["Gastos Bancarios", "Prestamo"]


# ─────────────────────────────────────────────────────────────────────────────
# CARGA Y NORMALIZACIÓN
# ─────────────────────────────────────────────────────────────────────────────

def load_excel_file(file) -> pd.DataFrame:
    return pd.read_excel(file, dtype=str)


def _a_numero(serie: pd.Series) -> pd.Series:
    """Convierte importes leídos como texto. Acepta 1234.56, 1234,56 y 1.234,56."""
    s = serie.astype(str).str.strip().str.replace(r"[\s$]", "", regex=True)
    s = s.where(~s.isin(["", "nan", "None", "NaN", "-"]), "0")
    ambos = s.str.contains(",", regex=False) & s.str.contains(".", regex=False)
    s = s.where(~ambos, s.str.replace(".", "", regex=False))
    s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _a_fechas(serie: pd.Series) -> pd.Series:
    s = serie.astype(str).str.strip()
    f = pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    resto = f.isna() & ~s.isin(["", "nan", "None", "NaT"])
    if resto.any():
        # Primero formato ISO (2026-04-13); solo lo que no lo es se lee como dd/mm/aaaa
        f[resto] = pd.to_datetime(s[resto], format="ISO8601", errors="coerce")
        resto = f.isna() & ~s.isin(["", "nan", "None", "NaT"])
        if resto.any():
            f[resto] = pd.to_datetime(s[resto], dayfirst=True, errors="coerce")
    return f


def _importe(positivo: pd.Series, negativo: pd.Series) -> pd.Series:
    return np.where((positivo != 0) & (negativo == 0), positivo,
                    np.where((negativo != 0) & (positivo == 0), -negativo, 0.0))


def normalize_mayor(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str).str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"[^\x20-\x7EáéíóúÁÉÍÓÚñÑüÜ]", "", regex=True)
        .str.title()
    )
    if "_Rid" in df.columns:
        df = df.rename(columns={"_Rid": "_rid"})
    faltan = {"Debe", "Haber", "Saldo", "Fecha", "Comentario"} - set(df.columns)
    if faltan:
        raise ValueError(f"Columnas faltantes en el mayor: {faltan}. Disponibles: {list(df.columns)}")
    for col in ("Serie", "Numero", "Tercero", "Cc", "Asiento", "Apunte"):
        if col not in df.columns:
            df[col] = np.nan
    for col in ("Debe", "Haber", "Saldo"):
        df[col] = _a_numero(df[col])
    df["Importe"] = _importe(df["Debe"], df["Haber"])
    df["Fecha"] = _a_fechas(df["Fecha"])
    return df


def _limpiar(texto) -> str:
    """Minúsculas, sin acentos, sin espacios ni puntuación (deja '/' para N/D, N/C)."""
    if not isinstance(texto, str):
        return ""
    t = texto.lower()
    for a, b in zip("áéíóúñü", "aeiounu"):
        t = t.replace(a, b)
    return re.sub(r"[^a-z0-9/]", "", t)


def _norm(texto) -> str:
    if not isinstance(texto, str):
        return ""
    t = texto.lower()
    for a, b in zip("áéíóúñü", "aeiounu"):
        t = t.replace(a, b)
    return re.sub(r"[^a-z0-9]", "", t)


def _tercero_extracto(desc) -> str:
    """Contraparte del movimiento, tomada de la descripción del banco:
      'N/C - TEF DATANET MT | 30716161591 ENTRETENIMIENTOS RECOLETA SA' -> 'ENTRETENIMIENTOS RECOLETA SA'
      'N/D - PAGO SERV DEBITO AUTOM - SLAKE - 8212 - A 0011-00163963'   -> 'SLAKE'"""
    if not isinstance(desc, str):
        return ""
    if "|" in desc:
        return re.sub(r"^\s*\d{11}\s*", "", desc.split("|", 1)[1]).strip()
    m = re.search(r"DEBITO\s+AUTOM\.?\s*-\s*([^-]+)", desc, flags=re.I)
    if m:
        return m.group(1).strip()
    return ""


def normalize_extracto_hipotecario(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def norm_cols(cols):
        c = (pd.Index(cols).astype(str).str.strip()
             .str.replace(r"\s+", " ", regex=True)
             .str.replace(r"[^\x20-\x7EáéíóúÁÉÍÓÚñÑüÜ]", "", regex=True)
             .str.lower())
        for a, b in zip("áéíóúñ", "aeioun"):
            c = c.str.replace(a, b)
        return c

    esperadas = {"fecha", COL_DEB, COL_CRED, COL_SALDO}
    df.columns = norm_cols(df.columns)
    if esperadas - set(df.columns):
        # El archivo del banco puede traer renglones de encabezado antes de la tabla
        for i in range(min(20, len(df))):
            fila = norm_cols(df.iloc[i].fillna(""))
            if esperadas <= set(fila):
                df.columns = fila
                df = df.iloc[i + 1:].reset_index(drop=True)
                break
    faltan = esperadas - set(df.columns)
    if faltan:
        raise ValueError(f"Columnas faltantes en el extracto: {faltan}. Disponibles: {list(df.columns)}")
    if "descripcion" not in df.columns:
        raise ValueError(f"Columna 'descripcion' no encontrada en el extracto. Disponibles: {list(df.columns)}")

    # El extracto termina en el renglón "TOTAL"
    col_ref = next((c for c in df.columns if "referencia" in c), None)
    if col_ref:
        es_total = df[col_ref].map(_limpiar).eq("total")
        if es_total.any():
            df = df.loc[:es_total.idxmax() - 1]
    for col in (COL_DEB, COL_CRED, COL_SALDO):
        df[col] = _a_numero(df[col])
    df["importe"] = _importe(df[COL_CRED], df[COL_DEB])
    df["fecha"] = _a_fechas(df["fecha"])
    df = df[df["fecha"].notna()]
    # Renglón de saldo inicial que traen algunos extractos: no es un movimiento
    df = df[~(df["descripcion"].astype(str).str.contains("saldo anterior|saldo inicial", case=False, na=False)
              & (df["importe"] == 0))]
    for col in ("sucursal", "referencia"):
        if col not in df.columns:
            df[col] = ""
    df["tercero"] = df["descripcion"].map(_tercero_extracto)
    df["observaciones"] = ""
    return df.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# CATEGORIZACIÓN
# ─────────────────────────────────────────────────────────────────────────────

_RE_MES = r"(?<!\d)(0[1-9]|1[0-2])\s*[-/]\s*(?:20)?(2\d)(?!\d)"


def _mes_de_texto(serie: pd.Series) -> pd.Series:
    """'Pago GB 09-26' -> '09-26'; sin mes en el texto -> NaN."""
    m = serie.astype(str).str.extract(_RE_MES)
    return (m[0] + "-" + m[1]).where(m[0].notna())


def categorizar_extracto(df: pd.DataFrame, senas: bool = False) -> pd.DataFrame:
    """senas=True (solo EASA): las transferencias de particulares van a 'Cobranzas/Señas'."""
    df = df.copy()
    raw = df["descripcion"].astype(str)
    d = raw.map(_limpiar)
    imp = pd.to_numeric(df["importe"], errors="coerce").fillna(0)

    def tiene(*claves):
        return d.apply(lambda t: any(_limpiar(k) in t for k in claves))

    propio = d.apply(lambda t: any(c in t for c in CUITS_PROPIOS))
    clave_sena = tiene("cashout", "recibisteuna transferencia", "transfenv")
    debito_aut = tiene("debitoautom", "pagoservdebito")
    ente_seguro = tiene("sancor", "swiss", "berkley", "laholando", "seguro", "poliza")
    ente_afip = tiene("afip") | raw.str.contains(r"\bARCA\b|\bVEP\b", case=False, regex=True)
    es_gb = (tiene("impuesto", "comision", "paquete", "interadel", "sircreb", "mantenimiento",
                   "percepcion", "retencion", "ley25413", "impdeb", "impcred", "sellos")
             | raw.str.contains(r"\bIVA\b", case=False, regex=True))

    tef = tiene("tefdatanet")
    reglas = [
        # Acreditaciones de tarjetas y Debin: antes que el CUIT propio, porque en los Debin
        # recibidos figura el CUIT de la propia sociedad como beneficiaria
        # (9dD: 'N/C - RECIBISTE DEBIN MT | 30716981076 9 DE DICIEMBRE DEL 2018').
        (tiene("comerciosprisma", "prismamp") | (tiene("payway") & ~tef), "Acred. Prisma"),
        (tiene("debin") & ~propio & (imp % 1000 == 0), "Transf. entre cuentas"),
        (tiene("debin"), "Acred. Debin"),
        # Solo liquidaciones a comercios: 'COMPRA CON TRANSFERENCIA | FIRST DATA CONO SUR' es un pago
        (tiene("comerciosfirstdata", "comerciosfiserv"), "Acred. Fiserv"),
        # palabra completa: "Matias Nicolas CABALLERO" no es una acreditación Cabal
        (raw.str.contains(r"\bCABAL\b", case=False, regex=True) & (imp > 0), "Acred. Cabal"),
        # Transferencias propias (CUIT del grupo o cuenta propia)
        (propio | tiene("ctaprop"), "Transf. entre cuentas"),
        # Transferencias recibidas de terceros (RECIBISTE UNA TRANSFERENCIA, CASH OUT): el equipo
        # las concilia como cobranzas (9dD); antes salían como "Señas"
        (clave_sena | tiene("recibistetransfcashout", "creditocashout"), COBRANZAS_SENAS if senas else "Cobranzas"),
        (tiene("prestamo") & tiene("cuota", "pago", "amortiz", "interes"), "Prestamo"),
        # Débitos automáticos: antes que gastos bancarios (la descripción del ente
        # puede traer "impuesto", "comisión", etc.)
        (debito_aut & ente_seguro, "Seguros"),
        (debito_aut & ente_afip, "Imp. AFIP"),
        (debito_aut, "Proveedores"),
        (ente_seguro & (imp < 0), "Seguros"),
        (ente_afip & (imp < 0), "Imp. AFIP"),
        (es_gb, "Gastos Bancarios"),
        # Lo que queda: notas de débito son pagos, notas de crédito son cobranzas
        (tiene("crtransfporpagoaproveedores"), "Cobranzas"),
        (raw.str.contains("N/D", case=False, regex=False), "Proveedores"),
        (raw.str.contains("N/C", case=False, regex=False), "Cobranzas"),
    ]
    df["conciliacion"] = "0"
    for cond, cat in reglas:
        df.loc[cond & (df["conciliacion"] == "0"), "conciliacion"] = cat
    df["mes_ref"] = df["fecha"].dt.strftime("%m-%y")
    return df


def categorizar_mayor(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    com_raw = df["Comentario"].astype(str)
    com = com_raw.map(_limpiar)
    terc = df["Tercero"].astype(str).map(_limpiar)
    serie = df["Serie"].astype(str).str.upper().str.strip()
    cc = df["Cc"].astype(str).str.upper().str.strip()

    reglas = [
        # "Acreditación Señas 09-2026" (total del mes). La "Devolución Señas" se cruza 1 a 1
        # contra la transferencia devuelta (EASA: devolución 300.000 a QUIROGA del 21/09).
        (com.str.contains("sena") & ~com.str.contains("devolucion"), COBRANZAS_SENAS),
        (com.str.contains("cabal"), "Acred. Cabal"),
        (com.str.contains("prisma|payway", regex=True), "Acred. Prisma"),
        (com.str.contains("debin"), "Acred. Debin"),
        (com_raw.str.contains(r"\bTC\b|first\s*data|fiserv", case=False, regex=True)
         | com.str.contains(r"acre+d\w*tc", regex=True), "Acred. Fiserv"),
        (com.str.contains("prestamo|pagocuota", regex=True), "Prestamo"),
        (com_raw.str.contains(r"\bGB\b|gastos?\s+bancarios?", case=False, regex=True), "Gastos Bancarios"),
        (com.str.contains("cobron/f|cobronf|devolucioncobro", regex=True), "Cobranzas"),
        (com.str.contains("seguro|poliza", regex=True), "Seguros"),
        (com.str.contains("propina"), "Propinas"),
    ]
    df["conciliacion"] = "0"
    for cond, cat in reglas:
        df.loc[cond & (df["conciliacion"] == "0"), "conciliacion"] = cat

    sin = df["conciliacion"] == "0"
    # Pagos al banco con serie de gastos (A1CG...) o centro de costo GB: gastos bancarios
    es_gb = terc.str.contains("hipotecario") & (serie.str.match(r"^A\d?CG$") | cc.eq("GB"))
    df.loc[sin & es_gb, "conciliacion"] = "Gastos Bancarios"
    sin = df["conciliacion"] == "0"
    df.loc[sin & serie.str.contains("TP", na=False), "conciliacion"] = "Transf. entre cuentas"
    sin = df["conciliacion"] == "0"
    df.loc[sin & terc.str.contains("swiss|sancor|berkley|laholando|seguro", regex=True), "conciliacion"] = "Seguros"
    sin = df["conciliacion"] == "0"
    df.loc[sin & (terc.str.contains("afip|arca") & ~terc.str.contains("marca")), "conciliacion"] = "Imp. AFIP"
    # Pagos a proveedores (serie A?CA con tercero), p. ej. 'Pago S/F' a POTENCIAR SGR
    sin = df["conciliacion"] == "0"
    df.loc[sin & serie.str.match(r"^A\d?CA$") & ~_vacio(df["Tercero"]), "conciliacion"] = "Proveedores"

    # Mes de referencia: para gastos bancarios y préstamos manda el mes del comentario
    mes_fecha = df["Fecha"].dt.strftime("%m-%y")
    mes_com = _mes_de_texto(com_raw)
    por_mes = df["conciliacion"].isin(CATEGORIAS_POR_MES)
    df["mes_ref"] = np.where(por_mes & mes_com.notna(), mes_com, mes_fecha)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# CRUCES 0 A 5
# ─────────────────────────────────────────────────────────────────────────────

def cruzar_mayor_extracto(mayor, extracto):
    """Cruce 0: misma fecha e importe. Ante varios candidatos, para proveedores se
    desempata por nombre (Tercero vs tercero del banco); el resto se aparea en orden."""
    mayor, extracto = mayor.reset_index(drop=True), extracto.reset_index(drop=True)
    km = mayor["Fecha"].astype(str) + "|" + mayor["Importe"].round(2).astype(str)
    ke = extracto["fecha"].astype(str) + "|" + extracto["importe"].round(2).astype(str)
    gm = mayor.groupby(km).indices
    im, ie = [], []
    for key, idx_e in extracto.groupby(ke).indices.items():
        idx_m = list(gm.get(key, []))
        if not idx_m:
            continue
        idx_e = list(idx_e)
        if len(idx_e) == 1 and len(idx_m) == 1:
            im.append(idx_m[0]); ie.append(idx_e[0]); continue
        # Mayor puntaje de nombre primero; a igual puntaje, en orden (el primer asiento con el
        # primer movimiento del banco), como el cruce del equipo.
        pares = sorted(((fuzz.token_sort_ratio(str(extracto.at[e, "tercero"]), str(mayor.at[m, "Tercero"])), e, m)
                        for e in idx_e for m in idx_m), key=lambda t: (-t[0], t[1], t[2]))
        ue, um = set(), set()
        for sc, e, m in pares:
            if e in ue or m in um:
                continue
            ue.add(e); um.add(m); im.append(m); ie.append(e)
    fe = mayor.drop(index=im).reset_index(drop=True)
    fm = extracto.drop(index=ie).reset_index(drop=True)
    return mayor.loc[im].reset_index(drop=True), extracto.loc[ie].reset_index(drop=True), fe, fm


def cruzar_con_tolerancia(mayor, extracto, tolerancia_importe=0.5, tolerancia_dias=3):
    """Cruce 1: importe ± 0,50 y fecha ± 3 días, sobre lo que dejó el cruce 0."""
    mayor, extracto = mayor.reset_index(drop=True), extracto.reset_index(drop=True)
    usado_m, usado_e, im, ie = set(), set(), [], []
    delta = timedelta(days=tolerancia_dias)
    for i, r in mayor.iterrows():
        c = extracto[(~extracto.index.isin(usado_e))
                     & ((extracto["fecha"] - r["Fecha"]).abs() <= delta)
                     & ((extracto["importe"] - r["Importe"]).abs() <= tolerancia_importe)]
        if c.empty:
            continue
        if len(c) > 1 and r["conciliacion"] == "Proveedores":
            c = c.assign(_s=c["tercero"].map(lambda n: fuzz.token_sort_ratio(str(r["Tercero"]), str(n))))
            j = c.sort_values("_s", ascending=False).index[0]
        else:
            c = c.assign(_dd=(c["fecha"] - r["Fecha"]).abs(), _di=(c["importe"] - r["Importe"]).abs())
            j = c.sort_values(["_dd", "_di"]).index[0]
        usado_m.add(i); usado_e.add(j); im.append(i); ie.append(j)
    return (mayor.loc[im].reset_index(drop=True), extracto.loc[ie].reset_index(drop=True),
            mayor.drop(index=im).reset_index(drop=True), extracto.drop(index=ie).reset_index(drop=True))


def cruzar_por_categoria(fe, fm, tolerancia=0.5):
    """Cruce 2: total de una categoría y mes de referencia del mayor contra el del
    banco (p. ej. 'Pago GB 05-26' + 'GB 05-26' contra los débitos de gastos de mayo)."""
    fe, fm = fe.reset_index(drop=True), fm.reset_index(drop=True)
    ke = fe["conciliacion"].astype(str) + "|" + fe["mes_ref"].astype(str)
    km = fm["conciliacion"].astype(str) + "|" + fm["mes_ref"].astype(str)
    ie, im = [], []
    for k in set(ke) & set(km):
        if k.startswith("0|"):
            continue
        ge, gm = fe[ke == k], fm[km == k]
        if abs(ge["Importe"].sum() - gm["importe"].sum()) <= tolerancia:
            ie += list(ge.index); im += list(gm.index)
    return (fe.loc[ie].reset_index(drop=True), fm.loc[im].reset_index(drop=True),
            fe.drop(index=ie).reset_index(drop=True), fm.drop(index=im).reset_index(drop=True))


def cruzar_proveedores(fe, fm, tolerancia=0.5):
    """Cruce 3: débitos de proveedores del banco de un día contra los asientos del
    mismo día (sin serie TP) por suma."""
    fe, fm = fe.reset_index(drop=True), fm.reset_index(drop=True)
    ie, im = [], []
    no_tp = ~fe["Serie"].astype(str).str.upper().str.contains("TP", na=False)
    for fecha in fm.loc[fm["conciliacion"] == "Proveedores", "fecha"].unique():
        gm = fm[(fm["fecha"] == fecha) & (fm["conciliacion"] == "Proveedores") & ~fm.index.isin(im)]
        ge = fe[(fe["Fecha"] == fecha) & no_tp & (fe["Importe"] < 0) & ~fe.index.isin(ie)
                & fe["conciliacion"].isin(["0", "Proveedores", "Seguros", "Imp. AFIP"])]
        if gm.empty or ge.empty:
            continue
        if abs(ge["Importe"].sum() - gm["importe"].sum()) <= tolerancia:
            ie += list(ge.index); im += list(gm.index)
    return (fe.loc[ie].reset_index(drop=True), fm.loc[im].reset_index(drop=True),
            fe.drop(index=ie).reset_index(drop=True), fm.drop(index=im).reset_index(drop=True))


def cruzar_tp(fe, fm, tolerancia=0.5):
    """Cruce 4: asientos de transferencias (serie TP) de un día contra los movimientos
    del banco del mismo día y signo que no son gastos ni acreditaciones, por suma."""
    fe, fm = fe.reset_index(drop=True), fm.reset_index(drop=True)
    ie, im = [], []
    es_tp = fe["Serie"].astype(str).str.upper().str.contains("TP", na=False)
    for fecha in fe.loc[es_tp, "Fecha"].unique():
        for signo in (1, -1):
            ge = fe[es_tp & (fe["Fecha"] == fecha) & (np.sign(fe["Importe"]) == signo) & ~fe.index.isin(ie)]
            gm = fm[(fm["fecha"] == fecha) & (np.sign(fm["importe"]) == signo) & ~fm.index.isin(im)
                    & ~fm["conciliacion"].isin(CATEGORIAS_MASIVAS)]
            if ge.empty or gm.empty:
                continue
            if abs(ge["Importe"].sum() - gm["importe"].sum()) <= tolerancia:
                ie += list(ge.index); im += list(gm.index)
    return (fe.loc[ie].reset_index(drop=True), fm.loc[im].reset_index(drop=True),
            fe.drop(index=ie).reset_index(drop=True), fm.drop(index=im).reset_index(drop=True))


def cruzar_acreditaciones(ext_cat, fe, fm, tolerancia=0.5):
    """Cruce 5: por cada tipo de acreditación (Debin, TC, Cabal, Prisma), el total del
    banco contra el total registrado en el mayor. Si no cierra, se aparean igual y queda
    UNA línea resumen con lo pendiente de registrar (banco - mayor)."""
    fe = fe.reset_index(drop=True)
    i_cat, i_fe, resumen = [], [], []
    for cat in CATEGORIAS_ACRED:
        a_ext = ext_cat[ext_cat["conciliacion"] == cat]
        a_may = fe[fe["conciliacion"] == cat]
        if a_ext.empty:
            continue
        s_ext, s_may = a_ext["importe"].sum(), a_may["Importe"].sum()
        i_cat += list(a_ext.index); i_fe += list(a_may.index)
        if abs(s_ext - s_may) > tolerancia:
            pend = round(s_ext - s_may, 2)
            fila = {c: "" for c in fm.columns}
            fila.update({"fecha": a_ext["fecha"].max(), "importe": pend, "conciliacion": cat,
                         "descripcion": f"{cat} pendiente de registrar",
                         COL_CRED: pend if pend > 0 else 0.0, COL_DEB: -pend if pend < 0 else 0.0,
                         COL_SALDO: np.nan, "mes_ref": a_ext["fecha"].max().strftime("%m-%y"),
                         "observaciones": f"Extracto {s_ext:,.2f} - registrado en mayor {s_may:,.2f} "
                                          f"({len(a_ext)} movimientos del banco, {len(a_may)} asientos)",
                         "_rid": f"DIF-ACR-{cat}"})
            resumen.append(fila)
    fm2 = pd.concat([fm, pd.DataFrame(resumen)], ignore_index=True) if resumen else fm.reset_index(drop=True)
    return ext_cat.loc[i_cat].reset_index(drop=True), fe.loc[i_fe].reset_index(drop=True), \
        fe.drop(index=i_fe).reset_index(drop=True), fm2


# ─────────────────────────────────────────────────────────────────────────────
# CRUCES SOBRE FALTANTES (match_tipo 9 a 19, mismos criterios que Galicia)
# ─────────────────────────────────────────────────────────────────────────────

TOL_EXACTO = 0.01
TOL_RESIDUAL = 5.0
DIAS_RESIDUAL = 5
SCORE_NOMBRE = 70
TOL_RELATIVA_COMPROBANTE = 0.001
TOL_LINEA_DIFERENCIA = 1.0

REGLAS_MATCH = {
    0: "Fecha e importe exactos",
    1: "Importe y fecha con tolerancia",
    2: "Total de categoría y mes",
    3: "Proveedores del día por suma",
    4: "Transferencias (serie TP) del día por suma",
    5: "Acreditaciones por total",
    9: "Pago y devolución en mayor",
    10: "Débito y devolución en banco",
    11: "Comprobante del mayor contra movimiento del banco",
    13: "Importe exacto en el período",
    14: "Mismo tercero/categoría con tolerancia",
    15: "Categoría del banco contra asientos sin categoría",
    16: "Suma exacta del mismo día",
    18: "Varios comprobantes contra un débito",
    19: "Todos los movimientos de un tercero en el día",
}


def _vacio(serie: pd.Series) -> pd.Series:
    return serie.isna() | serie.astype(str).str.strip().isin(["", "nan", "None", "NaN"])


class _ApareoFaltantes:
    """Tabla única de faltantes de ambos lados (M = mayor, E = extracto) sobre la que
    se aplican los cruces 9 a 19. Cada apareo recibe un match_id."""

    def __init__(self, falta_extracto, falta_mayor, id_inicio):
        fe = falta_extracto.reset_index(drop=True)   # renglones del MAYOR
        fm = falta_mayor.reset_index(drop=True)      # renglones del EXTRACTO
        real_m = fe["Fecha"].notna() & ~fe["Comentario"].astype(str).str.contains("saldo anterior", case=False, na=False)
        real_e = pd.to_numeric(fm[COL_SALDO], errors="coerce").notna() & pd.to_datetime(fm["fecha"], errors="coerce").notna()
        m = pd.DataFrame({
            "lado": "M", "pos": fe.index, "fecha": pd.to_datetime(fe["Fecha"], errors="coerce"),
            "importe": pd.to_numeric(fe["Importe"], errors="coerce").fillna(0.0).round(2),
            "categoria": fe["conciliacion"].astype(str), "nombre": fe["Tercero"].astype(str),
            "comprobante": np.where(_vacio(fe["Serie"]) | _vacio(fe["Numero"]), "",
                                    fe["Serie"].astype(str) + "-" + fe["Numero"].astype(str)),
        })[real_m.values]
        e = pd.DataFrame({
            "lado": "E", "pos": fm.index, "fecha": pd.to_datetime(fm["fecha"], errors="coerce"),
            "importe": pd.to_numeric(fm["importe"], errors="coerce").fillna(0.0).round(2),
            "categoria": fm["conciliacion"].astype(str), "nombre": fm["tercero"].astype(str),
            "comprobante": "",
        })[real_e.values]
        self.fe, self.fm = fe, fm
        self.p = pd.concat([m, e], ignore_index=True)
        self.p["match_id"] = 0
        self.p["match_tipo"] = ""
        self.sin_tercero = _vacio(self.p["nombre"])
        self.no_masivas = ~self.p["categoria"].isin(CATEGORIAS_MASIVAS)
        self.id = id_inicio
        self.diferencias = []

    def pend(self, mask=None):
        libre = self.p["match_id"] == 0
        return self.p[libre if mask is None else (libre & mask)]

    def cerrar(self, idx, tipo):
        self.p.loc[idx, "match_id"] = self.id
        self.p.loc[idx, "match_tipo"] = str(tipo)
        self.id += 1

    @staticmethod
    def score(a, b):
        na, nb = str(a), str(b)
        if na in ("", "nan", "None") or nb in ("", "nan", "None"):
            return 0
        sc = fuzz.token_set_ratio(na.upper(), nb.upper())
        pa = re.findall(r"[A-Z0-9]{3,}", na.upper())
        pb = re.findall(r"[A-Z0-9]{3,}", nb.upper())
        if pa and pb and pa[0] == pb[0] and pa[0] not in ("BANCO", "DISTRIBUIDORA", "GRUPO", "PAGO"):
            sc = max(sc, 90)
        return sc

    def _buscar_suma(self, cand, objetivo, tol, max_k):
        if len(cand) > 40:
            cand = cand.assign(_d=(cand["importe"] - objetivo).abs()).nsmallest(40, "_d")
        k_max = min(max_k, len(cand)) if len(cand) <= 20 else min(3, len(cand))
        for k in range(1, k_max + 1):
            for combo in itertools.combinations(cand.index, k):
                if abs(cand.loc[list(combo), "importe"].sum() - objetivo) <= tol:
                    return list(combo)
        if len(cand) and abs(cand["importe"].sum() - objetivo) <= tol:
            return list(cand.index)
        return None

    def _fila_diferencia(self, pos_e, dif, descripcion, obs):
        fila = self.fm.loc[pos_e].copy()
        fila["descripcion"] = descripcion
        fila["importe"] = dif
        fila[COL_DEB] = -dif if dif < 0 else 0.0
        fila[COL_CRED] = dif if dif > 0 else 0.0
        fila[COL_SALDO] = np.nan
        fila["observaciones"] = obs
        if "_rid" in fila.index:
            fila["_rid"] = f"DIF-{fila['_rid']}"
        self.diferencias.append(fila)

    # 9 / 10: anulaciones dentro de un mismo lado
    def anulaciones(self, lado, tipo, max_dias, excluir_masivas=False, max_k=4):
        base = self.p["lado"] == lado
        if excluir_masivas:
            base &= self.no_masivas
        for signo in (1, -1):
            anclas = self.pend(base & (np.sign(self.p["importe"]) == signo))
            for ia in anclas["importe"].abs().sort_values(ascending=False).index:
                if self.p.at[ia, "match_id"]:
                    continue
                a = self.p.loc[ia]
                cand = self.pend(base & (np.sign(self.p["importe"]) == -signo))
                cand = cand[(cand["importe"].abs() <= abs(a["importe"]) + TOL_EXACTO)
                            & ((cand["fecha"] - a["fecha"]).abs() <= pd.Timedelta(days=max_dias))]
                if cand.empty:
                    continue
                mismo_comp = (cand["comprobante"] == a["comprobante"]) & (a["comprobante"] != "")
                sc = cand["nombre"].apply(lambda n: self.score(a["nombre"], n))
                mismo_dia_banco = (lado == "E") & (cand["fecha"] == a["fecha"]) & \
                    ((cand["importe"] + a["importe"]).abs() <= TOL_EXACTO)
                cand = cand[(sc >= 90) | mismo_comp | mismo_dia_banco]
                if cand.empty:
                    continue
                hallado = self._buscar_suma(cand, -a["importe"], TOL_EXACTO, max_k)
                if hallado:
                    self.cerrar([ia] + hallado, tipo)

    # 11: comprobante del mayor contra un movimiento del banco
    def comprobante_vs_banco(self, tipo, max_dias=3):
        pm = self.pend((self.p["lado"] == "M") & (self.p["comprobante"] != "") & self.no_masivas)
        for comp, g in pm.groupby("comprobante"):
            g = g[self.p.loc[g.index, "match_id"] == 0]
            if g.empty:
                continue
            total, fecha = g["importe"].sum(), g["fecha"].max()
            exacto = self.pend((self.p["lado"] == "E") & self.no_masivas
                               & ((self.p["importe"] - total).abs() <= TOL_EXACTO))
            if len(g) > 1 and len(exacto):
                ib = exacto.assign(_dd=(exacto["fecha"] - fecha).abs()).sort_values("_dd").index[0]
                self.cerrar(list(g.index) + [ib], tipo)
                continue
            cand = self.pend((self.p["lado"] == "E") & self.no_masivas)
            cand = cand[(np.sign(cand["importe"]) == np.sign(total))
                        & ((cand["fecha"] - fecha).abs() <= pd.Timedelta(days=max_dias))]
            if cand.empty:
                continue
            cand = cand.assign(_d=(cand["importe"] - total).abs(), _dd=(cand["fecha"] - fecha).abs())
            cand = cand[cand["_d"] <= max(0.5, abs(total) * TOL_RELATIVA_COMPROBANTE)]
            if cand.empty:
                continue
            ib = cand.sort_values(["_d", "_dd"]).index[0]
            dif = round(self.p.at[ib, "importe"] - total, 2)
            id_match = self.id
            self.cerrar(list(g.index) + [ib], tipo)
            if abs(dif) >= TOL_LINEA_DIFERENCIA:
                self._fila_diferencia(self.p.at[ib, "pos"], dif, f"Diferencia comprobante {comp}",
                                      f"Diferencia del match {id_match}")

    # 13: importe exacto, cualquier fecha del período
    def importe_exacto(self, tipo):
        for ia in self.pend((self.p["lado"] == "M") & self.no_masivas).index:
            if self.p.at[ia, "match_id"]:
                continue
            a = self.p.loc[ia]
            cand = self.pend((self.p["lado"] == "E") & self.no_masivas)
            cand = cand[(cand["importe"] - a["importe"]).abs() <= TOL_EXACTO]
            if cand.empty:
                continue
            ib = cand.assign(_dd=(cand["fecha"] - a["fecha"]).abs()).sort_values("_dd").index[0]
            self.cerrar([ia, ib], tipo)

    # 14: mismo tercero o categoría, con tolerancia
    def residual_con_tolerancia(self, tipo):
        for lado_a, lado_b in (("M", "E"), ("E", "M")):
            for ia in self.pend((self.p["lado"] == lado_a) & self.no_masivas).index:
                if self.p.at[ia, "match_id"]:
                    continue
                a = self.p.loc[ia]
                cand = self.pend((self.p["lado"] == lado_b) & self.no_masivas)
                cand = cand[(np.sign(cand["importe"]) == np.sign(a["importe"]))
                            & ((cand["fecha"] - a["fecha"]).abs() <= pd.Timedelta(days=DIAS_RESIDUAL))
                            & (cand["importe"].abs() <= abs(a["importe"]) + TOL_RESIDUAL)]
                if cand.empty:
                    continue
                misma_cat = (cand["categoria"] == a["categoria"]) & (a["categoria"] != "0")
                sc = cand["nombre"].apply(lambda n: self.score(a["nombre"], n))
                cand = cand[misma_cat | (sc >= SCORE_NOMBRE)]
                if cand.empty:
                    continue
                hallado = self._buscar_suma(cand, a["importe"], TOL_RESIDUAL, max_k=4)
                if hallado:
                    self.cerrar([ia] + hallado, tipo)

    # 15: total de una categoría del banco contra asientos sin categoría ni tercero
    def categoria_vs_sin_categoria(self, tipo):
        pe = self.pend((self.p["lado"] == "E") & self.no_masivas & (self.p["categoria"] != "0"))
        for cat, g in pe.groupby("categoria"):
            objetivo = g["importe"].sum()
            cand = self.pend((self.p["lado"] == "M") & (self.p["categoria"] == "0") & self.sin_tercero)
            cand = cand[(np.sign(cand["importe"]) == np.sign(objetivo))
                        & (cand["importe"].abs() <= abs(objetivo) + TOL_EXACTO)]
            hallado = self._buscar_suma(cand, objetivo, TOL_EXACTO, max_k=4)
            if hallado:
                self.cerrar(list(g.index) + hallado, tipo)

    # 16: un renglón contra la suma exacta del otro lado, mismo día
    def suma_mismo_dia(self, tipo):
        for lado_a, lado_b in (("M", "E"), ("E", "M")):
            for ia in self.pend((self.p["lado"] == lado_a) & self.no_masivas).index:
                if self.p.at[ia, "match_id"]:
                    continue
                a = self.p.loc[ia]
                cand = self.pend((self.p["lado"] == lado_b) & self.no_masivas)
                exige_nombre = lado_a == "M" and not self.sin_tercero.loc[ia]
                if exige_nombre:
                    cand_dia = cand[cand["fecha"] == a["fecha"]]
                    if not (cand_dia["nombre"].apply(lambda n: self.score(a["nombre"], n)) >= SCORE_NOMBRE).any():
                        continue
                if lado_b == "M":
                    sc = cand["nombre"].apply(lambda n: self.score(a["nombre"], n))
                    if not (sc >= SCORE_NOMBRE).any():
                        cand = cand[self.sin_tercero.loc[cand.index]]
                cand = cand[(cand["fecha"] == a["fecha"]) & (np.sign(cand["importe"]) == np.sign(a["importe"]))
                            & (cand["importe"].abs() <= abs(a["importe"]) + TOL_EXACTO)]
                hallado = self._buscar_suma(cand, a["importe"], TOL_EXACTO, max_k=4)
                if hallado and exige_nombre:
                    if not (self.p.loc[hallado, "nombre"].apply(lambda n: self.score(a["nombre"], n)) >= SCORE_NOMBRE).any():
                        hallado = None
                if hallado:
                    self.cerrar([ia] + hallado, tipo)

    # 19: todos los movimientos de un tercero en el día contra un renglón
    def tercero_del_dia(self, tipo):
        for lado_a, lado_b in (("M", "E"), ("E", "M")):
            for ia in self.pend((self.p["lado"] == lado_a) & self.no_masivas & ~self.sin_tercero).index:
                if self.p.at[ia, "match_id"]:
                    continue
                a = self.p.loc[ia]
                cand = self.pend((self.p["lado"] == lado_b) & self.no_masivas)
                cand = cand[(cand["fecha"] == a["fecha"]) & (np.sign(cand["importe"]) == np.sign(a["importe"]))]
                if len(cand) < 2:
                    continue
                grupo = cand[cand["nombre"].apply(lambda n: self.score(a["nombre"], n)) >= SCORE_NOMBRE]
                if len(grupo) >= 2 and abs(grupo["importe"].sum() - a["importe"]) <= TOL_RESIDUAL:
                    self.cerrar([ia] + list(grupo.index), tipo)

    # 18: varios comprobantes sin tercero contra un débito del banco
    def comprobantes_vs_debito(self, tipo, max_dias=5):
        pm = self.pend((self.p["lado"] == "M") & self.sin_tercero & (self.p["categoria"] == "0"))
        if pm.empty:
            return
        unidades = [(list(g.index), round(g["importe"].sum(), 2), g["fecha"].max())
                    for _, g in pm.groupby(pm["comprobante"].where(pm["comprobante"] != "", pm.index.astype(str)))]
        u = pd.DataFrame({"idx": [x[0] for x in unidades], "importe": [x[1] for x in unidades],
                          "fecha": [x[2] for x in unidades]})
        for ie, e in self.pend((self.p["lado"] == "E") & self.no_masivas & (self.p["categoria"] != "0")).iterrows():
            libres = u[u["idx"].apply(lambda l: all(self.p.at[i, "match_id"] == 0 for i in l))]
            libres = libres[(np.sign(libres["importe"]) == np.sign(e["importe"]))
                            & ((libres["fecha"] - e["fecha"]).abs() <= pd.Timedelta(days=max_dias))
                            & (libres["importe"].abs() <= abs(e["importe"]) + TOL_EXACTO)]
            if len(libres) < 2:
                continue
            hallado = self._buscar_suma(libres, e["importe"], TOL_EXACTO, max_k=4)
            if hallado and len(hallado) > 1:
                self.cerrar([i for k in hallado for i in libres.at[k, "idx"]] + [ie], tipo)

    def resultado(self):
        P = self.p
        mm = P[(P["lado"] == "M") & (P["match_id"] > 0)]
        me = P[(P["lado"] == "E") & (P["match_id"] > 0)]
        match_mayor = self.fe.loc[mm["pos"]].copy()
        match_mayor["match_id"] = mm["match_id"].values
        match_mayor["match_tipo"] = mm["match_tipo"].values
        match_extracto = self.fm.loc[me["pos"]].copy()
        match_extracto["match_id"] = me["match_id"].values
        match_extracto["match_tipo"] = me["match_tipo"].values
        falta_extracto = self.fe.drop(index=mm["pos"]).reset_index(drop=True)
        falta_mayor = self.fm.drop(index=me["pos"])
        if self.diferencias:
            falta_mayor = pd.concat([falta_mayor, pd.DataFrame(self.diferencias)], ignore_index=True)
        return (match_mayor.reset_index(drop=True), match_extracto.reset_index(drop=True),
                falta_extracto, falta_mayor.reset_index(drop=True), self.id)


def cruzar_faltantes_adicionales(falta_extracto, falta_mayor, id_inicio):
    ap = _ApareoFaltantes(falta_extracto, falta_mayor, id_inicio)
    ap.anulaciones("M", 9, max_dias=31)
    ap.anulaciones("E", 10, max_dias=10, excluir_masivas=True)
    ap.comprobante_vs_banco(11)
    ap.importe_exacto(13)
    ap.residual_con_tolerancia(14)
    ap.categoria_vs_sin_categoria(15)
    ap.suma_mismo_dia(16)
    ap.comprobantes_vs_debito(18)
    ap.tercero_del_dia(19)
    return ap.resultado()


# ─────────────────────────────────────────────────────────────────────────────
# IDS, CONSOLIDACIÓN Y EXCEL DEL CRUCE
# ─────────────────────────────────────────────────────────────────────────────

def _asignar_ids(dm, de, tipo, id_inicio, clave_m=None, clave_e=None):
    """Asigna match_id. Sin clave: un id por par (mismo largo). Con clave: un id por grupo."""
    dm, de = dm.copy().reset_index(drop=True), de.copy().reset_index(drop=True)
    if clave_m is None:
        ids = list(range(id_inicio, id_inicio + len(dm)))
        dm["match_id"], de["match_id"] = ids, ids[:len(de)]
        fin = id_inicio + len(dm)
    else:
        mapa, cur = {}, id_inicio
        for v in list(clave_m(dm)) + list(clave_e(de)):
            if v not in mapa:
                mapa[v] = cur; cur += 1
        dm["match_id"] = [mapa[v] for v in clave_m(dm)]
        de["match_id"] = [mapa[v] for v in clave_e(de)]
        fin = cur
    dm["match_tipo"], de["match_tipo"] = str(tipo), str(tipo)
    return dm, de, fin


COLUMNAS_IMPORTE = ["Debe", "Haber", "Saldo", "Importe", COL_DEB, COL_CRED, COL_SALDO, "importe"]
AMOUNT_FORMAT = "#,##0.00"


def _forzar_importes_float(df):
    df = df.copy()
    for col in COLUMNAS_IMPORTE:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    return df


def _aplicar_formato(ws, df):
    for j, col in enumerate(df.columns, start=1):
        fmt = AMOUNT_FORMAT if col in COLUMNAS_IMPORTE else ("dd/mm/yyyy" if col.lower().startswith("fecha") else None)
        if fmt:
            for r in range(2, ws.max_row + 1):
                ws.cell(r, j).number_format = fmt
        ws.column_dimensions[get_column_letter(j)].width = 48 if col in ("descripcion", "Comentario") else \
            max(10, min(36, len(str(col)) + 4))
    ws.freeze_panes = "A2"


def generar_excel_cruce(falta_mayor, falta_extracto, match_mayor, match_extracto, ext_cat,
                        neteos_mayor=None, neteos_extracto=None):
    hojas = [("Faltante Mayor", falta_mayor), ("Faltante Extracto", falta_extracto),
             ("Match Mayor", match_mayor), ("Match Extracto", match_extracto), ("Extracto_cat", ext_cat),
             ("Neteos Mayor", neteos_mayor), ("Neteos Extracto", neteos_extracto)]
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for nombre, d in hojas:
            if d is None:
                continue
            d = _forzar_importes_float(d.drop(columns=[c for c in ("_rid", "mes_ref") if c in d.columns]))
            d.to_excel(w, sheet_name=nombre, index=False)
            _aplicar_formato(w.book[nombre], d)
    return buf.getvalue()


def _safe_concat(dfs):
    dfs = [d for d in dfs if d is not None and not d.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def ejecutar_cruces(df_mayor_dep, df_extracto_dep, senas=False):
    """Corre todos los cruces y devuelve los DataFrames."""
    ext_cat = categorizar_extracto(df_extracto_dep, senas)
    may_cat = categorizar_mayor(df_mayor_dep)
    if "_rid" not in may_cat:
        may_cat["_rid"] = ["M%06d" % i for i in range(len(may_cat))]
    if "_rid" not in ext_cat:
        ext_cat["_rid"] = ["E%06d" % i for i in range(len(ext_cat))]
    es_saldo = may_cat["Comentario"].astype(str).str.contains("saldo anterior", case=False, na=False) | may_cat["Fecha"].isna()
    may_mov = may_cat[~es_saldo].reset_index(drop=True)
    ext_sin_acred = ext_cat[~ext_cat["conciliacion"].isin(CATEGORIAS_ACRED)].reset_index(drop=True)
    may_sin_acred = may_mov[~may_mov["conciliacion"].isin(CATEGORIAS_ACRED)]
    may_acred = may_mov[may_mov["conciliacion"].isin(CATEGORIAS_ACRED)]

    ext_para_acred = ext_cat
    if senas:
        # Cobranzas/Señas: primero 1 a 1 contra los Cobro N/F (fecha e importe exactos); lo
        # que no cruza queda para el total contra las "Acreditación Señas" (cruce 5)
        es_cs = ext_cat["conciliacion"] == COBRANZAS_SENAS
        mm0, me0, fe0, fm0 = cruzar_mayor_extracto(may_sin_acred, ext_cat[~ext_cat["conciliacion"].isin(CATEGORIAS_ACRED) | es_cs])
        fm0 = fm0[fm0["conciliacion"] != COBRANZAS_SENAS].reset_index(drop=True)
        ext_para_acred = ext_cat[~ext_cat["_rid"].isin(me0["_rid"])]
    else:
        mm0, me0, fe0, fm0 = cruzar_mayor_extracto(may_sin_acred, ext_sin_acred)
    mm1, me1, fe1, fm1 = cruzar_con_tolerancia(fe0, fm0)
    mm2, me2, fe2, fm2 = cruzar_por_categoria(fe1, fm1)
    mm3, me3, fe3, fm3 = cruzar_proveedores(fe2, fm2)
    mm4, me4, fe4, fm4 = cruzar_tp(fe3, fm3)
    fe4 = pd.concat([fe4, may_acred], ignore_index=True)
    me5, mm5, fe5, fm5 = cruzar_acreditaciones(ext_para_acred, fe4, fm4)

    dm0, de0, i1 = _asignar_ids(mm0, me0, 0, 1)
    dm1, de1, i2 = _asignar_ids(mm1, me1, 1, i1)
    k = lambda c1, c2: (lambda d: (d[c1].astype(str) + "|" + d[c2].astype(str)).tolist())
    dm2, de2, i3 = _asignar_ids(mm2, me2, 2, i2, k("conciliacion", "mes_ref"), k("conciliacion", "mes_ref"))
    f = lambda col: (lambda d: d[col].astype(str).tolist())
    dm3, de3, i4 = _asignar_ids(mm3, me3, 3, i3, f("Fecha"), f("fecha"))
    dm4, de4, i5 = _asignar_ids(mm4, me4, 4, i4, f("Fecha"), f("fecha"))
    dm5, de5, i6 = _asignar_ids(mm5, me5, 5, i5, f("conciliacion"), f("conciliacion"))

    dm9, de9, falta_extracto, falta_mayor, _ = cruzar_faltantes_adicionales(fe5, fm5, i6)
    neteos_mayor = dm9[dm9["match_tipo"].astype(str) == "9"]
    neteos_extracto = de9[de9["match_tipo"].astype(str) == "10"]
    dm9, de9 = dm9.drop(neteos_mayor.index), de9.drop(neteos_extracto.index)

    match_mayor = _safe_concat([dm0, dm1, dm2, dm3, dm4, dm5, dm9])
    match_extracto = _safe_concat([de0, de1, de2, de3, de4, de5, de9])
    return {
        "df_mayor_cat": may_cat, "df_extracto_cat": ext_cat,
        "falta_mayor": falta_mayor, "falta_extracto": falta_extracto,
        "match_mayor_def": match_mayor, "match_extracto_def": match_extracto,
        "match_acreditaciones_cat": me5,
        "neteos_mayor": neteos_mayor.reset_index(drop=True),
        "neteos_extracto": neteos_extracto.reset_index(drop=True),
        "stats": {"match_exacto": len(mm0), "match_tolerancia": len(mm1),
                  "match_faltantes": len(dm9) + len(de9),
                  "falta_mayor": len(falta_mayor), "falta_extracto": len(falta_extracto)},
    }


def correr_cruce_hipotecario(archivo_mayor, archivo_extracto):
    """Solo el cruce (hojas Faltante/Match/Extracto_cat/Neteos), sin hoja CC."""
    r = ejecutar_cruces(normalize_mayor(load_excel_file(archivo_mayor)),
                        normalize_extracto_hipotecario(load_excel_file(archivo_extracto)))
    buf = generar_excel_cruce(r["falta_mayor"], r["falta_extracto"], r["match_mayor_def"],
                              r["match_extracto_def"], r["df_extracto_cat"], r["neteos_mayor"], r["neteos_extracto"])
    return buf, r["stats"]


# ─────────────────────────────────────────────────────────────────────────────
# CONCILIACIÓN COMPLETA (HOJA CC) — PARÁMETROS
# ─────────────────────────────────────────────────────────────────────────────

TOL_CENTAVOS = 0.01
TOL_CRUCE = 0.5
TOL_CAMBIO_SALDO = 0.05   # el equipo da por explicado un cambio de saldo con hasta 5 centavos
MAX_COMBINACION = 4

CATEGORIAS_AGRUPADAS = {
    "Gastos Bancarios": "GB",
    "Prestamo": "Prestamo",
    "Imp. AFIP": "Plan Afip",
    "Acred. Debin": "Acred. Debin",
    "Acred. Fiserv": "Acred. Fiserv",
    "Acred. Cabal": "Acred. Cabal",
    "Acred. Prisma": "Acred. Prisma",
    "Propinas": "Propinas",
    COBRANZAS_SENAS: COBRANZAS_SENAS,
}

SECCIONES = {
    "ING_MAYOR": "Ingresos en Mayor que No están en Extracto",
    "SAL_MAYOR": "Salidas en Mayor que No están en Extracto",
    "CRED_BANCO": "Créditos del banco a contabilizar",
    "DEB_BANCO": "Débitos del banco a contabilizar",
}


def _categoria_desde_concepto(concepto) -> str:
    """Mapea el texto libre de la hoja CC a una categoría del cruce."""
    c = _norm(concepto)
    reglas = [
        (("gb", "gastosbanc"), "Gastos Bancarios"),
        (("prestamo",), "Prestamo"),
        (("afip",), "Imp. AFIP"),
        (("debin",), "Acred. Debin"),
        (("cabal",), "Acred. Cabal"),
        (("prisma", "payway"), "Acred. Prisma"),
        (("acredtc", "acreditacionestc", "firstdata", "fiserv"), "Acred. Fiserv"),
        (("seguro", "sancor", "swiss", "berkley"), "Seguros"),
        (("proveedor",), "Proveedores"),
        (("sena",), COBRANZAS_SENAS),
        (("cobranza",), "Cobranzas"),
        (("transf",), "Transf. entre cuentas"),
        (("ajust",), "Ajuste"),
    ]
    for claves, cat in reglas:
        if any(c.startswith(k) or (len(k) > 2 and k in c) for k in claves):
            return cat
    return str(concepto) if concepto else "0"


def _subconjunto_exacto(valores, objetivo, tol=0.01, exigir_unico=True):
    """Subconjunto de 'valores' cuya suma da 'objetivo' (± tol), buscado en forma exacta
    en centavos partiendo la lista en dos mitades (hasta ~40 valores). Con exigir_unico
    solo devuelve resultado si hay UNA sola combinación posible."""
    vals = [int(round(v * 100)) for v in valores][:40]
    meta, tol_c = int(round(objetivo * 100)), int(round(tol * 100))
    mitad = len(vals) // 2

    def sumas(rango):
        res = {0: [1, ()]}
        for i in rango:
            nuevas = {}
            for s_, (n, comb) in res.items():
                t = s_ + vals[i]
                if t in nuevas:
                    nuevas[t][0] += n
                else:
                    nuevas[t] = [n, comb + (i,)]
            for t, (n, comb) in nuevas.items():
                if t in res:
                    res[t][0] += n
                else:
                    res[t] = [n, comb]
        return res

    izq, der = sumas(range(mitad)), sumas(range(mitad, len(vals)))
    soluciones, una = 0, None
    for s_i, (n_i, c_i) in izq.items():
        for d in range(-tol_c, tol_c + 1):
            r = der.get(meta - s_i + d)
            if r is not None:
                comb = c_i + r[1]
                if not comb:
                    continue
                soluciones += n_i * r[0]
                una = comb
    if una is None or (exigir_unico and soluciones != 1):
        return None
    return list(una)


def _seccion(lado: str, importe: float) -> str:
    if lado == "M":
        return "ING_MAYOR" if importe > 0 else "SAL_MAYOR"
    return "CRED_BANCO" if importe > 0 else "DEB_BANCO"


def _a_fecha(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return pd.NaT
    return pd.to_datetime(v, errors="coerce")


def _a_bytes(archivo):
    if archivo is None:
        return None
    if isinstance(archivo, (bytes, bytearray)):
        return bytes(archivo)
    if hasattr(archivo, "read"):
        if hasattr(archivo, "seek"):
            archivo.seek(0)
        return archivo.read()
    with open(archivo, "rb") as f:
        return f.read()


# ─────────────────────────────────────────────────────────────────────────────
# 1. CRUCE CON IDS
# ─────────────────────────────────────────────────────────────────────────────

def _correr_cruce_con_ids(df_mayor_dep, df_extracto_dep, senas=False):
    """Corre el cruce (match 0 a 19) y separa, por _rid, qué quedó conciliado y qué no."""
    r = ejecutar_cruces(df_mayor_dep, df_extracto_dep, senas)
    fe, fm = r["falta_extracto"], r["falta_mayor"]   # fe = renglones del MAYOR; fm = del EXTRACTO
    rid_m_pend = set(fe["_rid"].dropna().astype(str)) - {""}
    rid_fm = fm["_rid"].fillna("").astype(str)
    rid_e_pend = set(rid_fm[(rid_fm != "") & ~rid_fm.str.startswith("DIF-")])
    diferencias = fm[rid_fm.str.startswith("DIF-")].copy()
    diferencias["descripcion"] = diferencias["descripcion"].astype(str)

    may = r["df_mayor_cat"].copy()
    ext = r["df_extracto_cat"].copy()
    may["pendiente"] = may["_rid"].isin(rid_m_pend)
    ext["pendiente"] = ext["_rid"].isin(rid_e_pend)
    bytes_cruce = generar_excel_cruce(fm, fe, r["match_mayor_def"], r["match_extracto_def"], ext.drop(columns="pendiente"),
                                      r["neteos_mayor"], r["neteos_extracto"])
    return may, ext, bytes_cruce, diferencias, r["stats"]


# ─────────────────────────────────────────────────────────────────────────────
# 2. CONCILIACIÓN ANTERIOR
# ─────────────────────────────────────────────────────────────────────────────

HOJA_FOTO = "Foto mayor"
MESES_FOTO = 12
COLS_FOTO = ["Asiento", "Apunte", "Fecha", "Importe", "Comentario", "Tercero", "Serie", "Cc"]


def _nombre_hoja_foto(cuenta):
    nom = re.sub(r"[\\/?*\[\]:]", "", f"{HOJA_FOTO} {cuenta}".strip()) if cuenta else HOJA_FOTO
    return nom[:31]


def _contenido_xlsx(contenido: bytes) -> bytes:
    """Las conciliaciones viejas están en .xls: se pasan a .xlsx (con LibreOffice si está,
    que conserva fórmulas; si no, solo valores)."""
    if not contenido[:8].startswith(b"\xd0\xcf\x11\xe0"):
        return contenido
    import os, subprocess, tempfile
    with tempfile.TemporaryDirectory() as d:
        src = os.path.join(d, "anterior.xls")
        with open(src, "wb") as f:
            f.write(contenido)
        try:
            subprocess.run(["soffice", "--headless", "--convert-to", "xlsx", "--outdir", d, src],
                           check=True, capture_output=True, timeout=120)
            with open(os.path.join(d, "anterior.xlsx"), "rb") as f:
                return f.read()
        except Exception:
            hojas = pd.read_excel(BytesIO(contenido), sheet_name=None, header=None)
            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as w:
                for n, h in hojas.items():
                    h.to_excel(w, sheet_name=n[:31], index=False, header=False)
            return buf.getvalue()


def _mayor_de_hoja(df):
    """Hoja con el mayor (Match Mayor / Faltante Extracto / 'MAYOR 2788') -> columnas estándar."""
    df = df.copy()
    df.columns = [str(c).strip().title() for c in df.columns]
    if not {"Fecha", "Comentario"} <= set(df.columns):
        return None
    if "Importe" not in df.columns:
        if {"Debe", "Haber"} <= set(df.columns):
            debe = pd.to_numeric(df["Debe"], errors="coerce").fillna(0)
            haber = pd.to_numeric(df["Haber"], errors="coerce").fillna(0)
            df["Importe"] = _importe(debe, haber)
        else:
            return None
    return df


def leer_conciliacion_anterior(archivo, cuenta=None, saldo_ext_inicio=None, saldo_mayor_inicio=None) -> dict:
    """Lee la hoja CC/Conciliación (pendientes y saldos) y la foto del mayor de la
    conciliación anterior. Una hoja puede tener varias cuentas (ERSA: CTA 278-8 y
    CTA 591-2); la cuenta se elige por: la indicada, el saldo del extracto, el saldo
    contable, la cuenta de las hojas del cruce o la primera con saldo."""
    from openpyxl import load_workbook
    contenido = _contenido_xlsx(_a_bytes(archivo))
    wb_f = load_workbook(BytesIO(contenido))
    wb_v = load_workbook(BytesIO(contenido), data_only=True)
    hoja = next(s for s in wb_f.sheetnames if _norm(s).startswith("concilia") or _norm(s) == "cc")
    wf, wv = wb_f[hoja], wb_v[hoja]

    bloques, actual, seccion = {}, None, None
    mes, razon_social = None, None
    nuevo = lambda: {"lineas": [], "saldo_contable": None, "saldo_extracto": None, "diferencia": None}
    acum = 0.0
    for r in range(1, wf.max_row + 1):
        val = {c: wv.cell(r, c).value for c in range(2, 10)}
        texto = " ".join(str(v) for v in val.values() if isinstance(v, str))
        if isinstance(val.get(2), str) and _norm(val[2]).startswith("razonsocial") and razon_social is None:
            razon_social = str(val.get(3) or "").strip() or None
        if isinstance(val.get(2), str) and val[2].strip().lower().startswith("mes") and mes is None:
            mes = _a_fecha(val.get(3))
        if isinstance(val.get(9), str) and "cta" in val[9].lower():
            actual = val[9].strip()
            bloques[actual] = nuevo()
            seccion = None
            continue
        if actual is None:
            if "Saldo Contable" not in texto:
                continue
            actual = "Cuenta"
            bloques[actual] = nuevo()
        b = bloques[actual]
        if "Saldo Contable" in texto and b["saldo_contable"] is None:
            b["saldo_contable"] = val[9]; continue
        if "Saldo Extracto" in texto:
            b["saldo_extracto"] = val[9]; continue
        if "DIFERENCIA" in texto and "Inicial" not in texto:
            b["diferencia"] = val[9]; continue
        encontrada = [k for k, v in SECCIONES.items() if v.split(" que")[0].split(" a ")[0] in texto]
        if encontrada:
            seccion, acum = encontrada[0], 0.0; continue
        if texto.strip().startswith("Total"):
            seccion = None; continue
        if seccion is None:
            continue
        celda_f = wf.cell(r, 5).value
        imp = val.get(5)
        if isinstance(celda_f, str) and celda_f.startswith("="):
            continue  # subtotal con fórmula
        if not isinstance(imp, (int, float)) or imp == 0:
            continue
        sin_datos = all(val.get(c) in (None, "") for c in (3, 4, 6))
        if sin_datos and acum and abs(float(imp) - acum) < 0.005:
            continue  # subtotal sin fórmula (archivo convertido solo con valores)
        acum += float(imp)
        b["lineas"].append({"seccion": seccion, "fecha": _a_fecha(val.get(3)), "concepto": val.get(4),
                            "importe_hoja": float(imp), "obs": val.get(6), "fila": r})

    # Foto del mayor y período del extracto de la conciliación anterior
    xls = pd.ExcelFile(BytesIO(contenido))
    snap, saldo_ant, ext_desde, ext_hasta, snap_final = None, None, None, None, None
    hojas_mayor = [s for s in ("Match Mayor", "Faltante Extracto") if s in xls.sheet_names] or \
        [s for s in xls.sheet_names if _norm(s).startswith("mayor")]
    partes = [d for d in (_mayor_de_hoja(pd.read_excel(xls, sheet_name=s)) for s in hojas_mayor) if d is not None]
    if partes:
        m = pd.concat(partes, ignore_index=True)
        fila_sa = m[m["Comentario"].astype(str).str.contains("saldo anterior", case=False, na=False)]
        if len(fila_sa) and "Saldo" in fila_sa:
            saldo_ant = float(pd.to_numeric(fila_sa["Saldo"], errors="coerce").iloc[0])
        snap = m.dropna(subset=["Asiento"]).drop_duplicates(["Asiento", "Apunte"]).copy() if "Asiento" in m else m.iloc[0:0]
        snap["Fecha"] = pd.to_datetime(snap["Fecha"], errors="coerce")
        snap["Importe"] = pd.to_numeric(snap["Importe"], errors="coerce").fillna(0.0)
        for c in ("Tercero", "Serie", "Cc", "Numero"):
            if c not in snap:
                snap[c] = np.nan
        snap = categorizar_mayor(snap)
        if saldo_ant is not None:
            snap_final = saldo_ant + snap["Importe"].sum()
    hoja_ext = next((s for s in xls.sheet_names if s == "Extracto_cat"), None) or \
        next((s for s in xls.sheet_names if _norm(s).startswith("extracto")), None)
    if hoja_ext:
        d = pd.read_excel(xls, sheet_name=hoja_ext)
        col = next((c for c in d.columns if str(c).strip().lower() == "fecha"), d.columns[0])
        f = pd.to_datetime(d[col], errors="coerce")
        ext_desde, ext_hasta = f.min(), f.max()
    if (ext_hasta is None or pd.isna(ext_hasta)) and snap is not None and len(snap):
        ext_desde, ext_hasta = snap["Fecha"].min(), snap["Fecha"].max()
    if ext_hasta is not None and pd.isna(ext_hasta):
        ext_desde, ext_hasta = None, None
    if ext_hasta is None and mes is not None and pd.notna(mes):
        ext_desde = pd.Timestamp(mes).replace(day=1)
        ext_hasta = (ext_desde + pd.offsets.MonthEnd(0)).normalize()

    def _cerca(a, b, tol=1.0):
        try:
            return a is not None and b is not None and abs(float(a) - float(b)) <= tol
        except (TypeError, ValueError):
            return False

    con_saldo = [c for c, b in bloques.items() if isinstance(b["saldo_contable"], (int, float))]
    duenio_cruce = next((c for c, b in bloques.items() if _cerca(b["saldo_contable"], snap_final)), None)
    if cuenta is not None:
        num = re.sub(r"[^0-9]", "", str(cuenta))
        elegida = next((c for c in bloques if num and num in re.sub(r"[^0-9]", "", c)), None) or \
            next((c for c in bloques if _norm(c) == _norm(cuenta)), None)
        if elegida is None:
            raise ValueError(f"La cuenta {cuenta} no está en la conciliación anterior. Cuentas: {list(bloques)}")
        cuenta, criterio = elegida, "indicada"
    elif len(bloques) == 1:
        cuenta, criterio = next(iter(bloques)), "única cuenta de la hoja"
    elif (c := next((c for c, b in bloques.items() if _cerca(b["saldo_extracto"], saldo_ext_inicio)), None)):
        cuenta, criterio = c, "saldo del extracto coincide"
    elif (c := next((c for c, b in bloques.items() if _cerca(b["saldo_contable"], saldo_mayor_inicio)), None)):
        cuenta, criterio = c, "saldo contable coincide"
    elif duenio_cruce:
        cuenta, criterio = duenio_cruce, "cuenta de las hojas del cruce"
    else:
        cuenta, criterio = (con_saldo or list(bloques))[0], "primera cuenta con saldo de la hoja"
    blo = bloques[cuenta]

    foto = None
    hoja_foto = next((h for h in (_nombre_hoja_foto(cuenta), HOJA_FOTO) if h in xls.sheet_names
                      and (h != HOJA_FOTO or len(con_saldo) <= 1)), None)
    if hoja_foto:
        foto = _leer_foto_mayor(pd.read_excel(xls, sheet_name=hoja_foto, usecols="A:H"))
        if foto is not None:
            wsf = wb_v[hoja_foto]
            desde_f = _a_fecha(wsf["K1"].value) if _norm(str(wsf["J1"].value)) == "cubredesde" else pd.NaT
            hasta_f = _a_fecha(wsf["K2"].value) if _norm(str(wsf["J2"].value)) == "cubrehasta" else pd.NaT
            # Fotos sin período anotado: cubren desde su primer asiento
            foto.attrs["cubre_desde"] = desde_f if pd.notna(desde_f) else (foto["Fecha"].min() if len(foto) else pd.NaT)
            foto.attrs["cubre_hasta"] = hasta_f if pd.notna(hasta_f) else (foto["Fecha"].max() if len(foto) else pd.NaT)
            if pd.isna(foto.attrs["cubre_desde"]):
                foto = None
    lin = pd.DataFrame(blo["lineas"])
    if not lin.empty:
        signo = lin["seccion"].map({"ING_MAYOR": 1, "SAL_MAYOR": -1, "CRED_BANCO": 1, "DEB_BANCO": -1})
        lin["lado"] = lin["seccion"].map({"ING_MAYOR": "M", "SAL_MAYOR": "M", "CRED_BANCO": "E", "DEB_BANCO": "E"})
        lin["importe"] = signo * lin["importe_hoja"]
    if len(con_saldo) > 1 and duenio_cruce is not None and duenio_cruce != cuenta:
        snap, saldo_ant = None, None
    if foto is None and snap is not None and saldo_ant is not None and ext_desde is not None:
        foto = snap.copy()
        foto.attrs["cubre_desde"] = min(ext_desde, snap["Fecha"].min()) if len(snap) and pd.notna(snap["Fecha"].min()) else ext_desde
        foto.attrs["cubre_hasta"] = ext_hasta
        foto.attrs["origen"] = "hojas del cruce"
    if not lin.empty:
        lin = _completar_lineas_sin_dato(lin, xls)

    return {
        "cuenta": cuenta, "pendientes": lin, "mes": mes,
        "saldo_contable": blo["saldo_contable"], "saldo_extracto": blo["saldo_extracto"],
        "diferencia": blo["diferencia"], "snapshot_mayor": snap, "foto_mayor": foto,
        "saldo_anterior_mayor": saldo_ant, "desde": ext_desde, "hasta": ext_hasta,
        "otras_cuentas": [c for c in bloques if c != cuenta and c in con_saldo],
        "razon_social": razon_social, "criterio_cuenta": criterio,
    }


def _completar_lineas_sin_dato(lin, xls):
    """Líneas de la CC anterior sin fecha o sin concepto: se completan con el pendiente de
    esa misma conciliación que tiene ese importe, si es único."""
    lin = lin.copy()
    falta = lin["fecha"].isna() | lin["concepto"].isna() | lin["concepto"].astype(str).str.strip().isin(["", "nan", "None"])
    if not falta.any():
        return lin
    fuentes = {}
    if "Faltante Mayor" in xls.sheet_names:
        fm = pd.read_excel(xls, sheet_name="Faltante Mayor")
        if {"fecha", "importe"} <= set(fm.columns):
            fuentes["E"] = pd.DataFrame({
                "fecha": pd.to_datetime(fm["fecha"], errors="coerce"),
                "importe": pd.to_numeric(fm["importe"], errors="coerce").round(2),
                "cat": fm.get("conciliacion", pd.Series("0", index=fm.index)).astype(str),
                "desc": fm.get("descripcion", pd.Series("", index=fm.index)).astype(str),
                "obs": fm.get("tercero", pd.Series("", index=fm.index)).astype(str)})
    if "Faltante Extracto" in xls.sheet_names:
        fe = pd.read_excel(xls, sheet_name="Faltante Extracto")
        if {"Fecha", "Importe"} <= set(fe.columns):
            fuentes["M"] = pd.DataFrame({
                "fecha": pd.to_datetime(fe["Fecha"], errors="coerce"),
                "importe": pd.to_numeric(fe["Importe"], errors="coerce").round(2),
                "cat": fe.get("conciliacion", pd.Series("0", index=fe.index)).astype(str),
                "desc": fe.get("Comentario", pd.Series("", index=fe.index)).astype(str),
                "obs": fe.get("Tercero", pd.Series("", index=fe.index)).astype(str)})
    # Planillas del equipo: hoja "Pendientes" o "EXTRACTO 2788" (FECHA, DESCRIPCION,
    # CONCILIACION, DEBITO EN $, CREDITO EN $), con el encabezado en cualquier renglón
    if "E" not in fuentes:
        partes = []
        for s in xls.sheet_names:
            if not (_norm(s).startswith("pendiente") or _norm(s).startswith("extracto")):
                continue
            h = pd.read_excel(xls, sheet_name=s, header=None)
            for i in range(min(15, len(h))):
                cab = [_norm(str(v)) for v in h.iloc[i]]
                if "fecha" in cab and any(c.startswith("debito") for c in cab):
                    d = h.iloc[i + 1:].copy()
                    d.columns = cab
                    deb = pd.to_numeric(d[next(c for c in cab if c.startswith("debito"))], errors="coerce").fillna(0)
                    cc = next((c for c in cab if c.startswith("credito")), None)
                    cre = pd.to_numeric(d[cc], errors="coerce").fillna(0) if cc else 0
                    partes.append(pd.DataFrame({
                        "fecha": pd.to_datetime(d["fecha"], errors="coerce"), "importe": (cre - deb).round(2),
                        "cat": d.get("conciliacion", pd.Series("0", index=d.index)).astype(str),
                        "desc": d.get("descripcion", pd.Series("", index=d.index)).astype(str),
                        "obs": d.get("descripcion", pd.Series("", index=d.index)).map(_tercero_extracto)}))
                    break
        if partes:
            fuentes["E"] = pd.concat(partes, ignore_index=True).dropna(subset=["fecha"]) \
                .drop_duplicates(["fecha", "importe", "desc"])
    for i in lin.index[falta]:
        f = fuentes.get(lin.at[i, "lado"])
        if f is None:
            continue
        c = f[(f["importe"] - round(float(lin.at[i, "importe"]), 2)).abs() <= 0.01]
        if len(c) != 1:
            continue
        c = c.iloc[0]
        if pd.isna(lin.at[i, "fecha"]) and pd.notna(c["fecha"]):
            lin.at[i, "fecha"] = c["fecha"]
        if str(lin.at[i, "concepto"]).strip() in ("", "nan", "None"):
            lin.at[i, "concepto"] = c["cat"] if c["cat"] not in ("0", "nan", "") else c["desc"]
        if str(lin.at[i, "obs"]).strip() in ("", "nan", "None") and c["obs"] not in ("nan", ""):
            lin.at[i, "obs"] = c["obs"]
    return lin


# ─────────────────────────────────────────────────────────────────────────────
# 3. SALDOS, FOTO Y RETROACTIVOS
# ─────────────────────────────────────────────────────────────────────────────

def _saldos_extracto(ext):
    f = ext["fecha"]
    if f.is_monotonic_decreasing and not f.is_monotonic_increasing:
        ext = ext.iloc[::-1]
    inicio = float(ext[COL_SALDO].iloc[0] - ext["importe"].iloc[0])
    final = float(ext[COL_SALDO].iloc[-1])
    return inicio, final, abs(inicio + ext["importe"].sum() - final) < 0.05


def _leer_foto_mayor(df):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    if not {"Asiento", "Apunte", "Fecha", "Importe"} <= set(df.columns):
        return None
    for c in COLS_FOTO:
        if c not in df:
            df[c] = np.nan
    df = df[COLS_FOTO].copy()
    df["Asiento"] = pd.to_numeric(df["Asiento"], errors="coerce")
    df["Apunte"] = pd.to_numeric(df["Apunte"], errors="coerce")
    df = df.dropna(subset=["Asiento", "Apunte"]).drop_duplicates(["Asiento", "Apunte"])
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df["Importe"] = pd.to_numeric(df["Importe"], errors="coerce").fillna(0.0).round(2)
    df["Numero"] = np.nan
    try:
        df = categorizar_mayor(df)
    except Exception:
        df["conciliacion"], df["mes_ref"] = "0", df["Fecha"].dt.strftime("%m-%y")
    return df


def _armar_foto_mayor(may, hasta, desde, meses=MESES_FOTO, foto_anterior=None):
    """Foto que guarda cada CC: el mayor usado en la corrida, últimos `meses` meses hasta
    `hasta`. Guarda además desde qué fecha cubre: un mes sin asientos también es una foto
    válida (dice que en ese mes no había nada). Los meses que el mayor cargado no cubre se
    conservan de la foto anterior."""
    f = may.dropna(subset=["Asiento"]).copy()
    f["Fecha"] = pd.to_datetime(f["Fecha"], errors="coerce")
    cubre = min(desde, f["Fecha"].min()) if f["Fecha"].notna().any() else desde
    if foto_anterior is not None and pd.notna(foto_anterior.attrs.get("cubre_desde", pd.NaT)) \
            and pd.notna(foto_anterior.attrs.get("cubre_hasta", pd.NaT)) \
            and foto_anterior.attrs["cubre_hasta"] >= cubre - pd.Timedelta(days=1):
        cubre = min(cubre, foto_anterior.attrs["cubre_desde"])
    if foto_anterior is not None and len(foto_anterior) and f["Fecha"].notna().any():
        f = pd.concat([foto_anterior[foto_anterior["Fecha"] < f["Fecha"].min()], f], ignore_index=True)
    elif foto_anterior is not None and len(foto_anterior):
        f = foto_anterior.copy()
    f["Fecha"] = pd.to_datetime(f["Fecha"], errors="coerce")
    f = f[(f["Fecha"] <= hasta) & (f["Fecha"] > hasta - pd.DateOffset(months=meses))]
    for c in COLS_FOTO:
        if c not in f:
            f[c] = np.nan
    f = f[COLS_FOTO].copy()
    f["Asiento"] = pd.to_numeric(f["Asiento"], errors="coerce")
    f["Apunte"] = pd.to_numeric(f["Apunte"], errors="coerce")
    f["Importe"] = pd.to_numeric(f["Importe"], errors="coerce").round(2)
    f = f.sort_values(["Fecha", "Asiento", "Apunte"]).reset_index(drop=True)
    f.attrs["cubre_desde"] = max(pd.Timestamp(cubre), (hasta - pd.DateOffset(months=meses)).normalize() + pd.Timedelta(days=1))
    f.attrs["cubre_hasta"] = hasta
    return f


def _inicio_mayor(movs):
    """Desde qué fecha cubre el mayor cargado: el día 1 del mes de su primer asiento."""
    f = pd.to_datetime(movs["Fecha"], errors="coerce").min() if len(movs) else pd.NaT
    return f.replace(day=1).normalize() if pd.notna(f) else pd.NaT


def analizar_retroactivos(may_full, desde_actual, ant):
    """Compara el mayor actual contra la foto de la conciliación anterior."""
    snap = ant["snapshot_mayor"]
    misma_ventana = ant["hasta"] is not None and ant["hasta"] >= desde_actual
    movs = may_full.dropna(subset=["Asiento"]).copy()
    movs["Asiento"] = pd.to_numeric(movs["Asiento"], errors="coerce")
    movs["Apunte"] = pd.to_numeric(movs["Apunte"], errors="coerce")
    vacio = movs.iloc[0:0]
    res = {"modo": "mismo período" if misma_ventana else "período siguiente",
           "retro": vacio, "borrados": vacio, "modificados": pd.DataFrame(),
           "cubre_periodo_anterior": False, "con_foto": False}

    foto = ant.get("foto_mayor")
    hasta_ant = ant.get("hasta")
    if foto is not None and hasta_ant is None:
        hasta_ant = foto.attrs.get("cubre_hasta")
    if foto is not None and len(movs) and hasta_ant is not None and pd.notna(hasta_ant):
        res["con_foto"] = True
        # El mayor se baja por meses completos: cubre desde el día 1 del mes de su primer asiento
        ini = max(foto.attrs["cubre_desde"], _inicio_mayor(movs))
        f = foto[(foto["Fecha"] >= ini) & (foto["Fecha"] <= hasta_ant)]
        m = movs[(movs["Fecha"] >= ini) & (movs["Fecha"] <= hasta_ant)]
        kf = set(zip(f["Asiento"], f["Apunte"]))
        km = set(zip(movs["Asiento"], movs["Apunte"]))
        res["cubre_periodo_anterior"] = True
        res["inicio_comparacion"] = ini
        res["retro"] = m[np.array([k not in kf for k in zip(m["Asiento"], m["Apunte"])], dtype=bool)]
        # Antes de lo que cubre la foto no hay con qué comparar, pero los asientos se numeran
        # en orden: uno con número mayor que el último de la foto se cargó después de la
        # conciliación anterior, aunque tenga fecha vieja (9dD: asientos 147765 a 147789
        # cargados en octubre con fecha de enero a junio).
        max_foto = pd.to_numeric(foto["Asiento"], errors="coerce").max() if len(foto) else np.nan
        if pd.notna(max_foto):
            nuevos = movs[(movs["Fecha"] < ini) & (movs["Asiento"] > max_foto)]
            if len(nuevos):
                res["retro"] = pd.concat([nuevos, res["retro"]])
                res["nuevos_fuera_de_foto"] = len(nuevos)
        res["borrados"] = f[np.array([k not in km for k in zip(f["Asiento"], f["Apunte"])], dtype=bool)]
        j = f.merge(movs, on=["Asiento", "Apunte"], suffixes=("_ant", "_act"))
        res["modificados"] = j[(j["Importe_ant"].round(2) != j["Importe_act"].round(2)) | (j["Fecha_ant"] != j["Fecha_act"])]
        return res

    if snap is None or snap.empty or ant.get("desde") is None:
        res["sin_foto"] = True
        return res

    claves_snap = set(zip(snap["Asiento"], snap["Apunte"]))
    claves_act = set(zip(movs["Asiento"], movs["Apunte"]))
    max_asiento_snap = snap["Asiento"].max()
    ant_desde, ant_hasta = ant["desde"], ant["hasta"]
    if len(snap) and pd.notna(snap["Fecha"].min()):
        ant_desde = max(ant_desde, snap["Fecha"].min())
    cubre = movs["Fecha"].min() <= ant_desde if len(movs) else False
    res["cubre_periodo_anterior"] = bool(cubre)
    en_ventana = (movs["Fecha"] <= ant_hasta) & ((movs["Fecha"] >= ant_desde) | (movs["Asiento"] > max_asiento_snap))
    res["retro"] = movs[en_ventana & np.array([k not in claves_snap for k in zip(movs["Asiento"], movs["Apunte"])], dtype=bool)]
    if misma_ventana or cubre:
        res["borrados"] = snap[np.array([k not in claves_act for k in zip(snap["Asiento"], snap["Apunte"])], dtype=bool)]
        j = snap.merge(movs, on=["Asiento", "Apunte"], suffixes=("_ant", "_act"))
        res["modificados"] = j[(j["Importe_ant"].round(2) != j["Importe_act"].round(2)) | (j["Fecha_ant"] != j["Fecha_act"])]
    return res


# ─────────────────────────────────────────────────────────────────────────────
# 4. MOTOR DE APAREO SOBRE PARTIDAS PENDIENTES (mismas reglas que Galicia)
# ─────────────────────────────────────────────────────────────────────────────

class Apareo:
    def __init__(self, partidas: pd.DataFrame):
        self.p = partidas.reset_index(drop=True)
        self.p["estado"] = "pendiente"
        self.p["grupo"] = 0
        self.p["regla"] = ""
        self.grupo = 0

    def _pend(self, mask):
        return self.p[mask.reindex(self.p.index, fill_value=False) & (self.p["estado"] == "pendiente")]

    def _cerrar(self, idx, regla):
        self.grupo += 1
        self.p.loc[idx, ["estado", "grupo", "regla"]] = ["conciliado", self.grupo, regla]

    @staticmethod
    def _score(a, b):
        na, nb = str(a.get("nombre") or ""), str(b.get("nombre") or "")
        if not na or not nb or na == "nan" or nb == "nan":
            return 0
        return fuzz.token_set_ratio(na.upper(), nb.upper())

    @staticmethod
    def _clave_mes(d):
        """Categoría normalizada + mes de referencia (el del comentario si lo tiene)."""
        cat = d["categoria"].astype(str).str.lower().str.normalize("NFKD") \
            .str.encode("ascii", "ignore").str.decode("ascii").str.strip()
        mes = d["mes_ref"].where(d["mes_ref"].notna(), d["fecha"].dt.strftime("%m-%y")).astype(str)
        return cat + "|" + mes

    def uno_a_uno(self, mask_a, mask_b, tol, regla, misma_categoria=False, max_dias=None, exigir_vinculo=False):
        """Un renglón de A contra un renglón de B. Si A y B son del mismo lado se buscan
        importes opuestos que se anulan."""
        for ia in self._pend(mask_a).assign(_abs=lambda d: d["importe"].abs()).sort_values("_abs", ascending=False).index:
            if self.p.at[ia, "estado"] != "pendiente":
                continue
            a = self.p.loc[ia]
            cand = self._pend(mask_b)
            cand = cand[cand.index != ia]
            objetivo = -a["importe"] if (cand["lado"] == a["lado"]).all() and len(cand) else a["importe"]
            cand = cand[(cand["importe"] - objetivo).abs() <= tol]
            if misma_categoria:
                cand = cand[cand["categoria"] == a["categoria"]]
            if max_dias is not None and pd.notna(a["fecha"]):
                cand = cand[(cand["fecha"] - a["fecha"]).abs() <= pd.Timedelta(days=max_dias)]
            if cand.empty:
                continue
            c = cand.copy()
            c["_cat"] = (c["categoria"] == a["categoria"]) & (a["categoria"] != "0")
            c["_sc"] = c.apply(lambda b: self._score(a, b), axis=1)
            if exigir_vinculo:
                c = c[c["_cat"] | (c["_sc"] >= SCORE_NOMBRE)]
                if c.empty:
                    continue
            c["_dd"] = (c["fecha"] - a["fecha"]).abs() if pd.notna(a["fecha"]) else pd.Timedelta(0)
            c["_di"] = (c["importe"] - objetivo).abs()
            ib = c.sort_values(["_cat", "_sc", "_di", "_dd"], ascending=[False, False, True, True]).index[0]
            self._cerrar([ia, ib], regla)

    def combinacion(self, mask_a, mask_b, tol, regla, max_k=MAX_COMBINACION, vinculo="categoria", max_dias=None):
        """Un renglón de A contra la suma de 2..k renglones de B vinculados (misma categoría,
        mismo nombre, o sin vínculo)."""
        for ia in self._pend(mask_a).assign(_abs=lambda d: d["importe"].abs()).sort_values("_abs", ascending=False).index:
            if self.p.at[ia, "estado"] != "pendiente":
                continue
            a = self.p.loc[ia]
            cand = self._pend(mask_b)
            cand = cand[(cand.index != ia) & (np.sign(cand["importe"]) == np.sign(a["importe"]))]
            cand = cand[cand["importe"].abs() <= abs(a["importe"]) + tol]
            if max_dias is not None and pd.notna(a["fecha"]):
                cand = cand[(cand["fecha"] - a["fecha"]).abs() <= pd.Timedelta(days=max_dias)]
            if vinculo == "categoria":
                cand = cand[(cand["categoria"] == a["categoria"]) & (a["categoria"] != "0")]
            elif vinculo == "categoria_o_nombre":
                sc = cand.apply(lambda b: self._score(a, b), axis=1) if len(cand) else pd.Series(dtype=float)
                cand = cand[((cand["categoria"] == a["categoria"]) & (a["categoria"] != "0")) | (sc >= SCORE_NOMBRE)]
            if len(cand) < 2:
                continue
            if len(cand) > 40:
                cand = cand.assign(_d=(cand["importe"] - a["importe"]).abs()).nsmallest(40, "_d")
            k_max = min(max_k, len(cand)) if len(cand) <= 20 else min(3, len(cand))
            hallado = None
            for k in range(2, k_max + 1):
                hallado = next((list(c) for c in itertools.combinations(cand.index, k)
                                if abs(cand.loc[list(c), "importe"].sum() - a["importe"]) <= tol), None)
                if hallado:
                    break
            if hallado is None and abs(cand["importe"].sum() - a["importe"]) <= tol:
                hallado = list(cand.index)
            if hallado:
                self._cerrar([ia] + hallado, regla)

    def uno_a_uno_con_diferencia(self, mask_a, mask_b, regla, max_dias=3, tol_rel=TOL_RELATIVA_COMPROBANTE):
        """Un renglón de A contra uno de B de la misma categoría, a pocos días, cuyo importe
        difiere en menos del 0,1% (p. ej. 9dD: pendiente 'ORDENES DE PAGO COMEX' 5.126.699,25
        contra el asiento 146147 por 5.126.696,19, mismo día). Solo si hay UN candidato. Se
        cierran los dos y la diferencia queda como línea aparte, como en el cruce por
        comprobante de Galicia."""
        for ia in self._pend(mask_a).assign(_abs=lambda d: d["importe"].abs()).sort_values("_abs", ascending=False).index:
            if self.p.at[ia, "estado"] != "pendiente":
                continue
            a = self.p.loc[ia]
            if pd.isna(a["fecha"]) or str(a["categoria"]) in ("0", "nan", ""):
                continue
            cand = self._pend(mask_b)
            cand = cand[(cand.index != ia) & (cand["categoria"] == a["categoria"])
                        & (np.sign(cand["importe"]) == np.sign(a["importe"]))
                        & ((cand["fecha"] - a["fecha"]).abs() <= pd.Timedelta(days=max_dias))]
            cand = cand[(cand["importe"] - a["importe"]).abs() <= max(TOL_CRUCE, abs(a["importe"]) * tol_rel)]
            if len(cand) != 1:
                continue
            ib = cand.index[0]
            b = self.p.loc[ib]
            e, m = (a, b) if a["lado"] == "E" else (b, a)
            dif = round(e["importe"] - m["importe"], 2)   # lo que el banco tiene de más que el mayor
            self._cerrar([ia, ib], regla)
            if abs(dif) >= TOL_LINEA_DIFERENCIA:
                self.p.loc[len(self.p)] = {
                    **{c: None for c in self.p.columns},
                    "lado": "E", "origen": "Diferencia de apareo", "fecha": e["fecha"],
                    "categoria": e["categoria"], "concepto": f"Diferencia {e['categoria']}",
                    "nombre": str(m.get("referencia") or ""),
                    "detalle": f"Banco {e['importe']:,.2f} - mayor {m['importe']:,.2f} ({m.get('referencia') or ''})",
                    "referencia": f"{e.get('referencia') or ''} / {m.get('referencia') or ''}", "importe": dif,
                    "rid": f"D{ia}-{ib}", "mes_ref": e.get("mes_ref"), "comprobante": "",
                    "ya_conciliado_antes": False, "estado": "pendiente", "grupo": 0, "regla": ""}

    def par_mayor_vs_banco(self, mask_m, mask_e, mask_req, tol, regla, max_dias=3):
        """Un pendiente del banco contra la suma de DOS renglones del mayor de la misma fecha
        (± max_dias), de cualquier signo, con al menos uno tocado para atrás. Solo si la
        combinación es única."""
        req = set(self.p.index[mask_req.reindex(self.p.index, fill_value=False).astype(bool)])
        for ie in self._pend(mask_e).index:
            if self.p.at[ie, "estado"] != "pendiente":
                continue
            e = self.p.loc[ie]
            if pd.isna(e["fecha"]):
                continue
            cand = self._pend(mask_m)
            cand = cand[(cand["fecha"] - e["fecha"]).abs() <= pd.Timedelta(days=max_dias)]
            if len(cand) < 2 or len(cand) > 40:
                continue
            sol = [list(c) for c in itertools.combinations(cand.index, 2)
                   if (c[0] in req or c[1] in req) and abs(cand.loc[list(c), "importe"].sum() - e["importe"]) <= tol]
            if len(sol) == 1:
                self._cerrar([ie] + sol[0], regla)

    def bancarios_en_conjunto(self, mask_m, mask_e, tol, regla):
        """Todos los pendientes anteriores de gastos bancarios y préstamos (mayor y banco) se
        cierran juntos si el total del mayor iguala al del banco."""
        txt = (self.p["categoria"].astype(str) + " " + self.p["concepto"].astype(str)).str.lower()
        es_banc = txt.str.contains(r"\bgb\b|gastos bancarios|prestamo|préstamo", regex=True)
        pm, pe = self._pend(mask_m & es_banc), self._pend(mask_e & es_banc)
        if pm.empty or pe.empty:
            return
        if abs(pm["importe"].sum() - pe["importe"].sum()) <= tol:
            self._cerrar(list(pm.index) + list(pe.index), regla)

    def compensacion_categoria_mes(self, mask, tol, regla):
        """Todos los pendientes de una categoría y un mes de referencia (de ambos lados y de
        cualquier origen dentro de mask) se cierran juntos si el total del mayor iguala al
        del banco. P. ej. 'GB 09-26' pendiente de la conciliación anterior contra 'Pago GB
        09-26' + 'GB 09-26' registrados después."""
        pp = self._pend(mask)
        pp = pp[pp["fecha"].notna() & ~pp["categoria"].astype(str).isin(["0", "nan", ""])]
        if pp.empty:
            return
        for _, g in pp.groupby(self._clave_mes(pp)):
            if len(g) < 2 or g["lado"].nunique() < 2:
                continue
            sm = g.loc[g["lado"] == "M", "importe"].sum()
            se = g.loc[g["lado"] == "E", "importe"].sum()
            if abs(sm - se) <= tol:
                self._cerrar(list(g.index), regla)

    def subconjunto_categoria(self, mask_a, mask_b, tol, regla, max_dias=15, misma_categoria=True):
        """Un renglón de A contra un subconjunto de B del mismo signo, de cualquier tamaño
        (hasta 40 candidatos). Solo si la combinación es única; si hay varias que solo difieren
        en renglones de igual importe, se toma una y lo repetido queda pendiente."""
        for ia in self._pend(mask_a).assign(_abs=lambda d: d["importe"].abs()).sort_values("_abs", ascending=False).index:
            if self.p.at[ia, "estado"] != "pendiente":
                continue
            a = self.p.loc[ia]
            if misma_categoria and str(a["categoria"]) in ("0", "nan", ""):
                continue
            cand = self._pend(mask_b)
            cand = cand[(cand.index != ia) & (np.sign(cand["importe"]) == np.sign(a["importe"]))
                        & (cand["importe"].abs() <= abs(a["importe"]) + tol)]
            if misma_categoria:
                cand = cand[cand["categoria"] == a["categoria"]]
            if max_dias is not None:
                if pd.isna(a["fecha"]):
                    continue
                cand = cand[(cand["fecha"] - a["fecha"]).abs() <= pd.Timedelta(days=max_dias)]
            if len(cand) < 2 or len(cand) > 40:
                continue
            sel = _subconjunto_exacto(list(cand["importe"]), float(a["importe"]), tol=tol)
            if not sel:
                unicos = cand.drop_duplicates("importe")
                if len(unicos) < len(cand) and len(unicos) >= 2:
                    s2 = _subconjunto_exacto(list(unicos["importe"]), float(a["importe"]), tol=tol)
                    sel = [list(cand.index).index(unicos.index[i]) for i in s2] if s2 else []
            if sel and len(sel) >= 2:
                self._cerrar([ia] + [cand.index[i] for i in sel], regla)

    def anulacion_suma(self, mask, tol, regla, max_k=3):
        """Del mismo lado: un renglón se anula con la suma de 2..k renglones de signo contrario.
        Solo si la combinación es única."""
        for ia in self._pend(mask).assign(_abs=lambda d: d["importe"].abs()).sort_values("_abs", ascending=False).index:
            if self.p.at[ia, "estado"] != "pendiente":
                continue
            a = self.p.loc[ia]
            cand = self._pend(mask)
            cand = cand[(cand.index != ia) & (np.sign(cand["importe"]) == -np.sign(a["importe"]))
                        & (cand["importe"].abs() <= abs(a["importe"]) + tol)]
            if len(cand) < 2 or len(cand) > 40:
                continue
            soluciones = []
            for k in range(2, min(max_k, len(cand)) + 1):
                for combo in itertools.combinations(cand.index, k):
                    if abs(cand.loc[list(combo), "importe"].sum() + a["importe"]) <= tol:
                        soluciones.append(list(combo))
                        if len(soluciones) > 1:
                            break
                if soluciones:
                    break
            if len(soluciones) == 1:
                self._cerrar([ia] + soluciones[0], regla)

    def categoria_vs_sin_categoria(self, mask_a, mask_b, tol, regla, max_k=MAX_COMBINACION):
        """El total pendiente de cada categoría de A contra una combinación de renglones de B
        sin categoría (o de la misma)."""
        pa = self._pend(mask_a)
        for cat, ga in pa[pa["categoria"] != "0"].groupby("categoria"):
            objetivo = ga["importe"].sum()
            cand = self._pend(mask_b)
            cand = cand[cand["categoria"].isin(["0", cat]) & (np.sign(cand["importe"]) == np.sign(objetivo))
                        & (cand["importe"].abs() <= abs(objetivo) + tol)]
            if cand.empty:
                continue
            if len(cand) > 40:
                cand = cand.assign(_d=(cand["importe"] - objetivo).abs()).nsmallest(40, "_d")
            hallado = None
            for k in range(1, min(max_k, len(cand)) + 1):
                hallado = next((list(c) for c in itertools.combinations(cand.index, k)
                                if abs(cand.loc[list(c), "importe"].sum() - objetivo) <= tol), None)
                if hallado:
                    break
            if hallado:
                self._cerrar(list(ga.index) + hallado, regla)

    def explicar_cambio_saldo(self, mask_ru, mask_ant, regla, tol=TOL_CAMBIO_SALDO, max_items=40):
        """Qué pendientes anteriores explican el cambio de saldo inicial no identificado
        (registrados o anulados con fecha ya conciliada). Un pendiente del banco registrado
        suma su importe; un pendiente del mayor anulado resta el suyo."""
        ru = self._pend(mask_ru)
        if ru.empty:
            return
        ir = ru.index[0]
        objetivo = round(self.p.at[ir, "importe"], 2)
        items = self._pend(mask_ant)
        if items.empty:
            return
        items = items.assign(_v=np.where(items["lado"] == "E", items["importe"], -items["importe"]).round(2))
        # Un único pendiente que explica exacto el cambio, aunque sea chico (9dD: LX ARGENTINA 400)
        exacto = items[(items["_v"] - objetivo).abs() <= tol]
        if len(exacto) == 1:
            self._cerrar([ir, exacto.index[0]], regla)
            return
        items = items.reindex(items["_v"].abs().sort_values(ascending=False).index)[:max_items]
        idx = list(items.index)
        mejor = _subconjunto_exacto(list(items["_v"]), objetivo, tol)
        if mejor:
            self._cerrar([ir] + [idx[i] for i in mejor], regla)

    def por_categoria_mes(self, mask_a, mask_b, tol, regla):
        """Todos los pendientes de una categoría y mes de referencia de A contra los de B, por suma."""
        pa, pb = self._pend(mask_a), self._pend(mask_b)
        if pa.empty or pb.empty:
            return
        ka, kb = self._clave_mes(pa), self._clave_mes(pb)
        for k in set(ka) & set(kb):
            if k.startswith("0|") or k.endswith("|nan"):
                continue
            ga, gb = pa[ka == k], pb[kb == k]
            if abs(ga["importe"].sum() - gb["importe"].sum()) <= tol:
                self._cerrar(list(ga.index) + list(gb.index), regla)


# ─────────────────────────────────────────────────────────────────────────────
# 5. PIPELINE COMPLETO
# ─────────────────────────────────────────────────────────────────────────────

def _razon_social_anterior(contenido):
    """Razón social que figura en la hoja CC de la conciliación anterior (celda 'Razón social:')."""
    if not contenido:
        return None
    try:
        xls = pd.ExcelFile(BytesIO(_contenido_xlsx(contenido)))
        hoja = next((h for h in xls.sheet_names if _norm(h).startswith("concilia") or _norm(h) == "cc"), None)
        if hoja is None:
            return None
        d = xls.parse(hoja, header=None, nrows=15)
        for _, r in d.iterrows():
            vals = list(r.values)
            for k, v in enumerate(vals[:-1]):
                if isinstance(v, str) and _norm(v).startswith("razonsocial"):
                    sig = vals[k + 1]
                    return str(sig).strip() if pd.notna(sig) else None
    except Exception:
        return None
    return None


def correr_conciliacion_completa(archivo_mayor, archivo_extracto, archivo_conciliacion_anterior=None,
                                 analista="", razon_social=None, banco=BANCO, cuenta=None,
                                 archivos_conciliaciones_previas=None):
    # ── Carga ────────────────────────────────────────────────────────────────
    b_mayor, b_ext = _a_bytes(archivo_mayor), _a_bytes(archivo_extracto)
    raw_m = load_excel_file(BytesIO(b_mayor))
    raw_e = load_excel_file(BytesIO(b_ext))
    raw_m["_rid"] = ["M%06d" % i for i in range(len(raw_m))]
    may_full = normalize_mayor(raw_m)
    ext = normalize_extracto_hipotecario(raw_e)
    ext["_rid"] = ["E%06d" % i for i in range(len(ext))]
    if ext.empty:
        raise ValueError("El extracto no tiene movimientos.")

    desde, hasta = ext["fecha"].min(), ext["fecha"].max()
    # El período es el mes predominante del extracto (algunos traen movimientos del último
    # día hábil del mes anterior); si arranca a los pocos días del mes, empieza el día 1.
    mes_dominante = ext["fecha"].dt.to_period("M").mode()[0]
    desde = max(desde, mes_dominante.start_time)
    if (desde - mes_dominante.start_time).days <= 5:
        desde = mes_dominante.start_time
    # Si el extracto termina pocos días antes de fin de mes (fin de semana: mayo cierra el
    # viernes 29), el corte igual es fin de mes: el equipo registra los gastos con fecha 31.
    fin_mes = mes_dominante.end_time.normalize()
    if 0 < (fin_mes - hasta).days <= 3:
        hasta = fin_mes
    es_saldo_ant = may_full["Comentario"].astype(str).str.contains("saldo anterior", case=False, na=False)
    # Lo que el mayor trae con fecha posterior al último día del extracto entra en la próxima.
    post = (~es_saldo_ant) & (may_full["Fecha"] > hasta)
    aviso_posterior = None
    if post.any():
        aviso_posterior = (f"El mayor trae {int(post.sum())} movimiento(s) con fecha posterior al cierre del extracto "
                           f"({hasta:%d/%m/%Y}) por {may_full.loc[post, 'Importe'].sum():,.2f}: no se incluyen en esta "
                           f"conciliación (saldo al {hasta:%d/%m/%Y}) y entran en la próxima.")
        may_full = may_full[~post].copy()
        es_saldo_ant = es_saldo_ant[~post]
    if es_saldo_ant.any():
        saldo_ini_archivo = float(may_full.loc[es_saldo_ant, "Saldo"].iloc[0])
    else:
        primera = may_full.dropna(subset=["Fecha"]).iloc[0]
        saldo_ini_archivo = float(primera["Saldo"] - primera["Importe"])
    movs = may_full[~es_saldo_ant]
    # Mayor de varios meses: lo anterior al período forma el saldo inicial; solo el período
    # entra al cruce. Lo anterior se compara contra la foto de la conciliación anterior.
    previos = movs[movs["Fecha"] < desde]
    actuales = movs[~(movs["Fecha"] < desde)]
    saldo_mayor_inicio = saldo_ini_archivo + previos["Importe"].sum()
    saldo_mayor_final = saldo_mayor_inicio + actuales["Importe"].sum()
    saldo_ext_inicio, saldo_ext_final, ext_ok = _saldos_extracto(ext)

    # ── Cruce principal (match 0 a 19) solo con el mayor del período ─────────
    # Cobranzas/Señas: solo para las sociedades de SOCIEDADES_COBRANZAS_SENAS (EASA), según la
    # razón social indicada o, si no se indica, la de la conciliación anterior
    razon_det = razon_social or _razon_social_anterior(_a_bytes(archivo_conciliacion_anterior))
    senas = _norm(razon_det or "") in [_norm(x) for x in SOCIEDADES_COBRANZAS_SENAS]
    aviso_senas = None
    if not senas and categorizar_mayor(movs)["conciliacion"].eq(COBRANZAS_SENAS).any():
        aviso_senas = ("El mayor tiene asientos 'Acreditación Señas' pero la sociedad no está marcada para "
                       "conciliar Cobranzas/Señas por total (SOCIEDADES_COBRANZAS_SENAS). Indicar la razón social.")
    may, ext_c, bytes_cruce, dif_cruce_df, stats_cruce = _correr_cruce_con_ids(actuales.reset_index(drop=True), ext, senas)
    dif_cruce = (may.loc[~may["pendiente"], "Importe"].sum() - ext_c.loc[~ext_c["pendiente"], "importe"].sum()
                 + pd.to_numeric(dif_cruce_df["importe"], errors="coerce").sum())

    # ── Partidas pendientes del período ──────────────────────────────────────
    pm = may[may["pendiente"]]
    pe = ext_c[ext_c["pendiente"]]
    es_acr = dif_cruce_df["_rid"].astype(str).str.startswith("DIF-ACR-")
    partidas = [
        pd.DataFrame({
            "lado": "M", "origen": "Período", "fecha": pm["Fecha"], "categoria": pm["conciliacion"],
            "concepto": pm["conciliacion"], "nombre": pm["Tercero"].astype(str),
            "detalle": pm["Comentario"].astype(str), "referencia": "Asiento " + pm["Asiento"].astype(str),
            "importe": pm["Importe"], "rid": pm["_rid"], "mes_ref": pm["mes_ref"],
            "comprobante": np.where(_vacio(pm["Serie"]) | _vacio(pm["Numero"]), "",
                                    pm["Serie"].astype(str) + "-" + pm["Numero"].astype(str))}),
        pd.DataFrame({
            "lado": "E", "origen": "Período", "fecha": pe["fecha"], "categoria": pe["conciliacion"],
            "concepto": pe["conciliacion"], "nombre": pe["tercero"].astype(str),
            "detalle": pe["descripcion"].astype(str), "referencia": pe["referencia"].astype(str),
            "importe": pe["importe"], "rid": pe["_rid"], "mes_ref": pe["mes_ref"]}),
        pd.DataFrame({
            "lado": "E", "origen": np.where(es_acr, "Período", "Diferencia de apareo"),
            "fecha": pd.to_datetime(dif_cruce_df["fecha"]), "categoria": dif_cruce_df["conciliacion"].astype(str),
            "concepto": np.where(es_acr, dif_cruce_df["conciliacion"].astype(str),
                                 "Diferencia " + dif_cruce_df["conciliacion"].astype(str)),
            "nombre": dif_cruce_df["descripcion"].astype(str), "detalle": dif_cruce_df["descripcion"].astype(str),
            "referencia": dif_cruce_df["observaciones"].astype(str),
            "importe": pd.to_numeric(dif_cruce_df["importe"], errors="coerce"), "rid": dif_cruce_df["_rid"],
            "mes_ref": pd.to_datetime(dif_cruce_df["fecha"]).dt.strftime("%m-%y")}),
    ]

    # ── Conciliación anterior ────────────────────────────────────────────────
    control = {"desde": desde, "hasta": hasta, "saldo_mayor_inicio": saldo_mayor_inicio,
               "saldo_mayor_final": saldo_mayor_final, "saldo_ext_inicio": saldo_ext_inicio,
               "saldo_ext_final": saldo_ext_final, "extracto_consistente": ext_ok,
               "dif_cruce": dif_cruce, "tiene_anterior": archivo_conciliacion_anterior is not None,
               "aviso_posterior": " / ".join(x for x in (aviso_posterior, aviso_senas) if x) or None}
    ant, retro, pend_ant = None, None, pd.DataFrame()
    if archivo_conciliacion_anterior is not None:
        ant = leer_conciliacion_anterior(archivo_conciliacion_anterior, cuenta,
                                         saldo_ext_inicio=saldo_ext_inicio, saldo_mayor_inicio=saldo_mayor_inicio)
        cuenta = ant["cuenta"]
        razon_ant = ant.get("razon_social")
        if razon_social is None:
            razon_social = razon_ant
        elif razon_ant and _norm(razon_ant) != _norm(razon_social):
            control["aviso_sociedad"] = (f"La conciliación anterior es de '{razon_ant}' y se indicó "
                                         f"'{razon_social}'. Verificar que sea el archivo correcto.")
        control.update({"cuenta": cuenta, "criterio_cuenta": ant.get("criterio_cuenta"),
                        "otras_cuentas": ", ".join(ant.get("otras_cuentas") or [])})
        may_cat = categorizar_mayor(may_full[~es_saldo_ant])
        retro = analizar_retroactivos(may_cat, desde, ant)
        misma_ventana = retro["modo"] == "mismo período"

        # Saldo del mayor al inicio del período, según la conciliación anterior
        saldo_ref = ant["saldo_anterior_mayor"] if misma_ventana else ant["saldo_contable"]
        if not misma_ventana and ant.get("saldo_extracto") is not None \
                and abs(float(ant["saldo_extracto"]) - saldo_ext_inicio) > 1:
            control["aviso_cuenta"] = (
                f"El extracto empieza con saldo {saldo_ext_inicio:,.2f} y la conciliación anterior (cuenta "
                f"{cuenta}, elegida por: {ant.get('criterio_cuenta')}) cerró con saldo de extracto "
                f"{float(ant['saldo_extracto']):,.2f}. Verificar que la conciliación anterior sea la de esta cuenta "
                f"(otras cuentas en el archivo: {', '.join(ant.get('otras_cuentas') or []) or 'ninguna'}).")
        control.update({"modo": retro["modo"], "saldo_ref": saldo_ref,
                        "saldo_ext_ref": ant["saldo_extracto"] if not misma_ventana else None})

        # Pendientes que se arrastran: los de fecha anterior al período actual
        # (los del propio período los vuelve a calcular el cruce).
        lin = ant["pendientes"].copy()
        if not lin.empty:
            fecha_sin_dato = ant["hasta"] if ant["hasta"] is not None else desde - timedelta(days=1)
            lin["fecha_orig"] = lin["fecha"]
            lin["fecha"] = lin["fecha"].fillna(fecha_sin_dato)
            # Una fecha posterior al período es un error de tipeo: esa línea es de arrastre.
            futura = lin["fecha_orig"].notna() & (lin["fecha_orig"] > hasta)
            fechadas = lin[lin["fecha_orig"].notna() & ((lin["fecha_orig"] < desde) | futura)]
            sin_fecha = lin[lin["fecha_orig"].isna()]
            if misma_ventana and len(sin_fecha) and saldo_ref is not None and ant.get("diferencia") is not None:
                # Corte dentro del mismo mes (p. ej. 15-03 y luego 31-03): las líneas sin fecha
                # pueden ser del mes en curso (las recalcula el cruce) o arrastradas. Se arrastran
                # solo las que hacen que la conciliación anterior cierre con su misma DIFERENCIA.
                d0 = saldo_ref - fechadas.loc[fechadas["lado"] == "M", "importe"].sum() \
                    + fechadas.loc[fechadas["lado"] == "E", "importe"].sum() - saldo_ext_inicio
                efectos = np.where(sin_fecha["lado"] == "E", sin_fecha["importe"], -sin_fecha["importe"])
                elegidas = _subconjunto_exacto(list(efectos), float(ant["diferencia"]) - d0) \
                    if abs(float(ant["diferencia"]) - d0) > TOL_CENTAVOS else []
                arrastre = sin_fecha.iloc[elegidas] if elegidas else sin_fecha.iloc[0:0]
                arrastre = arrastre.assign(fecha=desde - timedelta(days=1))
                lin = pd.concat([fechadas, arrastre])
            else:
                lin = lin[(lin["fecha"] < desde) | futura].copy()
            lin["concepto"] = lin["concepto"].fillna("")
            pend_ant = lin
            mes_concepto = _mes_de_texto(lin["concepto"].astype(str))
            partidas.append(pd.DataFrame({
                "lado": lin["lado"], "origen": "Pendiente anterior", "fecha": lin["fecha"],
                "categoria": lin["concepto"].map(_categoria_desde_concepto),
                "concepto": lin["concepto"].astype(str), "nombre": lin["obs"].astype(str),
                "detalle": lin["obs"].astype(str), "referencia": "Fila " + lin["fila"].astype(str) + " conc. anterior",
                "importe": lin["importe"], "rid": "P" + lin["fila"].astype(str),
                "mes_ref": mes_concepto.fillna(pd.to_datetime(lin["fecha"]).dt.strftime("%m-%y"))}))

        # Movimientos retroactivos identificados (solo los anteriores al período: los del
        # propio período ya entran en el cruce)
        identificado = 0.0
        # También en un corte del mismo mes: con un mayor de varios meses, lo cargado/borrado con
        # fecha anterior al período se identifica igual (solo cuenta lo anterior al período).
        if True:
            r = retro["retro"]
            r = r[r["Fecha"] < desde]
            identificado += r["Importe"].sum()
            if len(r):
                partidas.append(pd.DataFrame({
                    "lado": "M", "origen": "Retroactivo", "fecha": r["Fecha"], "categoria": r["conciliacion"].astype(str),
                    "concepto": "Asiento retroactivo", "nombre": r["Tercero"].astype(str),
                    "detalle": r["Comentario"].astype(str),
                    "referencia": "Asiento " + r["Asiento"].astype(int).astype(str), "importe": r["Importe"],
                    "rid": "R" + r["Asiento"].astype(int).astype(str) + "-" + r["Apunte"].astype(int).astype(str),
                    "mes_ref": r["mes_ref"]}))
            b = retro["borrados"]
            if len(b):
                b = b[b["Fecha"] < desde]
                identificado -= b["Importe"].sum()
                partidas.append(pd.DataFrame({
                    "lado": "M", "origen": "Borrado", "fecha": b["Fecha"], "categoria": b["conciliacion"].astype(str),
                    "concepto": "Asiento borrado", "nombre": b["Tercero"].astype(str), "detalle": b["Comentario"].astype(str),
                    "referencia": "Asiento " + b["Asiento"].astype(int).astype(str), "importe": -b["Importe"],
                    "rid": "B" + b["Asiento"].astype(int).astype(str), "mes_ref": b["mes_ref"]}))
            md = retro["modificados"]
            if len(md):
                md = md[md["Fecha_ant"] < desde].copy()
                # Efecto sobre el saldo ya conciliado: si sigue con fecha ya conciliada, el cambio
                # de importe; si se movió al período actual, sale del saldo ya conciliado.
                movido = md["Fecha_act"] >= desde
                md["_efecto"] = np.where(movido, -md["Importe_ant"], md["Importe_act"] - md["Importe_ant"])
                md = md[md["_efecto"].abs() > TOL_CENTAVOS]
                retro["modificados"] = md
                if len(md):
                    identificado += md["_efecto"].sum()
                    partidas.append(pd.DataFrame({
                        "lado": "M", "origen": "Modificado", "fecha": md["Fecha_ant"],
                        "categoria": md["conciliacion_act"].astype(str) if "conciliacion_act" in md else "0",
                        "concepto": np.where(movido[md.index], "Asiento pasado al período", "Asiento modificado"),
                        "nombre": md["Tercero_act"].astype(str) if "Tercero_act" in md else "",
                        "detalle": md["Comentario_act"].astype(str) if "Comentario_act" in md else "",
                        "referencia": "Asiento " + md["Asiento"].astype(int).astype(str), "importe": md["_efecto"],
                        "rid": "X" + md["Asiento"].astype(int).astype(str) + "-" + md["Apunte"].astype(int).astype(str),
                        "mes_ref": md["mes_ref_act"] if "mes_ref_act" in md else md["Fecha_ant"].dt.strftime("%m-%y")}))

        no_identificado = (saldo_mayor_inicio - saldo_ref - identificado) if saldo_ref is not None else 0.0

        # Asientos borrados en meses anteriores al de la conciliación anterior: con conciliaciones
        # más viejas se busca la combinación ÚNICA de borrados que explica el cambio.
        if not misma_ventana and abs(no_identificado) > TOL_CENTAVOS and archivos_conciliaciones_previas:
            movs_act = may_cat.dropna(subset=["Asiento"]).copy()
            movs_act["Asiento"] = pd.to_numeric(movs_act["Asiento"], errors="coerce")
            movs_act["Apunte"] = pd.to_numeric(movs_act["Apunte"], errors="coerce")
            claves_act = set(zip(movs_act["Asiento"], movs_act["Apunte"]))
            ini_ant = ant["desde"] if ant.get("desde") is not None else desde
            cand, vistos = [], set()
            for arch in archivos_conciliaciones_previas:
                try:
                    a2 = leer_conciliacion_anterior(arch, cuenta)
                except Exception:
                    try:
                        a2 = leer_conciliacion_anterior(arch)
                    except Exception:
                        continue
                for sn in (a2.get("foto_mayor"), a2.get("snapshot_mayor")):
                    if sn is None or sn.empty:
                        continue
                    sn = sn[sn["Fecha"] < ini_ant].drop_duplicates(["Asiento", "Apunte"])
                    # Solo se puede afirmar que se borró si el mayor cargado cubre esa fecha
                    if len(movs_act):
                        sn = sn[sn["Fecha"] >= _inicio_mayor(movs_act)]
                    for _, f in sn.iterrows():
                        k = (f["Asiento"], f["Apunte"])
                        if k in claves_act or k in vistos:
                            continue
                        vistos.add(k)
                        cand.append(f)
            efectos = [-float(f["Importe"]) for f in cand]
            sel = _subconjunto_exacto(efectos, no_identificado) if cand else []
            if sel:
                b2 = pd.DataFrame([cand[i] for i in sel])
                b2 = categorizar_mayor(b2.assign(**{c: b2.get(c, np.nan) for c in ("Serie", "Tercero", "Cc", "Numero")}))
                partidas.append(pd.DataFrame({
                    "lado": "M", "origen": "Borrado", "fecha": pd.to_datetime(b2["Fecha"]),
                    "categoria": b2["conciliacion"].astype(str), "concepto": "Asiento borrado",
                    "nombre": b2["Tercero"].astype(str), "detalle": b2["Comentario"].astype(str),
                    "referencia": "Asiento " + b2["Asiento"].astype(int).astype(str),
                    "importe": -b2["Importe"].astype(float),
                    "rid": "B" + b2["Asiento"].astype(int).astype(str), "mes_ref": b2["mes_ref"]}))
                retro["borrados"] = pd.concat([retro["borrados"], b2], ignore_index=True)
                identificado += sum(efectos[i] for i in sel)
                no_identificado = saldo_mayor_inicio - saldo_ref - identificado
        control.update({"retro_identificado": identificado, "retro_no_identificado": no_identificado})

        # Si el mayor cargado no llega al período de la conciliación anterior, lo tocado para
        # atrás no se puede ver: se avisa en forma explícita.
        primer_mov = _inicio_mayor(movs) if len(movs) else None
        ini_ant = ant.get("desde") if ant.get("desde") is not None else (desde - pd.DateOffset(months=1))
        mes_ant = pd.Timestamp(ant["mes"]).replace(day=1) if ant.get("mes") is not None and pd.notna(ant.get("mes")) \
            else pd.Timestamp(ini_ant).replace(day=1)
        foto = ant.get("foto_mayor")
        detalle_ru = "Cambio del saldo inicial del mayor respecto de la conciliación anterior"
        if abs(no_identificado) > TOL_CENTAVOS and (primer_mov is None or pd.isna(primer_mov) or primer_mov > ini_ant):
            hasta_ant = ant["hasta"] if ant.get("hasta") is not None else desde - timedelta(days=1)
            control["aviso_mayor"] = (
                f"El mayor cargado " + (f"cubre desde el {primer_mov:%d/%m/%Y}" if primer_mov is not None and pd.notna(primer_mov)
                                        else "no tiene movimientos") +
                f" y no incluye el período de la conciliación anterior ({pd.Timestamp(ini_ant):%d/%m/%Y} al "
                f"{pd.Timestamp(hasta_ant):%d/%m/%Y}). Por eso no se puede ver qué se cargó, borró o modificó con fecha "
                f"ya conciliada, ni qué pendientes anteriores se registraron. Volver a correr con el mayor bajado desde "
                f"el {mes_ant:%d/%m/%Y} o antes.")
            detalle_ru = f"Mayor sin el período anterior: bajar el mayor desde el {mes_ant:%d/%m/%Y} o antes"
        elif (abs(no_identificado) > TOL_CENTAVOS and retro.get("con_foto") and foto is not None
              and primer_mov is not None and primer_mov > foto.attrs.get("cubre_desde", primer_mov)):
            ini_foto = foto.attrs["cubre_desde"]
            control["aviso_mayor"] = (
                f"Hay {no_identificado:,.2f} de cambio en el saldo ya conciliado que no se ve en el mayor cargado, "
                f"que cubre desde el {primer_mov:%d/%m/%Y}: el cambio es de un mes anterior. La foto de la conciliación "
                f"anterior cubre desde el {ini_foto:%d/%m/%Y}. Volver a correr con el mayor bajado desde el "
                f"{ini_foto:%d/%m/%Y} (o desde el mes que se sospeche) para identificarlo asiento por asiento.")
            detalle_ru = (f"Cambio en un mes anterior al {primer_mov:%d/%m/%Y}: bajar el mayor desde el "
                          f"{ini_foto:%d/%m/%Y} para identificarlo")
        elif abs(no_identificado) > TOL_CENTAVOS and not retro.get("con_foto"):
            control["aviso_mayor"] = (
                f"El saldo inicial del mayor cambió {no_identificado:,.2f} respecto de la conciliación anterior y la "
                f"conciliación anterior no tiene foto del mayor para comparar asiento por asiento. Las conciliaciones "
                f"generadas con esta lógica guardan la foto: a partir de la próxima se identifica solo.")
        if abs(no_identificado) > TOL_CENTAVOS and "aviso_mayor" not in control and primer_mov is not None:
            # El mayor cubre el período anterior y la foto, pero el cambio igual no se ve asiento por
            # asiento. Hay dos causas posibles y el aviso dice cuál aplica:
            #  a) hay pendientes anteriores con fecha anterior al inicio del mayor: lo que se registró
            #     con esas fechas no está en el mayor cargado -> bajarlo desde el pendiente más viejo;
            #  b) el mayor ya cubre todos los pendientes: el cambio es la modificación o el borrado de
            #     un asiento que ya existía, con fecha anterior a lo que cubre la foto o hecho antes de
            #     tomarla. Ningún mayor bajado hoy lo muestra (todos traen el asiento ya modificado):
            #     hay que buscarlo en el historial de asientos del sistema.
            viejo = pend_ant["fecha"].min() if len(pend_ant) else None
            hasta_ant_txt = (f"{pd.Timestamp(ant['hasta']):%d/%m/%Y}" if ant.get("hasta") is not None
                             else "la conciliación anterior")
            if viejo is not None and pd.notna(viejo) and pd.Timestamp(viejo).replace(day=1) < primer_mov:
                desde_bajar = pd.Timestamp(viejo).replace(day=1)
                control["aviso_mayor"] = (
                    f"Hay {no_identificado:,.2f} de cambio en el saldo ya conciliado que no se ve en el mayor cargado "
                    f"(cubre desde el {primer_mov:%d/%m/%Y}). Puede ser algo registrado con fecha anterior: volver a "
                    f"correr con el mayor bajado desde el {desde_bajar:%d/%m/%Y} (fecha del pendiente anterior más antiguo).")
                detalle_ru = f"Posible registro con fecha anterior al {primer_mov:%d/%m/%Y}: bajar el mayor desde el {desde_bajar:%d/%m/%Y}"
            else:
                ini_foto = foto.attrs.get("cubre_desde") if foto is not None else None
                desde_foto = f" (la foto cubre desde el {pd.Timestamp(ini_foto):%d/%m/%Y})" if ini_foto is not None and pd.notna(ini_foto) else ""
                control["aviso_mayor"] = (
                    f"Hay {no_identificado:,.2f} de cambio en el saldo ya conciliado que no se ve asiento por asiento, y el "
                    f"mayor cargado ya cubre todos los pendientes anteriores. No es algo cargado con fecha vieja: es la "
                    f"modificación o el borrado de un asiento que ya existía, con fecha anterior a lo que cubre la foto"
                    f"{desde_foto} o hecho antes de tomarla. Bajar más mayor no lo muestra: revisar en el sistema los "
                    f"asientos modificados o borrados después del {hasta_ant_txt}.")
                detalle_ru = f"Asiento modificado o borrado después del {hasta_ant_txt}: revisar el historial en el sistema"
        if abs(no_identificado) > TOL_CENTAVOS:
            partidas.append(pd.DataFrame([{
                "lado": "M", "origen": "Retroactivo no identificado", "fecha": desde - timedelta(days=1),
                "categoria": "Retroactivo", "concepto": "Mov. retroactivos sin identificar", "nombre": "",
                "detalle": detalle_ru, "referencia": "Saldo anterior", "importe": no_identificado, "rid": "RU",
                "mes_ref": np.nan}]))

        # Diferencia heredada: lo que no cerraba al inicio del período
        if saldo_ref is not None:
            arr = pend_ant
            control["dif_heredada"] = saldo_ref - (arr.loc[arr["lado"] == "M", "importe"].sum() if len(arr) else 0) \
                + (arr.loc[arr["lado"] == "E", "importe"].sum() if len(arr) else 0) - saldo_ext_inicio

    p = pd.concat(partidas, ignore_index=True)
    # Corte dentro del mismo mes: lo que ya existía al corte anterior y la CC anterior no lista
    # como pendiente ya estaba conciliado (p. ej. un asiento del 01/06 que cerró un pendiente
    # del banco de mayo: en el corte siguiente del mismo mes su par ya no está en la CC).
    p["ya_conciliado_antes"] = False
    if ant is not None and retro is not None and retro["modo"] == "mismo período":
        foto_ant = ant.get("foto_mayor")
        claves_ant = set(zip(pd.to_numeric(foto_ant["Asiento"], errors="coerce"),
                             pd.to_numeric(foto_ant["Apunte"], errors="coerce"))) if foto_ant is not None else set()
        lineas_ant = ant["pendientes"]
        imp_ant = {lado: set(lineas_ant.loc[lineas_ant["lado"] == lado, "importe_hoja"].round(2))
                   for lado in ("M", "E")} if len(lineas_ant) else {"M": set(), "E": set()}
        rid_m = set(pm.loc[[k in claves_ant for k in zip(pd.to_numeric(pm["Asiento"], errors="coerce"),
                                                         pd.to_numeric(pm["Apunte"], errors="coerce"))], "_rid"])
        hasta_ant = ant.get("hasta")
        rid_e = set(pe.loc[pe["fecha"] <= hasta_ant, "_rid"]) if hasta_ant is not None else set()
        cand = (p["origen"] == "Período") & ~p["categoria"].isin(list(CATEGORIAS_AGRUPADAS)) & (
            ((p["lado"] == "M") & p["rid"].isin(rid_m)) | ((p["lado"] == "E") & p["rid"].isin(rid_e)))
        no_listado = [round(abs(v), 2) not in imp_ant[l] for v, l in zip(p["importe"], p["lado"])]
        cand = cand & np.array(no_listado, dtype=bool)
        # Solo si hace falta: la diferencia al inicio del período no da la DIFERENCIA de la
        # conciliación anterior, y una combinación ÚNICA de estos renglones explica el salto.
        salto = (float(ant["diferencia"]) - control["dif_heredada"]) \
            if ant.get("diferencia") is not None and control.get("dif_heredada") is not None else 0.0
        if abs(salto) > TOL_CRUCE and cand.any():
            c = p[cand]
            efectos = np.where(c["lado"] == "M", c["importe"], -c["importe"]).astype(float)
            sel = _subconjunto_exacto(list(efectos), salto, tol=TOL_LINEA_DIFERENCIA) if len(c) <= 40 else None
            if sel:
                p.loc[c.index[sel], "ya_conciliado_antes"] = True
    p["fecha"] = pd.to_datetime(p["fecha"], errors="coerce")
    p["importe"] = p["importe"].astype(float).round(2)
    p["comprobante"] = p.get("comprobante", pd.Series("", index=p.index)).fillna("").astype(str)
    p["categoria"] = p["categoria"].fillna("0").astype(str)
    ap = Apareo(p)
    P = ap.p

    es_M, es_E = P["lado"] == "M", P["lado"] == "E"
    per = P["origen"] == "Período"
    ant_m = (P["origen"] == "Pendiente anterior") & es_M
    ant_e = (P["origen"] == "Pendiente anterior") & es_E
    retro_m = P["origen"].isin(["Retroactivo", "Borrado", "Modificado"])
    por_mes = P["categoria"].isin(CATEGORIAS_POR_MES)

    # R0. Pendiente del banco de la conciliación anterior registrado después con fecha ya
    #     conciliada: se cierra contra ese asiento ANTES de buscar anulaciones dentro del mayor.
    ap.uno_a_uno(ant_e, retro_m & (P["origen"] == "Retroactivo"), TOL_CENTAVOS,
                 "Pendiente anterior registrado (retroactivo)", max_dias=40)
    # R0.b Gastos bancarios / préstamo de un mes (mes del comentario, 'GB 09-26') pendientes de la
    #      conciliación anterior contra lo registrado para atrás o en el período
    ap.compensacion_categoria_mes(por_mes & (ant_e | ant_m | retro_m | (es_M & per)), TOL_CRUCE,
                                  "Gastos bancarios / préstamo de un mes registrados (mes del comentario)")
    # R1. Mismo lado que se anula (asiento + borrado/reversión, pendiente anterior + borrado)
    ap.uno_a_uno(es_M & ~per, es_M, TOL_CENTAVOS, "Anulación en mayor")
    ap.anulacion_suma(ant_m | retro_m, TOL_CENTAVOS, "Anulación en mayor (suma)")
    # R2. Pendientes de banco anteriores ya registrados (retroactivo o en el período)
    ap.uno_a_uno(ant_e, retro_m, TOL_CRUCE, "Pendiente anterior registrado (retroactivo)")
    ap.uno_a_uno(ant_e, es_M & per, TOL_CRUCE, "Pendiente anterior registrado en el período")
    ap.combinacion(retro_m | (es_M & per), ant_e, TOL_CRUCE, "Pendientes anteriores registrados en un asiento", vinculo="categoria")
    ap.combinacion(ant_e, retro_m, TOL_CRUCE, "Pendiente anterior registrado en varios asientos", vinculo="ninguno")
    ap.combinacion(ant_e, es_M & per, TOL_CRUCE, "Pendiente anterior registrado en varios asientos del período", vinculo="categoria")
    ap.subconjunto_categoria(ant_e, retro_m, 0.05, "Pendiente anterior registrado en varios asientos",
                             max_dias=None, misma_categoria=False)
    ap.categoria_vs_sin_categoria(ant_e, retro_m, TOL_CENTAVOS, "Pendientes anteriores de una categoría registrados juntos")
    ap.categoria_vs_sin_categoria(ant_e, es_M & per, TOL_CENTAVOS,
                                  "Pendientes anteriores de una categoría registrados en el período")
    ap.por_categoria_mes(ant_e, retro_m, TOL_CRUCE,
                         "Pendientes anteriores de una categoría y mes registrados con fecha ya conciliada")
    # Pendiente anterior del mayor corregido para atrás que así queda igual al del banco
    ap.par_mayor_vs_banco(ant_m | retro_m, ant_e, retro_m, TOL_CENTAVOS,
                          "Pendiente anterior corregido con asiento para atrás contra banco")
    # Pendientes anteriores del mayor y del banco que se compensan entre sí
    ap.uno_a_uno(ant_m, ant_e, TOL_CENTAVOS, "Pendientes anteriores que se compensan entre sí")
    ap.combinacion(ant_m, ant_e, TOL_CENTAVOS, "Pendientes anteriores que se compensan entre sí (suma)", vinculo="ninguno")
    ap.combinacion(ant_e, ant_m, TOL_CENTAVOS, "Pendientes anteriores que se compensan entre sí (suma)", vinculo="ninguno")
    # R2.b El cambio de saldo inicial sin identificar coincide con pendientes anteriores
    #      registrados o anulados con fecha ya conciliada
    ap.explicar_cambio_saldo(P["origen"] == "Retroactivo no identificado", ant_e | ant_m,
                             "Cambio de saldo inicial explicado por pendientes anteriores registrados/anulados")
    # R3. Pendientes de mayor anteriores que impactaron en el banco este período
    ap.uno_a_uno(ant_m, es_E & per, TOL_CRUCE, "Pendiente anterior impactó en banco")
    ap.combinacion(ant_m, es_E & per, TOL_CRUCE, "Pendiente anterior impactó en banco (suma)", vinculo="categoria")
    # R3.b Pendientes anteriores contra un asiento (para atrás o del período) o un movimiento del
    #      banco del período por casi el mismo importe (diferencia menor al 0,1%, mismo día ± 3):
    #      se cruzan y la diferencia queda en una línea aparte.
    ap.uno_a_uno_con_diferencia(ant_e, retro_m | (es_M & per), "Pendiente anterior registrado con diferencia")
    ap.uno_a_uno_con_diferencia(ant_m, es_E & per, "Pendiente anterior impactó en banco con diferencia")
    # Mismo día: se acepta hasta 1% de diferencia (9dD: cobranza 155.500 registrada por 155.000)
    ap.uno_a_uno_con_diferencia(ant_e, retro_m | (es_M & per), "Pendiente anterior registrado con diferencia",
                                max_dias=0, tol_rel=0.01)
    ap.uno_a_uno_con_diferencia(ant_m, es_E & per, "Pendiente anterior impactó en banco con diferencia",
                                max_dias=0, tol_rel=0.01)
    # R4. Asientos retroactivos contra movimientos del banco del período
    ap.uno_a_uno(retro_m, es_E & per, TOL_RESIDUAL, "Asiento retroactivo contra banco del período",
                 max_dias=DIAS_RESIDUAL, exigir_vinculo=True)
    # R4.a Del período: un asiento contra varios movimientos del banco de la misma categoría
    no_agr = ~P["categoria"].isin(list(CATEGORIAS_AGRUPADAS))
    ap.subconjunto_categoria(es_M & per & no_agr, es_E & per & no_agr, 0.05,
                             "Asiento del período contra varios movimientos del banco")
    ap.subconjunto_categoria(es_E & per & no_agr, es_M & per & no_agr, 0.05,
                             "Movimiento del banco del período contra varios asientos")
    # R4.b Lo que se compensa por categoría y mes no va a la CC
    ap.compensacion_categoria_mes(ant_m | ant_e | retro_m, TOL_CRUCE,
                                  "Pendientes y asientos para atrás de una categoría y mes que se compensan")
    # R5. Gastos bancarios y préstamos de meses anteriores que se compensan en conjunto
    ap.bancarios_en_conjunto(ant_m, ant_e, TOL_CRUCE,
                             "Gastos bancarios y préstamos anteriores que se compensan en conjunto")

    # R6. Corte del mismo mes: lo que ya estaba conciliado al corte anterior y hoy no tiene par
    REGLA_PREVIO = "Ya conciliado en el corte anterior (no figuraba como pendiente)"
    for i in ap._pend(P["ya_conciliado_antes"].astype(bool)).index:
        ap._cerrar([i], REGLA_PREVIO)

    P = ap.p
    # Si el cambio de saldo sin identificar quedó explicado por pendientes anteriores registrados
    # o anulados, no hace falta bajar más mayor: se reemplaza el aviso
    ru_ok = P[(P["origen"] == "Retroactivo no identificado") & (P["estado"] == "conciliado")]
    if len(ru_ok) and control.get("aviso_mayor"):
        control["aviso_mayor"] = (
            f"El cambio de saldo de {ru_ok['importe'].sum():,.2f} que no se vio asiento por asiento se explica con "
            f"pendientes de la conciliación anterior que ya no están (ver hojas Pendientes anteriores y Cruces "
            f"pend. anteriores). No va a la CC.")
    previo = P[P["regla"] == REGLA_PREVIO]
    if len(previo):
        # Su par quedó en la conciliación anterior: el efecto es parte de lo heredado
        efecto = previo.loc[previo["lado"] == "M", "importe"].sum() - previo.loc[previo["lado"] == "E", "importe"].sum()
        control["dif_heredada"] = (control.get("dif_heredada") or 0.0) + efecto
    conc = P[(P["estado"] == "conciliado") & (P["regla"] != REGLA_PREVIO)]
    # Las diferencias que quedaron en una línea aparte (cruces con diferencia) ya están en la CC
    dif_lineas = P.loc[P["rid"].astype(str).str.match(r"^D\d+-\d+$"), "importe"].sum()
    control["dif_redondeo_apareo"] = conc.loc[conc["lado"] == "M", "importe"].sum() \
        - conc.loc[conc["lado"] == "E", "importe"].sum() + dif_lineas

    lineas_cc, detalle, dif_neteo = _armar_lineas_cc(P, hasta)
    control["dif_cruce"] = control["dif_cruce"] + dif_neteo
    total = saldo_mayor_final - lineas_cc.query("seccion=='ING_MAYOR'")["importe_hoja"].sum() \
        + lineas_cc.query("seccion=='SAL_MAYOR'")["importe_hoja"].sum() \
        + lineas_cc.query("seccion=='CRED_BANCO'")["importe_hoja"].sum() \
        - lineas_cc.query("seccion=='DEB_BANCO'")["importe_hoja"].sum()
    control["total_cc"] = total
    control["diferencia"] = total - saldo_ext_final

    estado_ant = _estado_pendientes_anteriores(pend_ant, P)
    cuenta_hoja = cuenta if cuenta and cuenta != "Cuenta" else ""
    buf = _exportar(lineas_cc, control, retro, estado_ant, bytes_cruce, P,
                    dict(razon_social=razon_social or "", banco=banco, cuenta=cuenta_hoja,
                         analista=analista, mes=desde.replace(day=1)),
                    # Los meses de la foto anterior que el mayor cargado no cubre se arrastran solo si
                    # el saldo ya conciliado no cambió sin explicación: si cambió, esos meses se tocaron
                    # y la foto vieja ya no los representa.
                    foto=_armar_foto_mayor(may_full[~es_saldo_ant], hasta, desde,
                                           foto_anterior=ant.get("foto_mayor") if ant and abs(
                                               control.get("retro_no_identificado") or 0) <= TOL_CENTAVOS else None))
    try:
        pendientes_xlsx = armar_pendientes_por_persona(P, ext_c, hasta, razon_social or "", cuenta_hoja)
    except Exception as ex:   # el libro de pendientes no debe impedir la conciliación
        pendientes_xlsx = None
        control["aviso_pendientes"] = f"No se pudo armar el libro de pendientes: {ex}"
    stats = {k: control.get(k) for k in ("modo", "saldo_mayor_final", "saldo_ext_final", "diferencia",
                                          "dif_heredada", "retro_identificado", "retro_no_identificado",
                                          "dif_cruce", "dif_redondeo_apareo", "aviso_mayor", "aviso_posterior",
                                          "aviso_cuenta")}
    stats = {k: (round(float(v), 2) if isinstance(v, (int, float, np.floating)) and v is not None else v)
             for k, v in stats.items()}
    stats.update(stats_cruce)
    stats["lineas_cc"] = len(lineas_cc)
    stats["pendientes_anteriores_registrados"] = int((estado_ant.get("estado", pd.Series(dtype=str)) == "Registrado").sum()) if len(estado_ant) else 0
    stats["pendientes_anteriores_vigentes"] = int((estado_ant.get("estado", pd.Series(dtype=str)) == "Vigente").sum()) if len(estado_ant) else 0
    stats["pendientes_xlsx"] = pendientes_xlsx
    return buf, stats


# ─────────────────────────────────────────────────────────────────────────────
# 6. LÍNEAS DE LA HOJA CC
# ─────────────────────────────────────────────────────────────────────────────

def _armar_lineas_cc(P, hasta):
    pend = P[P["estado"] == "pendiente"].copy()
    lineas, det, centavos_neteo = [], [], []
    sufijo = hasta.strftime("%m-%y")

    def agregar(seccion, fecha, concepto, importe_hoja, obs, filas, origen):
        n = len(lineas) + 1
        lineas.append({"linea": n, "seccion": seccion, "fecha": fecha, "concepto": concepto,
                       "importe_hoja": round(importe_hoja, 2), "obs": obs, "origen": origen})
        for _, f in filas.iterrows():
            det.append({**f.to_dict(), "linea_cc": n, "seccion_cc": seccion})

    # Partidas del período en categorías agrupadas: una línea neta por categoría. Gastos
    # bancarios y préstamo, además, por mes de referencia (un 'Pago GB 09-26' cargado en
    # octubre no se netea contra el GB de octubre: va como GB 09-26).
    agr = pend[(pend["origen"] == "Período") & pend["categoria"].isin(CATEGORIAS_AGRUPADAS)].copy()
    agr["_mes"] = np.where(agr["categoria"].isin(CATEGORIAS_POR_MES), agr["mes_ref"].fillna(sufijo).astype(str), sufijo)
    for (cat, mes), g in agr.groupby(["categoria", "_mes"]):
        etiqueta = CATEGORIAS_AGRUPADAS[cat]
        etiqueta = f"{etiqueta} {mes}" if etiqueta in ("GB", "Prestamo", "Plan Afip") else etiqueta
        sm, se = g.loc[g["lado"] == "M", "importe"].sum(), g.loc[g["lado"] == "E", "importe"].sum()
        efecto = se - sm  # efecto sobre el total de la hoja
        if abs(efecto) < TOL_LINEA_DIFERENCIA:
            centavos_neteo.append(efecto)   # menos de 1 peso: no genera línea
            continue
        if abs(se) >= abs(sm):
            sec = "CRED_BANCO" if efecto > 0 else "DEB_BANCO"
        else:
            sec = "SAL_MAYOR" if efecto > 0 else "ING_MAYOR"
        obs = ("Neto mayor vs extracto" if (sm != 0 and se != 0)
               else str(g["detalle"].iloc[0]) if len(g) == 1 else f"{len(g)} movimientos")
        if str(cat).startswith("Acred.") or cat in ("Propinas", COBRANZAS_SENAS):
            sec, obs = (("CRED_BANCO", f"{etiqueta} pendiente de registrar") if efecto > 0
                        else ("ING_MAYOR", f"{etiqueta} registrada de más en el mayor"))
        agregar(sec, None if sm == 0 else g["fecha"].max(), etiqueta, abs(efecto), obs, g, "Período")
    resto = pend.drop(agr.index)

    # Asientos cargados con fecha ya conciliada de categorías agrupadas: una línea por
    # categoría y mes, como lo presenta el equipo ("GB 07-26 cargado con fecha ya conciliada")
    retro_agr = resto[(resto["origen"] == "Retroactivo") & resto["categoria"].isin(CATEGORIAS_AGRUPADAS)]
    mes_ret = retro_agr["mes_ref"].where(retro_agr["mes_ref"].notna(), retro_agr["fecha"].dt.strftime("%m-%y"))
    usados = []
    for (cat, mes), g in retro_agr.groupby([retro_agr["categoria"], mes_ret]):
        # Si hay un pendiente anterior de la misma categoría y mes (p. ej. 'GB 07-26' del banco),
        # se netea contra lo cargado para atrás y queda solo la diferencia, como lo presenta el
        # equipo (9dD: GB 07-26 61.202,07 pendiente - 61.855,27 cargado = 653,20).
        pa = resto[(resto["origen"] == "Pendiente anterior") & (resto["categoria"] == cat)
                   & (resto["mes_ref"].astype(str) == str(mes)) & ~resto.index.isin(usados)]
        g2 = pd.concat([g, pa])
        sm = g2.loc[g2["lado"] == "M", "importe"].sum()
        se = g2.loc[g2["lado"] == "E", "importe"].sum()
        efecto = se - sm
        usados += list(pa.index)
        if abs(efecto) < TOL_LINEA_DIFERENCIA:
            centavos_neteo.append(efecto)
            continue
        if len(pa):
            sec = ("CRED_BANCO" if efecto > 0 else "DEB_BANCO") if abs(se) >= abs(sm) else \
                ("SAL_MAYOR" if efecto > 0 else "ING_MAYOR")
            etiqueta = f"{CATEGORIAS_AGRUPADAS[cat]} {mes}"
            obs = (f"Pendiente anterior {abs(se):,.2f} neto de lo cargado con fecha ya conciliada {abs(sm):,.2f} "
                   f"(asientos " + ", ".join(g["referencia"].astype(str).str.replace("Asiento ", "")) + ")")
        else:
            sec = _seccion("M", sm)
            etiqueta = f"{CATEGORIAS_AGRUPADAS[cat]} {mes} cargado con fecha ya conciliada"
            obs = f"{len(g)} asientos: " + ", ".join(g["referencia"].astype(str).str.replace("Asiento ", ""))
        agregar(sec, g["fecha"].max(), etiqueta, abs(efecto), obs, g2, "Retroactivo")
    resto = resto.drop(list(retro_agr.index) + usados)

    for _, f in resto.sort_values(["origen", "fecha"]).iterrows():
        sec = _seccion(f["lado"], f["importe"])
        if f["origen"] == "Período":
            concepto = f["concepto"] if f["concepto"] not in ("0", "nan") else "Sin categoría"
            obs = f["nombre"] if f["nombre"] not in ("nan", "", "None") else f["detalle"]
        elif f["origen"] == "Pendiente anterior":
            concepto, obs = f["concepto"], (f["detalle"] if f["detalle"] not in ("nan", "None") else "")
        elif f["origen"] == "Borrado":
            # Un asiento borrado deja el movimiento del banco que conciliaba sin registrar
            sec = "CRED_BANCO" if f["importe"] < 0 else "DEB_BANCO"
            ref = str(f["referencia"]).replace("Asiento ", "")
            tipo = "Crédito" if sec == "CRED_BANCO" else "Débito"
            concepto = f"{tipo} del banco sin registrar (asiento {ref} borrado)"
            obs = f["nombre"] if f["nombre"] not in ("nan", "", "None") else f["detalle"]
        else:
            concepto = f"{f['concepto']} ({f['referencia']})" if f["referencia"] else f["concepto"]
            obs = f["nombre"] if f["nombre"] not in ("nan", "", "None") else f["detalle"]
            if f["origen"] == "Retroactivo":
                # Cargado con fecha ya conciliada y sin pendiente anterior ni movimiento del banco
                # que lo explique: lo más probable es que duplique algo ya registrado
                # (9dD: asiento 147777, 26.000, sin tercero, dentro del cobro de GABES de 94.000).
                tipo = "cobro" if f["importe"] > 0 else "pago"
                nom = str(f["nombre"]).strip()
                quien = f"{nom}. " if nom not in ("nan", "", "None", "NaN") else "Sin tercero. "
                obs = (f"{quien}Cargado después de la conciliación anterior con fecha ya conciliada y no "
                       f"corresponde a ningún pendiente anterior: posible duplicado de un {tipo} ya "
                       f"registrado. Verificar qué movimiento del banco lo respalda.")
        agregar(sec, f["fecha"], concepto, abs(f["importe"]), obs, pd.DataFrame([f]), f["origen"])

    lineas = pd.DataFrame(lineas, columns=["linea", "seccion", "fecha", "concepto", "importe_hoja", "obs", "origen"])
    orden = {"Pendiente anterior": 0, "Retroactivo no identificado": 1, "Retroactivo": 1, "Borrado": 1,
             "Modificado": 1, "Período": 2, "Diferencia de apareo": 2}
    lineas["_o"] = lineas["origen"].map(orden)
    lineas = lineas.sort_values(["seccion", "_o", "fecha"], na_position="last").drop(columns="_o")
    return lineas, pd.DataFrame(det), -sum(centavos_neteo)


def _estado_pendientes_anteriores(pend_ant, P):
    if pend_ant is None or pend_ant.empty:
        return pd.DataFrame()
    pa = P[P["origen"] == "Pendiente anterior"]
    out = []
    for _, f in pa.iterrows():
        base = f[["fecha", "concepto", "detalle", "importe"]].to_dict()
        if f["estado"] == "conciliado":
            grupo = P[(P["grupo"] == f["grupo"]) & (P["rid"] != f["rid"])]
            contra = grupo[grupo["origen"] != "Pendiente anterior"]
            partes = [f"{r['origen']} {r['referencia']} {r['fecha']:%d/%m/%Y} {r['importe']:,.2f}".strip()
                      if pd.notna(r["fecha"]) else f"{r['origen']} {r['importe']:,.2f}" for _, r in contra.iterrows()]
            if len(grupo) > len(contra):
                partes.append(f"junto con {len(grupo) - len(contra)} pendiente(s) anterior(es)")
            out.append({**base, "estado": "Registrado", "regla": f["regla"], "contrapartida": "; ".join(partes)})
        else:
            out.append({**base, "estado": "Vigente", "regla": "", "contrapartida": ""})
    return pd.DataFrame(out)

# ─────────────────────────────────────────────────────────────────────────────
# 7. PENDIENTES POR PERSONA (libro aparte)
# ─────────────────────────────────────────────────────────────────────────────

ORDEN_PERSONAS = ["MERI", "JOACO", "FIO", "MARU", "LUCAS"]


def _persona_de_categoria(cat):
    c = _norm(cat)
    if not c or c in ("0", "nan") or "gastosbancarios" in c or c.startswith("gb"):
        return None
    if c.startswith("acred") or "propina" in c:
        return "MERI"
    if "echeq" in c:
        return "JOACO"
    if "cobranza" in c or "sena" in c:
        return "MARU"
    if "prestamo" in c or "planafip" in c or "descuento" in c:
        return "LUCAS"
    return "FIO"


def armar_pendientes_por_persona(P, ext_c, hasta, razon_social="", cuenta=""):
    """Movimientos del banco del período que todavía no están en el mayor, repartidos por
    persona según la categoría (sin gastos bancarios ni pendientes anteriores)."""
    pend = P[(P["estado"] == "pendiente") & (P["lado"] == "E") & (P["origen"] == "Período")].copy()
    es_afip = pend["categoria"].astype(str).map(_norm).isin(["impafip", "planafip"])
    deb_aut = pend["detalle"].astype(str).map(_norm).str.contains("debitoautom")
    pend.loc[es_afip & deb_aut, "categoria"] = "Plan Afip"
    pend["persona"] = pend["categoria"].map(_persona_de_categoria)
    pend = pend[pend["persona"].notna()]
    ext = ext_c.set_index(ext_c["_rid"].astype(str))

    wb = Workbook()
    ws = wb.active
    ws.title = "Pendientes"
    neg = Font(name="Arial", size=10, bold=True)
    normal = Font(name="Arial", size=9)
    titulo_fill = PatternFill("solid", fgColor="D99694")
    head_fill = PatternFill("solid", fgColor="F2DCDB")
    fino = Side(style="thin", color="999999")
    caja = Border(left=fino, right=fino, top=fino, bottom=fino)
    num = '#,##0.00;-#,##0.00;"-"'
    for col, w in zip("ABCDEFGHIJ", [3, 12, 60, 22, 15, 15, 30, 10, 28, 24]):
        ws.column_dimensions[col].width = w
    r = 2
    ws.cell(r, 2, f"{razon_social}  -  Pendientes del banco por registrar al {hasta:%d/%m/%Y}".strip(" -")).font = \
        Font(name="Arial", size=12, bold=True)
    r += 1
    ws.cell(r, 2, "Movimientos del banco que todavía no están en el mayor. Pintar de verde lo que ya se registró "
                  "y dejar al lado cualquier observación.").font = Font(name="Arial", size=9, italic=True)
    r += 2
    if pend.empty:
        ws.cell(r, 2, "No hay movimientos del banco pendientes de registrar (fuera de gastos bancarios).").font = normal
    for persona in ORDEN_PERSONAS:
        g = pend[pend["persona"] == persona]
        if g.empty:
            continue
        ws.merge_cells(start_row=r, start_column=3, end_row=r, end_column=4)
        c = ws.cell(r, 3, f"PENDIENTES PARA {persona}")
        c.font, c.fill, c.alignment = neg, titulo_fill, Alignment(horizontal="center")
        r += 1
        if cuenta:
            ws.cell(r, 2, cuenta).font = neg
            r += 1
        ws.cell(r, 2, "BANCO").font = neg
        r += 1
        cab = ["Fecha", "Descripción", "Conciliación", "Débito en $", "Crédito en $"] + \
              (["Tercero", "Sucursal", "Referencia", "Observaciones"] if persona != "MERI" else [])
        for j, t in enumerate(cab, start=2):
            c = ws.cell(r, j, t)
            c.font, c.fill, c.border = Font(name="Arial", size=9, bold=True), head_fill, caja
        r += 1
        if persona == "MERI":
            filas = [(hasta, "", cat, x.sum(), None) for cat, x in g.groupby("categoria")["importe"]]
        else:
            filas = []
            for _, f in g.sort_values(["categoria", "fecha"]).iterrows():
                e = ext.loc[str(f["rid"])] if str(f["rid"]) in ext.index else None
                if isinstance(e, pd.DataFrame):
                    e = e.iloc[0]
                filas.append((f["fecha"], e["descripcion"] if e is not None else f["detalle"], f["categoria"], f["importe"], e))
        for fecha, desc, cat, imp, e in filas:
            vals = [fecha.to_pydatetime() if isinstance(fecha, pd.Timestamp) else fecha,
                    "" if str(desc) in ("nan", "None") else desc, cat, -imp if imp < 0 else 0, imp if imp > 0 else 0]
            if persona != "MERI":
                vals += ["" if e is None or str(e.get(k)) in ("nan", "None") else e.get(k)
                         for k in ("tercero", "sucursal", "referencia", "observaciones")]
            for j, v in enumerate(vals, start=2):
                c = ws.cell(r, j, v)
                c.font, c.border = normal, caja
                if j == 2:
                    c.number_format = "dd/mm/yyyy"
                elif j in (5, 6):
                    c.number_format = num
            r += 1
        r += 2
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# 8. EXPORTACIÓN (mismo formato que Galicia)
# ─────────────────────────────────────────────────────────────────────────────

FMT = "#,##0.00"
NEGRITA = Font(bold=True)
GRIS = PatternFill("solid", fgColor="D9D9D9")
ROJO = PatternFill("solid", fgColor="F8CBAD")

CC_ROSA = PatternFill("solid", fgColor="D99694")
CC_ROSA_MEDIO = PatternFill("solid", fgColor="E6B9B8")
CC_ROSA_CLARO = PatternFill("solid", fgColor="F2DCDB")
CC_LILA = PatternFill("solid", fgColor="E6E0EC")
CC_AMARILLO = PatternFill("solid", fgColor="FFFF00")
CC_BLANCO = PatternFill("solid", fgColor="FFFFFF")
CC_NUM = '_(* #,##0.00_);_(* \\(#,##0.00\\);_(* "-"??_);_(@_)'
CC_F8 = Font(name="Arial", size=8)
CC_F8B = Font(name="Arial", size=8, bold=True)
CC_F8BU = Font(name="Arial", size=8, bold=True, underline="single")
CC_F8IU = Font(name="Arial", size=8, italic=True, underline="single")
CC_F10B = Font(name="Arial", size=10, bold=True)
CC_F10BU = Font(name="Arial", size=10, bold=True, underline="single")
CC_F11B = Font(name="Arial", size=11, bold=True)
_MED = Side(style="medium")
_THIN = Side(style="thin")
_HAIR = Side(style="hair")


def _cc_borde(c, left=None, right=None, top=None, bottom=None):
    b = c.border
    c.border = Border(left=left if left is not None else b.left, right=right if right is not None else b.right,
                      top=top if top is not None else b.top, bottom=bottom if bottom is not None else b.bottom)


def _cc_marco(ws, r1, c1, r2, c2, lado=_MED):
    for r in range(r1, r2 + 1):
        _cc_borde(ws.cell(r, c1), left=lado)
        _cc_borde(ws.cell(r, c2), right=lado)
    for c in range(c1, c2 + 1):
        _cc_borde(ws.cell(r1, c), top=lado)
        _cc_borde(ws.cell(r2, c), bottom=lado)


def _cc_caja(ws, celda, valor, fill=None, font=CC_F8B, fmt=None, lado=_MED):
    c = ws[celda]
    c.value = valor
    c.font = font
    if fill is not None:
        c.fill = fill
    if fmt:
        c.number_format = fmt
    c.border = Border(left=lado, right=lado, top=lado, bottom=lado)
    return c


def _hoja_cc(ws, lineas, control, meta):
    for col, w in {"A": 3.9, "B": 19.6, "C": 20.9, "D": 34, "E": 18.7, "F": 34.8,
                   "G": 13.2, "H": 7.6, "I": 15.4, "J": 6, "K": 14.6}.items():
        ws.column_dimensions[col].width = w
    hasta = pd.Timestamp(control["hasta"])

    ws.merge_cells("B2:C2")
    c = ws["B2"]; c.value = f"{meta['razon_social']}  - Conciliacion Bancaria"
    c.font = CC_F10BU; c.fill = CC_ROSA; c.alignment = Alignment(horizontal="left")
    mes = meta["mes"]
    if isinstance(mes, pd.Timestamp):
        mes = mes.replace(day=1)
    datos = [("Razón social:", meta["razon_social"], None), ("País:", "ARGENTINA", None),
             ("Mes:", mes, "mmm-yy"), ("Banco:", meta["banco"], None),
             ("Cuenta Contable N°:", "", None),
             ("Fecha de Analisis:", pd.Timestamp(date.today()), "dd/mm/yy"),
             ("Analista :", meta["analista"], None)]
    for i, (k, v, fmt) in enumerate(datos, start=4):
        a = ws.cell(i, 2, k); a.font = CC_F10B; a.fill = CC_BLANCO
        b = ws.cell(i, 3, v); b.font = CC_F10B; b.fill = CC_BLANCO
        b.alignment = Alignment(horizontal="left")
        if fmt:
            b.number_format = fmt
        if i < 10:
            _cc_borde(b, bottom=_HAIR)
    ws.cell(3, 2).fill = CC_BLANCO; ws.cell(3, 3).fill = CC_BLANCO
    _cc_marco(ws, 2, 2, 10, 3)
    if meta.get("cuenta"):
        _cc_caja(ws, "I12", meta["cuenta"], CC_ROSA_MEDIO, CC_F11B)

    ws["J2"] = "Referencia:"; ws["J2"].font = CC_F8B
    for i, (fill, txt) in enumerate([(CC_AMARILLO, "PENDIENTE DE CONCILIACIÓN ANTERIOR"),
                                     (CC_ROSA_MEDIO, "CARGADO / MODIFICADO / BORRADO CON FECHA YA CONCILIADA")], start=3):
        _cc_caja(ws, f"J{i}", None, fill)
        ws.cell(i, 11, txt).font = CC_F8B
    _cc_caja(ws, "J5", None, None)
    ws.cell(5, 11, "SIN COLOR: PENDIENTE DEL PERÍODO").font = CC_F8B

    caja_ini, r = 13, 14
    ws.merge_cells(start_row=r, start_column=2, end_row=r, end_column=4)
    c = ws.cell(r, 2, "Saldo Contable al:"); c.font = CC_F10B; c.fill = CC_ROSA
    c.alignment = Alignment(horizontal="center")
    _cc_marco(ws, r, 2, r, 4)
    c = ws.cell(r, 5, hasta); c.font = CC_F8B; c.number_format = '[$-C0A]d" de "mmmm" de "yyyy;@'
    _cc_caja(ws, f"I{r}", round(control["saldo_mayor_final"], 2), CC_ROSA_CLARO, CC_F8B, CC_NUM)
    fila_saldo, refs = r, {}
    r += 2
    for sec in ["ING_MAYOR", "SAL_MAYOR", "CRED_BANCO", "DEB_BANCO"]:
        ws.cell(r, 3, SECCIONES[sec]).font = CC_F8B
        r += 2
        for j, h in enumerate(["Fecha", "Concepto", "Importe", "Observación"], start=3):
            c = ws.cell(r, j, h); c.font = CC_F8IU; c.alignment = Alignment(horizontal="center")
        r += 1
        ini = r
        for _, l in lineas[lineas["seccion"] == sec].iterrows():
            f = l["fecha"] if pd.notna(l["fecha"]) else None
            c = ws.cell(r, 3, f); c.font = CC_F8; c.alignment = Alignment(horizontal="center")
            if f is not None:
                c.number_format = "d-mmm" if pd.Timestamp(f).year == hasta.year else "d-mmm-yy"
            ws.cell(r, 4, l["concepto"]).font = CC_F8
            c = ws.cell(r, 5, l["importe_hoja"]); c.font = CC_F8; c.number_format = CC_NUM
            obs = l["obs"] if isinstance(l["obs"], str) and l["obs"] not in ("nan", "None") else ""
            ws.cell(r, 6, obs).font = CC_F8
            fill = CC_ROSA_MEDIO if l["origen"] in ("Retroactivo", "Borrado", "Modificado", "Retroactivo no identificado") \
                else CC_AMARILLO if l["origen"] == "Pendiente anterior" else None
            if fill is not None:
                for j in range(3, 6):
                    ws.cell(r, j).fill = fill
            r += 1
        fin = max(r - 1, ini)
        r += 1
        c = ws.cell(r, 5, f"=SUM(E{ini}:E{fin})"); c.number_format = CC_NUM; c.font = CC_F8BU
        r += 1
        for j in range(3, 9):
            _cc_borde(ws.cell(r, j), bottom=_HAIR)
        _cc_caja(ws, f"I{r}", f"=+E{r-1}", None, CC_F8, CC_NUM, lado=_THIN)
        refs[sec] = f"I{r}"
        r += 2

    ws.cell(r, 3, "Total ").font = CC_F8B
    _cc_caja(ws, f"I{r}", f"=+I{fila_saldo}-{refs['ING_MAYOR']}+{refs['SAL_MAYOR']}+{refs['CRED_BANCO']}-{refs['DEB_BANCO']}",
             None, CC_F8B, CC_NUM)
    fila_total = r
    r += 2
    ws.merge_cells(start_row=r, start_column=2, end_row=r, end_column=4)
    c = ws.cell(r, 2, "Saldo Extracto bancario:"); c.font = CC_F10B; c.fill = CC_ROSA
    c.alignment = Alignment(horizontal="center")
    _cc_marco(ws, r, 2, r, 4)
    c = ws.cell(r, 5, f"=+E{fila_saldo}"); c.font = CC_F8B; c.number_format = '[$-C0A]d" de "mmmm" de "yyyy;@'
    _cc_caja(ws, f"I{r}", round(control["saldo_ext_final"], 2), CC_ROSA_CLARO, CC_F8B, CC_NUM)
    fila_ext = r
    r += 2
    ws.merge_cells(start_row=r, start_column=6, end_row=r, end_column=7)
    c = ws.cell(r, 6, "DIFERENCIA:"); c.font = CC_F8B; c.fill = CC_ROSA
    c.alignment = Alignment(horizontal="center")
    _cc_marco(ws, r, 6, r, 7)
    _cc_caja(ws, f"I{r}", f"=+I{fila_total}-I{fila_ext}", CC_LILA, CC_F8B, CC_NUM)
    r += 1
    _cc_marco(ws, caja_ini, 2, r, 9)

    r += 2
    ws.cell(r + 1, 2, "Confeccionado por:").font = CC_F8
    ws.cell(r + 1, 6, "Revisado por:").font = CC_F8
    ws.cell(r + 3, 2, "Fecha:").font = CC_F8
    ws.cell(r + 3, 6, "Fecha:").font = CC_F8
    _cc_marco(ws, r, 2, r + 4, 5)
    _cc_marco(ws, r, 6, r + 4, 9)
    r += 7

    ws.cell(r, 6, "Explicación de la diferencia").font = CC_F8B
    expl = [("Diferencia heredada de la conciliación anterior", control.get("dif_heredada", 0.0) or 0.0),
            ("Diferencias de centavos en cruces del período", control.get("dif_cruce", 0.0) or 0.0),
            ("Diferencias de centavos en apareos (pendientes/residual)", control.get("dif_redondeo_apareo", 0.0) or 0.0)]
    r0, ini = r, r + 1
    for k, v in expl:
        r += 1
        ws.cell(r, 6, k).font = CC_F8
        c = ws.cell(r, 9, round(v, 2)); c.number_format = CC_NUM; c.font = CC_F8
    r += 1
    ws.cell(r, 6, "Sin explicar (debe ser 0)").font = CC_F8B
    _cc_caja(ws, f"I{r}", f"=ROUND(I{fila_total}-I{fila_ext}-SUM(I{ini}:I{r-1}),2)", CC_LILA, CC_F8B, CC_NUM)
    _cc_marco(ws, r0, 6, r, 9)
    for fila in ws.iter_rows(min_row=1, max_row=r, min_col=1, max_col=11):
        for c in fila:
            if c.font is None or c.font.name != "Arial":
                c.font = CC_F8


def _df_a_hoja(ws, df, anchos=None):
    df = df.reset_index(drop=True).copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.Series([None if pd.isna(v) else v.to_pydatetime() for v in df[col]], index=df.index, dtype=object)
    ws.append(list(df.columns))
    for c in ws[1]:
        c.font = NEGRITA; c.fill = GRIS
    for fila in df.itertuples(index=False):
        ws.append([None if (isinstance(v, float) and np.isnan(v)) or v is pd.NaT else v for v in fila])
    for j, col in enumerate(df.columns, start=1):
        ws.column_dimensions[get_column_letter(j)].width = (anchos or {}).get(col, max(10, min(45, len(str(col)) + 4)))
        if str(col).lower() in ("importe", "importe_hoja"):
            for i in range(2, ws.max_row + 1):
                ws.cell(i, j).number_format = FMT
        if str(col).lower().startswith("fecha"):
            for i in range(2, ws.max_row + 1):
                ws.cell(i, j).number_format = "dd/mm/yyyy"
    ws.freeze_panes = "A2"


def _hoja_control(ws, control, retro):
    ws.column_dimensions["A"].width = 78
    ws.column_dimensions["B"].width = 20
    tiene_ant = control.get("tiene_anterior")
    avisos = [control.get(k) for k in ("aviso_sociedad", "aviso_mayor", "aviso_posterior", "aviso_cuenta") if control.get(k)]
    filas = [
        ("0. SOCIEDAD Y CUENTA", None),
        ("Cuenta conciliada (bloque de la conciliación anterior)", control.get("cuenta")),
        ("Cómo se eligió la cuenta", control.get("criterio_cuenta")),
        ("Otras cuentas en la conciliación anterior (no incluidas en esta corrida)", control.get("otras_cuentas") or "-"),
        ("Aviso", " / ".join(avisos) or "-"),
        ("", None),
        ("1. ¿SE TOCÓ EL MAYOR DE PERÍODOS YA CONCILIADOS?", None),
        ("Saldo del mayor al inicio del período, según la conciliación anterior", control.get("saldo_ref")),
        ("Saldo del mayor al inicio del período, según el mayor que se cargó hoy", control["saldo_mayor_inicio"]),
        ("Diferencia = asientos cargados, borrados o modificados con fecha ya conciliada",
         (control["saldo_mayor_inicio"] - control["saldo_ref"]) if tiene_ant and control.get("saldo_ref") is not None else None),
        ("   identificada asiento por asiento (detalle más abajo)", control.get("retro_identificado")),
        ("   sin identificar (se busca qué pendientes anteriores se registraron; lo que no se explica va a la CC)",
         control.get("retro_no_identificado")),
        ("", None),
        ("2. ¿CIERRA LA CONCILIACIÓN?", None),
        ("Saldo contable al cierre (mayor)", control["saldo_mayor_final"]),
        ("Saldo del extracto al cierre", control["saldo_ext_final"]),
        ("Total de la hoja CC (saldo contable ± pendientes)", control["total_cc"]),
        ("DIFERENCIA (Total CC - saldo extracto)", control["diferencia"]),
        ("   heredada: ya estaba en la conciliación anterior", control.get("dif_heredada")),
        ("   centavos en el cruce principal y neteos menores a 1 peso", control.get("dif_cruce")),
        ("   centavos al cruzar pendientes anteriores (hoja Cruces pend. anteriores)", control.get("dif_redondeo_apareo")),
        ("Saldo del extracto al inicio del período, según la conciliación anterior", control.get("saldo_ext_ref")),
        ("Saldo del extracto al inicio del período, según el extracto que se cargó hoy", control["saldo_ext_inicio"]),
        ("Extracto consistente (saldo inicial + movimientos = saldo final)", "Sí" if control["extracto_consistente"] else "NO"),
    ]
    for i, (k, v) in enumerate(filas, start=1):
        a = ws.cell(i, 1, k)
        a.alignment = Alignment(wrap_text=True)
        if v is None and k and k[0].isdigit():
            a.font = NEGRITA
        b = ws.cell(i, 2, round(v, 2) if isinstance(v, float) else v)
        if isinstance(v, (int, float)):
            b.number_format = FMT
    if avisos:
        ws.cell(5, 2).fill = ROJO
        ws.cell(5, 2).alignment = Alignment(wrap_text=True)
        ws.column_dimensions["B"].width = 60
    r = len(filas) + 3
    if retro is not None:
        partes = []
        for nombre, df in (("Cargado con fecha ya conciliada", retro["retro"]), ("Borrado", retro["borrados"])):
            if df is not None and len(df):
                d = df[[c for c in ["Asiento", "Apunte", "Fecha", "Comentario", "Serie", "Tercero", "Importe"] if c in df.columns]].copy()
                d.insert(0, "Tipo", nombre)
                partes.append(d)
        md = retro.get("modificados")
        if md is not None and len(md) and "_efecto" in md:
            partes.append(pd.DataFrame({"Tipo": "Modificado", "Asiento": md["Asiento"], "Apunte": md["Apunte"],
                                        "Fecha": md["Fecha_ant"], "Comentario": md.get("Comentario_act"),
                                        "Serie": md.get("Serie_act"), "Tercero": md.get("Tercero_act"),
                                        "Importe": md["_efecto"]}))
        ws.cell(r, 1, "3. DETALLE DE ASIENTOS CARGADOS O BORRADOS CON FECHA YA CONCILIADA").font = NEGRITA
        r += 1
        if partes:
            det = pd.concat(partes, ignore_index=True).sort_values(["Fecha", "Asiento"])
            det.insert(1, "Mes tocado", det["Fecha"].dt.strftime("%m-%Y"))
            for j, col in enumerate(det.columns, start=1):
                c = ws.cell(r, j, col); c.font = NEGRITA; c.fill = GRIS
            for fila in det.itertuples(index=False):
                r += 1
                for j, v in enumerate(fila, start=1):
                    if isinstance(v, pd.Timestamp):
                        v = v.to_pydatetime()
                    if isinstance(v, float) and np.isnan(v):
                        v = None
                    c = ws.cell(r, j, v)
                    if isinstance(v, float):
                        c.number_format = FMT
        else:
            ws.cell(r, 1, "No se detectaron (o la conciliación anterior no tiene foto del mayor para comparar).")


def _exportar(lineas, control, retro, estado_ant, bytes_cruce, P, meta, foto=None):
    from openpyxl import load_workbook
    wb = load_workbook(BytesIO(bytes_cruce))
    _hoja_cc(wb.create_sheet("CC", 0), lineas, control, meta)
    _hoja_control(wb.create_sheet("Control", 1), control, retro)
    pos = 2
    if len(estado_ant):
        _df_a_hoja(wb.create_sheet("Pendientes anteriores", pos), estado_ant,
                   {"concepto": 22, "detalle": 40, "contrapartida": 70, "regla": 40})
        pos += 1
    adic = P[P["estado"] == "conciliado"].sort_values(["grupo", "lado"])
    if len(adic):
        adic = adic.rename(columns={"regla": "regla aplicada", "lado": "lado (M=mayor, E=extracto)"})
        _df_a_hoja(wb.create_sheet("Cruces pend. anteriores", pos),
                   adic[["grupo", "regla aplicada", "origen", "lado (M=mayor, E=extracto)", "fecha", "categoria",
                         "nombre", "referencia", "importe"]],
                   {"regla aplicada": 45, "nombre": 36, "referencia": 30, "lado (M=mayor, E=extracto)": 12})
    if foto is not None:
        varias = bool(control.get("otras_cuentas"))
        wf = wb.create_sheet(_nombre_hoja_foto(control.get("cuenta")) if varias else HOJA_FOTO)
        wf.append(COLS_FOTO)
        for r in foto.itertuples(index=False):
            wf.append([None if (isinstance(v, float) and pd.isna(v)) else
                       (v.to_pydatetime() if isinstance(v, pd.Timestamp) else v) for v in r])
        for fila in wf.iter_rows(min_row=2, max_col=len(COLS_FOTO)):
            fila[2].number_format = "dd/mm/yyyy"
            fila[3].number_format = "#,##0.00"
        for i, (k, v) in enumerate((("Cubre desde", foto.attrs.get("cubre_desde")),
                                    ("Cubre hasta", foto.attrs.get("cubre_hasta"))), start=1):
            wf.cell(i, 10, k)
            c = wf.cell(i, 11, pd.Timestamp(v).to_pydatetime() if v is not None and pd.notna(v) else None)
            c.number_format = "dd/mm/yyyy"
        wf.sheet_state = "hidden"
    wb.active = 0
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# ENTRADA PARA LA APP
# ─────────────────────────────────────────────────────────────────────────────

def correr_conciliacion_hipotecario(archivo_mayor, archivo_extracto, archivo_conciliacion_anterior=None,
                                    analista="", razon_social=None, banco=BANCO, cuenta=None,
                                    archivos_conciliaciones_previas=None):
    """Cruce + conciliación completa. Devuelve (bytes del Excel, stats); en
    stats["pendientes_xlsx"] van los bytes del libro de pendientes por persona."""
    return correr_conciliacion_completa(archivo_mayor, archivo_extracto,
                                        archivo_conciliacion_anterior=archivo_conciliacion_anterior,
                                        analista=analista, razon_social=razon_social, banco=banco, cuenta=cuenta,
                                        archivos_conciliaciones_previas=archivos_conciliaciones_previas)
