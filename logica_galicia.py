"""
Lógica de conciliación bancaria - Banco Galicia (v2)
"""
import re
from io import BytesIO
from datetime import timedelta

import pandas as pd
import numpy as np


# ─────────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────────

def load_excel_file(file) -> pd.DataFrame:
    return pd.read_excel(file, dtype=str)


# ─────────────────────────────────────────────
# NORMALIZACIÓN
# ─────────────────────────────────────────────

def normalize_mayor(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"[^\x20-\x7EáéíóúÁÉÍÓÚñÑüÜ]", "", regex=True)
        .str.title()
    )
    expected = {"Debe", "Haber", "Saldo", "Fecha"}
    missing  = expected - set(df.columns)
    if missing:
        raise ValueError(f"Columnas faltantes en el mayor: {missing}. Disponibles: {list(df.columns)}")

    for col in ["Debe", "Haber", "Saldo"]:
        df[col] = (
            df[col].astype(str).str.strip()
            .str.replace(r"\s+", "", regex=True)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce").fillna(0.0)
        )

    def calcular_importe(row):
        debe, haber = row["Debe"], row["Haber"]
        if debe != 0 and haber == 0:   return debe
        elif haber != 0 and debe == 0: return -haber
        else:                          return 0.0

    df["Importe"] = df.apply(calcular_importe, axis=1)
    df["Fecha"]   = pd.to_datetime(df["Fecha"].astype(str).str.strip(), format="%Y-%m-%d %H:%M:%S", errors="coerce")
    return df


def normalize_extracto_galicia(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"[^\x20-\x7EáéíóúÁÉÍÓÚñÑüÜ]", "", regex=True)
        .str.lower()
        .str.replace("á","a").str.replace("é","e").str.replace("í","i")
        .str.replace("ó","o").str.replace("ú","u").str.replace("ñ","n")
    )
    expected = {"debitos", "creditos", "saldo", "fecha"}
    missing  = expected - set(df.columns)
    if missing:
        raise ValueError(f"Columnas faltantes en extracto: {missing}. Disponibles: {list(df.columns)}")

    for col in ["debitos", "creditos", "saldo"]:
        df[col] = (
            df[col].astype(str).str.strip()
            .str.replace(r"\s+", "", regex=True)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce").fillna(0.0)
        )

    def calcular_importe(row):
        c, d = row["creditos"], row["debitos"]
        if c != 0 and d == 0:   return c
        elif d != 0 and c == 0: return -d
        else:                   return 0.0

    df["importe"] = df.apply(calcular_importe, axis=1)
    df["fecha"]   = pd.to_datetime(df["fecha"].astype(str).str.strip(), format="%Y-%m-%d %H:%M:%S", errors="coerce")
    return df


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _limpiar(texto: str) -> str:
    if not isinstance(texto, str): return ""
    texto = texto.lower()
    texto = re.sub(r"[.\-\s]", "", texto)
    texto = re.sub(r"[^\x20-\x7EáéíóúñüÁÉÍÓÚÑÜ]", "", texto)
    return texto

def _contiene(texto_limpio: str, *palabras: str) -> bool:
    return any(_limpiar(p) in texto_limpio for p in palabras)

def _get_col(df, *candidates):
    for c in candidates:
        if c in df.columns: return c
    raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

def _find_col(df, keyword):
    for c in df.columns:
        if keyword.lower() in c.lower(): return c
    raise KeyError(f"Columna con '{keyword}' no encontrada en {list(df.columns)}")


# ─────────────────────────────────────────────
# CATEGORIZACIÓN
# ─────────────────────────────────────────────

def categorizar_extracto_v1(df: pd.DataFrame) -> pd.DataFrame:
    df   = df.copy()
    desc = df["descripcion"].apply(_limpiar)

    condiciones = [
        desc.apply(lambda d: _contiene(d, "debitocontracargoventa","debitodevolucionventa",
                   "devolucionpagocontransferencia","naveventacontarjeta","navepagocontransferencia")),
        desc.apply(lambda d: _contiene(d, "serviciopagoaProveedores","serviciopagoproveedores")),
        desc.apply(lambda d: _contiene(d,"ajusteaportespromocionGalicia") or
                   ("ajuste" in d and ("aporte" in d or "aportes" in d) and "promocion" in d)),
        desc.apply(lambda d: "echeq" in d),
        desc.apply(lambda d: _contiene(d, "rescatefima","suscripcionfima")),
        desc.apply(lambda d: _contiene(d,"iva","comgestiontransffdosentrebcos","comisionserviciodecuenta",
                   "impcreley25413","impdebley25413","impingbrutos","impuestodesellos",
                   "interesessobresaldosdeudores","perceiva") or
                   ("imp" in d and ("iva" in d or "impuesto" in d or "impuestos" in d or "percep" in d))),
        desc.apply(lambda d: _contiene(d,"cobroiibb","cuotadeprestamo","debitoiibb") or
                   ("prestamo" in d or "iibb" in d)),
        desc.apply(lambda d: _contiene(d,"pagodeservicios","trfinmedproveed")),
        desc.apply(lambda d: _contiene(d,"transfinmedcp","transfctaspropias","transfercashmismatitularidad")),
        desc.apply(lambda d: _contiene(d,"pagovisaempresa")),
        desc.apply(lambda d: _contiene(d,"transfafip")),
        desc.apply(lambda d: _contiene(d,"propina")),
    ]
    categorias = ["Acreditaciones","Cobranzas","Descuento Galicia","Echeq","FCI",
                  "Gastos Bancarios","Prestamo","Proveedores","Transf. entre cuentas",
                  "Pago Tc. Corpo","Imp. AFIP","Propinas"]

    df["conciliacion"] = "0"
    for cond, cat in zip(condiciones, categorias):
        df.loc[cond & (df["conciliacion"] == "0"), "conciliacion"] = cat
    return df


def categorizar_extracto_v2(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def limpiar_con_espacios(texto: str) -> str:
        if not isinstance(texto, str): return ""
        texto = texto.lower()
        texto = re.sub(r"[.\-]", "", texto)
        texto = re.sub(r"\s+", " ", texto)
        texto = re.sub(r"[^\x20-\x7EáéíóúñüÁÉÍÓÚÑÜ ]", "", texto)
        return texto.strip()

    ABREV_PALABRA = ["sa","srl","sc","sh"]
    ABREV_SUB     = ["sas","sca","sociedadanonima","sociedadderesponsabilidadlimitada","ltda"]

    def es_sociedad(texto_original: str) -> bool:
        tl = _limpiar(texto_original)
        tc = limpiar_con_espacios(texto_original)
        palabras = tc.split()
        if any(p in palabras for p in ABREV_PALABRA): return True
        if any(a in tl for a in ABREV_SUB):            return True
        return False

    desc         = df["descripcion"].apply(_limpiar)
    ley_ad1      = df["leyenda adicional1"]
    ley_ad1_limp = df["leyenda adicional1"].apply(_limpiar)
    ley_ad2_limp = df["leyenda adicional2"].apply(_limpiar)
    concepto_limp = df["concepto"].apply(_limpiar)
    sin_cat      = df["conciliacion"] == "0"

    es_deb_autom = desc == _limpiar("DEB. AUTOM. DE SERV.")
    es_seguro    = ley_ad2_limp.apply(lambda d: any(p in d for p in ["pagoseguro","seguros","segurosp","pagopoliza","seguro","poliza"]))
    es_afip      = ley_ad1_limp.apply(lambda d: "afip" in d)

    df.loc[sin_cat & es_deb_autom & es_seguro,             "conciliacion"] = "Seguros"
    df.loc[sin_cat & es_deb_autom & ~es_seguro & es_afip,  "conciliacion"] = "Imp. AFIP"
    df.loc[sin_cat & es_deb_autom & ~es_seguro & ~es_afip, "conciliacion"] = "Proveedores"
    sin_cat = df["conciliacion"] == "0"

    es_acred = desc == _limpiar("SERVICIO ACREDITAMIENTO DE HABERES")
    df.loc[sin_cat & es_acred & ley_ad1_limp.apply(lambda d: "acredhaberes" in d),       "conciliacion"] = "Sueldos"
    df.loc[sin_cat & es_acred & ley_ad1_limp.apply(lambda d: "reintegroviaticos" in d),  "conciliacion"] = "Rendiciones"
    df.loc[sin_cat & es_acred & ley_ad1_limp.apply(lambda d: "indemnizaciones" in d),    "conciliacion"] = "Indemnizaciones"
    sin_cat = df["conciliacion"] == "0"

    es_transf = desc.apply(lambda d: d in [_limpiar("TRANSFERENCIA A TERCEROS"), _limpiar("TRANSF. A TERCEROS")])
    es_prov   = ley_ad1.apply(es_sociedad)
    df.loc[sin_cat & es_transf & es_prov,  "conciliacion"] = "Proveedores"
    df.loc[sin_cat & es_transf & ~es_prov, "conciliacion"] = "Sueldos"
    sin_cat = df["conciliacion"] == "0"

    es_art = ley_ad1_limp.apply(lambda d: any(p in d for p in ["prevencion","previcion","riesgo","aseguradora"]))
    df.loc[sin_cat & es_art, "conciliacion"] = "ART"
    sin_cat = df["conciliacion"] == "0"

    es_propia = concepto_limp.apply(lambda d: "propia" in d)
    df.loc[sin_cat & es_propia, "conciliacion"] = "Transf. entre cuentas"
    return df


def categorizar_mayor_v1(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def limpiar_norm(texto: str) -> str:
        if not isinstance(texto, str): return ""
        texto = texto.lower()
        texto = re.sub(r"[.\-\s]", "", texto)
        texto = re.sub(r"[^\x20-\x7EáéíóúñüÁÉÍÓÚÑÜ]", "", texto)
        return (texto.replace("á","a").replace("é","e").replace("í","i")
                     .replace("ó","o").replace("ú","u").replace("ñ","n").replace("ü","u"))

    comentario     = df["Comentario"].apply(limpiar_norm)
    comentario_raw = df["Comentario"].astype(str).str.strip()

    condiciones = [
        comentario.apply(lambda d: _contiene(d,"acreditacion","acreditaciones")),
        comentario.apply(lambda d: _contiene(d,"cobronf")),
        comentario.apply(lambda d: _contiene(d,"descuento","descuentos")),
        comentario.apply(lambda d: _contiene(d,"pagogb","gb")),
        comentario.apply(lambda d: _contiene(d,"difcambio","periodo")),
        comentario.apply(lambda d: _contiene(d,"pagocuota")),
        comentario_raw.str.contains("ART", regex=False),
    ]
    categorias = ["Acreditaciones","Cobranzas","Descuento Galicia","Gastos Bancarios","Seguros","Prestamo","ART"]

    df["conciliacion"] = "0"
    for cond, cat in zip(condiciones, categorias):
        df.loc[cond & (df["conciliacion"] == "0"), "conciliacion"] = cat
    return df


def categorizar_mayor_v2(df: pd.DataFrame) -> pd.DataFrame:
    df      = df.copy()
    sin_cat = df["conciliacion"] == "0"
    tercero = df["Tercero"].apply(_limpiar)

    es_seguro = tercero.apply(lambda d: "seguro" in d or "swiss" in d)
    es_rend   = tercero.apply(lambda d: "gastos" in d or "rendicion" in d)

    df.loc[sin_cat & es_seguro, "conciliacion"] = "Seguros"
    sin_cat = df["conciliacion"] == "0"
    df.loc[sin_cat & es_rend,   "conciliacion"] = "Rendiciones"
    return df


# ─────────────────────────────────────────────
# CRUCES
# ─────────────────────────────────────────────

def cruzar_mayor_extracto(df_mayor_dep, df_extracto_dep):
    mayor    = df_mayor_dep.copy().reset_index(drop=True)
    extracto = df_extracto_dep.copy().reset_index(drop=True)

    col_fm = _get_col(mayor,    "Fecha","fecha")
    col_im = _get_col(mayor,    "Importe","importe")
    col_fe = _get_col(extracto, "fecha","Fecha")
    col_ie = _get_col(extracto, "importe","Importe")

    mayor["_key"]    = mayor[col_fm].astype(str) + "|" + mayor[col_im].astype(str)
    extracto["_key"] = extracto[col_fe].astype(str) + "|" + extracto[col_ie].astype(str)
    mayor["_count"]    = mayor.groupby("_key").cumcount()
    extracto["_count"] = extracto.groupby("_key").cumcount()
    mayor["_idx_m"]    = mayor.index
    extracto["_idx_e"] = extracto.index

    merged = mayor[["_key","_count","_idx_m"]].merge(
        extracto[["_key","_count","_idx_e"]], on=["_key","_count"], how="outer", indicator=True)

    idx_mm = merged.loc[merged["_merge"]=="both",       "_idx_m"].dropna().astype(int).tolist()
    idx_me = merged.loc[merged["_merge"]=="both",       "_idx_e"].dropna().astype(int).tolist()
    idx_fe = merged.loc[merged["_merge"]=="left_only",  "_idx_m"].dropna().astype(int).tolist()
    idx_fm = merged.loc[merged["_merge"]=="right_only", "_idx_e"].dropna().astype(int).tolist()

    mayor    = mayor.drop(columns=["_key","_count","_idx_m"])
    extracto = extracto.drop(columns=["_key","_count","_idx_e"])

    return (mayor.loc[idx_mm].reset_index(drop=True),
            extracto.loc[idx_me].reset_index(drop=True),
            mayor.loc[idx_fe].reset_index(drop=True),
            extracto.loc[idx_fm].reset_index(drop=True))


def cruzar_con_tolerancia(df_mayor_cat, df_extracto_sin_acreditaciones,
                           tolerancia_importe=0.5, tolerancia_dias=3):
    mayor    = df_mayor_cat.copy().reset_index(drop=True)
    extracto = df_extracto_sin_acreditaciones.copy().reset_index(drop=True)

    col_fm = _get_col(mayor,    "Fecha","fecha")
    col_im = _get_col(mayor,    "Importe","importe")
    col_fe = _get_col(extracto, "fecha","Fecha")
    col_ie = _get_col(extracto, "importe","Importe")

    mayor["_idx_m"]    = mayor.index
    extracto["_idx_e"] = extracto.index
    usado_m, usado_e   = set(), set()
    match_m, match_e   = [], []
    delta = timedelta(days=tolerancia_dias)

    for idx_m, row_m in mayor.iterrows():
        if idx_m in usado_m: continue
        fecha_m, imp_m = row_m[col_fm], row_m[col_im]
        cands = extracto[
            (extracto[col_fe] >= fecha_m - delta) &
            (extracto[col_fe] <= fecha_m + delta) &
            (~extracto.index.isin(usado_e))
        ].copy()
        cands["_dist"] = (cands[col_fe] - fecha_m).abs()
        cands = cands.sort_values("_dist")
        for idx_e, row_e in cands.iterrows():
            if abs(imp_m - row_e[col_ie]) <= tolerancia_importe:
                es_exacto = (row_e[col_fe] == fecha_m and row_e[col_ie] == imp_m)
                usado_m.add(idx_m); usado_e.add(idx_e)
                if not es_exacto:
                    match_m.append(idx_m); match_e.append(idx_e)
                break

    mayor    = mayor.drop(columns=["_idx_m"])
    extracto = extracto.drop(columns=["_idx_e"])
    return (mayor.loc[match_m].reset_index(drop=True),
            extracto.loc[match_e].reset_index(drop=True),
            mayor[~mayor.index.isin(usado_m)].reset_index(drop=True),
            extracto[~extracto.index.isin(usado_e)].reset_index(drop=True))


def cruzar_por_categoria(falta_extracto1, falta_mayor1, tolerancia=0.5):
    col_ie = _get_col(falta_extracto1, "Importe","importe")
    col_im = _get_col(falta_mayor1,    "importe","Importe")
    col_ce = _get_col(falta_extracto1, "conciliacion")
    col_cm = _get_col(falta_mayor1,    "conciliacion")

    fe, fm = falta_extracto1.copy().reset_index(drop=True), falta_mayor1.copy().reset_index(drop=True)
    usado_e, usado_m, idx_e, idx_m = set(), set(), [], []

    for cat in (set(fe[col_ce].unique()) | set(fm[col_cm].unique())) - {"0"}:
        ge = fe[(fe[col_ce]==cat) & (~fe.index.isin(usado_e))]
        gm = fm[(fm[col_cm]==cat) & (~fm.index.isin(usado_m))]
        if ge.empty or gm.empty: continue
        if abs(ge[col_ie].sum() - gm[col_im].sum()) <= tolerancia:
            idx_e.extend(ge.index.tolist()); idx_m.extend(gm.index.tolist())
            usado_e.update(ge.index); usado_m.update(gm.index)

    return (fe.loc[list(set(idx_e))].reset_index(drop=True),
            fm.loc[list(set(idx_m))].reset_index(drop=True),
            fe[~fe.index.isin(usado_e)].reset_index(drop=True),
            fm[~fm.index.isin(usado_m)].reset_index(drop=True))


def cruzar_proveedores(falta_extracto2, falta_mayor2, tolerancia=0.5):
    col_fe = _get_col(falta_extracto2, "Fecha","fecha")
    col_ie = _get_col(falta_extracto2, "Importe","importe")
    col_ce = _get_col(falta_extracto2, "conciliacion")
    col_fm = _get_col(falta_mayor2,    "fecha","Fecha")
    col_im = _get_col(falta_mayor2,    "importe","Importe")
    col_cm = _get_col(falta_mayor2,    "conciliacion")
    col_se = _get_col(falta_extracto2, "Serie","serie")

    fe, fm = falta_extracto2.copy().reset_index(drop=True), falta_mayor2.copy().reset_index(drop=True)
    usado_e, usado_m, idx_e, idx_m = set(), set(), [], []

    for fecha in fm[fm[col_cm]=="Proveedores"][col_fm].unique():
        gm = fm[(fm[col_fm]==fecha) & (fm[col_cm]=="Proveedores") & (~fm.index.isin(usado_m))]
        if gm.empty: continue
        ge = fe[(fe[col_fe]==fecha) & (fe[col_ce]=="0") &
                (~fe[col_se].astype(str).str.strip().str.upper().str.contains("TP", na=False)) &
                (~fe.index.isin(usado_e))]
        if ge.empty: continue
        if abs(ge[col_ie].sum() - gm[col_im].sum()) <= tolerancia:
            idx_e.extend(ge.index.tolist()); idx_m.extend(gm.index.tolist())
            usado_e.update(ge.index); usado_m.update(gm.index)

    return (fe.loc[list(set(idx_e))].reset_index(drop=True),
            fm.loc[list(set(idx_m))].reset_index(drop=True),
            fe[~fe.index.isin(usado_e)].reset_index(drop=True),
            fm[~fm.index.isin(usado_m)].reset_index(drop=True))


def cruzar_a1tp(falta_extracto3, falta_mayor3, tolerancia=0.5):
    col_fe = _get_col(falta_extracto3, "Fecha","fecha")
    col_ie = _get_col(falta_extracto3, "Importe","importe")
    col_fm = _get_col(falta_mayor3,    "fecha","Fecha")
    col_im = _get_col(falta_mayor3,    "importe","Importe")
    col_se = _get_col(falta_extracto3, "Serie","serie")
    col_cm = _get_col(falta_mayor3,    "conciliacion")
    CATS   = {"Sueldos","Imp. AFIP","Indemnizaciones"}

    fe, fm = falta_extracto3.copy().reset_index(drop=True), falta_mayor3.copy().reset_index(drop=True)
    usado_e, usado_m, idx_e, idx_m = set(), set(), [], []

    tp_e = fe[fe[col_se].astype(str).str.strip().str.upper().str.contains("TP", na=False)]
    for fecha in tp_e[col_fe].unique():
        ge = fe[(fe[col_fe]==fecha) &
                (fe[col_se].astype(str).str.strip().str.upper().str.contains("TP", na=False)) &
                (~fe.index.isin(usado_e))]
        gm = fm[(fm[col_fm]==fecha) & (fm[col_cm].isin(CATS)) & (~fm.index.isin(usado_m))]
        if ge.empty or gm.empty: continue
        if abs(ge[col_ie].sum() - gm[col_im].sum()) <= tolerancia:
            idx_e.extend(ge.index.tolist()); idx_m.extend(gm.index.tolist())
            usado_e.update(ge.index); usado_m.update(gm.index)

    return (fe.loc[list(set(idx_e))].reset_index(drop=True),
            fm.loc[list(set(idx_m))].reset_index(drop=True),
            fe[~fe.index.isin(usado_e)].reset_index(drop=True),
            fm[~fm.index.isin(usado_m)].reset_index(drop=True))


def cruzar_por_proveedor(falta_extracto4, falta_mayor4, tolerancia_importe=0.5, top_candidatos=4):
    try:
        from rapidfuzz import process, fuzz
    except ImportError:
        raise ImportError("Instalá rapidfuzz: pip install rapidfuzz")

    col_ie = _get_col(falta_extracto4, "Importe","importe")
    col_im = _get_col(falta_mayor4,    "importe","Importe")
    col_te = _get_col(falta_extracto4, "Tercero","tercero")
    col_lm = _get_col(falta_mayor4,    "leyenda adicional1")

    fe, fm = falta_extracto4.copy().reset_index(drop=True), falta_mayor4.copy().reset_index(drop=True)
    usado_e, usado_m, idx_e, idx_m = set(), set(), [], []
    nombres_usados_m = set()

    suma_e_by = fe.groupby(col_te)[col_ie].sum()
    suma_m_by = fm.groupby(col_lm)[col_im].sum()
    nombres_m  = suma_m_by.index.tolist()

    for nombre_e, suma_e in suma_e_by.items():
        candidatos = process.extract(nombre_e, nombres_m, scorer=fuzz.token_sort_ratio, limit=top_candidatos)
        encontrado = False
        for nombre_m, score, _ in candidatos:
            if nombre_m in nombres_usados_m: continue
            if abs(suma_e - suma_m_by[nombre_m]) <= tolerancia_importe:
                ie = fe[(fe[col_te]==nombre_e) & (~fe.index.isin(usado_e))].index.tolist()
                im = fm[(fm[col_lm]==nombre_m) & (~fm.index.isin(usado_m))].index.tolist()
                idx_e.extend(ie); idx_m.extend(im)
                usado_e.update(ie); usado_m.update(im)
                nombres_usados_m.add(nombre_m); encontrado = True; break
        if not encontrado:
            for nombre_m, suma_m in suma_m_by.items():
                if nombre_m in nombres_usados_m: continue
                if abs(suma_e - suma_m) <= tolerancia_importe:
                    ie = fe[(fe[col_te]==nombre_e) & (~fe.index.isin(usado_e))].index.tolist()
                    im = fm[(fm[col_lm]==nombre_m) & (~fm.index.isin(usado_m))].index.tolist()
                    idx_e.extend(ie); idx_m.extend(im)
                    usado_e.update(ie); usado_m.update(im)
                    nombres_usados_m.add(nombre_m); encontrado = True; break

    return (fe.loc[list(set(idx_e))].reset_index(drop=True),
            fm.loc[list(set(idx_m))].reset_index(drop=True),
            fe[~fe.index.isin(usado_e)].reset_index(drop=True),
            fm[~fm.index.isin(usado_m)].reset_index(drop=True))


def cruzar_echeq(falta_extracto5, falta_mayor5, tolerancia=0.5):
    col_fe = _get_col(falta_extracto5, "Fecha","fecha")
    col_ie = _get_col(falta_extracto5, "Importe","importe")
    col_se = _get_col(falta_extracto5, "Serie","serie")
    col_ce = _get_col(falta_extracto5, "conciliacion")
    col_fm = _get_col(falta_mayor5,    "fecha","Fecha")
    col_im = _get_col(falta_mayor5,    "importe","Importe")
    col_cm = _get_col(falta_mayor5,    "conciliacion")

    fe, fm = falta_extracto5.copy().reset_index(drop=True), falta_mayor5.copy().reset_index(drop=True)
    usado_e, usado_m, idx_e, idx_m = set(), set(), [], []

    mask_e = ((fe[col_ce]=="0") &
              (fe[col_se].isna() | fe[col_se].astype(str).str.strip().eq("") |
               fe[col_se].astype(str).str.strip().eq("nan")))
    mask_m  = fm[col_cm] == "Echeq"

    for fecha in fe.loc[mask_e, col_fe].unique():
        ge = fe[mask_e & (fe[col_fe]==fecha) & (~fe.index.isin(usado_e))]
        gm = fm[mask_m & (fm[col_fm]==fecha) & (~fm.index.isin(usado_m))]
        if ge.empty or gm.empty: continue
        if abs(ge[col_ie].sum() - gm[col_im].sum()) <= tolerancia:
            idx_e.extend(ge.index.tolist()); idx_m.extend(gm.index.tolist())
            usado_e.update(ge.index); usado_m.update(gm.index)

    return (fe.loc[list(set(idx_e))].reset_index(drop=True),
            fm.loc[list(set(idx_m))].reset_index(drop=True),
            fe[~fe.index.isin(usado_e)].reset_index(drop=True),
            fm[~fm.index.isin(usado_m)].reset_index(drop=True))


def limpiar_proveedores(df_proveedores, match_mayor, match_mayor1,
                         match_extracto3, match_extracto5, tolerancia_importe=0.5):
    col_monto        = _find_col(df_proveedores, "monto")
    col_estado       = _find_col(df_proveedores, "estado")
    col_fecha_emis_p = next((c for c in df_proveedores.columns if "emis" in c.lower()), None)
    if col_fecha_emis_p is None:
        raise KeyError(f"Columna de fecha de emisión no encontrada en {list(df_proveedores.columns)}")

    fp = df_proveedores[
        ~df_proveedores[col_estado].astype(str).str.upper().str.contains("ERROR", na=False)
    ].copy().reset_index(drop=True)

    fp[col_monto] = (
        fp[col_monto].astype(str).str.replace(",",".",regex=False).str.strip()
        .pipe(pd.to_numeric, errors="coerce").fillna(0.0)
    )
    fp[col_fecha_emis_p] = pd.to_datetime(fp[col_fecha_emis_p], errors="coerce")

    def cruzar_y_sacar(fp, match, tolerancia):
        col_fm = _get_col(match, "Fecha","fecha")
        col_im = _get_col(match, "Importe","importe")
        mm = match.copy()
        mm[col_fm] = pd.to_datetime(mm[col_fm], errors="coerce")
        mm[col_im] = pd.to_numeric(mm[col_im], errors="coerce").fillna(0.0)
        idx_usados = set()
        for idx_p, row_p in fp.iterrows():
            fecha_p = row_p[col_fecha_emis_p]
            monto_p = row_p[col_monto] * -1
            coincide = mm[(mm[col_fm]==fecha_p) &
                          (mm[col_im].apply(lambda x: abs(x - monto_p) <= tolerancia))]
            if not coincide.empty:
                idx_usados.add(idx_p)
        return fp[~fp.index.isin(idx_usados)].reset_index(drop=True), len(idx_usados)

    fp, _ = cruzar_y_sacar(fp, match_mayor,    tolerancia=0.0)
    fp, _ = cruzar_y_sacar(fp, match_mayor1,   tolerancia=tolerancia_importe)
    fp, _ = cruzar_y_sacar(fp, match_extracto3, tolerancia=tolerancia_importe)
    fp, _ = cruzar_y_sacar(fp, match_extracto5, tolerancia=tolerancia_importe)
    return fp


def cruzar_proveedores_descarga(falta_extracto6, falta_mayor6, df_proveedores_def,
                                 tolerancia_importe=0.5, top_candidatos=3):
    try:
        from rapidfuzz import process, fuzz
    except ImportError:
        raise ImportError("Instalá rapidfuzz: pip install rapidfuzz")

    col_te    = _get_col(falta_extracto6, "Tercero","tercero")
    col_ie    = _get_col(falta_extracto6, "Importe","importe")
    col_fe    = _get_col(falta_extracto6, "Fecha","fecha")
    col_fm    = _get_col(falta_mayor6,    "fecha","Fecha")
    col_im    = _get_col(falta_mayor6,    "importe","Importe")
    col_cm    = _get_col(falta_mayor6,    "conciliacion")
    col_razon = next((c for c in df_proveedores_def.columns if "raz" in c.lower()), None)
    if col_razon is None:
        raise KeyError(f"Columna razón social no encontrada en {list(df_proveedores_def.columns)}")
    col_monto   = _find_col(df_proveedores_def, "monto")
    col_fecha_p = _find_col(df_proveedores_def, "fecha")

    fe = falta_extracto6.copy().reset_index(drop=True)
    fm = falta_mayor6.copy().reset_index(drop=True)
    fp = df_proveedores_def.copy().reset_index(drop=True)
    fp[col_monto]   = pd.to_numeric(fp[col_monto].astype(str).str.replace(",",".",regex=False).str.strip(), errors="coerce").fillna(0.0)
    fp[col_fecha_p] = pd.to_datetime(fp[col_fecha_p], errors="coerce")

    fp_neg = fp.copy(); fp_neg[col_monto] = fp_neg[col_monto] * -1
    suma_e_by  = fe.groupby(col_te)[col_ie].sum()
    suma_p_by  = fp_neg.groupby(col_razon)[col_monto].sum()
    nombres_p  = suma_p_by.index.tolist()

    usado_e, idx_e           = set(), []
    nombres_matcheados       = set()
    fechas_matcheadas        = set()
    nombres_no_matcheados    = set()

    for nombre_e, suma_e in suma_e_by.items():
        candidatos = process.extract(nombre_e, nombres_p, scorer=fuzz.token_sort_ratio, limit=top_candidatos)
        encontrado = False
        for nombre_p, score, _ in candidatos:
            if nombre_p in nombres_matcheados: continue
            if abs(suma_e - suma_p_by[nombre_p]) <= tolerancia_importe:
                ie = fe[(fe[col_te]==nombre_e) & (~fe.index.isin(usado_e))].index.tolist()
                fechas_matcheadas.update(fe.loc[ie, col_fe].unique().tolist())
                idx_e.extend(ie); usado_e.update(ie)
                nombres_matcheados.add(nombre_p); encontrado = True; break
        if not encontrado:
            if candidatos: nombres_no_matcheados.add(candidatos[0][0])

    nombres_no_matcheados = set(nombres_p) - nombres_matcheados

    # Sacar de falta_mayor6 filas de fechas matcheadas
    mask_match_mayor = (fm[col_fm].isin(fechas_matcheadas) & (fm[col_cm]=="Proveedores"))
    match_mayor7 = fm[mask_match_mayor].reset_index(drop=True)
    usado_mayor  = set(fm[mask_match_mayor].index.tolist())

    # Agregar proveedores no matcheados a falta_mayor7
    filas_nuevas = []
    for nombre_p in nombres_no_matcheados:
        for _, row in fp[fp[col_razon]==nombre_p].iterrows():
            monto = row[col_monto]; fecha = row[col_fecha_p]
            fila  = {col: "" for col in fm.columns}
            if col_fm          in fila: fila[col_fm]          = fecha
            if "descripcion"   in fila: fila["descripcion"]   = "TRF INMED PROVEED"
            if "origen"        in fila: fila["origen"]        = "AUTO"
            if "debitos"       in fila: fila["debitos"]       = monto
            if "leyenda adicional1" in fila: fila["leyenda adicional1"] = nombre_p
            if col_im          in fila: fila[col_im]          = -abs(monto)
            if col_cm          in fila: fila[col_cm]          = "Proveedores"
            filas_nuevas.append(fila)

    falta_mayor7_base = fm[~fm.index.isin(usado_mayor)].reset_index(drop=True)
    falta_mayor7 = pd.concat([falta_mayor7_base, pd.DataFrame(filas_nuevas)], ignore_index=True) if filas_nuevas else falta_mayor7_base

    match_extracto7 = fe.loc[list(set(idx_e))].reset_index(drop=True)
    falta_extracto7 = fe[~fe.index.isin(usado_e)].reset_index(drop=True)

    return match_extracto7, match_mayor7, falta_extracto7, falta_mayor7


def cruzar_acreditaciones(df_extracto_cat2, falta_extracto7, falta_mayor7, tolerancia=0.5):
    col_ic = _get_col(df_extracto_cat2, "importe","Importe")
    col_cc = _get_col(df_extracto_cat2, "conciliacion")
    col_if = _get_col(falta_extracto7,  "Importe","importe")
    col_cf = _get_col(falta_extracto7,  "conciliacion")

    acred_cat  = df_extracto_cat2[df_extracto_cat2[col_cc]=="Acreditaciones"]
    suma_cat   = acred_cat[col_ic].sum()
    acred_falt = falta_extracto7[falta_extracto7[col_cf]=="Acreditaciones"]
    suma_falt  = acred_falt[col_if].sum()

    if abs(suma_cat - suma_falt) <= tolerancia:
        match_cat  = acred_cat.reset_index(drop=True)
        match_falt = acred_falt.reset_index(drop=True)
        falta_ext8 = falta_extracto7[~falta_extracto7.index.isin(acred_falt.index)].reset_index(drop=True)
        falta_may8 = falta_mayor7.copy().reset_index(drop=True)
    else:
        match_cat  = pd.DataFrame(columns=df_extracto_cat2.columns)
        match_falt = pd.DataFrame(columns=falta_extracto7.columns)
        falta_ext8 = falta_extracto7.copy().reset_index(drop=True)
        col_im = _get_col(falta_mayor7, "importe","Importe")
        col_dm = _get_col(falta_mayor7, "descripcion","Descripcion","descripción")
        col_cm = _get_col(falta_mayor7, "conciliacion")
        fila   = {col: "" for col in falta_mayor7.columns}
        fila[col_im] = suma_cat; fila[col_dm] = "Acreditaciones sumadas"; fila[col_cm] = "Acreditaciones"
        falta_may8 = pd.concat([falta_mayor7, pd.DataFrame([fila])], ignore_index=True)

    return match_cat, match_falt, falta_ext8, falta_may8


# ─────────────────────────────────────────────
# ASIGNACIÓN DE IDs
# ─────────────────────────────────────────────

def _asignar_id(df_m, df_e, match_tipo, id_inicio, agrupar_por=None):
    dm = df_m.copy().reset_index(drop=True)
    de = df_e.copy().reset_index(drop=True)
    if agrupar_por is None:
        ids = list(range(id_inicio, id_inicio + len(dm)))
        dm["match_id"] = ids; de["match_id"] = ids
        id_fin = id_inicio + len(dm)
    else:
        col_m, col_e = agrupar_por
        current = id_inicio; mapa = {}
        for v in list(dm[col_m].unique()) + list(de[col_e].unique()):
            if v not in mapa: mapa[v] = current; current += 1
        dm["match_id"] = dm[col_m].map(mapa)
        de["match_id"] = de[col_e].map(mapa)
        id_fin = current
    dm["match_tipo"] = match_tipo; de["match_tipo"] = match_tipo
    return dm, de, id_fin


# ─────────────────────────────────────────────
# EXPORTAR EN MEMORIA (4 hojas — sin Dif Proveedores)
# ─────────────────────────────────────────────

def generar_excel_en_memoria_galicia(falta_mayor8, falta_extracto8,
                                      match_mayor_def, match_extracto_def):
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        falta_mayor8.to_excel(writer,       sheet_name="Faltante Mayor",    index=False)
        falta_extracto8.to_excel(writer,    sheet_name="Faltante Extracto", index=False)
        match_mayor_def.to_excel(writer,    sheet_name="Match Mayor",       index=False)
        match_extracto_def.to_excel(writer, sheet_name="Match Extracto",    index=False)
    return buf.getvalue()


# ─────────────────────────────────────────────
# PIPELINE COMPLETO
# ─────────────────────────────────────────────

def correr_conciliacion_galicia(archivo_mayor, archivo_extracto, archivo_proveedores):
    # 1. Cargar
    df_mayor    = load_excel_file(archivo_mayor)
    df_extracto = load_excel_file(archivo_extracto)
    df_prov     = load_excel_file(archivo_proveedores)

    # 2. Normalizar
    df_mayor_dep    = normalize_mayor(df_mayor)
    df_extracto_dep = normalize_extracto_galicia(df_extracto)

    # 3. Categorizar extracto
    df_ext_cat1      = categorizar_extracto_v1(df_extracto_dep)
    df_ext_cat2      = categorizar_extracto_v2(df_ext_cat1)
    df_ext_sin_acred = df_ext_cat2[df_ext_cat2["conciliacion"] != "Acreditaciones"].reset_index(drop=True)

    # 4. Categorizar mayor
    df_may_cat1 = categorizar_mayor_v1(df_mayor_dep)
    df_may_cat2 = categorizar_mayor_v2(df_may_cat1)

    # 5. Cruce 0 — exacto
    mm0, me0, fe0, fm0 = cruzar_mayor_extracto(df_may_cat2, df_ext_sin_acred)

    # 6. Cruce 1 — tolerancia
    mm1, me1, fe1, fm1 = cruzar_con_tolerancia(df_may_cat2, df_ext_sin_acred)

    # 7. Cruce 2 — por categoría
    me2, mm2, fe2, fm2 = cruzar_por_categoria(fe1, fm1)

    # 8. Cruce 3 — proveedores por fecha
    me3, mm3, fe3, fm3 = cruzar_proveedores(fe2, fm2)

    # 9. Cruce 4 — TP
    me4, mm4, fe4, fm4 = cruzar_a1tp(fe3, fm3)

    # 10. Cruce 5 — proveedores fuzzy
    me5, mm5, fe5, fm5 = cruzar_por_proveedor(fe4, fm4)

    # 11. Cruce 6 — Echeq
    me6, mm6, fe6, fm6 = cruzar_echeq(fe5, fm5)

    # 12. Limpiar proveedores y cruce 7
    df_prov_def = limpiar_proveedores(df_prov, mm0, mm1, me3, me5)
    me7, mm7, fe7, falta_may7 = cruzar_proveedores_descarga(fe6, fm6, df_prov_def)

    # 13. Cruce 8 — acreditaciones
    match_acred_cat, match_acred, falta_ext8, falta_may8 = cruzar_acreditaciones(df_ext_cat2, fe7, falta_may7)

    # 14. Asignar IDs
    dm0, de0, id1 = _asignar_id(mm0, me0, "0", 1)
    dm1, de1, id2 = _asignar_id(mm1, me1, "1", id1)
    col_ce2 = _get_col(me2,"conciliacion"); col_cm2 = _get_col(mm2,"conciliacion")
    dm2, de2, id3 = _asignar_id(mm2, me2, "2", id2, agrupar_por=(col_cm2, col_ce2))
    col_fe3 = _get_col(me3,"Fecha","fecha"); col_fm3 = _get_col(mm3,"fecha","Fecha")
    dm3, de3, id4 = _asignar_id(mm3, me3, "3", id3, agrupar_por=(col_fm3, col_fe3))
    col_fe4 = _get_col(me4,"Fecha","fecha"); col_fm4 = _get_col(mm4,"fecha","Fecha")
    dm4, de4, id5 = _asignar_id(mm4, me4, "4", id4, agrupar_por=(col_fm4, col_fe4))
    col_te5 = _get_col(me5,"Tercero","tercero"); col_lm5 = _get_col(mm5,"leyenda adicional1")
    dm5, de5, id6 = _asignar_id(mm5, me5, "5", id5, agrupar_por=(col_lm5, col_te5))
    col_fe6 = _get_col(me6,"Fecha","fecha"); col_fm6 = _get_col(mm6,"fecha","Fecha")
    dm6, de6, id7 = _asignar_id(mm6, me6, "6", id6, agrupar_por=(col_fm6, col_fe6))
    col_fe7 = _get_col(me7,"Fecha","fecha"); col_fm7 = _get_col(mm7,"fecha","Fecha")
    dm7, de7, id8 = _asignar_id(mm7, me7, "7", id7, agrupar_por=(col_fm7, col_fe7))

    da8 = match_acred.copy().reset_index(drop=True)
    dc8 = match_acred_cat.copy().reset_index(drop=True)
    if len(da8) > 0: da8["match_id"] = id8; da8["match_tipo"] = "8"
    if len(dc8) > 0: dc8["match_id"] = id8; dc8["match_tipo"] = "8"

    # 15. Consolidar
    match_mayor_def = pd.concat([dm0, dm1, de2, de3, de4, de5, de6, de7, da8], ignore_index=True)
    match_ext_def   = pd.concat([de0, de1, dm2, dm3, dm4, dm5, dm6, dm7, dc8], ignore_index=True)

    stats = {
        "match_exacto":     len(mm0),
        "match_tolerancia": len(mm1),
        "falta_mayor":      len(falta_may8),
        "falta_extracto":   len(falta_ext8),
    }

    buf = generar_excel_en_memoria_galicia(falta_may8, falta_ext8, match_mayor_def, match_ext_def)
    return buf, stats
