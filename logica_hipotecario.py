"""
Lógica de conciliación bancaria - Banco Hipotecario
"""
import re
from io import BytesIO
from datetime import timedelta

import pandas as pd


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


def normalize_extracto_hipotecario(df: pd.DataFrame) -> pd.DataFrame:
    import re
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"[^\x20-\x7EáéíóúÁÉÍÓÚñÑüÜ]", "", regex=True)
        .str.lower()
        .str.replace("á","a").str.replace("é","e").str.replace("í","i")
        .str.replace("ó","o").str.replace("ú","u").str.replace("ñ","n")
    )

    def limpiar_texto(texto: str) -> str:
        if not isinstance(texto, str): return ""
        return re.sub(r"[.\-\s:,]", "", texto.lower())

    col_ref = next((c for c in df.columns if "referencia" in c), None)
    if col_ref:
        mask_total = df[col_ref].apply(lambda x: limpiar_texto(str(x)) == "total")
        if mask_total.any():
            idx_total = mask_total.idxmax()
            df = df.loc[:idx_total - 1].reset_index(drop=True)

    expected = {"debito en $", "credito en $", "saldo en $", "fecha"}
    missing  = expected - set(df.columns)
    if missing:
        raise ValueError(f"Columnas faltantes en extracto: {missing}. Disponibles: {list(df.columns)}")

    for col in ["debito en $", "credito en $", "saldo en $"]:
        df[col] = (
            df[col].astype(str).str.strip()
            .str.replace(r"\s+", "", regex=True)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce").fillna(0.0)
        )

    def calcular_importe(row):
        c, d = row["credito en $"], row["debito en $"]
        if c != 0 and d == 0:   return c
        elif d != 0 and c == 0: return -d
        else:                   return 0.0

    df["importe"] = df.apply(calcular_importe, axis=1)
    df["fecha"]   = pd.to_datetime(df["fecha"].astype(str).str.strip(), format="%Y-%m-%d %H:%M:%S", errors="coerce")
    return df


# ─────────────────────────────────────────────
# CATEGORIZACIÓN
# ─────────────────────────────────────────────

def _limpiar(texto: str) -> str:
    if not isinstance(texto, str): return ""
    texto = texto.lower()
    texto = re.sub(r"[.\-\s,:;]", "", texto)
    texto = re.sub(r"[^\x20-\x7EáéíóúñüÁÉÍÓÚÑÜ]", "", texto)
    return (texto.replace("á","a").replace("é","e").replace("í","i")
                 .replace("ó","o").replace("ú","u").replace("ñ","n").replace("ü","u"))

def _contiene(texto_limpio: str, *palabras: str) -> bool:
    return any(_limpiar(p) in texto_limpio for p in palabras)


def categorizar_extracto_v1(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    col_desc    = next((c for c in df.columns if "descripcion" in _limpiar(c)), None)
    col_importe = next((c for c in df.columns if "importe" in _limpiar(c)), None)
    if col_desc is None:    raise ValueError(f"Columna 'descripcion' no encontrada. Disponibles: {list(df.columns)}")
    if col_importe is None: raise ValueError(f"Columna 'importe' no encontrada. Disponibles: {list(df.columns)}")

    desc = df[col_desc].apply(_limpiar)

    def es_redondo_1000(importe) -> bool:
        try:    return float(importe) % 1000 == 0
        except: return False

    condiciones = [
        desc.apply(lambda d: _contiene(d, "prisma")),
        desc.apply(lambda d: _contiene(d, "debin")) & df[col_importe].apply(es_redondo_1000),
        desc.apply(lambda d: _contiene(d, "debin")),
        desc.apply(lambda d: _contiene(d, "comerciosfirstdata")),
        desc.apply(lambda d: _contiene(d, "cabal")),
        desc.apply(lambda d: _contiene(d, "impuesto","iva","comision","paquete","n/dinteradelccs/acuerd")),
        desc.apply(lambda d: "cuota" in d and "prestamo" in d),
        desc.apply(lambda d: _contiene(d, "sancor","swiss","berkley","laholando")),
        desc.apply(lambda d: _contiene(d, "tefdatanetmt","ctaprop","transfenv")),
    ]
    categorias = ["Prisma","Transf. entre cuentas","Acred. Debin","Acred. TC","Cabal",
                  "Gastos Bancarios","Prestamo","Seguros","Transf. entre cuentas"]

    df["conciliacion"] = "0"
    for cond, cat in zip(condiciones, categorias):
        df.loc[cond & (df["conciliacion"] == "0"), "conciliacion"] = cat
    return df


def categorizar_extracto_v2(df: pd.DataFrame) -> pd.DataFrame:
    df       = df.copy()
    col_desc = next((c for c in df.columns if "descripcion" in _limpiar(c)), None)
    if col_desc is None: raise ValueError(f"Columna 'descripcion' no encontrada. Disponibles: {list(df.columns)}")

    desc_raw  = df[col_desc].astype(str).str.lower().str.strip()
    desc_limp = df[col_desc].apply(_limpiar)
    sin_cat   = df["conciliacion"] == "0"

    df.loc[sin_cat & desc_raw.str.contains("n/d", na=False), "conciliacion"] = "Proveedores"
    sin_cat = df["conciliacion"] == "0"
    df.loc[sin_cat & desc_raw.str.contains("n/c", na=False), "conciliacion"] = "Cobranzas"
    sin_cat = df["conciliacion"] == "0"
    df.loc[sin_cat & desc_limp.str.contains(_limpiar("CR TRANSF.POR PAGO A PROVEEDORES O/B"), na=False), "conciliacion"] = "Cobranzas"
    return df


def categorizar_mayor_v1(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "Comentario" not in df.columns:
        raise ValueError(f"Columna 'Comentario' no encontrada. Disponibles: {list(df.columns)}")

    comentario = df["Comentario"].apply(_limpiar)
    condiciones = [
        comentario.apply(lambda d: _contiene(d, "tc")),
        comentario.apply(lambda d: _contiene(d, "cabal")),
        comentario.apply(lambda d: _contiene(d, "debin")),
        comentario.apply(lambda d: _contiene(d, "gb")),
        comentario.apply(lambda d: _contiene(d, "difcambio","periodo")),
        comentario.apply(lambda d: _contiene(d, "pagocuota")),
        comentario.apply(lambda d: _contiene(d, "prisma")),
        comentario.apply(lambda d: _contiene(d, "cobron/f")),
    ]
    categorias = ["Acred. TC","Cabal","Acred. Debin","Gastos Bancarios","Seguros","Prestamo","Prisma","Cobranzas"]

    df["conciliacion"] = "0"
    for cond, cat in zip(condiciones, categorias):
        df.loc[cond & (df["conciliacion"] == "0"), "conciliacion"] = cat
    return df


def categorizar_mayor_v2(df: pd.DataFrame) -> pd.DataFrame:
    df      = df.copy()
    serie   = df["Serie"].apply(_limpiar)
    tercero = df["Tercero"].apply(_limpiar)
    sin_cat = df["conciliacion"] == "0"

    es_tp     = serie.apply(lambda d: "tp" in d)
    es_seguro = tercero.apply(lambda d: any(p in d for p in ["swiss","sancor","berkley","laholando"]))

    df.loc[sin_cat & es_tp,     "conciliacion"] = "Transf. entre cuentas"
    sin_cat = df["conciliacion"] == "0"
    df.loc[sin_cat & es_seguro, "conciliacion"] = "Seguros"
    return df


# ─────────────────────────────────────────────
# CRUCES
# ─────────────────────────────────────────────

def _get_col(df, *candidates):
    for c in candidates:
        if c in df.columns: return c
    raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")


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


def cruzar_con_tolerancia(df_mayor_cat, df_extracto_sin_debin,
                           tolerancia_importe=0.5, tolerancia_dias=3):
    mayor    = df_mayor_cat.copy().reset_index(drop=True)
    extracto = df_extracto_sin_debin.copy().reset_index(drop=True)

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
    col_fm = _get_col(falta_mayor2,    "fecha","Fecha")
    col_im = _get_col(falta_mayor2,    "importe","Importe")
    col_cm = _get_col(falta_mayor2,    "conciliacion")
    col_se = _get_col(falta_extracto2, "Serie","serie")

    fe, fm = falta_extracto2.copy().reset_index(drop=True), falta_mayor2.copy().reset_index(drop=True)
    usado_e, usado_m, idx_e, idx_m = set(), set(), [], []

    for fecha in fm[fm[col_cm]=="Proveedores"][col_fm].unique():
        gm = fm[(fm[col_fm]==fecha) & (fm[col_cm]=="Proveedores") & (~fm.index.isin(usado_m))]
        if gm.empty: continue
        ge = fe[(fe[col_fe]==fecha) &
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


def cruzar_tp(falta_extracto3, falta_mayor3, tolerancia=0.5):
    col_fe = _get_col(falta_extracto3, "Fecha","fecha")
    col_ie = _get_col(falta_extracto3, "Importe","importe")
    col_fm = _get_col(falta_mayor3,    "fecha","Fecha")
    col_im = _get_col(falta_mayor3,    "importe","Importe")
    col_se = _get_col(falta_extracto3, "Serie","serie")

    fe, fm = falta_extracto3.copy().reset_index(drop=True), falta_mayor3.copy().reset_index(drop=True)
    usado_e, usado_m, idx_e, idx_m = set(), set(), [], []

    tp_e   = fe[fe[col_se].astype(str).str.strip().str.upper().str.contains("TP", na=False)]
    for fecha in tp_e[col_fe].unique():
        ge = fe[(fe[col_fe]==fecha) &
                (fe[col_se].astype(str).str.strip().str.upper().str.contains("TP", na=False)) &
                (~fe.index.isin(usado_e))]
        gm = fm[(fm[col_fm]==fecha) & (~fm.index.isin(usado_m))]
        if ge.empty or gm.empty: continue
        if abs(ge[col_ie].sum() - gm[col_im].sum()) <= tolerancia:
            idx_e.extend(ge.index.tolist()); idx_m.extend(gm.index.tolist())
            usado_e.update(ge.index); usado_m.update(gm.index)

    return (fe.loc[list(set(idx_e))].reset_index(drop=True),
            fm.loc[list(set(idx_m))].reset_index(drop=True),
            fe[~fe.index.isin(usado_e)].reset_index(drop=True),
            fm[~fm.index.isin(usado_m)].reset_index(drop=True))


def cruzar_debin(df_extracto_cat2, falta_extracto4, tolerancia=0.5):
    col_ic = _get_col(df_extracto_cat2, "importe","Importe")
    col_cc = _get_col(df_extracto_cat2, "conciliacion")
    col_if = _get_col(falta_extracto4,  "Importe","importe")
    col_cf = _get_col(falta_extracto4,  "conciliacion")

    debin_cat  = df_extracto_cat2[df_extracto_cat2[col_cc]=="Acred. Debin"]
    suma_cat   = debin_cat[col_ic].sum()
    debin_falt = falta_extracto4[falta_extracto4[col_cf]=="Acred. Debin"]
    suma_falt  = debin_falt[col_if].sum()

    if abs(suma_cat - suma_falt) <= tolerancia:
        match_debin     = debin_falt.reset_index(drop=True)
        falta_extracto5 = falta_extracto4[~falta_extracto4.index.isin(debin_falt.index)].reset_index(drop=True)
    else:
        match_debin     = pd.DataFrame(columns=falta_extracto4.columns)
        falta_extracto5 = falta_extracto4.copy().reset_index(drop=True)

    return match_debin, falta_extracto5


# ─────────────────────────────────────────────
# ASIGNACIÓN DE IDs
# ─────────────────────────────────────────────

def _asignar_id(df_m, df_e, match_tipo, id_inicio, agrupar_por=None):
    dm = df_m.copy().reset_index(drop=True)
    de = df_e.copy().reset_index(drop=True)
    if agrupar_por is None:
        n   = max(len(dm), len(de))
        ids = list(range(id_inicio, id_inicio + n))
        if len(dm) > 0: dm["match_id"] = ids[:len(dm)]
        if len(de) > 0: de["match_id"] = ids[:len(de)]
        id_fin = id_inicio + n
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
# EXPORTAR EN MEMORIA
# ─────────────────────────────────────────────

def generar_excel_en_memoria_hipotecario(falta_mayor4, falta_extracto5,
                                          match_mayor_def, match_extracto_def):
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        falta_mayor4.to_excel(writer,       sheet_name="Faltante Mayor",    index=False)
        falta_extracto5.to_excel(writer,    sheet_name="Faltante Extracto", index=False)
        match_mayor_def.to_excel(writer,    sheet_name="Match Mayor",       index=False)
        match_extracto_def.to_excel(writer, sheet_name="Match Extracto",    index=False)
    return buf.getvalue()


# ─────────────────────────────────────────────
# PIPELINE COMPLETO
# ─────────────────────────────────────────────

def correr_conciliacion_hipotecario(archivo_mayor, archivo_extracto):
    # 1. Cargar
    df_mayor    = load_excel_file(archivo_mayor)
    df_extracto = load_excel_file(archivo_extracto)

    # 2. Normalizar
    df_mayor_dep    = normalize_mayor(df_mayor)
    df_extracto_dep = normalize_extracto_hipotecario(df_extracto)

    # 3. Categorizar extracto
    df_ext_cat1 = categorizar_extracto_v1(df_extracto_dep)
    df_ext_cat2 = categorizar_extracto_v2(df_ext_cat1)
    df_ext_sin_debin = df_ext_cat2[df_ext_cat2["conciliacion"] != "Acred. Debin"].reset_index(drop=True)

    # 4. Categorizar mayor
    df_may_cat1 = categorizar_mayor_v1(df_mayor_dep)
    df_may_cat2 = categorizar_mayor_v2(df_may_cat1)

    # 5. Cruce 0 — exacto
    mm0, me0, fe0, fm0 = cruzar_mayor_extracto(df_may_cat2, df_ext_sin_debin)

    # 6. Cruce 1 — tolerancia
    mm1, me1, fe1, fm1 = cruzar_con_tolerancia(df_may_cat2, df_ext_sin_debin)

    # 7. Cruce 2 — por categoría
    me2, mm2, fe2, fm2 = cruzar_por_categoria(fe1, fm1)

    # 8. Cruce 3 — proveedores por fecha
    me3, mm3, fe3, fm3 = cruzar_proveedores(fe2, fm2)

    # 9. Cruce 4 — TP
    me4, mm4, fe4, fm4 = cruzar_tp(fe3, fm3)

    # 10. Cruce 5 — Debin
    match_debin, falta_ext5 = cruzar_debin(df_ext_cat2, fe4)

    # 11. Asignar IDs
    dm0, de0, id1 = _asignar_id(mm0, me0, "0", 1)
    dm1, de1, id2 = _asignar_id(mm1, me1, "1", id1)

    col_ce2 = _get_col(me2, "conciliacion"); col_cm2 = _get_col(mm2, "conciliacion")
    dm2, de2, id3 = _asignar_id(mm2, me2, "2", id2, agrupar_por=(col_cm2, col_ce2))

    col_fe3 = _get_col(me3, "Fecha","fecha"); col_fm3 = _get_col(mm3, "fecha","Fecha")
    dm3, de3, id4 = _asignar_id(mm3, me3, "3", id3, agrupar_por=(col_fm3, col_fe3))

    col_fe4 = _get_col(me4, "Fecha","fecha"); col_fm4 = _get_col(mm4, "fecha","Fecha")
    dm4, de4, id5 = _asignar_id(mm4, me4, "4", id4, agrupar_por=(col_fm4, col_fe4))

    dd5 = match_debin.copy().reset_index(drop=True)
    if len(dd5) > 0: dd5["match_id"] = id5; dd5["match_tipo"] = "5"

    # Debin del mayor (todas las acred. debin del extracto original)
    debin_mayor = df_ext_cat2[df_ext_cat2["conciliacion"]=="Acred. Debin"].copy()
    if len(debin_mayor) > 0:
        debin_mayor["match_id"]   = id5 if len(dd5) > 0 else -1
        debin_mayor["match_tipo"] = "5"

    # 12. Consolidar match definitivo
    match_mayor_def = pd.concat([dm0, dm1, de2, de3, de4], ignore_index=True)
    match_ext_def   = pd.concat([de0, de1, dm2, dm3, dm4, debin_mayor], ignore_index=True)

    stats = {
        "match_exacto":     len(mm0),
        "match_tolerancia": len(mm1),
        "falta_mayor":      len(fm4),
        "falta_extracto":   len(falta_ext5),
    }

    buf = generar_excel_en_memoria_hipotecario(fm4, falta_ext5, match_mayor_def, match_ext_def)
    return buf, stats
