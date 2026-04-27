"""
logica_galicia.py — Conciliación Banco Galicia
Código extraído literalmente del notebook conciliacion_galicia__8_.ipynb
Solo se agregó:
  - imports necesarios al tope
  - load_excel_file() adaptada para recibir file-like objects de Streamlit
  - correr_conciliacion_galicia() como pipeline de entrada para la app
"""

import re
from io import BytesIO
from datetime import timedelta

import pandas as pd
from rapidfuzz import process, fuzz


# ─────────────────────────────────────────────────────────────────────────────
# CARGA  (adaptada para Streamlit: recibe file-like en vez de Path)
# ─────────────────────────────────────────────────────────────────────────────

def load_excel_file(file) -> pd.DataFrame:
    return pd.read_excel(file, dtype=str)


# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZACIÓN
# ─────────────────────────────────────────────────────────────────────────────

def normalize_mayor(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.columns = (
        df.columns
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"[^\x20-\x7EáéíóúÁÉÍÓÚñÑüÜ]", "", regex=True)
        .str.title()
    )

    expected = {"Debe", "Haber", "Saldo", "Fecha"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(
            f"Columnas faltantes en el mayor: {missing}\n"
            f"Columnas disponibles: {list(df.columns)}"
        )

    for col in ["Debe", "Haber", "Saldo"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.replace(r"\s+", "", regex=True)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0.0)
        )

    def calcular_importe(row) -> float:
        debe, haber = row["Debe"], row["Haber"]
        if debe != 0 and haber == 0:
            return debe
        elif haber != 0 and debe == 0:
            return -haber
        else:
            return 0.0

    df["Importe"] = df.apply(calcular_importe, axis=1)

    df["Fecha"] = pd.to_datetime(
        df["Fecha"].astype(str).str.strip(),
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce"
    )

    return df


def normalize_extracto_galicia(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.columns = (
        df.columns
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"[^\x20-\x7EáéíóúÁÉÍÓÚñÑüÜ]", "", regex=True)
        .str.lower()
        .str.replace("á", "a").str.replace("é", "e")
        .str.replace("í", "i").str.replace("ó", "o")
        .str.replace("ú", "u").str.replace("ñ", "n")
    )

    expected = {"debitos", "creditos", "saldo", "fecha"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(
            f"Columnas faltantes en el extracto: {missing}\n"
            f"Columnas disponibles: {list(df.columns)}"
        )

    for col in ["debitos", "creditos", "saldo"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.replace(r"\s+", "", regex=True)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0.0)
        )

    def calcular_importe(row) -> float:
        creditos = row["creditos"]
        debitos  = row["debitos"]
        if creditos != 0 and debitos == 0:
            return creditos
        elif debitos != 0 and creditos == 0:
            return -debitos
        else:
            return 0.0

    df["importe"] = df.apply(calcular_importe, axis=1)

    df["fecha"] = pd.to_datetime(
        df["fecha"].astype(str).str.strip(),
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce"
    )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# CATEGORIZACIÓN EXTRACTO
# ─────────────────────────────────────────────────────────────────────────────

def categorizar_extracto_v1(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def limpiar(texto: str) -> str:
        if not isinstance(texto, str):
            return ""
        texto = texto.lower()
        texto = re.sub(r"[.\-\s]", "", texto)
        texto = re.sub(r"[^\x20-\x7EáéíóúñüÁÉÍÓÚÑÜ]", "", texto)
        return texto

    def contiene(texto_limpio: str, *palabras_clave: str) -> bool:
        return any(limpiar(p) in texto_limpio for p in palabras_clave)

    desc = df["descripcion"].apply(limpiar)

    condiciones = [
        desc.apply(lambda d: contiene(d,
            "debitocontracargoventa",
            "debitodevolucionventa",
            "devolucionpagocontransferencia",
            "naveventacontarjeta",
            "navepagocontransferencia",
        )),
        desc.apply(lambda d: contiene(d,
            "serviciopagoaProveedores",
            "serviciopagoproveedores",
        )),
        desc.apply(lambda d: contiene(d,
            "ajusteaportespromocionGalicia",
        ) or (
            "ajuste" in d and ("aporte" in d or "aportes" in d) and "promocion" in d
        )),
        desc.apply(lambda d: "echeq" in d),
        desc.apply(lambda d: contiene(d,
            "rescatefima",
            "suscripcionfima",
        )),
        desc.apply(lambda d: contiene(d,
            "iva",
            "comgestiontransffdosentrebcos",
            "comisionserviciodecuenta",
            "impcreley25413",
            "impdebley25413",
            "impingbrutos",
            "impuestodesellos",
            "interesessobresaldosdeudores",
            "perceiva",
            "ingbrutoss/cred",
            "comservinterbanking",
            "devolucioncomisionesportransfere",
        ) or (
            "imp" in d and ("iva" in d or "impuesto" in d or "impuestos" in d or "percep" in d)
        )),
        desc.apply(lambda d: contiene(d,
            "cobroiibb",
            "cuotadeprestamo",
            "debitoiibb",
        ) or (
            "prestamo" in d or "iibb" in d
        )),
        desc.apply(lambda d: contiene(d,
            "pagodeservicios",
            "trfinmedproveed",
            "ajustetransferenciaProveedores",
        )),
        desc.apply(lambda d: contiene(d,
            "transfinmedcp",
            "transfctaspropias",
            "transfercashmismatitularidad",
        )),
        desc.apply(lambda d: contiene(d,
            "pagovisaempresa",
        )),
        desc.apply(lambda d: contiene(d,
            "transfafip",
        )),
        desc.apply(lambda d: contiene(d,
            "propina",
        )),
        desc.apply(lambda d: contiene(d,
            "trfordenjudic",
        )),
    ]

    categorias = [
        "Acreditaciones",
        "Cobranzas",
        "Descuento Galicia",
        "Echeq",
        "FCI",
        "Gastos Bancarios",
        "Prestamo",
        "Proveedores",
        "Transf. entre cuentas",
        "Pago Tc. Corpo",
        "Imp. AFIP",
        "Propinas",
        "Transf. Judicial",
    ]

    df["conciliacion"] = "0"
    for condicion, categoria in zip(condiciones, categorias):
        df.loc[condicion & (df["conciliacion"] == "0"), "conciliacion"] = categoria

    return df


def categorizar_extracto_v2(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def limpiar(texto: str) -> str:
        if not isinstance(texto, str):
            return ""
        texto = texto.lower()
        texto = re.sub(r"[.\-\s]", "", texto)
        texto = re.sub(r"[^\x20-\x7EáéíóúñüÁÉÍÓÚÑÜ]", "", texto)
        return texto

    def limpiar_con_espacios(texto: str) -> str:
        if not isinstance(texto, str):
            return ""
        texto = texto.lower()
        texto = re.sub(r"[.\-]", "", texto)
        texto = re.sub(r"\s+", " ", texto)
        texto = re.sub(r"[^\x20-\x7EáéíóúñüÁÉÍÓÚÑÜ ]", "", texto)
        return texto.strip()

    ABREV_PALABRA_COMPLETA = ["sa", "srl", "sc", "sh"]
    ABREV_SUBCADENA = [
        "sas", "sca",
        "sociedadanonima",
        "sociedadderesponsabilidadlimitada",
        "ltda",
    ]

    def es_sociedad(texto_original: str) -> bool:
        texto_limpio  = limpiar(texto_original)
        texto_con_esp = limpiar_con_espacios(texto_original)
        palabras      = texto_con_esp.split()
        if any(p in palabras for p in ABREV_PALABRA_COMPLETA):
            return True
        if any(abrev in texto_limpio for abrev in ABREV_SUBCADENA):
            return True
        return False

    desc           = df["descripcion"].apply(limpiar)
    ley_ad1        = df["leyenda adicional1"]
    ley_ad1_limpio = df["leyenda adicional1"].apply(limpiar)
    ley_ad2_limpio = df["leyenda adicional2"].apply(limpiar)
    concepto_limp  = df["concepto"].apply(limpiar)

    sin_cat = df["conciliacion"] == "0"

    es_deb_autom = desc == limpiar("DEB. AUTOM. DE SERV.")
    es_seguro = ley_ad2_limpio.apply(lambda d: any(p in d for p in [
        "pagoseguro", "seguros", "segurosp", "pagopoliza", "seguro", "poliza"
    ]))
    es_afip = ley_ad1_limpio.apply(lambda d: "afip" in d)
    df.loc[sin_cat & es_deb_autom & es_seguro,             "conciliacion"] = "Seguros"
    df.loc[sin_cat & es_deb_autom & ~es_seguro & es_afip,  "conciliacion"] = "Imp. AFIP"
    df.loc[sin_cat & es_deb_autom & ~es_seguro & ~es_afip, "conciliacion"] = "Proveedores"
    sin_cat = df["conciliacion"] == "0"

    es_acred = desc == limpiar("SERVICIO ACREDITAMIENTO DE HABERES")
    es_sueldos       = ley_ad1_limpio.apply(lambda d: "acredhaberes" in d)
    es_rendiciones   = ley_ad1_limpio.apply(lambda d: "reintegroviaticos" in d)
    es_indemnizacion = ley_ad1_limpio.apply(lambda d: "indemnizaciones" in d)
    df.loc[sin_cat & es_acred & es_sueldos,       "conciliacion"] = "Sueldos"
    df.loc[sin_cat & es_acred & es_rendiciones,   "conciliacion"] = "Rendiciones"
    df.loc[sin_cat & es_acred & es_indemnizacion, "conciliacion"] = "Indemnizaciones"
    sin_cat = df["conciliacion"] == "0"

    es_transf_terceros = desc.apply(lambda d: d in [
        limpiar("TRANSFERENCIA A TERCEROS"),
        limpiar("TRANSF. A TERCEROS"),
    ])
    es_prov = ley_ad1.apply(es_sociedad)
    df.loc[sin_cat & es_transf_terceros & es_prov,  "conciliacion"] = "Proveedores"
    df.loc[sin_cat & es_transf_terceros & ~es_prov, "conciliacion"] = "Sueldos"
    sin_cat = df["conciliacion"] == "0"

    es_art = ley_ad1_limpio.apply(lambda d: any(p in d for p in [
        "prevencion", "previcion", "riesgo", "aseguradora",
    ]))
    df.loc[sin_cat & es_art, "conciliacion"] = "ART"
    sin_cat = df["conciliacion"] == "0"

    es_propia = concepto_limp.apply(lambda d: "propia" in d)
    df.loc[sin_cat & es_propia, "conciliacion"] = "Transf. entre cuentas"
    sin_cat = df["conciliacion"] == "0"

    es_transf_cash_prov = desc == limpiar("TRANSFERENCIAS CASH PROVEEDORES")
    es_rappi    = ley_ad1_limpio.apply(lambda d: "rappi" in d)
    es_delivery = ley_ad1_limpio.apply(lambda d: limpiar("delivery hero fi") in d)
    df.loc[sin_cat & es_transf_cash_prov & es_rappi,    "conciliacion"] = "Acred. Rappi"
    df.loc[sin_cat & es_transf_cash_prov & es_delivery, "conciliacion"] = "Acred. PY"
    sin_cat = df["conciliacion"] == "0"

    es_reintegro_promo = desc == limpiar("REINTEGRO PROMOCION GALICIA")
    df.loc[sin_cat & es_reintegro_promo, "conciliacion"] = "Descuento Galicia"
    sin_cat = df["conciliacion"] == "0"

    es_snp_prov = desc == limpiar("SNP PAGO A PROVEEDORES")
    df.loc[sin_cat & es_snp_prov, "conciliacion"] = "Proveedores"

    return df


# ─────────────────────────────────────────────────────────────────────────────
# CATEGORIZACIÓN MAYOR
# ─────────────────────────────────────────────────────────────────────────────

def categorizar_mayor_v1(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def limpiar(texto: str) -> str:
        if not isinstance(texto, str):
            return ""
        texto = texto.lower()
        texto = re.sub(r"[.\-\s]", "", texto)
        texto = re.sub(r"[^\x20-\x7EáéíóúñüÁÉÍÓÚÑÜ]", "", texto)
        texto = (texto
            .replace("á", "a").replace("é", "e")
            .replace("í", "i").replace("ó", "o")
            .replace("ú", "u").replace("ñ", "n")
            .replace("ü", "u")
        )
        return texto

    def contiene(texto_limpio: str, *palabras_clave: str) -> bool:
        return any(limpiar(p) in texto_limpio for p in palabras_clave)

    if "Comentario" not in df.columns:
        raise ValueError(
            f"Columna 'Comentario' no encontrada.\n"
            f"Columnas disponibles: {list(df.columns)}"
        )

    comentario     = df["Comentario"].apply(limpiar)
    comentario_raw = df["Comentario"].astype(str).str.strip()

    condiciones = [
        comentario.apply(lambda d: contiene(d, "acreditacion", "acreditaciones")),
        comentario.apply(lambda d: contiene(d, "cobronf")),
        comentario.apply(lambda d: contiene(d, "descuento", "descuentos")),
        comentario.apply(lambda d: contiene(d, "pagogb", "gb", "sircreb", "impdeb/cred")),
        comentario.apply(lambda d: contiene(d, "difcambio", "periodo")),
        comentario.apply(lambda d: contiene(d, "pagocuota")),
        comentario_raw.str.contains("ART", regex=False),
    ]

    categorias = [
        "Acreditaciones",
        "Cobranzas",
        "Descuento Galicia",
        "Gastos Bancarios",
        "Seguros",
        "Prestamo",
        "ART",
    ]

    df["conciliacion"] = "0"
    for condicion, categoria in zip(condiciones, categorias):
        df.loc[condicion & (df["conciliacion"] == "0"), "conciliacion"] = categoria

    return df


def categorizar_mayor_v2(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def limpiar(texto: str) -> str:
        if not isinstance(texto, str):
            return ""
        texto = texto.lower()
        texto = re.sub(r"[.\-\s]", "", texto)
        texto = re.sub(r"[^\x20-\x7EáéíóúñüÁÉÍÓÚÑÜ]", "", texto)
        return texto

    if "Tercero" not in df.columns:
        raise ValueError(
            f"Columna 'Tercero' no encontrada.\n"
            f"Columnas disponibles: {list(df.columns)}"
        )

    sin_cat = df["conciliacion"] == "0"
    tercero = df["Tercero"].apply(limpiar)

    es_seguro      = tercero.apply(lambda d: "seguro" in d or "swiss" in d)
    es_rendiciones = tercero.apply(lambda d: "gastos" in d or "rendicion" in d)

    df.loc[sin_cat & es_seguro,      "conciliacion"] = "Seguros"
    sin_cat = df["conciliacion"] == "0"
    df.loc[sin_cat & es_rendiciones, "conciliacion"] = "Rendiciones"

    return df


# ─────────────────────────────────────────────────────────────────────────────
# CRUCES
# ─────────────────────────────────────────────────────────────────────────────

def cruzar_mayor_extracto(
    df_mayor_dep: pd.DataFrame,
    df_extracto_dep: pd.DataFrame,
) -> tuple:
    mayor    = df_mayor_dep.copy().reset_index(drop=True)
    extracto = df_extracto_dep.copy().reset_index(drop=True)

    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_fecha_m   = get_col(mayor,    "Fecha", "fecha")
    col_importe_m = get_col(mayor,    "Importe", "importe")
    col_fecha_e   = get_col(extracto, "fecha", "Fecha")
    col_importe_e = get_col(extracto, "importe", "Importe")

    mayor["_key"]    = mayor[col_fecha_m].astype(str) + "|" + mayor[col_importe_m].astype(str)
    extracto["_key"] = extracto[col_fecha_e].astype(str) + "|" + extracto[col_importe_e].astype(str)
    mayor["_count"]    = mayor.groupby("_key").cumcount()
    extracto["_count"] = extracto.groupby("_key").cumcount()
    mayor["_idx_m"]    = mayor.index
    extracto["_idx_e"] = extracto.index

    merged = mayor[["_key", "_count", "_idx_m"]].merge(
        extracto[["_key", "_count", "_idx_e"]],
        on=["_key", "_count"],
        how="outer",
        indicator=True,
    )

    idx_match_m        = merged.loc[merged["_merge"] == "both",       "_idx_m"].dropna().astype(int).tolist()
    idx_match_e        = merged.loc[merged["_merge"] == "both",       "_idx_e"].dropna().astype(int).tolist()
    idx_falta_extracto = merged.loc[merged["_merge"] == "left_only",  "_idx_m"].dropna().astype(int).tolist()
    idx_falta_mayor    = merged.loc[merged["_merge"] == "right_only", "_idx_e"].dropna().astype(int).tolist()

    mayor    = mayor.drop(columns=["_key", "_count", "_idx_m"])
    extracto = extracto.drop(columns=["_key", "_count", "_idx_e"])

    match_mayor    = mayor.loc[idx_match_m].reset_index(drop=True)
    match_extracto = extracto.loc[idx_match_e].reset_index(drop=True)
    falta_extracto = mayor.loc[idx_falta_extracto].reset_index(drop=True)
    falta_mayor    = extracto.loc[idx_falta_mayor].reset_index(drop=True)

    return match_mayor, match_extracto, falta_extracto, falta_mayor


def cruzar_con_tolerancia(
    df_mayor_cat: pd.DataFrame,
    df_extracto_sin_acreditaciones: pd.DataFrame,
    tolerancia_importe: float = 0.5,
    tolerancia_dias: int = 3,
) -> tuple:
    mayor    = df_mayor_cat.copy().reset_index(drop=True)
    extracto = df_extracto_sin_acreditaciones.copy().reset_index(drop=True)

    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_fecha_m   = get_col(mayor,    "Fecha", "fecha")
    col_importe_m = get_col(mayor,    "Importe", "importe")
    col_fecha_e   = get_col(extracto, "fecha", "Fecha")
    col_importe_e = get_col(extracto, "importe", "Importe")

    mayor["_idx_m"]    = mayor.index
    extracto["_idx_e"] = extracto.index

    usado_mayor    = set()
    usado_extracto = set()
    match_idx_m        = []
    match_idx_e        = []
    match_idx_m_exacto = []
    match_idx_e_exacto = []

    delta = timedelta(days=tolerancia_dias)

    for idx_m, row_m in mayor.iterrows():
        if idx_m in usado_mayor:
            continue

        fecha_m   = row_m[col_fecha_m]
        importe_m = row_m[col_importe_m]

        candidatos = extracto[
            (extracto[col_fecha_e] >= fecha_m - delta) &
            (extracto[col_fecha_e] <= fecha_m + delta) &
            (~extracto.index.isin(usado_extracto))
        ].copy()

        candidatos["_dist_fecha"] = (candidatos[col_fecha_e] - fecha_m).abs()
        candidatos = candidatos.sort_values("_dist_fecha")

        for idx_e, row_e in candidatos.iterrows():
            importe_e = row_e[col_importe_e]
            if abs(importe_m - importe_e) <= tolerancia_importe:
                es_exacto = (
                    row_e[col_fecha_e] == fecha_m and
                    importe_e == importe_m
                )
                usado_mayor.add(idx_m)
                usado_extracto.add(idx_e)
                if es_exacto:
                    match_idx_m_exacto.append(idx_m)
                    match_idx_e_exacto.append(idx_e)
                else:
                    match_idx_m.append(idx_m)
                    match_idx_e.append(idx_e)
                break

    mayor    = mayor.drop(columns=["_idx_m"])
    extracto = extracto.drop(columns=["_idx_e"])

    match_mayor    = mayor.loc[match_idx_m].reset_index(drop=True)
    match_extracto = extracto.loc[match_idx_e].reset_index(drop=True)
    falta_extracto = mayor[~mayor.index.isin(usado_mayor)].reset_index(drop=True)
    falta_mayor    = extracto[~extracto.index.isin(usado_extracto)].reset_index(drop=True)

    return match_mayor, match_extracto, falta_extracto, falta_mayor


def cruzar_por_categoria(
    falta_extracto1: pd.DataFrame,
    falta_mayor1: pd.DataFrame,
    tolerancia: float = 0.5,
) -> tuple:
    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_importe_e = get_col(falta_extracto1, "Importe", "importe")
    col_importe_m = get_col(falta_mayor1,    "importe", "Importe")
    col_conc_e    = get_col(falta_extracto1, "conciliacion")
    col_conc_m    = get_col(falta_mayor1,    "conciliacion")

    fe = falta_extracto1.copy().reset_index(drop=True)
    fm = falta_mayor1.copy().reset_index(drop=True)

    usado_extracto = set()
    usado_mayor    = set()
    match_idx_e    = []
    match_idx_m    = []

    categorias = set(fe[col_conc_e].unique()) | set(fm[col_conc_m].unique())
    categorias.discard("0")

    for cat in categorias:
        grupo_e = fe[(fe[col_conc_e] == cat) & (~fe.index.isin(usado_extracto))]
        grupo_m = fm[(fm[col_conc_m] == cat) & (~fm.index.isin(usado_mayor))]

        if grupo_e.empty or grupo_m.empty:
            continue

        suma_e = grupo_e[col_importe_e].sum()
        suma_m = grupo_m[col_importe_m].sum()

        if abs(suma_e - suma_m) <= tolerancia:
            match_idx_e.extend(grupo_e.index.tolist())
            match_idx_m.extend(grupo_m.index.tolist())
            usado_extracto.update(grupo_e.index.tolist())
            usado_mayor.update(grupo_m.index.tolist())

    match_extracto2  = fe.loc[list(set(match_idx_e))].reset_index(drop=True)
    match_mayor2     = fm.loc[list(set(match_idx_m))].reset_index(drop=True)
    falta_extracto2  = fe[~fe.index.isin(usado_extracto)].reset_index(drop=True)
    falta_mayor2     = fm[~fm.index.isin(usado_mayor)].reset_index(drop=True)

    return match_extracto2, match_mayor2, falta_extracto2, falta_mayor2


def cruzar_proveedores(
    falta_extracto2: pd.DataFrame,
    falta_mayor2: pd.DataFrame,
    tolerancia: float = 0.5,
) -> tuple:
    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_fecha_e   = get_col(falta_extracto2, "Fecha", "fecha")
    col_importe_e = get_col(falta_extracto2, "Importe", "importe")
    col_conc_e    = get_col(falta_extracto2, "conciliacion")
    col_fecha_m   = get_col(falta_mayor2,    "fecha", "Fecha")
    col_importe_m = get_col(falta_mayor2,    "importe", "Importe")
    col_conc_m    = get_col(falta_mayor2,    "conciliacion")
    col_serie_e   = get_col(falta_extracto2, "Serie", "serie")

    fe = falta_extracto2.copy().reset_index(drop=True)
    fm = falta_mayor2.copy().reset_index(drop=True)

    usado_extracto = set()
    usado_mayor    = set()
    match_idx_e    = []
    match_idx_m    = []

    proveedores_m = fm[fm[col_conc_m] == "Proveedores"]
    fechas = proveedores_m[col_fecha_m].unique()

    for fecha in fechas:
        grupo_m = fm[
            (fm[col_fecha_m] == fecha) &
            (fm[col_conc_m] == "Proveedores") &
            (~fm.index.isin(usado_mayor))
        ]
        if grupo_m.empty:
            continue

        suma_m = grupo_m[col_importe_m].sum()

        grupo_e = fe[
            (fe[col_fecha_e] == fecha) &
            (fe[col_conc_e] == "0") &
            (~fe[col_serie_e].astype(str).str.strip().str.upper().str.contains("TP", na=False)) &
            (~fe.index.isin(usado_extracto))
        ]
        if grupo_e.empty:
            continue

        suma_e = grupo_e[col_importe_e].sum()

        if abs(suma_e - suma_m) <= tolerancia:
            match_idx_e.extend(grupo_e.index.tolist())
            match_idx_m.extend(grupo_m.index.tolist())
            usado_extracto.update(grupo_e.index.tolist())
            usado_mayor.update(grupo_m.index.tolist())

    match_extracto3  = fe.loc[list(set(match_idx_e))].reset_index(drop=True)
    match_mayor3     = fm.loc[list(set(match_idx_m))].reset_index(drop=True)
    falta_extracto3  = fe[~fe.index.isin(usado_extracto)].reset_index(drop=True)
    falta_mayor3     = fm[~fm.index.isin(usado_mayor)].reset_index(drop=True)

    return match_extracto3, match_mayor3, falta_extracto3, falta_mayor3


def cruzar_a1tp(
    falta_extracto3: pd.DataFrame,
    falta_mayor3: pd.DataFrame,
    tolerancia: float = 0.5,
) -> tuple:
    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_fecha_e   = get_col(falta_extracto3, "Fecha", "fecha")
    col_importe_e = get_col(falta_extracto3, "Importe", "importe")
    col_fecha_m   = get_col(falta_mayor3,    "fecha", "Fecha")
    col_importe_m = get_col(falta_mayor3,    "importe", "Importe")
    col_serie_e   = get_col(falta_extracto3, "Serie", "serie")
    col_conc_m    = get_col(falta_mayor3,    "conciliacion")

    CATEGORIAS_MAYOR = {"Sueldos", "Imp. AFIP", "Indemnizaciones"}

    fe = falta_extracto3.copy().reset_index(drop=True)
    fm = falta_mayor3.copy().reset_index(drop=True)

    usado_extracto = set()
    usado_mayor    = set()
    match_idx_e    = []
    match_idx_m    = []

    tp_e   = fe[fe[col_serie_e].astype(str).str.strip().str.upper().str.contains("TP", na=False)]
    fechas = tp_e[col_fecha_e].unique()

    for fecha in fechas:
        grupo_e = fe[
            (fe[col_fecha_e] == fecha) &
            (fe[col_serie_e].astype(str).str.strip().str.upper().str.contains("TP", na=False)) &
            (~fe.index.isin(usado_extracto))
        ]
        if grupo_e.empty:
            continue

        suma_e = grupo_e[col_importe_e].sum()

        grupo_m = fm[
            (fm[col_fecha_m] == fecha) &
            (fm[col_conc_m].isin(CATEGORIAS_MAYOR)) &
            (~fm.index.isin(usado_mayor))
        ]
        if grupo_m.empty:
            continue

        suma_m = grupo_m[col_importe_m].sum()

        if abs(suma_e - suma_m) <= tolerancia:
            match_idx_e.extend(grupo_e.index.tolist())
            match_idx_m.extend(grupo_m.index.tolist())
            usado_extracto.update(grupo_e.index.tolist())
            usado_mayor.update(grupo_m.index.tolist())

    match_extracto4  = fe.loc[list(set(match_idx_e))].reset_index(drop=True)
    match_mayor4     = fm.loc[list(set(match_idx_m))].reset_index(drop=True)
    falta_extracto4  = fe[~fe.index.isin(usado_extracto)].reset_index(drop=True)
    falta_mayor4     = fm[~fm.index.isin(usado_mayor)].reset_index(drop=True)

    return match_extracto4, match_mayor4, falta_extracto4, falta_mayor4


def cruzar_por_proveedor(
    falta_extracto4: pd.DataFrame,
    falta_mayor4: pd.DataFrame,
    tolerancia_importe: float = 0.5,
    top_candidatos: int = 4,
) -> tuple:
    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_importe_e = get_col(falta_extracto4, "Importe", "importe")
    col_importe_m = get_col(falta_mayor4,    "importe", "Importe")
    col_tercero_e = get_col(falta_extracto4, "Tercero", "tercero")
    col_leyenda_m = get_col(falta_mayor4,    "leyenda adicional1")
    col_conc_m    = get_col(falta_mayor4,    "conciliacion")

    fe = falta_extracto4.copy().reset_index(drop=True)

    fm = falta_mayor4[
        falta_mayor4[col_conc_m].isin(["Proveedores", "Seguros"])
    ].copy().reset_index(drop=True)

    usado_extracto   = set()
    usado_mayor      = set()
    match_idx_e      = []
    match_idx_m      = []
    nombres_usados_m = set()

    suma_por_tercero = fe.groupby(col_tercero_e)[col_importe_e].sum()
    suma_por_leyenda = fm.groupby(col_leyenda_m)[col_importe_m].sum()
    nombres_m        = suma_por_leyenda.index.tolist()

    for nombre_e, suma_e in suma_por_tercero.items():
        candidatos = process.extract(
            nombre_e,
            nombres_m,
            scorer=fuzz.token_sort_ratio,
            limit=top_candidatos,
        )

        if not candidatos:
            continue

        encontrado = False

        for nombre_m, score, _ in candidatos:
            if nombre_m in nombres_usados_m:
                continue
            suma_m = suma_por_leyenda[nombre_m]
            if abs(suma_e - suma_m) <= tolerancia_importe:
                idx_e = fe[(fe[col_tercero_e] == nombre_e) & (~fe.index.isin(usado_extracto))].index.tolist()
                idx_m = fm[(fm[col_leyenda_m] == nombre_m) & (~fm.index.isin(usado_mayor))].index.tolist()
                match_idx_e.extend(idx_e)
                match_idx_m.extend(idx_m)
                usado_extracto.update(idx_e)
                usado_mayor.update(idx_m)
                nombres_usados_m.add(nombre_m)
                encontrado = True
                break

        if not encontrado:
            for nombre_m, suma_m in suma_por_leyenda.items():
                if nombre_m in nombres_usados_m:
                    continue
                if abs(suma_e - suma_m) <= tolerancia_importe:
                    idx_e = fe[(fe[col_tercero_e] == nombre_e) & (~fe.index.isin(usado_extracto))].index.tolist()
                    idx_m = fm[(fm[col_leyenda_m] == nombre_m) & (~fm.index.isin(usado_mayor))].index.tolist()
                    match_idx_e.extend(idx_e)
                    match_idx_m.extend(idx_m)
                    usado_extracto.update(idx_e)
                    usado_mayor.update(idx_m)
                    nombres_usados_m.add(nombre_m)
                    encontrado = True
                    break

    match_extracto5  = fe.loc[list(set(match_idx_e))].reset_index(drop=True)
    match_mayor5     = fm.loc[list(set(match_idx_m))].reset_index(drop=True)
    falta_extracto5  = fe[~fe.index.isin(usado_extracto)].reset_index(drop=True)

    fm_no_considerado = falta_mayor4[
        ~falta_mayor4[col_conc_m].isin(["Proveedores", "Seguros"])
    ].copy()
    falta_mayor5 = pd.concat([
        fm[~fm.index.isin(usado_mayor)].reset_index(drop=True),
        fm_no_considerado
    ], ignore_index=True)

    return match_extracto5, match_mayor5, falta_extracto5, falta_mayor5


def cruzar_echeq(
    falta_extracto5: pd.DataFrame,
    falta_mayor5: pd.DataFrame,
    tolerancia: float = 0.5,
) -> tuple:
    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_fecha_e   = get_col(falta_extracto5, "Fecha", "fecha")
    col_importe_e = get_col(falta_extracto5, "Importe", "importe")
    col_serie_e   = get_col(falta_extracto5, "Serie", "serie")
    col_conc_e    = get_col(falta_extracto5, "conciliacion")
    col_fecha_m   = get_col(falta_mayor5,    "fecha", "Fecha")
    col_importe_m = get_col(falta_mayor5,    "importe", "Importe")
    col_conc_m    = get_col(falta_mayor5,    "conciliacion")

    fe = falta_extracto5.copy().reset_index(drop=True)
    fm = falta_mayor5.copy().reset_index(drop=True)

    usado_extracto = set()
    usado_mayor    = set()
    match_idx_e    = []
    match_idx_m    = []

    mask_e = (
        (fe[col_conc_e] == "0") &
        (fe[col_serie_e].isna() | fe[col_serie_e].astype(str).str.strip().eq("") | fe[col_serie_e].astype(str).str.strip().eq("nan"))
    )
    mask_m = fm[col_conc_m] == "Echeq"

    fechas = fe.loc[mask_e, col_fecha_e].unique()

    for fecha in fechas:
        grupo_e = fe[mask_e & (fe[col_fecha_e] == fecha) & (~fe.index.isin(usado_extracto))]
        grupo_m = fm[mask_m & (fm[col_fecha_m] == fecha) & (~fm.index.isin(usado_mayor))]

        if grupo_e.empty or grupo_m.empty:
            continue

        suma_e = grupo_e[col_importe_e].sum()
        suma_m = grupo_m[col_importe_m].sum()

        if abs(suma_e - suma_m) <= tolerancia:
            match_idx_e.extend(grupo_e.index.tolist())
            match_idx_m.extend(grupo_m.index.tolist())
            usado_extracto.update(grupo_e.index.tolist())
            usado_mayor.update(grupo_m.index.tolist())

    match_extracto6  = fe.loc[list(set(match_idx_e))].reset_index(drop=True)
    match_mayor6     = fm.loc[list(set(match_idx_m))].reset_index(drop=True)
    falta_extracto6  = fe[~fe.index.isin(usado_extracto)].reset_index(drop=True)
    falta_mayor6     = fm[~fm.index.isin(usado_mayor)].reset_index(drop=True)

    return match_extracto6, match_mayor6, falta_extracto6, falta_mayor6


def limpiar_proveedores(
    df_proveedores: pd.DataFrame,
    match_mayor: pd.DataFrame,
    match_mayor1: pd.DataFrame,
    match_extracto3: pd.DataFrame,
    match_extracto5: pd.DataFrame,
    tolerancia_importe: float = 0.5,
) -> pd.DataFrame:
    def find_col(df, keyword):
        keyword_lower = keyword.lower()
        for c in df.columns:
            if keyword_lower in c.lower():
                return c
        raise KeyError(f"Columna con '{keyword}' no encontrada en {list(df.columns)}")

    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_monto        = find_col(df_proveedores, "monto")
    col_estado       = find_col(df_proveedores, "estado")
    col_fecha_emis_p = next((c for c in df_proveedores.columns if "emis" in c.lower()), None)
    if col_fecha_emis_p is None:
        raise KeyError(f"Columna de fecha de emisión no encontrada en {list(df_proveedores.columns)}")

    fp = df_proveedores[
        ~df_proveedores[col_estado].astype(str).str.upper().str.contains("ERROR", na=False)
    ].copy().reset_index(drop=True)

    fp[col_monto] = (
        fp[col_monto]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .str.strip()
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0.0)
    )

    fp[col_fecha_emis_p] = pd.to_datetime(fp[col_fecha_emis_p], errors="coerce")

    def cruzar_y_sacar(fp, match, tolerancia):
        col_fecha_m   = get_col(match, "Fecha", "fecha")
        col_importe_m = get_col(match, "Importe", "importe")

        mm = match.copy()
        mm[col_fecha_m]   = pd.to_datetime(mm[col_fecha_m], errors="coerce")
        mm[col_importe_m] = pd.to_numeric(mm[col_importe_m], errors="coerce").fillna(0.0)

        idx_usados = set()
        for idx_p, row_p in fp.iterrows():
            fecha_p = row_p[col_fecha_emis_p]
            monto_p = row_p[col_monto] * -1

            coincide = mm[
                (mm[col_fecha_m] == fecha_p) &
                (mm[col_importe_m].apply(lambda x: abs(x - monto_p) <= tolerancia))
            ]
            if not coincide.empty:
                idx_usados.add(idx_p)

        eliminados = len(idx_usados)
        fp = fp[~fp.index.isin(idx_usados)].reset_index(drop=True)
        return fp, eliminados

    fp, _ = cruzar_y_sacar(fp, match_mayor,     tolerancia=0.0)
    fp, _ = cruzar_y_sacar(fp, match_mayor1,    tolerancia=tolerancia_importe)
    fp, _ = cruzar_y_sacar(fp, match_extracto3, tolerancia=tolerancia_importe)
    fp, _ = cruzar_y_sacar(fp, match_extracto5, tolerancia=tolerancia_importe)

    return fp


def cruzar_proveedores_descarga(
    falta_extracto6: pd.DataFrame,
    falta_mayor6: pd.DataFrame,
    ejecutar: bool = True,
    df_proveedores_def=None,
    tolerancia_importe: float = 0.5,
    top_candidatos: int = 3,
) -> tuple:
    if not ejecutar or df_proveedores_def is None or df_proveedores_def.empty:
        return (
            pd.DataFrame(columns=falta_extracto6.columns),
            pd.DataFrame(columns=falta_mayor6.columns),
            falta_extracto6.copy(),
            falta_mayor6.copy(),
        )

    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    def find_col(df, keyword):
        keyword_lower = keyword.lower()
        for c in df.columns:
            if keyword_lower in c.lower():
                return c
        raise KeyError(f"Columna con '{keyword}' no encontrada en {list(df.columns)}")

    col_tercero_e  = get_col(falta_extracto6, "Tercero", "tercero")
    col_importe_e  = get_col(falta_extracto6, "Importe", "importe")
    col_fecha_e    = get_col(falta_extracto6, "Fecha",   "fecha")
    col_fecha_m    = get_col(falta_mayor6,    "fecha",   "Fecha")
    col_importe_m  = get_col(falta_mayor6,    "importe", "Importe")
    col_conc_m     = get_col(falta_mayor6,    "conciliacion")
    col_razon      = next((c for c in df_proveedores_def.columns if "raz" in c.lower()), None)
    if col_razon is None:
        raise KeyError(f"Columna razón social no encontrada en {list(df_proveedores_def.columns)}")
    col_monto   = find_col(df_proveedores_def, "monto")
    col_fecha_p = next((c for c in df_proveedores_def.columns if "emis" in c.lower()), None)
    if col_fecha_p is None:
        raise KeyError(f"Columna fecha emisión no encontrada en {list(df_proveedores_def.columns)}")

    fe = falta_extracto6.copy().reset_index(drop=True)
    fm = falta_mayor6.copy().reset_index(drop=True)
    fp = df_proveedores_def.copy().reset_index(drop=True)

    fp[col_fecha_p] = pd.to_datetime(fp[col_fecha_p], errors="coerce")

    fp_monto_neg = fp.copy()
    fp_monto_neg[col_monto] = fp_monto_neg[col_monto] * -1

    suma_por_tercero_fecha = fe.groupby([col_tercero_e, col_fecha_e])[col_importe_e].sum()
    suma_por_razon_fecha   = fp_monto_neg.groupby([col_razon, col_fecha_p])[col_monto].sum()
    nombres_p = fp_monto_neg[col_razon].unique().tolist()

    usado_extracto     = set()
    nombres_matcheados = set()
    fechas_matcheadas  = set()
    match_idx_e        = []

    for (nombre_e, fecha_e), suma_e in suma_por_tercero_fecha.items():
        candidatos = process.extract(
            nombre_e,
            nombres_p,
            scorer=fuzz.token_sort_ratio,
            limit=top_candidatos,
        )

        if not candidatos:
            continue

        encontrado = False

        for nombre_p, score, _ in candidatos:
            if (nombre_p, fecha_e) not in suma_por_razon_fecha.index:
                continue
            suma_p = suma_por_razon_fecha[(nombre_p, fecha_e)]
            if abs(suma_e - suma_p) <= tolerancia_importe:
                idx_e = fe[
                    (fe[col_tercero_e] == nombre_e) &
                    (fe[col_fecha_e] == fecha_e) &
                    (~fe.index.isin(usado_extracto))
                ].index.tolist()
                fechas_matcheadas.add(fecha_e)
                match_idx_e.extend(idx_e)
                usado_extracto.update(idx_e)
                nombres_matcheados.add(nombre_p)
                encontrado = True
                break

    nombres_no_matcheados = set(nombres_p) - nombres_matcheados

    match_extracto7 = fe.loc[list(set(match_idx_e))].reset_index(drop=True)

    suma_matcheada_por_fecha = {}
    for fecha in fechas_matcheadas:
        idx_fecha = [i for i in match_idx_e if fe.loc[i, col_fecha_e] == fecha]
        suma_matcheada_por_fecha[fecha] = fe.loc[idx_fecha, col_importe_e].sum() if idx_fecha else 0

    filas_match_mayor = []
    for fecha in fechas_matcheadas:
        suma_importe = suma_matcheada_por_fecha[fecha]
        fila = {col: "" for col in fm.columns}
        if col_fecha_m          in fila: fila[col_fecha_m]          = fecha
        if "descripcion"        in fila: fila["descripcion"]         = "TRF INMED PROVEED"
        if "leyenda adicional1" in fila: fila["leyenda adicional1"]  = "TRF INMED PROVEED"
        if "leyenda adicional2" in fila: fila["leyenda adicional2"]  = "TRF INMED PROVEED"
        if "leyenda adicional3" in fila: fila["leyenda adicional3"]  = "TRF INMED PROVEED"
        if col_importe_m        in fila: fila[col_importe_m]         = suma_importe
        if col_conc_m           in fila: fila[col_conc_m]            = "Proveedores"
        filas_match_mayor.append(fila)

    match_mayor7 = pd.DataFrame(filas_match_mayor) if filas_match_mayor else pd.DataFrame(columns=fm.columns)

    concepto_limpio = fm.get(
        "concepto", pd.Series([""] * len(fm))
    ).astype(str).str.replace(r"[.\-\s]", "", regex=True).str.upper()

    mask_trf = (
        fm[col_fecha_m].isin(fechas_matcheadas) &
        concepto_limpio.str.contains("TRFINMEDPROVEED", na=False)
    )

    usado_mayor = set(fm[mask_trf].index.tolist())
    falta_mayor7_base = fm[~fm.index.isin(usado_mayor)].reset_index(drop=True)

    filas_nuevas = []
    for nombre_p in nombres_no_matcheados:
        filas_p = fp[fp[col_razon] == nombre_p]
        for _, row in filas_p.iterrows():
            monto = row[col_monto]
            fecha = row[col_fecha_p]
            fila  = {col: "" for col in fm.columns}
            if col_fecha_m          in fila: fila[col_fecha_m]          = fecha
            if "descripcion"        in fila: fila["descripcion"]         = "TRF INMED PROVEED"
            if "debitos"            in fila: fila["debitos"]             = monto
            if "concepto"           in fila: fila["concepto"]            = "TRF INMED PROVEED"
            if "leyenda adicional1" in fila: fila["leyenda adicional1"]  = row[col_razon]
            if col_importe_m        in fila: fila[col_importe_m]         = -abs(monto)
            if col_conc_m           in fila: fila[col_conc_m]            = "Proveedores"
            filas_nuevas.append(fila)

    if filas_nuevas:
        falta_mayor7 = pd.concat([
            falta_mayor7_base,
            pd.DataFrame(filas_nuevas)
        ], ignore_index=True)
    else:
        falta_mayor7 = falta_mayor7_base

    falta_extracto7 = fe[~fe.index.isin(usado_extracto)].reset_index(drop=True)

    return match_extracto7, match_mayor7, falta_extracto7, falta_mayor7


def cruzar_acreditaciones(
    df_extracto_cat2: pd.DataFrame,
    falta_extracto7: pd.DataFrame,
    falta_mayor7: pd.DataFrame,
    tolerancia: float = 0.5,
) -> tuple:
    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    def limpiar(texto: str) -> str:
        if not isinstance(texto, str):
            return ""
        texto = texto.lower()
        texto = re.sub(r"[.\-\s]", "", texto)
        texto = re.sub(r"[^\x20-\x7EáéíóúñüÁÉÍÓÚÑÜ]", "", texto)
        texto = (texto
            .replace("á", "a").replace("é", "e")
            .replace("í", "i").replace("ó", "o")
            .replace("ú", "u").replace("ñ", "n")
            .replace("ü", "u")
        )
        return texto

    falta_extracto7 = falta_extracto7.copy()

    col_conc_falt = get_col(falta_extracto7, "conciliacion")
    col_com_falt  = get_col(falta_extracto7, "Comentario", "comentario")

    comentario_limp = falta_extracto7[col_com_falt].apply(limpiar)

    mask_acred = falta_extracto7[col_conc_falt] == "Acreditaciones"
    mask_py    = comentario_limp.apply(lambda d: "acreditacionpy" in d)
    mask_rappi = comentario_limp.apply(lambda d: "acreditacionesrappi" in d or "acreditacionrappi" in d)

    falta_extracto7.loc[mask_acred & mask_py,    col_conc_falt] = "Acred. PY"
    falta_extracto7.loc[mask_acred & mask_rappi, col_conc_falt] = "Acred. Rappi"

    col_importe_cat  = get_col(df_extracto_cat2, "importe", "Importe")
    col_conc_cat     = get_col(df_extracto_cat2, "conciliacion")
    col_importe_falt = get_col(falta_extracto7,  "Importe", "importe")

    CATEGORIAS = ["Acreditaciones", "Acred. PY", "Acred. Rappi"]

    match_cat_idx  = []
    match_falt_idx = []
    filas_resumen  = []

    for cat in CATEGORIAS:
        acred_cat  = df_extracto_cat2[df_extracto_cat2[col_conc_cat] == cat]
        acred_falt = falta_extracto7[falta_extracto7[col_conc_falt] == cat]

        if acred_cat.empty and acred_falt.empty:
            continue

        suma_cat  = acred_cat[col_importe_cat].sum()
        suma_falt = acred_falt[col_importe_falt].sum()

        if abs(suma_cat - suma_falt) <= tolerancia:
            match_cat_idx.extend(acred_cat.index.tolist())
            match_falt_idx.extend(acred_falt.index.tolist())
        else:
            if suma_cat != 0:
                col_importe_m = get_col(falta_mayor7, "importe", "Importe")
                col_desc_m    = get_col(falta_mayor7, "descripcion", "Descripcion", "descripción")
                col_conc_m    = get_col(falta_mayor7, "conciliacion")
                fila_resumen  = {col: "" for col in falta_mayor7.columns}
                fila_resumen[col_importe_m] = suma_cat
                fila_resumen[col_desc_m]    = f"{cat} sumadas"
                fila_resumen[col_conc_m]    = cat
                filas_resumen.append(fila_resumen)

    match_acreditaciones_cat = df_extracto_cat2.loc[list(set(match_cat_idx))].reset_index(drop=True)
    match_acreditaciones     = falta_extracto7.loc[list(set(match_falt_idx))].reset_index(drop=True)
    falta_extracto8          = falta_extracto7[~falta_extracto7.index.isin(match_falt_idx)].reset_index(drop=True)

    if filas_resumen:
        falta_mayor8 = pd.concat([
            falta_mayor7,
            pd.DataFrame(filas_resumen)
        ], ignore_index=True)
    else:
        falta_mayor8 = falta_mayor7.copy().reset_index(drop=True)

    return match_acreditaciones_cat, match_acreditaciones, falta_extracto8, falta_mayor8


# ─────────────────────────────────────────────────────────────────────────────
# ASIGNACIÓN DE IDs
# ─────────────────────────────────────────────────────────────────────────────

def asignar_id_match(
    match_mayor: pd.DataFrame,
    match_extracto: pd.DataFrame,
) -> tuple:
    df_match_mayor    = match_mayor.copy().reset_index(drop=True)
    df_match_extracto = match_extracto.copy().reset_index(drop=True)

    ids = list(range(1, len(df_match_mayor) + 1))

    df_match_mayor["match_id"]    = ids
    df_match_mayor["match_tipo"]  = "0"
    df_match_extracto["match_id"]   = ids
    df_match_extracto["match_tipo"] = "0"

    return df_match_mayor, df_match_extracto


def asignar_id_match1(
    match_mayor1: pd.DataFrame,
    match_extracto1: pd.DataFrame,
    id_inicio: int,
) -> tuple:
    df_match_mayor1    = match_mayor1.copy().reset_index(drop=True)
    df_match_extracto1 = match_extracto1.copy().reset_index(drop=True)

    ids_m = list(range(id_inicio, id_inicio + len(df_match_mayor1)))
    ids_e = list(range(id_inicio, id_inicio + len(df_match_extracto1)))

    df_match_mayor1["match_id"]    = ids_m
    df_match_mayor1["match_tipo"]  = "1"
    df_match_extracto1["match_id"]   = ids_e
    df_match_extracto1["match_tipo"] = "1"

    return df_match_mayor1, df_match_extracto1


def asignar_id_match_categoria(
    match_extracto2: pd.DataFrame,
    match_mayor2: pd.DataFrame,
    id_inicio: int,
) -> tuple:
    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_conc_e = get_col(match_extracto2, "conciliacion")
    col_conc_m = get_col(match_mayor2,    "conciliacion")

    df_match_extracto2 = match_extracto2.copy().reset_index(drop=True)
    df_match_mayor2    = match_mayor2.copy().reset_index(drop=True)

    current_id = id_inicio
    cat_to_id = {}
    for cat in df_match_extracto2[col_conc_e].unique():
        cat_to_id[cat] = current_id
        current_id += 1
    for cat in df_match_mayor2[col_conc_m].unique():
        if cat not in cat_to_id:
            cat_to_id[cat] = current_id
            current_id += 1

    df_match_extracto2["match_id"]   = df_match_extracto2[col_conc_e].map(cat_to_id)
    df_match_extracto2["match_tipo"] = "2"
    df_match_mayor2["match_id"]      = df_match_mayor2[col_conc_m].map(cat_to_id)
    df_match_mayor2["match_tipo"]    = "2"

    return df_match_extracto2, df_match_mayor2, current_id


def asignar_id_match_fecha(
    match_extracto: pd.DataFrame,
    match_mayor: pd.DataFrame,
    id_inicio: int,
    match_tipo: str,
) -> tuple:
    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_fecha_e = get_col(match_extracto, "Fecha", "fecha")
    col_fecha_m = get_col(match_mayor,    "fecha", "Fecha")

    dm = match_extracto.copy().reset_index(drop=True)
    de = match_mayor.copy().reset_index(drop=True)

    current_id  = id_inicio
    fecha_to_id = {}
    for fecha in list(dm[col_fecha_e].unique()) + list(de[col_fecha_m].unique()):
        if fecha not in fecha_to_id:
            fecha_to_id[fecha] = current_id
            current_id += 1

    dm["match_id"]   = dm[col_fecha_e].map(fecha_to_id)
    dm["match_tipo"] = match_tipo
    de["match_id"]   = de[col_fecha_m].map(fecha_to_id)
    de["match_tipo"] = match_tipo

    return dm, de, current_id


def asignar_id_match5(
    match_extracto5: pd.DataFrame,
    match_mayor5: pd.DataFrame,
    id_inicio: int,
    tolerancia_importe: float = 0.5,
    top_candidatos: int = 4,
) -> tuple:
    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_tercero_e = get_col(match_extracto5, "Tercero", "tercero")
    col_leyenda_m = get_col(match_mayor5,    "leyenda adicional1")
    col_importe_e = get_col(match_extracto5, "Importe", "importe")
    col_importe_m = get_col(match_mayor5,    "importe", "Importe")

    df_match_extracto5 = match_extracto5.copy().reset_index(drop=True)
    df_match_mayor5    = match_mayor5.copy().reset_index(drop=True)

    current_id = id_inicio

    nombres_e = df_match_extracto5[col_tercero_e].unique().tolist()
    nombres_m = df_match_mayor5[col_leyenda_m].unique().tolist()

    suma_por_tercero = df_match_extracto5.groupby(col_tercero_e)[col_importe_e].sum()
    suma_por_leyenda = df_match_mayor5.groupby(col_leyenda_m)[col_importe_m].sum()

    tercero_to_id    = {}
    leyenda_to_id    = {}
    nombres_m_usados = set()

    for nombre_e in nombres_e:
        suma_e = suma_por_tercero.get(nombre_e, 0)
        tercero_to_id[nombre_e] = current_id

        candidatos = process.extract(
            nombre_e,
            [n for n in nombres_m if n not in nombres_m_usados],
            scorer=fuzz.token_sort_ratio,
            limit=top_candidatos,
        )

        encontrado = False
        for nombre_m, score, _ in candidatos:
            suma_m = suma_por_leyenda.get(nombre_m, 0)
            if abs(suma_e - suma_m) <= tolerancia_importe:
                leyenda_to_id[nombre_m] = current_id
                nombres_m_usados.add(nombre_m)
                encontrado = True
                break

        current_id += 1

    df_match_extracto5["match_id"]   = df_match_extracto5[col_tercero_e].map(tercero_to_id)
    df_match_extracto5["match_tipo"] = "5"
    df_match_mayor5["match_id"]      = df_match_mayor5[col_leyenda_m].map(leyenda_to_id)
    df_match_mayor5["match_tipo"]    = "5"

    return df_match_extracto5, df_match_mayor5, current_id


def asignar_id_match8(
    match_acreditaciones: pd.DataFrame,
    match_acreditaciones_cat: pd.DataFrame,
    id_inicio: int,
) -> tuple:
    df_match_acreditaciones     = match_acreditaciones.copy().reset_index(drop=True)
    df_match_acreditaciones_cat = match_acreditaciones_cat.copy().reset_index(drop=True)

    df_match_acreditaciones["match_id"]       = id_inicio
    df_match_acreditaciones["match_tipo"]     = "8"
    df_match_acreditaciones_cat["match_id"]   = id_inicio
    df_match_acreditaciones_cat["match_tipo"] = "8"

    return df_match_acreditaciones, df_match_acreditaciones_cat, id_inicio + 1


# ─────────────────────────────────────────────────────────────────────────────
# EXPORTAR EN MEMORIA (5 hojas)
# ─────────────────────────────────────────────────────────────────────────────

def generar_excel_en_memoria_galicia(
    falta_mayor8: pd.DataFrame,
    falta_extracto8: pd.DataFrame,
    match_mayor_def: pd.DataFrame,
    match_extracto_def: pd.DataFrame,
    df_extracto_cat2: pd.DataFrame,
) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        falta_mayor8.to_excel(writer,       sheet_name="Faltante Mayor",    index=False)
        falta_extracto8.to_excel(writer,    sheet_name="Faltante Extracto", index=False)
        match_mayor_def.to_excel(writer,    sheet_name="Match Mayor",       index=False)
        match_extracto_def.to_excel(writer, sheet_name="Match Extracto",    index=False)
        df_extracto_cat2.to_excel(writer,   sheet_name="Extracto_cat",      index=False)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE — entrada para la app Streamlit
# ─────────────────────────────────────────────────────────────────────────────

def correr_conciliacion_galicia(archivo_mayor, archivo_extracto, archivo_proveedores=None):
    """
    archivo_proveedores es opcional.
    Si es None se saltea limpiar_proveedores y cruzar_proveedores_descarga.
    """
    # 1. Cargar y normalizar
    df_mayor_dep    = normalize_mayor(load_excel_file(archivo_mayor))
    df_extracto_dep = normalize_extracto_galicia(load_excel_file(archivo_extracto))

    # 2. Categorizar
    df_extracto_cat  = categorizar_extracto_v1(df_extracto_dep)
    df_extracto_cat2 = categorizar_extracto_v2(df_extracto_cat)
    df_extracto_sin_acreditaciones = df_extracto_cat2[
        ~df_extracto_cat2["conciliacion"].isin(["Acreditaciones", "Acred. PY", "Acred. Rappi"])
    ].reset_index(drop=True)

    df_mayor_cat = categorizar_mayor_v1(df_mayor_dep)
    df_mayor_cat = categorizar_mayor_v2(df_mayor_cat)

    # 3. Cruces
    match_mayor,  match_extracto,  falta_extracto,  falta_mayor  = cruzar_mayor_extracto(df_mayor_cat, df_extracto_sin_acreditaciones)
    match_mayor1, match_extracto1, falta_extracto1, falta_mayor1 = cruzar_con_tolerancia(df_mayor_cat, df_extracto_sin_acreditaciones)
    match_extracto2, match_mayor2, falta_extracto2, falta_mayor2 = cruzar_por_categoria(falta_extracto1, falta_mayor1)
    match_extracto3, match_mayor3, falta_extracto3, falta_mayor3 = cruzar_proveedores(falta_extracto2, falta_mayor2)
    match_extracto4, match_mayor4, falta_extracto4, falta_mayor4 = cruzar_a1tp(falta_extracto3, falta_mayor3)
    match_extracto5, match_mayor5, falta_extracto5, falta_mayor5 = cruzar_por_proveedor(falta_extracto4, falta_mayor4)
    match_extracto6, match_mayor6, falta_extracto6, falta_mayor6 = cruzar_echeq(falta_extracto5, falta_mayor5)

    # 4. Cruce 7 (opcional)
    if archivo_proveedores is not None:
        df_proveedores     = load_excel_file(archivo_proveedores)
        df_proveedores_def = limpiar_proveedores(
            df_proveedores, match_mayor, match_mayor1, match_extracto3, match_extracto5)
        match_extracto7, match_mayor7, falta_extracto7, falta_mayor7 = cruzar_proveedores_descarga(
            falta_extracto6, falta_mayor6, ejecutar=True, df_proveedores_def=df_proveedores_def)
    else:
        match_extracto7 = pd.DataFrame(columns=falta_extracto6.columns)
        match_mayor7    = pd.DataFrame(columns=falta_mayor6.columns)
        falta_extracto7 = falta_extracto6.copy()
        falta_mayor7    = falta_mayor6.copy()

    # 5. Cruce 8
    match_acreditaciones_cat, match_acreditaciones, falta_extracto8, falta_mayor8 = cruzar_acreditaciones(
        df_extracto_cat2, falta_extracto7, falta_mayor7)

    # 6. Asignar IDs
    df_match_mayor,  df_match_extracto  = asignar_id_match(match_mayor, match_extracto)
    id2 = len(df_match_mayor) + 1
    df_match_mayor1, df_match_extracto1 = asignar_id_match1(match_mayor1, match_extracto1, id2)
    id3 = id2 + len(df_match_mayor1)
    df_match_extracto2, df_match_mayor2, id4 = asignar_id_match_categoria(match_extracto2, match_mayor2, id3)
    df_match_extracto3, df_match_mayor3, id5 = asignar_id_match_fecha(match_extracto3, match_mayor3, id4, "3")
    df_match_extracto4, df_match_mayor4, id6 = asignar_id_match_fecha(match_extracto4, match_mayor4, id5, "4")
    df_match_extracto5, df_match_mayor5, id7 = asignar_id_match5(match_extracto5, match_mayor5, id6)
    df_match_extracto6, df_match_mayor6, id8 = asignar_id_match_fecha(match_extracto6, match_mayor6, id7, "6")
    df_match_extracto7, df_match_mayor7, id9 = asignar_id_match_fecha(match_extracto7, match_mayor7, id8, "7")
    df_match_acreditaciones, df_match_acreditaciones_cat, _ = asignar_id_match8(
        match_acreditaciones, match_acreditaciones_cat, id9)

    # 7. Consolidar (siguiendo generar_match_definitivo del notebook)
    def _safe_concat(dfs):
        dfs = [d for d in dfs if d is not None and not d.empty]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    match_mayor_def = _safe_concat([
        df_match_mayor,
        df_match_mayor1,
        df_match_extracto2,
        df_match_extracto3,
        df_match_extracto4,
        df_match_extracto5,
        df_match_extracto6,
        df_match_extracto7,
        df_match_acreditaciones,
    ])

    match_extracto_def = _safe_concat([
        df_match_extracto,
        df_match_extracto1,
        df_match_mayor2,
        df_match_mayor3,
        df_match_mayor4,
        df_match_mayor5,
        df_match_mayor6,
        df_match_mayor7,
        df_match_acreditaciones_cat,
    ])

    stats = {
        "match_exacto":     len(match_mayor),
        "match_tolerancia": len(match_mayor1),
        "falta_mayor":      len(falta_mayor8),
        "falta_extracto":   len(falta_extracto8),
        "con_masivos":      archivo_proveedores is not None,
    }

    buf = generar_excel_en_memoria_galicia(
        falta_mayor8, falta_extracto8, match_mayor_def, match_extracto_def, df_extracto_cat2)
    return buf, stats
