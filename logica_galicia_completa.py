"""
logica_galicia_completa.py — Conciliación completa Banco Galicia

Versión nueva, en prueba. Convive con logica_galicia.py (cruce original),
que no se modifica hasta validar este módulo.

Contiene todo el proceso:
  1. Cruce mayor contra extracto (match_tipo 0 a 8, del notebook original, y
     9 a 19 sobre los faltantes).
  2. Conciliación completa (hoja CC) a partir del cruce y de la conciliación
     anterior: pendientes anteriores, control de saldos y asientos tocados con
     fecha ya conciliada.

Entrada para la app: correr_conciliacion_galicia(archivo_mayor, archivo_extracto,
                     archivo_proveedores=None, archivo_conciliacion_anterior=None, ...)
"""

import re
import itertools
from io import BytesIO
from datetime import date, timedelta

import numpy as np
import pandas as pd
from rapidfuzz import process, fuzz
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter


# ─────────────────────────────────────────────────────────────────────────────
# CARGA
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
            "navepagocontarjeta",
            "navedevolución",
        )),
        desc.apply(lambda d: contiene(d,
            "serviciopagoaProveedores",
            "serviciopagoproveedores",
            "g.deecheq",
            "factura",
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
        desc.apply(lambda d: contiene(d,
            "depefvoautoservicio",
        )),
        desc.apply(lambda d: contiene(d,
            "acreditamientoprismacomercio",
            "transferenciapei",
            "devolucionpei",
            "anulpagocomercio",
        )),
    ]

    categorias = [
        "Acred. Nave",
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
        "Deposito",
        "Acred. Prisma",
    ]

    df["conciliacion"] = "0"
    for condicion, categoria in zip(condiciones, categorias):
        df.loc[condicion & (df["conciliacion"] == "0"), "conciliacion"] = categoria

    print(df["conciliacion"].value_counts())
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
    es_amex     = ley_ad1_limpio.apply(lambda d: limpiar("american express") in d)
    df.loc[sin_cat & es_transf_cash_prov & es_rappi,    "conciliacion"] = "Acred. Rappi"
    df.loc[sin_cat & es_transf_cash_prov & es_delivery, "conciliacion"] = "Acred. PY"
    df.loc[sin_cat & es_transf_cash_prov & es_amex,     "conciliacion"] = "Acred. AmEx"
    sin_cat = df["conciliacion"] == "0"

    es_reintegro_promo = desc == limpiar("REINTEGRO PROMOCION GALICIA")
    df.loc[sin_cat & es_reintegro_promo, "conciliacion"] = "Descuento Galicia"
    sin_cat = df["conciliacion"] == "0"

    es_snp_prov = desc == limpiar("SNP PAGO A PROVEEDORES")
    df.loc[sin_cat & es_snp_prov, "conciliacion"] = "Proveedores"
    sin_cat = df["conciliacion"] == "0"

    es_sueldo_ley = ley_ad1_limpio.apply(lambda d: "sueldo" in d)
    df.loc[sin_cat & es_sueldo_ley, "conciliacion"] = "Sueldos"

    print(df["conciliacion"].value_counts())
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
        # Acred. Nave  → comentario contiene "acrednave"
        comentario.apply(lambda d: "acrednave" in d),
        # Acred. PY    → comentario contiene "acredpy"
        comentario.apply(lambda d: "acredpy" in d),
        # Acred. Rappi → comentario contiene "acredrappi"
        comentario.apply(lambda d: "acredrappi" in d),
        # Acred. Prisma → comentario contiene "acredprisma"
        comentario.apply(lambda d: "acredprisma" in d),
        # Acred. AmEx  → comentario contiene "acredamex"
        comentario.apply(lambda d: "acredamex" in d),
        comentario.apply(lambda d: contiene(d, "cobronf")),
        comentario.apply(lambda d: contiene(d, "descuento", "descuentos")),
        comentario.apply(lambda d: contiene(d, "pagogb", "gb", "sircreb", "impdeb/cred")),
        comentario.apply(lambda d: contiene(d, "difcambio", "periodo")),
        comentario.apply(lambda d: contiene(d, "pagocuota")),
        comentario_raw.str.contains("ART", regex=False),
    ]

    categorias = [
        "Acred. Nave",
        "Acred. PY",
        "Acred. Rappi",
        "Acred. Prisma",
        "Acred. AmEx",
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

def cruzar_mayor_extracto(df_mayor_dep, df_extracto_dep):
    mayor    = df_mayor_dep.copy().reset_index(drop=True)
    extracto = df_extracto_dep.copy().reset_index(drop=True)

    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_fecha_m   = get_col(mayor,    "Fecha", "fecha")
    col_importe_m = get_col(mayor,    "Importe", "importe")
    col_tercero_m = get_col(mayor,    "Tercero", "tercero")
    col_fecha_e   = get_col(extracto, "fecha", "Fecha")
    col_importe_e = get_col(extracto, "importe", "Importe")
    col_conc_e    = get_col(extracto, "conciliacion")
    col_leyenda_e = get_col(extracto, "leyenda adicional1")

    mayor["_key"]    = mayor[col_fecha_m].astype(str) + "|" + mayor[col_importe_m].astype(str)
    extracto["_key"] = extracto[col_fecha_e].astype(str) + "|" + extracto[col_importe_e].astype(str)

    grupos_m = mayor.groupby("_key").indices
    grupos_e = extracto.groupby("_key").indices

    idx_match_m = []
    idx_match_e = []

    for key, idx_e in grupos_e.items():
        idx_e = list(idx_e)
        idx_m = list(grupos_m.get(key, []))

        if not idx_m:
            continue

        if len(idx_e) == 1 and len(idx_m) == 1:
            idx_match_e.append(idx_e[0])
            idx_match_m.append(idx_m[0])
            continue

        # Ambigüedad: más de un candidato de algún lado para la misma Fecha+Importe.
        disponibles_m = list(idx_m)

        # Renglones de extracto categoría "Proveedores": desambiguar por fuzzy
        # Leyenda Adicional1 (extracto) vs Tercero (mayor).
        idx_e_prov  = [i for i in idx_e if extracto.at[i, col_conc_e] == "Proveedores"]
        idx_e_resto = [i for i in idx_e if i not in idx_e_prov]

        pares_score = []
        for i_e in idx_e_prov:
            nombre_e = str(extracto.at[i_e, col_leyenda_e])
            for i_m in disponibles_m:
                nombre_m = str(mayor.at[i_m, col_tercero_m])
                score = fuzz.token_sort_ratio(nombre_e, nombre_m)
                pares_score.append((score, i_e, i_m))

        pares_score.sort(key=lambda x: x[0], reverse=True)

        usados_e = set()
        usados_m = set()
        for score, i_e, i_m in pares_score:
            if i_e in usados_e or i_m in usados_m:
                continue
            idx_match_e.append(i_e)
            idx_match_m.append(i_m)
            usados_e.add(i_e)
            usados_m.add(i_m)

        disponibles_m = [i for i in disponibles_m if i not in usados_m]

        # Resto de categorías: sin identificador confiable, aparear en orden
        # asegurando 1 a 1 (nunca repetir una misma entrada de ningún lado).
        pendientes_e = [i for i in idx_e_resto if i not in usados_e] + \
                       [i for i in idx_e_prov if i not in usados_e]

        for i_e, i_m in zip(pendientes_e, disponibles_m):
            idx_match_e.append(i_e)
            idx_match_m.append(i_m)

    idx_match_m_set = set(idx_match_m)
    idx_match_e_set = set(idx_match_e)

    idx_falta_extracto = [i for i in mayor.index    if i not in idx_match_m_set]
    idx_falta_mayor     = [i for i in extracto.index if i not in idx_match_e_set]

    mayor    = mayor.drop(columns=["_key"])
    extracto = extracto.drop(columns=["_key"])

    match_mayor    = mayor.loc[idx_match_m].reset_index(drop=True)
    match_extracto = extracto.loc[idx_match_e].reset_index(drop=True)
    falta_extracto = mayor.loc[idx_falta_extracto].reset_index(drop=True)
    falta_mayor    = extracto.loc[idx_falta_mayor].reset_index(drop=True)

    return match_mayor, match_extracto, falta_extracto, falta_mayor


def cruzar_con_tolerancia(df_mayor_cat, df_extracto_sin_acreditaciones,
                           tolerancia_importe=0.5, tolerancia_dias=3, incluir_exactos=False):
    """Cruce con tolerancia de importe y fecha.
    Se corre sobre lo que dejó cruzar_mayor_extracto (no sobre todo el mayor), para
    que ningún renglón quede apareado dos veces. Con incluir_exactos=True los pares
    que coinciden exacto (posibles entre lo que el cruce 0 no pudo desempatar) se
    devuelven también como match."""
    mayor    = df_mayor_cat.copy().reset_index(drop=True)
    extracto = df_extracto_sin_acreditaciones.copy().reset_index(drop=True)

    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_fecha_m   = get_col(mayor,    "Fecha", "fecha")
    col_importe_m = get_col(mayor,    "Importe", "importe")
    col_tercero_m = get_col(mayor,    "Tercero", "tercero")
    col_conc_m    = get_col(mayor,    "conciliacion")
    col_fecha_e   = get_col(extracto, "fecha", "Fecha")
    col_importe_e = get_col(extracto, "importe", "Importe")
    col_leyenda_e = get_col(extracto, "leyenda adicional1")

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

        if candidatos.empty:
            continue

        candidatos["_dist_importe"] = (candidatos[col_importe_e] - importe_m).abs()
        candidatos = candidatos[candidatos["_dist_importe"] <= tolerancia_importe]

        if candidatos.empty:
            continue

        if len(candidatos) == 1:
            idx_e = candidatos.index[0]
        elif row_m[col_conc_m] == "Proveedores":
            # Ambigüedad en categoría Proveedores: desambiguar por fuzzy
            # Tercero (mayor) vs Leyenda Adicional1 (extracto).
            nombre_m = str(row_m[col_tercero_m])
            candidatos["_score"] = candidatos[col_leyenda_e].apply(
                lambda nombre_e: fuzz.token_sort_ratio(nombre_m, str(nombre_e))
            )
            candidatos = candidatos.sort_values(
                ["_score", "_dist_importe"], ascending=[False, True]
            )
            idx_e = candidatos.index[0]
        else:
            # Sin identificador confiable: el más cercano en fecha y, en
            # caso de empate, en importe.
            candidatos["_dist_fecha"] = (candidatos[col_fecha_e] - fecha_m).abs()
            candidatos = candidatos.sort_values(
                ["_dist_fecha", "_dist_importe"], ascending=[True, True]
            )
            idx_e = candidatos.index[0]

        row_e     = extracto.loc[idx_e]
        importe_e = row_e[col_importe_e]
        es_exacto = (row_e[col_fecha_e] == fecha_m and importe_e == importe_m)

        usado_mayor.add(idx_m)
        usado_extracto.add(idx_e)
        if es_exacto:
            match_idx_m_exacto.append(idx_m)
            match_idx_e_exacto.append(idx_e)
        else:
            match_idx_m.append(idx_m)
            match_idx_e.append(idx_e)

    mayor    = mayor.drop(columns=["_idx_m"])
    extracto = extracto.drop(columns=["_idx_e"])

    if incluir_exactos:
        match_idx_m = match_idx_m + match_idx_m_exacto
        match_idx_e = match_idx_e + match_idx_e_exacto

    match_mayor    = mayor.loc[match_idx_m].reset_index(drop=True)
    match_extracto = extracto.loc[match_idx_e].reset_index(drop=True)
    falta_extracto = mayor[~mayor.index.isin(usado_mayor)].reset_index(drop=True)
    falta_mayor    = extracto[~extracto.index.isin(usado_extracto)].reset_index(drop=True)

    return match_mayor, match_extracto, falta_extracto, falta_mayor


def cruzar_por_categoria(falta_extracto1, falta_mayor1, tolerancia=0.5):
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


def cruzar_proveedores(falta_extracto2, falta_mayor2, tolerancia=0.5):
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


def cruzar_a1tp(falta_extracto3, falta_mayor3, tolerancia=0.5):
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


def cruzar_por_proveedor(falta_extracto4, falta_mayor4, tolerancia_importe=0.5, top_candidatos=4):
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


def cruzar_echeq(falta_extracto5, falta_mayor5, tolerancia=0.5):
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


def limpiar_proveedores(df_proveedores, match_mayor, match_mayor1,
                         match_extracto3, match_extracto5, tolerancia_importe=0.5):
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
    col_fecha_emis_p = "Fecha de emisiÃ³n"
    col_fecha_pago_p = "Fecha de pago"

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
    fp[col_fecha_pago_p] = pd.to_datetime(fp[col_fecha_pago_p], errors="coerce")

    def cruzar_y_sacar(fp, match, tolerancia):
        col_fecha_m   = get_col(match, "Fecha", "fecha")
        col_importe_m = get_col(match, "Importe", "importe")

        mm = match.copy()
        mm[col_fecha_m]   = pd.to_datetime(mm[col_fecha_m], errors="coerce")
        mm[col_importe_m] = pd.to_numeric(mm[col_importe_m], errors="coerce").fillna(0.0)

        idx_usados = set()
        for idx_p, row_p in fp.iterrows():
            monto_p = row_p[col_monto] * -1
            candidatos_monto = mm[
                mm[col_importe_m].apply(lambda x: abs(x - monto_p) <= tolerancia)
            ]
            if candidatos_monto.empty:
                continue
            fecha_emis = row_p[col_fecha_emis_p]
            fecha_pago = row_p[col_fecha_pago_p]
            coincide = candidatos_monto[
                (candidatos_monto[col_fecha_m] == fecha_emis) |
                (candidatos_monto[col_fecha_m] == fecha_pago)
            ]
            if not coincide.empty:
                idx_usados.add(idx_p)

        eliminados = len(idx_usados)
        fp = fp[~fp.index.isin(idx_usados)].reset_index(drop=True)
        return fp, eliminados

    fp, elim1 = cruzar_y_sacar(fp, match_mayor,     tolerancia=0.0)
    fp, elim2 = cruzar_y_sacar(fp, match_mayor1,    tolerancia=tolerancia_importe)
    fp, elim3 = cruzar_y_sacar(fp, match_extracto3, tolerancia=tolerancia_importe)
    fp, elim4 = cruzar_y_sacar(fp, match_extracto5, tolerancia=tolerancia_importe)

    return fp


def cruzar_proveedores_descarga(
    falta_extracto6,
    falta_mayor6,
    ejecutar=True,
    df_proveedores_def=None,
    tolerancia_importe=0.5,
    top_candidatos=3,
):
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
    col_fecha_e    = get_col(falta_extracto6, "Fecha", "fecha")
    col_serie_e    = get_col(falta_extracto6, "Serie", "serie")
    col_fecha_m    = get_col(falta_mayor6,    "fecha", "Fecha")
    col_importe_m  = get_col(falta_mayor6,    "importe", "Importe")
    col_conc_m     = get_col(falta_mayor6,    "conciliacion")
    col_razon      = "RazÃ³n Social Beneficiario"
    col_monto      = find_col(df_proveedores_def, "monto")
    col_fecha_pago = "Fecha de pago"
    col_fecha_emis = "Fecha de emisiÃ³n"

    fe = falta_extracto6.copy().reset_index(drop=True)
    fm = falta_mayor6.copy().reset_index(drop=True)
    fp = df_proveedores_def.copy().reset_index(drop=True)

    fp[col_fecha_pago] = pd.to_datetime(fp[col_fecha_pago], errors="coerce")
    fp[col_fecha_emis] = pd.to_datetime(fp[col_fecha_emis], errors="coerce")

    fp_monto_neg = fp.copy()
    fp_monto_neg[col_monto] = fp_monto_neg[col_monto] * -1

    # Se excluyen renglones de Mayor con Serie "TP": son asientos internos de
    # cuenta transitoria/clearing (ej. "Cobro a cuenta A4TP-x") que pueden
    # compartir Tercero y Fecha con un pago real a proveedor y, de incluirse,
    # contaminan la suma agrupada haciendo fallar el match por tolerancia.
    fe_prov   = fe[~fe[col_serie_e].astype(str).str.contains("TP", case=False, na=False)]
    nombres_p = fp_monto_neg[col_razon].unique().tolist()

    usado_extracto     = set()
    idx_fp_usados      = set()
    nombres_matcheados = set()
    pares_matcheados   = set()
    fechas_matcheadas  = set()
    match_idx_e        = []
    combos_agregar     = set()

    # Pre-computar candidatos fuzzy por nombre único de Tercero
    candidatos_por_tercero = {}
    for nombre_e in fe_prov[col_tercero_e].unique():
        candidatos_por_tercero[nombre_e] = process.extract(
            nombre_e, nombres_p, scorer=fuzz.token_sort_ratio, limit=top_candidatos,
        )

    # ── Paso 1: matching 1 a 1 (fila de mayor vs fila de pagos masivos) ──────
    for idx_e, row_e in fe_prov.iterrows():
        if idx_e in usado_extracto:
            continue
        nombre_e  = row_e[col_tercero_e]
        fecha_e   = row_e[col_fecha_e]
        importe_e = row_e[col_importe_e]

        candidatos = candidatos_por_tercero.get(nombre_e, [])
        if not candidatos:
            continue

        for nombre_p, score, _ in candidatos:
            fp_cand = fp_monto_neg[
                (fp_monto_neg[col_razon] == nombre_p) &
                (~fp_monto_neg.index.isin(idx_fp_usados)) &
                (
                    (fp_monto_neg[col_fecha_pago] == fecha_e) |
                    (fp_monto_neg[col_fecha_emis] == fecha_e)
                ) &
                (fp_monto_neg[col_monto].apply(lambda m: abs(importe_e - m) <= tolerancia_importe))
            ]

            if not fp_cand.empty:
                idx_fp = fp_cand.index[0]
                idx_fp_usados.add(idx_fp)
                usado_extracto.add(idx_e)
                match_idx_e.append(idx_e)
                fechas_matcheadas.add(fecha_e)
                nombres_matcheados.add(nombre_p)
                pares_matcheados.add((nombre_p, fecha_e))
                break

    # ── Paso 2: matching por suma sobre remanentes ────────────────────────────
    fe_prov_rest = fe_prov[~fe_prov.index.isin(usado_extracto)]
    fp_neg_rest  = fp_monto_neg[~fp_monto_neg.index.isin(idx_fp_usados)]

    suma_por_tercero_fecha  = fe_prov_rest.groupby([col_tercero_e, col_fecha_e])[col_importe_e].sum()
    suma_por_razon_pago     = fp_neg_rest.groupby([col_razon, col_fecha_pago])[col_monto].sum()
    suma_por_razon_emision  = fp_neg_rest.groupby([col_razon, col_fecha_emis])[col_monto].sum()

    def buscar_suma_p(nombre_p, fecha_e):
        if (nombre_p, fecha_e) in suma_por_razon_pago.index:
            return suma_por_razon_pago[(nombre_p, fecha_e)]
        if (nombre_p, fecha_e) in suma_por_razon_emision.index:
            return suma_por_razon_emision[(nombre_p, fecha_e)]
        return None

    for (nombre_e, fecha_e), suma_e in suma_por_tercero_fecha.items():
        candidatos = candidatos_por_tercero.get(nombre_e) or process.extract(
            nombre_e, fp_neg_rest[col_razon].unique().tolist(),
            scorer=fuzz.token_sort_ratio, limit=top_candidatos,
        )

        if not candidatos:
            continue

        encontrado = False

        for nombre_p, score, _ in candidatos:
            suma_p = buscar_suma_p(nombre_p, fecha_e)
            if suma_p is None:
                continue
            if abs(suma_e - suma_p) <= tolerancia_importe:
                idx_e_list = fe_prov_rest[
                    (fe_prov_rest[col_tercero_e] == nombre_e) &
                    (fe_prov_rest[col_fecha_e] == fecha_e) &
                    (~fe_prov_rest.index.isin(usado_extracto))
                ].index.tolist()
                idx_fp_list = fp_neg_rest[
                    (fp_neg_rest[col_razon] == nombre_p) &
                    (
                        (fp_neg_rest[col_fecha_pago] == fecha_e) |
                        (fp_neg_rest[col_fecha_emis] == fecha_e)
                    ) &
                    (~fp_neg_rest.index.isin(idx_fp_usados))
                ].index.tolist()
                fechas_matcheadas.add(fecha_e)
                match_idx_e.extend(idx_e_list)
                usado_extracto.update(idx_e_list)
                idx_fp_usados.update(idx_fp_list)
                nombres_matcheados.add(nombre_p)
                pares_matcheados.add((nombre_p, fecha_e))
                encontrado = True
                break

        if not encontrado:
            mejor = candidatos[0]
            mejor_nombre, mejor_score = mejor[0], mejor[1]
            suma_mejor = buscar_suma_p(mejor_nombre, fecha_e)
            if suma_mejor is None:
                suma_mejor = 0
            if mejor_score == 100 and suma_mejor != 0:
                combos_agregar.add((mejor_nombre, fecha_e))

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

    leyenda_limpia = fm.get(
        "leyenda adicional1", pd.Series([""] * len(fm))
    ).astype(str).str.upper()

    mask_trf = (
        fm[col_fecha_m].isin(fechas_matcheadas) &
        leyenda_limpia.str.contains("PROVEEDORES", na=False)
    )

    usado_mayor = set(fm[mask_trf].index.tolist())
    falta_mayor7_base = fm[~fm.index.isin(usado_mayor)].reset_index(drop=True)

    filas_nuevas  = []
    for nombre_p, fecha in combos_agregar:
        filas_p = fp[(fp[col_razon] == nombre_p) & (fp[col_fecha_pago] == fecha) & (~fp.index.isin(idx_fp_usados))]
        if filas_p.empty:
            filas_p = fp[(fp[col_razon] == nombre_p) & (fp[col_fecha_emis] == fecha) & (~fp.index.isin(idx_fp_usados))]
        for idx_p, row in filas_p.iterrows():
            monto = row[col_monto]
            fila  = {col: "" for col in fm.columns}
            if col_fecha_m          in fila: fila[col_fecha_m]          = fecha
            if "descripcion"        in fila: fila["descripcion"]         = "TRF INMED PROVEED"
            if "debitos"            in fila: fila["debitos"]             = monto
            if "concepto"           in fila: fila["concepto"]            = "TRF INMED PROVEED"
            if "leyenda adicional1" in fila: fila["leyenda adicional1"]  = row[col_razon]
            if col_importe_m        in fila: fila[col_importe_m]         = -abs(monto)
            if col_conc_m           in fila: fila[col_conc_m]            = "Proveedores"
            filas_nuevas.append(fila)
            idx_fp_usados.add(idx_p)

    # Entradas de pagos masivos que no fueron matcheadas ni están en combos_agregar:
    # pueden ser pagos emitidos que aún no aparecen en el mayor → van a falta_mayor.
    filas_sin_match = []
    for idx_p, row in fp.iterrows():
        if idx_p in idx_fp_usados:
            continue
        razon      = row[col_razon]
        fecha_pago = row[col_fecha_pago]
        fecha_emis = row[col_fecha_emis]
        ya_matcheado = any((razon, f) in pares_matcheados for f in [fecha_pago, fecha_emis])
        if not ya_matcheado:
            monto = row[col_monto]
            fecha_usar = fecha_pago if pd.notna(fecha_pago) else fecha_emis
            fila  = {col: "" for col in fm.columns}
            if col_fecha_m          in fila: fila[col_fecha_m]          = fecha_usar
            if "descripcion"        in fila: fila["descripcion"]         = "TRF INMED PROVEED"
            if "debitos"            in fila: fila["debitos"]             = monto
            if "concepto"           in fila: fila["concepto"]            = "TRF INMED PROVEED"
            if "leyenda adicional1" in fila: fila["leyenda adicional1"]  = razon
            if col_importe_m        in fila: fila[col_importe_m]         = -abs(float(monto)) if monto else 0
            if col_conc_m           in fila: fila[col_conc_m]            = "Proveedores"
            filas_sin_match.append(fila)

    todas_filas_nuevas = filas_nuevas + filas_sin_match
    if todas_filas_nuevas:
        falta_mayor7 = pd.concat([
            falta_mayor7_base,
            pd.DataFrame(todas_filas_nuevas)
        ], ignore_index=True)
    else:
        falta_mayor7 = falta_mayor7_base

    falta_extracto7 = fe[~fe.index.isin(usado_extracto)].reset_index(drop=True)

    return match_extracto7, match_mayor7, falta_extracto7, falta_mayor7


def cruzar_acreditaciones(df_extracto_cat2, falta_extracto7, falta_mayor7, tolerancia=0.5):
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

    # Normalizar nombre viejo → Acred. Nave (por si el mayor viene con el nombre anterior)
    mask_acred_viejo = falta_extracto7[col_conc_falt] == "Acreditaciones"
    falta_extracto7.loc[mask_acred_viejo, col_conc_falt] = "Acred. Nave"

    # Subdividir Acred. Nave según el comentario
    mask_acred   = falta_extracto7[col_conc_falt] == "Acred. Nave"
    mask_py      = comentario_limp.apply(lambda d: "acredpy"     in d)
    mask_rappi   = comentario_limp.apply(lambda d: "acredrappi"  in d)
    mask_prisma  = comentario_limp.apply(lambda d: "acredprisma" in d)
    mask_amex    = comentario_limp.apply(lambda d: "acredamex"   in d)

    falta_extracto7.loc[mask_acred & mask_py,     col_conc_falt] = "Acred. PY"
    falta_extracto7.loc[mask_acred & mask_rappi,  col_conc_falt] = "Acred. Rappi"
    falta_extracto7.loc[mask_acred & mask_prisma, col_conc_falt] = "Acred. Prisma"
    falta_extracto7.loc[mask_acred & mask_amex,   col_conc_falt] = "Acred. AmEx"

    col_importe_cat  = get_col(df_extracto_cat2, "importe", "Importe")
    col_conc_cat     = get_col(df_extracto_cat2, "conciliacion")
    col_importe_falt = get_col(falta_extracto7,  "Importe", "importe")

    CATEGORIAS = ["Acred. Nave", "Acred. PY", "Acred. Rappi", "Acred. Prisma", "Acred. AmEx"]

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
                # No cierra por suma: los asientos de acreditación del mayor se aparean
                # contra los movimientos del extracto de la categoría (match 8 parcial) y
                # queda UNA línea resumen con lo que falta registrar (extracto - mayor).
                match_cat_idx.extend(acred_cat.index.tolist())
                match_falt_idx.extend(acred_falt.index.tolist())
                pendiente = round(suma_cat - suma_falt, 2)
                col_importe_m = get_col(falta_mayor7, "importe", "Importe")
                col_desc_m    = get_col(falta_mayor7, "descripcion", "Descripcion", "descripción")
                col_conc_m    = get_col(falta_mayor7, "conciliacion")
                col_fecha_m   = get_col(falta_mayor7, "fecha", "Fecha")
                fila_resumen  = {col: "" for col in falta_mayor7.columns}
                fila_resumen[col_fecha_m]   = acred_cat[get_col(acred_cat, "fecha", "Fecha")].max()
                fila_resumen[col_importe_m] = pendiente
                fila_resumen[col_desc_m]    = f"{cat} pendiente de registrar"
                fila_resumen[col_conc_m]    = cat
                if "creditos" in fila_resumen:
                    fila_resumen["creditos"] = pendiente if pendiente > 0 else 0.0
                if "debitos" in fila_resumen:
                    fila_resumen["debitos"] = -pendiente if pendiente < 0 else 0.0
                if "observaciones cliente" in fila_resumen:
                    fila_resumen["observaciones cliente"] = (
                        f"Extracto {suma_cat:,.2f} - registrado en mayor {suma_falt:,.2f} "
                        f"({len(acred_cat)} movimientos del banco, {len(acred_falt)} asientos)")
                if "_rid" in fila_resumen:
                    fila_resumen["_rid"] = f"DIF-ACR-{cat}"
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
# CATEGORIZACIÓN COMPLEMENTARIA (v3)
# Completa categorías que v1/v2 dejan en "0". No asigna "Echeq" al mayor para
# no alterar cruzar_echeq, que busca esos asientos justamente con categoría "0".
# ─────────────────────────────────────────────────────────────────────────────

def _limpiar_v3(texto) -> str:
    if not isinstance(texto, str):
        return ""
    t = texto.lower()
    for a, b in zip("áéíóúñü", "aeiounu"):
        t = t.replace(a, b)
    return re.sub(r"[^a-z0-9]", "", t)


def _es_vacio(serie: pd.Series) -> pd.Series:
    return serie.isna() | serie.astype(str).str.strip().isin(["", "nan", "None", "NaN"])


def categorizar_mayor_v3(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    com = df["Comentario"].apply(_limpiar_v3)
    # Acreditaciones de tarjetas registradas en el mayor. Se acepta "Acreeditación"
    # (error de tipeo frecuente). Si dice solo "TC", la tarjeta se resuelve después
    # con la que más acreditó en el extracto del período (resolver_acred_generica).
    reglas = [
        (r"acre+ditacion\w*amex|acredamex", "Acred. AmEx"),
        (r"acre+ditacion\w*(?:prisma|payway)|acredprisma", "Acred. Prisma"),
        (r"acre+ditacion\w*nave|acrednave", "Acred. Nave"),
        (r"acre+ditacion\w*(?:py|pedidosya)|acredpy", "Acred. PY"),
        (r"acre+ditacion\w*rappi|acredrappi", "Acred. Rappi"),
        (r"acre+ditacion(?:es)?tc", "Acred. TC"),
        (r"propina", "Propinas"),
        (r"^(?:pago)?gb\d|gastosbancarios", "Gastos Bancarios"),
        (r"prestamo", "Prestamo"),
    ]
    for patron, cat in reglas:
        sin_cat = df["conciliacion"] == "0"
        df.loc[sin_cat & com.str.contains(patron, regex=True), "conciliacion"] = cat
    # Pagos al Banco Galicia con serie de gastos bancarios (A1CG, A3CG, ...) = Gastos Bancarios
    terc = df["Tercero"].astype(str).apply(_limpiar_v3)
    serie = df["Serie"].astype(str).str.upper().str.strip()
    es_gb = (df["conciliacion"] == "0") & terc.str.contains("galicia") & serie.str.match(r"^A\d?CG$")
    df.loc[es_gb, "conciliacion"] = "Gastos Bancarios"
    # Si el comentario dice PRESTAMO prevalece sobre GB (p. ej. "Pago FC PRESTAMO GB 09-26",
    # que la regla original marca como gasto bancario por contener "gb").
    df.loc[(df["conciliacion"] == "Gastos Bancarios") & com.str.contains("prestamo"), "conciliacion"] = "Prestamo"
    return df


def resolver_acred_generica(df_mayor_cat, df_extracto_cat):
    """Los asientos "Acreditación TC" (sin tarjeta) se asignan a la tarjeta que más
    acreditó en el extracto del período (Nave en ERSA, Prisma/Payway en Patagonia)."""
    df = df_mayor_cat.copy()
    gen = df["conciliacion"] == "Acred. TC"
    if gen.any():
        acred = df_extracto_cat[df_extracto_cat["conciliacion"].astype(str).str.startswith("Acred.")]
        tot = pd.to_numeric(acred["importe"], errors="coerce").groupby(acred["conciliacion"]).sum()
        df.loc[gen, "conciliacion"] = tot.abs().idxmax() if len(tot) else "Acred. Nave"
    return df


def categorizar_extracto_v3(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Cobros de American Express que entran como "pago a proveedores": son acreditaciones AmEx
    ley = df["leyenda adicional1"].astype(str).apply(_limpiar_v3)
    es_amex = ley.str.contains("americanexpress") & (pd.to_numeric(df["importe"], errors="coerce") > 0) \
        & df["conciliacion"].isin(["0", "Cobranzas"])
    df.loc[es_amex, "conciliacion"] = "Acred. AmEx"
    # Transferencias de Delivery Hero (PedidosYa) que no entraron por "Transferencias cash"
    es_py = ley.str.contains("deliveryhero") & (pd.to_numeric(df["importe"], errors="coerce") > 0) \
        & (df["conciliacion"] == "0")
    df.loc[es_py, "conciliacion"] = "Acred. PY"
    txt = (df["descripcion"].astype(str) + " " + df["concepto"].astype(str)).apply(_limpiar_v3)
    reglas = [
        (r"navedesconocimiento|debitocontracargoventa", "Acred. Nave"),
        (r"devolucioncomisiones|ajustescomisiones|devimpcredley|^comision", "Gastos Bancarios"),
        (r"depositoenefectivo", "Deposito"),
    ]
    for patron, cat in reglas:
        sin_cat = df["conciliacion"] == "0"
        df.loc[sin_cat & txt.str.contains(patron, regex=True), "conciliacion"] = cat
    return df


# ─────────────────────────────────────────────────────────────────────────────
# CRUCES SOBRE FALTANTES (match_tipo 9 a 15)
# Se aplican sobre lo que queda después del cruce 8.
#   9  Pago y devolución dentro del mayor (mismo tercero o mismo comprobante)
#  10  Débito y devolución/anulación dentro del extracto
#  11  Comprobante del mayor (Serie-Número) contra un movimiento del banco;
#      si difiere en más de 1 peso, la diferencia queda como faltante
#  12  Echeq emitido (asiento "Pago S/F" sin serie, número ni tercero) contra
#      la suma de débitos de echeq del banco
#  13  Importe exacto dentro del período (cualquier fecha)
#  14  Mismo tercero o categoría, tolerancia 5 pesos y 5 días (1 a 1 o suma)
#  15  Total de una categoría del banco contra asientos sin categoría (suma exacta)
#  16  Un renglón contra la suma exacta de renglones del otro lado del mismo día
#  18  Varios comprobantes del mayor sin tercero contra un débito del banco
#  19  Un asiento contra todos los débitos del mismo tercero en el día
#  17  Lote de proveedores: todos los pagos con tercero de un día contra los débitos
#      agrupados "TRF INMED PROVEED" del mismo día (cuando no hay pagos masivos o
#      quedaron fuera del cruce 7)
# ─────────────────────────────────────────────────────────────────────────────

TOL_EXACTO = 0.01
TOL_RESIDUAL = 5.0
DIAS_RESIDUAL = 5
SCORE_NOMBRE = 70
TOL_RELATIVA_COMPROBANTE = 0.001
TOL_LINEA_DIFERENCIA = 1.0
CATEGORIAS_MASIVAS = ["Gastos Bancarios", "Acred. Nave", "Acred. PY", "Acred. Rappi", "Acred. Prisma", "Acred. AmEx"]

REGLAS_MATCH = {
    9: "Pago y devolución en mayor",
    10: "Débito y devolución en banco",
    11: "Comprobante del mayor contra movimiento del banco",
    12: "Echeq emitido contra débitos de echeq",
    13: "Importe exacto en el período",
    14: "Mismo tercero/categoría con tolerancia",
    15: "Categoría del banco contra asientos sin categoría",
    16: "Suma exacta del mismo día",
    17: "Lote de proveedores del día",
    18: "Varios comprobantes contra un débito",
    19: "Todos los movimientos de un tercero en el día",
}


class _ApareoFaltantes:
    """Tabla única de faltantes de ambos lados (M = mayor, E = extracto) sobre la
    que se aplican los cruces 9 a 15. Cada apareo recibe un match_id."""

    def __init__(self, falta_extracto, falta_mayor, id_inicio):
        fe = falta_extracto.reset_index(drop=True)   # renglones del MAYOR
        fm = falta_mayor.reset_index(drop=True)      # renglones del EXTRACTO
        real_m = fe["Fecha"].notna() & ~fe["Comentario"].astype(str).str.contains("saldo anterior", case=False, na=False)
        saldo_e = pd.to_numeric(fm.get("saldo"), errors="coerce") if "saldo" in fm else pd.Series(np.nan, index=fm.index)
        real_e = saldo_e.notna() & pd.to_datetime(fm["fecha"], errors="coerce").notna()
        m = pd.DataFrame({
            "lado": "M", "pos": fe.index, "fecha": pd.to_datetime(fe["Fecha"], errors="coerce"),
            "importe": pd.to_numeric(fe["Importe"], errors="coerce").fillna(0.0).round(2),
            "categoria": fe["conciliacion"].astype(str), "nombre": fe["Tercero"].astype(str),
            "comprobante": np.where(_es_vacio(fe["Serie"]) | _es_vacio(fe["Numero"]), "",
                                    fe["Serie"].astype(str) + "-" + fe["Numero"].astype(str)),
            "echeq_emitido": (fe["conciliacion"].astype(str) == "0")
                             & (fe["Comentario"].apply(_limpiar_v3) == "pagosf")
                             & _es_vacio(fe["Serie"]) & _es_vacio(fe["Numero"]) & _es_vacio(fe["Tercero"]),
        })[real_m.values]
        e = pd.DataFrame({
            "lado": "E", "pos": fm.index, "fecha": pd.to_datetime(fm["fecha"], errors="coerce"),
            "importe": pd.to_numeric(fm["importe"], errors="coerce").fillna(0.0).round(2),
            "categoria": fm["conciliacion"].astype(str), "nombre": fm["leyenda adicional1"].astype(str),
            "comprobante": "", "echeq_emitido": False,
        })[real_e.values]
        self.fe, self.fm = fe, fm
        self.p = pd.concat([m, e], ignore_index=True)
        self.p["match_id"] = 0
        self.p["match_tipo"] = ""
        self.sin_tercero = _es_vacio(self.p["nombre"])
        self.id = id_inicio
        self.diferencias = []   # renglones nuevos del extracto por diferencia de comprobante

    # ── utilidades ───────────────────────────────────────────────────────────
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
        # Misma primera palabra significativa (p. ej. "SMG COMPAÑIA ARG..." vs "SMG-CIADESEGGROS")
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

    # ── 9 / 10: anulaciones dentro de un mismo lado ──────────────────────────
    def anulaciones(self, lado, tipo, max_dias, excluir_masivas=False, max_k=4):
        base = self.p["lado"] == lado
        if excluir_masivas:
            base &= ~self.p["categoria"].isin(CATEGORIAS_MASIVAS)
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
                # En el banco, un crédito y un débito del mismo día por el mismo importe
                # (p. ej. depósito de echeq y su rechazo) se anulan aunque la leyenda difiera
                mismo_dia_banco = (lado == "E") & (cand["fecha"] == a["fecha"]) & \
                    ((cand["importe"] + a["importe"]).abs() <= TOL_EXACTO)
                cand = cand[(sc >= 90) | mismo_comp | mismo_dia_banco]
                if cand.empty:
                    continue
                hallado = self._buscar_suma(cand, -a["importe"], TOL_EXACTO, max_k)
                if hallado:
                    self.cerrar([ia] + hallado, tipo)

    # ── 11: comprobante del mayor contra un movimiento del banco ─────────────
    def comprobante_vs_banco(self, tipo, max_dias=3):
        pm = self.pend((self.p["lado"] == "M") & (self.p["comprobante"] != ""))
        for comp, g in pm.groupby("comprobante"):
            g = g[self.p.loc[g.index, "match_id"] == 0]
            if g.empty:
                continue
            total, fecha = g["importe"].sum(), g["fecha"].max()
            # Coincidencia exacta al centavo con un solo débito: vale cualquier fecha del
            # período (p. ej. Ronda: comprobante del 21/09 contra Transf. AFIP del 11/09)
            exacto = self.pend((self.p["lado"] == "E") & ~self.p["categoria"].isin(CATEGORIAS_MASIVAS)
                               & ((self.p["importe"] - total).abs() <= TOL_EXACTO))
            if len(g) > 1 and len(exacto):
                ib = exacto.assign(_dd=(exacto["fecha"] - fecha).abs()).sort_values("_dd").index[0]
                self.cerrar(list(g.index) + [ib], tipo)
                continue
            cand = self.pend((self.p["lado"] == "E") & ~self.p["categoria"].isin(CATEGORIAS_MASIVAS))
            cand = cand[(np.sign(cand["importe"]) == np.sign(total))
                        & ((cand["fecha"] - fecha).abs() <= pd.Timedelta(days=max_dias))]
            if cand.empty:
                continue
            # Varios débitos del banco de una misma categoría que suman exacto el comprobante
            if len(g) > 1:
                hallado = None
                for cat, gc in cand[cand["categoria"] != "0"].groupby("categoria"):
                    hallado = self._buscar_suma(gc, total, TOL_EXACTO, max_k=4)
                    if hallado:
                        break
                if hallado and len(hallado) > 1:
                    self.cerrar(list(g.index) + hallado, tipo)
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
                fila = self.fm.loc[self.p.at[ib, "pos"]].copy()
                fila["descripcion"] = f"Diferencia comprobante {comp}"
                fila["importe"] = dif
                fila["debitos"] = -dif if dif < 0 else 0.0
                fila["creditos"] = dif if dif > 0 else 0.0
                fila["saldo"] = np.nan
                fila["observaciones cliente"] = f"Diferencia del match {id_match}"
                if "_rid" in fila.index:
                    fila["_rid"] = f"DIF-{fila['_rid']}"
                self.diferencias.append(fila)

    # ── 12: echeq emitidos contra débitos de echeq ───────────────────────────
    def echeq_emitidos(self, tipo):
        for ia in self.pend(self.p["echeq_emitido"]).index:
            a = self.p.loc[ia]
            cand = self.pend((self.p["lado"] == "E") & (self.p["categoria"] == "Echeq"))
            cand = cand[(cand["importe"] < 0) & (cand["importe"].abs() <= abs(a["importe"]) + TOL_EXACTO)]
            hallado = self._buscar_suma(cand, a["importe"], TOL_EXACTO, max_k=5)
            if hallado:
                self.cerrar([ia] + hallado, tipo)

    # ── 13: importe exacto, cualquier fecha del período ─────────────────────
    def importe_exacto(self, tipo):
        no_masivas = ~self.p["categoria"].isin(CATEGORIAS_MASIVAS)
        for ia in self.pend((self.p["lado"] == "M") & no_masivas).index:
            a = self.p.loc[ia]
            cand = self.pend((self.p["lado"] == "E") & no_masivas)
            cand = cand[(cand["importe"] - a["importe"]).abs() <= TOL_EXACTO]
            if cand.empty:
                continue
            ib = cand.assign(_dd=(cand["fecha"] - a["fecha"]).abs()).sort_values("_dd").index[0]
            self.cerrar([ia, ib], tipo)

    # ── 14: mismo tercero o categoría, con tolerancia ───────────────────────
    def residual_con_tolerancia(self, tipo):
        no_masivas = ~self.p["categoria"].isin(CATEGORIAS_MASIVAS)
        for lado_a, lado_b in (("M", "E"), ("E", "M")):
            for ia in self.pend((self.p["lado"] == lado_a) & no_masivas).index:
                if self.p.at[ia, "match_id"]:
                    continue
                a = self.p.loc[ia]
                cand = self.pend((self.p["lado"] == lado_b) & no_masivas)
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

    # ── 15: total de una categoría del banco contra asientos sin categoría ──
    def categoria_vs_sin_categoria(self, tipo):
        pe = self.pend((self.p["lado"] == "E") & ~self.p["categoria"].isin(CATEGORIAS_MASIVAS + ["0"]))
        for cat, g in pe.groupby("categoria"):
            objetivo = g["importe"].sum()
            # Solo asientos sin tercero: un pago con tercero se identifica por su nombre
            cand = self.pend((self.p["lado"] == "M") & (self.p["categoria"] == "0") & self.sin_tercero)
            cand = cand[(np.sign(cand["importe"]) == np.sign(objetivo))
                        & (cand["importe"].abs() <= abs(objetivo) + TOL_EXACTO)]
            hallado = self._buscar_suma(cand, objetivo, TOL_EXACTO, max_k=4)
            if hallado:
                self.cerrar(list(g.index) + hallado, tipo)

    # ── 16: un renglón contra la suma exacta del otro lado, mismo día ────────
    def suma_mismo_dia(self, tipo):
        # Del lado del mayor, solo asientos sin tercero (los que tienen tercero se
        # identifican por nombre en el cruce 14 o con pagos masivos)
        no_masivas = ~self.p["categoria"].isin(CATEGORIAS_MASIVAS)
        elegibles = no_masivas
        for lado_a, lado_b in (("M", "E"), ("E", "M")):
            for ia in self.pend((self.p["lado"] == lado_a) & elegibles).index:
                if self.p.at[ia, "match_id"]:
                    continue
                a = self.p.loc[ia]
                cand = self.pend((self.p["lado"] == lado_b) & no_masivas)
                exige_nombre = lado_a == "M" and not self.sin_tercero.loc[ia]
                if exige_nombre:
                    # Pago con tercero: vale si al menos un débito del día es del mismo
                    # tercero (p. ej. parte por débito automático y parte con echeq)
                    cand_dia = cand[cand["fecha"] == a["fecha"]]
                    sc_dia = cand_dia["nombre"].apply(lambda n: self.score(a["nombre"], n))
                    if not (sc_dia >= SCORE_NOMBRE).any():
                        continue
                if lado_b == "M":
                    # asientos con tercero solo si alguno coincide en nombre con el banco
                    sc = cand["nombre"].apply(lambda n: self.score(a["nombre"], n))
                    if not (sc >= SCORE_NOMBRE).any():
                        cand = cand[self.sin_tercero.loc[cand.index]]
                cand = cand[(cand["fecha"] == a["fecha"]) & (np.sign(cand["importe"]) == np.sign(a["importe"]))
                            & (cand["importe"].abs() <= abs(a["importe"]) + TOL_EXACTO)]
                hallado = self._buscar_suma(cand, a["importe"], TOL_EXACTO, max_k=4)
                if hallado and exige_nombre:
                    nombres = self.p.loc[hallado, "nombre"].apply(lambda n: self.score(a["nombre"], n))
                    if not (nombres >= SCORE_NOMBRE).any():
                        hallado = None
                if hallado:
                    self.cerrar([ia] + hallado, tipo)

    # ── 19: todos los movimientos de un tercero en el día contra un renglón ────
    def tercero_del_dia(self, tipo):
        """Un asiento contra TODOS los débitos del mismo tercero en el mismo día (o al
        revés), sin límite de cantidad. P. ej. EASA: un pago a SMG de 960.306,96
        contra 15 débitos automáticos de pólizas SMG del 03/09."""
        no_masivas = ~self.p["categoria"].isin(CATEGORIAS_MASIVAS)
        for lado_a, lado_b in (("M", "E"), ("E", "M")):
            for ia in self.pend((self.p["lado"] == lado_a) & no_masivas & ~self.sin_tercero).index:
                if self.p.at[ia, "match_id"]:
                    continue
                a = self.p.loc[ia]
                cand = self.pend((self.p["lado"] == lado_b) & no_masivas)
                cand = cand[(cand["fecha"] == a["fecha"]) & (np.sign(cand["importe"]) == np.sign(a["importe"]))]
                if len(cand) < 2:
                    continue
                sc = cand["nombre"].apply(lambda n: self.score(a["nombre"], n))
                grupo = cand[sc >= SCORE_NOMBRE]
                if len(grupo) >= 2 and abs(grupo["importe"].sum() - a["importe"]) <= TOL_RESIDUAL:
                    self.cerrar([ia] + list(grupo.index), tipo)

    # ── 18: varios comprobantes sin tercero contra un débito del banco ─────────
    def comprobantes_vs_debito(self, tipo, max_dias=5):
        pm = self.pend((self.p["lado"] == "M") & self.sin_tercero & (self.p["categoria"] == "0"))
        if pm.empty:
            return
        unidades = []
        for comp, g in pm.groupby(pm["comprobante"].where(pm["comprobante"] != "", pm.index.astype(str))):
            unidades.append((list(g.index), round(g["importe"].sum(), 2), g["fecha"].max()))
        u = pd.DataFrame({"idx": [x[0] for x in unidades], "importe": [x[1] for x in unidades],
                          "fecha": [x[2] for x in unidades]})
        cand_e = self.pend((self.p["lado"] == "E") & ~self.p["categoria"].isin(CATEGORIAS_MASIVAS + ["0"]))
        for ie, e in cand_e.iterrows():
            libres = u[u["idx"].apply(lambda l: all(self.p.at[i, "match_id"] == 0 for i in l))]
            libres = libres[(np.sign(libres["importe"]) == np.sign(e["importe"]))
                            & ((libres["fecha"] - e["fecha"]).abs() <= pd.Timedelta(days=max_dias))
                            & (libres["importe"].abs() <= abs(e["importe"]) + TOL_EXACTO)]
            if len(libres) < 2:
                continue
            hallado = self._buscar_suma(libres, e["importe"], TOL_EXACTO, max_k=4)
            if hallado and len(hallado) > 1:
                filas = [i for k in hallado for i in libres.at[k, "idx"]]
                self.cerrar(filas + [ie], tipo)

    # ── 17: lote de proveedores del día ──────────────────────────────────────
    def lote_proveedores(self, tipo, tol=0.5):
        fm = self.fm
        # Débitos agrupados del banco por pagos a proveedores: leyenda "PROVEEDORES"
        # (TRF INMED PROVEED, SNP PAGO A PROVEEDORES y sus ajustes/anulaciones)
        lote_pos = set(fm.index[fm["descripcion"].astype(str).str.upper().str.contains("PROVEED", na=False)
                                & fm["leyenda adicional1"].astype(str).str.upper().str.strip().eq("PROVEEDORES")])
        mask_lote = (self.p["lado"] == "E") & self.p["pos"].isin(lote_pos)
        for fecha, g_e in self.pend(mask_lote).groupby("fecha"):
            g_m = self.pend((self.p["lado"] == "M") & ~self.sin_tercero & (self.p["fecha"] == fecha)
                            & (self.p["importe"] < 0) & self.p["categoria"].isin(["0", "Proveedores"]))
            if g_m.empty:
                continue
            dif = round(g_e["importe"].sum() - g_m["importe"].sum(), 2)
            id_match = self.id
            self.cerrar(list(g_e.index) + list(g_m.index), tipo)
            if abs(dif) > tol:
                # Sin pagos masivos no se puede saber qué pago explica la diferencia:
                # queda una línea neta por día para revisar con el archivo de pagos masivos.
                fila = self.fm.loc[g_e.iloc[0]["pos"]].copy()
                fila["descripcion"] = f"Diferencia lote proveedores {pd.Timestamp(fecha):%d/%m/%Y}"
                fila["importe"] = dif
                fila["debitos"] = -dif if dif < 0 else 0.0
                fila["creditos"] = dif if dif > 0 else 0.0
                fila["saldo"] = np.nan
                fila["observaciones cliente"] = f"Diferencia del match {id_match} (banco menos mayor)"
                if "_rid" in fila.index:
                    fila["_rid"] = f"DIF-{fila['_rid']}"
                self.diferencias.append(fila)

    # ── resultado ────────────────────────────────────────────────────────────
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


def cruzar_faltantes_adicionales(falta_extracto8, falta_mayor8, id_inicio):
    """Aplica los cruces 9 a 15 sobre los faltantes que quedan después del cruce 8.
    Devuelve match_mayor9, match_extracto9, falta_extracto9, falta_mayor9, próximo id."""
    ap = _ApareoFaltantes(falta_extracto8, falta_mayor8, id_inicio)
    ap.anulaciones("M", 9, max_dias=31)
    ap.anulaciones("E", 10, max_dias=10, excluir_masivas=True)
    ap.comprobante_vs_banco(11)
    ap.echeq_emitidos(12)
    ap.importe_exacto(13)
    ap.residual_con_tolerancia(14)
    ap.categoria_vs_sin_categoria(15)
    ap.suma_mismo_dia(16)
    ap.lote_proveedores(17)
    ap.comprobantes_vs_debito(18)
    ap.tercero_del_dia(19)
    return ap.resultado()


# ─────────────────────────────────────────────────────────────────────────────
# ASIGNACIÓN DE IDs
# ─────────────────────────────────────────────────────────────────────────────

def asignar_id_match(match_mayor, match_extracto):
    assert len(match_mayor) == len(match_extracto), \
        f"Los dfs deben tener la misma cantidad de filas: {len(match_mayor)} vs {len(match_extracto)}"

    df_match_mayor    = match_mayor.copy().reset_index(drop=True)
    df_match_extracto = match_extracto.copy().reset_index(drop=True)

    ids = list(range(1, len(df_match_mayor) + 1))

    df_match_mayor["match_id"]    = ids
    df_match_mayor["match_tipo"]  = "0"
    df_match_extracto["match_id"]   = ids
    df_match_extracto["match_tipo"] = "0"

    return df_match_mayor, df_match_extracto


def asignar_id_match1(match_mayor1, match_extracto1, id_inicio):
    df_match_mayor1    = match_mayor1.copy().reset_index(drop=True)
    df_match_extracto1 = match_extracto1.copy().reset_index(drop=True)

    ids_m = list(range(id_inicio, id_inicio + len(df_match_mayor1)))
    ids_e = list(range(id_inicio, id_inicio + len(df_match_extracto1)))

    df_match_mayor1["match_id"]    = ids_m
    df_match_mayor1["match_tipo"]  = "1"
    df_match_extracto1["match_id"]   = ids_e
    df_match_extracto1["match_tipo"] = "1"

    return df_match_mayor1, df_match_extracto1


def asignar_id_match3_categoria(match_extracto2, match_mayor2, id_inicio):
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


def asignar_id_match3_fecha(match_extracto3, match_mayor3, id_inicio):
    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_fecha_e = get_col(match_extracto3, "Fecha", "fecha")
    col_fecha_m = get_col(match_mayor3,    "fecha", "Fecha")

    df_match_extracto3 = match_extracto3.copy().reset_index(drop=True)
    df_match_mayor3    = match_mayor3.copy().reset_index(drop=True)

    current_id  = id_inicio
    fecha_to_id = {}
    for fecha in list(df_match_extracto3[col_fecha_e].unique()) + list(df_match_mayor3[col_fecha_m].unique()):
        if fecha not in fecha_to_id:
            fecha_to_id[fecha] = current_id
            current_id += 1

    df_match_extracto3["match_id"]   = df_match_extracto3[col_fecha_e].map(fecha_to_id)
    df_match_extracto3["match_tipo"] = "3"
    df_match_mayor3["match_id"]      = df_match_mayor3[col_fecha_m].map(fecha_to_id)
    df_match_mayor3["match_tipo"]    = "3"

    return df_match_extracto3, df_match_mayor3, current_id


def asignar_id_match4(match_extracto4, match_mayor4, id_inicio):
    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_fecha_e = get_col(match_extracto4, "Fecha", "fecha")
    col_fecha_m = get_col(match_mayor4,    "fecha", "Fecha")

    df_match_extracto4 = match_extracto4.copy().reset_index(drop=True)
    df_match_mayor4    = match_mayor4.copy().reset_index(drop=True)

    current_id  = id_inicio
    fecha_to_id = {}
    for fecha in list(df_match_extracto4[col_fecha_e].unique()) + list(df_match_mayor4[col_fecha_m].unique()):
        if fecha not in fecha_to_id:
            fecha_to_id[fecha] = current_id
            current_id += 1

    df_match_extracto4["match_id"]   = df_match_extracto4[col_fecha_e].map(fecha_to_id)
    df_match_extracto4["match_tipo"] = "4"
    df_match_mayor4["match_id"]      = df_match_mayor4[col_fecha_m].map(fecha_to_id)
    df_match_mayor4["match_tipo"]    = "4"

    return df_match_extracto4, df_match_mayor4, current_id


def asignar_id_match5(match_extracto5, match_mayor5, id_inicio, tolerancia_importe=0.5, top_candidatos=4):
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


def asignar_id_match6(match_extracto6, match_mayor6, id_inicio):
    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_fecha_e = get_col(match_extracto6, "Fecha", "fecha")
    col_fecha_m = get_col(match_mayor6,    "fecha", "Fecha")

    df_match_extracto6 = match_extracto6.copy().reset_index(drop=True)
    df_match_mayor6    = match_mayor6.copy().reset_index(drop=True)

    current_id  = id_inicio
    fecha_to_id = {}
    for fecha in list(df_match_extracto6[col_fecha_e].unique()) + list(df_match_mayor6[col_fecha_m].unique()):
        if fecha not in fecha_to_id:
            fecha_to_id[fecha] = current_id
            current_id += 1

    df_match_extracto6["match_id"]   = df_match_extracto6[col_fecha_e].map(fecha_to_id)
    df_match_extracto6["match_tipo"] = "6"
    df_match_mayor6["match_id"]      = df_match_mayor6[col_fecha_m].map(fecha_to_id)
    df_match_mayor6["match_tipo"]    = "6"

    return df_match_extracto6, df_match_mayor6, current_id


def asignar_id_match7(match_extracto7, match_mayor7, id_inicio):
    def get_col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"Ninguna de {candidates} encontrada en {list(df.columns)}")

    col_fecha_e = get_col(match_extracto7, "Fecha", "fecha")
    col_fecha_m = get_col(match_mayor7,    "fecha", "Fecha")

    df_match_extracto7 = match_extracto7.copy().reset_index(drop=True)
    df_match_mayor7    = match_mayor7.copy().reset_index(drop=True)

    current_id  = id_inicio
    fecha_to_id = {}
    for fecha in list(df_match_extracto7[col_fecha_e].unique()) + list(df_match_mayor7[col_fecha_m].unique()):
        if fecha not in fecha_to_id:
            fecha_to_id[fecha] = current_id
            current_id += 1

    df_match_extracto7["match_id"]   = df_match_extracto7[col_fecha_e].map(fecha_to_id)
    df_match_extracto7["match_tipo"] = "7"
    df_match_mayor7["match_id"]      = df_match_mayor7[col_fecha_m].map(fecha_to_id)
    df_match_mayor7["match_tipo"]    = "7"

    return df_match_extracto7, df_match_mayor7, current_id


def asignar_id_match8(match_acreditaciones, match_acreditaciones_cat, id_inicio):
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

COLUMNAS_IMPORTE = ["Debe", "Haber", "Saldo", "Importe", "debitos", "creditos", "saldo", "importe"]


AMOUNT_FORMAT = "#,##0.00"


def _forzar_importes_float(df: pd.DataFrame, columnas: list[str] = COLUMNAS_IMPORTE) -> pd.DataFrame:
    df = df.copy()
    for col in columnas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    return df


def _aplicar_formato_importes(ws, df: pd.DataFrame, columnas: list[str] = COLUMNAS_IMPORTE) -> None:
    col_indices = [i for i, col in enumerate(df.columns, start=1) if col in columnas]
    for col_idx in col_indices:
        for row in range(2, ws.max_row + 1):
            ws.cell(row=row, column=col_idx).number_format = AMOUNT_FORMAT


def generar_excel_en_memoria_galicia(falta_mayor8, falta_extracto8,
                                      match_mayor_def, match_extracto_def, df_extracto_cat2,
                                      neteos_mayor=None, neteos_extracto=None):
    falta_mayor8       = _forzar_importes_float(falta_mayor8)
    falta_extracto8    = _forzar_importes_float(falta_extracto8)
    match_mayor_def    = _forzar_importes_float(match_mayor_def)
    match_extracto_def = _forzar_importes_float(match_extracto_def)
    df_extracto_cat2   = _forzar_importes_float(df_extracto_cat2)

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        falta_mayor8.to_excel(writer,       sheet_name="Faltante Mayor",    index=False)
        falta_extracto8.to_excel(writer,    sheet_name="Faltante Extracto", index=False)
        match_mayor_def.to_excel(writer,    sheet_name="Match Mayor",       index=False)
        match_extracto_def.to_excel(writer, sheet_name="Match Extracto",    index=False)
        df_extracto_cat2.to_excel(writer,   sheet_name="Extracto_cat",      index=False)
        extras = {}
        for nombre, d in (("Neteos Mayor", neteos_mayor), ("Neteos Extracto", neteos_extracto)):
            if d is not None:
                d = _forzar_importes_float(d)
                d.to_excel(writer, sheet_name=nombre, index=False)
                extras[nombre] = d

        wb = writer.book
        _aplicar_formato_importes(wb["Faltante Mayor"],    falta_mayor8)
        _aplicar_formato_importes(wb["Faltante Extracto"], falta_extracto8)
        _aplicar_formato_importes(wb["Match Mayor"],        match_mayor_def)
        _aplicar_formato_importes(wb["Match Extracto"],     match_extracto_def)
        _aplicar_formato_importes(wb["Extracto_cat"],       df_extracto_cat2)
        for nombre, d in extras.items():
            _aplicar_formato_importes(wb[nombre], d)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def ejecutar_cruces(df_mayor_dep, df_extracto_dep, df_proveedores=None):
    """Corre todos los cruces y devuelve los DataFrames (lo usa también conciliacion_cc.py)."""
    # 2. Categorizar
    df_extracto_cat  = categorizar_extracto_v1(df_extracto_dep)
    df_extracto_cat2 = categorizar_extracto_v3(categorizar_extracto_v2(df_extracto_cat))
    df_extracto_sin_acreditaciones = df_extracto_cat2[
        ~df_extracto_cat2["conciliacion"].isin([
            "Acred. Nave", "Acred. PY", "Acred. Rappi", "Acred. Prisma", "Acred. AmEx",
        ])
    ].reset_index(drop=True)
    df_mayor_cat = categorizar_mayor_v1(df_mayor_dep)
    df_mayor_cat = categorizar_mayor_v2(df_mayor_cat)
    df_mayor_cat = categorizar_mayor_v3(df_mayor_cat)
    df_mayor_cat = resolver_acred_generica(df_mayor_cat, df_extracto_cat2)

    # 3. Cruces
    match_mayor,  match_extracto,  falta_extracto,  falta_mayor  = cruzar_mayor_extracto(df_mayor_cat, df_extracto_sin_acreditaciones)
    # Corrección: el cruce con tolerancia trabaja sobre lo que quedó del cruce exacto
    match_mayor1, match_extracto1, falta_extracto1, falta_mayor1 = cruzar_con_tolerancia(
        falta_extracto, falta_mayor, incluir_exactos=True)
    match_extracto2, match_mayor2, falta_extracto2, falta_mayor2 = cruzar_por_categoria(falta_extracto1, falta_mayor1)
    match_extracto3, match_mayor3, falta_extracto3, falta_mayor3 = cruzar_proveedores(falta_extracto2, falta_mayor2)
    match_extracto4, match_mayor4, falta_extracto4, falta_mayor4 = cruzar_a1tp(falta_extracto3, falta_mayor3)
    match_extracto5, match_mayor5, falta_extracto5, falta_mayor5 = cruzar_por_proveedor(falta_extracto4, falta_mayor4)
    match_extracto6, match_mayor6, falta_extracto6, falta_mayor6 = cruzar_echeq(falta_extracto5, falta_mayor5)

    # 4. Cruce 7 (opcional)
    if df_proveedores is not None:
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
    df_match_extracto2, df_match_mayor2, id4 = asignar_id_match3_categoria(match_extracto2, match_mayor2, id3)
    df_match_extracto3, df_match_mayor3, id5 = asignar_id_match3_fecha(match_extracto3, match_mayor3, id4)
    df_match_extracto4, df_match_mayor4, id6 = asignar_id_match4(match_extracto4, match_mayor4, id5)
    df_match_extracto5, df_match_mayor5, id7 = asignar_id_match5(match_extracto5, match_mayor5, id6)
    df_match_extracto6, df_match_mayor6, id8 = asignar_id_match6(match_extracto6, match_mayor6, id7)
    df_match_extracto7, df_match_mayor7, id9 = asignar_id_match7(match_extracto7, match_mayor7, id8)
    df_match_acreditaciones, df_match_acreditaciones_cat, id10 = asignar_id_match8(
        match_acreditaciones, match_acreditaciones_cat, id9)

    # 7. Cruces 9 a 15 sobre los faltantes
    df_match_mayor9, df_match_extracto9, falta_extracto9, falta_mayor9, _ = cruzar_faltantes_adicionales(
        falta_extracto8, falta_mayor8, id10)
    # Los neteos de un mismo lado van a hojas propias, no a Match
    neteos_mayor = df_match_mayor9[df_match_mayor9["match_tipo"].astype(str) == "9"]
    neteos_extracto = df_match_extracto9[df_match_extracto9["match_tipo"].astype(str) == "10"]
    df_match_mayor9 = df_match_mayor9.drop(neteos_mayor.index)
    df_match_extracto9 = df_match_extracto9.drop(neteos_extracto.index)

    # 8. Consolidar (generar_match_definitivo del notebook)
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
        df_match_mayor9,
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
        df_match_extracto9,
    ])

    return {
        "df_mayor_cat": df_mayor_cat, "df_extracto_cat2": df_extracto_cat2,
        "falta_mayor": falta_mayor9, "falta_extracto": falta_extracto9,
        "match_mayor_def": match_mayor_def, "match_extracto_def": match_extracto_def,
        "match_acreditaciones_cat": match_acreditaciones_cat,
        "neteos_mayor": neteos_mayor.reset_index(drop=True),
        "neteos_extracto": neteos_extracto.reset_index(drop=True),
        "stats": {
            "match_exacto":     len(match_mayor),
            "match_tolerancia": len(match_mayor1),
            "match_faltantes":  len(df_match_mayor9) + len(df_match_extracto9),
            "falta_mayor":      len(falta_mayor9),
            "falta_extracto":   len(falta_extracto9),
            "con_masivos":      df_proveedores is not None,
        },
    }


def correr_cruce_galicia(archivo_mayor, archivo_extracto, archivo_proveedores=None):
    """Solo el cruce (5 hojas), sin hoja CC."""
    # 1. Cargar y normalizar
    df_mayor_dep    = normalize_mayor(load_excel_file(archivo_mayor))
    df_extracto_dep = normalize_extracto_galicia(load_excel_file(archivo_extracto))
    df_proveedores  = load_excel_file(archivo_proveedores) if archivo_proveedores is not None else None

    r = ejecutar_cruces(df_mayor_dep, df_extracto_dep, df_proveedores)
    buf = generar_excel_en_memoria_galicia(
        r["falta_mayor"], r["falta_extracto"], r["match_mayor_def"], r["match_extracto_def"], r["df_extracto_cat2"],
        r["neteos_mayor"], r["neteos_extracto"])
    return buf, r["stats"]


# ─────────────────────────────────────────────────────────────────────────────
# CONCILIACIÓN COMPLETA (HOJA CC) — PARÁMETROS
# ─────────────────────────────────────────────────────────────────────────────

TOL_CENTAVOS = 0.01   # igualdad de importes entre períodos
TOL_CRUCE = 0.5       # misma tolerancia que usa el cruce
MAX_COMBINACION = 4   # máximo de renglones que se combinan para igualar un importe

CATEGORIAS_ACRED = ["Acred. Nave", "Acred. PY", "Acred. Rappi", "Acred. Prisma", "Acred. AmEx"]

# Categorías que en la hoja CC se informan en una sola línea por período (neteando
# mayor contra extracto). El resto se informa renglón por renglón.
CATEGORIAS_AGRUPADAS = {
    "Gastos Bancarios": "GB",
    "Acred. Nave": "Acred. Nave",
    "Acred. PY": "Acred. PY",
    "Acred. Rappi": "Acred. Rappi",
    "Acred. Prisma": "Acred. Prisma",
    "Acred. AmEx": "Acred. AmEx",
    "Prestamo": "Prestamo",
    "Imp. AFIP": "Plan Afip",
    "Echeq": "Echeq",
    "Pago Tc. Corpo": "Pago Tc. Corpo",
    "Descuento Galicia": "Descuento",
    "Propinas": "Propinas",
}


SECCIONES = {
    "ING_MAYOR": "Ingresos en Mayor que No están en Extracto",
    "SAL_MAYOR": "Salidas en Mayor que No están en Extracto",
    "CRED_BANCO": "Créditos del banco a contabilizar",
    "DEB_BANCO": "Débitos del banco a contabilizar",
}


# ─────────────────────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────────────────────

def _norm(texto) -> str:
    if not isinstance(texto, str):
        return ""
    t = texto.lower()
    for a, b in zip("áéíóúñü", "aeiounu"):
        t = t.replace(a, b)
    return re.sub(r"[^a-z0-9]", "", t)


def _categoria_desde_concepto(concepto) -> str:
    """Mapea el texto libre de la hoja CC a una categoría del cruce."""
    c = _norm(concepto)
    reglas = [
        (("gb", "gastosbanc"), "Gastos Bancarios"),
        (("prestamo",), "Prestamo"),
        (("afip",), "Imp. AFIP"),
        (("echeq",), "Echeq"),
        (("acrednave", "nave"), "Acred. Nave"),
        (("acredpy",), "Acred. PY"),
        (("rappi",), "Acred. Rappi"),
        (("prisma",), "Acred. Prisma"),
        (("amex",), "Acred. AmEx"),
        (("propina",), "Propinas"),
        (("seguro", "berkley", "art"), "Seguros"),
        (("proveedor", "fiserv"), "Proveedores"),
        (("cobranza",), "Cobranzas"),
        (("fci", "fima"), "FCI"),
        (("transf",), "Transf. entre cuentas"),
        (("sueldo",), "Sueldos"),
    ]
    for claves, cat in reglas:
        if any(c.startswith(k) or k in c for k in claves):
            return cat
    return str(concepto) if concepto else "0"


def _vacio(serie: pd.Series) -> pd.Series:
    return serie.isna() | serie.astype(str).str.strip().isin(["", "nan", "None", "NaN"])


def _inferir_categoria_mayor(df: pd.DataFrame) -> pd.Series:
    """Completa la categoría de los asientos que categorizar_mayor dejó en '0',
    con los mismos criterios que se usan al armar la hoja CC a mano."""
    cat = df["conciliacion"].astype(str).copy()
    com = df["Comentario"].map(_norm)
    sin = cat == "0"
    reglas = [
        (r"acreditaciontc|acrednave|acreditacionnave", "Acred. Nave"),
        (r"acredpy|pedidosya", "Acred. PY"),
        (r"acredrappi", "Acred. Rappi"),
        (r"acredprisma", "Acred. Prisma"),
        (r"acredamex", "Acred. AmEx"),
        (r"propina", "Propinas"),
        (r"^(?:pago)?gb\d", "Gastos Bancarios"),
    ]
    for patron, c in reglas:
        cat[sin & com.str.contains(patron, regex=True)] = c
        sin = cat == "0"
    # Echeq emitidos: "Pago S/F" sin serie, sin número y sin tercero
    # (mismo criterio que usa cruzar_echeq del lado del mayor).
    es_echeq = sin & com.eq("pagosf") & _vacio(df["Serie"]) & _vacio(df["Numero"]) & _vacio(df["Tercero"])
    cat[es_echeq] = "Echeq"
    return cat


def _inferir_categoria_extracto(df: pd.DataFrame) -> pd.Series:
    """Completa la categoría de los movimientos del banco que el cruce dejó en '0'."""
    cat = df["conciliacion"].astype(str).copy()
    txt = (df["descripcion"].astype(str) + " " + df["concepto"].astype(str)).map(_norm)
    sin = cat == "0"
    reglas = [
        (r"navedesconocimiento|debitocontracargoventa", "Acred. Nave"),
        (r"devolucioncomisiones|ajustescomisiones", "Gastos Bancarios"),
        (r"depositoenefectivo", "Deposito"),
    ]
    for patron, c in reglas:
        cat[sin & txt.str.contains(patron, regex=True)] = c
        sin = cat == "0"
    return cat


def _subconjunto_exacto(valores, objetivo, tol=0.01, exigir_unico=True):
    """Subconjunto de 'valores' cuya suma da 'objetivo' (± tol), buscado en forma
    exacta en centavos partiendo la lista en dos mitades (hasta ~40 valores).
    Con exigir_unico=True solo devuelve resultado si hay UNA sola combinación
    posible: si hay varias, no se puede saber cuál es la real y no se aparea."""
    vals = [int(round(v * 100)) for v in valores][:40]
    meta, tol_c = int(round(objetivo * 100)), int(round(tol * 100))
    mitad = len(vals) // 2

    def sumas(rango):
        res = {0: [1, ()]}          # suma -> [cantidad de combinaciones, una combinación]
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


# ─────────────────────────────────────────────────────────────────────────────
# 1. CRUCE CON IDS (mismo pipeline que correr_conciliacion_galicia)
# ─────────────────────────────────────────────────────────────────────────────

def _correr_cruce_con_ids(df_mayor_dep, df_extracto_dep, df_proveedores=None):
    """Corre el cruce principal (logica_galicia.ejecutar_cruces, match 0 a 19) una
    sola vez y separa, por _rid, qué renglones quedaron conciliados y cuáles no."""
    r = ejecutar_cruces(df_mayor_dep, df_extracto_dep, df_proveedores)
    fe, fm = r["falta_extracto"], r["falta_mayor"]   # fe = renglones del MAYOR; fm = del EXTRACTO
    rid_m_pend = set(fe["_rid"].dropna().astype(str)) - {""}
    rid_fm = fm["_rid"].fillna("").astype(str)
    rid_e_pend = set(rid_fm[(rid_fm != "") & ~rid_fm.str.startswith("DIF-")])
    # Las acreditaciones no cerradas por suma quedan pendientes renglón a renglón
    ext_cat = r["df_extracto_cat2"]
    acred_ext = ext_cat[ext_cat["conciliacion"].isin(CATEGORIAS_ACRED)]
    rid_e_pend |= set(acred_ext["_rid"]) - set(r["match_acreditaciones_cat"]["_rid"])
    diferencias = fm[rid_fm.str.startswith("DIF-")].copy()
    diferencias["descripcion"] = diferencias["descripcion"].astype(str)

    # Cruce 7 (pagos masivos): reemplaza los débitos agrupados del banco ("PROVEEDORES")
    # por el detalle de pagos masivos. Los pagos masivos sin asiento quedan en Faltante
    # Mayor como renglones nuevos y se informan como débitos del banco a contabilizar.
    sint = fm[(rid_fm == "") & fm["descripcion"].astype(str).eq("TRF INMED PROVEED")].copy()
    if len(sint) or (r["match_mayor_def"].get("match_tipo", pd.Series(dtype=str)).astype(str) == "7").any():
        sint["_rid"] = [f"PM{i}" for i in range(len(sint))]
        sint["observaciones cliente"] = "Pago masivo sin asiento en el mayor"
        sint["descripcion"] = "Pago masivo: " + sint["leyenda adicional1"].astype(str)
        diferencias = pd.concat([diferencias, sint], ignore_index=True)

    may = r["df_mayor_cat"].copy()
    ext = ext_cat.copy()
    may["pendiente"] = may["_rid"].isin(rid_m_pend)
    ext["pendiente"] = ext["_rid"].isin(rid_e_pend)

    sin_rid = lambda d: d.drop(columns=[c for c in ("_rid",) if c in d.columns])
    bytes_cruce = generar_excel_en_memoria_galicia(
        sin_rid(fm), sin_rid(fe), sin_rid(r["match_mayor_def"]), sin_rid(r["match_extracto_def"]), sin_rid(ext_cat),
        sin_rid(r["neteos_mayor"]), sin_rid(r["neteos_extracto"]))
    return may, ext, bytes_cruce, diferencias


# ─────────────────────────────────────────────────────────────────────────────
# 2. CONCILIACIÓN ANTERIOR
# ─────────────────────────────────────────────────────────────────────────────

def leer_conciliacion_anterior(archivo, cuenta=None, saldo_ext_inicio=None, saldo_mayor_inicio=None) -> dict:
    """Lee la hoja CC/Conciliación (pendientes y saldos) y las hojas del cruce
    (foto del mayor) de la conciliación anterior.

    Cada sociedad tiene su archivo y una hoja puede tener varias cuentas (p. ej. ERSA:
    Cta 2213-8 y Cta 2212-5). La cuenta se elige así:
      1. la indicada en 'cuenta' (alcanza con el número, p. ej. "2212-5");
      2. la cuyo saldo de extracto coincide con el saldo inicial del extracto cargado;
      3. la cuyo saldo contable coincide con el saldo inicial del mayor cargado;
      4. la cuenta a la que corresponden las hojas del cruce (Match/Faltante);
      5. la primera de la hoja."""
    from openpyxl import load_workbook
    contenido = archivo.read() if hasattr(archivo, "read") else open(archivo, "rb").read()
    wb_f = load_workbook(BytesIO(contenido))
    wb_v = load_workbook(BytesIO(contenido), data_only=True)

    hoja = next(s for s in wb_f.sheetnames
                if _norm(s).startswith("concilia") or _norm(s) == "cc")
    wf, wv = wb_f[hoja], wb_v[hoja]

    bloques, actual, seccion = {}, None, None
    mes, razon_social = None, None
    for r in range(1, wf.max_row + 1):
        val = {c: wv.cell(r, c).value for c in range(2, 10)}
        texto = " ".join(str(v) for v in val.values() if isinstance(v, str))
        if isinstance(val.get(2), str) and _norm(val[2]).startswith("razonsocial") and razon_social is None:
            razon_social = str(val.get(3) or "").strip() or None
        if isinstance(val.get(2), str) and val[2].strip().lower().startswith("mes") and mes is None:
            mes = _a_fecha(val.get(3))
        if isinstance(val.get(9), str) and "cta" in val[9].lower():
            actual = val[9].strip()
            bloques[actual] = {"lineas": [], "saldo_contable": None, "saldo_extracto": None, "diferencia": None}
            seccion = None
            continue
        if actual is None:
            if "Saldo Contable" not in texto:
                continue
            # Hoja con una sola cuenta (sin encabezado "Cta ...")
            actual = "Cuenta"
            bloques[actual] = {"lineas": [], "saldo_contable": None, "saldo_extracto": None, "diferencia": None}
        b = bloques[actual]
        if "Saldo Contable" in texto:
            b["saldo_contable"] = val[9]; continue
        if "Saldo Extracto" in texto:
            b["saldo_extracto"] = val[9]; continue
        if "DIFERENCIA" in texto:
            b["diferencia"] = val[9]; continue
        encontrada = [k for k, v in SECCIONES.items() if v.split(" que")[0].split(" a ")[0] in texto]
        if encontrada:
            seccion = encontrada[0]; continue
        if texto.strip().startswith("Total"):
            seccion = None; continue
        if seccion is None:
            continue
        celda_f = wf.cell(r, 5).value
        imp = val.get(5)
        if isinstance(celda_f, str) and celda_f.startswith("="):
            continue  # subtotal
        if not isinstance(imp, (int, float)) or imp == 0:
            continue
        b["lineas"].append({
            "seccion": seccion, "fecha": _a_fecha(val.get(3)), "concepto": val.get(4),
            "importe_hoja": float(imp), "obs": val.get(6), "fila": r,
        })

    # Foto del mayor y del extracto usada en la conciliación anterior
    snap, saldo_ant, ext_desde, ext_hasta, snap_final = None, None, None, None, None
    xls = pd.ExcelFile(BytesIO(contenido))
    partes = [pd.read_excel(xls, sheet_name=s_) for s_ in ("Match Mayor", "Faltante Extracto") if s_ in xls.sheet_names]
    if partes:
        m = pd.concat(partes, ignore_index=True)
        fila_sa = m[m["Comentario"].astype(str).str.contains("saldo anterior", case=False, na=False)]
        if len(fila_sa):
            saldo_ant = float(fila_sa["Saldo"].iloc[0])
        snap = m.dropna(subset=["Asiento"]).drop_duplicates(["Asiento", "Apunte"]).copy()
        snap["Fecha"] = pd.to_datetime(snap["Fecha"], errors="coerce")
        if saldo_ant is not None:
            snap_final = saldo_ant + pd.to_numeric(snap["Importe"], errors="coerce").sum()
    if "Extracto_cat" in xls.sheet_names:
        f = pd.to_datetime(pd.read_excel(xls, sheet_name="Extracto_cat", usecols=[0]).iloc[:, 0], errors="coerce")
        ext_desde, ext_hasta = f.min(), f.max()
    if ext_hasta is None and snap is not None:
        ext_desde, ext_hasta = snap["Fecha"].min(), snap["Fecha"].max()

    def _cerca(a, b, tol=1.0):
        try:
            return a is not None and b is not None and abs(float(a) - float(b)) <= tol
        except (TypeError, ValueError):
            return False

    duenio_cruce = next((c for c, b in bloques.items() if _cerca(b["saldo_contable"], snap_final)), None)
    if cuenta is not None:
        num = re.sub(r"[^0-9]", "", str(cuenta))
        elegida = next((c for c in bloques if num and num in re.sub(r"[^0-9]", "", c)), None)
        if elegida is None:
            elegida = next((c for c in bloques if _norm(c) == _norm(cuenta)), None)
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
        cuenta, criterio = next(iter(bloques)), "primera cuenta de la hoja"
    blo = bloques[cuenta]
    lin = pd.DataFrame(blo["lineas"])
    if not lin.empty:
        signo = lin["seccion"].map({"ING_MAYOR": 1, "SAL_MAYOR": -1, "CRED_BANCO": 1, "DEB_BANCO": -1})
        lin["lado"] = lin["seccion"].map({"ING_MAYOR": "M", "SAL_MAYOR": "M", "CRED_BANCO": "E", "DEB_BANCO": "E"})
        lin["importe"] = signo * lin["importe_hoja"]

    # Las hojas del cruce son de una sola cuenta: si la elegida es otra, no se usan
    if len(bloques) > 1 and duenio_cruce is not None and duenio_cruce != cuenta:
        snap, saldo_ant = None, None

    return {
        "cuenta": cuenta, "pendientes": lin, "mes": mes,
        "saldo_contable": blo["saldo_contable"], "saldo_extracto": blo["saldo_extracto"],
        "diferencia": blo["diferencia"], "snapshot_mayor": snap,
        "saldo_anterior_mayor": saldo_ant, "desde": ext_desde, "hasta": ext_hasta,
        "otras_cuentas": [c for c in bloques if c != cuenta],
        "razon_social": razon_social, "criterio_cuenta": criterio,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 3. SALDOS Y RETROACTIVOS
# ─────────────────────────────────────────────────────────────────────────────

def _saldos_extracto(ext):
    f = ext["fecha"]
    if f.is_monotonic_decreasing and not f.is_monotonic_increasing:
        ext = ext.iloc[::-1]
    inicio = float(ext["saldo"].iloc[0] - ext["importe"].iloc[0])
    final = float(ext["saldo"].iloc[-1])
    return inicio, final, abs(inicio + ext["importe"].sum() - final) < 0.05


def analizar_retroactivos(may_full, saldo_ini_archivo, desde_actual, ant):
    """Compara el mayor actual contra la foto de la conciliación anterior."""
    snap = ant["snapshot_mayor"]
    misma_ventana = ant["hasta"] is not None and ant["hasta"] >= desde_actual
    movs = may_full.dropna(subset=["Asiento"]).copy()
    movs["Asiento"] = pd.to_numeric(movs["Asiento"], errors="coerce")
    movs["Apunte"] = pd.to_numeric(movs["Apunte"], errors="coerce")

    vacio = movs.iloc[0:0]
    res = {"modo": "mismo período" if misma_ventana else "período siguiente",
           "retro": vacio, "borrados": vacio, "modificados": pd.DataFrame(),
           "cubre_periodo_anterior": False}

    if snap is None or snap.empty:
        res["sin_foto"] = True
        return res

    claves_snap = set(zip(snap["Asiento"], snap["Apunte"]))
    claves_act = set(zip(movs["Asiento"], movs["Apunte"]))
    max_asiento_snap = snap["Asiento"].max()
    ant_desde, ant_hasta = ant["desde"], ant["hasta"]
    # La foto solo cubre las fechas que tiene. Si el extracto anterior arrancaba con el último
    # día del mes previo (p. ej. Ronda: 31/08), esos días no forman parte de la foto y sus
    # asientos no pueden marcarse como "no estaban en la foto".
    if len(snap) and pd.notna(snap["Fecha"].min()):
        ant_desde = max(ant_desde, snap["Fecha"].min()) if ant_desde is not None else snap["Fecha"].min()
    cubre = movs["Fecha"].min() <= ant_desde if len(movs) else False
    res["cubre_periodo_anterior"] = bool(cubre)

    # Retroactivos: fecha dentro de lo ya conciliado y no estaban en la foto
    en_ventana = (movs["Fecha"] <= ant_hasta) & ((movs["Fecha"] >= ant_desde) | (movs["Asiento"] > max_asiento_snap))
    nuevos = movs[en_ventana & np.array([k not in claves_snap for k in zip(movs["Asiento"], movs["Apunte"])], dtype=bool)]
    res["retro"] = nuevos

    # Borrados / modificados: sólo verificable si el mayor actual cubre la ventana anterior
    if misma_ventana or cubre:
        res["borrados"] = snap[np.array([k not in claves_act for k in zip(snap["Asiento"], snap["Apunte"])], dtype=bool)]
        j = snap.merge(movs, on=["Asiento", "Apunte"], suffixes=("_ant", "_act"))
        res["modificados"] = j[(j["Importe_ant"].round(2) != j["Importe_act"].round(2)) |
                               (j["Fecha_ant"] != j["Fecha_act"])]
    return res


# ─────────────────────────────────────────────────────────────────────────────
# 4. MOTOR DE APAREO SOBRE PARTIDAS PENDIENTES
# ─────────────────────────────────────────────────────────────────────────────

class Apareo:
    def __init__(self, partidas: pd.DataFrame):
        self.p = partidas.reset_index(drop=True)
        self.p["estado"] = "pendiente"
        self.p["grupo"] = 0
        self.p["regla"] = ""
        self.grupo = 0

    def _pend(self, mask):
        return self.p[mask & (self.p["estado"] == "pendiente")]

    def _cerrar(self, idx, regla):
        self.grupo += 1
        self.p.loc[idx, ["estado", "grupo", "regla"]] = ["conciliado", self.grupo, regla]

    @staticmethod
    def _score(a, b):
        na, nb = str(a.get("nombre") or ""), str(b.get("nombre") or "")
        if not na or not nb or na == "nan" or nb == "nan":
            return 0
        return fuzz.token_set_ratio(na.upper(), nb.upper())

    def uno_a_uno(self, mask_a, mask_b, tol, regla, misma_categoria=False,
                  max_dias=None, exigir_vinculo=False):
        """Un renglón de A contra un renglón de B. Si A y B son del mismo lado
        (mayor contra mayor) se buscan importes opuestos que se anulan."""
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

    def combinacion(self, mask_a, mask_b, tol, regla, max_k=MAX_COMBINACION,
                    vinculo="categoria", max_dias=None):
        """Un renglón de A contra la suma de 2..k renglones de B vinculados
        (misma categoría o mismo nombre)."""
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
                for combo in itertools.combinations(cand.index, k):
                    if abs(cand.loc[list(combo), "importe"].sum() - a["importe"]) <= tol:
                        hallado = list(combo)
                        break
                if hallado:
                    break
            if hallado is None and abs(cand["importe"].sum() - a["importe"]) <= tol:
                hallado = list(cand.index)
            if hallado:
                self._cerrar([ia] + hallado, regla)

    def par_mayor_vs_banco(self, mask_m, mask_e, mask_req, tol, regla, max_dias=3):
        """Un pendiente del banco contra la suma de DOS renglones del mayor de la misma
        fecha (±max_dias), de cualquier signo, con al menos uno tocado para atrás.
        Ej.: pago pendiente 5.408.048,90 + modificación del asiento +2.659.261,65 =
        débito del banco 2.748.787,25. Solo si la combinación es única."""
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
            req = set(self.p.index[mask_req.reindex(self.p.index, fill_value=False).astype(bool)])
            sol = [list(c) for c in itertools.combinations(cand.index, 2)
                   if (c[0] in req or c[1] in req)
                   and abs(cand.loc[list(c), "importe"].sum() - e["importe"]) <= tol]
            if len(sol) == 1:
                self._cerrar([ie] + sol[0], regla)

    def bancarios_en_conjunto(self, mask_m, mask_e, tol, regla):
        """Todos los pendientes anteriores de gastos bancarios y préstamos (mayor y banco)
        se cierran juntos si el total del mayor iguala al del banco."""
        txt = (self.p["categoria"].astype(str) + " " + self.p["concepto"].astype(str)).str.lower()
        es_banc = txt.str.contains(r"\bgb\b|gastos bancarios|prestamo|préstamo", regex=True)
        pm, pe = self._pend(mask_m & es_banc), self._pend(mask_e & es_banc)
        if pm.empty or pe.empty:
            return
        if abs(pm["importe"].sum() - pe["importe"].sum()) <= tol:
            self._cerrar(list(pm.index) + list(pe.index), regla)

    def compensacion_categoria_mes(self, mask, tol, regla):
        """Regla general: todos los pendientes de una categoría y un mes (de ambos lados y de
        cualquier origen dentro de mask) se cierran juntos si el total del mayor iguala al del
        banco. Lo que se compensa no debe llegar a la CC (p. ej. préstamo 09-26 de EASA:
        pendiente del mayor 1.562.500 + cuotas cargadas después 1.839.811,69 = débitos del
        banco 3.402.311,65)."""
        pp = self._pend(mask)
        pp = pp[pp["fecha"].notna() & ~pp["categoria"].astype(str).isin(["0", "nan", ""])]
        if pp.empty:
            return
        cat = pp["categoria"].astype(str).str.lower().str.normalize("NFKD") \
            .str.encode("ascii", "ignore").str.decode("ascii").str.strip()
        clave = cat + "|" + pp["fecha"].dt.strftime("%Y-%m")
        for k, g in pp.groupby(clave):
            if len(g) < 2:
                continue
            sm = g.loc[g["lado"] == "M", "importe"].sum()
            se = g.loc[g["lado"] == "E", "importe"].sum()
            if abs(sm - se) <= tol:
                self._cerrar(list(g.index), regla)

    def anulacion_suma(self, mask, tol, regla, max_k=3):
        """Del mismo lado: un renglón se anula con la suma de 2..k renglones de signo
        contrario (p. ej. pendiente de salida 6.366.295,14 contra pendiente de ingreso
        5.758.690,25 + devolución cargada para atrás 607.604,89). Solo si la combinación es única."""
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
        """El total pendiente de cada categoría de A contra una combinación de
        renglones de B sin categoría (p. ej. asientos 'Pago S/F' sin tercero)."""
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
                for combo in itertools.combinations(cand.index, k):
                    if abs(cand.loc[list(combo), "importe"].sum() - objetivo) <= tol:
                        hallado = list(combo); break
                if hallado:
                    break
            if hallado:
                self._cerrar(list(ga.index) + hallado, regla)

    def anulaciones(self, mask, tol, regla, max_k=MAX_COMBINACION, max_dias=31, score_min=90):
        """Dentro de un mismo lado: un renglón contra su devolución/reversión
        (mismo tercero o mismo comprobante), 1 a 1 o contra la suma de varios."""
        for signo in (1, -1):
            anclas = self._pend(mask)
            anclas = anclas[np.sign(anclas["importe"]) == signo]
            for ia in anclas.assign(_abs=lambda d: d["importe"].abs()).sort_values("_abs", ascending=False).index:
                if self.p.at[ia, "estado"] != "pendiente":
                    continue
                a = self.p.loc[ia]
                cand = self._pend(mask)
                cand = cand[(cand.index != ia) & (np.sign(cand["importe"]) == -signo)
                            & (cand["importe"].abs() <= abs(a["importe"]) + tol)]
                if pd.notna(a["fecha"]):
                    cand = cand[(cand["fecha"] - a["fecha"]).abs() <= pd.Timedelta(days=max_dias)]
                if cand.empty:
                    continue
                comp_a = str(a.get("comprobante") or "")
                mismo_comp = cand["comprobante"].astype(str).eq(comp_a) & (comp_a != "") if "comprobante" in cand else False
                sc = cand.apply(lambda b: self._score(a, b), axis=1)
                cand = cand[(sc >= score_min) | mismo_comp]
                if cand.empty:
                    continue
                exactos = cand[(cand["importe"] + a["importe"]).abs() <= tol]
                if len(exactos):
                    ib = exactos.assign(_dd=(exactos["fecha"] - a["fecha"]).abs()).sort_values("_dd").index[0]
                    self._cerrar([ia, ib], regla)
                    continue
                for k in range(2, min(max_k, len(cand)) + 1):
                    hallado = next((list(c) for c in itertools.combinations(cand.index, k)
                                    if abs(cand.loc[list(c), "importe"].sum() + a["importe"]) <= tol), None)
                    if hallado:
                        self._cerrar([ia] + hallado, regla)
                        break

    def comprobante_vs_banco(self, mask_m, mask_e, regla, max_dias=3, tol_rel=TOL_RELATIVA_COMPROBANTE):
        """Los renglones del mayor de un mismo comprobante (Serie-Número) contra un
        movimiento del banco. Si difieren en menos de tol_rel, se aparean y la
        diferencia queda como partida pendiente del banco."""
        pm = self._pend(mask_m)
        pm = pm[pm["comprobante"].astype(str) != ""] if "comprobante" in pm else pm.iloc[0:0]
        for comp, g in pm.groupby("comprobante"):
            g = g[self.p.loc[g.index, "estado"] == "pendiente"]
            if g.empty:
                continue
            total, fecha = g["importe"].sum(), g["fecha"].max()
            cand = self._pend(mask_e)
            cand = cand[(np.sign(cand["importe"]) == np.sign(total))
                        & ((cand["fecha"] - fecha).abs() <= pd.Timedelta(days=max_dias))]
            if cand.empty:
                continue
            cand = cand.assign(_d=(cand["importe"] - total).abs(), _dd=(cand["fecha"] - fecha).abs())
            cand = cand[cand["_d"] <= max(TOL_CRUCE, abs(total) * tol_rel)]
            if cand.empty:
                continue
            ib = cand.sort_values(["_d", "_dd"]).index[0]
            dif = round(self.p.at[ib, "importe"] - total, 2)
            self._cerrar(list(g.index) + [ib], regla)
            if abs(dif) >= TOL_LINEA_DIFERENCIA:
                b = self.p.loc[ib]
                self.p.loc[len(self.p)] = {
                    **{c: None for c in self.p.columns},
                    "lado": "E", "origen": "Diferencia de apareo", "fecha": b["fecha"],
                    "categoria": b["categoria"], "concepto": f"Diferencia {b['categoria']}",
                    "nombre": f"Comprobante {comp}", "detalle": "Diferencia entre lo debitado y lo registrado",
                    "referencia": f"Comprobante {comp}", "importe": dif, "rid": f"D{ib}",
                    "comprobante": "", "estado": "pendiente", "grupo": 0, "regla": ""}

    def explicar_cambio_saldo(self, mask_ru, mask_ant, regla, tol=TOL_CENTAVOS, max_items=40):
        """Busca qué pendientes anteriores explican exactamente el cambio de saldo
        inicial no identificado (registrados o anulados con fecha ya conciliada).
        Efecto en el mayor: un pendiente del banco registrado suma su importe; un
        pendiente del mayor anulado resta el suyo. Búsqueda exacta por mitades."""
        ru = self._pend(mask_ru)
        if ru.empty:
            return
        ir = ru.index[0]
        objetivo = round(self.p.at[ir, "importe"], 2)
        items = self._pend(mask_ant)
        if items.empty:
            return
        items = items.assign(_v=np.where(items["lado"] == "E", items["importe"], -items["importe"]).round(2))
        items = items.reindex(items["_v"].abs().sort_values(ascending=False).index)[:max_items]
        idx = list(items.index)
        mejor = _subconjunto_exacto(list(items["_v"]), objetivo, tol)
        if mejor:
            self._cerrar([ir] + [idx[i] for i in mejor], regla)

    def por_categoria_mes(self, mask_a, mask_b, tol, regla):
        """Todos los pendientes de una categoría y un mes de A contra los de la misma
        categoría y mes de B, por suma (p. ej. débitos de préstamo de septiembre pendientes
        contra las cuotas de septiembre cargadas después con fecha ya conciliada)."""
        pa, pb = self._pend(mask_a), self._pend(mask_b)
        if pa.empty or pb.empty:
            return
        ka = pa["categoria"].astype(str) + "|" + pa["fecha"].dt.strftime("%Y-%m").fillna("")
        kb = pb["categoria"].astype(str) + "|" + pb["fecha"].dt.strftime("%Y-%m").fillna("")
        for k in set(ka) & set(kb):
            if k.startswith("0|") or k.endswith("|"):
                continue
            ga, gb = pa[ka == k], pb[kb == k]
            if abs(ga["importe"].sum() - gb["importe"].sum()) <= tol:
                self._cerrar(list(ga.index) + list(gb.index), regla)

    def por_categoria(self, mask_a, mask_b, tol, regla):
        """Todos los pendientes de una categoría de A contra los de B, por suma."""
        pa, pb = self._pend(mask_a), self._pend(mask_b)
        for cat in set(pa["categoria"]) & set(pb["categoria"]) - {"0"}:
            ga, gb = pa[pa["categoria"] == cat], pb[pb["categoria"] == cat]
            if abs(ga["importe"].sum() - gb["importe"].sum()) <= tol:
                self._cerrar(list(ga.index) + list(gb.index), regla)


# ─────────────────────────────────────────────────────────────────────────────
# 5. PIPELINE COMPLETO
# ─────────────────────────────────────────────────────────────────────────────

def _a_bytes(archivo):
    if archivo is None:
        return None
    if hasattr(archivo, "read"):
        if hasattr(archivo, "seek"):
            archivo.seek(0)
        return archivo.read()
    with open(archivo, "rb") as f:
        return f.read()


def correr_conciliacion_completa(archivo_mayor, archivo_extracto, archivo_conciliacion_anterior=None,
                                 archivo_proveedores=None, analista="", razon_social=None,
                                 banco="Galicia", cuenta=None, archivos_conciliaciones_previas=None):
    # ── Carga ────────────────────────────────────────────────────────────────
    b_mayor, b_ext, b_prov = _a_bytes(archivo_mayor), _a_bytes(archivo_extracto), _a_bytes(archivo_proveedores)
    raw_m = load_excel_file(BytesIO(b_mayor))
    raw_e = load_excel_file(BytesIO(b_ext))
    raw_m["_rid"] = ["M%06d" % i for i in range(len(raw_m))]
    raw_e["_rid"] = ["E%06d" % i for i in range(len(raw_e))]
    may_full = normalize_mayor(raw_m).rename(columns={"_Rid": "_rid"})
    ext = normalize_extracto_galicia(raw_e)

    desde, hasta = ext["fecha"].min(), ext["fecha"].max()
    # Algunos extractos traen 2 o 3 movimientos del último día hábil del mes anterior
    # (p. ej. Ronda: el de junio empieza el 29/05). El período es el mes predominante.
    mes_dominante = ext["fecha"].dt.to_period("M").mode()[0]
    desde = max(desde, mes_dominante.start_time)
    es_saldo_ant = may_full["Comentario"].astype(str).str.contains("saldo anterior", case=False, na=False)
    if es_saldo_ant.any():
        saldo_ini_archivo = float(may_full.loc[es_saldo_ant, "Saldo"].iloc[0])
    else:
        primera = may_full.dropna(subset=["Fecha"]).iloc[0]
        saldo_ini_archivo = float(primera["Saldo"] - primera["Importe"])
    movs = may_full[~es_saldo_ant]
    previos = movs[movs["Fecha"] < desde]
    actuales = movs[~(movs["Fecha"] < desde)]
    saldo_mayor_inicio = saldo_ini_archivo + previos["Importe"].sum()
    saldo_mayor_final = saldo_mayor_inicio + actuales["Importe"].sum()
    saldo_ext_inicio, saldo_ext_final, ext_ok = _saldos_extracto(ext)

    df_prov = load_excel_file(BytesIO(b_prov)) if b_prov is not None else None

    # ── Cruce principal (logica_galicia, match 0 a 19) solo con el mayor del período ──
    may, ext_c, bytes_cruce, dif_comprobantes = _correr_cruce_con_ids(actuales.reset_index(drop=True), ext, df_prov)
    dif_cruce = (may.loc[~may["pendiente"], "Importe"].sum() - ext_c.loc[~ext_c["pendiente"], "importe"].sum()
                 + pd.to_numeric(dif_comprobantes["importe"], errors="coerce").sum())

    # ── Partidas pendientes del período ──────────────────────────────────────
    pm = may[may["pendiente"]]
    pe = ext_c[ext_c["pendiente"]]
    partidas = [
        pd.DataFrame({
            "lado": "M", "origen": "Período", "fecha": pm["Fecha"], "categoria": _inferir_categoria_mayor(pm),
            "concepto": _inferir_categoria_mayor(pm), "nombre": pm["Tercero"].astype(str),
            "detalle": pm["Comentario"].astype(str), "referencia": "Asiento " + pm["Asiento"].astype(str),
            "importe": pm["Importe"], "rid": pm["_rid"],
            "comprobante": np.where(_vacio(pm["Serie"]) | _vacio(pm["Numero"]), "",
                                    pm["Serie"].astype(str) + "-" + pm["Numero"].astype(str))}),
        pd.DataFrame({
            "lado": "E", "origen": "Período", "fecha": pe["fecha"], "categoria": _inferir_categoria_extracto(pe),
            "concepto": _inferir_categoria_extracto(pe), "nombre": pe["leyenda adicional1"].astype(str),
            "detalle": pe["descripcion"].astype(str), "referencia": pe["concepto"].astype(str),
            "importe": pe["importe"], "rid": pe["_rid"]}),
        pd.DataFrame({
            "lado": "E",
            "origen": np.where(dif_comprobantes["_rid"].astype(str).str.match(r"^(PM\d|DIF-ACR-)"), "Período", "Diferencia de apareo"),
            "fecha": pd.to_datetime(dif_comprobantes["fecha"]),
            "categoria": dif_comprobantes["conciliacion"].astype(str),
            "concepto": np.where(dif_comprobantes["_rid"].astype(str).str.match(r"^(PM\d|DIF-ACR-)"),
                                 dif_comprobantes["conciliacion"].astype(str),
                                 "Diferencia " + dif_comprobantes["conciliacion"].astype(str)),
            "nombre": np.where(dif_comprobantes["_rid"].astype(str).str.match(r"^PM\d"),
                               dif_comprobantes.get("leyenda adicional1", pd.Series("", index=dif_comprobantes.index)).astype(str),
                               dif_comprobantes["descripcion"].astype(str)),
            "detalle": dif_comprobantes["descripcion"].astype(str),
            "referencia": dif_comprobantes["observaciones cliente"].astype(str),
            "importe": pd.to_numeric(dif_comprobantes["importe"], errors="coerce"),
            "rid": dif_comprobantes["_rid"]}),
    ]

    # ── Conciliación anterior ────────────────────────────────────────────────
    control = {"desde": desde, "hasta": hasta, "saldo_mayor_inicio": saldo_mayor_inicio,
               "saldo_mayor_final": saldo_mayor_final, "saldo_ext_inicio": saldo_ext_inicio,
               "saldo_ext_final": saldo_ext_final, "extracto_consistente": ext_ok,
               "dif_cruce": dif_cruce, "tiene_anterior": archivo_conciliacion_anterior is not None}
    ant, retro, pend_ant = None, None, pd.DataFrame()
    if archivo_conciliacion_anterior is not None:
        ant = leer_conciliacion_anterior(archivo_conciliacion_anterior, cuenta,
                                         saldo_ext_inicio=saldo_ext_inicio, saldo_mayor_inicio=saldo_mayor_inicio)
        cuenta = ant["cuenta"]
        # Sociedad: la indicada o la de la conciliación anterior; si no coinciden, se avisa
        razon_ant = ant.get("razon_social")
        if razon_social is None:
            razon_social = razon_ant
        elif razon_ant and _norm(razon_ant) != _norm(razon_social):
            control["aviso_sociedad"] = (f"La conciliación anterior es de '{razon_ant}' y se indicó "
                                         f"'{razon_social}'. Verificar que sea el archivo correcto.")
        control.update({"cuenta": cuenta, "criterio_cuenta": ant.get("criterio_cuenta"),
                        "otras_cuentas": ", ".join(ant.get("otras_cuentas") or [])})
        may_cat = categorizar_mayor_v3(categorizar_mayor_v2(categorizar_mayor_v1(may_full[~es_saldo_ant])))
        retro = analizar_retroactivos(may_cat, saldo_ini_archivo, desde, ant)
        misma_ventana = retro["modo"] == "mismo período"

        # Saldo del mayor al inicio del período, según la conciliación anterior
        if misma_ventana:
            saldo_ref = ant["saldo_anterior_mayor"]
        else:
            saldo_ref = ant["saldo_contable"]
        control.update({"modo": retro["modo"], "saldo_ref": saldo_ref,
                        "saldo_ext_ref": ant["saldo_extracto"] if not misma_ventana else None})

        # Pendientes que se arrastran: los de fecha anterior al período actual
        # (los del propio período los vuelve a calcular el cruce).
        lin = ant["pendientes"].copy()
        if not lin.empty:
            fecha_sin_dato = ant["hasta"] if ant["hasta"] is not None else desde - timedelta(days=1)
            lin["fecha_orig"] = lin["fecha"]
            lin["fecha"] = lin["fecha"].fillna(fecha_sin_dato)
            # Una fecha posterior al período que se concilia es un error de tipeo
            # (p. ej. EASA "Dif Sancor" 26/12/2026): esa línea es de arrastre, no del mes.
            futura = lin["fecha_orig"].notna() & (lin["fecha_orig"] > hasta)
            fechadas = lin[lin["fecha_orig"].notna() & ((lin["fecha_orig"] < desde) | futura)]
            sin_fecha = lin[lin["fecha_orig"].isna()]
            if misma_ventana and len(sin_fecha) and saldo_ref is not None and ant.get("diferencia") is not None:
                # Las líneas sin fecha pueden ser del mes en curso (las recalcula el cruce) o
                # arrastradas de meses anteriores. Se arrastran solo las que hacen que la
                # conciliación anterior cierre con su misma DIFERENCIA al inicio del período.
                d0 = saldo_ref - fechadas.loc[fechadas["lado"] == "M", "importe"].sum() \
                    + fechadas.loc[fechadas["lado"] == "E", "importe"].sum() - saldo_ext_inicio
                efectos = np.where(sin_fecha["lado"] == "E", sin_fecha["importe"], -sin_fecha["importe"])
                elegidas = _subconjunto_exacto(list(efectos), float(ant["diferencia"]) - d0) \
                    if abs(float(ant["diferencia"]) - d0) > TOL_CENTAVOS else []
                arrastre = sin_fecha.iloc[elegidas] if elegidas else sin_fecha.iloc[0:0]
                arrastre = arrastre.assign(fecha=desde - timedelta(days=1))
                lin = pd.concat([fechadas, arrastre])
            else:
                futura = lin["fecha_orig"].notna() & (lin["fecha_orig"] > hasta)
                lin = lin[(lin["fecha"] < desde) | futura].copy()
            lin["concepto"] = lin["concepto"].fillna("")
            pend_ant = lin
            partidas.append(pd.DataFrame({
                "lado": lin["lado"], "origen": "Pendiente anterior", "fecha": lin["fecha"],
                "categoria": lin["concepto"].map(_categoria_desde_concepto),
                "concepto": lin["concepto"].astype(str), "nombre": lin["obs"].astype(str),
                "detalle": lin["obs"].astype(str), "referencia": "Fila " + lin["fila"].astype(str) + " conc. anterior",
                "importe": lin["importe"], "rid": "P" + lin["fila"].astype(str)}))

        # Movimientos retroactivos identificados (sólo los anteriores al período:
        # los del propio período ya entran en el cruce)
        identificado = 0.0
        if not misma_ventana:
            r = retro["retro"]
            r = r[r["Fecha"] < desde]
            identificado += r["Importe"].sum()
            if len(r):
                partidas.append(pd.DataFrame({
                    "lado": "M", "origen": "Retroactivo", "fecha": r["Fecha"],
                    "categoria": r["conciliacion"].astype(str) if "conciliacion" in r else "0",
                    "concepto": "Asiento retroactivo", "nombre": r["Tercero"].astype(str),
                    "detalle": r["Comentario"].astype(str),
                    "referencia": "Asiento " + r["Asiento"].astype(int).astype(str), "importe": r["Importe"],
                    "rid": "R" + r["Asiento"].astype(int).astype(str) + "-" + r["Apunte"].astype(int).astype(str)}))
            b = retro["borrados"]
            if len(b):
                b = b[b["Fecha"] < desde]
                identificado -= b["Importe"].sum()
                partidas.append(pd.DataFrame({
                    "lado": "M", "origen": "Borrado", "fecha": b["Fecha"], "categoria": b["conciliacion"].astype(str),
                    "concepto": "Asiento borrado", "nombre": b["Tercero"].astype(str),
                    "detalle": b["Comentario"].astype(str),
                    "referencia": "Asiento " + b["Asiento"].astype(int).astype(str), "importe": -b["Importe"],
                    "rid": "B" + b["Asiento"].astype(int).astype(str)}))
            md = retro["modificados"]
            if len(md):
                md = md[md["Fecha_ant"] < desde].copy()
                # Efecto sobre el saldo ya conciliado:
                #  - si sigue con fecha ya conciliada, solo cuenta el cambio de importe
                #    (un cambio de fecha entre meses ya cerrados no altera el saldo y no se informa);
                #  - si se movió a una fecha del período actual, sale del saldo ya conciliado
                #    (la versión nueva entra al cruce del período).
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
                        "rid": "X" + md["Asiento"].astype(int).astype(str) + "-" + md["Apunte"].astype(int).astype(str)}))

        no_identificado = (saldo_mayor_inicio - saldo_ref - identificado) if saldo_ref is not None else 0.0

        # Asientos borrados o modificados en meses anteriores al de la conciliación anterior.
        # La foto de la conciliación anterior solo cubre su propio período; con conciliaciones
        # más viejas se busca la combinación ÚNICA de borrados/modificados que explica el cambio.
        if not misma_ventana and abs(no_identificado) > TOL_CENTAVOS and archivos_conciliaciones_previas:
            movs_act = may_cat.dropna(subset=["Asiento"]).copy()
            movs_act["Asiento"] = pd.to_numeric(movs_act["Asiento"], errors="coerce")
            movs_act["Apunte"] = pd.to_numeric(movs_act["Apunte"], errors="coerce")
            claves_act = set(zip(movs_act["Asiento"], movs_act["Apunte"]))
            cand, vistos = [], set()
            for arch in archivos_conciliaciones_previas:
                try:
                    sn = leer_conciliacion_anterior(arch, cuenta).get("snapshot_mayor")
                except Exception:
                    try:
                        sn = leer_conciliacion_anterior(arch).get("snapshot_mayor")
                    except Exception:
                        continue
                if sn is None or sn.empty:
                    continue
                sn = sn[sn["Fecha"] < ant["desde"]].drop_duplicates(["Asiento", "Apunte"])
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
                partidas.append(pd.DataFrame({
                    "lado": "M", "origen": "Borrado", "fecha": pd.to_datetime(b2["Fecha"]),
                    "categoria": b2["conciliacion"].astype(str) if "conciliacion" in b2 else "0",
                    "concepto": "Asiento borrado", "nombre": b2["Tercero"].astype(str),
                    "detalle": b2["Comentario"].astype(str),
                    "referencia": "Asiento " + b2["Asiento"].astype(int).astype(str),
                    "importe": -b2["Importe"].astype(float),
                    "rid": "B" + b2["Asiento"].astype(int).astype(str)}))
                retro["borrados"] = pd.concat([retro["borrados"], b2], ignore_index=True)
                identificado += sum(efectos[i] for i in sel)
                no_identificado = saldo_mayor_inicio - saldo_ref - identificado
        control.update({"retro_identificado": identificado, "retro_no_identificado": no_identificado})
        # Si el mayor cargado no llega al período de la conciliación anterior, lo tocado
        # para atrás no se puede ver: se avisa en forma explícita.
        primer_mov = movs["Fecha"].min() if len(movs) else None
        ini_ant = ant.get("desde") if ant.get("desde") is not None else desde
        detalle_ru = "Cambio del saldo inicial del mayor respecto de la conciliación anterior"
        mes_ant = pd.Timestamp(ant["mes"]) if ant.get("mes") is not None else ini_ant.replace(day=1)
        if abs(no_identificado) > TOL_CENTAVOS and primer_mov is not None and primer_mov > ini_ant:
            control["aviso_mayor"] = (
                f"El mayor cargado empieza el {primer_mov:%d/%m/%Y} y no incluye el período de la conciliación "
                f"anterior ({ini_ant:%d/%m/%Y} al {ant['hasta']:%d/%m/%Y}). Por eso no se puede ver qué se cargó, "
                f"borró o modificó con fecha ya conciliada, ni qué pendientes anteriores se registraron. "
                f"Volver a correr con el mayor bajado desde el {mes_ant:%d/%m/%Y} o antes.")
            detalle_ru = (f"Mayor cargado desde el {primer_mov:%d/%m/%Y}: no se puede identificar. "
                          f"Bajar el mayor desde el {mes_ant:%d/%m/%Y} o antes")
        if abs(no_identificado) > TOL_CENTAVOS:
            partidas.append(pd.DataFrame([{
                "lado": "M", "origen": "Retroactivo no identificado", "fecha": desde - timedelta(days=1),
                "categoria": "Retroactivo", "concepto": "Mov. retroactivos sin identificar",
                "nombre": "", "detalle": detalle_ru,
                "referencia": "Saldo anterior", "importe": no_identificado, "rid": "RU"}]))

        # Diferencia heredada: lo que no cerraba al inicio del período
        if saldo_ref is not None:
            arr = pend_ant
            saldo_ext_ref = saldo_ext_inicio
            d_ref = saldo_ref - (arr.loc[arr["lado"] == "M", "importe"].sum() if len(arr) else 0) \
                + (arr.loc[arr["lado"] == "E", "importe"].sum() if len(arr) else 0) - saldo_ext_ref
            control["dif_heredada"] = d_ref

    p = pd.concat(partidas, ignore_index=True)
    p["fecha"] = pd.to_datetime(p["fecha"], errors="coerce")
    p["importe"] = p["importe"].astype(float).round(2)
    if "comprobante" not in p:
        p["comprobante"] = ""
    p["comprobante"] = p["comprobante"].fillna("").astype(str)
    ap = Apareo(p)
    P = ap.p

    es_M, es_E = P["lado"] == "M", P["lado"] == "E"
    per = P["origen"] == "Período"
    ant_m = (P["origen"] == "Pendiente anterior") & es_M
    ant_e = (P["origen"] == "Pendiente anterior") & es_E
    retro_m = P["origen"].isin(["Retroactivo", "Borrado", "Modificado"])

    # R1. Mismo lado que se anula (asiento + borrado/reversión, pendiente anterior + borrado)
    ap.uno_a_uno(es_M & ~per, es_M, TOL_CENTAVOS, "Anulación en mayor")
    ap.anulacion_suma(ant_m | retro_m, TOL_CENTAVOS, "Anulación en mayor (suma)")
    # R2. Pendientes de banco anteriores ya registrados (retroactivo o en el período)
    ap.uno_a_uno(ant_e, retro_m, TOL_CRUCE, "Pendiente anterior registrado (retroactivo)")
    ap.uno_a_uno(ant_e, es_M & per, TOL_CRUCE, "Pendiente anterior registrado en el período")
    ap.combinacion(retro_m | (es_M & per), ant_e, TOL_CRUCE, "Pendientes anteriores registrados en un asiento", vinculo="categoria")
    ap.combinacion(ant_e, retro_m, TOL_CRUCE, "Pendiente anterior registrado en varios asientos", vinculo="ninguno")
    ap.categoria_vs_sin_categoria(ant_e, retro_m, TOL_CENTAVOS,
                                  "Pendientes anteriores de una categoría registrados juntos")
    ap.categoria_vs_sin_categoria(ant_e, es_M & per, TOL_CENTAVOS,
                                  "Pendientes anteriores de una categoría registrados en el período")
    ap.por_categoria_mes(ant_e, retro_m, TOL_CRUCE,
                         "Pendientes anteriores de una categoría y mes registrados con fecha ya conciliada")
    # Pendiente anterior del mayor corregido para atrás (modificación o devolución)
    # que así queda igual al pendiente del banco: se cierran los tres
    ap.par_mayor_vs_banco(ant_m | retro_m, ant_e, retro_m, TOL_CENTAVOS,
                          "Pendiente anterior corregido con asiento para atrás contra banco")
    # R2.b El cambio de saldo inicial sin identificar coincide con pendientes anteriores
    #      del banco (se registraron con fecha ya conciliada)
    # Un pendiente anterior del mayor y uno del banco por el mismo importe se compensan
    # (p. ej. cobranza registrada en abril y acreditada en mayo)
    ap.uno_a_uno(ant_m, ant_e, TOL_CENTAVOS, "Pendientes anteriores que se compensan entre sí")
    ap.combinacion(ant_m, ant_e, TOL_CENTAVOS, "Pendientes anteriores que se compensan entre sí (suma)",
                   vinculo="ninguno")
    ap.combinacion(ant_e, ant_m, TOL_CENTAVOS, "Pendientes anteriores que se compensan entre sí (suma)",
                   vinculo="ninguno")
    ru = P["origen"] == "Retroactivo no identificado"
    ap.explicar_cambio_saldo(ru, ant_e | ant_m,
                             "Cambio de saldo inicial explicado por pendientes anteriores registrados/anulados")
    # R3. Pendientes de mayor anteriores que impactaron en el banco este período
    ap.uno_a_uno(ant_m, es_E & per, TOL_CRUCE, "Pendiente anterior impactó en banco")
    ap.combinacion(ant_m, es_E & per, TOL_CRUCE, "Pendiente anterior impactó en banco (suma)", vinculo="categoria")
    # R4. Asientos retroactivos contra movimientos del banco del período
    #     (lo del propio período ya lo resolvió el cruce principal, match 0 a 19)
    ap.uno_a_uno(retro_m, es_E & per, TOL_RESIDUAL, "Asiento retroactivo contra banco del período",
                 max_dias=DIAS_RESIDUAL, exigir_vinculo=True)

    # R4.b Regla general: lo que se compensa por categoría y mes no va a la CC
    ap.compensacion_categoria_mes(ant_m | ant_e | retro_m, TOL_CRUCE,
                                  "Pendientes y asientos para atrás de una categoría y mes que se compensan")
    # R5. Gastos bancarios y préstamos de meses anteriores: si el total pendiente del mayor
    #     y el del banco se compensan (como hace la analista, "compensa con suma de importe"),
    #     se cierran todos juntos; la diferencia de centavos queda explicada.
    ap.bancarios_en_conjunto(ant_m, ant_e, TOL_CRUCE,
                             "Gastos bancarios y préstamos anteriores que se compensan en conjunto")

    P = ap.p
    conc = P[P["estado"] == "conciliado"]
    dif_redondeo = conc.loc[conc["lado"] == "M", "importe"].sum() - conc.loc[conc["lado"] == "E", "importe"].sum()
    control["dif_redondeo_apareo"] = dif_redondeo

    lineas_cc, detalle, dif_neteo = _armar_lineas_cc(P, hasta)
    control["dif_cruce"] = control["dif_cruce"] + dif_neteo
    total = saldo_mayor_final - lineas_cc.query("seccion=='ING_MAYOR'")["importe_hoja"].sum() \
        + lineas_cc.query("seccion=='SAL_MAYOR'")["importe_hoja"].sum() \
        + lineas_cc.query("seccion=='CRED_BANCO'")["importe_hoja"].sum() \
        - lineas_cc.query("seccion=='DEB_BANCO'")["importe_hoja"].sum()
    control["total_cc"] = total
    control["diferencia"] = total - saldo_ext_final

    estado_ant = _estado_pendientes_anteriores(pend_ant, P)
    buf = _exportar(lineas_cc, detalle, control, retro, estado_ant, bytes_cruce, P,
                    dict(razon_social=razon_social or "", banco=banco,
                         cuenta="",   # el N° de cuenta se deja vacío: solo importa la sociedad
                         analista=analista, mes=desde.replace(day=1)))
    stats = {k: control.get(k) for k in ("modo", "saldo_mayor_final", "saldo_ext_final", "diferencia",
                                          "dif_heredada", "retro_identificado", "retro_no_identificado",
                                          "dif_cruce", "dif_redondeo_apareo", "aviso_mayor")}
    stats = {k: (round(float(v), 2) if isinstance(v, (int, float, np.floating)) and v is not None else v)
             for k, v in stats.items()}
    stats["lineas_cc"] = len(lineas_cc)
    stats["pendientes_anteriores_registrados"] = int((estado_ant.get("estado", pd.Series()) == "Registrado").sum()) if len(estado_ant) else 0
    stats["pendientes_anteriores_vigentes"] = int((estado_ant.get("estado", pd.Series()) == "Vigente").sum()) if len(estado_ant) else 0
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

    # Partidas del período en categorías agrupadas: una línea neta por categoría
    agrupables = pend[(pend["origen"] == "Período") & pend["categoria"].isin(CATEGORIAS_AGRUPADAS)]
    for cat, g in agrupables.groupby("categoria"):
        etiqueta = CATEGORIAS_AGRUPADAS[cat]
        etiqueta = f"{etiqueta} {sufijo}" if etiqueta in ("GB", "Prestamo", "Plan Afip") else etiqueta
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
        agregar(sec, hasta, etiqueta, abs(efecto), obs, g, "Período")
    resto = pend.drop(agrupables.index)

    # Asientos cargados con fecha ya conciliada de categorías agrupadas (GB, Préstamo,
    # etc.): una línea por categoría y mes, como lo presenta el equipo (p. ej. "GB 07-26")
    retro_agr = resto[(resto["origen"] == "Retroactivo") & resto["categoria"].isin(CATEGORIAS_AGRUPADAS)]
    # el mes sale del comentario ("GB 07-2026", "Pago GB 08-26"); si no tiene, de la fecha
    mes_com = retro_agr["detalle"].astype(str).str.extract(r"(?<!\d)(0[1-9]|1[0-2])[-/](?:20)?(2\d)(?!\d)")
    mes_ret = (mes_com[0] + "-" + mes_com[1]).where(mes_com[0].notna(), retro_agr["fecha"].dt.strftime("%m-%y"))
    for (cat, mes), g in retro_agr.groupby([retro_agr["categoria"], mes_ret]):
        total = g["importe"].sum()
        if abs(total) < TOL_LINEA_DIFERENCIA:
            continue
        etiqueta = f"{CATEGORIAS_AGRUPADAS[cat]} {mes} cargado con fecha ya conciliada"
        agregar(_seccion("M", total), g["fecha"].max(), etiqueta, abs(total),
                f"{len(g)} asientos: " + ", ".join(g["referencia"].astype(str).str.replace("Asiento ", "")), g, "Retroactivo")
    resto = resto.drop(retro_agr.index)

    for _, f in resto.sort_values(["origen", "fecha"]).iterrows():
        sec = _seccion(f["lado"], f["importe"])
        if f["origen"] == "Período":
            concepto = f["concepto"] if f["concepto"] not in ("0", "nan") else "Sin categoría"
            obs = f["nombre"] if f["nombre"] not in ("nan", "", "None") else f["detalle"]
        elif f["origen"] == "Pendiente anterior":
            concepto, obs = f["concepto"], (f["detalle"] if f["detalle"] not in ("nan", "None") else "")
        elif f["origen"] == "Borrado":
            # Un asiento borrado deja el movimiento del banco que conciliaba sin registrar:
            # se presenta del lado del banco, como lo hace el equipo (mismo efecto en el total).
            sec = "CRED_BANCO" if f["importe"] < 0 else "DEB_BANCO"
            ref = str(f["referencia"]).replace("Asiento ", "")
            tipo = "Crédito" if sec == "CRED_BANCO" else "Débito"
            concepto = f"{tipo} del banco sin registrar (asiento {ref} borrado)"
            obs = f["nombre"] if f["nombre"] not in ("nan", "", "None") else f["detalle"]
        else:
            concepto = f"{f['concepto']} ({f['referencia']})" if f["referencia"] else f["concepto"]
            obs = f["nombre"] if f["nombre"] not in ("nan", "", "None") else f["detalle"]
        agregar(sec, f["fecha"], concepto, abs(f["importe"]), obs, pd.DataFrame([f]), f["origen"])

    lineas = pd.DataFrame(lineas)
    orden = {"Pendiente anterior": 0, "Retroactivo no identificado": 1, "Retroactivo": 1, "Borrado": 1,
             "Modificado": 1, "Período": 2, "Diferencia de apareo": 2}
    lineas["_o"] = lineas["origen"].map(orden)
    lineas = lineas.sort_values(["seccion", "_o", "fecha"]).drop(columns="_o")
    return lineas, pd.DataFrame(det), -sum(centavos_neteo)


def _estado_pendientes_anteriores(pend_ant, P):
    if pend_ant is None or pend_ant.empty:
        return pd.DataFrame()
    pa = P[P["origen"] == "Pendiente anterior"]
    out = []
    for _, f in pa.iterrows():
        if f["estado"] == "conciliado":
            grupo = P[(P["grupo"] == f["grupo"]) & (P["rid"] != f["rid"])]
            contra = grupo[grupo["origen"] != "Pendiente anterior"]
            if len(grupo) > len(contra):
                contra = pd.concat([contra, pd.DataFrame([{
                    "origen": f"junto con {len(grupo) - len(contra)} pendiente(s) anterior(es)",
                    "referencia": "", "fecha": pd.NaT, "importe": np.nan}])])
            desc = "; ".join(
                f"{r['origen']} {r['referencia']} {r['fecha']:%d/%m/%Y} {r['importe']:,.2f}".strip()
                if pd.notna(r["fecha"]) else str(r["origen"]) for _, r in contra.iterrows())
            out.append({**f[["fecha", "concepto", "detalle", "importe"]].to_dict(),
                        "estado": "Registrado", "regla": f["regla"], "contrapartida": desc})
        else:
            out.append({**f[["fecha", "concepto", "detalle", "importe"]].to_dict(),
                        "estado": "Vigente", "regla": "", "contrapartida": ""})
    return pd.DataFrame(out)


# ─────────────────────────────────────────────────────────────────────────────
# 7. EXPORTACIÓN
# ─────────────────────────────────────────────────────────────────────────────

FMT = "#,##0.00"
NEGRITA = Font(bold=True)
TITULO = Font(bold=True, size=12)
GRIS = PatternFill("solid", fgColor="D9D9D9")
AMARILLO = PatternFill("solid", fgColor="FFF2CC")
ROJO = PatternFill("solid", fgColor="F8CBAD")
VERDE = PatternFill("solid", fgColor="C6EFCE")
FINA = Side(style="thin", color="999999")


# Paleta y formatos de la plantilla de conciliación de Contaduría
# (colores del tema Office 2007 que usa la plantilla, convertidos a RGB)
CC_ROSA = PatternFill("solid", fgColor="D99694")        # rótulos: título, saldos, diferencia
CC_ROSA_MEDIO = PatternFill("solid", fgColor="E6B9B8")  # cuenta / retroactivos
CC_ROSA_CLARO = PatternFill("solid", fgColor="F2DCDB")  # importes de saldo
CC_LILA = PatternFill("solid", fgColor="E6E0EC")        # importe de la diferencia
CC_AMARILLO = PatternFill("solid", fgColor="FFFF00")    # pendiente de conciliación anterior
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
    """Agrega lados al borde de una celda conservando los que ya tiene."""
    b = c.border
    c.border = Border(left=left if left is not None else b.left, right=right if right is not None else b.right,
                      top=top if top is not None else b.top, bottom=bottom if bottom is not None else b.bottom)


def _cc_marco(ws, r1, c1, r2, c2, lado=_MED):
    """Marco exterior de un rango."""
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
    anchos = {"A": 3.9, "B": 19.6, "C": 20.9, "D": 34, "E": 18.7, "F": 34.8,
              "G": 13.2, "H": 7.6, "I": 15.4, "J": 6, "K": 14.6}
    for col, w in anchos.items():
        ws.column_dimensions[col].width = w
    hasta = pd.Timestamp(control["hasta"])

    # ── Encabezado (B2:C10) ──────────────────────────────────────────────
    ws.merge_cells("B2:C2")
    c = ws["B2"]; c.value = f"{meta['razon_social']}  - Conciliacion Bancaria"
    c.font = CC_F10BU; c.fill = CC_ROSA; c.alignment = Alignment(horizontal="left")
    mes = meta["mes"]
    if isinstance(mes, pd.Timestamp):
        mes = mes.replace(day=1)
    datos = [("Razón social:", meta["razon_social"], None), ("País:", "ARGENTINA", None),
             ("Mes:", mes, "mmm-yy"), ("Banco:", meta["banco"], None),
             ("Cuenta Contable N°:", meta["cuenta"], None),
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

    # ── Referencias (J2:K5) ──────────────────────────────────────────────
    ws["J2"] = "Referencia:"; ws["J2"].font = CC_F8B
    for i, (fill, txt) in enumerate([(CC_AMARILLO, "PENDIENTE DE CONCILIACIÓN ANTERIOR"),
                                     (CC_ROSA_MEDIO, "CARGADO / MODIFICADO / BORRADO CON FECHA YA CONCILIADA")],
                                    start=3):
        _cc_caja(ws, f"J{i}", None, fill)
        ws.cell(i, 11, txt).font = CC_F8B
    _cc_caja(ws, "J5", None, None)
    ws.cell(5, 11, "SIN COLOR: PENDIENTE DEL PERÍODO").font = CC_F8B

    # ── Saldo contable ──────────────────────────────────────────────────
    caja_ini = 13
    r = 14
    ws.merge_cells(start_row=r, start_column=2, end_row=r, end_column=4)
    c = ws.cell(r, 2, "Saldo Contable al:"); c.font = CC_F10B; c.fill = CC_ROSA
    c.alignment = Alignment(horizontal="center")
    _cc_marco(ws, r, 2, r, 4)
    c = ws.cell(r, 5, hasta); c.font = CC_F8B; c.number_format = '[$-C0A]d" de "mmmm" de "yyyy;@'
    _cc_caja(ws, f"I{r}", round(control["saldo_mayor_final"], 2), CC_ROSA_CLARO, CC_F8B, CC_NUM)
    fila_saldo = r
    refs = {}
    r += 2

    # ── Secciones ───────────────────────────────────────────────────────
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
            fill = None
            if l["origen"] in ("Retroactivo", "Borrado", "Modificado", "Retroactivo no identificado"):
                fill = CC_ROSA_MEDIO
            elif l["origen"] == "Pendiente anterior":
                fill = CC_AMARILLO
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

    # ── Total, saldo extracto y diferencia ──────────────────────────────
    ws.cell(r, 3, "Total ").font = CC_F8B
    _cc_caja(ws, f"I{r}",
             f"=+I{fila_saldo}-{refs['ING_MAYOR']}+{refs['SAL_MAYOR']}+{refs['CRED_BANCO']}-{refs['DEB_BANCO']}",
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

    # ── Firmas ──────────────────────────────────────────────────────────
    r += 2
    ws.cell(r + 1, 2, "Confeccionado por:").font = CC_F8
    ws.cell(r + 1, 6, "Revisado por:").font = CC_F8
    ws.cell(r + 3, 2, "Fecha:").font = CC_F8
    ws.cell(r + 3, 6, "Fecha:").font = CC_F8
    _cc_marco(ws, r, 2, r + 4, 5)
    _cc_marco(ws, r, 6, r + 4, 9)
    r += 7

    # ── Explicación de la diferencia (control propio del proceso) ───────
    ws.cell(r, 6, "Explicación de la diferencia").font = CC_F8B
    expl = [("Diferencia heredada de la conciliación anterior", control.get("dif_heredada", 0.0) or 0.0),
            ("Diferencias de centavos en cruces del período", control.get("dif_cruce", 0.0) or 0.0),
            ("Diferencias de centavos en apareos (pendientes/residual)", control.get("dif_redondeo_apareo", 0.0))]
    r0 = r
    ini = r + 1
    for k, v in expl:
        r += 1
        ws.cell(r, 6, k).font = CC_F8
        c = ws.cell(r, 9, round(v, 2)); c.number_format = CC_NUM; c.font = CC_F8
    r += 1
    ws.cell(r, 6, "Sin explicar (debe ser 0)").font = CC_F8B
    c = _cc_caja(ws, f"I{r}", f"=ROUND(I{fila_total}-I{fila_ext}-SUM(I{ini}:I{r-1}),2)", CC_LILA, CC_F8B, CC_NUM)
    _cc_marco(ws, r0, 6, r, 9)

    # Celdas restantes del área en Arial 8, como la plantilla
    for fila in ws.iter_rows(min_row=1, max_row=r, min_col=1, max_col=11):
        for c in fila:
            if c.font is None or c.font.name != "Arial":
                c.font = CC_F8


def _df_a_hoja(ws, df, anchos=None):
    df = df.reset_index(drop=True).copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.Series([None if pd.isna(v) else v.to_pydatetime() for v in df[col]],
                                index=df.index, dtype=object)
    ws.append(list(df.columns))
    for c in ws[1]:
        c.font = NEGRITA; c.fill = GRIS
    for fila in df.itertuples(index=False):
        ws.append([None if (isinstance(v, float) and np.isnan(v)) or v is pd.NaT else v for v in fila])
    for j, col in enumerate(df.columns, start=1):
        letra = get_column_letter(j)
        ws.column_dimensions[letra].width = (anchos or {}).get(col, max(10, min(45, len(str(col)) + 4)))
        if pd.api.types.is_numeric_dtype(df[col]) and col.lower() in ("importe", "debe", "haber", "saldo", "debitos",
                                                                      "creditos", "importe_hoja", "importe_ant",
                                                                      "importe_act"):
            for i in range(2, ws.max_row + 1):
                ws.cell(i, j).number_format = FMT
        if pd.api.types.is_datetime64_any_dtype(df[col]) or col.lower().startswith("fecha"):
            for i in range(2, ws.max_row + 1):
                ws.cell(i, j).number_format = "dd/mm/yyyy"
    ws.freeze_panes = "A2"


def _hoja_control(ws, control, retro):
    """Explica en lenguaje llano los dos controles: si cambió el mayor ya conciliado
    y por qué la hoja CC da la diferencia que da."""
    ws.column_dimensions["A"].width = 78
    ws.column_dimensions["B"].width = 20
    tiene_ant = control.get("tiene_anterior")
    filas = [
        ("0. SOCIEDAD Y CUENTA", None),
        ("Cuenta conciliada (bloque de la conciliación anterior)", control.get("cuenta")),
        ("Cómo se eligió la cuenta", control.get("criterio_cuenta")),
        ("Otras cuentas en la conciliación anterior (no incluidas en esta corrida)", control.get("otras_cuentas") or "-"),
        ("Aviso", " / ".join(x for x in (control.get("aviso_sociedad"), control.get("aviso_mayor")) if x) or "-"),
        ("", None),
        ("1. ¿SE TOCÓ EL MAYOR DE PERÍODOS YA CONCILIADOS?", None),
        ("Saldo del mayor al inicio del período, según la conciliación anterior", control.get("saldo_ref")),
        ("Saldo del mayor al inicio del período, según el mayor que se cargó hoy", control["saldo_mayor_inicio"]),
        ("Diferencia = asientos cargados, borrados o modificados con fecha ya conciliada",
         (control["saldo_mayor_inicio"] - control["saldo_ref"]) if tiene_ant and control.get("saldo_ref") is not None else None),
        ("   identificada asiento por asiento (detalle más abajo)", control.get("retro_identificado")),
        ("   sin identificar (va a la CC en rojo; se identifica cargando el mayor desde el "
         "inicio del período anterior)", control.get("retro_no_identificado")),
        ("", None),
        ("2. ¿CIERRA LA CONCILIACIÓN?", None),
        ("Saldo contable al cierre (mayor)", control["saldo_mayor_final"]),
        ("Saldo del extracto al cierre", control["saldo_ext_final"]),
        ("Total de la hoja CC (saldo contable ± pendientes)", control["total_cc"]),
        ("DIFERENCIA (Total CC - saldo extracto)", control["diferencia"]),
        ("   heredada: ya estaba en la conciliación anterior", control.get("dif_heredada")),
        ("   centavos en el cruce principal (match 0 a 19) y neteos menores a 1 peso", control.get("dif_cruce")),
        ("   centavos al cruzar pendientes anteriores (hoja Cruces pend. anteriores)", control.get("dif_redondeo_apareo")),
        ("Saldo del extracto al inicio del período, según la conciliación anterior", control.get("saldo_ext_ref")),
        ("Saldo del extracto al inicio del período, según el extracto que se cargó hoy", control["saldo_ext_inicio"]),
        ("Extracto consistente (saldo inicial + movimientos = saldo final)",
         "Sí" if control["extracto_consistente"] else "NO"),
    ]
    for i, (k, v) in enumerate(filas, start=1):
        a = ws.cell(i, 1, k)
        a.alignment = Alignment(wrap_text=True)
        if v is None and k and k[0].isdigit():
            a.font = NEGRITA
        b = ws.cell(i, 2, round(v, 2) if isinstance(v, float) else v)
        if isinstance(v, (int, float)):
            b.number_format = FMT
    if abs(control.get("retro_no_identificado") or 0) > TOL_CENTAVOS:
        fila_ru = next(i for i, (k, _) in enumerate(filas, start=1) if k.strip().startswith("sin identificar"))
        ws.cell(fila_ru, 2).fill = ROJO
    if control.get("aviso_sociedad") or control.get("aviso_mayor"):
        ws.cell(5, 2).fill = ROJO
        ws.cell(5, 2).alignment = Alignment(wrap_text=True)
        ws.column_dimensions["B"].width = 60
    r = len(filas) + 3
    if retro is not None:
        partes = []
        for nombre, df in (("Cargado con fecha ya conciliada", retro["retro"]), ("Borrado", retro["borrados"])):
            if df is not None and len(df):
                d = df[[c for c in ["Asiento", "Apunte", "Fecha", "Comentario", "Serie", "Tercero", "Importe"]
                        if c in df.columns]].copy()
                d.insert(0, "Tipo", nombre)
                partes.append(d)
        md = retro.get("modificados")
        if md is not None and len(md) and "_efecto" in md:
            d = pd.DataFrame({"Tipo": "Modificado", "Asiento": md["Asiento"], "Apunte": md["Apunte"],
                              "Fecha": md["Fecha_ant"], "Comentario": md.get("Comentario_act"),
                              "Serie": md.get("Serie_act"), "Tercero": md.get("Tercero_act"),
                              "Importe": md["_efecto"]})
            partes.append(d)
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
            for j, w in zip("CDEFGH", (8, 12, 26, 8, 38, 16)):
                ws.column_dimensions[j].width = w
        else:
            ws.cell(r, 1, "No se detectaron (o el mayor cargado no cubre el período anterior).")


def _exportar(lineas, detalle, control, retro, estado_ant, bytes_cruce, P, meta):
    """Libro final = hojas del cruce tal cual las genera logica_galicia (Faltante Mayor,
    Faltante Extracto, Match Mayor, Match Extracto, Extracto_cat) + la hoja CC
    adelante + tres hojas de soporte."""
    from openpyxl import load_workbook
    wb = load_workbook(BytesIO(bytes_cruce))
    ws = wb.create_sheet("CC", 0)
    _hoja_cc(ws, lineas, control, meta)
    _hoja_control(wb.create_sheet("Control", 1), control, retro)
    pos = 2
    if len(estado_ant):
        _df_a_hoja(wb.create_sheet("Pendientes anteriores", pos), estado_ant,
                   {"concepto": 22, "detalle": 40, "contrapartida": 70, "regla": 40})
        pos += 1
    adic = P[(P["estado"] == "conciliado")].sort_values(["grupo", "lado"])
    if len(adic):
        adic = adic.rename(columns={"grupo": "grupo", "regla": "regla aplicada", "lado": "lado (M=mayor, E=extracto)"})
        _df_a_hoja(wb.create_sheet("Cruces pend. anteriores", pos),
                   adic[["grupo", "regla aplicada", "origen", "lado (M=mayor, E=extracto)", "fecha", "categoria",
                         "nombre", "referencia", "importe"]],
                   {"regla aplicada": 45, "nombre": 36, "referencia": 30, "lado (M=mayor, E=extracto)": 12})
    wb.active = 0
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# ENTRADA PARA LA APP
# ─────────────────────────────────────────────────────────────────────────────

def correr_conciliacion_galicia(archivo_mayor, archivo_extracto, archivo_proveedores=None,
                                archivo_conciliacion_anterior=None, analista="", razon_social=None,
                                banco="Galicia", cuenta=None, archivos_conciliaciones_previas=None):
    """Cruce + conciliación completa.
    Devuelve (bytes del Excel, stats). El Excel tiene la hoja CC, Control,
    Pendientes anteriores, Cruces pend. anteriores y las 5 hojas del cruce."""
    return correr_conciliacion_completa(
        archivo_mayor, archivo_extracto,
        archivo_conciliacion_anterior=archivo_conciliacion_anterior,
        archivo_proveedores=archivo_proveedores, analista=analista,
        razon_social=razon_social, banco=banco, cuenta=cuenta,
        archivos_conciliaciones_previas=archivos_conciliaciones_previas)
