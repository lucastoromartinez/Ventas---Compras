import re
import unicodedata
from io import BytesIO
from itertools import combinations

import numpy as np
import pandas as pd
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.utils import get_column_letter


# ─────────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────────

def load_excel_file(file) -> pd.DataFrame:
    return pd.read_excel(file, dtype=str)


# ─────────────────────────────────────────────
# SOCIEDAD (por CUIT del receptor)
# ─────────────────────────────────────────────

SOCIEDADES_POR_CUIT = {
    "30716161591": "ersa",
    "30718257510": "easa",
    "30716981076": "9dD",
    "30717516288": "ronda",
    "30719170699": "patagonia",
}


def _cuit_digitos(valor) -> str:
    """Deja solo los dígitos de un CUIT (tolera guiones, espacios y '.0' final)."""
    txt = str(valor).strip()
    txt = re.sub(r"\.0+$", "", txt)
    return re.sub(r"\D", "", txt)


def _norm_texto(valor) -> str:
    txt = unicodedata.normalize("NFKD", str(valor))
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    return txt.lower()


def _sociedad_mas_frecuente(valores) -> str | None:
    sociedades = pd.Series([SOCIEDADES_POR_CUIT.get(_cuit_digitos(v)) for v in valores]).dropna()
    if sociedades.empty:
        return None
    return sociedades.value_counts().idxmax()


def detectar_sociedad(df_arca: pd.DataFrame) -> str | None:
    """
    Detecta la sociedad a partir del CUIT del receptor del Excel de ARCA.
    Orden de búsqueda:
      1. Columnas que contengan "receptor" (prioriza "Nro. Doc. Receptor").
      2. Encabezados del archivo (fila de título "... - CUIT 30-xxxxxxxx-x").
      3. Cualquier celda que no sea de columnas del emisor.
    Devuelve el nombre de la sociedad más frecuente, o None si no se reconoce.
    """
    columnas = [c for c in df_arca.columns if "receptor" in _norm_texto(c)]
    columnas.sort(key=lambda c: "doc" not in _norm_texto(c))

    for col in columnas:
        sociedad = _sociedad_mas_frecuente(df_arca[col].dropna())
        if sociedad:
            return sociedad

    patron_cuit = re.compile(r"\d{2}-?\d{8}-?\d")
    encontrados = [m for c in df_arca.columns for m in patron_cuit.findall(str(c))]
    sociedad = _sociedad_mas_frecuente(encontrados)
    if sociedad:
        return sociedad

    resto = [c for c in df_arca.columns if "emisor" not in _norm_texto(c)]
    valores = df_arca[resto].astype(str).to_numpy().ravel() if resto else []
    return _sociedad_mas_frecuente(valores)


# ─────────────────────────────────────────────
# DEPURACIÓN SISTEMA
# ─────────────────────────────────────────────

def depurar_sistema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Depura el DataFrame del sistema (robusto a variaciones de nombres de columnas:
    mayúsculas/minúsculas, tildes, puntos, espacios, guiones).

    - Normaliza CUIT: elimina guiones/espacios
    - Divide Nro. en "Pto. Venta" y "N°Comprobante" (split por guion, sin ceros a izquierda)
      Maneja: sin guion, más de un guion, em/en dash, espacios alrededor del guion,
      y Pto. Venta vacío por error de carga
    - Filtra filas donde Tipo Doc. == 'Factura Gastos' (robusto)
    - Convierte columnas de importes a float
    - Dropea columnas originales CUIT y Nro. (las reales detectadas)
    """

    df = df.copy()
    df.columns = df.columns.astype(str).str.strip()

    def _norm_col(s: str) -> str:
        s = str(s)
        s = s.replace("\u00a0", " ")
        s = s.strip().lower()
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = re.sub(r"[.\-_/]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    norm_map: dict[str, str] = {}
    for c in df.columns:
        k = _norm_col(c)
        norm_map.setdefault(k, c)

    def _resolve(candidates: list[str], required: bool = True) -> str | None:
        for cand in candidates:
            k = _norm_col(cand)
            if k in norm_map:
                return norm_map[k]
        if required:
            raise KeyError(
                f"depurar_sistema: no pude resolver columna. "
                f"Candidatos={candidates}. Disponibles={list(df.columns)}"
            )
        return None

    c_cuit     = _resolve(["CUIT", "Cuit", "CUIT ", "C.U.I.T", "C U I T", "CUIT/CUIL"])
    c_nro      = _resolve(["Nro.", "Nro", "Numero", "Número", "N°", "Nro comprobante", "Nro Comprobante"])
    c_tipo_doc = _resolve(["Tipo Doc.", "Tipo Doc", "Tipo Documento", "Tipo de Documento"], required=False)

    df["CUIT_norm"] = (
        df[c_cuit]
        .astype(str)
        .str.replace("-", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.strip()
        .astype(str)
    )

    def _parse_nro(s: str) -> tuple[str, str]:
        s = str(s).strip()
        s = s.replace('\u2212', '-').replace('\u2013', '-')  # em dash / en dash
        s = re.sub(r'\s*-\s*', '-', s)
        count = s.count('-')
        if count == 0:
            left, right = s[:4], s[4:]
        elif count == 1:
            left, right = s.split('-', 1)
        else:
            idx   = s.index('-')
            left  = s[:idx]
            right = s[idx + 1:].replace('-', '')
        pto  = left.lstrip('0')
        comp = right.lstrip('0')
        if pto == '' and len(right) >= 3 and right[0] != '0' and right[1:3] == '00':
            pto  = right[0]
            comp = right[1:].lstrip('0')
        return pto, comp

    parsed = df[c_nro].astype(str).str.strip().apply(_parse_nro)

    df["Pto. Venta"]    = parsed.apply(lambda x: x[0]).astype(str)
    df["N°Comprobante"] = parsed.apply(lambda x: x[1]).astype(str)

    importes_aliases: dict[str, list[str]] = {
        "Imp. Neto Gravado":    ["Imp. Neto Gravado", "Imp Neto Gravado", "Neto Gravado", "Neto Grav"],
        "Imp. Neto No Gravado": ["Imp. Neto No Gravado", "Imp Neto No Gravado", "Neto No Gravado", "No Gravado"],
        "IVA 10,5%":            ["IVA 10,5%", "IVA 10.5%", "IVA 10,5", "IVA 10.5", "IVA 10"],
        "IVA 21%":              ["IVA 21%", "IVA 21", "IVA21"],
        "IVA 27%":              ["IVA 27%", "IVA 27", "IVA27"],
        "Imp. Int.":            ["Imp. Int.", "Imp Int", "Impuestos Internos", "Imp Internos"],
        "Perc. Gcias.":         ["Perc. Gcias.", "Perc Gcias", "Percepcion Ganancias", "Perc. Ganancias"],
        "Perc. IVA":            ["Perc. IVA", "Perc IVA", "Percepcion IVA"],
        "Perc. IIBB CABA":      ["Perc. IIBB CABA", "Perc IIBB CABA", "Percep IIBB CABA", "IIBB CABA"],
        "Perc. IIBB BS AS":     ["Perc. IIBB BS AS", "Perc IIBB BS AS", "Perc. IIBB Bs As", "IIBB BS AS", "IIBB Buenos Aires"],
        "Perc. SUSS":           ["Perc. SUSS", "Perc SUSS", "Percepcion SUSS", "SUSS"],
        "SIRCREB":              ["SIRCREB", "Sircreb"],
        "Total":                ["Total", "Importe Total", "Total Comprobante", "Total Factura"],
    }

    col_importes_reales: dict[str, str] = {}
    for canon, aliases in importes_aliases.items():
        col = _resolve(aliases, required=False)
        if col is not None:
            col_importes_reales[canon] = col

    for canon, col in col_importes_reales.items():
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    if c_tipo_doc is not None:
        tipo_norm = df[c_tipo_doc].astype(str).str.strip().str.lower()
        df = df[~tipo_norm.eq("factura gastos")].copy()

    df = df.drop(columns=[c_cuit, c_nro], errors="ignore")

    return df


# ─────────────────────────────────────────────
# DEPURACIÓN ARCA
# ─────────────────────────────────────────────

def depurar_arca(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.columns = df.columns.str.strip()

    df = df.rename(columns={
        "Punto de Venta": "Pto. Venta",
        "NÃºmero Desde":  "N°Comprobante",
    })

    df = df.drop(columns=["NÃºmero Hasta"], errors="ignore")

    df["Nro. Doc. Emisor"] = df["Nro. Doc. Emisor"].astype(str).str.strip()

    columnas_importe = [
        "Imp. Neto Gravado IVA 0%",
        "IVA 2,5%",
        "Imp. Neto Gravado IVA 2,5%",
        "IVA 5%",
        "Imp. Neto Gravado IVA 5%",
        "IVA 10,5%",
        "Imp. Neto Gravado IVA 10,5%",
        "IVA 21%",
        "Imp. Neto Gravado IVA 21%",
        "IVA 27%",
        "Imp. Neto Gravado IVA 27%",
        "Imp. Neto Gravado Total",
        "Imp. Neto No Gravado",
        "Imp. Op. Exentas",
        "Otros Tributos",
        "Total IVA",
        "Imp. Total",
    ]
    columnas_importe = [c for c in columnas_importe if c in df.columns]

    df[columnas_importe] = (
        df[columnas_importe]
        .apply(pd.to_numeric, errors="coerce")
        .astype(float)
    )

    if (
        "Tipo de Comprobante" in df.columns
        and "Imp. Neto No Gravado" in df.columns
        and "Imp. Total" in df.columns
    ):
        tipo_tmp = pd.to_numeric(df["Tipo de Comprobante"].astype(str).str.strip(), errors="coerce")
        mask_111213 = tipo_tmp.isin([11, 12, 13])

        mask_fill = mask_111213 & (
            df["Imp. Neto No Gravado"].isna() | (df["Imp. Neto No Gravado"] == 0)
        )

        df.loc[mask_fill, "Imp. Neto No Gravado"] = df.loc[mask_fill, "Imp. Total"]

    if "Tipo Cambio" in df.columns:
        df["Tipo Cambio"] = pd.to_numeric(df["Tipo Cambio"], errors="coerce")

        mask_tc = df["Tipo Cambio"].notna() & (df["Tipo Cambio"] != 1)

        for col in columnas_importe:
            df.loc[mask_tc, col] = df.loc[mask_tc, col] * df.loc[mask_tc, "Tipo Cambio"]

    df["Tipo de Comprobante"] = pd.to_numeric(
        df["Tipo de Comprobante"].astype(str).str.strip(),
        errors="coerce"
    )

    mask_nc = df["Tipo de Comprobante"].isin([3, 8, 13, 53, 58, 63])
    df.loc[mask_nc, columnas_importe] *= -1

    if "Imp. Neto Gravado IVA 0%" in df.columns:
        df["Imp. Neto Gravado IVA 0%"] = pd.to_numeric(df["Imp. Neto Gravado IVA 0%"], errors="coerce")

    if "Imp. Neto No Gravado" in df.columns:
        df["Imp. Neto No Gravado"] = pd.to_numeric(df["Imp. Neto No Gravado"], errors="coerce")

    return df


# ─────────────────────────────────────────────
# CRUCE 1: por Pto. Venta + N°Comprobante + CUIT
# ─────────────────────────────────────────────

def cruce1(df_arca_dep: pd.DataFrame, df_sistema_dep: pd.DataFrame, tolerancia_total: float = 1.0):
    sis  = df_sistema_dep.copy().reset_index(drop=True)
    arca = df_arca_dep.copy().reset_index(drop=True)

    sis.columns  = sis.columns.str.strip()
    arca.columns = arca.columns.str.strip()

    sis["_idx_sis"]   = sis.index
    arca["_idx_arca"] = arca.index

    sis["_total_sis"]   = pd.to_numeric(sis["Total"],       errors="coerce")
    arca["_total_arca"] = pd.to_numeric(arca["Imp. Total"], errors="coerce")

    # Join many-to-many por Pto. Venta + N°Comprobante + CUIT
    cand = pd.merge(
        sis,
        arca,
        left_on  =["Pto. Venta", "N°Comprobante", "CUIT_norm"],
        right_on =["Pto. Venta", "N°Comprobante", "Nro. Doc. Emisor"],
        how      ="inner",
        suffixes =("_sis", "_arca"),
    )

    # Mismo Pto. Venta + N°Comprobante + CUIT ya es matchear: no se filtra
    # por importe acá (la tolerancia de importe es un criterio de
    # desambiguación para los cruces posteriores, que cruzan con menos
    # precisión -cruce2 sin Pto. Venta, cruce3 sin CUIT o sin N°Comprobante-,
    # no para descartar un match de clave exacta). Cualquier diferencia de
    # importe sobre un match así queda reflejada después en "revisar" vía
    # revisar_inconsistencias_en_match (comentario "Total").
    cand["_diff_total"] = (cand["_total_sis"] - cand["_total_arca"]).abs()

    # Resolver 1-a-1: entre duplicados de la misma clave, tomar el par con menor diferencia de importe
    cand_ok = cand.sort_values("_diff_total")
    usados_sis, usados_arca = set(), set()
    matched_sis_idx, matched_arca_idx = [], []

    for _, row in cand_ok.iterrows():
        i_sis  = int(row["_idx_sis"])
        i_arca = int(row["_idx_arca"])
        if i_sis not in usados_sis and i_arca not in usados_arca:
            usados_sis.add(i_sis)
            usados_arca.add(i_arca)
            matched_sis_idx.append(i_sis)
            matched_arca_idx.append(i_arca)

    sis_rename = {
        "Fecha":               "Fecha_Sistema",
        "Pto. Venta":          "Pto. Venta_sistema",
        "N°Comprobante":       "N°Comprobante_sistema",
        "CUIT_norm":           "cuit_sistema",
        "Imp. Neto Gravado":   "Gravado_sistema",
        "Imp. Neto No Gravado":"No Gravado_sistema",
        "Total":               "Imp. Total_sistema",
    }

    arca_cols_rename = {
        "Fecha de EmisiÃ³n":      "Fecha_arca",
        "Pto. Venta":             "Pto. Venta_arca",
        "N°Comprobante":          "N°Comprobante_arca",
        "Nro. Doc. Emisor":       "cuit_arca",
        "Imp. Neto No Gravado":   "No gravado_arca",
        "Imp. Neto Gravado Total":"Gravado_arca",
        "Imp. Op. Exentas":       "Exentas_arca",
        "Otros Tributos":         "Otros Tributos_arca",
        "Imp. Total":             "Imp. Total_arca",
    }

    arca_cols_available = [c for c in arca_cols_rename if c in arca.columns]

    temp_cols_sis  = ["_idx_sis",  "_total_sis"]
    temp_cols_arca = ["_idx_arca", "_total_arca"]

    # ---------------------------------------------------------------
    # Duplicados exactos: misma clave (Pto. Venta + N°Comprobante + CUIT)
    # Y mismo importe repetidos más de una vez de un mismo lado (la misma
    # factura cargada dos veces). La copia "sobrante" (la que no ganó el
    # match 1 a 1 de arriba) nunca va a encontrar contraparte en la otra
    # fuente -esta ya la "gastó" la primera copia-, así que terminaría
    # en falta_arca/falta_sistema como si fuera un comprobante realmente
    # faltante. Se manda a "revisar" con comentario "Duplicado" en vez de
    # a "falta".
    #
    # El caso de una Nota de Crédito cargada reutilizando el N° de la
    # factura (mismo Pto. Venta+N°Comprobante+CUIT, importe con signo
    # inverso) tampoco es un duplicado en este sentido -los importes no
    # coinciden entre sí, así que no cae acá-: al compartir clave exacta,
    # el match 1 a 1 de arriba ya la empareja directo con la factura y
    # queda reflejada en "revisar" por la diferencia de Total (no se
    # busca su respaldo real en otro lado vía cruce3: clave exacta matchea
    # sin mirar el importe).
    # ---------------------------------------------------------------
    def _detectar_duplicados(df_full, key_cols, idx_col, total_col, usados):
        totales = df_full.set_index(idx_col)[total_col]
        filas, usados_extra = [], set()
        for idxs in df_full.groupby(key_cols)[idx_col].apply(list):
            if len(idxs) < 2:
                continue
            for i in idxs:
                if i in usados or i in usados_extra:
                    continue
                otros = [j for j in idxs if j != i]
                if any(abs(totales[i] - totales[j]) <= tolerancia_total for j in otros):
                    fila = df_full.loc[[i]].copy()
                    fila["comentario"] = "Duplicado"
                    filas.append(fila)
                    usados_extra.add(i)
        if filas:
            return pd.concat(filas, ignore_index=True), usados_extra
        vacio = df_full.iloc[0:0].copy()
        vacio["comentario"] = pd.Series(dtype="object")
        return vacio, usados_extra

    dup_sis, extra_usados_sis = _detectar_duplicados(
        sis, ["Pto. Venta", "N°Comprobante", "CUIT_norm"], "_idx_sis", "_total_sis", usados_sis
    )
    usados_sis |= extra_usados_sis

    dup_arca, extra_usados_arca = _detectar_duplicados(
        arca, ["Pto. Venta", "N°Comprobante", "Nro. Doc. Emisor"], "_idx_arca", "_total_arca", usados_arca
    )
    usados_arca |= extra_usados_arca

    revisar_duplicados = pd.concat([
        dup_sis.drop(columns=temp_cols_sis, errors="ignore").rename(columns=sis_rename),
        dup_arca[[c for c in arca_cols_available if c in dup_arca.columns] + ["comentario"]]
            .rename(columns=arca_cols_rename),
    ], ignore_index=True)

    match_sis = (
        sis.loc[matched_sis_idx]
        .drop(columns=temp_cols_sis, errors="ignore")
        .rename(columns=sis_rename)
        .reset_index(drop=True)
    )
    match_arca = (
        arca.loc[matched_arca_idx, arca_cols_available]
        .drop(columns=temp_cols_arca, errors="ignore")
        .rename(columns=arca_cols_rename)
        .reset_index(drop=True)
    )
    match = pd.concat([match_sis, match_arca], axis=1)

    falta_arca = (
        sis[~sis["_idx_sis"].isin(usados_sis)]
        .drop(columns=temp_cols_sis, errors="ignore")
        .rename(columns=sis_rename)
        .reset_index(drop=True)
    )

    falta_sistema = (
        arca[~arca["_idx_arca"].isin(usados_arca)]
        .drop(columns=temp_cols_arca, errors="ignore")
        .rename(columns=arca_cols_rename)
        .reset_index(drop=True)
    )

    # "revisar" se arma acá mismo (inconsistencias del match, con los
    # casos de composición Gravado/No Gravado ya separados a
    # "no_gravado" dentro de revisar_inconsistencias_en_match, + los
    # duplicados detectados en este cruce); cruce2 y cruce3 le van
    # agregando filas a "revisar".
    revisar_base, no_gravado = revisar_inconsistencias_en_match(match, tol_pesos=tolerancia_total)
    revisar = pd.concat([revisar_base, revisar_duplicados], ignore_index=True)

    return match, falta_sistema, falta_arca, revisar, no_gravado


# ─────────────────────────────────────────────
# REVISAR INCONSISTENCIAS EN MATCH
# ─────────────────────────────────────────────

# Columnas del sistema que pueden estar "escondiendo" un importe dentro
# de No Gravado en vez de discriminarlo aparte (percepciones que ARCA
# agrupa en Otros Tributos). Puede ser más de una a la vez (ej. IIBB
# CABA + Perc. IVA juntas). No incluye IVA 10,5/21/27%: son un concepto
# distinto, no forman parte de Otros Tributos.
# Diferencias de Gravado / Total con |sistema / arca - 1| <= 0,1% no van a "revisar".
TOL_RELATIVA_IMPORTES = 0.001

PERCEP_COLS_NO_GRAVADO = [
    "Perc. IIBB CABA", "Perc. IIBB BS AS", "Perc. SUSS", "Perc. Gcias.", "Perc. IVA", "SIRCREB", "Imp. Int.",
]


def revisar_inconsistencias_en_match(
    match: pd.DataFrame,
    tol_pesos: float = 1.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Arma `revisar` desde `match` cuando NO coinciden:
      - Fecha_Sistema vs Fecha_arca                → no se marca si caen en
        el mismo mes/año, ni si el sistema tiene día 1 y ARCA una fecha
        anterior (carga del período cerrado sin abrir el candado)
      - Gravado_sistema vs Gravado_arca            → tolerancia +/- tol_pesos
      - No Gravado_sistema vs No gravado_arca      → tolerancia +/- tol_pesos.
        Si no coincide directo, "No Gravado" no se marca como
        inconsistencia si se explica por composición (no es una
        diferencia real, solo cambia cómo se reparte entre columnas):
          a) coincide con Otros Tributos_arca
          b) coincide con Exentas_arca
          c) IVA 0%: el sistema no tiene columna de IVA 0% y lo carga
             junto con No Gravado -> (Gravado_arca - Gravado_sistema)
             == (No Gravado_sistema - No gravado_arca). Esto también
             explica el Gravado (mismo movimiento).
          d) una o más columnas de PERCEP_COLS_NO_GRAVADO: el sistema
             carga esa(s) percepción(es) junto con No Gravado en vez de
             discriminarla(s) -> No Gravado_sistema + (una o la suma de
             varias de esas columnas) == Otros Tributos_arca.
        Las filas explicadas por (a)/(b)/(c)/(d) no van a "revisar":
        van a "no_gravado", con el motivo puntual en el comentario
        más cualquier otro motivo que la fila tenga sin explicar (ej.
        "Iva 0% registrado como no gravado, Fecha").
      - Imp. Total_sistema vs Imp. Total_arca      → tolerancia +/- tol_pesos
      Gravado y Total tampoco se marcan si la diferencia relativa
      |sistema / arca - 1| es <= TOL_RELATIVA_IMPORTES (0,1%).

    Regla extra:
      - Si el motivo es SOLO "No Gravado" y Tipo de Comprobante == 11, NO incluir (Factura C)
      - Si Tipo de Comprobante no está en match, la exclusión se omite sin romper.

    Agrega `comentario` con: "Fecha" / "Gravado" / "No Gravado" / "Total".
    Pueden combinarse: "Fecha, Gravado, No Gravado, Total".

    Devuelve (revisar, no_gravado).
    """

    df = match.copy()
    df.columns = df.columns.astype(str).str.strip()

    def _pick(df_: pd.DataFrame, candidates: list[str]) -> str:
        cols = set(df_.columns)
        for c in candidates:
            if c in cols:
                return c
        raise KeyError(
            f"revisar_inconsistencias_en_match: no encontré columna. "
            f"Candidatos={candidates}. Disponibles={list(df_.columns)}"
        )

    def _pick_optional(df_: pd.DataFrame, candidates: list[str]) -> str | None:
        cols = set(df_.columns)
        for c in candidates:
            if c in cols:
                return c
        return None

    def _to_dt_norm(series: pd.Series) -> pd.Series:
        return pd.to_datetime(series, errors="coerce").dt.normalize()

    def _to_num(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce")

    c_fecha_sis   = _pick(df, ["Fecha_Sistema"])
    c_fecha_arca  = _pick(df, ["Fecha_arca"])

    c_grav_sis    = _pick(df, ["Gravado_sistema"])
    c_grav_arca   = _pick(df, ["Gravado_arca"])

    c_nograv_sis    = _pick(df, ["No Gravado_sistema"])
    c_nograv_arca   = _pick(df, ["No gravado_arca"])
    c_otros_trib    = _pick_optional(df, ["Otros Tributos_arca"])
    c_exentas_arca  = _pick_optional(df, ["Exentas_arca"])

    c_total_sis   = _pick(df, ["Imp. Total_sistema"])
    c_total_arca  = _pick(df, ["Imp. Total_arca"])

    c_tipo_comp   = _pick_optional(df, ["Tipo de Comprobante"])

    tol = float(tol_pesos)

    # Diferencias de fecha que no son inconsistencia:
    #   - dentro del mismo mes (ej. 01/01/2026 en el sistema y 04/01/2026
    #     en ARCA): el comprobante cae en el mismo período.
    #   - carga el día 1 del mes con fecha ARCA anterior: los comprobantes
    #     de un período ya cerrado se cargan el primero del mes siguiente
    #     para no abrir el candado.
    fecha_sis  = _to_dt_norm(df[c_fecha_sis])
    fecha_arca = _to_dt_norm(df[c_fecha_arca])
    mismo_mes  = fecha_sis.dt.to_period("M") == fecha_arca.dt.to_period("M")
    carga_dia1 = (fecha_sis.dt.day == 1) & (fecha_arca < fecha_sis)
    mask_fecha = ~(mismo_mes | carga_dia1)

    grav_sis  = _to_num(df[c_grav_sis ]).fillna(0.0).round(2)
    grav_arca = _to_num(df[c_grav_arca]).fillna(0.0).round(2)
    def _difiere(sis: pd.Series, arca: pd.Series) -> pd.Series:
        # Diferencia mayor a tol_pesos y, además, mayor a la tolerancia relativa
        relativa = (sis / arca.where(arca != 0) - 1).abs()
        return ((sis - arca).abs() > tol) & ~(relativa <= TOL_RELATIVA_IMPORTES)

    mask_grav_raw = _difiere(grav_sis, grav_arca)

    nograv_sis  = _to_num(df[c_nograv_sis ]).fillna(0.0).round(2)
    nograv_arca = _to_num(df[c_nograv_arca]).fillna(0.0).round(2)

    total_sis  = _to_num(df[c_total_sis ]).fillna(0.0).round(2)
    total_arca = _to_num(df[c_total_arca]).fillna(0.0).round(2)
    mask_total = _difiere(total_sis, total_arca)

    if c_tipo_comp is not None:
        tipo_comp    = pd.to_numeric(df[c_tipo_comp], errors="coerce")
        mask_excluir = tipo_comp == 11
    else:
        mask_excluir = pd.Series(False, index=df.index)

    match_directo = (nograv_sis - nograv_arca).abs() <= tol

    # Motivo por el que "No Gravado" no coincide directo pero no es una
    # diferencia real (composición) -> a "no_gravado", no a "revisar".
    etiqueta_no_gravado = pd.Series(pd.NA, index=df.index, dtype="object")
    explica_gravado      = pd.Series(False, index=df.index)

    pendiente = (~match_directo) & (~mask_excluir)

    if c_otros_trib is not None:
        otros_trib = _to_num(df[c_otros_trib]).fillna(0.0).round(2)
        ok = pendiente & ((nograv_sis - otros_trib).abs() <= tol)
        etiqueta_no_gravado.loc[ok] = "No Gravado coincide con Otros Tributos"
        pendiente &= ~ok

    if c_exentas_arca is not None:
        exentas_arca = _to_num(df[c_exentas_arca]).fillna(0.0).round(2)
        ok = pendiente & ((nograv_sis - exentas_arca).abs() <= tol)
        etiqueta_no_gravado.loc[ok] = "No Gravado coincide con Exentas"
        pendiente &= ~ok

    diff_gravado   = grav_arca - grav_sis
    diff_nogravado = nograv_sis - nograv_arca
    ok = pendiente & ((diff_gravado - diff_nogravado).abs() <= tol)
    etiqueta_no_gravado.loc[ok] = "Iva 0% registrado como no gravado"
    explica_gravado.loc[ok] = True
    pendiente &= ~ok

    if c_otros_trib is not None:
        percep_disponibles = [c for c in PERCEP_COLS_NO_GRAVADO if c in df.columns]
        # Se prueban combinaciones de 1, 2, 3... columnas (puede ser más de
        # una percepción junta, ej. IIBB CABA + Perc. IVA) hasta explicar
        # la fila; se prioriza la combinación más chica que cierre.
        for r in range(1, len(percep_disponibles) + 1):
            if not pendiente.any():
                break
            for combo in combinations(percep_disponibles, r):
                if not pendiente.any():
                    break
                suma_percep = df.loc[pendiente, list(combo)].apply(_to_num).fillna(0.0).sum(axis=1).round(2)
                ok_idx = suma_percep.index[
                    (nograv_sis.loc[pendiente] + suma_percep - otros_trib.loc[pendiente]).abs() <= tol
                ]
                if len(ok_idx):
                    etiqueta_no_gravado.loc[ok_idx] = (
                        f"{' + '.join(combo)} registrada(s) junto con No Gravado (coincide con Otros Tributos)"
                    )
                    pendiente.loc[ok_idx] = False

    mask_nograv_explicado = etiqueta_no_gravado.notna()
    mask_nograv = pendiente
    mask_grav   = mask_grav_raw & ~explica_gravado

    def _comentario_row(i, incluir_no_gravado_flag: bool) -> str:
        motivos = []
        if bool(mask_fecha.loc[i]):  motivos.append("Fecha")
        if bool(mask_grav.loc[i]):   motivos.append("Gravado")
        if incluir_no_gravado_flag and bool(mask_nograv.loc[i]): motivos.append("No Gravado")
        if bool(mask_total.loc[i]):  motivos.append("Total")
        return ", ".join(motivos)

    mask_any = (mask_fecha | mask_grav | mask_nograv | mask_total) & ~mask_nograv_explicado
    revisar  = df.loc[mask_any].copy()
    revisar["comentario"] = (
        [_comentario_row(i, incluir_no_gravado_flag=True) for i in revisar.index]
        if not revisar.empty
        else pd.Series(dtype="object")
    )

    no_gravado = df.loc[mask_nograv_explicado].copy()
    no_gravado["comentario"] = (
        [
            ", ".join(
                [etiqueta_no_gravado.loc[i]]
                + [m for m in _comentario_row(i, incluir_no_gravado_flag=False).split(", ") if m]
            )
            for i in no_gravado.index
        ]
        if not no_gravado.empty
        else pd.Series(dtype="object")
    )

    return revisar.reset_index(drop=True), no_gravado.reset_index(drop=True)


# ─────────────────────────────────────────────
# CRUCE 2: faltantes por N°Comprobante + CUIT (sin Pto. Venta)
# ─────────────────────────────────────────────

def cruce2(
    revisar1: pd.DataFrame,
    falta_arca: pd.DataFrame,
    falta_sistema: pd.DataFrame,
    tol_pesos: float = 1.0,
):
    """
    Segundo cruce: cruza falta_arca y falta_sistema por N°Comprobante + CUIT (sin Pto. Venta).

    El N°Comprobante no es único por CUIT (se repite entre distintos Ptos.
    de Venta, o entre una factura y una nota de crédito), así que no alcanza
    con que coincidan N°Comprobante + CUIT: además se exige que el Imp.
    Total coincida dentro de la tolerancia (igual criterio de importe que
    usa cruce1 para el match principal). Sin ese chequeo, dos comprobantes
    de un mismo proveedor que no tienen nada que ver entre sí (fechas e
    importes totalmente distintos) podían "matchear" solo por coincidir de
    casualidad en el número, tapando un faltante real de cada lado con un
    falso "Pto. Venta".

    - Los que matchean (importe dentro de tolerancia) se sacan de los
      faltantes y se agregan a revisar con comentario "Pto. Venta".
    - Los que no matchean permanecen en sus respectivos faltantes.
    """

    fa = falta_arca.copy().reset_index(drop=True)
    fs = falta_sistema.copy().reset_index(drop=True)

    fa.columns = fa.columns.str.strip()
    fs.columns = fs.columns.str.strip()

    fa["_idx_fa"] = fa.index
    fs["_idx_fs"] = fs.index

    fa["_total_key"] = pd.to_numeric(fa["Imp. Total_sistema"], errors="coerce").round(2).fillna(0.0)
    fs["_total_key"] = pd.to_numeric(fs["Imp. Total_arca"   ], errors="coerce").round(2).fillna(0.0)

    cand = pd.merge(
        fa,
        fs,
        left_on  =["N°Comprobante_sistema", "cuit_sistema"],
        right_on =["N°Comprobante_arca",    "cuit_arca"],
        how      ="inner",
        suffixes =("_fa", "_fs"),
    )

    arca_cols_match = [
        "Fecha_arca", "Pto. Venta_arca", "N°Comprobante_arca",
        "cuit_arca", "No gravado_arca", "Gravado_arca",
        "Exentas_arca", "Otros Tributos_arca", "Imp. Total_arca",
    ]
    arca_cols_available = [c for c in arca_cols_match if c in fs.columns]

    if not cand.empty:
        cand["_diff_total"] = (cand["_total_key_fa"] - cand["_total_key_fs"]).abs()

        cand_ok = cand[cand["_diff_total"] <= tol_pesos].sort_values("_diff_total")

        usados_fa_idx, usados_fs_idx = set(), set()
        fa_idx_sel, fs_idx_sel = [], []
        for _, row in cand_ok.iterrows():
            i_fa = int(row["_idx_fa"])
            i_fs = int(row["_idx_fs"])
            if i_fa not in usados_fa_idx and i_fs not in usados_fs_idx:
                usados_fa_idx.add(i_fa)
                usados_fs_idx.add(i_fs)
                fa_idx_sel.append(i_fa)
                fs_idx_sel.append(i_fs)
    else:
        usados_fa_idx, usados_fs_idx = set(), set()
        fa_idx_sel, fs_idx_sel = [], []

    if fa_idx_sel:
        fa_match = (
            fa.loc[fa_idx_sel]
            .drop(columns=["_idx_fa", "_total_key"], errors="ignore")
            .reset_index(drop=True)
        )
        fs_match = (
            fs.loc[fs_idx_sel, arca_cols_available]
            .reset_index(drop=True)
        )

        new_match = pd.concat([fa_match, fs_match], axis=1)
        new_match["comentario"] = "Pto. Venta"

        revisar = pd.concat([revisar1, new_match], ignore_index=True)
    else:
        revisar = revisar1.copy()

    matched_fa_idx = usados_fa_idx
    matched_fs_idx = usados_fs_idx

    falta_arca_new = (
        fa.loc[~fa["_idx_fa"].isin(matched_fa_idx)]
        .drop(columns=["_idx_fa", "_total_key"], errors="ignore")
        .reset_index(drop=True)
    )

    falta_sistema_new = (
        fs.loc[~fs["_idx_fs"].isin(matched_fs_idx)]
        .drop(columns=["_idx_fs", "_total_key"], errors="ignore")
        .reset_index(drop=True)
    )

    return revisar, falta_arca_new, falta_sistema_new


# ─────────────────────────────────────────────
# CRUCE 3: tolerante a errores en N°Comprobante o CUIT
# ─────────────────────────────────────────────

def cruce3(
    revisar: pd.DataFrame,
    falta_arca: pd.DataFrame,
    falta_sistema: pd.DataFrame,
    tol_pesos: float = 1.0,
    preferir_match_minimo: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Tercer cruce (tolerante a errores en N°Comprobante o CUIT) entre faltantes.

    A) Primero busca por N°Comprobante + Fecha + importes (tol)
       => falló el CUIT en cruces anteriores → comentario "CUIT"
    B) Luego, sobre el remanente, busca por CUIT + Fecha + importes (tol)
       => falló el N°Comprobante → comentario "N°Comprobante"
    """

    def _to_date(series: pd.Series) -> pd.Series:
        return pd.to_datetime(series, errors="coerce").dt.date

    def _to_amount(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce").round(2)

    def _align_columns(base: pd.DataFrame, add: pd.DataFrame) -> pd.DataFrame:
        add2 = add.copy()
        for c in base.columns:
            if c not in add2.columns:
                add2[c] = np.nan
        extras = [c for c in add2.columns if c not in base.columns]
        return add2[base.columns.tolist() + extras]

    def _resolver_1a1(candidatos_ok: pd.DataFrame) -> tuple[pd.DataFrame, set[int], set[int]]:
        usados_fa: set[int] = set()
        usados_fs: set[int] = set()
        seleccionados = []
        for _, row in candidatos_ok.iterrows():
            id_fa = int(row["_id_fa"])
            id_fs = int(row["_id_fs"])
            if (id_fa not in usados_fa) and (id_fs not in usados_fs):
                usados_fa.add(id_fa)
                usados_fs.add(id_fs)
                seleccionados.append(row)
        return pd.DataFrame(seleccionados).copy(), usados_fa, usados_fs

    fa = falta_arca.copy().reset_index(drop=True)
    fs = falta_sistema.copy().reset_index(drop=True)
    fa.columns = fa.columns.str.strip()
    fs.columns = fs.columns.str.strip()

    fa["_id_fa"] = np.arange(len(fa), dtype=int)
    fs["_id_fs"] = np.arange(len(fs), dtype=int)

    fa["_nro_key"]  = fa["N°Comprobante_sistema"].astype(str).str.strip()
    fs["_nro_key"]  = fs["N°Comprobante_arca"].astype(str).str.strip()

    fa["_cuit_key"] = (
        fa["cuit_sistema"].astype(str).str.strip()
        .str.replace("-", "", regex=False).str.replace(" ", "", regex=False)
    )
    fs["_cuit_key"] = (
        fs["cuit_arca"].astype(str).str.strip()
        .str.replace("-", "", regex=False).str.replace(" ", "", regex=False)
    )

    fa["_fecha_key"] = _to_date(fa["Fecha_Sistema"])
    fs["_fecha_key"] = _to_date(fs["Fecha_arca"])

    fa["_ng_key"]  = _to_amount(fa["Gravado_sistema"  ]).fillna(0.0)
    fa["_nng_key"] = _to_amount(fa["No Gravado_sistema"]).fillna(0.0)
    fs["_ng_key"]  = _to_amount(fs["Gravado_arca"     ]).fillna(0.0)
    fs["_nng_key"] = _to_amount(fs["No gravado_arca"  ]).fillna(0.0)

    arca_cols_match     = ["Fecha_arca", "Pto. Venta_arca", "N°Comprobante_arca",
                           "cuit_arca", "No gravado_arca", "Gravado_arca",
                           "Exentas_arca", "Otros Tributos_arca", "Imp. Total_arca"]
    arca_cols_available = [c for c in arca_cols_match if c in fs.columns]

    def _build_match_rows(resolved: pd.DataFrame, comentario_val: str) -> pd.DataFrame:
        if resolved.empty:
            return pd.DataFrame()
        fa_idx = resolved["_id_fa"].astype(int).values
        fs_idx = resolved["_id_fs"].astype(int).values
        temp_cols = [c for c in fa.columns if c.startswith("_")]
        fa_match = (
            fa.loc[fa_idx]
            .drop(columns=temp_cols, errors="ignore")
            .reset_index(drop=True)
        )
        fs_match = (
            fs.loc[fs_idx, arca_cols_available]
            .reset_index(drop=True)
        )
        result = pd.concat([fa_match, fs_match], axis=1)
        result["comentario"] = comentario_val
        return result

    # A) N°Comprobante + Fecha + importes → "CUIT"
    cand_nro = pd.merge(
        fa, fs,
        on=["_nro_key", "_fecha_key"],
        how="inner",
        suffixes=("_fa", "_fs"),
        validate="many_to_many",
    )

    match_nro_rows = pd.DataFrame()
    usados_fa_nro: set[int] = set()
    usados_fs_nro: set[int] = set()

    if not cand_nro.empty:
        cand_nro["_diff_ng"]    = (cand_nro["_ng_key_fa"]  - cand_nro["_ng_key_fs"]).abs()
        cand_nro["_diff_nng"]   = (cand_nro["_nng_key_fa"] - cand_nro["_nng_key_fs"]).abs()
        cand_nro["_diff_total"] = cand_nro["_diff_ng"] + cand_nro["_diff_nng"]

        cand_nro_ok = cand_nro[
            (cand_nro["_diff_ng"]  <= tol_pesos) &
            (cand_nro["_diff_nng"] <= tol_pesos)
        ].copy()

        if not cand_nro_ok.empty and preferir_match_minimo:
            cand_nro_ok = cand_nro_ok.sort_values(
                by=["_nro_key", "_fecha_key", "_diff_total", "_diff_ng", "_diff_nng", "_id_fa", "_id_fs"],
                ascending=True,
            )

        if not cand_nro_ok.empty:
            resolved, usados_fa_nro, usados_fs_nro = _resolver_1a1(cand_nro_ok)
            match_nro_rows = _build_match_rows(resolved, "CUIT")

    fa_rem = fa[~fa["_id_fa"].isin(usados_fa_nro)].copy()
    fs_rem = fs[~fs["_id_fs"].isin(usados_fs_nro)].copy()

    # B) CUIT + Fecha + importes → "N°Comprobante"
    cand_cuit = pd.merge(
        fa_rem, fs_rem,
        on=["_cuit_key", "_fecha_key"],
        how="inner",
        suffixes=("_fa", "_fs"),
        validate="many_to_many",
    )

    match_cuit_rows = pd.DataFrame()
    usados_fa_cuit: set[int] = set()
    usados_fs_cuit: set[int] = set()

    if not cand_cuit.empty:
        cand_cuit["_diff_ng"]    = (cand_cuit["_ng_key_fa"]  - cand_cuit["_ng_key_fs"]).abs()
        cand_cuit["_diff_nng"]   = (cand_cuit["_nng_key_fa"] - cand_cuit["_nng_key_fs"]).abs()
        cand_cuit["_diff_total"] = cand_cuit["_diff_ng"] + cand_cuit["_diff_nng"]

        cand_cuit_ok = cand_cuit[
            (cand_cuit["_diff_ng"]  <= tol_pesos) &
            (cand_cuit["_diff_nng"] <= tol_pesos)
        ].copy()

        if not cand_cuit_ok.empty and preferir_match_minimo:
            cand_cuit_ok = cand_cuit_ok.sort_values(
                by=["_cuit_key", "_fecha_key", "_diff_total", "_diff_ng", "_diff_nng", "_id_fa", "_id_fs"],
                ascending=True,
            )

        if not cand_cuit_ok.empty:
            resolved, usados_fa_cuit, usados_fs_cuit = _resolver_1a1(cand_cuit_ok)
            match_cuit_rows = _build_match_rows(resolved, "N°Comprobante")

    falta_arca_new = (
        fa_rem[~fa_rem["_id_fa"].isin(usados_fa_cuit)]
        .drop(columns=[c for c in fa_rem.columns if c.startswith("_")], errors="ignore")
        .reset_index(drop=True)
    )
    falta_sistema_new = (
        fs_rem[~fs_rem["_id_fs"].isin(usados_fs_cuit)]
        .drop(columns=[c for c in fs_rem.columns if c.startswith("_")], errors="ignore")
        .reset_index(drop=True)
    )

    nuevos = [df for df in [match_nro_rows, match_cuit_rows] if not df.empty]

    if not nuevos:
        revisar_new = revisar.copy()
    else:
        revisar_2         = pd.concat(nuevos, ignore_index=True)
        revisar_2_aligned = _align_columns(revisar, revisar_2)
        revisar_new       = pd.concat([revisar, revisar_2_aligned], ignore_index=True)

    return revisar_new, falta_arca_new, falta_sistema_new


# ─────────────────────────────────────────────
# NETEAR FALTA_SISTEMA: comprobantes del mismo proveedor
# que se cancelan entre sí (mismo criterio que la fórmula de Excel)
# ─────────────────────────────────────────────

def netear_falta_sistema(
    falta_sistema: pd.DataFrame,
    tol_pesos: float = 1.0,
) -> pd.DataFrame:
    """
    Dentro de falta_sistema, agrupa por proveedor (cuit_arca) y busca pares
    de comprobantes cuyos Imp. Total_arca se cancelan entre sí (suma ~0,
    tolerancia +/- tol_pesos). Esos pares se sacan del faltante: si se
    netean, no representan una diferencia real y no hace falta buscarlos.

    Resolución 1 a 1 por proveedor: cada fila se usa como máximo en un par.
    Si una fila tiene más de un candidato, se desempata por la fecha más
    próxima (Fecha_arca); a igual fecha, por menor diferencia de importe y
    luego por orden de aparición.
    """

    df = falta_sistema.copy().reset_index(drop=True)
    df.columns = df.columns.astype(str).str.strip()

    c_cuit  = "cuit_arca"
    c_total = "Imp. Total_arca"

    c_fecha = "Fecha_arca"

    df["_id"]    = df.index
    df["_total"] = pd.to_numeric(df[c_total], errors="coerce").fillna(0.0).round(2)
    if c_fecha in df.columns:
        df["_fecha"] = pd.to_datetime(df[c_fecha], errors="coerce", dayfirst=True)
    else:
        df["_fecha"] = pd.NaT

    usados: set[int] = set()
    SIN_FECHA = 10**6  # días: los pares sin fecha quedan al final

    for _, grupo in df.groupby(c_cuit):
        idxs    = grupo["_id"].tolist()
        totales = grupo.set_index("_id")["_total"]
        fechas  = grupo.set_index("_id")["_fecha"]

        candidatos = []
        for a in range(len(idxs)):
            id_a = idxs[a]
            for b in range(a + 1, len(idxs)):
                id_b = idxs[b]
                dif_importe = abs(totales[id_a] + totales[id_b])
                if dif_importe > tol_pesos:
                    continue
                if pd.notna(fechas[id_a]) and pd.notna(fechas[id_b]):
                    dif_dias = abs((fechas[id_a] - fechas[id_b]).days)
                else:
                    dif_dias = SIN_FECHA
                candidatos.append((dif_dias, dif_importe, id_a, id_b))

        candidatos.sort()
        for _, _, id_a, id_b in candidatos:
            if id_a in usados or id_b in usados:
                continue
            usados.add(id_a)
            usados.add(id_b)

    falta_sistema_new = (
        df[~df["_id"].isin(usados)]
        .drop(columns=["_id", "_total", "_fecha"], errors="ignore")
        .reset_index(drop=True)
    )

    return falta_sistema_new


# ─────────────────────────────────────────────
# EXPORTAR A BUFFER EN MEMORIA (descargable único)
# ─────────────────────────────────────────────

COLUMNAS_IMPORTE = [
    "Imp. Neto Gravado IVA 0%", "IVA 2,5%", "Imp. Neto Gravado IVA 2,5%",
    "IVA 5%", "Imp. Neto Gravado IVA 5%", "IVA 10,5%", "Imp. Neto Gravado IVA 10,5%",
    "IVA 21%", "Imp. Neto Gravado IVA 21%", "IVA 27%", "Imp. Neto Gravado IVA 27%",
    "Imp. Neto Gravado Total", "Imp. Neto No Gravado", "Imp. Op. Exentas",
    "Otros Tributos", "Total IVA", "Imp. Total",
    "Gravado_sistema", "Gravado_arca", "No Gravado_sistema", "No gravado_arca",
    "Otros Tributos_arca", "Exentas_arca", "Imp. Total_sistema", "Imp. Total_arca",
]


def _forzar_importes_float(df: pd.DataFrame, columnas: list[str] = COLUMNAS_IMPORTE) -> pd.DataFrame:
    df = df.copy()
    for col in columnas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    return df


# ─────────────────────────────────────────────
# REVISAR CON TODAS LAS COLUMNAS DE SISTEMA Y ARCA
# ─────────────────────────────────────────────

# Orden de salida de "revisar": cada par (sistema, arca) queda lado a lado;
# None indica que el concepto existe en una sola de las dos fuentes.
ORDEN_REVISAR_ANCHO: list[tuple[str | None, str | None]] = [
    ("Fecha",                "Fecha de EmisiÃ³n"),
    ("Fecha Contable",       None),
    ("Razón Social",         "DenominaciÃ³n Emisor"),
    ("CUIT_norm",            "Nro. Doc. Emisor"),
    (None,                   "Tipo Doc. Emisor"),
    ("Condición",            None),
    ("Tipo Doc.",            "Tipo de Comprobante"),
    ("Pto. Venta",           "Pto. Venta"),
    ("N°Comprobante",        "N°Comprobante"),
    (None,                   "CÃ³d. AutorizaciÃ³n"),
    (None,                   "Moneda"),
    (None,                   "Tipo Cambio"),
    ("Imp. Neto Gravado",    "Imp. Neto Gravado Total"),
    (None,                   "Imp. Neto Gravado IVA 0%"),
    (None,                   "Imp. Neto Gravado IVA 2,5%"),
    (None,                   "Imp. Neto Gravado IVA 5%"),
    (None,                   "Imp. Neto Gravado IVA 10,5%"),
    (None,                   "Imp. Neto Gravado IVA 21%"),
    (None,                   "Imp. Neto Gravado IVA 27%"),
    ("Imp. Neto No Gravado", "Imp. Neto No Gravado"),
    (None,                   "Imp. Op. Exentas"),
    (None,                   "IVA 2,5%"),
    (None,                   "IVA 5%"),
    ("IVA 10,5%",            "IVA 10,5%"),
    ("IVA 21%",              "IVA 21%"),
    ("IVA 27%",              "IVA 27%"),
    (None,                   "Total IVA"),
    ("Imp. Int.",            None),
    ("Perc. Gcias.",         None),
    ("Perc. IVA",            None),
    ("Perc. IIBB CABA",      None),
    ("Perc. IIBB BS AS",     None),
    ("Perc. SUSS",           None),
    ("SIRCREB",              None),
    (None,                   "Otros Tributos"),
    ("Total",                "Imp. Total"),
    (None,                   "Tipo Doc. Receptor"),
    (None,                   "Nro. Doc. Receptor"),
]

COLUMNAS_IMPORTE_SISTEMA = [
    "Imp. Neto Gravado", "Imp. Neto No Gravado", "IVA 10,5%", "IVA 21%", "IVA 27%",
    "Imp. Int.", "Perc. Gcias.", "Perc. IVA", "Perc. IIBB CABA", "Perc. IIBB BS AS",
    "Perc. SUSS", "SIRCREB", "Total",
]


def _arreglar_encoding(nombre: str) -> str:
    try:
        return nombre.encode("latin1").decode("utf8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return nombre


def _clave_valor(v) -> str:
    if pd.isna(v):
        return ""
    try:
        return f"{float(v):.2f}"
    except (TypeError, ValueError):
        return str(v).strip()


def _clave_fecha(v) -> str:
    f = pd.to_datetime(v, errors="coerce")
    return "" if pd.isna(f) else f.strftime("%Y-%m-%d")


def expandir_revisar(
    revisar: pd.DataFrame,
    df_arca_dep: pd.DataFrame,
    df_sistema_dep: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Reemplaza las columnas resumidas de `revisar` por todas las columnas
    de sistema (sufijo _sis) y de ARCA (sufijo _arca), tomadas de los
    DataFrames depurados. Cada fila se ubica por Pto. Venta + N°Comprobante
    + CUIT + Total (+ Fecha en ARCA). Devuelve el DataFrame y las columnas
    de fecha e importe para el formato.
    """
    sis  = df_sistema_dep.reset_index(drop=True)
    arca = df_arca_dep.reset_index(drop=True)
    c_fecha_arca = "Fecha de EmisiÃ³n"

    idx_sis: dict[tuple, list[int]] = {}
    for i, r in sis.iterrows():
        k = (_clave_valor(r["Pto. Venta"]), _clave_valor(r["N°Comprobante"]),
             _clave_valor(r["CUIT_norm"]), _clave_valor(r.get("Total")))
        idx_sis.setdefault(k, []).append(i)

    idx_arca: dict[tuple, list[int]] = {}
    for i, r in arca.iterrows():
        k = (_clave_valor(r["Pto. Venta"]), _clave_valor(r["N°Comprobante"]),
             _clave_valor(r["Nro. Doc. Emisor"]), _clave_valor(r.get("Imp. Total")),
             _clave_fecha(r.get(c_fecha_arca)))
        idx_arca.setdefault(k, []).append(i)

    def _tomar(indice: dict, k: tuple, usados: dict) -> int | None:
        cands = indice.get(k, [])
        if not cands:
            return None
        n = usados.get(k, 0)
        usados[k] = n + 1
        return cands[min(n, len(cands) - 1)]

    usados_sis: dict = {}
    usados_arca: dict = {}
    filas_sis, filas_arca = [], []
    for _, r in revisar.iterrows():
        i_sis = None
        if pd.notna(r.get("cuit_sistema")):
            i_sis = _tomar(idx_sis, (
                _clave_valor(r.get("Pto. Venta_sistema")), _clave_valor(r.get("N°Comprobante_sistema")),
                _clave_valor(r.get("cuit_sistema")), _clave_valor(r.get("Imp. Total_sistema")),
            ), usados_sis)
        i_arca = None
        if pd.notna(r.get("cuit_arca")):
            i_arca = _tomar(idx_arca, (
                _clave_valor(r.get("Pto. Venta_arca")), _clave_valor(r.get("N°Comprobante_arca")),
                _clave_valor(r.get("cuit_arca")), _clave_valor(r.get("Imp. Total_arca")),
                _clave_fecha(r.get("Fecha_arca")),
            ), usados_arca)
        filas_sis.append(i_sis)
        filas_arca.append(i_arca)

    def _lado(df: pd.DataFrame, filas: list) -> pd.DataFrame:
        vacio = pd.Series(index=df.columns, dtype=object)
        return pd.DataFrame(
            [df.loc[i] if i is not None else vacio for i in filas], columns=df.columns
        ).reset_index(drop=True)

    lado_sis  = _lado(sis, filas_sis)
    lado_arca = _lado(arca, filas_arca)

    # Si la fila no se ubicó en la fuente depurada, se conservan los datos
    # resumidos que ya traía revisar.
    respaldo_sis = {
        "Fecha": "Fecha_Sistema", "Pto. Venta": "Pto. Venta_sistema",
        "N°Comprobante": "N°Comprobante_sistema", "CUIT_norm": "cuit_sistema",
        "Imp. Neto Gravado": "Gravado_sistema", "Imp. Neto No Gravado": "No Gravado_sistema",
        "Total": "Imp. Total_sistema",
    }
    respaldo_arca = {
        c_fecha_arca: "Fecha_arca", "Pto. Venta": "Pto. Venta_arca",
        "N°Comprobante": "N°Comprobante_arca", "Nro. Doc. Emisor": "cuit_arca",
        "Imp. Neto Gravado Total": "Gravado_arca", "Imp. Neto No Gravado": "No gravado_arca",
        "Imp. Op. Exentas": "Exentas_arca", "Otros Tributos": "Otros Tributos_arca",
        "Imp. Total": "Imp. Total_arca",
    }
    rev = revisar.reset_index(drop=True)
    for lado, filas, respaldo in ((lado_sis, filas_sis, respaldo_sis), (lado_arca, filas_arca, respaldo_arca)):
        faltan = [n for n, i in enumerate(filas) if i is None]
        for col, col_rev in respaldo.items():
            if faltan and col in lado.columns and col_rev in rev.columns:
                lado.loc[faltan, col] = rev.loc[faltan, col_rev].values

    columnas_sis  = [c for c in lado_sis.columns  if not str(c).startswith("_")]
    columnas_arca = [c for c in lado_arca.columns if not str(c).startswith("_")]

    orden: list[tuple[str, str]] = []
    for c_sis, c_arca in ORDEN_REVISAR_ANCHO:
        if c_sis in columnas_sis:
            orden.append(("sis", c_sis))
        if c_arca in columnas_arca:
            orden.append(("arca", c_arca))
    ya = set(orden)
    orden += [("sis", c) for c in columnas_sis if ("sis", c) not in ya]
    orden += [("arca", c) for c in columnas_arca if ("arca", c) not in ya]

    salida = pd.DataFrame({"comentario": rev["comentario"].values})
    cols_fecha, cols_importe = [], []
    for lado_nombre, c in orden:
        df_lado = lado_sis if lado_nombre == "sis" else lado_arca
        base    = "CUIT" if c == "CUIT_norm" else _arreglar_encoding(str(c))
        nombre  = f"{base}_{lado_nombre}"
        salida[nombre] = df_lado[c].values
        if "fecha" in str(c).lower():
            cols_fecha.append(nombre)
        elif (lado_nombre == "sis" and c in COLUMNAS_IMPORTE_SISTEMA) or (lado_nombre == "arca" and c in COLUMNAS_IMPORTE):
            cols_importe.append(nombre)

    for c in cols_importe:
        salida[c] = pd.to_numeric(salida[c], errors="coerce").astype(float)

    return salida, cols_fecha, cols_importe



# ─────────────────────────────────────────────
# DIAGNÓSTICOS (no gravado / gravado y total)
# ─────────────────────────────────────────────

ALICUOTAS_IVA = {"10,5%": 0.105, "21%": 0.21, "27%": 0.27}
ALICUOTAS_IVA_SOLO_ARCA = {"2,5%": 0.025, "5%": 0.05}
PERCEP_COLS_SIS_ANCHO = [f"{c}_sis" for c in PERCEP_COLS_NO_GRAVADO]

COLUMNAS_DIFERENCIA = [
    "Dif Gravado",
    "Dif No Gravado + Exentas",
    "Dif IVA",
    "Dif Percepciones vs Otros Tributos",
    "Dif Total",
    "Sistema: componentes - Total",
]


def _num(fila: pd.Series, col: str) -> float:
    v = pd.to_numeric(fila.get(col), errors="coerce")
    return 0.0 if pd.isna(v) else float(v)


def _fmt_importe(x: float) -> str:
    return f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _diferencias(fila: pd.Series) -> dict[str, float]:
    g_s, g_a = _num(fila, "Imp. Neto Gravado_sis"), _num(fila, "Imp. Neto Gravado Total_arca")
    ng_s     = _num(fila, "Imp. Neto No Gravado_sis")
    ng_a     = _num(fila, "Imp. Neto No Gravado_arca") + _num(fila, "Imp. Op. Exentas_arca")
    iva_s    = sum(_num(fila, f"IVA {k}_sis") for k in ALICUOTAS_IVA)
    iva_a    = sum(_num(fila, f"IVA {k}_arca") for k in list(ALICUOTAS_IVA) + list(ALICUOTAS_IVA_SOLO_ARCA))
    perc_s   = sum(_num(fila, c) for c in PERCEP_COLS_SIS_ANCHO)
    ot_a     = _num(fila, "Otros Tributos_arca")
    t_s, t_a = _num(fila, "Total_sis"), _num(fila, "Imp. Total_arca")
    return {
        "Dif Gravado":                        round(g_s - g_a, 2),
        "Dif No Gravado + Exentas":           round(ng_s - ng_a, 2),
        "Dif IVA":                            round(iva_s - iva_a, 2),
        "Dif Percepciones vs Otros Tributos": round(perc_s - ot_a, 2),
        "Dif Total":                          round(t_s - t_a, 2),
        "Sistema: componentes - Total":       round(g_s + ng_s + iva_s + perc_s - t_s, 2),
    }


def _explicar_gravado_total(fila: pd.Series, tol: float) -> str:
    """Busca qué columna explica la diferencia de Gravado / Total."""
    d = _diferencias(fila)
    ok = lambda x, t=tol: abs(x) <= t

    g_s, g_a = _num(fila, "Imp. Neto Gravado_sis"), _num(fila, "Imp. Neto Gravado Total_arca")
    ng_s     = _num(fila, "Imp. Neto No Gravado_sis")
    iva_s    = {k: _num(fila, f"IVA {k}_sis") for k in ALICUOTAS_IVA}
    iva_a    = {k: _num(fila, f"IVA {k}_arca") for k in ALICUOTAS_IVA}
    bases_a  = {k: _num(fila, f"Imp. Neto Gravado IVA {k}_arca") for k in ALICUOTAS_IVA}
    perc_s   = sum(_num(fila, c) for c in PERCEP_COLS_SIS_ANCHO)
    ot_a     = _num(fila, "Otros Tributos_arca")
    t_s, t_a = _num(fila, "Total_sis"), _num(fila, "Imp. Total_arca")

    # 1) Comprobante registrado en el sistema sin importes
    if ok(t_s, 0.005) and ok(g_s, 0.005) and ok(ng_s, 0.005) and not ok(t_a):
        txt = "Comprobante registrado en sistema sin importes"
        tc = _num(fila, "Tipo Cambio_arca")
        if tc and abs(tc - 1) > 1e-9:
            txt += f" (comprobante en {fila.get('Moneda_arca')}, tipo de cambio {_fmt_importe(tc)})"
        return txt

    expl = []

    # 2) Los componentes del sistema no suman su propio Total
    if not ok(d["Sistema: componentes - Total"]):
        g_implicito = t_s - (ng_s + sum(iva_s.values()) + perc_s)
        if ok(g_implicito - g_a) and not ok(g_s - g_a):
            ratio = g_s / g_a if g_a else np.nan
            veces = (
                f" (≈ {ratio:.2f} veces el de ARCA)"
                if np.isfinite(ratio) and round(ratio) > 1 and abs(ratio - round(ratio)) < 0.01 else ""
            )
            expl.append(
                f"Gravado mal cargado en sistema: {_fmt_importe(g_s)} en lugar de {_fmt_importe(g_a)}{veces}; "
                f"el Total del sistema sí coincide con ARCA"
            )
        else:
            expl.append(
                f"Los componentes del sistema suman {_fmt_importe(t_s + d['Sistema: componentes - Total'])} "
                f"y su Total es {_fmt_importe(t_s)}"
            )

    # 3) Otros Tributos de ARCA cargados como Gravado (+ IVA) en el sistema
    otros_como_gravado = False
    if not ok(d["Dif Percepciones vs Otros Tributos"]) and not ok(d["Dif Gravado"]):
        falta_ot = ot_a - perc_s
        t_rel = max(tol, 0.001 * abs(falta_ot))
        if ok(d["Dif Gravado"] - falta_ot, t_rel):
            expl.append(f"Otros Tributos de ARCA ({_fmt_importe(ot_a)}) cargados como Gravado en sistema")
            otros_como_gravado = True
        elif ok(d["Dif Gravado"] + d["Dif IVA"] - falta_ot, t_rel):
            expl.append(
                f"Otros Tributos de ARCA ({_fmt_importe(ot_a)}) cargados como Gravado + IVA en sistema "
                f"(Gravado +{_fmt_importe(d['Dif Gravado'])}, IVA +{_fmt_importe(d['Dif IVA'])})"
            )
            otros_como_gravado = True

    # 4) Otros Tributos no registrados (o por otro importe) en el sistema
    if (
        not ok(d["Dif Percepciones vs Otros Tributos"]) and ok(d["Dif Gravado"])
        and ok(d["Dif Total"] - d["Dif Percepciones vs Otros Tributos"])
    ):
        if ok(perc_s, 0.005):
            expl.append(f"Otros Tributos de ARCA ({_fmt_importe(ot_a)}) no registrados en sistema (sin percepciones cargadas)")
        else:
            expl.append(
                f"Percepciones del sistema ({_fmt_importe(perc_s)}) distintas de Otros Tributos de ARCA ({_fmt_importe(ot_a)})"
            )

    # 5) IVA mal cargado
    difs_iva = {k: iva_s[k] - iva_a[k] for k in ALICUOTAS_IVA if not ok(iva_s[k] - iva_a[k])}
    if difs_iva and not otros_como_gravado:
        alicuota_errada = False
        for k_s, tasa_s in ALICUOTAS_IVA.items():
            for k_a in ALICUOTAS_IVA:
                if (
                    k_s != k_a and not ok(bases_a[k_a]) and ok(iva_a[k_s]) and ok(iva_s[k_a])
                    and ok(iva_s[k_s] - bases_a[k_a] * tasa_s)
                ):
                    expl.append(
                        f"IVA cargado al {k_s} en lugar de {k_a} "
                        f"(sistema IVA {k_s} {_fmt_importe(iva_s[k_s])}; ARCA IVA {k_a} {_fmt_importe(iva_a[k_a])})"
                    )
                    alicuota_errada = True
        if not alicuota_errada:
            det = "; ".join(
                f"IVA {k}: sistema {_fmt_importe(iva_s[k])} vs ARCA {_fmt_importe(iva_a[k])}" for k in difs_iva
            )
            txt = f"IVA mal cargado en sistema ({det})"
            bases_s = {k: iva_s[k] / ALICUOTAS_IVA[k] for k in difs_iva if iva_s[k]}
            if bases_s and ok(d["Dif Gravado"], 5):
                txt += ". Bases implícitas del sistema: " + ", ".join(
                    f"{k} {_fmt_importe(b)} (ARCA {_fmt_importe(bases_a[k])})" for k, b in bases_s.items()
                )
            expl.append(txt)

    return " | ".join(expl) if expl else "Sin explicación automática: ver apertura de diferencias"


def _explicar_no_gravado(fila: pd.Series) -> str:
    """Detalla con importes el motivo de composición que ya trae el comentario."""
    motivos = [m.strip() for m in str(fila.get("comentario", "")).split(", ") if m.strip()]
    if not motivos:
        return ""
    motivo, otros = motivos[0], motivos[1:]

    ng_s = _num(fila, "Imp. Neto No Gravado_sis")
    if motivo == "No Gravado coincide con Otros Tributos":
        txt = (f"No Gravado del sistema ({_fmt_importe(ng_s)}) corresponde a Otros Tributos de ARCA "
               f"({_fmt_importe(_num(fila, 'Otros Tributos_arca'))})")
    elif motivo == "No Gravado coincide con Exentas":
        txt = (f"No Gravado del sistema ({_fmt_importe(ng_s)}) informado en ARCA como Exentas "
               f"({_fmt_importe(_num(fila, 'Imp. Op. Exentas_arca'))})")
    elif motivo == "Iva 0% registrado como no gravado":
        g0_a = _num(fila, "Imp. Neto Gravado IVA 0%_arca")
        if abs(g0_a) > 0.005:
            txt = (f"Neto gravado IVA 0% de ARCA ({_fmt_importe(g0_a)}) "
                   f"registrado como No Gravado en sistema (No Gravado sistema {_fmt_importe(ng_s)}, "
                   f"ARCA {_fmt_importe(_num(fila, 'Imp. Neto No Gravado_arca'))})")
        else:
            # ARCA no informa neto al 0%: el sistema movió importe entre Gravado y No Gravado
            d = _diferencias(fila)
            txt = (f"ARCA no informa neto IVA 0%: en sistema el Gravado difiere en {_fmt_importe(d['Dif Gravado'])} "
                   f"y se compensa con No Gravado ({_fmt_importe(d['Dif No Gravado + Exentas'])})")
            if abs(d["Dif IVA"]) > 0.005 and abs(d["Dif IVA"] + d["Dif Percepciones vs Otros Tributos"]) <= 1:
                txt += (f"; el IVA del sistema difiere en {_fmt_importe(d['Dif IVA'])} y se compensa con "
                        f"Otros Tributos de ARCA ({_fmt_importe(_num(fila, 'Otros Tributos_arca'))})")
    elif "registrada(s) junto con No Gravado" in motivo:
        cols = motivo.split(" registrada(s)")[0].split(" + ")
        perc = sum(_num(fila, f"{c}_sis") for c in cols)
        txt = (f"{' + '.join(cols)} ({_fmt_importe(perc)}) registrada(s) dentro de No Gravado: "
               f"No Gravado sistema {_fmt_importe(ng_s)} + percepciones = Otros Tributos ARCA "
               f"({_fmt_importe(_num(fila, 'Otros Tributos_arca'))})")
    else:
        txt = motivo
    if otros:
        txt += f" | Además: {', '.join(otros)}"
    return txt


def _armar_diagnostico(ancho: pd.DataFrame, explicaciones: list[str]) -> pd.DataFrame:
    """
    Columnas: comentario, explicacion, Razón Social, CUIT, Fecha, Comprobante,
    apertura de diferencias y, después, todas las columnas de sistema y ARCA
    en el mismo orden que "revisar". Los datos de identificación se toman
    del sistema y, si la fila no lo tiene, de ARCA.
    """
    ancho = ancho.reset_index(drop=True)

    def _primero(col_sis: str, col_arca: str) -> pd.Series:
        a = ancho[col_sis] if col_sis in ancho.columns else pd.Series(pd.NA, index=ancho.index)
        b = ancho[col_arca] if col_arca in ancho.columns else pd.Series(pd.NA, index=ancho.index)
        return a.where(a.notna() & (a.astype(str).str.strip() != ""), b)

    def _comprobante(fila: pd.Series) -> str:
        for lado in ("sis", "arca"):
            pto, nro = fila.get(f"Pto. Venta_{lado}"), fila.get(f"N°Comprobante_{lado}")
            if pd.notna(pto) and pd.notna(nro) and str(nro).strip():
                pto = str(pto).split(".")[0].strip()
                nro = str(nro).split(".")[0].strip()
                return f"{pto.zfill(4) if pto.isdigit() else pto}-{nro.zfill(8) if nro.isdigit() else nro}"
        return ""

    cuit = _primero("CUIT_sis", "Nro. Doc. Emisor_arca").astype(str).str.split(".").str[0]
    salida = pd.DataFrame({
        "comentario":   ancho["comentario"],
        "explicacion":  explicaciones,
        "Razón Social": _primero("Razón Social_sis", "Denominación Emisor_arca").astype(str).str.strip(),
        "CUIT":         cuit.where(cuit != "nan", ""),
        "Fecha":        pd.to_datetime(_primero("Fecha_sis", "Fecha de Emisión_arca"), errors="coerce"),
        "Comprobante":  [_comprobante(f) for _, f in ancho.iterrows()],
    })
    difs = pd.DataFrame([_diferencias(f) for _, f in ancho.iterrows()], columns=COLUMNAS_DIFERENCIA)
    return pd.concat([salida, difs, ancho.drop(columns=["comentario"])], axis=1)


def generar_diagnosticos(
    revisar: pd.DataFrame,
    no_gravado: pd.DataFrame,
    df_arca_dep: pd.DataFrame,
    df_sistema_dep: pd.DataFrame,
    tol_pesos: float = 1.0,
) -> dict[str, tuple[pd.DataFrame, list[str], list[str]]]:
    """
    Arma las solapas diagnostico_no_gravado y diagnostico_gravado_total
    (todavía no se exportan en el reporte de la app). Devuelve, por solapa,
    el DataFrame y sus columnas de fecha e importe para el formato.
    """
    ng_ancho, fechas, importes = expandir_revisar(no_gravado, df_arca_dep, df_sistema_dep)
    rev_ancho, _, _ = expandir_revisar(revisar, df_arca_dep, df_sistema_dep)

    motivos = rev_ancho["comentario"].astype(str).str.split(", ")
    gt_ancho = rev_ancho[motivos.apply(lambda m: "Gravado" in m or "Total" in m)].reset_index(drop=True)

    diag_ng = _armar_diagnostico(ng_ancho, [_explicar_no_gravado(f) for _, f in ng_ancho.iterrows()])
    diag_gt = _armar_diagnostico(gt_ancho, [_explicar_gravado_total(f, tol_pesos) for _, f in gt_ancho.iterrows()])

    fechas_diag   = ["Fecha"] + fechas
    importes_diag = COLUMNAS_DIFERENCIA + importes
    return {
        "diagnostico_no_gravado":    (diag_ng, fechas_diag, importes_diag),
        "diagnostico_gravado_total": (diag_gt, fechas_diag, importes_diag),
    }


def generar_excel_en_memoria(
    revisar: pd.DataFrame,
    falta_sistema: pd.DataFrame,
    df_arca_dep: pd.DataFrame | None = None,
    df_sistema_dep: pd.DataFrame | None = None,
) -> bytes:
    DATE_FORMAT   = "DD/MM/YYYY"
    AMOUNT_FORMAT = "#,##0.00"

    ordered_rev = [
        "Fecha_Sistema", "Fecha_arca",
        "Razón Social",
        "Pto. Venta_sistema",    "Pto. Venta_arca",
        "N°Comprobante_sistema", "N°Comprobante_arca",
        "cuit_sistema",          "cuit_arca",
        "Gravado_sistema",       "Gravado_arca",
        "No Gravado_sistema",    "No gravado_arca",
        "Otros Tributos_arca",   "Exentas_arca",
        "Imp. Total_sistema",    "Imp. Total_arca",
        "comentario",
    ]
    rev_out = _forzar_importes_float(
        revisar[[c for c in ordered_rev if c in revisar.columns]]
    )
    rev_date_cols = ["Fecha_Sistema", "Fecha_arca"]

    fs_out       = _forzar_importes_float(falta_sistema)
    fs_date_cols = ["Fecha_arca"]

    def _prep_df(df: pd.DataFrame, date_col_names: list[str]) -> tuple[pd.DataFrame, list[int], list[int]]:
        df = df.copy()
        date_col_indices   = []
        amount_col_indices = []
        for i, col in enumerate(df.columns, start=1):
            if col in date_col_names:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                date_col_indices.append(i)
            elif col in COLUMNAS_IMPORTE:
                amount_col_indices.append(i)
        return df, date_col_indices, amount_col_indices

    if df_arca_dep is not None and df_sistema_dep is not None:
        rev_out, rev_date_cols, rev_amount_cols = expandir_revisar(revisar, df_arca_dep, df_sistema_dep)
        rev_prep = rev_out.copy()
        rev_date_idx, rev_amount_idx = [], []
        for i, col in enumerate(rev_prep.columns, start=1):
            if col in rev_date_cols:
                rev_prep[col] = pd.to_datetime(rev_prep[col], errors="coerce")
                rev_date_idx.append(i)
            elif col in rev_amount_cols:
                rev_amount_idx.append(i)
    else:
        rev_prep, rev_date_idx, rev_amount_idx = _prep_df(rev_out, rev_date_cols)
    fs_prep,  fs_date_idx,  fs_amount_idx  = _prep_df(fs_out,  fs_date_cols)

    def _style_sheet(ws, date_col_indices: list[int], amount_col_indices: list[int]) -> None:
        thin          = Side(style="thin")
        header_fill   = PatternFill(start_color="C0C0C0", end_color="C0C0C0", fill_type="solid")
        header_font   = Font(bold=True)
        header_border = Border(left=thin, right=thin, top=thin, bottom=thin)

        for cell in ws[1]:
            cell.fill      = header_fill
            cell.font      = header_font
            cell.border    = header_border
            cell.alignment = Alignment(horizontal="center", vertical="center")

        for col_idx in date_col_indices:
            for row in range(2, ws.max_row + 1):
                ws.cell(row=row, column=col_idx).number_format = DATE_FORMAT

        for col_idx in amount_col_indices:
            for row in range(2, ws.max_row + 1):
                ws.cell(row=row, column=col_idx).number_format = AMOUNT_FORMAT

        for col in ws.columns:
            max_len = max(
                (len(str(cell.value)) if cell.value is not None else 0 for cell in col),
                default=10,
            )
            ws.column_dimensions[get_column_letter(col[0].column)].width = min(max_len + 4, 45)

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        rev_prep.to_excel(writer, sheet_name="revisar",       index=False)
        fs_prep.to_excel( writer, sheet_name="falta_sistema", index=False)

        wb = writer.book
        _style_sheet(wb["revisar"],       rev_date_idx, rev_amount_idx)
        _style_sheet(wb["falta_sistema"], fs_date_idx, fs_amount_idx)
        wb["revisar"].freeze_panes = "B2"

    return buf.getvalue()


# ─────────────────────────────────────────────
# PIPELINE COMPLETO
# ─────────────────────────────────────────────

def correr_cruce(archivo_arca, archivo_sistema, tol_pesos: float = 1.0):
    df_arca    = load_excel_file(archivo_arca)
    df_sistema = load_excel_file(archivo_sistema)

    df_arca_dep    = depurar_arca(df_arca)
    df_sistema_dep = depurar_sistema(df_sistema)

    match, falta_sistema1, falta_arca1, revisar1, no_gravado = cruce1(
        df_arca_dep, df_sistema_dep, tolerancia_total=tol_pesos
    )

    revisar2, falta_arca2, falta_sistema2 = cruce2(
        revisar1, falta_arca1, falta_sistema1, tol_pesos=tol_pesos
    )

    revisar3, falta_arca3, falta_sistema3 = cruce3(
        revisar2, falta_arca2, falta_sistema2, tol_pesos=tol_pesos
    )

    falta_sistema_final = netear_falta_sistema(falta_sistema3, tol_pesos=tol_pesos)

    # Las filas de "no_gravado" (diferencias de composición explicadas)
    # también quedan en "revisar", con su motivo en el comentario.
    revisar3 = pd.concat([revisar3, no_gravado], ignore_index=True)

    stats = {
        "match":             len(match),
        "revisar":           len(revisar3),
        "duplicados":        int((revisar1["comentario"] == "Duplicado").sum()),
        "no_gravado":        len(no_gravado),
        "faltante_arca":     len(falta_arca3),
        "faltante_sistema":  len(falta_sistema_final),
    }

    buf_reporte = generar_excel_en_memoria(
        revisar3, falta_sistema_final,
        df_arca_dep=df_arca_dep, df_sistema_dep=df_sistema_dep,
    )

    return buf_reporte, stats
