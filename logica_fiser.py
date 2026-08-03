"""
logica_fiser.py — Conciliación Fiser (Extracto Banco vs Reporte Fiser)

Cruza el extracto bancario contra el reporte de liquidaciones de Fiser:
  1. Depura ambos lados (fechas a datetime, importes a float64 redondeado a 2 decimales).
  2. Cruce exacto uno a uno por (fecha, importe).
  3. Sobre lo remanente, cruce acumulado por día (suma de importes del día).
  4. Arma tabla resumen diaria (Banco vs Fiser) y exporta todo a un Excel en memoria.
"""

from io import BytesIO
from collections import defaultdict, deque

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────────

def load_excel_file(archivo) -> pd.DataFrame:
    return pd.read_excel(archivo)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _get_col(df: pd.DataFrame, *candidatos: str) -> str:
    """Busca una columna ignorando mayúsculas/espacios extra."""
    mapa = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidatos:
        real = mapa.get(cand.strip().lower())
        if real is not None:
            return real
    raise KeyError(f"Ninguna de {candidatos} encontrada. Disponibles: {list(df.columns)}")


def _a_float(col: pd.Series) -> pd.Series:
    """Convierte a float manejando formato argentino (punto de miles, coma decimal)."""
    if pd.api.types.is_numeric_dtype(col):
        return col.fillna(0).astype(float)
    return (
        col.astype(str).str.strip()
           .str.replace(".", "", regex=False)
           .str.replace(",", ".", regex=False)
           .replace({"": "0", "nan": "0", "None": "0"})
           .astype(float)
    )


# ─────────────────────────────────────────────
# DEPURACIÓN
# ─────────────────────────────────────────────

def depurar_banco(df_banco: pd.DataFrame) -> pd.DataFrame:
    df = df_banco.copy()
    mapa = {str(c).strip().lower(): c for c in df.columns}

    if "importe" not in mapa:
        col_credito = _get_col(df, "CREDITO EN $")
        col_debito  = _get_col(df, "DEBITO EN $")
        credito = _a_float(df[col_credito])
        debito  = _a_float(df[col_debito])
        df["Importe"] = (credito - debito).round(2).astype("float64")
    else:
        col_importe = mapa["importe"]
        df[col_importe] = _a_float(df[col_importe]).round(2).astype("float64")

    return df


COLUMNAS_FLOAT_FISER = [
    "IMPORTE NETO",
    "VENTAS C/DESCUENTO CONTADO",
    "TOTAL IMPORTE ACEPTADO",
    "ARANCEL",
    "IVA CRED.FISC.COMERCIO S/ARANC 21,00%",
    "TOTAL DEDUCCIONES",
    "TOTAL LIQUIDACION",
    "SUBTOTAL NETO DE PAGOS",
    "CARGO TERMINAL FISERV",
    "TOTAL PAGOS DE COMERCIOS",
    "QR PERCEPCION IVA 3337",
    "QR PERC. IIBB. CABA  REG GRAL",
]


def depurar_fiser(df_fiser: pd.DataFrame) -> pd.DataFrame:
    df = df_fiser.copy()

    for nombre in ["FECHA DE PAGO", "FECHA DE PRESENTACION"]:
        col = _get_col(df, nombre)
        df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")

    for nombre in COLUMNAS_FLOAT_FISER:
        try:
            col = _get_col(df, nombre)
        except KeyError:
            continue
        df[col] = _a_float(df[col]).round(2).astype("float64")

    return df


# ─────────────────────────────────────────────
# CRUCE
# ─────────────────────────────────────────────

def cruzar_banco_fiser(df_banco_dep: pd.DataFrame, df_fiser_dep: pd.DataFrame, tolerancia: float = 1.0):
    banco = df_banco_dep.copy().reset_index(drop=True)
    fiser = df_fiser_dep.copy().reset_index(drop=True)

    col_fecha_banco   = _get_col(banco, "FECHA")
    col_importe_banco = _get_col(banco, "importe", "Importe")
    col_fecha_fiser   = _get_col(fiser, "FECHA DE PAGO")
    col_importe_fiser = _get_col(fiser, "IMPORTE NETO")

    banco["Match ID"] = np.nan
    banco["Match Tipo"] = None
    fiser["Match ID"] = np.nan
    fiser["Match Tipo"] = None

    match_id = 1

    # Fase 1: match exacto uno a uno (misma fecha + mismo importe)
    pool_fiser = defaultdict(deque)
    for idx, row in fiser.iterrows():
        key = (row[col_fecha_fiser], round(row[col_importe_fiser], 2))
        pool_fiser[key].append(idx)

    for idx, row in banco.iterrows():
        key = (row[col_fecha_banco], round(row[col_importe_banco], 2))
        if pool_fiser.get(key):
            idx_fiser = pool_fiser[key].popleft()
            banco.loc[idx, "Match ID"] = match_id
            banco.loc[idx, "Match Tipo"] = "Exacto"
            fiser.loc[idx_fiser, "Match ID"] = match_id
            fiser.loc[idx_fiser, "Match Tipo"] = "Exacto"
            match_id += 1

    # Fase 2: match acumulado por día, sobre lo remanente
    banco_pend = banco[banco["Match Tipo"].isna()]
    fiser_pend = fiser[fiser["Match Tipo"].isna()]

    suma_banco_dia = banco_pend.groupby(col_fecha_banco)[col_importe_banco].sum().round(2)
    suma_fiser_dia = fiser_pend.groupby(col_fecha_fiser)[col_importe_fiser].sum().round(2)

    dias_comunes = suma_banco_dia.index.intersection(suma_fiser_dia.index)

    for dia in dias_comunes:
        if abs(suma_banco_dia[dia] - suma_fiser_dia[dia]) < tolerancia:
            idx_banco_dia = banco_pend[banco_pend[col_fecha_banco] == dia].index
            idx_fiser_dia = fiser_pend[fiser_pend[col_fecha_fiser] == dia].index

            banco.loc[idx_banco_dia, "Match ID"] = match_id
            banco.loc[idx_banco_dia, "Match Tipo"] = "Acumulado"
            fiser.loc[idx_fiser_dia, "Match ID"] = match_id
            fiser.loc[idx_fiser_dia, "Match Tipo"] = "Acumulado"
            match_id += 1

    match_banco = banco[banco["Match Tipo"].notna()].copy()
    match_banco["Match ID"] = match_banco["Match ID"].astype(int)

    match_fiser = fiser[fiser["Match Tipo"].notna()].copy()
    match_fiser["Match ID"] = match_fiser["Match ID"].astype(int)

    falta_banco = banco[banco["Match Tipo"].isna()].drop(columns=["Match ID", "Match Tipo"]).copy()
    falta_fiser = fiser[fiser["Match Tipo"].isna()].drop(columns=["Match ID", "Match Tipo"]).copy()

    resumen_banco = banco.groupby(col_fecha_banco)[col_importe_banco].sum().round(2)
    resumen_fiser = fiser.groupby(col_fecha_fiser)[col_importe_fiser].sum().round(2)

    todas_fechas = sorted(set(resumen_banco.index) | set(resumen_fiser.index))
    tabla_resumen = pd.DataFrame({"Fecha": todas_fechas})
    tabla_resumen["Importe Banco"] = tabla_resumen["Fecha"].map(resumen_banco).fillna(0).round(2)
    tabla_resumen["Importe Fiser"] = tabla_resumen["Fecha"].map(resumen_fiser).fillna(0).round(2)
    tabla_resumen["Diferencia"] = (tabla_resumen["Importe Banco"] - tabla_resumen["Importe Fiser"]).round(2)

    stats = {
        "match_exacto":    int((match_banco["Match Tipo"] == "Exacto").sum()),
        "match_acumulado": int(match_banco.loc[match_banco["Match Tipo"] == "Acumulado", "Match ID"].nunique()),
        "falta_banco":     len(falta_banco),
        "falta_fiser":     len(falta_fiser),
    }

    return tabla_resumen, match_banco, match_fiser, falta_banco, falta_fiser, stats


# ─────────────────────────────────────────────
# EXPORTAR EN MEMORIA
# ─────────────────────────────────────────────

def generar_excel_en_memoria_fiser(tabla_resumen, match_banco, match_fiser, falta_banco, falta_fiser) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        tabla_resumen.to_excel(writer, sheet_name="Tabla Resumen", index=False)
        match_banco.to_excel(writer,   sheet_name="Match Banco",   index=False)
        match_fiser.to_excel(writer,   sheet_name="Match Fiser",   index=False)
        falta_banco.to_excel(writer,   sheet_name="Falta Banco",   index=False)
        falta_fiser.to_excel(writer,   sheet_name="Falta Fiser",   index=False)
    return buf.getvalue()


# ─────────────────────────────────────────────
# PIPELINE COMPLETO
# ─────────────────────────────────────────────

def correr_conciliacion_fiser(archivo_banco, archivo_fiser, tolerancia: float = 1.0):
    df_banco = load_excel_file(archivo_banco)
    df_fiser = load_excel_file(archivo_fiser)

    df_banco_dep = depurar_banco(df_banco)
    df_fiser_dep = depurar_fiser(df_fiser)

    tabla_resumen, match_banco, match_fiser, falta_banco, falta_fiser, stats = cruzar_banco_fiser(
        df_banco_dep, df_fiser_dep, tolerancia=tolerancia
    )

    buf = generar_excel_en_memoria_fiser(tabla_resumen, match_banco, match_fiser, falta_banco, falta_fiser)
    return buf, stats
