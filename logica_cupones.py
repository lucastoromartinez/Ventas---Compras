"""
logica_cupones.py — Conciliación Cupones (Nave vs Extracto Banco)
Código extraído del notebook Cupones.ipynb. Cruza el reporte de
acreditaciones de Nave contra el extracto bancario (que ya llega
normalizado y categorizado, con la columna "conciliacion" cargada).
Solo se agregó:
  - imports al tope
  - loaders adaptados para Streamlit (file-like en vez de Path)
  - correr_conciliacion_cupones() como pipeline de entrada para la app
"""

from io import BytesIO
from itertools import combinations

import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────────────────────────────────────────

def importar_extracto_banco(archivo) -> pd.DataFrame:
    """
    El extracto de banco ya llega normalizado y categorizado (fecha,
    debitos/creditos/saldo/importe tipados y columna "conciliacion"
    cargada), así que se importa tal cual.
    """
    return pd.read_excel(archivo)


def importar_reporte_nave(
    archivo,
    hoja: str | int = 0,
    columna_busqueda: int = 0,
    texto_encabezado: str = "Fecha de operación",
    max_filas_busqueda: int = 50,
) -> pd.DataFrame:
    raw = pd.read_excel(archivo, sheet_name=hoja, header=None, nrows=max_filas_busqueda)

    mascara = raw.iloc[:, columna_busqueda].astype(str).str.strip() == texto_encabezado
    filas_encontradas = raw.index[mascara].tolist()
    if not filas_encontradas:
        raise ValueError(
            f"No se encontró '{texto_encabezado}' en las primeras "
            f"{max_filas_busqueda} filas de la columna {columna_busqueda}."
        )
    fila_header = filas_encontradas[0]

    df = pd.read_excel(archivo, sheet_name=hoja, header=fila_header)
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)

    mascara_total = df.iloc[:, 0].astype(str).str.strip() == "Total"
    filas_total = df.index[mascara_total].tolist()
    if filas_total:
        df = df.loc[:filas_total[0] - 1]
    df.reset_index(drop=True, inplace=True)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# DEPURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

def depurar_leyenda(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deja "leyenda adicional1" reducida al código de operación puro, sacando
    los prefijos que agrega el banco ("Devolución - ", "Operación ",
    "Grupo de acreditación ") y los sufijos "PCT"/"TCTD". Solo se saca el
    guion pegado a "Devolución": el resto de los guiones se respeta porque
    los códigos de grupo también los usan (p.ej. "LS-21825349").
    """
    df = df.copy()
    df["leyenda adicional1"] = (
        df["leyenda adicional1"]
        .astype(str)
        .str.replace(r"^Devolución\s*-\s*", "", regex=True)
        .str.replace(r"^Operación\s+", "", regex=True)
        .str.replace(r"^Grupo de acreditación\s+", "", regex=True)
        .str.replace(" PCT", "", regex=False)
        .str.replace(" TCTD", "", regex=False)
        .str.strip()
    )
    return df


def depurar_nave(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # --- Detectar nombre de columna de operación ---
    if "Número de operación" in df.columns:
        col_operacion = "Número de operación"
    elif "Código de operación" in df.columns:
        col_operacion = "Código de operación"
    else:
        raise ValueError("No se encontró 'Número de operación' ni 'Código de operación' en el DataFrame.")

    # --- Fecha de operación ---
    fecha_op = pd.to_datetime(
        df["Fecha de operación"].astype(str).str.strip(),
        format="%d/%m/%Y %H:%M",
        errors="coerce"
    )
    # 00:00 a 00:05 → corresponde al día anterior
    mask_anterior = (fecha_op.dt.hour == 0) & (fecha_op.dt.minute <= 5)
    fecha_op = fecha_op.where(~mask_anterior, fecha_op - pd.Timedelta(days=1))
    df["Fecha de operación"] = fecha_op.dt.normalize()

    # --- Fecha de acreditación ---
    df["Fecha de acreditación"] = pd.to_datetime(
        df["Fecha de acreditación"].astype(str).str.strip(),
        format="%d/%m/%Y",
        errors="coerce"
    )

    # --- Grupo de acreditación ---
    mask_vacio = (
        df["Grupo de acreditación"].isna() |
        df["Grupo de acreditación"].astype(str).str.strip().isin(["", "nan", "None", "-"])
    )
    df.loc[mask_vacio, "Grupo de acreditación"] = df.loc[mask_vacio, col_operacion]
    df["Grupo de acreditación"] = df["Grupo de acreditación"].astype(str).str.strip()

    # --- Redondeo de columnas float (algunos meses no traen todas) ---
    cols_float = [
        "Monto bruto",
        "Costo del servicio",
        "IVA del costo",
        "Retención IIBB CABA",
        "Monto neto",
        "Propina",
    ]
    for col in cols_float:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = df[col].round(2)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# CRUCE
# ─────────────────────────────────────────────────────────────────────────────

COLUMNAS_AJUSTE = ["Retención IIBB CABA"]  # agregar acá otras deducciones si aparecen


def cruce_nave_banco(
    df_nave_dep: pd.DataFrame,
    df_banco_acred: pd.DataFrame,
    tolerancia: float = 1.0,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Cruza nave vs banco por Grupo de acreditación ↔ leyenda adicional1.

    Primero intenta matchear cada grupo contra "Monto neto" tal cual. Si no
    cierra dentro de la tolerancia, prueba restarle -una por una, y después
    combinadas- las columnas de COLUMNAS_AJUSTE (deducciones que a veces no
    están contempladas en "Monto neto", como la Retención IIBB CABA) hasta
    encontrar la que hace que el importe coincida con el banco. La columna
    "Diferencia Identificada" en match_nave/match_banco indica qué hubo que
    restar para que matcheara ("" si matcheó directo contra Monto neto).

    Si un código se repite en banco (p.ej. acreditación + devolución), se
    toma como match la combinación con menor diferencia y el resto queda
    faltante.

    falta_banco y falta_nave incluyen columna "comentario":
        - "Falta Cupón": el código/grupo no tiene contraparte del otro lado.
        - "Diferencia de Importe": el código está en ambos lados pero ningún
          ajuste probado hizo que el monto cierre dentro de la tolerancia.
        - "Falta Cancelación" (solo en falta_nave): fila de banco duplicada
          de un código ya matcheado (p.ej. la devolución de una
          acreditación que sí reconcilió).

    Retorna: (match_nave, match_banco, falta_banco, falta_nave)
    """
    columnas_ajuste = [c for c in COLUMNAS_AJUSTE if c in df_nave_dep.columns]

    # 1. Sumarizar nave por grupo: Monto neto + cada columna de ajuste disponible
    nave_sum = (
        df_nave_dep
        .groupby("Grupo de acreditación", as_index=False)[["Monto neto"] + columnas_ajuste]
        .sum()
    )

    # 2. Banco con índice propio para tracking
    banco = df_banco_acred.copy().reset_index(drop=True)
    banco["_banco_idx"] = banco.index

    # 3. Merge outer con indicator
    merged = pd.merge(
        nave_sum,
        banco[["_banco_idx", "leyenda adicional1", "importe"]],
        left_on="Grupo de acreditación",
        right_on="leyenda adicional1",
        how="outer",
        indicator=True,
    )

    # 4. Por cada par (grupo, fila de banco) buscar el ajuste que hace matchear
    def buscar_ajuste(row):
        base = row["Monto neto"]
        importe = row["importe"]

        candidatos = [("", base)]  # sin ajuste
        for r in range(1, len(columnas_ajuste) + 1):
            for combo in combinations(columnas_ajuste, r):
                etiqueta = " + ".join(combo)
                ajustado = base - sum(row[c] for c in combo)
                candidatos.append((etiqueta, ajustado))

        mejor_etiqueta = mejor_monto = mejor_diff = None
        for etiqueta, monto in candidatos:
            diff = abs(monto - importe)
            if diff <= tolerancia:
                return pd.Series([monto, diff, etiqueta])
            if mejor_diff is None or diff < mejor_diff:
                mejor_etiqueta, mejor_monto, mejor_diff = etiqueta, monto, diff

        return pd.Series([mejor_monto, mejor_diff, mejor_etiqueta])

    both = merged[merged["_merge"] == "both"].copy()
    if len(both) > 0:
        ajustes = both.apply(buscar_ajuste, axis=1)
        ajustes.columns = ["monto_neto_suma", "diferencia", "diferencia_identificada"]
        both = both.join(ajustes)
    else:
        both["monto_neto_suma"] = both.get("Monto neto")
        both["diferencia"] = pd.Series(dtype=float)
        both["diferencia_identificada"] = pd.Series(dtype=str)

    # 5. Si un código se repite en banco, nos quedamos con la combinación de
    #    menor diferencia como el match real; el resto son duplicados.
    both = both.sort_values("diferencia")
    elegidos = both.drop_duplicates(subset="Grupo de acreditación", keep="first")
    duplicados_idx = set(both["_banco_idx"]) - set(elegidos["_banco_idx"])

    mask_match      = elegidos["diferencia"] <= tolerancia
    mask_left_only  = merged["_merge"] == "left_only"
    mask_right_only = merged["_merge"] == "right_only"

    # 6. Asignar match_id a los matches
    matches = elegidos[mask_match].copy().reset_index(drop=True)
    matches["match_id"] = [f"MATCH-{i+1:04d}" for i in range(len(matches))]

    # 7. match_nave: filas de nave de grupos matcheados, con el ajuste identificado
    grupo_a_id     = matches.set_index("Grupo de acreditación")["match_id"].to_dict()
    grupo_a_ajuste = matches.set_index("Grupo de acreditación")["diferencia_identificada"].to_dict()
    nave_out = df_nave_dep.copy()
    nave_out["match_id"] = nave_out["Grupo de acreditación"].map(grupo_a_id)
    nave_out["Diferencia Identificada"] = nave_out["Grupo de acreditación"].map(grupo_a_ajuste)
    match_nave = nave_out[nave_out["match_id"].notna()].reset_index(drop=True)

    # 8. match_banco: filas de banco matcheadas, con el ajuste identificado
    idx_a_id     = matches.set_index("_banco_idx")["match_id"].to_dict()
    idx_a_ajuste = matches.set_index("_banco_idx")["diferencia_identificada"].to_dict()
    banco_out = banco.copy()
    banco_out["match_id"] = banco_out["_banco_idx"].map(idx_a_id)
    banco_out["Diferencia Identificada"] = banco_out["_banco_idx"].map(idx_a_ajuste)
    match_banco = (
        banco_out[banco_out["match_id"].notna()]
        .drop(columns=["_banco_idx"])
        .reset_index(drop=True)
    )

    # 9. falta_banco: grupos de nave sin match, con motivo
    grupos_falta_cupon = set(merged.loc[mask_left_only, "Grupo de acreditación"].dropna())
    grupos_diferencia  = set(elegidos.loc[~mask_match, "Grupo de acreditación"])

    falta_banco = (
        df_nave_dep[df_nave_dep["Grupo de acreditación"].isin(grupos_falta_cupon | grupos_diferencia)]
        .copy()
        .reset_index(drop=True)
    )
    falta_banco["comentario"] = falta_banco["Grupo de acreditación"].map(
        lambda g: "Diferencia de Importe" if g in grupos_diferencia else "Falta Cupón"
    )

    # 10. falta_nave: filas de banco sin match, con motivo
    idx_falta_cupon = set(merged.loc[mask_right_only, "_banco_idx"].dropna().astype(int))
    idx_diferencia  = set(elegidos.loc[~mask_match, "_banco_idx"].dropna().astype(int))

    def comentario_banco(idx: int) -> str:
        if idx in duplicados_idx:
            return "Falta Cancelación"
        if idx in idx_diferencia:
            return "Diferencia de Importe"
        return "Falta Cupón"

    idx_sin_match = idx_falta_cupon | idx_diferencia | duplicados_idx
    falta_nave = banco[banco["_banco_idx"].isin(idx_sin_match)].copy()
    falta_nave["comentario"] = falta_nave["_banco_idx"].map(comentario_banco)
    falta_nave = falta_nave.drop(columns=["_banco_idx"]).reset_index(drop=True)

    return match_nave, match_banco, falta_banco, falta_nave


# ─────────────────────────────────────────────────────────────────────────────
# EXPORTAR EN MEMORIA
# ─────────────────────────────────────────────────────────────────────────────

def generar_excel_en_memoria_cupones(match_nave, match_banco, falta_banco, falta_nave) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        match_nave.to_excel(writer,  sheet_name="Match Nave",   index=False)
        match_banco.to_excel(writer, sheet_name="Match Banco",  index=False)
        falta_banco.to_excel(writer, sheet_name="Falta Banco",  index=False)
        falta_nave.to_excel(writer,  sheet_name="Falta Nave",   index=False)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE COMPLETO
# ─────────────────────────────────────────────────────────────────────────────

def correr_conciliacion_cupones(archivo_banco, archivo_nave, tolerancia: float = 1.0):
    # 1. Cargar
    df_banco = importar_extracto_banco(archivo_banco)
    df_nave  = importar_reporte_nave(archivo_nave)

    # 2. Depurar
    df_banco_acred = depurar_leyenda(df_banco)
    df_nave_dep    = depurar_nave(df_nave)

    # 3. Cruzar
    match_nave, match_banco, falta_banco, falta_nave = cruce_nave_banco(
        df_nave_dep, df_banco_acred, tolerancia=tolerancia
    )

    match_ajustado = int((match_banco["Diferencia Identificada"].fillna("") != "").sum())

    stats = {
        "grupos_matcheados": len(match_banco),
        "match_ajustado":    match_ajustado,
        "falta_banco":       len(falta_banco),
        "falta_nave":        len(falta_nave),
    }

    buf = generar_excel_en_memoria_cupones(match_nave, match_banco, falta_banco, falta_nave)
    return buf, stats
