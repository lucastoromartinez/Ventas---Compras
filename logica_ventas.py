import re
import pandas as pd
from io import BytesIO


# ─────────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────────

def load_excel_file(file) -> pd.DataFrame:
    return pd.read_excel(file, dtype=str)


# ─────────────────────────────────────────────
# DEPURACIÓN SISTEMA
# ─────────────────────────────────────────────

def depurar_sistema_ventas(df_sistema: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara el dataframe del sistema para el cruce de ventas.
    Normaliza nombres de columnas, genera nro_norm y convierte importes a numérico.
    """
    def norm_col(nombre: str) -> str:
        nombre = nombre.lower()
        nombre = re.sub(r'[\.\s]+', '_', nombre)
        nombre = re.sub(r'_+', '_', nombre)
        return nombre.strip('_')

    df = df_sistema.copy()
    df.columns = [norm_col(c) for c in df.columns]

    pto_vta = (
        pd.to_numeric(df["nro_pto_vta"], errors="coerce")
        .fillna(0).astype(int).astype(str).str.zfill(4)
    )
    nro = (
        pd.to_numeric(df["nro"], errors="coerce")
        .fillna(0).astype(int).astype(str).str.zfill(8)
    )
    df["nro_norm"] = (pto_vta + nro).astype(str)
    df = df.drop(columns=["nro_pto_vta", "nro"])

    for col in ["imp_neto_gravado", "iva_ri", "perc_iibb", "perc_iva", "perc_gcias", "imp_total"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["nro_norm"] = df["nro_norm"].astype(str)
    return df


# ─────────────────────────────────────────────
# DEPURACIÓN ARCA
# ─────────────────────────────────────────────

def depurar_arca_ventas(df_arca: pd.DataFrame, pv_excluir: list = None) -> pd.DataFrame:
    """
    Prepara el dataframe de ARCA para el cruce de ventas.
    pv_excluir: lista de puntos de venta a excluir (ej: [2, 16, 60] para Ronda)
    """
    df = df_arca.copy()

    # --- 0. Excluir puntos de venta si se especifica ---
    if pv_excluir:
        df = df[
            ~pd.to_numeric(df["Punto de Venta"], errors="coerce").isin(pv_excluir)
        ].copy()

    columnas_importes = [
        "Imp. Neto Gravado Total",
        "Imp. Neto No Gravado",
        "Imp. Op. Exentas",
        "Otros Tributos",
        "Total IVA",
        "Imp. Total"
    ]

    # Detectar columna de número robustamente
    def _find_col(df, candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    c_nro = _find_col(df, ["Número Desde", "NÃºmero Desde", "Numero Desde", "Nro Desde"])

    # --- 1. Generar nro_norm ---
    pv = (
        pd.to_numeric(df["Punto de Venta"], errors="coerce")
        .fillna(0).astype(int).astype(str).str.zfill(4)
    )
    nro = (
        pd.to_numeric(df[c_nro], errors="coerce")
        .fillna(0).astype(int).astype(str).str.zfill(8)
    )
    df["nro_norm"] = (pv + nro).astype(str)

    # --- 2. Convertir columnas a numérico ---
    columnas_numericas = ["Tipo Cambio", "Tipo de Comprobante"] + columnas_importes
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- 3. Cambiar signo a notas de crédito (tipo 3 o 8) ---
    mask_nc = df["Tipo de Comprobante"].isin([3, 8])
    for col in columnas_importes:
        if col in df.columns:
            df.loc[mask_nc, col] = df.loc[mask_nc, col] * -1

    # --- 4. Aplicar tipo de cambio cuando sea distinto de 1 ---
    mask_tc = df["Tipo Cambio"].notna() & (df["Tipo Cambio"] != 1)
    for col in columnas_importes:
        if col in df.columns:
            df.loc[mask_tc, col] = df.loc[mask_tc, col] * df.loc[mask_tc, "Tipo Cambio"]

    # --- 5. Normalizar nro_norm a string ---
    df["nro_norm"] = df["nro_norm"].astype(str)

    return df


# ─────────────────────────────────────────────
# CRUCE 1: Sistema vs ARCA por nro_norm
# ─────────────────────────────────────────────

def cruzar_ventas_por_nro_norm(
    sistema_dep: pd.DataFrame,
    arca_dep: pd.DataFrame
):
    """
    Cruza sistema vs ARCA usando únicamente la columna 'nro_norm'.
    """
    sistema = sistema_dep.copy()
    arca    = arca_dep.copy()

    sistema["nro_norm"] = sistema["nro_norm"].astype(str).str.strip()
    arca["nro_norm"]    = arca["nro_norm"].astype(str).str.strip()

    matcheado = sistema.merge(
        arca, on="nro_norm", how="inner", suffixes=("_sistema", "_arca")
    )

    faltante_en_sistema = arca.loc[
        ~arca["nro_norm"].isin(sistema["nro_norm"])
    ].copy()

    faltante_en_arca = sistema.loc[
        ~sistema["nro_norm"].isin(arca["nro_norm"])
    ].copy()

    return matcheado, faltante_en_sistema, faltante_en_arca


# ─────────────────────────────────────────────
# CRUCE 2: Faltante en sistema vs sistema previo
# ─────────────────────────────────────────────

def consolidar_segundo_cruce_con_sistema_prev(
    match_viejo: pd.DataFrame,
    faltante_en_sistema_viejo: pd.DataFrame,
    sistema_prev_dep: pd.DataFrame
):
    match        = match_viejo.copy()
    faltante     = faltante_en_sistema_viejo.copy()
    sistema_prev = sistema_prev_dep.copy()

    match["nro_norm"]        = match["nro_norm"].astype(str).str.strip()
    faltante["nro_norm"]     = faltante["nro_norm"].astype(str).str.strip()
    sistema_prev["nro_norm"] = sistema_prev["nro_norm"].astype(str).str.strip()

    nuevo_match = faltante.merge(
        sistema_prev[["nro_norm"]],
        on="nro_norm",
        how="inner"
    )

    match_definitivo = pd.concat([match, nuevo_match], ignore_index=True)

    faltante_en_sistema_definitivo = faltante.loc[
        ~faltante["nro_norm"].isin(sistema_prev["nro_norm"])
    ].copy()

    return match_definitivo, faltante_en_sistema_definitivo


# ─────────────────────────────────────────────
# CRUCE 3: Faltante en ARCA vs ARCA posterior
# ─────────────────────────────────────────────

def consolidar_cruce_faltante_arca(
    match_definitivo: pd.DataFrame,
    faltante_en_arca: pd.DataFrame,
    df_arca_post_dep: pd.DataFrame
):
    """
    Cruza el faltante en arca contra arca_post para detectar
    registros que finalmente aparecieron en el mes siguiente.
    """
    match     = match_definitivo.copy()
    faltante  = faltante_en_arca.copy()
    arca_post = df_arca_post_dep.copy()

    match["nro_norm"]     = match["nro_norm"].astype(str).str.strip()
    faltante["nro_norm"]  = faltante["nro_norm"].astype(str).str.strip()
    arca_post["nro_norm"] = arca_post["nro_norm"].astype(str).str.strip()

    nuevo_match = faltante.merge(
        arca_post[["nro_norm"]], on="nro_norm", how="inner"
    )

    match_definitivo_final = pd.concat([match, nuevo_match], ignore_index=True)

    faltante_en_arca_def = faltante.loc[
        ~faltante["nro_norm"].isin(arca_post["nro_norm"])
    ].copy()

    return match_definitivo_final, faltante_en_arca_def


# ─────────────────────────────────────────────
# EXPORTAR EN MEMORIA
# ─────────────────────────────────────────────

def generar_excel_en_memoria(
    faltante_en_sistema_definitivo: pd.DataFrame,
    faltante_en_arca_def: pd.DataFrame,
) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        faltante_en_sistema_definitivo.to_excel(
            writer, sheet_name="faltante_en_sistema", index=False
        )
        faltante_en_arca_def.to_excel(
            writer, sheet_name="faltante_en_arca", index=False
        )
    return buffer.getvalue()


# ─────────────────────────────────────────────
# PIPELINE COMPLETO
# ─────────────────────────────────────────────

def correr_cruce_ventas(
    archivo_arca,
    archivo_sistema,
    archivo_sistema_prev,
    archivo_arca_post,
    pv_excluir: list = None,
):
    # 1. Cargar
    df_arca         = load_excel_file(archivo_arca)
    df_sistema      = load_excel_file(archivo_sistema)
    df_sistema_prev = load_excel_file(archivo_sistema_prev)
    df_arca_post    = load_excel_file(archivo_arca_post)

    # 2. Depurar
    sistema_dep     = depurar_sistema_ventas(df_sistema)
    arca_dep        = depurar_arca_ventas(df_arca, pv_excluir=pv_excluir)
    sistema_prev_dep = depurar_sistema_ventas(df_sistema_prev)
    arca_post_dep   = depurar_arca_ventas(df_arca_post, pv_excluir=pv_excluir)

    # 3. Cruce 1: Sistema vs ARCA
    matcheado, faltante_en_sistema, faltante_en_arca = cruzar_ventas_por_nro_norm(
        sistema_dep, arca_dep
    )

    # 4. Cruce 2: Faltante en sistema vs sistema previo
    match_definitivo, faltante_en_sistema_definitivo = consolidar_segundo_cruce_con_sistema_prev(
        match_viejo=matcheado,
        faltante_en_sistema_viejo=faltante_en_sistema,
        sistema_prev_dep=sistema_prev_dep
    )

    # 5. Cruce 3: Faltante en ARCA vs ARCA posterior
    match_definitivo_final, faltante_en_arca_def = consolidar_cruce_faltante_arca(
        match_definitivo=match_definitivo,
        faltante_en_arca=faltante_en_arca,
        df_arca_post_dep=arca_post_dep
    )

    # 6. Estadísticas
    stats = {
        "matcheado":                len(matcheado),
        "match_definitivo":         len(match_definitivo_final),
        "faltante_en_sistema":      len(faltante_en_sistema_definitivo),
        "faltante_en_arca":         len(faltante_en_arca_def),
    }

    # 7. Exportar
    buf = generar_excel_en_memoria(faltante_en_sistema_definitivo, faltante_en_arca_def)

    return buf, stats
