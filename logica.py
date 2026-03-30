from pathlib import Path
import re
import unicodedata
import numpy as np
import pandas as pd
from typing import Literal


# ─────────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────────

def load_excel_file(file) -> pd.DataFrame:
    return pd.read_excel(file, dtype=str)


# ─────────────────────────────────────────────
# DEPURACIÓN SISTEMA
# ─────────────────────────────────────────────

def depurar_sistema(df: pd.DataFrame) -> pd.DataFrame:
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

    c_cuit = _resolve(["CUIT", "Cuit", "CUIT ", "C.U.I.T", "C U I T", "CUIT/CUIL"])
    c_nro  = _resolve(["Nro.", "Nro", "Numero", "Número", "N°", "Nro comprobante", "Nro Comprobante"])
    c_tipo_doc = _resolve(["Tipo Doc.", "Tipo Doc", "Tipo Documento", "Tipo de Documento"], required=False)

    df["CUIT_norm"] = (
        df[c_cuit]
        .astype(str)
        .str.replace("-", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.strip()
    )

    df["Nro_norm"] = (
        df[c_nro]
        .astype(str)
        .str.strip()
        .str.replace("-", "", regex=False)
    )

    df["CUIT_norm"] = df["CUIT_norm"].astype(str)
    df["Nro_norm"]  = df["Nro_norm"].astype(str)

    importes_aliases: dict[str, list[str]] = {
        "Imp. Neto Gravado":    ["Imp. Neto Gravado", "Imp Neto Gravado", "Neto Gravado", "Neto Grav"],
        "Imp. Neto No Gravado": ["Imp. Neto No Gravado", "Imp Neto No Gravado", "Neto No Gravado", "No Gravado"],
        "IVA 10,5%":  ["IVA 10,5%", "IVA 10.5%", "IVA 10,5", "IVA 10.5", "IVA 10"],
        "IVA 21%":    ["IVA 21%", "IVA 21", "IVA21"],
        "IVA 27%":    ["IVA 27%", "IVA 27", "IVA27"],
        "Imp. Int.":  ["Imp. Int.", "Imp Int", "Impuestos Internos", "Imp Internos"],
        "Perc. Gcias.":    ["Perc. Gcias.", "Perc Gcias", "Percepcion Ganancias", "Perc. Ganancias"],
        "Perc. IVA":       ["Perc. IVA", "Perc IVA", "Percepcion IVA"],
        "Perc. IIBB CABA": ["Perc. IIBB CABA", "Perc IIBB CABA", "Percep IIBB CABA", "IIBB CABA"],
        "Perc. IIBB BS AS":["Perc. IIBB BS AS", "Perc IIBB BS AS", "Perc. IIBB Bs As", "IIBB BS AS", "IIBB Buenos Aires"],
        "Perc. SUSS":  ["Perc. SUSS", "Perc SUSS", "Percepcion SUSS", "SUSS"],
        "SIRCREB":     ["SIRCREB", "Sircreb"],
        "Total":       ["Total", "Importe Total", "Total Comprobante", "Total Factura"],
    }

    col_importes_reales: dict[str, str] = {}
    for canon, aliases in importes_aliases.items():
        col = _resolve(aliases, required=False)
        if col is not None:
            col_importes_reales[canon] = col

    for canon, col in col_importes_reales.items():
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    if c_tipo_doc is not None:
        tipo_norm = (
            df[c_tipo_doc]
            .astype(str)
            .str.strip()
            .str.lower()
        )
        df = df[~tipo_norm.eq("factura gastos")].copy()

    df = df.drop(columns=[c_cuit, c_nro], errors="ignore")
    return df


# ─────────────────────────────────────────────
# DEPURACIÓN ARCA
# ─────────────────────────────────────────────

def depurar_arca(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()

    # Detectar columnas de Punto de Venta y Número Desde robustamente
    def _find_col(df, candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    c_pto_vta = _find_col(df, ["Punto de Venta", "Pto. Venta", "Pto Venta"])
    c_nro_desde = _find_col(df, ["Número Desde", "NÃºmero Desde", "Numero Desde", "Nro Desde", "Nro. Desde"])

    if c_pto_vta and c_nro_desde:
        df[c_pto_vta]   = pd.to_numeric(df[c_pto_vta], errors="coerce")
        df[c_nro_desde] = pd.to_numeric(df[c_nro_desde], errors="coerce")
        df[c_pto_vta]   = df[c_pto_vta].astype("Int64").astype(str).str.zfill(4)
        df[c_nro_desde] = df[c_nro_desde].astype("Int64").astype(str).str.zfill(8)
        df["Nro_norm"]  = df[c_pto_vta] + df[c_nro_desde]
        df = df.drop(columns=[c_pto_vta, c_nro_desde])

    c_emisor = _find_col(df, ["Nro. Doc. Emisor", "Nro Doc Emisor", "CUIT Emisor"])
    if c_emisor:
        df[c_emisor] = df[c_emisor].astype(str).str.strip()

    columnas_importe = [
        "Imp. Neto Gravado IVA 0%", "IVA 2,5%", "Imp. Neto Gravado IVA 2,5%",
        "IVA 5%", "Imp. Neto Gravado IVA 5%", "IVA 10,5%", "Imp. Neto Gravado IVA 10,5%",
        "IVA 21%", "Imp. Neto Gravado IVA 21%", "IVA 27%", "Imp. Neto Gravado IVA 27%",
        "Imp. Neto Gravado Total", "Imp. Neto No Gravado", "Imp. Op. Exentas",
        "Otros Tributos", "Total IVA", "Imp. Total",
    ]
    columnas_importe = [c for c in columnas_importe if c in df.columns]
    df[columnas_importe] = df[columnas_importe].apply(pd.to_numeric, errors="coerce")

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
        df["Tipo de Comprobante"].astype(str).str.strip(), errors="coerce"
    )
    mask_nc = df["Tipo de Comprobante"].isin([3, 8, 13])
    df.loc[mask_nc, columnas_importe] *= -1

    if "Imp. Neto Gravado IVA 0%" in df.columns:
        df["Imp. Neto Gravado IVA 0%"] = pd.to_numeric(df["Imp. Neto Gravado IVA 0%"], errors="coerce")
    if "Imp. Neto No Gravado" in df.columns:
        df["Imp. Neto No Gravado"] = pd.to_numeric(df["Imp. Neto No Gravado"], errors="coerce")

    return df


# ─────────────────────────────────────────────
# CRUCE 1: por Nro + CUIT
# ─────────────────────────────────────────────

def cruzar_por_nro_y_cuit(df_sistema: pd.DataFrame, df_arca: pd.DataFrame):
    sis  = df_sistema.copy()
    arca = df_arca.copy()
    sis.columns  = sis.columns.str.strip()
    arca.columns = arca.columns.str.strip()

    sis["_conteo"]  = sis.groupby(["Nro_norm", "CUIT_norm"]).cumcount() + 1
    arca["_conteo"] = arca.groupby(["Nro_norm", "Nro. Doc. Emisor"]).cumcount() + 1

    merge = pd.merge(
        sis, arca,
        left_on=["Nro_norm", "CUIT_norm", "_conteo"],
        right_on=["Nro_norm", "Nro. Doc. Emisor", "_conteo"],
        how="outer", indicator=True, suffixes=("_sis", "_arca")
    )

    merge["CUIT"] = merge["CUIT_norm"].combine_first(merge["Nro. Doc. Emisor"])

    match            = merge[merge["_merge"] == "both"].copy()
    faltantes_arca   = merge[merge["_merge"] == "left_only"].copy()
    faltantes_sistema = merge[merge["_merge"] == "right_only"].copy()

    cols_keep = ["Nro_norm", "CUIT"]
    for df_ in [match, faltantes_arca, faltantes_sistema]:
        extra = [c for c in df_.columns if c not in cols_keep + ["_conteo", "_merge"]]
        df_ = df_[cols_keep + extra]

    match.drop(columns=["_conteo", "_merge"], errors="ignore", inplace=True)
    faltantes_arca.drop(columns=["_conteo", "_merge"], errors="ignore", inplace=True)
    faltantes_sistema.drop(columns=["_conteo", "_merge"], errors="ignore", inplace=True)

    return match, faltantes_sistema, faltantes_arca


# ─────────────────────────────────────────────
# REVISAR INCONSISTENCIAS EN MATCH
# ─────────────────────────────────────────────

def revisar_inconsistencias_en_match(match: pd.DataFrame, tol_pesos: float = 1.0) -> pd.DataFrame:
    df = match.copy()
    df.columns = df.columns.astype(str).str.strip()

    def _pick(df_: pd.DataFrame, candidates: list[str]) -> str:
        cols = set(df_.columns)
        for c in candidates:
            if c in cols:
                return c
        raise KeyError(f"No encontré columna. Candidatos={candidates}. Disponibles={list(df_.columns)}")

    def _to_dt_norm(series): return pd.to_datetime(series, errors="coerce").dt.normalize()
    def _to_num(series):     return pd.to_numeric(series, errors="coerce")

    c_nro       = _pick(df, ["Nro_norm"])
    c_cuit_norm = _pick(df, ["CUIT_norm", "CUIT"])
    c_fecha_sis  = _pick(df, ["Fecha"])
    c_fecha_arca = _pick(df, ["Fecha de EmisiÃ³n", "Fecha de Emisión", "Fecha de Emision"])
    c_grav_sis   = _pick(df, ["Imp. Neto Gravado"])
    c_grav_arca  = _pick(df, ["Imp. Neto Gravado Total"])
    c_nograv_sis  = _pick(df, ["Imp. Neto No Gravado_sis", "Imp. Neto No Gravado"])
    c_nograv_arca = _pick(df, ["Imp. Neto No Gravado_arca", "Imp. Neto No Gravado"])
    c_tipo_comp   = _pick(df, ["Tipo de Comprobante"])

    mask_fecha  = ~(_to_dt_norm(df[c_fecha_sis]) == _to_dt_norm(df[c_fecha_arca]))
    grav_sis    = _to_num(df[c_grav_sis]).fillna(0.0).round(2)
    grav_arca   = _to_num(df[c_grav_arca]).fillna(0.0).round(2)
    mask_grav   = (grav_sis - grav_arca).abs() > float(tol_pesos)

    nograv_sis  = _to_num(df[c_nograv_sis]).fillna(0.0).round(2)
    nograv_arca = _to_num(df[c_nograv_arca]).fillna(0.0).round(2)
    mask_nograv_raw = (nograv_sis - nograv_arca).abs() > float(tol_pesos)

    tipo_comp = pd.to_numeric(df[c_tipo_comp], errors="coerce")
    mask_nograv = mask_nograv_raw & ~(tipo_comp == 11)
    mask_any    = mask_fecha | mask_grav | mask_nograv
    revisar     = df.loc[mask_any].copy()

    def _comentario_row(i):
        motivos = []
        if bool(mask_fecha.loc[i]):  motivos.append("Fecha")
        if bool(mask_grav.loc[i]):   motivos.append("Gravado")
        if bool(mask_nograv.loc[i]): motivos.append("No Gravado")
        return ", ".join(motivos)

    revisar["comentario"] = (
        [_comentario_row(i) for i in revisar.index] if not revisar.empty
        else pd.Series(dtype="object")
    )
    revisar["Nro_norm_sis"]  = revisar[c_nro]
    revisar["Nro_norm_arca"] = revisar[c_nro]
    revisar["CUIT_sis"]      = revisar[c_cuit_norm]
    revisar["CUIT_arca"]     = revisar[c_cuit_norm]

    return revisar


# ─────────────────────────────────────────────
# DEPURAR FALTANTES POST MERGE
# ─────────────────────────────────────────────

def depurar_faltantes_post_merge(
    df_faltante: pd.DataFrame,
    origen: Literal["sis", "arca"],
    drop_all_null_cols: bool = True
) -> pd.DataFrame:
    df = df_faltante.copy()
    df.columns = df.columns.str.strip()

    sufijo_origen = f"_{origen}"
    sufijo_otro   = "_arca" if origen == "sis" else "_sis"

    df = df.drop(columns=[c for c in df.columns if c.endswith(sufijo_otro)], errors="ignore")
    rename_map = {c: c.replace(sufijo_origen, "") for c in df.columns if c.endswith(sufijo_origen)}
    df = df.rename(columns=rename_map)

    if drop_all_null_cols:
        df = df.dropna(axis=1, how="all")

    return df


# ─────────────────────────────────────────────
# CRUCE 2: faltantes por CUIT/Fecha/Importes
# ─────────────────────────────────────────────

def cruzar_faltantes_por_cuit_fecha_importes_append_revisar(
    revisar: pd.DataFrame,
    faltantes_sistema_dep: pd.DataFrame,
    faltantes_arca_dep: pd.DataFrame,
    tol_pesos: float = 1.0,
    fecha_format_sis: str | None = None,
    fecha_format_arca: str | None = None,
    preferir_match_minimo: bool = True,
):
    def _pick_col(df, candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"No encontré: {candidates}. Disponibles: {list(df.columns)}")

    def _to_date(series, fmt):
        if fmt:
            dt = pd.to_datetime(series, errors="coerce", format=fmt)
            mask = dt.isna()
            if mask.any():
                dt.loc[mask] = pd.to_datetime(series.loc[mask], errors="coerce")
        else:
            dt = pd.to_datetime(series, errors="coerce")
        return dt.dt.date

    def _to_amount(series):
        return pd.to_numeric(series, errors="coerce").round(2)

    def _align_columns(base, add):
        add2 = add.copy()
        for c in base.columns:
            if c not in add2.columns:
                add2[c] = np.nan
        extras = [c for c in add2.columns if c not in base.columns]
        return add2[base.columns.tolist() + extras]

    def _resolver_1a1(candidatos_ok):
        usados_sis, usados_arca, seleccionados = set(), set(), []
        for _, row in candidatos_ok.iterrows():
            id_s, id_a = int(row["_id_sis"]), int(row["_id_arca"])
            if id_s not in usados_sis and id_a not in usados_arca:
                usados_sis.add(id_s); usados_arca.add(id_a)
                seleccionados.append(row)
        return pd.DataFrame(seleccionados).copy(), usados_sis, usados_arca

    arca = faltantes_sistema_dep.copy()
    sis  = faltantes_arca_dep.copy()
    arca.columns = arca.columns.astype(str).str.strip()
    sis.columns  = sis.columns.astype(str).str.strip()

    col_nro_arca  = _pick_col(arca, ["Nro_norm"])
    col_nro_sis   = _pick_col(sis,  ["Nro_norm"])
    col_cuit_arca = _pick_col(arca, ["CUIT"])
    col_cuit_sis  = _pick_col(sis,  ["CUIT"])
    col_fecha_arca = _pick_col(arca, ["Fecha de Emisión", "Fecha de EmisiÃ³n", "Fecha de Emision", "Fecha"])
    col_fecha_sis  = _pick_col(sis,  ["Fecha"])
    col_ng_arca    = _pick_col(arca, ["Imp. Neto Gravado Total", "Neto Gravado Total"])
    col_nng_arca   = _pick_col(arca, ["Imp. Neto No Gravado", "Neto No Gravado"])
    col_ng_sis     = _pick_col(sis,  ["Imp. Neto Gravado"])
    col_nng_sis    = _pick_col(sis,  ["Imp. Neto No Gravado"])

    for df_, col_nro, col_cuit, col_fecha, col_ng, col_nng, fmt in [
        (arca, col_nro_arca, col_cuit_arca, col_fecha_arca, col_ng_arca, col_nng_arca, fecha_format_arca),
        (sis,  col_nro_sis,  col_cuit_sis,  col_fecha_sis,  col_ng_sis,  col_nng_sis,  fecha_format_sis),
    ]:
        df_["_nro_key"]   = df_[col_nro].astype(str).str.strip()
        df_["_cuit_key"]  = df_[col_cuit].astype(str).str.strip().str.replace("-", "", regex=False).str.replace(" ", "", regex=False)
        df_["_fecha_key"] = _to_date(df_[col_fecha], fmt)
        df_["_ng_key"]    = _to_amount(df_[col_ng]).fillna(0.0)
        df_["_nng_key"]   = _to_amount(df_[col_nng]).fillna(0.0)

    arca["_id_arca"] = np.arange(len(arca), dtype=int)
    sis["_id_sis"]   = np.arange(len(sis),  dtype=int)

    # A) Match por Nro + Fecha
    cand_nro = pd.merge(sis, arca, on=["_nro_key", "_fecha_key"], how="inner",
                        suffixes=("_sis", "_arca"), validate="many_to_many")
    revisar_nro = pd.DataFrame()
    usados_sis_nro, usados_arca_nro = set(), set()

    if not cand_nro.empty:
        cand_nro["_diff_ng"]    = (cand_nro["_ng_key_sis"]  - cand_nro["_ng_key_arca"]).abs()
        cand_nro["_diff_nng"]   = (cand_nro["_nng_key_sis"] - cand_nro["_nng_key_arca"]).abs()
        cand_nro["_diff_total"] = cand_nro["_diff_ng"] + cand_nro["_diff_nng"]
        cand_nro_ok = cand_nro[(cand_nro["_diff_ng"] <= tol_pesos) & (cand_nro["_diff_nng"] <= tol_pesos)].copy()
        if not cand_nro_ok.empty and preferir_match_minimo:
            cand_nro_ok = cand_nro_ok.sort_values(["_nro_key", "_fecha_key", "_diff_total", "_id_sis", "_id_arca"])
        if not cand_nro_ok.empty:
            revisar_nro, usados_sis_nro, usados_arca_nro = _resolver_1a1(cand_nro_ok)
            if not revisar_nro.empty:
                revisar_nro["comentario"] = "CUIT"

    sis_rem  = sis[~sis["_id_sis"].isin(usados_sis_nro)].copy()
    arca_rem = arca[~arca["_id_arca"].isin(usados_arca_nro)].copy()

    # B) Match por CUIT + Fecha
    cand_cuit = pd.merge(sis_rem, arca_rem, on=["_cuit_key", "_fecha_key"], how="inner",
                         suffixes=("_sis", "_arca"), validate="many_to_many")
    revisar_cuit = pd.DataFrame()
    usados_sis_cuit, usados_arca_cuit = set(), set()

    if not cand_cuit.empty:
        cand_cuit["_diff_ng"]    = (cand_cuit["_ng_key_sis"]  - cand_cuit["_ng_key_arca"]).abs()
        cand_cuit["_diff_nng"]   = (cand_cuit["_nng_key_sis"] - cand_cuit["_nng_key_arca"]).abs()
        cand_cuit["_diff_total"] = cand_cuit["_diff_ng"] + cand_cuit["_diff_nng"]
        cand_cuit_ok = cand_cuit[(cand_cuit["_diff_ng"] <= tol_pesos) & (cand_cuit["_diff_nng"] <= tol_pesos)].copy()
        if not cand_cuit_ok.empty and preferir_match_minimo:
            cand_cuit_ok = cand_cuit_ok.sort_values(["_cuit_key", "_fecha_key", "_diff_total", "_id_sis", "_id_arca"])
        if not cand_cuit_ok.empty:
            revisar_cuit, usados_sis_cuit, usados_arca_cuit = _resolver_1a1(cand_cuit_ok)
            if not revisar_cuit.empty:
                revisar_cuit["comentario"] = "Nro Factura"

    faltante_arca_def      = sis_rem[~sis_rem["_id_sis"].isin(usados_sis_cuit)].copy()
    faltantes_sistema_def  = arca_rem[~arca_rem["_id_arca"].isin(usados_arca_cuit)].copy()

    for d in (faltante_arca_def, faltantes_sistema_def):
        d.drop(columns=[c for c in d.columns if c.startswith("_")], errors="ignore", inplace=True)

    nuevos = [df_ for df_ in [revisar_nro, revisar_cuit] if not df_.empty]
    if not nuevos:
        revisar_total = revisar.copy()
    else:
        revisar_2         = pd.concat(nuevos, ignore_index=True)
        revisar_2_aligned = _align_columns(revisar, revisar_2)
        revisar_total     = pd.concat([revisar, revisar_2_aligned], ignore_index=True)

    return revisar_total, faltante_arca_def, faltantes_sistema_def


# ─────────────────────────────────────────────
# EXPORTAR A BUFFER EN MEMORIA
# ─────────────────────────────────────────────

def generar_excel_en_memoria(
    revisar: pd.DataFrame,
    faltante_arca_def: pd.DataFrame,
    faltantes_sistema_def: pd.DataFrame,
):
    from io import BytesIO

    columnas_revisar = [
        "Nro_norm_sis", "CUIT_sis", "Razón Social", "Tipo Doc.", "Fecha",
        "Tipo Cambio", "Imp. Neto Gravado", "Imp. Neto No Gravado_sis",
        "Imp. Neto Gravado Total", "Imp. Neto No Gravado_arca",
        "IVA 21%_sis", "IVA 10,5%_sis", "IVA 27%_sis", "comentario",
    ]
    columnas_revisar = [c for c in columnas_revisar if c in revisar.columns]
    revisar_out = revisar[columnas_revisar].copy()

    revisar_out = revisar_out.rename(columns={
        "Nro_norm_sis":           "Nro Comprobante",
        "CUIT_sis":               "CUIT",
        "Imp. Neto Gravado":      "Gravado_sis",
        "Imp. Neto No Gravado_sis":  "No Gravado_sis",
        "Imp. Neto Gravado Total":   "Gravado_arca",
        "Imp. Neto No Gravado_arca": "No Gravado_arca",
        "IVA 21%_sis":   "IVA 21%",
        "IVA 10,5%_sis": "IVA 10,5%",
        "IVA 27%_sis":   "IVA 27%",
        "comentario":    "Comentario",
    })

    if "Nro Comprobante" in revisar_out.columns:
        revisar_out["Nro Comprobante"] = (
            revisar_out["Nro Comprobante"].astype(str).str.zfill(12).str.slice(0, 4)
            + "-"
            + revisar_out["Nro Comprobante"].astype(str).str.zfill(12).str.slice(4)
        )

    # Reporte principal (3 solapas)
    buf_reporte = BytesIO()
    with pd.ExcelWriter(buf_reporte, engine="openpyxl") as writer:
        revisar_out.to_excel(writer, sheet_name="revisar", index=False)
        faltante_arca_def.to_excel(writer, sheet_name="faltante_arca", index=False)
        faltantes_sistema_def.to_excel(writer, sheet_name="faltante_sistema", index=False)

    # Faltante sistema (archivo separado)
    buf_faltante = BytesIO()
    with pd.ExcelWriter(buf_faltante, engine="openpyxl") as writer:
        faltantes_sistema_def.to_excel(writer, sheet_name="faltante_sistema", index=False)

    return buf_reporte.getvalue(), buf_faltante.getvalue()


# ─────────────────────────────────────────────
# PIPELINE COMPLETO
# ─────────────────────────────────────────────

def correr_cruce(archivo_arca, archivo_sistema, tol_pesos: float = 1.0):
    df_arca    = load_excel_file(archivo_arca)
    df_sistema = load_excel_file(archivo_sistema)

    df_arca    = depurar_arca(df_arca)
    df_sistema = depurar_sistema(df_sistema)

    match, faltantes_sistema, faltantes_arca = cruzar_por_nro_y_cuit(df_sistema, df_arca)

    revisar = revisar_inconsistencias_en_match(match, tol_pesos=tol_pesos)

    faltantes_sistema_dep = depurar_faltantes_post_merge(faltantes_sistema, origen="arca")
    faltantes_arca_dep    = depurar_faltantes_post_merge(faltantes_arca,    origen="sis")

    revisar, faltante_arca_def, faltantes_sistema_def = \
        cruzar_faltantes_por_cuit_fecha_importes_append_revisar(
            revisar=revisar,
            faltantes_sistema_dep=faltantes_sistema_dep,
            faltantes_arca_dep=faltantes_arca_dep,
            tol_pesos=tol_pesos,
        )

    stats = {
        "match":             len(match),
        "revisar":           len(revisar),
        "faltante_arca":     len(faltante_arca_def),
        "faltante_sistema":  len(faltantes_sistema_def),
    }

    buf_reporte, buf_faltante = generar_excel_en_memoria(revisar, faltante_arca_def, faltantes_sistema_def)

    return buf_reporte, buf_faltante, stats
