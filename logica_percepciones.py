import re
from io import BytesIO

import pandas as pd
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.utils import get_column_letter

from logica_cruce_impuestos import (
    armar_resultados, cruzar_impuestos, cuit_por_tercero, revisar_faltantes, tabla_revisar_comprobante,
)
from proveedores import PADRON_PROVEEDORES


# ─────────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────────

def load_excel_sistema(file) -> pd.DataFrame:
    file.seek(0)
    return pd.read_excel(file)


def load_excel_arca(file, filas_a_revisar: int = 5) -> pd.DataFrame:
    """
    Lee el Excel de percepciones de AGIP detectando automáticamente si los
    encabezados arrancan en la primera o en la segunda columna.
    """
    file.seek(0)
    raw = pd.read_excel(file, header=None, nrows=filas_a_revisar)

    fila_header = None
    for i in range(len(raw)):
        if raw.iloc[i].notna().sum() > 0:
            fila_header = i
            break

    if fila_header is None:
        raise ValueError("No se pudo detectar una fila de encabezados válida en el Excel de AGIP.")

    primera_col_vacia = pd.isna(raw.iloc[fila_header].iloc[0])

    file.seek(0)
    if primera_col_vacia:
        df = pd.read_excel(file, header=fila_header, usecols=lambda c: c != raw.columns[0])
    else:
        df = pd.read_excel(file, header=fila_header)

    df = df.loc[:, ~df.columns.astype(str).str.contains('^Unnamed', na=False)]
    df = df.dropna(axis=1, how='all')

    return df


# ─────────────────────────────────────────────
# DEPURACIÓN SISTEMA
# ─────────────────────────────────────────────

def _parse_nro(s: str) -> tuple[str, str]:
    s = str(s).strip()
    s = s.replace('−', '-').replace('–', '-')  # em dash / en dash
    s = re.sub(r'\s*-\s*', '-', s)
    count = s.count('-')
    if count == 0:
        left, right = s[:4], s[4:]
    elif count == 1:
        left, right = s.split('-', 1)
    else:
        idx = s.index('-')
        left = s[:idx]
        right = s[idx + 1:].replace('-', '')
    pto = left.lstrip('0')
    comp = right.lstrip('0')
    if pto == '' and len(right) >= 3 and right[0] != '0' and right[1:3] == '00':
        pto = right[0]
        comp = right[1:].lstrip('0')
    return pto, comp


def depurar_sistema_percepciones(df: pd.DataFrame, padron: dict | None = None) -> pd.DataFrame:
    """
    Depura el reporte del sistema:
    - Descarta la fila 'Saldo anterior' del mayor (no es un movimiento).
    - Calcula 'Importe' = Debe - Haber.
    - A partir de 'Su Factura' genera 'Pto. Venta' y 'N°Comprobante'.
    - Si el archivo ya trae una columna CUIT (sin importar mayúsculas o
      minúsculas), la depura como siempre (sin guiones ni espacios) y la usa
      directamente, sin pasar por el padrón. Si no viene, genera 'CUIT'
      buscando, para cada 'Tercero', qué nombre del padrón de proveedores
      del repo (proveedores.py) coincide (comparación exacta sin espacios ni
      mayúsculas), y le asigna el primer CUIT encontrado para ese nombre (si
      el mismo nombre tiene más de un CUIT cargado en el padrón, se queda
      con el primero).
    """
    if padron is None:
        padron = PADRON_PROVEEDORES

    df = df.copy()
    df.columns = df.columns.astype(str).str.strip()

    norm_cols = {c.lower(): c for c in df.columns}

    def _resolver(candidatos: list[str]) -> str:
        for cand in candidatos:
            k = cand.lower()
            if k in norm_cols:
                return norm_cols[k]
        raise KeyError(f"No se encontró ninguna columna de {candidatos}. Disponibles: {list(df.columns)}")

    c_comentario = norm_cols.get("comentario")
    if c_comentario is not None:
        es_saldo_anterior = (
            df[c_comentario].astype(str).str.strip().str.lower().str.startswith("saldo anterior")
        )
        df = df.loc[~es_saldo_anterior].reset_index(drop=True)

    c_debe = _resolver(["Debe"])
    c_haber = _resolver(["Haber"])
    c_factura = _resolver(["Su Factura", "Su factura"])
    c_tercero = _resolver(["Tercero"])

    df[c_debe] = pd.to_numeric(df[c_debe], errors="coerce").fillna(0).round(2)
    df[c_haber] = pd.to_numeric(df[c_haber], errors="coerce").fillna(0).round(2)
    df["Importe"] = (df[c_debe] - df[c_haber]).round(2)

    df[["Pto. Venta", "N°Comprobante"]] = pd.DataFrame(
        df[c_factura].apply(_parse_nro).tolist(), index=df.index
    )

    col_cuit = next(
        (c for c in df.columns if str(c).replace(".", "").strip().upper() == "CUIT"),
        None
    )
    if col_cuit is not None:
        if pd.api.types.is_numeric_dtype(df[col_cuit]):
            df[col_cuit] = df[col_cuit].astype("Int64").astype(str)
        else:
            df[col_cuit] = df[col_cuit].astype(str)
        df[col_cuit] = (
            df[col_cuit]
            .str.replace("-", "", regex=False)
            .str.replace(" ", "", regex=False)
        )
        if col_cuit != "CUIT":
            df = df.rename(columns={col_cuit: "CUIT"})
    else:
        df["CUIT"] = cuit_por_tercero(df[c_tercero], padron)

    return df


# ─────────────────────────────────────────────
# DEPURACIÓN AGIP
# ─────────────────────────────────────────────

def depurar_arca_percepciones(df: pd.DataFrame) -> pd.DataFrame:
    """
    Depura el reporte de percepciones de AGIP:
    - Normaliza 'CUIT' a texto.
    - Redondea 'Monto Percibido' a 2 decimales.
    - Separa 'N° Comprobante' en 'Pto. Venta' y 'N°Comprobante' (8 dígitos).
    """
    df = df.copy()
    df.columns = df.columns.astype(str).str.strip()

    norm_cols = {c.lower(): c for c in df.columns}

    def _resolver(candidatos: list[str]) -> str:
        for cand in candidatos:
            k = cand.lower()
            if k in norm_cols:
                return norm_cols[k]
        raise KeyError(f"No se encontró ninguna columna de {candidatos}. Disponibles: {list(df.columns)}")

    c_cuit = _resolver(["CUIT"])
    c_monto = _resolver(["Monto Percibido"])
    c_comp = _resolver(["N° Comprobante", "N°Comprobante", "Nro Comprobante", "Numero Comprobante"])

    if pd.api.types.is_numeric_dtype(df[c_cuit]):
        df[c_cuit] = df[c_cuit].astype("Int64").astype(str)
    else:
        df[c_cuit] = df[c_cuit].astype(str).str.replace(r"[^0-9]", "", regex=True)
    if c_cuit != "CUIT":
        df = df.rename(columns={c_cuit: "CUIT"})

    df[c_monto] = pd.to_numeric(df[c_monto], errors="coerce").round(2)
    if c_monto != "Monto Percibido":
        df = df.rename(columns={c_monto: "Monto Percibido"})

    def _separar_comprobante(valor):
        if pd.isna(valor):
            return pd.Series([pd.NA, pd.NA])

        s = str(valor).strip()
        if s.endswith(".0"):
            s = s[:-2]
        if s[:3] == "A0 ":
            s = s[3:]
        s = re.sub(r"\D", "", s)

        if s == "":
            return pd.Series([pd.NA, pd.NA])

        if len(s) < 9:
            comprobante = s.zfill(8)
            pto_venta = pd.NA
        else:
            comprobante = s[-8:]
            pto_venta = s[:-8]

        return pd.Series([pto_venta, comprobante])

    df[["Pto. Venta", "N°Comprobante"]] = df[c_comp].apply(_separar_comprobante)
    df = df.drop(columns=[c_comp])

    return df


# ─────────────────────────────────────────────
# CRUCE
# ─────────────────────────────────────────────

def cruce_percepciones(
    df_arca: pd.DataFrame,
    df_sistema: pd.DataFrame,
    tolerancia_importe: float = 1.0,
    score_nombre_min: int = 70,
    padron: dict | None = None,
):
    """
    Cruza percepciones de AGIP contra el sistema con el criterio común de
    los módulos de impuestos (ver logica_cruce_impuestos.cruzar_impuestos).
    Desempata por Pto. Venta / N°Comprobante, y el mismo comprobante con
    importe distinto va a df_revisar con 'Comentario' = 'Importe'.

    Retorna:
        tuple de 5 pd.DataFrame:
            - df_match_sistema, df_match_arca (ligados por 'id_match', con
              'match_tipo' y 'match_score')
            - df_falta_sistema (líneas de AGIP sin match)
            - df_falta_arca (líneas de sistema sin match)
            - df_revisar (pares AGIP / sistema del mismo comprobante con
              importe distinto)
    """
    if padron is None:
        padron = PADRON_PROVEEDORES

    df_arca = df_arca.reset_index(drop=True).copy()
    df_sistema = df_sistema.reset_index(drop=True).copy()

    def _buscar_columna(df, nombre):
        return next(
            (c for c in df.columns if str(c).replace(".", "").strip().upper() == nombre),
            None
        )

    cols = [
        _buscar_columna(df_arca, "PTO VENTA"), _buscar_columna(df_arca, "N°COMPROBANTE"),
        _buscar_columna(df_sistema, "PTO VENTA"), _buscar_columna(df_sistema, "N°COMPROBANTE"),
    ]

    matches, revisar = cruzar_impuestos(
        df_arca, df_sistema,
        col_cuit_arca=_buscar_columna(df_arca, "CUIT"),
        col_importe_arca="Monto Percibido",
        col_nombre_arca="Razon Social",
        col_cuit_sistema=_buscar_columna(df_sistema, "CUIT"),
        col_importe_sistema="Importe",
        col_nombre_sistema="Tercero",
        padron=padron,
        tolerancia_importe=tolerancia_importe,
        score_nombre_min=score_nombre_min,
        cols_comprobante=tuple(cols) if all(cols) else None,
        revisar_comprobante=True,
    )

    df_match_sistema, df_match_arca, df_falta_sistema, df_falta_arca = armar_resultados(
        df_arca, df_sistema, matches,
        excluidos_arca={a for a, _, _ in revisar},
        excluidos_sistema={s for _, s, _ in revisar},
    )

    df_revisar = tabla_revisar_comprobante(df_arca, df_sistema, revisar, "AGIP")

    return df_match_sistema, df_match_arca, df_falta_sistema, df_falta_arca, df_revisar


# ─────────────────────────────────────────────
# PROVEEDORES NUEVOS (matchearon por nombre, no están en el padrón)
# ─────────────────────────────────────────────

def detectar_proveedores_nuevos(
    df_match_arca: pd.DataFrame,
    padron: dict | None = None,
    umbral_nombre: int = 80,
) -> pd.DataFrame:
    """
    Devuelve los CUIT que matchearon por nombre (no por CUIT) con score >=
    umbral_nombre y que todavía no están en el padrón de proveedores del repo.
    """
    if padron is None:
        padron = PADRON_PROVEEDORES

    candidatos = df_match_arca[
        df_match_arca["match_tipo"].astype(str).str.startswith("nombre")
        & (df_match_arca["match_score"] >= umbral_nombre)
    ]

    nuevos = []
    vistos = set()
    for _, fila in candidatos.iterrows():
        cuit = fila["CUIT"]
        if cuit in padron or cuit in vistos:
            continue
        vistos.add(cuit)
        nuevos.append({"CUIT": cuit, "Nombre": fila["Razon Social"], "score": fila["match_score"]})

    return pd.DataFrame(nuevos, columns=["CUIT", "Nombre", "score"])


# ─────────────────────────────────────────────
# EXPORTAR A BUFFER EN MEMORIA (descargable único)
# ─────────────────────────────────────────────

def generar_excel_percepciones(
    df_match_sistema: pd.DataFrame,
    df_match_arca: pd.DataFrame,
    df_falta_sistema: pd.DataFrame,
    df_falta_arca: pd.DataFrame,
    df_proveedores_nuevos: pd.DataFrame | None = None,
    hojas_revisar: dict | None = None,
) -> bytes:
    DATE_FORMAT = "DD/MM/YYYY"
    DATE_COLS = {"Fecha", "Fecha factura", "Fecha Percepcion", "Fecha Comprobante"}
    IMPORTE_FORMAT = "#,##0.00"
    IMPORTE_COLS = {
        "Debe", "Haber", "Saldo", "Importe", "Monto Percibido", "Base Calculo",
        "Importe Sistema", "Monto AGIP", "Diferencia",
    }

    def _nombre_base(header) -> str:
        h = str(header).strip()
        for sufijo in (" AGIP", " Sistema"):
            if h.endswith(sufijo):
                return h[: -len(sufijo)]
        return h

    def _style_sheet(ws) -> None:
        thin = Side(style="thin")
        header_fill = PatternFill(start_color="C0C0C0", end_color="C0C0C0", fill_type="solid")
        header_font = Font(bold=True)
        header_border = Border(left=thin, right=thin, top=thin, bottom=thin)

        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.border = header_border
            cell.alignment = Alignment(horizontal="center", vertical="center")

        for col_idx, cell in enumerate(ws[1], start=1):
            base = _nombre_base(cell.value)
            if base in DATE_COLS:
                formato = DATE_FORMAT
            elif base in IMPORTE_COLS:
                formato = IMPORTE_FORMAT
            else:
                continue
            for row in range(2, ws.max_row + 1):
                ws.cell(row=row, column=col_idx).number_format = formato

        for col in ws.columns:
            max_len = max(
                (len(str(c.value)) if c.value is not None else 0 for c in col),
                default=10,
            )
            ws.column_dimensions[get_column_letter(col[0].column)].width = min(max_len + 4, 45)

    hojas = {
        "Match_Sistema": df_match_sistema,
        "Match_AGIP": df_match_arca,
        "Falta_Sistema": df_falta_sistema,
        "Falta_AGIP": df_falta_arca,
    }
    hojas.update(hojas_revisar or {})
    if df_proveedores_nuevos is not None and not df_proveedores_nuevos.empty:
        hojas["Proveedores_Nuevos"] = df_proveedores_nuevos

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for nombre_hoja, df in hojas.items():
            df.to_excel(writer, sheet_name=nombre_hoja, index=False)

        wb = writer.book
        for nombre_hoja in hojas:
            _style_sheet(wb[nombre_hoja])

    return buf.getvalue()


# ─────────────────────────────────────────────
# PIPELINE COMPLETO
# ─────────────────────────────────────────────

def correr_cruce_percepciones(
    archivo_arca, archivo_sistema, tolerancia_importe: float = 1.0, score_nombre_min: int = 70
):
    df_sistema = load_excel_sistema(archivo_sistema)
    df_arca = load_excel_arca(archivo_arca)

    df_sistema_dep = depurar_sistema_percepciones(df_sistema)
    df_arca_dep = depurar_arca_percepciones(df_arca)

    df_match_sistema, df_match_arca, df_falta_sistema, df_falta_arca, df_revisar = cruce_percepciones(
        df_arca_dep, df_sistema_dep, tolerancia_importe=tolerancia_importe, score_nombre_min=score_nombre_min
    )

    df_falta_sistema, df_falta_arca, hojas_revisar, stats_revisar = revisar_faltantes(
        df_falta_sistema, df_falta_arca,
        col_cuit_arca="CUIT", col_importe_arca="Monto Percibido",
        col_fecha_arca="Fecha Percepcion", col_nombre_arca="Razon Social",
        etiqueta="AGIP", df_revisar_comprobante=df_revisar,
        score_nombre_min=score_nombre_min,
    )

    df_proveedores_nuevos = detectar_proveedores_nuevos(df_match_arca, umbral_nombre=max(score_nombre_min, 80))

    stats = {
        "match": len(df_match_arca),
        "faltante_sistema": len(df_falta_sistema),
        "faltante_arca": len(df_falta_arca),
        "proveedores_nuevos": len(df_proveedores_nuevos),
        **stats_revisar,
    }

    buf_reporte = generar_excel_percepciones(
        df_match_sistema, df_match_arca, df_falta_sistema, df_falta_arca, df_proveedores_nuevos, hojas_revisar,
    )

    return buf_reporte, stats
