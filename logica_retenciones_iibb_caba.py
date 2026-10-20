import re
from io import BytesIO

import pandas as pd
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.utils import get_column_letter

from logica_cruce_impuestos import (
    armar_resultados, cruzar_impuestos, cuit_por_tercero, revisar_faltantes, tabla_revisar_comprobante,
)
from logica_percepciones import load_excel_sistema, load_excel_arca, _parse_nro
from proveedores import PADRON_PROVEEDORES


# ─────────────────────────────────────────────
# DEPURACIÓN SISTEMA
# ─────────────────────────────────────────────

def depurar_sistema_retenciones(df: pd.DataFrame, padron: dict | None = None) -> pd.DataFrame:
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
      mayúsculas), y le asigna el primer CUIT encontrado para ese nombre.
    - Conserva 'Serie': define si la fila se cruza línea a línea (Serie
      cargada) o de forma sumarizada por proveedor (Serie vacía).
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
    c_serie = _resolver(["Serie"])

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

    if c_tercero != "Tercero":
        df = df.rename(columns={c_tercero: "Tercero"})
    if c_serie != "Serie":
        df = df.rename(columns={c_serie: "Serie"})

    return df


# ─────────────────────────────────────────────
# DEPURACIÓN AGIP
# ─────────────────────────────────────────────

def depurar_arca_retenciones(df: pd.DataFrame) -> pd.DataFrame:
    """
    Depura el reporte de retenciones de AGIP:
    - Normaliza 'CUIT' a texto.
    - Redondea 'Monto Retenido' a 2 decimales.
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
    c_monto = _resolver(["Monto Retenido"])
    c_comp = _resolver(["N° Comprobante", "N°Comprobante", "Nro Comprobante", "Numero Comprobante"])

    if pd.api.types.is_numeric_dtype(df[c_cuit]):
        df[c_cuit] = df[c_cuit].astype("Int64").astype(str)
    else:
        df[c_cuit] = df[c_cuit].astype(str).str.replace(r"[^0-9]", "", regex=True)
    if c_cuit != "CUIT":
        df = df.rename(columns={c_cuit: "CUIT"})

    df[c_monto] = pd.to_numeric(df[c_monto], errors="coerce").round(2)
    if c_monto != "Monto Retenido":
        df = df.rename(columns={c_monto: "Monto Retenido"})

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

def cruce_retenciones(
    df_arca: pd.DataFrame,
    df_sistema: pd.DataFrame,
    tolerancia_importe: float = 1.0,
    score_nombre_min: int = 70,
    padron: dict | None = None,
):
    """
    Cruza retenciones de AGIP contra el sistema con el criterio común de los
    módulos de impuestos (ver logica_cruce_impuestos.cruzar_impuestos). A
    diferencia de percepciones, las retenciones no siempre tienen
    comprobante asociado: las filas del sistema con 'Serie' vacía solo se
    cruzan de forma sumarizada. Entre candidatos empatados se queda con el
    mismo comprobante y después con la fecha más cercana. El mismo
    comprobante con importe distinto va a df_revisar con 'Comentario' =
    'Importe'.

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

    col_serie = _buscar_columna(df_sistema, "SERIE")
    if col_serie is None:
        raise ValueError("No se encontró la columna 'Serie' en df_sistema.")

    col_fecha_arca = _buscar_columna(df_arca, "FECHA RETENCION")
    col_fecha_sistema = _buscar_columna(df_sistema, "FECHA")

    cols = [
        _buscar_columna(df_arca, "PTO VENTA"), _buscar_columna(df_arca, "N°COMPROBANTE"),
        _buscar_columna(df_sistema, "PTO VENTA"), _buscar_columna(df_sistema, "N°COMPROBANTE"),
    ]

    matches, revisar = cruzar_impuestos(
        df_arca, df_sistema,
        col_cuit_arca=_buscar_columna(df_arca, "CUIT"),
        col_importe_arca="Monto Retenido",
        col_nombre_arca="Razon Social",
        col_cuit_sistema=_buscar_columna(df_sistema, "CUIT"),
        col_importe_sistema="Importe",
        col_nombre_sistema="Tercero",
        padron=padron,
        tolerancia_importe=tolerancia_importe,
        score_nombre_min=score_nombre_min,
        cols_comprobante=tuple(cols) if all(cols) else None,
        cols_fecha=(col_fecha_arca, col_fecha_sistema) if col_fecha_arca and col_fecha_sistema else None,
        solo_agrupado=set(df_sistema.index[df_sistema[col_serie].isna()]),
        revisar_comprobante=True,
    )

    resultados = armar_resultados(
        df_arca, df_sistema, matches,
        excluidos_arca={a for a, _, _ in revisar},
        excluidos_sistema={s for _, s, _ in revisar},
    )
    return (*resultados, tabla_revisar_comprobante(df_arca, df_sistema, revisar, "AGIP"))


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

def generar_excel_retenciones(
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
    IMPORTE_COLS_REVISAR = {"Importe Sistema", "Monto AGIP", "Diferencia", "Importe", "Monto Retenido"}

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
            if str(cell.value).strip() in DATE_COLS:
                for row in range(2, ws.max_row + 1):
                    ws.cell(row=row, column=col_idx).number_format = DATE_FORMAT
            elif str(cell.value).strip() in IMPORTE_COLS_REVISAR:
                for row in range(2, ws.max_row + 1):
                    ws.cell(row=row, column=col_idx).number_format = IMPORTE_FORMAT

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

def correr_cruce_retenciones(
    archivo_arca, archivo_sistema, tolerancia_importe: float = 1.0, score_nombre_min: int = 70
):
    df_sistema = load_excel_sistema(archivo_sistema)
    df_arca = load_excel_arca(archivo_arca)

    df_sistema_dep = depurar_sistema_retenciones(df_sistema)
    df_arca_dep = depurar_arca_retenciones(df_arca)

    df_match_sistema, df_match_arca, df_falta_sistema, df_falta_arca, df_revisar = cruce_retenciones(
        df_arca_dep, df_sistema_dep, tolerancia_importe=tolerancia_importe, score_nombre_min=score_nombre_min
    )

    df_falta_sistema, df_falta_arca, hojas_revisar, stats_revisar = revisar_faltantes(
        df_falta_sistema, df_falta_arca,
        col_cuit_arca="CUIT", col_importe_arca="Monto Retenido",
        col_fecha_arca=next(
            (c for c in df_falta_sistema.columns if str(c).replace(".", "").strip().upper() == "FECHA RETENCION"),
            None,
        ),
        col_nombre_arca="Razon Social",
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

    buf_reporte = generar_excel_retenciones(
        df_match_sistema, df_match_arca, df_falta_sistema, df_falta_arca, df_proveedores_nuevos, hojas_revisar,
    )

    return buf_reporte, stats
