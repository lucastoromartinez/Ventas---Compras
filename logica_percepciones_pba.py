import re
from io import BytesIO

import pandas as pd
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.utils import get_column_letter

from logica_cruce_impuestos import (
    armar_resultados, cruzar_impuestos, cuit_por_tercero, revisar_faltantes, tabla_revisar_comprobante,
)
from logica_percepciones import load_excel_sistema
from proveedores import PADRON_PROVEEDORES


# ─────────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────────

_PATRON_TXT_ARCA = re.compile(
    r'^(?P<cuit>\d{2}-\d+-\d)'          # CUIT: nn-nnnnnnnn-n
    r'(?P<fecha>\d{2}/\d{2}/\d{4})'      # Fecha: dd/mm/aaaa
    r'(?P<tipo>.{2})'                    # Tipo comprobante: FA, CA, F (espacio), etc.
    r'(?P<comprobante>\d{13})'           # Nro de comprobante (13 dígitos, con ceros)
    r'(?P<importe>-?\d+,\d{2})$'         # Importe: puede tener signo negativo
)


def cargar_txt_arca(file) -> pd.DataFrame:
    """
    Parsea el archivo de texto de percepciones IIBB PBA de ARCA, con formato:
    CUIT(13) + FECHA(10, dd/mm/aaaa) + TIPO(2) + COMPROBANTE(13) + IMPORTE(coma decimal).
    """
    file.seek(0)
    contenido = file.read()
    if isinstance(contenido, bytes):
        contenido = contenido.decode("utf-8")

    rows = []
    for line in contenido.splitlines():
        line = line.rstrip()
        if not line.strip():
            continue
        m = _PATRON_TXT_ARCA.match(line)
        if m:
            rows.append(m.groupdict())

    if not rows:
        raise ValueError("No se pudo interpretar ninguna línea del archivo de ARCA (formato inesperado).")

    df = pd.DataFrame(rows)

    df["fecha"] = pd.to_datetime(df["fecha"], format="%d/%m/%Y")
    df["tipo"] = df["tipo"].str.strip()
    df["importe"] = df["importe"].str.replace(",", ".", regex=False).astype(float)
    df["cuit"] = df["cuit"].str.replace("-", "", regex=False)
    # Comprobante de 13 dígitos: punto de venta (5) + número (8)
    df["pto_venta"] = df["comprobante"].str[:-8].str.lstrip("0").replace("", "0")
    df["n_comprobante"] = df["comprobante"].str[-8:].str.lstrip("0").replace("", "0")

    return df[["cuit", "fecha", "tipo", "pto_venta", "n_comprobante", "importe"]]


# ─────────────────────────────────────────────
# DEPURACIÓN
# ─────────────────────────────────────────────

def depurar_arca_pba(df_arca: pd.DataFrame, padron: dict | None = None) -> pd.DataFrame:
    """Agrega la columna 'nombre' buscando cada CUIT en el padrón de proveedores del repo."""
    if padron is None:
        padron = PADRON_PROVEEDORES

    df = df_arca.copy()
    df["nombre"] = df["cuit"].map(padron)
    return df


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


def depurar_sistema_pba(df: pd.DataFrame, padron: dict | None = None) -> pd.DataFrame:
    """
    Depura el reporte del sistema:
    - Descarta la fila 'Saldo anterior' del mayor (no es un movimiento).
    - Calcula 'Importe' = Debe - Haber.
    - A partir de 'Su Factura' genera 'Pto. Venta' y 'N°Comprobante'.
    - Si el archivo ya trae una columna CUIT (sin importar mayúsculas o
      minúsculas), la depura como siempre (sin guiones ni espacios) y la usa
      directamente, sin pasar por el padrón. Si no viene, genera 'CUIT'
      buscando, para cada 'Tercero', qué nombre del padrón de proveedores
      del repo coincide (comparación exacta sin espacios ni mayúsculas), y
      le asigna el primer CUIT encontrado para ese nombre.
    """
    if padron is None:
        padron = PADRON_PROVEEDORES

    df = df.copy()

    def _normalizar(nombre):
        return re.sub(r"\s+", "", str(nombre)).strip().upper()

    def _buscar_columna(df, objetivo):
        return next((c for c in df.columns if _normalizar(c) == _normalizar(objetivo)), None)

    col_debe = _buscar_columna(df, "Debe")
    col_haber = _buscar_columna(df, "Haber")
    col_su_factura = _buscar_columna(df, "Su Factura")
    col_tercero = _buscar_columna(df, "Tercero")
    col_comentario = _buscar_columna(df, "Comentario")

    if col_comentario is not None:
        es_saldo_anterior = (
            df[col_comentario].astype(str).str.strip().str.lower().str.startswith("saldo anterior")
        )
        df = df.loc[~es_saldo_anterior].reset_index(drop=True)

    columnas_encontradas = {
        "Debe": col_debe, "Haber": col_haber,
        "Su Factura": col_su_factura, "Tercero": col_tercero,
    }
    faltantes = {nombre for nombre, col in columnas_encontradas.items() if col is None}
    if faltantes:
        raise ValueError(f"Faltan las siguientes columnas en el sistema: {faltantes}")

    df[col_debe] = pd.to_numeric(df[col_debe], errors="coerce").fillna(0).round(2)
    df[col_haber] = pd.to_numeric(df[col_haber], errors="coerce").fillna(0).round(2)
    df["Importe"] = (df[col_debe] - df[col_haber]).round(2)

    df[["Pto. Venta", "N°Comprobante"]] = pd.DataFrame(
        df[col_su_factura].apply(_parse_nro).tolist(), index=df.index
    )

    col_cuit = _buscar_columna(df, "CUIT")
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
        df["CUIT"] = cuit_por_tercero(df[col_tercero], padron)

    return df


# ─────────────────────────────────────────────
# CRUCE
# ─────────────────────────────────────────────

def cruce_percepciones_pba(
    df_arca: pd.DataFrame,
    df_sistema: pd.DataFrame,
    tolerancia_importe: float = 1.0,
    score_nombre_min: int = 70,
    padron: dict | None = None,
):
    """
    Cruza percepciones de ARCA (PBA) contra el sistema con el criterio común
    de los módulos de impuestos (ver logica_cruce_impuestos.cruzar_impuestos).
    Si hay más de un candidato, desempata por Pto. Venta / N°Comprobante, y
    el mismo comprobante con importe distinto va a df_revisar con
    'Comentario' = 'Importe'.

    Retorna:
        tuple de 5 pd.DataFrame:
            - df_match_sistema, df_match_arca (ligados por 'id_match', con
              'match_tipo' y 'match_score')
            - df_falta_sistema (líneas de ARCA sin match)
            - df_falta_arca (líneas de sistema sin match)
            - df_revisar (pares ARCA / sistema del mismo comprobante con
              importe distinto)
    """
    if padron is None:
        padron = PADRON_PROVEEDORES

    df_arca = df_arca.reset_index(drop=True).copy()
    df_sistema = df_sistema.reset_index(drop=True).copy()

    def _normalizar(nombre):
        return re.sub(r"\s+", "", str(nombre)).strip().upper()

    def _buscar_columna(df, *nombres):
        objetivo = {_normalizar(n) for n in nombres}
        return next((c for c in df.columns if _normalizar(c) in objetivo), None)

    col_cuit_arca = _buscar_columna(df_arca, "CUIT")
    col_importe_arca = _buscar_columna(df_arca, "Importe", "Monto Percibido", "Monto")
    col_importe_sistema = _buscar_columna(df_sistema, "Importe", "Monto")

    if col_cuit_arca is None or col_importe_arca is None:
        raise ValueError("df_arca necesita columnas de CUIT e Importe.")
    if col_importe_sistema is None:
        raise ValueError("df_sistema necesita una columna de Importe.")

    cols = [
        _buscar_columna(df_arca, "pto_venta", "Pto. Venta"),
        _buscar_columna(df_arca, "n_comprobante", "N°Comprobante"),
        _buscar_columna(df_sistema, "Pto. Venta", "pto_venta"),
        _buscar_columna(df_sistema, "N°Comprobante", "n_comprobante"),
    ]

    matches, revisar = cruzar_impuestos(
        df_arca, df_sistema,
        col_cuit_arca=col_cuit_arca,
        col_importe_arca=col_importe_arca,
        col_nombre_arca=_buscar_columna(df_arca, "Nombre", "Razon Social"),
        col_cuit_sistema=_buscar_columna(df_sistema, "CUIT"),
        col_importe_sistema=col_importe_sistema,
        col_nombre_sistema=_buscar_columna(df_sistema, "Tercero", "Nombre", "Razon Social", "Proveedor"),
        padron=padron,
        tolerancia_importe=tolerancia_importe,
        score_nombre_min=score_nombre_min,
        cols_comprobante=tuple(cols) if all(cols) else None,
        revisar_comprobante=True,
    )

    resultados = armar_resultados(
        df_arca, df_sistema, matches,
        excluidos_arca={a for a, _, _ in revisar},
        excluidos_sistema={s for _, s, _ in revisar},
    )
    return (*resultados, tabla_revisar_comprobante(df_arca, df_sistema, revisar, "ARCA"))


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
        cuit = fila["cuit"]
        if cuit in padron or cuit in vistos:
            continue
        vistos.add(cuit)
        nuevos.append({"CUIT": cuit, "Nombre": fila["nombre"], "score": fila["match_score"]})

    return pd.DataFrame(nuevos, columns=["CUIT", "Nombre", "score"])


# ─────────────────────────────────────────────
# EXPORTAR A BUFFER EN MEMORIA (descargable único)
# ─────────────────────────────────────────────

def generar_excel_percepciones_pba(
    df_match_sistema: pd.DataFrame,
    df_match_arca: pd.DataFrame,
    df_falta_sistema: pd.DataFrame,
    df_falta_arca: pd.DataFrame,
    df_proveedores_nuevos: pd.DataFrame | None = None,
    hojas_revisar: dict | None = None,
) -> bytes:
    DATE_FORMAT = "DD/MM/YYYY"
    DATE_COLS = {"fecha", "Fecha", "Fecha_Sistema", "Fecha_arca"}

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

        for col in ws.columns:
            max_len = max(
                (len(str(c.value)) if c.value is not None else 0 for c in col),
                default=10,
            )
            ws.column_dimensions[get_column_letter(col[0].column)].width = min(max_len + 4, 45)

    hojas = {
        "Match_Sistema": df_match_sistema,
        "Match_Arca": df_match_arca,
        "Falta_Sistema": df_falta_sistema,
        "Falta_Arca": df_falta_arca,
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

def correr_cruce_percepciones_pba(
    archivo_arca_txt,
    archivo_sistema,
    tolerancia_importe: float = 1.0,
    score_nombre_min: int = 70,
):
    df_arca = cargar_txt_arca(archivo_arca_txt)
    df_arca_dep = depurar_arca_pba(df_arca)

    df_sistema = load_excel_sistema(archivo_sistema)
    df_sistema_dep = depurar_sistema_pba(df_sistema)

    df_match_sistema, df_match_arca, df_falta_sistema, df_falta_arca, df_revisar = cruce_percepciones_pba(
        df_arca_dep, df_sistema_dep,
        tolerancia_importe=tolerancia_importe,
        score_nombre_min=score_nombre_min,
    )

    df_falta_sistema, df_falta_arca, hojas_revisar, stats_revisar = revisar_faltantes(
        df_falta_sistema, df_falta_arca,
        col_cuit_arca="cuit", col_importe_arca="importe", col_fecha_arca="fecha", col_nombre_arca="nombre",
        etiqueta="ARCA", df_revisar_comprobante=df_revisar,
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

    buf_reporte = generar_excel_percepciones_pba(
        df_match_sistema, df_match_arca, df_falta_sistema, df_falta_arca, df_proveedores_nuevos, hojas_revisar,
    )

    return buf_reporte, stats
