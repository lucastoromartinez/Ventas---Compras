"""
Lógica de lectura de liquidaciones Payway desde PDFs
"""
import re
import io
from io import BytesIO
from typing import List

import pandas as pd


# ─────────────────────────────────────────────
# EXTRACCIÓN BASE
# ─────────────────────────────────────────────

def extraer_datos_base_pdf(pdf_bytes: bytes, nombre_archivo: str) -> dict:
    import fitz

    datos = {
        "ID": "",
        "Total Neto": 0.0,
        "Total Presentado": 0.0,
        "Total Descuentos": 0.0
    }

    def a_float(valor: str) -> float:
        return float(valor.replace(".", "").replace(",", ".").strip())

    def es_importe(linea: str) -> bool:
        return bool(re.fullmatch(r"\d{1,3}(?:\.\d{3})*,\d{2}", linea.strip()))

    doc   = fitz.open(stream=pdf_bytes, filetype="pdf")
    texto = ""
    for pagina in doc:
        texto += pagina.get_text("text") + "\n"

    lineas      = [l.strip() for l in texto.splitlines() if l.strip()]
    texto_lower = texto.lower()
    nombre_lower = nombre_archivo.lower()

    # Tarjeta
    if "amex" in nombre_lower or "american" in texto_lower:
        tarjeta = "american"
    elif "master" in nombre_lower or "master" in texto_lower:
        tarjeta = "master"
    elif "visa" in nombre_lower or "visa" in texto_lower:
        tarjeta = "visa"
    elif "cabal" in nombre_lower or "cabal" in texto_lower:
        tarjeta = "cabal"
    else:
        tarjeta = ""

    # Número de resumen
    nro_resumen = ""
    patrones_resumen = [
        r"n[°ºo]?\s*de\s*resumen\s*:?\s*(\d+)",
        r"resumen\s*:?\s*(\d+)"
    ]
    for patron in patrones_resumen:
        m = re.search(patron, texto, re.IGNORECASE)
        if m:
            nro_resumen = m.group(1)
            break

    if not nro_resumen:
        for i, linea in enumerate(lineas):
            linea_norm = linea.lower().replace("º", "°")
            if "resumen" in linea_norm:
                m_linea = re.search(r"(\d+)", linea)
                if m_linea:
                    nro_resumen = m_linea.group(1)
                    break
                for j in range(i + 1, min(i + 6, len(lineas))):
                    m_sig = re.search(r"(\d+)", lineas[j])
                    if m_sig:
                        nro_resumen = m_sig.group(1)
                        break
                if nro_resumen:
                    break

    if not nro_resumen:
        m_arch = re.search(r"(?:amex|american|masterd?|visa|cabal)\s+(\d+)", nombre_lower)
        if m_arch:
            nro_resumen = m_arch.group(1)

    if nro_resumen:
        nro_resumen = str(int(nro_resumen))

    datos["ID"] = f"{tarjeta}{nro_resumen}"

    # Totales
    idx_est = None
    for i, linea in enumerate(lineas):
        if linea.lower() == "establecimiento":
            idx_est = i
            break

    if idx_est is not None:
        importes = []
        for linea in lineas[idx_est + 1:]:
            if es_importe(linea):
                importes.append(linea)
                if len(importes) == 6:
                    break
        if len(importes) >= 3:
            datos["Total Presentado"] = a_float(importes[0])
            datos["Total Descuentos"] = a_float(importes[1])
            datos["Total Neto"]       = a_float(importes[2])

    return datos


# ─────────────────────────────────────────────
# EXTRACCIÓN DETALLE DESCUENTOS
# ─────────────────────────────────────────────

def extraer_detalle_descuentos_pdf(pdf_bytes: bytes) -> dict:
    import fitz

    datos = {
        "IVA 10,5%": 0.0,
        "IVA 21%": 0.0,
        "Retenciones Totales": 0.0,
        "Percepciones Totales": 0.0
    }

    def convertir_numero(valor: str) -> float:
        return float(valor.replace(".", "").replace(",", ".").strip())

    def extraer_ultimo_importe_monetario(linea: str):
        matches = re.findall(r"(?<!\d)(\d{1,3}(?:\.\d{3})*,\d{2})(?!\s*%)", linea)
        return matches[-1] if matches else None

    def limpiar_nombre_concepto(linea: str, prefijo: str) -> str:
        linea = re.sub(r"\s+", " ", linea).strip()
        linea_sin_importe = re.sub(r"\s*\$?\s*\d{1,3}(?:\.\d{3})*,\d{2}\s*$", "", linea).strip()
        return f"{prefijo}{linea_sin_importe}" if linea_sin_importe else ""

    def es_linea_percepcion_valida(linea: str) -> bool:
        linea_norm = re.sub(r"\s+", " ", linea).strip().lower()
        if not extraer_ultimo_importe_monetario(linea):
            return False
        patrones_invalidos = [
            r"\bventa[s]?\b", r"\bcuota[s]?\b", r"\blote\b", r"\bfecha\b",
            r"\btotal del día\b", r"\btotal del dia\b", r"\barancel\b",
            r"\bplan\b", r"\bserv\.", r"\bcostos financieros\b"
        ]
        for patron in patrones_invalidos:
            if re.search(patron, linea_norm):
                return False
        patrones_validos = [
            r"\biva\b", r"\brg\b", r"\bperc", r"\biibb\b",
            r"\bingresos brutos\b", r"\bsircreb\b"
        ]
        return any(re.search(patron, linea_norm) for patron in patrones_validos)

    def procesar_subbloque_detallado(subbloque, prefijo_columna, columna_total, filtrar_percepciones=False):
        resultados = {}
        total      = 0.0
        lineas     = [l.strip() for l in subbloque.splitlines() if l.strip()]
        lineas_unidas = []
        i = 0
        while i < len(lineas):
            linea_actual = re.sub(r"\s+", " ", lineas[i]).strip()
            if extraer_ultimo_importe_monetario(linea_actual):
                lineas_unidas.append(linea_actual); i += 1
            else:
                if i + 1 < len(lineas):
                    siguiente = re.sub(r"\s+", " ", lineas[i + 1]).strip()
                    lineas_unidas.append(f"{linea_actual} {siguiente}".strip()); i += 2
                else:
                    i += 1
        for linea in lineas_unidas:
            if filtrar_percepciones and not es_linea_percepcion_valida(linea):
                continue
            importe = extraer_ultimo_importe_monetario(linea)
            if importe:
                valor = convertir_numero(importe)
                nombre_columna = limpiar_nombre_concepto(linea, prefijo_columna)
                if nombre_columna:
                    resultados[nombre_columna] = resultados.get(nombre_columna, 0) + valor
                total += valor
        resultados[columna_total] = total
        return resultados

    doc   = fitz.open(stream=pdf_bytes, filetype="pdf")
    texto = ""
    for pagina in doc:
        texto += pagina.get_text()

    match_bloque = re.search(
        r"-Impuestos(.*?)(?:Total del día|Base Imponible IVA|SE ACREDITO EN:|Fin de la información|$)",
        texto, re.IGNORECASE | re.DOTALL
    )
    bloque = match_bloque.group(1) if match_bloque else texto

    # IVA 21%
    for valor in re.findall(r"IVA\s*21,00%\s*\$?\s*([\d\.,]+)", bloque, re.IGNORECASE):
        datos["IVA 21%"] += convertir_numero(valor)

    # IVA 10,5%
    for patron in [r"IVA\s*10,50\s*%\s*Ley\s*25\.?063\s*\$?\s*([\d\.,]+)",
                   r"IVA\s*10,5\s*%\s*Ley\s*25\.?063\s*\$?\s*([\d\.,]+)"]:
        for valor in re.findall(patron, bloque, re.IGNORECASE):
            datos["IVA 10,5%"] += convertir_numero(valor)

    # Percepciones
    match_per = re.search(
        r"-Percepciones(.*?)(?:-Retenciones|Total del día|Base Imponible IVA|SE ACREDITO EN:|$)",
        bloque, re.IGNORECASE | re.DOTALL
    )
    if match_per:
        datos.update(procesar_subbloque_detallado(
            match_per.group(1), "P. ", "Percepciones Totales", filtrar_percepciones=True))

    # Retenciones
    match_ret = re.search(
        r"-Retenciones(.*?)(?:-Percepciones|Total del día|Base Imponible IVA|SE ACREDITO EN:|$)",
        bloque, re.IGNORECASE | re.DOTALL
    )
    if match_ret:
        datos.update(procesar_subbloque_detallado(
            match_ret.group(1), "R. ", "Retenciones Totales", filtrar_percepciones=False))

    return datos


# ─────────────────────────────────────────────
# ENRIQUECER CON BASES IMPONIBLES
# ─────────────────────────────────────────────

def calcular_bases_imponibles_pdf(pdf_bytes: bytes, detalle: dict) -> dict:
    import fitz

    detalle = detalle.copy()

    detalle["Monto Gravado IVA 21%"]   = round(detalle.get("IVA 21%", 0.0) / 0.21, 2)   if detalle.get("IVA 21%", 0.0)   else 0.0
    detalle["Monto Gravado IVA 10,5%"] = round(detalle.get("IVA 10,5%", 0.0) / 0.105, 2) if detalle.get("IVA 10,5%", 0.0) else 0.0
    detalle["Base Exenta"] = 0.0

    def convertir_numero(valor: str) -> float:
        return float(valor.replace(".", "").replace(",", ".").strip())

    doc   = fitz.open(stream=pdf_bytes, filetype="pdf")
    texto = ""
    for pagina in doc:
        texto += pagina.get_text()

    match_base = re.search(r"Base\s+Exenta\s*\$?\s*([\d\.,]+)", texto, re.IGNORECASE)
    if match_base:
        detalle["Base Exenta"] = convertir_numero(match_base.group(1))

    return detalle


# ─────────────────────────────────────────────
# PIPELINE COMPLETO
# ─────────────────────────────────────────────

def procesar_pdfs_payway(archivos_subidos) -> bytes:
    """
    Recibe una lista de objetos subidos por Streamlit (file_uploader),
    procesa cada PDF y devuelve un Excel en memoria.
    """
    columnas_fijas = [
        "ID", "Total Neto", "Total Presentado", "Total Descuentos",
        "IVA 10,5%", "IVA 21%", "Retenciones Totales", "Percepciones Totales",
        "Monto Gravado IVA 21%", "Monto Gravado IVA 10,5%", "Base Exenta"
    ]

    filas             = []
    columnas_dinamicas = set()

    for archivo in archivos_subidos:
        pdf_bytes     = archivo.read()
        nombre        = archivo.name

        datos   = extraer_datos_base_pdf(pdf_bytes, nombre)
        detalle = extraer_detalle_descuentos_pdf(pdf_bytes)
        detalle.update(datos)
        fila    = calcular_bases_imponibles_pdf(pdf_bytes, detalle)

        filas.append(fila)

        for clave in fila.keys():
            if clave not in columnas_fijas:
                columnas_dinamicas.add(clave)

    columnas_finales = columnas_fijas + sorted(columnas_dinamicas)

    df = pd.DataFrame(filas)
    for col in columnas_finales:
        if col not in df.columns:
            df[col] = "" if col == "ID" else 0.0

    df = df[columnas_finales]

    columnas_num = [c for c in df.columns if c != "ID"]
    df[columnas_num] = df[columnas_num].fillna(0.0)

    fila_total = {"ID": "TOTAL"}
    for col in columnas_num:
        fila_total[col] = round(df[col].sum(), 2)

    df = pd.concat([df, pd.DataFrame([fila_total])], ignore_index=True)

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Liquidaciones", index=False)

    return buf.getvalue()
