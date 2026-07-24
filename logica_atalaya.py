import io
import re
import datetime as dt

import pandas as pd


# ─────────────────────────────────────────────
# DEPURAR RAPPI (extracto de órdenes)
# ─────────────────────────────────────────────

def depurar_rappi(df):
    meses = {
        'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'ago': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12'
    }

    def parsear_fecha(texto):
        if pd.isna(texto):
            return pd.NaT
        if isinstance(texto, (pd.Timestamp, dt.datetime, dt.date)):
            return pd.Timestamp(texto)

        texto = str(texto).strip().lower()
        primer_digito = re.search(r'\d', texto)
        if not primer_digito:
            raise ValueError(f"Formato de fecha no reconocido: {texto}")
        texto_recortado = texto[primer_digito.start():]

        m = re.match(
            r'(\d{1,2})\s+([a-zñáéíóú]+)\.\s+(\d{4}),\s+(\d{1,2}):(\d{2}):(\d{2})\s*(a|p)\.\s*m\.',
            texto_recortado
        )
        if not m:
            try:
                return pd.to_datetime(texto)
            except Exception:
                raise ValueError(f"Formato de fecha no reconocido: {texto}")

        dia, mes_abr, anio, hora, minuto, segundo, periodo = m.groups()
        mes = meses[mes_abr[:3]]
        hora = int(hora)
        if periodo == 'p' and hora != 12:
            hora += 12
        if periodo == 'a' and hora == 12:
            hora = 0
        return f"{anio}-{mes}-{dia.zfill(2)} {hora:02d}:{minuto}:{segundo}"

    fecha_hora = pd.to_datetime(df["Fecha de creación orden"].apply(parsear_fecha))
    df["Fecha de creación orden"] = fecha_hora.dt.normalize()

    for col in ["Venta Bruta", "Descuento de Producto asumido por el aliado"]:
        df[col] = df[col].astype(float).round(2)

    df["Ventas Totales"] = (
        df["Venta Bruta"] + df["Descuento de Producto asumido por el aliado"]
    ).round(2)

    columnas_a_dropear = [
        "Tiempo de preparación", "Prime  ", "Retroactivo", "Porcentaje de Cancelación",
        "Porcentaje de Uso y alquiler de plataforma Rappi",
        "Porcentaje de Uso y alquiler de plataforma Rappi Prime", "Tipo de transacción",
        "Ventas base por Uso y alquiler de plataforma Rappi (informativo)", "Venta Bruta",
        "Descuento en créditos", "Descuento de Producto asumido por el aliado",
        "Descuentos por inversión de Rappi DAR", "Costo de Domicilio - Propinas (marketplace)",
        "Meal Vouchers", "Total pagado por Repartidor independiente al Aliado en Efectivo",
        "Total pagado por el Usuario al Aliado  (marketplace)", "Descuento por Domicilio gratis",
        "Compensaciones", "Costo Canceladas", "Uso y alquiler de plataforma Rappi",
        "Descuento por inversión de Rappi  a aplicar sobre Uso y alquiler de plataforma Rappi DAR",
        "Uso y alquiler de plataforma Rappi Prime", "Tarifa de Integration", "Tarifa por demora",
        "Tarifa Transaccional", "Tarifa por activación (marketplace)", "Cuota de RappiAds",
        "Tarifa de servicio al usuario", "Contracargos", "Descuento por Service Fee",
        "Servicio de Cargo", "Descuento por pago anticipado", "Subtotal antes de impuestos",
        "IVA Uso y alquiler de plataforma Rappi",
        "Descuento por inversión de Rappi a aplicar sobre el IVA Uso y alquiler de plataforma Rappi DAR",
        "IVA Campañas", "Reteiva Uso y alquiler de plataforma Rappi", "Percepcion",
        "Descuento por inversión de Rappi a aplicar sobre la PERCEPCIÓN DE BA DAR",
        "CABA  ", "CBDA  ", "SANTA FE ", "Retencion Ganancias ", "Retencion Buenos Aires",
        "IVA Rappi Ads", "ReteIVA Rappi Ads", "IVA Descuento por Service Fee",
        "Retefuente Descuento por Service Fee", "IVA Servicio de Cargo",
        "Percepción Servicio de Cargo", "Percepción Córdoba Servicio de Cargo",
        "Percepción Corrientes Servicio de Cargo", "Percepción Tucuman Servicio de Cargo",
        "Percepcion Cordoba",
        "Descuento por inversión de Rappi a aplicar sobre la PERCEPCIÓN DE CÓRDOBA DAR",
        "Retencion Cordoba ", "Percepción Tucuman",
        "Descuento por inversión de Rappi a aplicar sobre la PERCEPCIÓN DE TUCUMAN DAR",
        "Retención Tucuman", "Perceptión Corrientes",
        "Descuento por inversión de Rappi a aplicar sobre la PERCEPCIÓN DE CORRIENTES DAR",
        "Percepción Campañas", "Percepción Campañas Tucuman", "Percepción Campañas Corrientes",
        "Percepción Campañas Cordoba", "Percepción de IVA", "Unnamed: 75",
        "Percepción activación fee", "Percepción tarifa de servicio",
        "Percepción Cordoba Tarifa de servicio", "Percepción Current sobre Tarifa de servicio",
        "Percepción tucuman sobre Tarifa de servicio", "Valor Ajustes Manuales",
        "Deuda Periodos Anteriores", "Cuota de préstamo",
        "Cashback en Rappi creditos asumido por el aliado",
        "Challenge Rappi créditos asumidos por el aliado", "Cashback 15 mis o gratis",
        "Valor Neto", "Valor a transferir",
        "ID relacionado (global offer / campaña ADs / paidlot deuda)",
        "ID Paidlot retroactivo", "Razon (Ajuste / RADs)",
        "Descripción o comentarios (Ajustes  / RADs)  ", "Hora"
    ]

    df = df.drop(columns=columnas_a_dropear, errors="ignore")

    return df


# ─────────────────────────────────────────────
# DEPURAR ATALAYA (extracto de ventas del sistema interno)
# ─────────────────────────────────────────────

def depurar_atalaya(df):
    df["CON IVA"] = df["CON IVA"].astype(float).round(2)
    return df


# ─────────────────────────────────────────────
# CRUCE RAPPI vs ATALAYA
# ─────────────────────────────────────────────

def cruzar_rappi_atalaya(df_rappi_dep, df_atalaya_dep):
    rappi = df_rappi_dep[
        df_rappi_dep["Estado de la órden"].str.contains("pending_review", na=False)
    ].reset_index(drop=True).copy()
    atalaya = df_atalaya_dep[
        df_atalaya_dep["Medio Pago"].str.contains("RAPPI", na=False)
    ].reset_index(drop=True).copy()

    rappi["_id_rappi"] = rappi.index
    atalaya["_id_atalaya"] = atalaya.index

    r_pool = rappi.copy()
    a_pool = atalaya.copy()
    r_pool["_rank"] = r_pool.groupby(["Fecha de creación orden", "Ventas Totales"]).cumcount()
    a_pool["_rank"] = a_pool.groupby(["Fecha", "CON IVA"]).cumcount()

    pares = r_pool.merge(
        a_pool[["Fecha", "CON IVA", "_rank", "_id_atalaya"]],
        left_on=["Fecha de creación orden", "Ventas Totales", "_rank"],
        right_on=["Fecha", "CON IVA", "_rank"],
        how="inner"
    )[["_id_rappi", "_id_atalaya"]]

    match_rappi = rappi[rappi["_id_rappi"].isin(pares["_id_rappi"])].drop(columns=["_id_rappi"])
    match_atalaya = atalaya[atalaya["_id_atalaya"].isin(pares["_id_atalaya"])].drop(columns=["_id_atalaya"])

    falta_rappi = atalaya[~atalaya["_id_atalaya"].isin(pares["_id_atalaya"])].drop(columns=["_id_atalaya"])
    falta_atalaya = rappi[~rappi["_id_rappi"].isin(pares["_id_rappi"])].drop(columns=["_id_rappi"])

    return match_atalaya, match_rappi, falta_rappi, falta_atalaya


# ─────────────────────────────────────────────
# FUNCIÓN PRINCIPAL → devuelve Excel en memoria
# ─────────────────────────────────────────────

def correr_atalaya(archivo_rappi, archivo_atalaya):
    df_rappi = pd.read_excel(archivo_rappi)
    df_atalaya = pd.read_excel(archivo_atalaya, header=0)

    df_rappi_dep = depurar_rappi(df_rappi)
    df_atalaya_dep = depurar_atalaya(df_atalaya)

    match_atalaya, match_rappi, falta_rappi, falta_atalaya = cruzar_rappi_atalaya(
        df_rappi_dep, df_atalaya_dep
    )

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        match_atalaya.to_excel(writer, sheet_name="match_atalaya", index=False)
        match_rappi.to_excel(writer, sheet_name="match_rappi", index=False)
        falta_rappi.to_excel(writer, sheet_name="falta_rappi", index=False)
        falta_atalaya.to_excel(writer, sheet_name="falta_atalaya", index=False)
    buf.seek(0)

    stats = {
        'match': len(match_atalaya),
        'falta_rappi': len(falta_rappi),
        'falta_atalaya': len(falta_atalaya),
    }
    return buf, stats
