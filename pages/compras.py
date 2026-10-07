from io import BytesIO

import streamlit as st
from estilos import aplicar_estilos, encabezado
from logica import correr_cruce, detectar_sociedad, load_excel_file, SOCIEDADES_POR_CUIT

aplicar_estilos()

encabezado(
    "Compras",
    "Comprobantes recibidos: ARCA contra sistema interno.",
    "Comprobantes / Compras",
)

col1, col2 = st.columns(2)
with col1:
    st.markdown('<div class="upload-label">Excel ARCA</div>', unsafe_allow_html=True)
    archivo_arca = st.file_uploader("arca", type=["xlsx", "xls"],
                                     label_visibility="collapsed", key="arca")
with col2:
    st.markdown('<div class="upload-label">Excel Sistema (ICG)</div>', unsafe_allow_html=True)
    archivo_sistema = st.file_uploader("sistema", type=["xlsx", "xls"],
                                        label_visibility="collapsed", key="sistema")

# Detección de sociedad apenas se carga el Excel de ARCA
sociedad = None
if archivo_arca is not None:
    file_id = getattr(archivo_arca, "file_id", archivo_arca.name)
    det = st.session_state.get("sociedad_arca")
    if not det or det["file_id"] != file_id:
        try:
            df_tmp = load_excel_file(BytesIO(archivo_arca.getvalue()))
            det = {
                "file_id":  file_id,
                "sociedad": detectar_sociedad(df_tmp),
                "columnas": [str(c) for c in df_tmp.columns],
            }
        except Exception as e:
            det = {"file_id": file_id, "sociedad": None, "columnas": [], "error": str(e)}
        st.session_state["sociedad_arca"] = det

    opciones = sorted(set(SOCIEDADES_POR_CUIT.values()), key=str.lower)
    if det["sociedad"]:
        st.success(f"Sociedad detectada: {det['sociedad']}")
    else:
        st.warning(
            "No se encontró el CUIT de ninguna sociedad en el Excel de ARCA "
            "(se buscó en 'Nro. Doc. Receptor', en los encabezados y en el resto de las columnas). "
            "Seleccioná la sociedad antes de cruzar."
        )
        if det.get("error"):
            st.caption(f"Error al leer el archivo: {det['error']}")
        with st.expander("Columnas recibidas del Excel ARCA"):
            st.write(det["columnas"])

    sociedad = st.selectbox(
        "Sociedad",
        options=opciones,
        index=opciones.index(det["sociedad"]) if det["sociedad"] in opciones else None,
        placeholder="Elegí la sociedad",
        key=f"sociedad_{file_id}",
    )

st.markdown("<hr class='divider'>", unsafe_allow_html=True)

tol = st.slider("Tolerancia de importes ($ ±)", min_value=0.0, max_value=10.0, value=1.0, step=0.5)

ambos_cargados = archivo_arca is not None and archivo_sistema is not None
listo = ambos_cargados and bool(sociedad)
if not ambos_cargados:
    st.info("Cargá los dos archivos Excel para habilitar el cruce.")
elif not sociedad:
    st.info("Seleccioná la sociedad para habilitar el cruce.")

boton = st.button("Cruzar comprobantes", disabled=not listo, use_container_width=True)

if boton and listo:
    with st.spinner("Procesando..."):
        try:
            buf_reporte, stats = correr_cruce(
                archivo_arca=archivo_arca,
                archivo_sistema=archivo_sistema,
                tol_pesos=tol,
            )
            st.session_state["resultado_compras"] = {
                "buf_reporte": buf_reporte,
                "stats":       stats,
                "sociedad":    sociedad,
            }
        except Exception as e:
            st.error(f"Error al procesar: {e}")
            st.stop()

if "resultado_compras" in st.session_state:
    r     = st.session_state["resultado_compras"]
    stats = r["stats"]

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    clase_revisar = "warn"  if stats["revisar"]          > 0 else "metric-card"
    clase_dup     = "warn"  if stats["duplicados"]       > 0 else "metric-card"
    clase_ng      = "metric-card"
    clase_falt_a  = "error" if stats["faltante_arca"]    > 0 else "metric-card"
    clase_falt_s  = "error" if stats["faltante_sistema"] > 0 else "metric-card"

    st.markdown(f"""
    <div class="metric-row">
        <div class="metric-card">
            <div class="metric-value">{stats['match']}</div>
            <div class="metric-label">Con match</div>
        </div>
        <div class="metric-card {clase_revisar}">
            <div class="metric-value">{stats['revisar']}</div>
            <div class="metric-label">A revisar</div>
        </div>
        <div class="metric-card {clase_dup}">
            <div class="metric-value">{stats['duplicados']}</div>
            <div class="metric-label">Duplicados (en revisar)</div>
        </div>
        <div class="metric-card {clase_ng}">
            <div class="metric-value">{stats['no_gravado']}</div>
            <div class="metric-label">No Gravado (composición)</div>
        </div>
        <div class="metric-card {clase_falt_a}">
            <div class="metric-value">{stats['faltante_arca']}</div>
            <div class="metric-label">Faltante ARCA</div>
        </div>
        <div class="metric-card {clase_falt_s}">
            <div class="metric-value">{stats['faltante_sistema']}</div>
            <div class="metric-label">Faltante sistema</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.download_button(
        label="Descargar reporte completo",
        icon=":material/download:",
        data=r["buf_reporte"],
        file_name=f"reporte_{r.get('sociedad') or 'cruce'}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
