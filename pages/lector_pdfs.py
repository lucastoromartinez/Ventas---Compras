import streamlit as st
from estilos import aplicar_estilos, encabezado
from logica_payway import procesar_pdfs_payway

aplicar_estilos()

encabezado(
    "Lector de PDFs",
    "Procesamiento de liquidaciones Payway.",
    "Herramientas / Lector de PDFs",
)

st.markdown('<div class="upload-label">Liquidaciones PDF</div>', unsafe_allow_html=True)
archivos = st.file_uploader(
    "pdfs",
    type=["pdf"],
    accept_multiple_files=True,
    label_visibility="collapsed",
    key="payway_pdfs"
)

if archivos:
    st.markdown(f"""
    <div class="counter-box">
        <div class="counter-num">{len(archivos)}</div>
        <div class="counter-label">PDF{"s" if len(archivos) != 1 else ""} cargado{"s" if len(archivos) != 1 else ""}</div>
    </div>
    """, unsafe_allow_html=True)
    items = "".join(f'<div class="pdf-item">{a.name}</div>' for a in archivos)
    st.markdown(f'<div class="pdf-list">{items}</div>', unsafe_allow_html=True)

st.markdown("<hr class='divider'>", unsafe_allow_html=True)

if not archivos:
    st.info("Arrastrá todos los PDFs de liquidaciones o hacé click para seleccionarlos.")

boton = st.button(
    "Procesar PDFs",
    disabled=not archivos,
    use_container_width=True,
    key="btn_payway"
)

if boton and archivos:
    with st.spinner(f"Procesando {len(archivos)} PDF{'s' if len(archivos) != 1 else ''}..."):
        try:
            buf = procesar_pdfs_payway(archivos)
            st.session_state["resultado_payway"] = buf
        except Exception as e:
            st.error(f"Error al procesar: {e}")

if "resultado_payway" in st.session_state:
    st.markdown("<hr class='divider'>", unsafe_allow_html=True)
    st.success("¡Listo! El resumen está generado.")
    st.download_button(
        label="Descargar resumen liquidaciones",
        icon=":material/download:",
        data=st.session_state["resultado_payway"],
        file_name="resumen_liquidaciones.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key="dl_payway"
    )

