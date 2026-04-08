import streamlit as st
from logica_payway import procesar_pdfs_payway

st.set_page_config(
    page_title="Lector PDFs",
    page_icon="📷",
    layout="centered",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.stApp { background-color: #0f0f0f; color: #e8e8e8; }

.header-block {
    border-left: 3px solid #c084fc;
    padding: 0.4rem 0 0.4rem 1.2rem;
    margin-bottom: 2rem;
}
.header-block h1 {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.6rem; font-weight: 600;
    color: #ffffff; margin: 0; letter-spacing: -0.5px;
}
.header-block p {
    font-size: 0.82rem; color: #666;
    margin: 0.2rem 0 0 0;
    font-family: 'IBM Plex Mono', monospace;
}
.upload-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem; color: #c084fc;
    letter-spacing: 1.5px; text-transform: uppercase;
    margin-bottom: 0.4rem;
}
[data-testid="stFileUploader"] {
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 0.8rem; transition: border-color 0.2s;
}
[data-testid="stFileUploader"]:hover { border-color: #c084fc; }

.stButton > button {
    background: #c084fc !important; color: #0f0f0f !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-weight: 600 !important; font-size: 0.85rem !important;
    letter-spacing: 1px !important; border: none !important;
    border-radius: 4px !important; padding: 0.6rem 2rem !important;
    width: 100% !important; margin-top: 1rem !important;
    transition: opacity 0.2s !important;
}
.stButton > button:hover { opacity: 0.85 !important; }
.stButton > button:disabled { background: #2a2a2a !important; color: #555 !important; }

.counter-box {
    background: #1a1a1a;
    border: 1px solid #2a2a2a;
    border-radius: 6px;
    padding: 1.2rem;
    text-align: center;
    margin: 1.5rem 0;
}
.counter-box .counter-num {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 2.5rem; font-weight: 600;
    color: #c084fc; line-height: 1;
}
.counter-box .counter-label {
    font-size: 0.75rem; color: #555;
    text-transform: uppercase; letter-spacing: 1px;
    margin-top: 0.4rem;
    font-family: 'IBM Plex Mono', monospace;
}

.divider { border: none; border-top: 1px solid #1e1e1e; margin: 2rem 0; }

[data-testid="stDownloadButton"] > button {
    background: #1a1a1a !important; color: #e8e8e8 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.8rem !important; border: 1px solid #2a2a2a !important;
    border-radius: 4px !important; width: 100% !important;
    transition: border-color 0.2s !important;
}
[data-testid="stDownloadButton"] > button:hover {
    border-color: #c084fc !important; color: #c084fc !important;
}

.back-btn > button {
    background: transparent !important; color: #444 !important;
    border: 1px solid #2a2a2a !important; font-size: 0.75rem !important;
    margin-top: 0 !important; margin-bottom: 1rem !important;
}
.back-btn > button:hover { color: #c084fc !important; border-color: #c084fc !important; }

.pdf-list {
    background: #1a1a1a;
    border: 1px solid #2a2a2a;
    border-radius: 6px;
    padding: 1rem 1.2rem;
    margin: 1rem 0;
    max-height: 200px;
    overflow-y: auto;
}
.pdf-item {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem; color: #888;
    padding: 0.2rem 0;
    border-bottom: 1px solid #222;
}
.pdf-item:last-child { border-bottom: none; }
.pdf-item::before { content: "📄 "; }
</style>
""", unsafe_allow_html=True)

# Botón volver
st.markdown('<div class="back-btn">', unsafe_allow_html=True)
if st.button("← Volver al inicio"):
    st.switch_page("app_home.py")
st.markdown('</div>', unsafe_allow_html=True)

st.markdown("""
<div class="header-block">
    <h1>Lector PDFs</h1>
    <p>Liquidaciones Payway &nbsp;→&nbsp; Excel</p>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# UPLOADER MÚLTIPLE
# ─────────────────────────────────────────────
st.markdown('<div class="upload-label">Liquidaciones PDF</div>', unsafe_allow_html=True)
archivos = st.file_uploader(
    "pdfs",
    type=["pdf"],
    accept_multiple_files=True,
    label_visibility="collapsed",
    key="payway_pdfs"
)

# Mostrar lista y contador si hay archivos
if archivos:
    st.markdown(f"""
    <div class="counter-box">
        <div class="counter-num">{len(archivos)}</div>
        <div class="counter-label">PDF{"s" if len(archivos) != 1 else ""} cargado{"s" if len(archivos) != 1 else ""}</div>
    </div>
    """, unsafe_allow_html=True)

    # Lista de archivos
    items = "".join(f'<div class="pdf-item">{a.name}</div>' for a in archivos)
    st.markdown(f'<div class="pdf-list">{items}</div>', unsafe_allow_html=True)

st.markdown("<hr class='divider'>", unsafe_allow_html=True)

if not archivos:
    st.info("Arrastrá todos los PDFs de liquidaciones o hacé click para seleccionarlos.")

boton = st.button(
    "PROCESAR PDFs",
    disabled=not archivos,
    use_container_width=True,
    key="btn_payway"
)

# ─────────────────────────────────────────────
# PROCESAMIENTO
# ─────────────────────────────────────────────
if boton and archivos:
    with st.spinner(f"Procesando {len(archivos)} PDF{'s' if len(archivos) != 1 else ''}..."):
        try:
            buf = procesar_pdfs_payway(archivos)
            st.session_state["resultado_payway"] = buf
        except Exception as e:
            st.error(f"Error al procesar: {e}")

# ─────────────────────────────────────────────
# RESULTADO
# ─────────────────────────────────────────────
if "resultado_payway" in st.session_state:
    st.markdown("<hr class='divider'>", unsafe_allow_html=True)
    st.success("¡Listo! El resumen está generado.")
    st.download_button(
        label="📥 Descargar resumen liquidaciones",
        data=st.session_state["resultado_payway"],
        file_name="resumen_liquidaciones.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key="dl_payway"
    )
