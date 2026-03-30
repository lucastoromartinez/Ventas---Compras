import streamlit as st
from logica import correr_cruce

st.set_page_config(
    page_title="Cruce de Compras",
    page_icon="🧾",
    layout="centered",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.stApp { background-color: #0f0f0f; color: #e8e8e8; }
.header-block {
    border-left: 3px solid #00ff87;
    padding: 0.4rem 0 0.4rem 1.2rem;
    margin-bottom: 2.5rem;
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
    font-size: 0.72rem; color: #00ff87;
    letter-spacing: 1.5px; text-transform: uppercase;
    margin-bottom: 0.4rem;
}
[data-testid="stFileUploader"] {
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 0.5rem; transition: border-color 0.2s;
}
[data-testid="stFileUploader"]:hover { border-color: #00ff87; }
.stButton > button {
    background: #00ff87 !important; color: #0f0f0f !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-weight: 600 !important; font-size: 0.85rem !important;
    letter-spacing: 1px !important; border: none !important;
    border-radius: 4px !important; padding: 0.6rem 2rem !important;
    width: 100% !important; margin-top: 1rem !important;
    transition: opacity 0.2s !important;
}
.stButton > button:hover { opacity: 0.85 !important; }
.stButton > button:disabled { background: #2a2a2a !important; color: #555 !important; }
.metric-row { display: flex; gap: 1rem; margin: 1.5rem 0; }
.metric-card {
    flex: 1; background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 1rem; text-align: center;
}
.metric-card .metric-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 2rem; font-weight: 600; color: #00ff87; line-height: 1;
}
.metric-card .metric-label {
    font-size: 0.7rem; color: #555; text-transform: uppercase;
    letter-spacing: 1px; margin-top: 0.4rem;
    font-family: 'IBM Plex Mono', monospace;
}
.metric-card.warn .metric-value  { color: #ffaa00; }
.metric-card.error .metric-value { color: #ff4444; }
.divider { border: none; border-top: 1px solid #1e1e1e; margin: 2rem 0; }
[data-testid="stDownloadButton"] > button {
    background: #1a1a1a !important; color: #e8e8e8 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.8rem !important; border: 1px solid #2a2a2a !important;
    border-radius: 4px !important; width: 100% !important;
    transition: border-color 0.2s !important;
}
[data-testid="stDownloadButton"] > button:hover {
    border-color: #00ff87 !important; color: #00ff87 !important;
}
.back-btn > button {
    background: transparent !important; color: #444 !important;
    border: 1px solid #2a2a2a !important; font-size: 0.75rem !important;
    margin-top: 0 !important; margin-bottom: 1rem !important;
}
.back-btn > button:hover { color: #00ff87 !important; border-color: #00ff87 !important; }
</style>
""", unsafe_allow_html=True)

# Botón volver
st.markdown('<div class="back-btn">', unsafe_allow_html=True)
if st.button("← Volver al inicio"):
    st.switch_page(st.Page("app_principal.py", title="Inicio", icon="⚡"))
st.markdown('</div>', unsafe_allow_html=True)

st.markdown("""
<div class="header-block">
    <h1>Cruce de Compras</h1>
    <p>ARCA &nbsp;×&nbsp; Sistema Interno</p>
</div>
""", unsafe_allow_html=True)

col1, col2 = st.columns(2)
with col1:
    st.markdown('<div class="upload-label">Excel ARCA</div>', unsafe_allow_html=True)
    archivo_arca = st.file_uploader("arca", type=["xlsx", "xls"],
                                     label_visibility="collapsed", key="arca")
with col2:
    st.markdown('<div class="upload-label">Excel Sistema (ICG)</div>', unsafe_allow_html=True)
    archivo_sistema = st.file_uploader("sistema", type=["xlsx", "xls"],
                                        label_visibility="collapsed", key="sistema")

st.markdown("<hr class='divider'>", unsafe_allow_html=True)

tol = st.slider("Tolerancia de importes ($ ±)", min_value=0.0, max_value=10.0, value=1.0, step=0.5)

ambos_cargados = archivo_arca is not None and archivo_sistema is not None
if not ambos_cargados:
    st.info("Cargá los dos archivos Excel para habilitar el cruce.")

boton = st.button("CRUZAR COMPROBANTES", disabled=not ambos_cargados, use_container_width=True)

if boton and ambos_cargados:
    with st.spinner("Procesando..."):
        try:
            buf_reporte, buf_faltante, stats = correr_cruce(
                archivo_arca=archivo_arca,
                archivo_sistema=archivo_sistema,
                tol_pesos=tol,
            )
            st.session_state["resultado_compras"] = {
                "buf_reporte":  buf_reporte,
                "buf_faltante": buf_faltante,
                "stats":        stats,
            }
        except Exception as e:
            st.error(f"Error al procesar: {e}")
            st.stop()

if "resultado_compras" in st.session_state:
    r     = st.session_state["resultado_compras"]
    stats = r["stats"]

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    clase_revisar = "warn"  if stats["revisar"]          > 0 else "metric-card"
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
    dcol1, dcol2 = st.columns(2)
    with dcol1:
        st.download_button(
            label="📥 Descargar reporte completo",
            data=r["buf_reporte"],
            file_name="reporte_cruce.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with dcol2:
        st.download_button(
            label="📥 Descargar faltante sistema",
            data=r["buf_faltante"],
            file_name="faltante_sistema.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
