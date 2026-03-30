import streamlit as st
from logica_ventas import correr_cruce_ventas

st.set_page_config(
    page_title="Cruce de Ventas",
    page_icon="📊",
    layout="centered",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.stApp { background-color: #0f0f0f; color: #e8e8e8; }
.header-block {
    border-left: 3px solid #00aaff;
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
    font-size: 0.72rem; color: #00aaff;
    letter-spacing: 1.5px; text-transform: uppercase;
    margin-bottom: 0.4rem;
}
.upload-label.secondary { color: #aaaaaa; }
.section-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.7rem; color: #444;
    letter-spacing: 2px; text-transform: uppercase;
    margin: 1.5rem 0 1rem 0;
}
[data-testid="stFileUploader"] {
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 0.5rem; transition: border-color 0.2s;
}
[data-testid="stFileUploader"]:hover { border-color: #00aaff; }
.stButton > button {
    background: #00aaff !important; color: #0f0f0f !important;
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
    font-size: 2rem; font-weight: 600; color: #00aaff; line-height: 1;
}
.metric-card .metric-label {
    font-size: 0.7rem; color: #555; text-transform: uppercase;
    letter-spacing: 1px; margin-top: 0.4rem;
    font-family: 'IBM Plex Mono', monospace;
}
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
    border-color: #00aaff !important; color: #00aaff !important;
}
.back-btn > button {
    background: transparent !important; color: #444 !important;
    border: 1px solid #2a2a2a !important; font-size: 0.75rem !important;
    margin-top: 0 !important; margin-bottom: 1rem !important;
}
.back-btn > button:hover { color: #00aaff !important; border-color: #00aaff !important; }
</style>
""", unsafe_allow_html=True)

# Botón volver
st.markdown('<div class="back-btn">', unsafe_allow_html=True)
if st.button("← Volver al inicio"):
    st.switch_page("app_principal.py")
st.markdown('</div>', unsafe_allow_html=True)

st.markdown("""
<div class="header-block">
    <h1>Cruce de Ventas</h1>
    <p>ARCA &nbsp;×&nbsp; Sistema Interno</p>
</div>
""", unsafe_allow_html=True)

st.markdown('<div class="section-title">Archivos del mes</div>', unsafe_allow_html=True)
col1, col2 = st.columns(2)
with col1:
    st.markdown('<div class="upload-label">Excel ARCA</div>', unsafe_allow_html=True)
    archivo_arca = st.file_uploader("arca", type=["xlsx", "xls"],
                                     label_visibility="collapsed", key="arca")
with col2:
    st.markdown('<div class="upload-label">Excel Sistema</div>', unsafe_allow_html=True)
    archivo_sistema = st.file_uploader("sistema", type=["xlsx", "xls"],
                                        label_visibility="collapsed", key="sistema")

st.markdown('<div class="section-title">Archivos auxiliares</div>', unsafe_allow_html=True)
col3, col4 = st.columns(2)
with col3:
    st.markdown('<div class="upload-label secondary">Sistema mes anterior</div>', unsafe_allow_html=True)
    archivo_sistema_prev = st.file_uploader("sistema_prev", type=["xlsx", "xls"],
                                             label_visibility="collapsed", key="sistema_prev")
with col4:
    st.markdown('<div class="upload-label secondary">ARCA mes siguiente</div>', unsafe_allow_html=True)
    archivo_arca_post = st.file_uploader("arca_post", type=["xlsx", "xls"],
                                          label_visibility="collapsed", key="arca_post")

st.markdown("<hr class='divider'>", unsafe_allow_html=True)

todos_cargados = all([archivo_arca, archivo_sistema, archivo_sistema_prev, archivo_arca_post])
if not todos_cargados:
    st.info("Cargá los cuatro archivos Excel para habilitar el cruce.")

boton = st.button("CRUZAR VENTAS", disabled=not todos_cargados, use_container_width=True)

if boton and todos_cargados:
    with st.spinner("Procesando..."):
        try:
            buf, stats = correr_cruce_ventas(
                archivo_arca=archivo_arca,
                archivo_sistema=archivo_sistema,
                archivo_sistema_prev=archivo_sistema_prev,
                archivo_arca_post=archivo_arca_post,
            )
            st.session_state["resultado_ventas"] = {"buf": buf, "stats": stats}
        except Exception as e:
            st.error(f"Error al procesar: {e}")
            st.stop()

if "resultado_ventas" in st.session_state:
    r     = st.session_state["resultado_ventas"]
    stats = r["stats"]

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    clase_falt_sis  = "error" if stats["faltante_en_sistema"] > 0 else "metric-card"
    clase_falt_arca = "error" if stats["faltante_en_arca"]    > 0 else "metric-card"

    st.markdown(f"""
    <div class="metric-row">
        <div class="metric-card">
            <div class="metric-value">{stats['matcheado']}</div>
            <div class="metric-label">Match inicial</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">{stats['match_definitivo']}</div>
            <div class="metric-label">Match definitivo</div>
        </div>
        <div class="metric-card {clase_falt_sis}">
            <div class="metric-value">{stats['faltante_en_sistema']}</div>
            <div class="metric-label">Faltante sistema</div>
        </div>
        <div class="metric-card {clase_falt_arca}">
            <div class="metric-value">{stats['faltante_en_arca']}</div>
            <div class="metric-label">Faltante ARCA</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.download_button(
        label="📥 Descargar faltantes definitivos",
        data=r["buf"],
        file_name="faltante_definitivo.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
