import streamlit as st
from logica_tesoreria import correr_conciliacion_tesoreria

st.set_page_config(
    page_title="Tesorería",
    page_icon="💰",
    layout="centered",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.stApp { background-color: #0f0f0f; color: #e8e8e8; }

.header-block {
    border-left: 3px solid #ffb020;
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
    font-size: 0.72rem; color: #ffb020;
    letter-spacing: 1.5px; text-transform: uppercase;
    margin-bottom: 0.4rem;
}
[data-testid="stFileUploader"] {
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 0.8rem; transition: border-color 0.2s;
}
[data-testid="stFileUploader"]:hover { border-color: #ffb020; }

.stButton > button {
    background: #ffb020 !important; color: #0f0f0f !important;
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
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 1.2rem;
    text-align: center; margin: 1rem 0;
}
.counter-box .counter-num {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 2.5rem; font-weight: 600;
    color: #ffb020; line-height: 1;
}
.counter-box .counter-label {
    font-size: 0.75rem; color: #555;
    text-transform: uppercase; letter-spacing: 1px;
    margin-top: 0.4rem; font-family: 'IBM Plex Mono', monospace;
}

.metric-row { display: flex; gap: 1rem; margin: 1.5rem 0; flex-wrap: wrap; }
.metric-card {
    flex: 1; min-width: 120px; background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 1rem; text-align: center;
}
.metric-card .metric-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 2rem; font-weight: 600; color: #ffb020; line-height: 1;
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
    border-color: #ffb020 !important; color: #ffb020 !important;
}

.back-btn > button {
    background: transparent !important; color: #444 !important;
    border: 1px solid #2a2a2a !important; font-size: 0.75rem !important;
    margin-top: 0 !important; margin-bottom: 1rem !important;
}
.back-btn > button:hover { color: #ffb020 !important; border-color: #ffb020 !important; }
</style>
""", unsafe_allow_html=True)

# Botón volver
st.markdown('<div class="back-btn">', unsafe_allow_html=True)
if st.button("← Volver al inicio"):
    st.switch_page(st.session_state["_pages"]["home"])
st.markdown('</div>', unsafe_allow_html=True)

st.markdown("""
<div class="header-block">
    <h1>💰 Tesorería</h1>
    <p>Caja Central &nbsp;×&nbsp; Contabilidad</p>
</div>
""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

st.markdown('<div class="upload-label">Caja Tesorería</div>', unsafe_allow_html=True)
archivos_caja_central = st.file_uploader(
    "caja_central", type=["xlsx", "xls"],
    accept_multiple_files=True,
    label_visibility="collapsed", key="caja_central",
)

if archivos_caja_central:
    n = len(archivos_caja_central)
    st.markdown(f"""
    <div class="counter-box">
        <div class="counter-num">{n}</div>
        <div class="counter-label">mes{"es" if n != 1 else ""} de Caja Central cargado{"s" if n != 1 else ""}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="upload-label">Caja Sistema Unificada</div>', unsafe_allow_html=True)
archivo_caja_unificada = st.file_uploader("caja_unificada", type=["xlsx", "xls"],
                                           label_visibility="collapsed", key="caja_unificada")

st.markdown("<hr class='divider'>", unsafe_allow_html=True)

todo_ok = bool(archivos_caja_central and archivo_caja_unificada)
if not archivos_caja_central:
    st.info("Cargá al menos un Excel de Caja Central.")
elif not archivo_caja_unificada:
    st.info("Cargá el Excel de Caja Unificada del sistema.")

boton_tesoreria = st.button("CRUZAR TESORERÍA", disabled=not todo_ok,
                             use_container_width=True, key="btn_tesoreria")

if boton_tesoreria and todo_ok:
    with st.spinner("Procesando Tesorería..."):
        try:
            buf, stats = correr_conciliacion_tesoreria(
                archivos_caja_central=archivos_caja_central,
                archivo_caja_unificada=archivo_caja_unificada,
            )
            st.session_state["resultado_tesoreria"] = {"buf": buf, "stats": stats}
        except Exception as e:
            st.error(f"Error al procesar Tesorería: {e}")

if "resultado_tesoreria" in st.session_state:
    r = st.session_state["resultado_tesoreria"]
    s = r["stats"]

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    for w in s["warnings"]:
        st.warning(w)

    clase_fc = "error" if s["falta_contabilidad"] > 0 else "metric-card"
    clase_ft = "error" if s["falta_tesoreria"] > 0 else "metric-card"

    st.markdown(f"""
    <div class="metric-row">
        <div class="metric-card">
            <div class="metric-value">{s['match']}</div>
            <div class="metric-label">Match</div>
        </div>
        <div class="metric-card {clase_fc}">
            <div class="metric-value">{s['falta_contabilidad']}</div>
            <div class="metric-label">Falta contabilidad</div>
        </div>
        <div class="metric-card {clase_ft}">
            <div class="metric-value">{s['falta_tesoreria']}</div>
            <div class="metric-label">Falta tesorería</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.download_button(
        label="📥 Descargar reporte Tesorería",
        data=r["buf"],
        file_name="reporte_tesoreria.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
