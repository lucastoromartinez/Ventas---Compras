import streamlit as st
from logica_payway import procesar_pdfs_payway
from logica_rappi  import correr_rappi

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

.metric-row { display: flex; gap: 1rem; margin: 1.5rem 0; flex-wrap: wrap; }
.metric-card {
    flex: 1; min-width: 80px; background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 1rem; text-align: center;
}
.metric-card .metric-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 2rem; font-weight: 600; color: #c084fc; line-height: 1;
}
.metric-card .metric-label {
    font-size: 0.7rem; color: #555; text-transform: uppercase;
    letter-spacing: 1px; margin-top: 0.4rem;
    font-family: 'IBM Plex Mono', monospace;
}
.metric-card.warn .metric-value { color: #facc15; }
.metric-card.ok   .metric-value { color: #4ade80; }

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

div[data-testid="stTabs"] button {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.8rem !important; color: #555 !important;
}
div[data-testid="stTabs"] button[aria-selected="true"] {
    color: #c084fc !important; border-bottom-color: #c084fc !important;
}
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
    <p>Procesamiento de liquidaciones y facturas</p>
</div>
""", unsafe_allow_html=True)

tab_payway, tab_rappi = st.tabs(["💳  Payway", "🛵  Rappi"])


# ═══════════════════════════════════════════════
# TAB PAYWAY
# ═══════════════════════════════════════════════
with tab_payway:
    st.markdown("<br>", unsafe_allow_html=True)

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
        "PROCESAR PDFs",
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
            label="📥 Descargar resumen liquidaciones",
            data=st.session_state["resultado_payway"],
            file_name="resumen_liquidaciones.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_payway"
        )


# ═══════════════════════════════════════════════
# TAB RAPPI
# ═══════════════════════════════════════════════
with tab_rappi:
    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown('<div class="upload-label">Liquidación Rappi (Excel)</div>', unsafe_allow_html=True)
    archivo_liq = st.file_uploader(
        "rappi_liq",
        type=["xlsx", "xls"],
        accept_multiple_files=False,
        label_visibility="collapsed",
        key="rappi_liquidacion"
    )

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="upload-label">Facturas Rappi (PDFs)</div>', unsafe_allow_html=True)
    archivos_pdf = st.file_uploader(
        "rappi_pdfs",
        type=["pdf"],
        accept_multiple_files=True,
        label_visibility="collapsed",
        key="rappi_pdfs"
    )

    if archivos_pdf:
        st.markdown(f"""
        <div class="counter-box">
            <div class="counter-num">{len(archivos_pdf)}</div>
            <div class="counter-label">factura{"s" if len(archivos_pdf) != 1 else ""} cargada{"s" if len(archivos_pdf) != 1 else ""}</div>
        </div>
        """, unsafe_allow_html=True)

        items = "".join(f'<div class="pdf-item">{a.name}</div>' for a in archivos_pdf)
        st.markdown(f'<div class="pdf-list">{items}</div>', unsafe_allow_html=True)

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    todo_ok_rappi = bool(archivo_liq and archivos_pdf)
    if not archivo_liq:
        st.info("Cargá la liquidación Excel de Rappi para continuar.")
    elif not archivos_pdf:
        st.info("Cargá al menos una factura PDF de Rappi.")

    boton_rappi = st.button(
        "CRUZAR FACTURAS vs LIQUIDACIÓN",
        disabled=not todo_ok_rappi,
        use_container_width=True,
        key="btn_rappi"
    )

    if boton_rappi and todo_ok_rappi:
        with st.spinner("Procesando Rappi..."):
            try:
                buf_r, stats_r = correr_rappi(archivo_liq, archivos_pdf)
                st.session_state["resultado_rappi"] = {"buf": buf_r, "stats": stats_r}
            except Exception as e:
                st.error(f"Error al procesar: {e}")

    if "resultado_rappi" in st.session_state:
        r = st.session_state["resultado_rappi"]
        s = r["stats"]

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)
        st.success("¡Listo! El cruce está generado.")

        falta_fac  = s.get('falta_fac', 0)
        falta_liq2 = s.get('falta_liq2', 0)
        clase_ff   = "warn" if falta_fac  > 0 else "ok"
        clase_fl   = "warn" if falta_liq2 > 0 else "ok"

        st.markdown(f"""
        <div class="metric-row">
            <div class="metric-card ok">
                <div class="metric-value">{s.get('match_pub', 0)}</div>
                <div class="metric-label">Match publicidad</div>
            </div>
            <div class="metric-card ok">
                <div class="metric-value">{s.get('match_srv', 0)}</div>
                <div class="metric-label">Match servicios</div>
            </div>
            <div class="metric-card {clase_ff}">
                <div class="metric-value">{falta_fac}</div>
                <div class="metric-label">Sin factura</div>
            </div>
            <div class="metric-card {clase_fl}">
                <div class="metric-value">{falta_liq2}</div>
                <div class="metric-label">Sin liquidación</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            label="📥 Descargar reporte Rappi",
            data=r["buf"],
            file_name="reporte_rappi.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_rappi"
        )
