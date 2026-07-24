import streamlit as st
from logica_rappi import correr_rappi
from logica_atalaya import correr_atalaya

st.set_page_config(
    page_title="Rappi",
    page_icon="🛵",
    layout="centered",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.stApp { background-color: #0f0f0f; color: #e8e8e8; }

.header-block {
    border-left: 3px solid #FF441F;
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
    font-size: 0.72rem; color: #FF441F;
    letter-spacing: 1.5px; text-transform: uppercase;
    margin-bottom: 0.4rem;
}
[data-testid="stFileUploader"] {
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 0.8rem; transition: border-color 0.2s;
}
[data-testid="stFileUploader"]:hover { border-color: #FF441F; }

.stButton > button {
    background: #FF441F !important; color: #0f0f0f !important;
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
    text-align: center; margin: 1.5rem 0;
}
.counter-box .counter-num {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 2.5rem; font-weight: 600;
    color: #FF441F; line-height: 1;
}
.counter-box .counter-label {
    font-size: 0.75rem; color: #555;
    text-transform: uppercase; letter-spacing: 1px;
    margin-top: 0.4rem; font-family: 'IBM Plex Mono', monospace;
}

.metric-row { display: flex; gap: 1rem; margin: 1.5rem 0; flex-wrap: wrap; }
.metric-card {
    flex: 1; min-width: 80px; background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 1rem; text-align: center;
}
.metric-card .metric-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 2rem; font-weight: 600; color: #FF441F; line-height: 1;
}
.metric-card .metric-label {
    font-size: 0.7rem; color: #555; text-transform: uppercase;
    letter-spacing: 1px; margin-top: 0.4rem;
    font-family: 'IBM Plex Mono', monospace;
}
.metric-card.warn .metric-value { color: #facc15; }
.metric-card.ok   .metric-value { color: #4ade80; }

.liq-card {
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 0.9rem 1.2rem; margin: 0.5rem 0;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.78rem;
}
.liq-card .liq-id { color: #FF441F; font-weight: 600; font-size: 0.85rem; }
.liq-card .liq-detail { color: #888; margin-top: 0.3rem; }
.liq-card.warn { border-color: #facc1555; }

.divider { border: none; border-top: 1px solid #1e1e1e; margin: 2rem 0; }

[data-testid="stDownloadButton"] > button {
    background: #1a1a1a !important; color: #e8e8e8 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.8rem !important; border: 1px solid #2a2a2a !important;
    border-radius: 4px !important; width: 100% !important;
    transition: border-color 0.2s !important;
}
[data-testid="stDownloadButton"] > button:hover {
    border-color: #FF441F !important; color: #FF441F !important;
}

.back-btn > button {
    background: transparent !important; color: #444 !important;
    border: 1px solid #2a2a2a !important; font-size: 0.75rem !important;
    margin-top: 0 !important; margin-bottom: 1rem !important;
}
.back-btn > button:hover { color: #FF441F !important; border-color: #FF441F !important; }

.pdf-list {
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 1rem 1.2rem;
    margin: 1rem 0; max-height: 200px; overflow-y: auto;
}
.pdf-item {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem; color: #888;
    padding: 0.2rem 0; border-bottom: 1px solid #222;
}
.pdf-item:last-child { border-bottom: none; }
.pdf-item::before { content: "📄 "; }

div[data-testid="stTabs"] button {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.8rem !important; color: #555 !important;
}
div[data-testid="stTabs"] button[aria-selected="true"] {
    color: #FF441F !important; border-bottom-color: #FF441F !important;
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
    <h1>🛵 Rappi</h1>
    <p>Liquidaciones y conciliación de ventas Rappi</p>
</div>
""", unsafe_allow_html=True)

tab_liquidaciones, tab_atalaya = st.tabs(["📑  Liquidaciones Rappi", "🏪  Atalaya"])


# ═══════════════════════════════════════════════
# TAB LIQUIDACIONES RAPPI
# ═══════════════════════════════════════════════
with tab_liquidaciones:
    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown('<div class="upload-label">Liquidaciones Rappi (Excel — una o más)</div>', unsafe_allow_html=True)
    archivos_liq = st.file_uploader(
        "rappi_liq",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
        label_visibility="collapsed",
        key="rappi_liquidaciones"
    )

    if archivos_liq:
        st.markdown(f"""
        <div class="counter-box">
            <div class="counter-num">{len(archivos_liq)}</div>
            <div class="counter-label">liquidación{"es" if len(archivos_liq) != 1 else ""} cargada{"s" if len(archivos_liq) != 1 else ""}</div>
        </div>
        """, unsafe_allow_html=True)

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
        items = "".join(f'<div class="pdf-item">{a.name}</div>' for a in archivos_pdf)
        st.markdown(f'<div class="pdf-list">{items}</div>', unsafe_allow_html=True)

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    todo_ok = bool(archivos_liq and archivos_pdf)
    if not archivos_liq:
        st.info("Cargá al menos una liquidación Excel de Rappi.")
    elif not archivos_pdf:
        st.info("Cargá al menos una factura PDF de Rappi.")

    boton_rappi = st.button(
        "CRUZAR FACTURAS vs LIQUIDACIONES",
        disabled=not todo_ok,
        use_container_width=True,
        key="btn_rappi"
    )

    if boton_rappi and todo_ok:
        with st.spinner("Procesando Rappi..."):
            try:
                zip_buf, stats = correr_rappi(archivos_liq, archivos_pdf)
                st.session_state["resultado_rappi"] = {"zip": zip_buf, "stats": stats}
            except Exception as e:
                st.error(f"Error al procesar: {e}")

    if "resultado_rappi" in st.session_state:
        r = st.session_state["resultado_rappi"]
        s = r["stats"]

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)
        st.success("¡Listo! El cruce está generado.")

        st.markdown(f"""
        <div class="metric-row">
            <div class="metric-card ok">
                <div class="metric-value">{s['n_liquidaciones']}</div>
                <div class="metric-label">Liquidaciones</div>
            </div>
            <div class="metric-card ok">
                <div class="metric-value">{s['n_facturas']}</div>
                <div class="metric-label">Facturas</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        for d in s['detalle']:
            clase = "liq-card warn" if d['falta'] > 0 else "liq-card"
            facturas_str = ', '.join(d['facturas']) if d['facturas'] else '—'
            st.markdown(f"""
            <div class="{clase}">
                <div class="liq-id">ID Pago: {d['id_pago']}</div>
                <div class="liq-detail">✅ {d['match']} match &nbsp;|&nbsp; ⚠️ {d['falta']} sin factura</div>
                <div class="liq-detail">Facturas: {facturas_str}</div>
            </div>
            """, unsafe_allow_html=True)

        if s['advertencias']:
            for adv in s['advertencias']:
                st.warning(adv)

        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            label="📥 Descargar resultados Rappi (.zip)",
            data=r["zip"],
            file_name="resultados_rappi.zip",
            mime="application/zip",
            use_container_width=True,
            key="dl_rappi"
        )


# ═══════════════════════════════════════════════
# TAB ATALAYA
# ═══════════════════════════════════════════════
with tab_atalaya:
    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown('<div class="upload-label">Extracto Rappi (Excel)</div>', unsafe_allow_html=True)
    archivo_rappi = st.file_uploader(
        "atalaya_rappi",
        type=["xlsx", "xls"],
        accept_multiple_files=False,
        label_visibility="collapsed",
        key="atalaya_rappi_extracto"
    )

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="upload-label">Extracto Atalaya (Excel)</div>', unsafe_allow_html=True)
    archivo_atalaya = st.file_uploader(
        "atalaya_atalaya",
        type=["xlsx", "xls"],
        accept_multiple_files=False,
        label_visibility="collapsed",
        key="atalaya_atalaya_extracto"
    )

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    todo_ok_atalaya = bool(archivo_rappi and archivo_atalaya)
    if not archivo_rappi:
        st.info("Cargá el extracto Excel de Rappi.")
    elif not archivo_atalaya:
        st.info("Cargá el extracto Excel de Atalaya.")

    boton_atalaya = st.button(
        "CRUZAR RAPPI vs ATALAYA",
        disabled=not todo_ok_atalaya,
        use_container_width=True,
        key="btn_atalaya"
    )

    if boton_atalaya and todo_ok_atalaya:
        with st.spinner("Procesando cruce Rappi vs Atalaya..."):
            try:
                buf, stats = correr_atalaya(archivo_rappi, archivo_atalaya)
                st.session_state["resultado_atalaya"] = {"buf": buf, "stats": stats}
            except Exception as e:
                st.error(f"Error al procesar: {e}")

    if "resultado_atalaya" in st.session_state:
        r = st.session_state["resultado_atalaya"]
        s = r["stats"]

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)
        st.success("¡Listo! El cruce está generado.")

        st.markdown(f"""
        <div class="metric-row">
            <div class="metric-card ok">
                <div class="metric-value">{s['match']}</div>
                <div class="metric-label">Match</div>
            </div>
            <div class="metric-card warn">
                <div class="metric-value">{s['falta_atalaya']}</div>
                <div class="metric-label">Falta atalaya</div>
            </div>
            <div class="metric-card warn">
                <div class="metric-value">{s['falta_rappi']}</div>
                <div class="metric-label">Falta rappi</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            label="📥 Descargar cruce Rappi vs Atalaya (.xlsx)",
            data=r["buf"],
            file_name="cruce_rappi_atalaya.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_atalaya"
        )
