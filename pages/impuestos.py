import streamlit as st
from logica_percepciones import correr_cruce_percepciones
from logica_percepciones_pba import correr_cruce_percepciones_pba
from logica_retenciones_iibb_caba import correr_cruce_retenciones
from logica_retenciones_pba import correr_cruce_retenciones_pba

st.set_page_config(
    page_title="Impuestos",
    page_icon="👮",
    layout="centered",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.stApp { background-color: #0f0f0f; color: #e8e8e8; }
.header-block {
    border-left: 3px solid #f5c518;
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
    font-size: 0.72rem; color: #f5c518;
    letter-spacing: 1.5px; text-transform: uppercase;
    margin-bottom: 0.4rem;
}
[data-testid="stFileUploader"] {
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 0.5rem; transition: border-color 0.2s;
}
[data-testid="stFileUploader"]:hover { border-color: #f5c518; }
.stButton > button {
    background: #f5c518 !important; color: #0f0f0f !important;
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
    font-size: 2rem; font-weight: 600; color: #f5c518; line-height: 1;
}
.metric-card .metric-label {
    font-size: 0.7rem; color: #555; text-transform: uppercase;
    letter-spacing: 1px; margin-top: 0.4rem;
    font-family: 'IBM Plex Mono', monospace;
}
.metric-card.error .metric-value { color: #ff4444; }
.metric-card.warn .metric-value { color: #ffaa00; }
.divider { border: none; border-top: 1px solid #1e1e1e; margin: 2rem 0; }
[data-testid="stDownloadButton"] > button {
    background: #1a1a1a !important; color: #e8e8e8 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.8rem !important; border: 1px solid #2a2a2a !important;
    border-radius: 4px !important; width: 100% !important;
    transition: border-color 0.2s !important;
}
[data-testid="stDownloadButton"] > button:hover {
    border-color: #f5c518 !important; color: #f5c518 !important;
}
.back-btn > button {
    background: transparent !important; color: #444 !important;
    border: 1px solid #2a2a2a !important; font-size: 0.75rem !important;
    margin-top: 0 !important; margin-bottom: 1rem !important;
}
.back-btn > button:hover { color: #f5c518 !important; border-color: #f5c518 !important; }
div[data-testid="stTabs"] button {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.8rem !important; color: #555 !important;
}
div[data-testid="stTabs"] button[aria-selected="true"] {
    color: #f5c518 !important;
    border-bottom-color: #f5c518 !important;
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
    <h1>👮 Impuestos</h1>
    <p>Percepciones y retenciones ARCA &nbsp;×&nbsp; Sistema Interno</p>
</div>
""", unsafe_allow_html=True)

tab_percepciones, tab_percepciones_pba, tab_retenciones, tab_retenciones_pba = st.tabs([
    "👮  Percepciones IIBB CABA",
    "👮  Percepciones IIBB PBA",
    "👮  Retenciones IIBB CABA",
    "👮  Retenciones IIBB PBA",
])


# ═══════════════════════════════════════════════
# TAB PERCEPCIONES IIBB CABA
# ═══════════════════════════════════════════════
with tab_percepciones:
    st.markdown("<br>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="upload-label">Excel ARCA (percepciones)</div>', unsafe_allow_html=True)
        archivo_arca = st.file_uploader("arca_percep", type=["xlsx", "xls"],
                                         label_visibility="collapsed", key="percep_arca")
    with col2:
        st.markdown('<div class="upload-label">Excel Sistema (mayor)</div>', unsafe_allow_html=True)
        archivo_sistema = st.file_uploader("sistema_percep", type=["xlsx", "xls"],
                                            label_visibility="collapsed", key="percep_sistema")

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    tol = st.slider("Tolerancia de importes ($ ±)", min_value=0.0, max_value=10.0, value=1.0, step=0.5,
                     key="percep_tol")

    ambos_cargados = archivo_arca is not None and archivo_sistema is not None
    if not ambos_cargados:
        st.info("Cargá los dos archivos Excel para habilitar el cruce.")

    boton = st.button("CRUZAR PERCEPCIONES", disabled=not ambos_cargados, use_container_width=True,
                       key="btn_percepciones")

    if boton and ambos_cargados:
        with st.spinner("Procesando..."):
            try:
                buf_reporte, stats = correr_cruce_percepciones(
                    archivo_arca=archivo_arca,
                    archivo_sistema=archivo_sistema,
                    tolerancia_importe=tol,
                )
                st.session_state["resultado_percepciones"] = {
                    "buf_reporte": buf_reporte,
                    "stats": stats,
                }
            except Exception as e:
                st.error(f"Error al procesar: {e}")
                st.stop()

    if "resultado_percepciones" in st.session_state:
        r = st.session_state["resultado_percepciones"]
        stats = r["stats"]

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)

        clase_falt_a = "error" if stats["faltante_arca"] > 0 else "metric-card"
        clase_falt_s = "error" if stats["faltante_sistema"] > 0 else "metric-card"
        clase_nuevos = "warn" if stats["proveedores_nuevos"] > 0 else "metric-card"

        st.markdown(f"""
        <div class="metric-row">
            <div class="metric-card">
                <div class="metric-value">{stats['match']}</div>
                <div class="metric-label">Con match</div>
            </div>
            <div class="metric-card {clase_falt_a}">
                <div class="metric-value">{stats['faltante_arca']}</div>
                <div class="metric-label">Faltante ARCA</div>
            </div>
            <div class="metric-card {clase_falt_s}">
                <div class="metric-value">{stats['faltante_sistema']}</div>
                <div class="metric-label">Faltante sistema</div>
            </div>
            <div class="metric-card {clase_nuevos}">
                <div class="metric-value">{stats['proveedores_nuevos']}</div>
                <div class="metric-label">Proveedores nuevos</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        if stats["proveedores_nuevos"] > 0:
            st.warning(
                "Hay proveedores que matchearon por nombre y no están en el padrón (proveedores.py). "
                "Revisá la hoja 'Proveedores_Nuevos' del reporte y agregalos al archivo."
            )

        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            label="📥 Descargar reporte completo",
            data=r["buf_reporte"],
            file_name="cruce_percepciones.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_percepciones",
        )


# ═══════════════════════════════════════════════
# TAB PERCEPCIONES IIBB PBA
# ═══════════════════════════════════════════════
with tab_percepciones_pba:
    st.markdown("<br>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="upload-label">Archivo ARCA (.txt)</div>', unsafe_allow_html=True)
        archivo_arca_pba = st.file_uploader("arca_percep_pba", type=["txt"],
                                             label_visibility="collapsed", key="percep_pba_arca")
    with col2:
        st.markdown('<div class="upload-label">Excel Sistema (mayor)</div>', unsafe_allow_html=True)
        archivo_sistema_pba = st.file_uploader("sistema_percep_pba", type=["xlsx", "xls"],
                                                label_visibility="collapsed", key="percep_pba_sistema")

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    tol_pba = st.slider("Tolerancia de importes ($ ±)", min_value=0.0, max_value=10.0, value=1.0, step=0.5,
                         key="percep_pba_tol")

    ambos_cargados_pba = archivo_arca_pba is not None and archivo_sistema_pba is not None
    if not ambos_cargados_pba:
        st.info("Cargá el archivo de ARCA (.txt) y el Excel del sistema para habilitar el cruce.")

    boton_pba = st.button("CRUZAR PERCEPCIONES", disabled=not ambos_cargados_pba, use_container_width=True,
                           key="btn_percepciones_pba")

    if boton_pba and ambos_cargados_pba:
        with st.spinner("Procesando..."):
            try:
                buf_reporte_pba, stats_pba = correr_cruce_percepciones_pba(
                    archivo_arca_txt=archivo_arca_pba,
                    archivo_sistema=archivo_sistema_pba,
                    tolerancia_importe=tol_pba,
                )
                st.session_state["resultado_percepciones_pba"] = {
                    "buf_reporte": buf_reporte_pba,
                    "stats": stats_pba,
                }
            except Exception as e:
                st.error(f"Error al procesar: {e}")
                st.stop()

    if "resultado_percepciones_pba" in st.session_state:
        r_pba = st.session_state["resultado_percepciones_pba"]
        stats_pba = r_pba["stats"]

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)

        clase_falt_a_pba = "error" if stats_pba["faltante_arca"] > 0 else "metric-card"
        clase_falt_s_pba = "error" if stats_pba["faltante_sistema"] > 0 else "metric-card"
        clase_nuevos_pba = "warn" if stats_pba["proveedores_nuevos"] > 0 else "metric-card"

        st.markdown(f"""
        <div class="metric-row">
            <div class="metric-card">
                <div class="metric-value">{stats_pba['match']}</div>
                <div class="metric-label">Con match</div>
            </div>
            <div class="metric-card {clase_falt_a_pba}">
                <div class="metric-value">{stats_pba['faltante_arca']}</div>
                <div class="metric-label">Faltante ARCA</div>
            </div>
            <div class="metric-card {clase_falt_s_pba}">
                <div class="metric-value">{stats_pba['faltante_sistema']}</div>
                <div class="metric-label">Faltante sistema</div>
            </div>
            <div class="metric-card {clase_nuevos_pba}">
                <div class="metric-value">{stats_pba['proveedores_nuevos']}</div>
                <div class="metric-label">Proveedores nuevos</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        if stats_pba["proveedores_nuevos"] > 0:
            st.warning(
                "Hay proveedores que matchearon por nombre y no están en el padrón (proveedores.py). "
                "Revisá la hoja 'Proveedores_Nuevos' del reporte y agregalos al archivo."
            )

        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            label="📥 Descargar reporte completo",
            data=r_pba["buf_reporte"],
            file_name="cruce_percepciones_pba.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_percepciones_pba",
        )


# ═══════════════════════════════════════════════
# TAB RETENCIONES IIBB CABA
# ═══════════════════════════════════════════════
with tab_retenciones:
    st.markdown("<br>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="upload-label">Excel ARCA (retenciones)</div>', unsafe_allow_html=True)
        archivo_arca_ret = st.file_uploader("arca_ret", type=["xlsx", "xls"],
                                             label_visibility="collapsed", key="ret_arca")
    with col2:
        st.markdown('<div class="upload-label">Excel Sistema (mayor)</div>', unsafe_allow_html=True)
        archivo_sistema_ret = st.file_uploader("sistema_ret", type=["xlsx", "xls"],
                                                label_visibility="collapsed", key="ret_sistema")

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    tol_ret = st.slider("Tolerancia de importes ($ ±)", min_value=0.0, max_value=10.0, value=1.0, step=0.5,
                         key="ret_tol")

    ambos_cargados_ret = archivo_arca_ret is not None and archivo_sistema_ret is not None
    if not ambos_cargados_ret:
        st.info("Cargá los dos archivos Excel para habilitar el cruce.")

    boton_ret = st.button("CRUZAR RETENCIONES", disabled=not ambos_cargados_ret, use_container_width=True,
                           key="btn_retenciones")

    if boton_ret and ambos_cargados_ret:
        with st.spinner("Procesando..."):
            try:
                buf_reporte_ret, stats_ret = correr_cruce_retenciones(
                    archivo_arca=archivo_arca_ret,
                    archivo_sistema=archivo_sistema_ret,
                    tolerancia_importe=tol_ret,
                )
                st.session_state["resultado_retenciones"] = {
                    "buf_reporte": buf_reporte_ret,
                    "stats": stats_ret,
                }
            except Exception as e:
                st.error(f"Error al procesar: {e}")
                st.stop()

    if "resultado_retenciones" in st.session_state:
        r_ret = st.session_state["resultado_retenciones"]
        stats_ret = r_ret["stats"]

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)

        clase_falt_a_ret = "error" if stats_ret["faltante_arca"] > 0 else "metric-card"
        clase_falt_s_ret = "error" if stats_ret["faltante_sistema"] > 0 else "metric-card"
        clase_nuevos_ret = "warn" if stats_ret["proveedores_nuevos"] > 0 else "metric-card"

        st.markdown(f"""
        <div class="metric-row">
            <div class="metric-card">
                <div class="metric-value">{stats_ret['match']}</div>
                <div class="metric-label">Con match</div>
            </div>
            <div class="metric-card {clase_falt_a_ret}">
                <div class="metric-value">{stats_ret['faltante_arca']}</div>
                <div class="metric-label">Faltante ARCA</div>
            </div>
            <div class="metric-card {clase_falt_s_ret}">
                <div class="metric-value">{stats_ret['faltante_sistema']}</div>
                <div class="metric-label">Faltante sistema</div>
            </div>
            <div class="metric-card {clase_nuevos_ret}">
                <div class="metric-value">{stats_ret['proveedores_nuevos']}</div>
                <div class="metric-label">Proveedores nuevos</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        if stats_ret["proveedores_nuevos"] > 0:
            st.warning(
                "Hay proveedores que matchearon por nombre y no están en el padrón (proveedores.py). "
                "Revisá la hoja 'Proveedores_Nuevos' del reporte y agregalos al archivo."
            )

        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            label="📥 Descargar reporte completo",
            data=r_ret["buf_reporte"],
            file_name="cruce_retenciones.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_retenciones",
        )


# ═══════════════════════════════════════════════
# TAB RETENCIONES IIBB PBA (SIRTAC)
# ═══════════════════════════════════════════════
with tab_retenciones_pba:
    st.markdown("<br>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="upload-label">Archivo SIRTAC (.txt)</div>', unsafe_allow_html=True)
        archivo_sirtac_pba = st.file_uploader("sirtac_ret_pba", type=["txt"],
                                               label_visibility="collapsed", key="ret_pba_sirtac")
    with col2:
        st.markdown('<div class="upload-label">Excel Sistema (mayor)</div>', unsafe_allow_html=True)
        archivo_sistema_ret_pba = st.file_uploader("sistema_ret_pba", type=["xlsx", "xls"],
                                                     label_visibility="collapsed", key="ret_pba_sistema")

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    tol_ret_pba = st.slider("Tolerancia de importes ($ ±)", min_value=0.0, max_value=10.0, value=1.0, step=0.5,
                             key="ret_pba_tol")

    ambos_cargados_ret_pba = archivo_sirtac_pba is not None and archivo_sistema_ret_pba is not None
    if not ambos_cargados_ret_pba:
        st.info("Cargá el archivo SIRTAC (.txt) y el Excel del sistema para habilitar el cruce.")

    boton_ret_pba = st.button("CRUZAR RETENCIONES", disabled=not ambos_cargados_ret_pba, use_container_width=True,
                               key="btn_retenciones_pba")

    if boton_ret_pba and ambos_cargados_ret_pba:
        with st.spinner("Procesando..."):
            try:
                buf_reporte_ret_pba, stats_ret_pba = correr_cruce_retenciones_pba(
                    archivo_sirtac_txt=archivo_sirtac_pba,
                    archivo_sistema=archivo_sistema_ret_pba,
                    tolerancia_importe=tol_ret_pba,
                )
                st.session_state["resultado_retenciones_pba"] = {
                    "buf_reporte": buf_reporte_ret_pba,
                    "stats": stats_ret_pba,
                }
            except Exception as e:
                st.error(f"Error al procesar: {e}")
                st.stop()

    if "resultado_retenciones_pba" in st.session_state:
        r_ret_pba = st.session_state["resultado_retenciones_pba"]
        stats_ret_pba = r_ret_pba["stats"]

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)

        clase_falt_a_ret_pba = "error" if stats_ret_pba["faltante_arca"] > 0 else "metric-card"
        clase_falt_s_ret_pba = "error" if stats_ret_pba["faltante_sistema"] > 0 else "metric-card"
        clase_nuevos_ret_pba = "warn" if stats_ret_pba["proveedores_nuevos"] > 0 else "metric-card"

        st.markdown(f"""
        <div class="metric-row">
            <div class="metric-card">
                <div class="metric-value">{stats_ret_pba['match']}</div>
                <div class="metric-label">Con match</div>
            </div>
            <div class="metric-card {clase_falt_a_ret_pba}">
                <div class="metric-value">{stats_ret_pba['faltante_arca']}</div>
                <div class="metric-label">Faltante ARCA</div>
            </div>
            <div class="metric-card {clase_falt_s_ret_pba}">
                <div class="metric-value">{stats_ret_pba['faltante_sistema']}</div>
                <div class="metric-label">Faltante sistema</div>
            </div>
            <div class="metric-card {clase_nuevos_ret_pba}">
                <div class="metric-value">{stats_ret_pba['proveedores_nuevos']}</div>
                <div class="metric-label">Proveedores nuevos</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        if stats_ret_pba["proveedores_nuevos"] > 0:
            st.warning(
                "Hay proveedores que matchearon por nombre y no están en el padrón (proveedores.py). "
                "Revisá la hoja 'Proveedores_Nuevos' del reporte y agregalos al archivo."
            )

        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            label="📥 Descargar reporte completo",
            data=r_ret_pba["buf_reporte"],
            file_name="cruce_retenciones_pba.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_retenciones_pba",
        )
