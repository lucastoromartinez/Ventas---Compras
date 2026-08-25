import streamlit as st
from logica_galicia     import correr_conciliacion_galicia
from logica_hipotecario import correr_conciliacion_hipotecario
from logica_cupones     import correr_conciliacion_cupones_plantilla
from logica_fiser       import correr_conciliacion_fiser

st.set_page_config(
    page_title="Conciliaciones Bancarias",
    page_icon="🏦",
    layout="centered",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.stApp { background-color: #0f0f0f; color: #e8e8e8; }

.header-block {
    border-left: 3px solid #ff6b35;
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
    font-size: 0.72rem; color: #ff6b35;
    letter-spacing: 1.5px; text-transform: uppercase;
    margin-bottom: 0.4rem;
}
.upload-label.optional { color: #888; }
.section-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.68rem; color: #444;
    letter-spacing: 2px; text-transform: uppercase;
    margin: 1.2rem 0 0.8rem 0;
}
[data-testid="stFileUploader"] {
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 0.5rem; transition: border-color 0.2s;
}
[data-testid="stFileUploader"]:hover { border-color: #ff6b35; }
.stButton > button {
    background: #ff6b35 !important; color: #0f0f0f !important;
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
    font-size: 2rem; font-weight: 600; color: #ff6b35; line-height: 1;
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
}
[data-testid="stDownloadButton"] > button:hover {
    border-color: #ff6b35 !important; color: #ff6b35 !important;
}
.back-btn > button {
    background: transparent !important; color: #444 !important;
    border: 1px solid #2a2a2a !important; font-size: 0.75rem !important;
    margin-top: 0 !important; margin-bottom: 1rem !important;
}
.back-btn > button:hover { color: #ff6b35 !important; border-color: #ff6b35 !important; }
div[data-testid="stTabs"] button {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.8rem !important; color: #555 !important;
}
div[data-testid="stTabs"] button[aria-selected="true"] {
    color: #ff6b35 !important; border-bottom-color: #ff6b35 !important;
}
.toggle-box {
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 1rem 1.2rem;
    margin: 1rem 0;
}
</style>
""", unsafe_allow_html=True)

# Botón volver
st.markdown('<div class="back-btn">', unsafe_allow_html=True)
if st.button("← Volver al inicio"):
    st.switch_page(st.session_state["_pages"]["home"])
st.markdown('</div>', unsafe_allow_html=True)

st.markdown("""
<div class="header-block">
    <h1>Conciliaciones Bancarias</h1>
    <p>Mayor &nbsp;×&nbsp; Extracto Bancario</p>
</div>
""", unsafe_allow_html=True)

tab_galicia, tab_hipotecario, tab_cupones, tab_fiser = st.tabs(
    ["🏦  Banco Galicia", "🏦  Banco Hipotecario", "🏦  Cupones", "🏦  Fiser"]
)


# ═══════════════════════════════════════════════
# TAB GALICIA
# ═══════════════════════════════════════════════
with tab_galicia:
    st.markdown("<br>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="upload-label">Mayor</div>', unsafe_allow_html=True)
        archivo_mayor_g = st.file_uploader("mayor_g", type=["xlsx","xls"],
                                            label_visibility="collapsed", key="mayor_g")
    with col2:
        st.markdown('<div class="upload-label">Extracto Galicia</div>', unsafe_allow_html=True)
        archivo_extracto_g = st.file_uploader("extracto_g", type=["xlsx","xls"],
                                               label_visibility="collapsed", key="extracto_g")

    # Toggle pagos masivos
    st.markdown("<hr class='divider'>", unsafe_allow_html=True)
    st.markdown('<div class="toggle-box">', unsafe_allow_html=True)
    con_masivos = st.toggle("Incluir Pagos Masivos", value=False, key="toggle_masivos")
    st.markdown('</div>', unsafe_allow_html=True)

    archivo_prov_g = None
    if con_masivos:
        st.markdown('<div class="upload-label optional">Proveedores Días Masivos</div>', unsafe_allow_html=True)
        archivo_prov_g = st.file_uploader("prov_g", type=["xlsx","xls"],
                                           label_visibility="collapsed", key="prov_g")

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    # Validación: mayor + extracto siempre requeridos; proveedores solo si toggle activo
    base_ok    = all([archivo_mayor_g, archivo_extracto_g])
    masivos_ok = (not con_masivos) or (con_masivos and archivo_prov_g)
    todo_ok_g  = base_ok and masivos_ok

    if not base_ok:
        st.info("Cargá el Mayor y el Extracto para habilitar la conciliación.")
    elif con_masivos and not archivo_prov_g:
        st.info("Cargá el archivo de Proveedores Días Masivos para continuar.")

    boton_g = st.button("CONCILIAR GALICIA", disabled=not todo_ok_g,
                         use_container_width=True, key="btn_galicia")

    if boton_g and todo_ok_g:
        with st.spinner("Procesando Galicia..."):
            try:
                buf_g, stats_g = correr_conciliacion_galicia(
                    archivo_mayor=archivo_mayor_g,
                    archivo_extracto=archivo_extracto_g,
                    archivo_proveedores=archivo_prov_g,  # None si no hay masivos
                )
                st.session_state["resultado_galicia"] = {"buf": buf_g, "stats": stats_g}
            except Exception as e:
                st.error(f"Error al procesar Galicia: {e}")

    if "resultado_galicia" in st.session_state:
        r = st.session_state["resultado_galicia"]
        s = r["stats"]

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)

        if s.get("con_masivos"):
            st.success("✅ Conciliación con Pagos Masivos incluidos")
        else:
            st.info("ℹ️ Conciliación sin Pagos Masivos")

        clase_fm = "error" if s["falta_mayor"]    > 0 else "metric-card"
        clase_fe = "error" if s["falta_extracto"] > 0 else "metric-card"

        st.markdown(f"""
        <div class="metric-row">
            <div class="metric-card">
                <div class="metric-value">{s['match_exacto']}</div>
                <div class="metric-label">Match exacto</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{s['match_tolerancia']}</div>
                <div class="metric-label">Match tolerancia</div>
            </div>
            <div class="metric-card {clase_fm}">
                <div class="metric-value">{s['falta_mayor']}</div>
                <div class="metric-label">Faltante mayor</div>
            </div>
            <div class="metric-card {clase_fe}">
                <div class="metric-value">{s['falta_extracto']}</div>
                <div class="metric-label">Faltante extracto</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            label="📥 Descargar reporte Galicia",
            data=r["buf"],
            file_name="conciliacion_galicia.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )


# ═══════════════════════════════════════════════
# TAB HIPOTECARIO
# ═══════════════════════════════════════════════
with tab_hipotecario:
    st.markdown("<br>", unsafe_allow_html=True)

    col4, col5 = st.columns(2)
    with col4:
        st.markdown('<div class="upload-label">Mayor</div>', unsafe_allow_html=True)
        archivo_mayor_h = st.file_uploader("mayor_h", type=["xlsx","xls"],
                                            label_visibility="collapsed", key="mayor_h")
    with col5:
        st.markdown('<div class="upload-label">Extracto Hipotecario</div>', unsafe_allow_html=True)
        archivo_extracto_h = st.file_uploader("extracto_h", type=["xlsx","xls"],
                                               label_visibility="collapsed", key="extracto_h")

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    todos_h = all([archivo_mayor_h, archivo_extracto_h])
    if not todos_h:
        st.info("Cargá los dos archivos para habilitar la conciliación.")

    boton_h = st.button("CONCILIAR HIPOTECARIO", disabled=not todos_h,
                         use_container_width=True, key="btn_hipotecario")

    if boton_h and todos_h:
        with st.spinner("Procesando Hipotecario..."):
            try:
                buf_h, stats_h = correr_conciliacion_hipotecario(
                    archivo_mayor=archivo_mayor_h,
                    archivo_extracto=archivo_extracto_h,
                )
                st.session_state["resultado_hipotecario"] = {"buf": buf_h, "stats": stats_h}
            except Exception as e:
                st.error(f"Error al procesar Hipotecario: {e}")

    if "resultado_hipotecario" in st.session_state:
        r = st.session_state["resultado_hipotecario"]
        s = r["stats"]

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)

        clase_fm = "error" if s["falta_mayor"]    > 0 else "metric-card"
        clase_fe = "error" if s["falta_extracto"] > 0 else "metric-card"

        st.markdown(f"""
        <div class="metric-row">
            <div class="metric-card">
                <div class="metric-value">{s['match_exacto']}</div>
                <div class="metric-label">Match exacto</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{s['match_tolerancia']}</div>
                <div class="metric-label">Match tolerancia</div>
            </div>
            <div class="metric-card {clase_fm}">
                <div class="metric-value">{s['falta_mayor']}</div>
                <div class="metric-label">Faltante mayor</div>
            </div>
            <div class="metric-card {clase_fe}">
                <div class="metric-value">{s['falta_extracto']}</div>
                <div class="metric-label">Faltante extracto</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            label="📥 Descargar reporte Hipotecario",
            data=r["buf"],
            file_name="conciliacion_hipotecario.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )


# ═══════════════════════════════════════════════
# TAB CUPONES
# ═══════════════════════════════════════════════
with tab_cupones:
    st.markdown("<br>", unsafe_allow_html=True)

    con_anterior = st.checkbox(
        "También tengo el mes anterior (para resolver pendientes y diferencias)",
        key="cupones_con_anterior",
    )

    col6, col7 = st.columns(2)
    with col6:
        st.markdown('<div class="upload-label">Extracto Banco (mes actual)</div>', unsafe_allow_html=True)
        archivo_banco_c = st.file_uploader("banco_c", type=["xlsx","xls"],
                                            label_visibility="collapsed", key="banco_c")
    with col7:
        st.markdown('<div class="upload-label">Reporte Nave (mes actual)</div>', unsafe_allow_html=True)
        archivo_nave_c = st.file_uploader("nave_c", type=["xlsx","xls"],
                                           label_visibility="collapsed", key="nave_c")

    archivo_banco_ant = archivo_nave_ant = archivo_nave_acred_mes = None
    if con_anterior:
        col8, col9 = st.columns(2)
        with col8:
            st.markdown('<div class="upload-label">Extracto Banco (mes anterior)</div>', unsafe_allow_html=True)
            archivo_banco_ant = st.file_uploader("banco_ant", type=["xlsx","xls"],
                                                  label_visibility="collapsed", key="banco_ant")
        with col9:
            st.markdown('<div class="upload-label">Reporte Nave (mes anterior)</div>', unsafe_allow_html=True)
            archivo_nave_ant = st.file_uploader("nave_ant", type=["xlsx","xls"],
                                                 label_visibility="collapsed", key="nave_ant")

        con_acred_mes = st.checkbox(
            "También tengo el reporte Nave por fecha de acreditación del mes actual "
            "(resuelve cupones pendientes del mes anterior que se acreditaron dentro de un grupo nuevo)",
            key="cupones_con_acred_mes",
        )
        if con_acred_mes:
            st.markdown('<div class="upload-label">Reporte Nave por fecha de acreditación (mes actual)</div>',
                        unsafe_allow_html=True)
            archivo_nave_acred_mes = st.file_uploader("nave_acred_mes", type=["xlsx","xls"],
                                                        label_visibility="collapsed", key="nave_acred_mes")

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    if con_anterior:
        requeridos = [archivo_banco_c, archivo_nave_c, archivo_banco_ant, archivo_nave_ant]
        if con_acred_mes:
            requeridos.append(archivo_nave_acred_mes)
        todos_c = all(requeridos)
        if not todos_c:
            st.info("Cargá los archivos requeridos (mes actual, mes anterior"
                    + (" y Nave por fecha de acreditación" if con_acred_mes else "")
                    + ") para habilitar la conciliación.")
    else:
        todos_c = all([archivo_banco_c, archivo_nave_c])
        if not todos_c:
            st.info("Cargá el Extracto Banco y el Reporte Nave para habilitar la conciliación.")

    boton_c = st.button("CONCILIAR CUPONES", disabled=not todos_c,
                         use_container_width=True, key="btn_cupones")

    if boton_c and todos_c:
        with st.spinner("Procesando Cupones..."):
            try:
                buf_c, stats_c = correr_conciliacion_cupones_plantilla(
                    archivo_banco_actual=archivo_banco_c,
                    archivo_nave_actual=archivo_nave_c,
                    archivo_banco_anterior=archivo_banco_ant,
                    archivo_nave_anterior=archivo_nave_ant,
                    archivo_nave_acred_mes=archivo_nave_acred_mes,
                )
                st.session_state["resultado_cupones"] = {"buf": buf_c, "stats": stats_c}
            except Exception as e:
                st.error(f"Error al procesar Cupones: {e}")

    if "resultado_cupones" in st.session_state:
        r = st.session_state["resultado_cupones"]
        s = r["stats"]

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)

        clase_bsn = "error" if s["banco_sin_nave"] > 0 else "metric-card"
        clase_dif = "error" if s["diferencia_banco_vs_nave"] > 0 else "metric-card"

        titulo_meses = s["mes_actual"]
        if s["mes_anterior"]:
            titulo_meses += f" (con pendientes de {s['mes_anterior']})"
        st.caption(titulo_meses)

        st.markdown(f"""
        <div class="metric-row">
            <div class="metric-card">
                <div class="metric-value">{s['conciliado']}</div>
                <div class="metric-label">Conciliado</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{s['mes_anterior_acreditado']}</div>
                <div class="metric-label">Mes anterior acreditado</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{s['pendiente_actual']}</div>
                <div class="metric-label">Pendiente por acreditar</div>
            </div>
            <div class="metric-card {clase_dif}">
                <div class="metric-value">{s['diferencia_banco_vs_nave']}</div>
                <div class="metric-label">Diferencia Banco vs Nave</div>
            </div>
            <div class="metric-card {clase_bsn}">
                <div class="metric-value">{s['banco_sin_nave']}</div>
                <div class="metric-label">Banco sin Nave / revisar</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            label="📥 Descargar reporte Cupones",
            data=r["buf"],
            file_name="conciliacion_cupones.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )


# ═══════════════════════════════════════════════
# TAB FISER
# ═══════════════════════════════════════════════
with tab_fiser:
    st.markdown("<br>", unsafe_allow_html=True)

    col10, col11 = st.columns(2)
    with col10:
        st.markdown('<div class="upload-label">Extracto Banco</div>', unsafe_allow_html=True)
        archivo_banco_f = st.file_uploader("banco_f", type=["xlsx","xls"],
                                            label_visibility="collapsed", key="banco_f")
    with col11:
        st.markdown('<div class="upload-label">Reporte Fiser</div>', unsafe_allow_html=True)
        archivo_fiser_f = st.file_uploader("fiser_f", type=["xlsx","xls"],
                                            label_visibility="collapsed", key="fiser_f")

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    todos_f = all([archivo_banco_f, archivo_fiser_f])
    if not todos_f:
        st.info("Cargá el Extracto Banco y el Reporte Fiser para habilitar la conciliación.")

    boton_f = st.button("CONCILIAR FISER", disabled=not todos_f,
                         use_container_width=True, key="btn_fiser")

    if boton_f and todos_f:
        with st.spinner("Procesando Fiser..."):
            try:
                buf_f, stats_f = correr_conciliacion_fiser(
                    archivo_banco=archivo_banco_f,
                    archivo_fiser=archivo_fiser_f,
                )
                st.session_state["resultado_fiser"] = {"buf": buf_f, "stats": stats_f}
            except Exception as e:
                st.error(f"Error al procesar Fiser: {e}")

    if "resultado_fiser" in st.session_state:
        r = st.session_state["resultado_fiser"]
        s = r["stats"]

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)

        clase_fb = "error" if s["falta_banco"] > 0 else "metric-card"
        clase_ff = "error" if s["falta_fiser"] > 0 else "metric-card"

        st.markdown(f"""
        <div class="metric-row">
            <div class="metric-card">
                <div class="metric-value">{s['match_exacto']}</div>
                <div class="metric-label">Match exacto</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{s['match_acumulado']}</div>
                <div class="metric-label">Match acumulado</div>
            </div>
            <div class="metric-card {clase_fb}">
                <div class="metric-value">{s['falta_banco']}</div>
                <div class="metric-label">Falta Banco</div>
            </div>
            <div class="metric-card {clase_ff}">
                <div class="metric-value">{s['falta_fiser']}</div>
                <div class="metric-label">Falta Fiser</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            label="📥 Descargar reporte Fiser",
            data=r["buf"],
            file_name="conciliacion_fiser.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
