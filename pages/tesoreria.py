import streamlit as st
from estilos import aplicar_estilos, encabezado
from logica_tesoreria import correr_conciliacion_tesoreria

aplicar_estilos()

encabezado(
    "Tesorería",
    "Caja Central contra contabilidad.",
    "Conciliaciones / Tesorería",
)

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

boton_tesoreria = st.button("Cruzar tesorería", disabled=not todo_ok,
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

    st.markdown(f"""
    <div class="metric-row">
        <div class="metric-card">
            <div class="metric-value">{s['comisiones']}</div>
            <div class="metric-label">Comisiones</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">{s['diferencia_arqueo']}</div>
            <div class="metric-label">Diferencia arqueo</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">{s['neteos']}</div>
            <div class="metric-label">Neteos contabilidad</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">{s['revisar']}</div>
            <div class="metric-label">A revisar</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.download_button(
        label="Descargar reporte Tesorería",
        icon=":material/download:",
        data=r["buf"],
        file_name="reporte_tesoreria.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
