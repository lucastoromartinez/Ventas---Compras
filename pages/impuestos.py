import streamlit as st
from estilos import aplicar_estilos, encabezado
from logica_percepciones import correr_cruce_percepciones
from logica_percepciones_pba import correr_cruce_percepciones_pba
from logica_retenciones_iibb_caba import correr_cruce_retenciones
from logica_retenciones_pba import correr_cruce_retenciones_pba
from logica_sircreb_pba import correr_cruce_sircreb

aplicar_estilos()

encabezado(
    "Impuestos",
    "Percepciones y retenciones ARCA contra sistema interno.",
    "Impuestos",
)

OPCIONES_SUBMODULO = [
    "Percepciones IIBB CABA",
    "Percepciones IIBB PBA",
    "Retenciones IIBB CABA",
    "Retenciones IIBB PBA",
]
submodulo_actual = st.selectbox(
    "Submódulo", OPCIONES_SUBMODULO, label_visibility="collapsed", key="impuestos_submodulo"
)


# ═══════════════════════════════════════════════
# TAB PERCEPCIONES IIBB CABA
# ═══════════════════════════════════════════════
if submodulo_actual == "Percepciones IIBB CABA":
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

    boton = st.button("Cruzar percepciones", disabled=not ambos_cargados, use_container_width=True,
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
            label="Descargar reporte completo",
            icon=":material/download:",
            data=r["buf_reporte"],
            file_name="cruce_percepciones.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_percepciones",
        )


# ═══════════════════════════════════════════════
# TAB PERCEPCIONES IIBB PBA
# ═══════════════════════════════════════════════
elif submodulo_actual == "Percepciones IIBB PBA":
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

    boton_pba = st.button("Cruzar percepciones", disabled=not ambos_cargados_pba, use_container_width=True,
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
            label="Descargar reporte completo",
            icon=":material/download:",
            data=r_pba["buf_reporte"],
            file_name="cruce_percepciones_pba.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_percepciones_pba",
        )


# ═══════════════════════════════════════════════
# TAB RETENCIONES IIBB CABA
# ═══════════════════════════════════════════════
elif submodulo_actual == "Retenciones IIBB CABA":
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

    boton_ret = st.button("Cruzar retenciones", disabled=not ambos_cargados_ret, use_container_width=True,
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
            label="Descargar reporte completo",
            icon=":material/download:",
            data=r_ret["buf_reporte"],
            file_name="cruce_retenciones.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_retenciones",
        )


# ═══════════════════════════════════════════════
# TAB RETENCIONES IIBB PBA (SIRTAC o SIRCREB)
# ═══════════════════════════════════════════════
elif submodulo_actual == "Retenciones IIBB PBA":
    st.markdown("<br>", unsafe_allow_html=True)

    modo_ret_pba = st.radio(
        "Tipo de retención",
        ["SIRTAC", "SIRCREB"],
        horizontal=True,
        key="ret_pba_modo",
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # ─────────────────────────────────────────
    # MODO SIRTAC
    # ─────────────────────────────────────────
    if modo_ret_pba == "SIRTAC":
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

        tol_sirtac = st.slider("Tolerancia de importes ($ ±)", min_value=0.0, max_value=10.0, value=1.0, step=0.5,
                                key="ret_pba_tol_sirtac")

        ambos_cargados_sirtac = archivo_sirtac_pba is not None and archivo_sistema_ret_pba is not None
        if not ambos_cargados_sirtac:
            st.info("Cargá el archivo SIRTAC (.txt) y el Excel del sistema para habilitar el cruce.")

        boton_sirtac = st.button("Cruzar retenciones", disabled=not ambos_cargados_sirtac, use_container_width=True,
                                  key="btn_sirtac")

        if boton_sirtac and ambos_cargados_sirtac:
            with st.spinner("Procesando..."):
                try:
                    buf_reporte_sirtac, stats_sirtac = correr_cruce_retenciones_pba(
                        archivo_sirtac_txt=archivo_sirtac_pba,
                        archivo_sistema=archivo_sistema_ret_pba,
                        tolerancia_importe=tol_sirtac,
                    )
                    st.session_state["resultado_sirtac"] = {
                        "buf_reporte": buf_reporte_sirtac,
                        "stats": stats_sirtac,
                    }
                except Exception as e:
                    st.error(f"Error al procesar: {e}")
                    st.stop()

        if "resultado_sirtac" in st.session_state:
            r_sirtac = st.session_state["resultado_sirtac"]
            stats_sirtac = r_sirtac["stats"]

            st.markdown("<hr class='divider'>", unsafe_allow_html=True)

            clase_falt_a_sirtac = "error" if stats_sirtac["faltante_arca"] > 0 else "metric-card"
            clase_falt_s_sirtac = "error" if stats_sirtac["faltante_sistema"] > 0 else "metric-card"
            clase_nuevos_sirtac = "warn" if stats_sirtac["proveedores_nuevos"] > 0 else "metric-card"

            st.markdown(f"""
            <div class="metric-row">
                <div class="metric-card">
                    <div class="metric-value">{stats_sirtac['match']}</div>
                    <div class="metric-label">Con match</div>
                </div>
                <div class="metric-card {clase_falt_a_sirtac}">
                    <div class="metric-value">{stats_sirtac['faltante_arca']}</div>
                    <div class="metric-label">Faltante ARCA</div>
                </div>
                <div class="metric-card {clase_falt_s_sirtac}">
                    <div class="metric-value">{stats_sirtac['faltante_sistema']}</div>
                    <div class="metric-label">Faltante sistema</div>
                </div>
                <div class="metric-card {clase_nuevos_sirtac}">
                    <div class="metric-value">{stats_sirtac['proveedores_nuevos']}</div>
                    <div class="metric-label">Proveedores nuevos</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            if stats_sirtac["proveedores_nuevos"] > 0:
                st.warning(
                    "Hay proveedores que matchearon por nombre y no están en el padrón (proveedores.py). "
                    "Revisá la hoja 'Proveedores_Nuevos' del reporte y agregalos al archivo."
                )

            st.markdown("<br>", unsafe_allow_html=True)
            st.download_button(
                label="Descargar reporte completo",
                icon=":material/download:",
                data=r_sirtac["buf_reporte"],
                file_name="cruce_sirtac.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="dl_sirtac",
            )

    # ─────────────────────────────────────────
    # MODO SIRCREB
    # ─────────────────────────────────────────
    else:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown('<div class="upload-label">Archivo SIRCREB (.txt)</div>', unsafe_allow_html=True)
            archivo_sircreb_txt = st.file_uploader("sircreb_txt", type=["txt"],
                                                    label_visibility="collapsed", key="sircreb_txt")
        with col2:
            st.markdown('<div class="upload-label">Excel Sistema (SIRCREB)</div>', unsafe_allow_html=True)
            archivo_sircreb_excel = st.file_uploader("sircreb_excel", type=["xlsx", "xls"],
                                                       label_visibility="collapsed", key="sircreb_excel")
        with col3:
            st.markdown('<div class="upload-label">Excel Sistema (retenciones IIBB)</div>', unsafe_allow_html=True)
            archivo_ret_excel = st.file_uploader("sircreb_ret_excel", type=["xlsx", "xls"],
                                                  label_visibility="collapsed", key="sircreb_ret_excel")

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)

        tol_sircreb = st.slider("Tolerancia de importes ($ ±)", min_value=0.0, max_value=10.0, value=1.0, step=0.5,
                                 key="sircreb_tol")

        todos_cargados_sircreb = (
            archivo_sircreb_txt is not None
            and archivo_sircreb_excel is not None
            and archivo_ret_excel is not None
        )
        if not todos_cargados_sircreb:
            st.info("Cargá el archivo SIRCREB (.txt) y los dos Excel del sistema para habilitar el cruce.")

        boton_sircreb = st.button("Cruzar retenciones", disabled=not todos_cargados_sircreb, use_container_width=True,
                                   key="btn_sircreb")

        if boton_sircreb and todos_cargados_sircreb:
            with st.spinner("Procesando..."):
                try:
                    buf_reporte_sircreb, stats_sircreb = correr_cruce_sircreb(
                        archivo_sircreb_txt=archivo_sircreb_txt,
                        archivo_sircreb_excel=archivo_sircreb_excel,
                        archivo_ret_excel=archivo_ret_excel,
                        tolerancia_importe=tol_sircreb,
                    )
                    st.session_state["resultado_sircreb"] = {
                        "buf_reporte": buf_reporte_sircreb,
                        "stats": stats_sircreb,
                    }
                except Exception as e:
                    st.error(f"Error al procesar: {e}")
                    st.stop()

        if "resultado_sircreb" in st.session_state:
            r_sircreb = st.session_state["resultado_sircreb"]
            stats_sircreb = r_sircreb["stats"]

            st.markdown("<hr class='divider'>", unsafe_allow_html=True)

            clase_falt_a_sircreb = "error" if stats_sircreb["faltante_arca"] > 0 else "metric-card"
            clase_falt_s_sircreb = "error" if stats_sircreb["faltante_sistema"] > 0 else "metric-card"

            st.markdown(f"""
            <div class="metric-row">
                <div class="metric-card">
                    <div class="metric-value">{stats_sircreb['match']}</div>
                    <div class="metric-label">Con match</div>
                </div>
                <div class="metric-card {clase_falt_a_sircreb}">
                    <div class="metric-value">{stats_sircreb['faltante_arca']}</div>
                    <div class="metric-label">Faltante ARCA</div>
                </div>
                <div class="metric-card {clase_falt_s_sircreb}">
                    <div class="metric-value">{stats_sircreb['faltante_sistema']}</div>
                    <div class="metric-label">Faltante sistema</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)
            st.download_button(
                label="Descargar reporte completo",
                icon=":material/download:",
                data=r_sircreb["buf_reporte"],
                file_name="cruce_sircreb.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="dl_sircreb",
            )
