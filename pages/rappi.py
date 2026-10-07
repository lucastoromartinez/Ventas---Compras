import streamlit as st
from estilos import aplicar_estilos, encabezado
from logica_rappi import correr_rappi, correr_rappi_resumen_facturas
from logica_atalaya import correr_atalaya
from logica_conciliacion_rappi import correr_conciliacion_rappi_easa
from logica_conciliacion_rappi_ronda import correr_conciliacion_rappi_ronda

aplicar_estilos()

encabezado(
    "Rappi",
    "Liquidaciones y conciliación de ventas Rappi.",
    "Conciliaciones / Rappi",
)

tab_liquidaciones, tab_conciliacion, tab_atalaya = st.tabs(
    [":material/description: Liquidaciones Rappi", ":material/fact_check: Conciliación Rappi", ":material/storefront: Atalaya"]
)


# ═══════════════════════════════════════════════
# TAB LIQUIDACIONES RAPPI
# ═══════════════════════════════════════════════
with tab_liquidaciones:
    st.markdown("<br>", unsafe_allow_html=True)

    modo_rappi = st.radio(
        "Modo",
        ["Cruce con liquidaciones", "Resumen simple de facturas"],
        horizontal=True,
        label_visibility="collapsed",
        key="rappi_modo",
    )

    st.markdown("<br>", unsafe_allow_html=True)

    if modo_rappi == "Cruce con liquidaciones":
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
            "Cruzar facturas contra liquidaciones",
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
                    <div class="liq-detail">{d['match']} con factura &nbsp;·&nbsp; {d['falta']} sin factura</div>
                    <div class="liq-detail">Facturas: {facturas_str}</div>
                </div>
                """, unsafe_allow_html=True)

            if s['advertencias']:
                for adv in s['advertencias']:
                    st.warning(adv)

            st.markdown("<br>", unsafe_allow_html=True)
            st.download_button(
                label="Descargar resultados Rappi (.zip)",
                icon=":material/download:",
                data=r["zip"],
                file_name="resultados_rappi.zip",
                mime="application/zip",
                use_container_width=True,
                key="dl_rappi"
            )

    else:
        st.markdown('<div class="upload-label">Facturas Rappi (PDFs)</div>', unsafe_allow_html=True)
        archivos_pdf_simple = st.file_uploader(
            "rappi_pdfs_simple",
            type=["pdf"],
            accept_multiple_files=True,
            label_visibility="collapsed",
            key="rappi_pdfs_simple"
        )

        if archivos_pdf_simple:
            items = "".join(f'<div class="pdf-item">{a.name}</div>' for a in archivos_pdf_simple)
            st.markdown(f'<div class="pdf-list">{items}</div>', unsafe_allow_html=True)

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)

        if not archivos_pdf_simple:
            st.info("Cargá al menos una factura PDF de Rappi.")

        boton_rappi_simple = st.button(
            "Generar resumen de facturas",
            disabled=not archivos_pdf_simple,
            use_container_width=True,
            key="btn_rappi_simple"
        )

        if boton_rappi_simple and archivos_pdf_simple:
            with st.spinner("Procesando facturas..."):
                try:
                    buf, stats = correr_rappi_resumen_facturas(archivos_pdf_simple)
                    st.session_state["resultado_rappi_facturas"] = {"buf": buf, "stats": stats}
                except Exception as e:
                    st.error(f"Error al procesar: {e}")

        if "resultado_rappi_facturas" in st.session_state:
            r = st.session_state["resultado_rappi_facturas"]
            s = r["stats"]

            st.markdown("<hr class='divider'>", unsafe_allow_html=True)
            st.success("¡Listo! El resumen de facturas está generado.")

            st.markdown(f"""
            <div class="counter-box">
                <div class="counter-num">{s['n_facturas']}</div>
                <div class="counter-label">factura{"s" if s['n_facturas'] != 1 else ""} procesada{"s" if s['n_facturas'] != 1 else ""}</div>
            </div>
            """, unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)
            st.download_button(
                label="Descargar resumen de facturas (.xlsx)",
                icon=":material/download:",
                data=r["buf"],
                file_name="resumen_facturas.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="dl_rappi_facturas"
            )


# ═══════════════════════════════════════════════
# TAB CONCILIACIÓN RAPPI
# ═══════════════════════════════════════════════
with tab_conciliacion:
    st.markdown("<br>", unsafe_allow_html=True)

    tab_easa, tab_ronda = st.tabs(["EASA", "Ronda"])

    with tab_easa:
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(
            '<div class="liq-card"><div class="liq-detail">'
            'Corre el cruce de facturas contra liquidaciones y sigue desde el cuadro de '
            'conceptos: ventas contra Dean y HIO, facturas y acreditaciones contra el mayor, '
            'y deja armado el cuadro de pendientes del mes siguiente.'
            '</div></div>',
            unsafe_allow_html=True
        )

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="upload-label">Liquidaciones Rappi (Excel — una o más)</div>', unsafe_allow_html=True)
        conc_liq = st.file_uploader(
            "conc_liq", type=["xlsx", "xls"], accept_multiple_files=True,
            label_visibility="collapsed", key="conc_easa_liquidaciones"
        )

        st.markdown('<div class="upload-label">Facturas Rappi (PDFs)</div>', unsafe_allow_html=True)
        conc_pdf = st.file_uploader(
            "conc_pdf", type=["pdf"], accept_multiple_files=True,
            label_visibility="collapsed", key="conc_easa_pdfs"
        )

        if conc_liq or conc_pdf:
            st.markdown(f"""
            <div class="metric-row">
                <div class="metric-card ok">
                    <div class="metric-value">{len(conc_liq or [])}</div>
                    <div class="metric-label">Liquidaciones</div>
                </div>
                <div class="metric-card ok">
                    <div class="metric-value">{len(conc_pdf or [])}</div>
                    <div class="metric-label">Facturas</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)

        st.markdown('<div class="upload-label">Reporte HIO por documento (Excel)</div>', unsafe_allow_html=True)
        conc_hio_doc = st.file_uploader(
            "conc_hio_doc", type=["xlsx", "xls"], label_visibility="collapsed",
            key="conc_easa_hio_documento"
        )

        st.markdown('<div class="upload-label">Reporte HIO por método de pago (Excel)</div>', unsafe_allow_html=True)
        conc_hio_met = st.file_uploader(
            "conc_hio_met", type=["xlsx", "xls"], label_visibility="collapsed",
            key="conc_easa_hio_metodo"
        )

        st.markdown('<div class="upload-label">Reporte Dean (Excel)</div>', unsafe_allow_html=True)
        conc_dean = st.file_uploader(
            "conc_dean", type=["xlsx", "xls"], label_visibility="collapsed",
            key="conc_easa_dean"
        )

        st.markdown('<div class="upload-label">Cuenta recaudación Rappi — mayor (Excel)</div>', unsafe_allow_html=True)
        conc_recaudacion = st.file_uploader(
            "conc_recaudacion", type=["xlsx", "xls"], label_visibility="collapsed",
            key="conc_easa_recaudacion"
        )

        st.markdown('<div class="upload-label">Cuadro de pendientes del mes anterior (Excel)</div>', unsafe_allow_html=True)
        conc_pendientes = st.file_uploader(
            "conc_pendientes", type=["xlsx", "xls"], label_visibility="collapsed",
            key="conc_easa_pendientes"
        )

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)

        faltantes = [
            nombre for nombre, valor in [
                ("las liquidaciones Rappi", conc_liq),
                ("las facturas PDF", conc_pdf),
                ("el reporte HIO por documento", conc_hio_doc),
                ("el reporte HIO por método de pago", conc_hio_met),
                ("el reporte de Dean", conc_dean),
                ("la cuenta recaudación Rappi", conc_recaudacion),
                ("el cuadro de pendientes", conc_pendientes),
            ] if not valor
        ]
        if faltantes:
            st.info("Falta cargar: " + ", ".join(faltantes) + ".")

        boton_conc = st.button(
            "Conciliar Rappi — EASA",
            disabled=bool(faltantes),
            use_container_width=True,
            key="btn_conciliacion_easa"
        )

        if boton_conc and not faltantes:
            with st.spinner("Conciliando Rappi EASA..."):
                try:
                    zip_buf, stats = correr_conciliacion_rappi_easa(
                        conc_liq, conc_pdf, conc_hio_doc, conc_hio_met,
                        conc_dean, conc_recaudacion, conc_pendientes
                    )
                    st.session_state["resultado_conciliacion_easa"] = {"zip": zip_buf, "stats": stats}
                except Exception as e:
                    st.error(f"Error al procesar: {e}")

        if "resultado_conciliacion_easa" in st.session_state:
            r = st.session_state["resultado_conciliacion_easa"]
            s = r["stats"]
            c = s["conciliacion"]

            st.markdown("<hr class='divider'>", unsafe_allow_html=True)
            st.success(f"¡Listo! Conciliación de {c['nombre_mes']} {c['anio']} generada.")

            def _pesos(valor):
                if valor is None:
                    return "—"
                return f"$ {valor:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")

            st.markdown(f"""
            <div class="metric-row">
                <div class="metric-card ok">
                    <div class="metric-value">{c['match_dean'] + c['match_hio'] + c['match_hio_fecha']}</div>
                    <div class="metric-label">Ventas cruzadas</div>
                </div>
                <div class="metric-card warn">
                    <div class="metric-value">{c['falta_rappi_dean'] + c['falta_dean'] + c['falta_rappi_hio'] + c['falta_hio']}</div>
                    <div class="metric-label">Ventas sin cruzar</div>
                </div>
                <div class="metric-card ok">
                    <div class="metric-value">{c['match_facturas'] + c['match_pendientes']}</div>
                    <div class="metric-label">Facturas cobradas</div>
                </div>
                <div class="metric-card warn">
                    <div class="metric-value">{c['falta_facturas'] + c['falta_pendientes']}</div>
                    <div class="metric-label">Facturas pendientes</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            st.markdown(f"""
            <div class="liq-card">
                <div class="liq-id">Ventas del mes</div>
                <div class="liq-detail">Dean: {_pesos(c['venta_dean'])} &nbsp;|&nbsp; HIO: {_pesos(c['venta_hio'])}</div>
                <div class="liq-detail">Diferencia D&amp;D: {_pesos(c['total_dean'])} &nbsp;|&nbsp; Diferencia HIO: {_pesos(c['total_hio'])}</div>
            </div>
            <div class="liq-card">
                <div class="liq-id">Acreditaciones {c['mes']:02d}-{c['anio']}</div>
                <div class="liq-detail">Mayor: {_pesos(c['acred_mayor'])} &nbsp;|&nbsp; Esperado: {_pesos(c['acred_esperado'])} &nbsp;|&nbsp; Diferencia: {_pesos(c['acred_diferencia'])}</div>
                <div class="liq-detail">Sin registrar: {c['acred_faltantes']} &nbsp;|&nbsp; Negativas: {c['acred_negativas']} &nbsp;|&nbsp; A cobrar el mes que viene: {c['acred_mes_siguiente']}</div>
            </div>
            <div class="liq-card">
                <div class="liq-id">Cierre del cuadro</div>
                <div class="liq-detail">Saldo según mayor: {_pesos(c.get('saldo_mayor'))} &nbsp;|&nbsp; Total saldo del mes: {_pesos(c.get('total_saldo_mes'))}</div>
                <div class="liq-detail">Saldo a acreditar: {_pesos(c.get('saldo_acreditar'))} &nbsp;|&nbsp; Diferencia de centavos: {_pesos(c.get('diferencia_centavos'))}</div>
            </div>
            """, unsafe_allow_html=True)

            if c['dif_facturas'] or c['dif_pendientes']:
                st.warning(
                    f"Hay {c['dif_facturas'] + c['dif_pendientes']} factura/s con diferencia "
                    "no explicada contra el mayor: están en el detalle del cruce."
                )

            for adv in s['advertencias']:
                st.warning(adv)

            st.markdown("<br>", unsafe_allow_html=True)
            st.download_button(
                label="Descargar conciliación Rappi EASA (.zip)",
                icon=":material/download:",
                data=r["zip"],
                file_name=f"conciliacion_rappi_easa_{c['mes']:02d}_{c['anio']}.zip",
                mime="application/zip",
                use_container_width=True,
                key="dl_conciliacion_easa"
            )

    with tab_ronda:
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(
            '<div class="liq-card"><div class="liq-detail">'
            'Corre el cruce de facturas contra liquidaciones y sigue desde el cuadro de conceptos: '
            'ventas contra HIO —por localizador y, lo que no tiene, por fecha, local e importe— '
            'y contra Atalaya, facturas y acreditaciones contra el mayor, y deja armado el cuadro '
            'de pendientes del mes siguiente. Las liquidaciones con valor total negativo no se '
            'cruzan: van al cuadro como Acreditaciones Negativas.'
            '</div></div>',
            unsafe_allow_html=True
        )

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="upload-label">Liquidaciones Rappi (Excel — una o más)</div>', unsafe_allow_html=True)
        ronda_liq = st.file_uploader(
            "ronda_liq", type=["xlsx", "xls"], accept_multiple_files=True,
            label_visibility="collapsed", key="conc_ronda_liquidaciones"
        )

        st.markdown('<div class="upload-label">Facturas Rappi (PDFs)</div>', unsafe_allow_html=True)
        ronda_pdf = st.file_uploader(
            "ronda_pdf", type=["pdf"], accept_multiple_files=True,
            label_visibility="collapsed", key="conc_ronda_pdfs"
        )

        if ronda_liq or ronda_pdf:
            st.markdown(f"""
            <div class="metric-row">
                <div class="metric-card ok">
                    <div class="metric-value">{len(ronda_liq or [])}</div>
                    <div class="metric-label">Liquidaciones</div>
                </div>
                <div class="metric-card ok">
                    <div class="metric-value">{len(ronda_pdf or [])}</div>
                    <div class="metric-label">Facturas</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)

        st.markdown('<div class="upload-label">Reporte HIO por documento (Excel)</div>', unsafe_allow_html=True)
        ronda_hio_doc = st.file_uploader(
            "ronda_hio_doc", type=["xlsx", "xls"], label_visibility="collapsed",
            key="conc_ronda_hio_documento"
        )

        st.markdown('<div class="upload-label">Reporte HIO por método de pago (Excel)</div>', unsafe_allow_html=True)
        ronda_hio_met = st.file_uploader(
            "ronda_hio_met", type=["xlsx", "xls"], label_visibility="collapsed",
            key="conc_ronda_hio_metodo"
        )

        st.markdown('<div class="upload-label">Reporte Atalaya (Excel)</div>', unsafe_allow_html=True)
        ronda_atalaya = st.file_uploader(
            "ronda_atalaya", type=["xlsx", "xls"], label_visibility="collapsed",
            key="conc_ronda_atalaya"
        )

        st.markdown('<div class="upload-label">Cuenta recaudación Rappi — mayor (Excel)</div>', unsafe_allow_html=True)
        ronda_recaudacion = st.file_uploader(
            "ronda_recaudacion", type=["xlsx", "xls"], label_visibility="collapsed",
            key="conc_ronda_recaudacion"
        )

        st.markdown('<div class="upload-label">Cuadro de pendientes del mes anterior (Excel)</div>', unsafe_allow_html=True)
        ronda_pendientes = st.file_uploader(
            "ronda_pendientes", type=["xlsx", "xls"], label_visibility="collapsed",
            key="conc_ronda_pendientes"
        )

        st.markdown("<hr class='divider'>", unsafe_allow_html=True)

        faltantes_ronda = [
            nombre for nombre, valor in [
                ("las liquidaciones Rappi", ronda_liq),
                ("las facturas PDF", ronda_pdf),
                ("el reporte HIO por documento", ronda_hio_doc),
                ("el reporte HIO por método de pago", ronda_hio_met),
                ("el reporte de Atalaya", ronda_atalaya),
                ("la cuenta recaudación Rappi", ronda_recaudacion),
                ("el cuadro de pendientes", ronda_pendientes),
            ] if not valor
        ]
        if faltantes_ronda:
            st.info("Falta cargar: " + ", ".join(faltantes_ronda) + ".")

        boton_ronda = st.button(
            "Conciliar Rappi — Ronda",
            disabled=bool(faltantes_ronda),
            use_container_width=True,
            key="btn_conciliacion_ronda"
        )

        if boton_ronda and not faltantes_ronda:
            with st.spinner("Conciliando Rappi Ronda..."):
                try:
                    zip_buf, stats, _ = correr_conciliacion_rappi_ronda(
                        ronda_liq, ronda_pdf, ronda_recaudacion,
                        ronda_hio_doc, ronda_hio_met, ronda_atalaya, ronda_pendientes
                    )
                    st.session_state["resultado_conciliacion_ronda"] = {"zip": zip_buf, "stats": stats}
                except Exception as e:
                    st.error(f"Error al procesar: {e}")

        if "resultado_conciliacion_ronda" in st.session_state:
            r = st.session_state["resultado_conciliacion_ronda"]
            s = r["stats"]
            fm, fp, ac = s["facturas_mes"], s["facturas_pendientes"], s["acreditaciones"]

            st.markdown("<hr class='divider'>", unsafe_allow_html=True)
            st.success(f"¡Listo! Conciliación de {s['mes']} generada.")

            def _pesos_ronda(valor):
                if valor is None:
                    return "—"
                return f"$ {valor:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")

            st.markdown(f"""
            <div class="metric-row">
                <div class="metric-card ok">
                    <div class="metric-value">{s['match_hio'] + s['match_atalaya']}</div>
                    <div class="metric-label">Ventas cruzadas</div>
                </div>
                <div class="metric-card warn">
                    <div class="metric-value">{s['falta_hio'] + s['falta_atalaya']}</div>
                    <div class="metric-label">Ventas sin cruzar</div>
                </div>
                <div class="metric-card ok">
                    <div class="metric-value">{fm['match'] + fp['match']}</div>
                    <div class="metric-label">Facturas cobradas</div>
                </div>
                <div class="metric-card warn">
                    <div class="metric-value">{fm['falta'] + fp['falta']}</div>
                    <div class="metric-label">Facturas pendientes</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            st.markdown(f"""
            <div class="liq-card">
                <div class="liq-id">Ventas del mes</div>
                <div class="liq-detail">HIO — Rappi: {_pesos_ronda(s['venta_rappi_hio'])} &nbsp;|&nbsp; sistema: {_pesos_ronda(s['venta_sistema_hio'])} &nbsp;|&nbsp; diferencia: {_pesos_ronda(s['diferencia_hio'])}</div>
                <div class="liq-detail">Atalaya — Rappi: {_pesos_ronda(s['venta_rappi_atalaya'])} &nbsp;|&nbsp; sistema: {_pesos_ronda(s['venta_sistema_atalaya'])} &nbsp;|&nbsp; diferencia: {_pesos_ronda(s['diferencia_atalaya'])}</div>
            </div>
            <div class="liq-card">
                <div class="liq-id">Acreditaciones {s['mes_numero']:02d}-{s['anio']}</div>
                <div class="liq-detail">Mayor: {_pesos_ronda(ac['mayor'])} &nbsp;|&nbsp; Esperado: {_pesos_ronda(ac['esperado'])} &nbsp;|&nbsp; Diferencia: {_pesos_ronda(ac['diferencia'])}</div>
                <div class="liq-detail">Sin registrar: {ac['faltantes']} &nbsp;|&nbsp; Negativas: {ac['negativas']} &nbsp;|&nbsp; A cobrar el mes que viene: {ac['mes_siguiente']}</div>
            </div>
            """, unsafe_allow_html=True)

            if s["facturas_saldadas_no_cruzadas"]:
                detalle = "".join(
                    f'<div class="liq-detail">{texto} &nbsp;—&nbsp; {_pesos_ronda(monto)}</div>'
                    for texto, monto in s["facturas_saldadas_no_cruzadas"]
                )
                st.markdown(
                    f'<div class="liq-card warn"><div class="liq-id">Facturas saldadas no cruzadas</div>{detalle}</div>',
                    unsafe_allow_html=True
                )

            if fm["diferencias"] or fp["diferencias"]:
                st.warning(
                    f"Hay {fm['diferencias'] + fp['diferencias']} factura/s con diferencia "
                    "no explicada contra el mayor."
                )

            for adv in s["advertencias"]:
                st.warning(adv)

            st.markdown("<br>", unsafe_allow_html=True)
            st.download_button(
                label="Descargar conciliación Rappi Ronda (.zip)",
                icon=":material/download:",
                data=r["zip"],
                file_name=f"conciliacion_rappi_ronda_{s['mes_numero']:02d}_{s['anio']}.zip",
                mime="application/zip",
                use_container_width=True,
                key="dl_conciliacion_ronda"
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
        "Cruzar Rappi contra Atalaya",
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
            label="Descargar cruce Rappi vs Atalaya (.xlsx)",
            icon=":material/download:",
            data=r["buf"],
            file_name="cruce_rappi_atalaya.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_atalaya"
        )
