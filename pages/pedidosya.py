import streamlit as st
from logica_pedidosya import correr_conciliacion_peya

st.set_page_config(
    page_title="Pedidos Ya",
    page_icon="🍔",
    layout="centered",
)

ACENTO = "#FA0050"

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] {{ font-family: 'IBM Plex Sans', sans-serif; }}
.stApp {{ background-color: #0f0f0f; color: #e8e8e8; }}

.header-block {{
    border-left: 3px solid {ACENTO};
    padding: 0.4rem 0 0.4rem 1.2rem;
    margin-bottom: 2rem;
}}
.header-block h1 {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.6rem; font-weight: 600;
    color: #ffffff; margin: 0; letter-spacing: -0.5px;
}}
.header-block p {{
    font-size: 0.82rem; color: #666;
    margin: 0.2rem 0 0 0;
    font-family: 'IBM Plex Mono', monospace;
}}
.upload-label {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem; color: {ACENTO};
    letter-spacing: 1.5px; text-transform: uppercase;
    margin-bottom: 0.4rem;
}}
[data-testid="stFileUploader"] {{
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 0.8rem; transition: border-color 0.2s;
}}
[data-testid="stFileUploader"]:hover {{ border-color: {ACENTO}; }}

.stButton > button {{
    background: {ACENTO} !important; color: #0f0f0f !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-weight: 600 !important; font-size: 0.85rem !important;
    letter-spacing: 1px !important; border: none !important;
    border-radius: 4px !important; padding: 0.6rem 2rem !important;
}}
.stButton > button:disabled {{ background: #2a2a2a !important; color: #555 !important; }}

.metric-row {{ display: flex; gap: 1rem; margin: 1.5rem 0; flex-wrap: wrap; }}
.metric-card {{
    flex: 1; min-width: 80px; background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 1rem; text-align: center;
}}
.metric-card .metric-value {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.3rem; font-weight: 600; color: {ACENTO}; line-height: 1.2;
}}
.metric-card .metric-label {{
    font-size: 0.7rem; color: #555; text-transform: uppercase;
    letter-spacing: 1px; margin-top: 0.4rem;
    font-family: 'IBM Plex Mono', monospace;
}}
.metric-card.warn .metric-value {{ color: #facc15; }}
.metric-card.ok   .metric-value {{ color: #4ade80; }}

.liq-card {{
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 0.9rem 1.2rem; margin: 0.5rem 0;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.78rem;
}}
.liq-card .liq-id {{ color: {ACENTO}; font-weight: 600; font-size: 0.85rem; }}
.liq-card .liq-detail {{ color: #888; margin-top: 0.3rem; }}
.liq-card.warn {{ border-color: #facc1555; }}
.liq-card.naranja {{ border-color: #fb923c66; }}
.liq-card.naranja .liq-id {{ color: #fb923c; }}

.divider {{ border: none; border-top: 1px solid #1e1e1e; margin: 2rem 0; }}

[data-testid="stDownloadButton"] > button {{
    background: #1a1a1a !important; color: #e8e8e8 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.8rem !important; border: 1px solid #2a2a2a !important;
    border-radius: 4px !important; width: 100% !important;
    transition: border-color 0.2s !important;
}}
[data-testid="stDownloadButton"] > button:hover {{
    border-color: {ACENTO} !important; color: {ACENTO} !important;
}}

.back-btn > button {{
    background: transparent !important; color: #444 !important;
    border: 1px solid #2a2a2a !important; font-size: 0.75rem !important;
    margin-top: 0 !important; margin-bottom: 1rem !important;
}}
.back-btn > button:hover {{ color: {ACENTO} !important; border-color: {ACENTO} !important; }}

.pdf-list {{
    background: #1a1a1a; border: 1px solid #2a2a2a;
    border-radius: 6px; padding: 1rem 1.2rem;
    margin: 1rem 0; max-height: 200px; overflow-y: auto;
}}
.pdf-item {{
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem; color: #888;
    padding: 0.2rem 0; border-bottom: 1px solid #222;
}}
.pdf-item:last-child {{ border-bottom: none; }}

div[data-testid="stTabs"] button {{
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.8rem !important; color: #555 !important;
}}
div[data-testid="stTabs"] button[aria-selected="true"] {{
    color: {ACENTO} !important; border-bottom-color: {ACENTO} !important;
}}
</style>
""", unsafe_allow_html=True)

# Botón volver
st.markdown('<div class="back-btn">', unsafe_allow_html=True)
if st.button("← Volver al inicio"):
    st.switch_page(
        st.session_state["_pages"]["home"] if "_pages" in st.session_state
        else st.Page("app_home.py", title="Inicio", icon="⚡", default=True)
    )
st.markdown('</div>', unsafe_allow_html=True)

st.markdown("""
<div class="header-block">
    <h1>🍔 Pedidos Ya</h1>
    <p>Conciliación de la cuenta recaudación PedidosYa</p>
</div>
""", unsafe_allow_html=True)


def _pesos(valor):
    if valor is None:
        return "—"
    return f"$ {valor:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")


def _lista_archivos(archivos):
    if archivos:
        items = "".join(f'<div class="pdf-item">{a.name}</div>' for a in archivos)
        st.markdown(f'<div class="pdf-list">{items}</div>', unsafe_allow_html=True)


def _uploader(etiqueta, clave, tipos, multiple=False):
    st.markdown(f'<div class="upload-label">{etiqueta}</div>', unsafe_allow_html=True)
    return st.file_uploader(
        clave, type=tipos, accept_multiple_files=multiple,
        label_visibility="collapsed", key=clave,
    )


def _resultado(clave_estado, entidad_nombre):
    r = st.session_state[clave_estado]
    s = r["stats"]
    c = s["controles"]

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)
    if s["valido"]:
        st.success(f"Conciliación {entidad_nombre} {s['periodo']} generada: cierra en cero y la verificación coincide.")
    else:
        st.warning(
            f"Conciliación {entidad_nombre} {s['periodo']} generada, pero no cumple las condiciones de "
            "cierre válido (saldo final ±1 peso y verificación independiente coincidente). Revisar los controles."
        )
    if s["subtitulo"]:
        st.caption(s["subtitulo"])

    clase_final = "ok" if s["cierre_ok"] else "warn"
    clase_verif = "ok" if s["verificacion_ok"] else "warn"
    st.markdown(f"""
    <div class="metric-row">
        <div class="metric-card">
            <div class="metric-value">{_pesos(c.get('saldo_mayor'))}</div>
            <div class="metric-label">Saldo del mayor</div>
        </div>
        <div class="metric-card {clase_final}">
            <div class="metric-value">{_pesos(c.get('saldo_final'))}</div>
            <div class="metric-label">Saldo final</div>
        </div>
        <div class="metric-card {clase_verif}">
            <div class="metric-value">{c.get('liquidaciones', '—')}</div>
            <div class="metric-label">Liquidaciones</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    v = c.get("verificacion") or {}
    ac = c.get("acreditaciones") or {}
    fa = c.get("facturas") or {}
    re_ = c.get("reintegros") or {}
    scope = c.get("scope") or ["", ""]
    st.markdown(f"""
    <div class="liq-card">
        <div class="liq-id">Verificación independiente</div>
        <div class="liq-detail">Ventas internas: {_pesos(v.get('ventas_internas'))} &nbsp;|&nbsp; PeYa: {_pesos(v.get('peya'))}</div>
        <div class="liq-detail">Residuo independiente: {_pesos(v.get('residuo_independiente'))} &nbsp;|&nbsp; Explicado por cruces: {_pesos(v.get('cruces'))}</div>
    </div>
    <div class="liq-card">
        <div class="liq-id">Acreditaciones</div>
        <div class="liq-detail">Liquidaciones: {_pesos(ac.get('liq'))} &nbsp;|&nbsp; Mayor: {_pesos(ac.get('mayor'))}</div>
        <div class="liq-detail">Pendientes: {', '.join(ac.get('pendientes') or []) or '—'}</div>
    </div>
    <div class="liq-card">
        <div class="liq-id">Facturas</div>
        <div class="liq-detail">Pendientes: {len(fa.get('pendientes') or [])} &nbsp;|&nbsp; Con diferencia: {len(fa.get('con_diferencia') or [])} &nbsp;|&nbsp; Tipeadas: {len(fa.get('tipeadas') or [])} &nbsp;|&nbsp; Huérfanas: {len(fa.get('huerfanas') or [])}</div>
    </div>
    <div class="liq-card">
        <div class="liq-id">Reintegros</div>
        <div class="liq-detail">Líneas: {_pesos(re_.get('lineas'))} &nbsp;|&nbsp; Liquidaciones: {_pesos(re_.get('liquidaciones'))} &nbsp;|&nbsp; Casos: {re_.get('casos', '—')}</div>
        <div class="liq-detail">Scope: {scope[0]} al {scope[-1]}</div>
    </div>
    """, unsafe_allow_html=True)

    if c.get("liq_no_cierran"):
        st.warning("Liquidaciones cuya ecuación no cierra: " + ", ".join(map(str, c["liq_no_cierran"])))
    if c.get("ventas_vs_mayor"):
        st.warning(f"Ventas internas vs mayor con diferencias en {len(c['ventas_vs_mayor'])} caso/s (control 2).")
    if c.get("devengado_vs_reporte"):
        st.warning(f"Devengado anterior vs reporte nuevo con diferencias en {len(c['devengado_vs_reporte'])} caso/s (control 2b).")

    # Líneas que requieren decisión humana: el cálculo no cambia con esa decisión.
    if s["lineas_naranja"]:
        detalle = "".join(
            f'<div class="liq-detail">{x["concepto"]} &nbsp;—&nbsp; {_pesos(x["importe"])}</div>'
            for x in s["lineas_naranja"]
        )
        st.markdown(
            f'<div class="liq-card naranja"><div class="liq-id">Líneas deducidas o arrastradas '
            f'({len(s["lineas_naranja"])})</div>{detalle}</div>',
            unsafe_allow_html=True,
        )
    if s["pendiente_definicion"]:
        detalle = "".join(
            f'<div class="liq-detail">{x["concepto"]} &nbsp;—&nbsp; {_pesos(x["importe"])}</div>'
            for x in s["pendiente_definicion"]
        )
        st.markdown(
            f'<div class="liq-card warn"><div class="liq-id">Pendiente de definición (fuera del saldo)</div>{detalle}</div>',
            unsafe_allow_html=True,
        )

    for adv in s["advertencias"]:
        st.warning(adv)

    st.markdown("<br>", unsafe_allow_html=True)
    st.download_button(
        label="📥 Descargar conciliación (.xlsx)",
        data=r["excel"],
        file_name=r["nombre"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key=f"dl_{clave_estado}",
    )
    st.caption("Guardar este Excel sin modificar: es el insumo del cierre del mes siguiente.")
    st.download_button(
        label="📥 Descargar papeles de trabajo y controles (.zip)",
        data=r["trabajo"],
        file_name=r["nombre"].replace(".xlsx", "_trabajo.zip"),
        mime="application/zip",
        use_container_width=True,
        key=f"dl_trabajo_{clave_estado}",
    )
    with st.expander("Log del proceso"):
        st.code(s["log"] or "(sin salida)", language="text")


def _pestania_entidad(entidad, nombre, descripcion, reportes):
    """reportes: lista de (clave, etiqueta, descripción para 'falta cargar')."""
    pre = f"peya_{entidad}"
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(f'<div class="liq-card"><div class="liq-detail">{descripcion}</div></div>',
                unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    zips = _uploader("Estados de cuenta PeYa (ZIP semanales, sin huecos)", f"{pre}_zips", ["zip"], True)
    _lista_archivos(zips)
    facturas = _uploader("Facturas PEDIDOSYA / PAGOS YA (PDFs)", f"{pre}_facturas", ["pdf"], True)
    _lista_archivos(facturas)

    if zips or facturas:
        st.markdown(f"""
        <div class="metric-row">
            <div class="metric-card ok">
                <div class="metric-value">{len(zips or [])}</div>
                <div class="metric-label">Estados de cuenta</div>
            </div>
            <div class="metric-card ok">
                <div class="metric-value">{len(facturas or [])}</div>
                <div class="metric-label">Facturas</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    mayor = _uploader("Mayor cuenta recaudación PeYa — completo desde el origen (Excel)",
                      f"{pre}_mayor", ["xlsx"])
    archivos = {}
    for clave, etiqueta, _ in reportes:
        archivos[clave] = _uploader(etiqueta, f"{pre}_{clave}", ["xlsx"])

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    mensual = st.checkbox(
        "Cierre mensual (con la conciliación del mes anterior)", value=True, key=f"{pre}_es_mensual",
        help="Sin conciliación anterior se corre un cierre inicial sobre todo el rango de ZIPs.",
    )
    anterior = None
    if mensual:
        anterior = _uploader("Conciliación del mes anterior (Excel generado por este módulo)",
                             f"{pre}_anterior", ["xlsx"])

    faltantes = [
        desc for desc, valor in [
            ("los ZIP de estado de cuenta", zips),
            ("las facturas PDF", facturas),
            ("el mayor de recaudación", mayor),
            *[(desc, archivos[clave]) for clave, _, desc in reportes],
            *([("la conciliación del mes anterior", anterior)] if mensual else []),
        ] if not valor
    ]
    if faltantes:
        st.info("Falta cargar: " + ", ".join(faltantes) + ".")

    boton = st.button(
        f"CONCILIAR PEDIDOS YA — {nombre}",
        disabled=bool(faltantes),
        use_container_width=True,
        key=f"btn_{pre}",
    )

    clave_estado = f"resultado_{pre}"
    if boton and not faltantes:
        st.session_state.pop(clave_estado, None)
        with st.spinner(f"Conciliando PedidosYa {nombre}..."):
            try:
                excel, nombre_excel, trabajo, stats = correr_conciliacion_peya(
                    entidad, zips, facturas, mayor,
                    archivo_dean=archivos.get("dean"),
                    archivo_hio_documento=archivos.get("hio_documento"),
                    archivo_hio_metodo=archivos.get("hio_metodo"),
                    archivo_atalaya=archivos.get("atalaya"),
                    archivo_anterior=anterior,
                )
                st.session_state[clave_estado] = {
                    "excel": excel, "nombre": nombre_excel, "trabajo": trabajo, "stats": stats,
                }
            except Exception as e:
                st.error(f"Error al procesar: {e}")

    if clave_estado in st.session_state:
        _resultado(clave_estado, nombre)


tab_easa, tab_ronda = st.tabs(["🏢  EASA", "🏢  Ronda"])

with tab_easa:
    _pestania_entidad(
        "easa", "EASA",
        "Parte del saldo del mayor de la cuenta recaudación PedidosYa (local 411335) y lo lleva a cero: "
        "ajustes de registración, devengamiento, pendientes al cierre y cruces contra Dean "
        "(por Nº de transacción) y HIOffice (por localizador y, lo que no tiene, por fecha, local y monto bruto).",
        [
            ("dean", "Ventas Dean (Excel)", "el reporte de Dean"),
            ("hio_documento", "Reporte HIO por documento (Excel)", "el reporte HIO por documento"),
            ("hio_metodo", "Reporte HIO por método de pago (Excel)", "el reporte HIO por método de pago"),
        ],
    )

with tab_ronda:
    _pestania_entidad(
        "ronda", "RONDA",
        "Parte del saldo del mayor de la cuenta recaudación PedidosYa (local 409994) y lo lleva a cero: "
        "ajustes de registración, devengamiento, pendientes al cierre y cruces contra HIOffice "
        "(por localizador y por fecha, local y BASE) y Atalaya (por fecha y criterio de facturación). "
        "El histórico de diferencias se abre por mes y por local.",
        [
            ("hio_documento", "Reporte HIO por documento (Excel)", "el reporte HIO por documento"),
            ("hio_metodo", "Reporte HIO por método de pago (Excel)", "el reporte HIO por método de pago"),
            ("atalaya", "Tickets Atalaya (Excel)", "el reporte de Atalaya"),
        ],
    )
