"""
Estilos compartidos de la app.

Los colores, la tipografía y los bordes base se definen en
.streamlit/config.toml. Acá queda solo lo que el tema de Streamlit no cubre:
las tarjetas y bloques HTML que usan las páginas (metric-card, liq-card,
upload-label, etc.), el encabezado de cada página y el ancho del contenido.

Cada página llama a aplicar_estilos() y a encabezado(...) al comienzo.
"""
import html
import os

import streamlit as st

RAIZ = os.path.dirname(os.path.abspath(__file__))
LOGO_ICONO = os.path.join(RAIZ, "assets", "logo_enter.png")
LOGO_COMPLETO = os.path.join(RAIZ, "assets", "logo_sistema.png")

# Paleta (la misma del tema en config.toml)
AZUL = "#0516BB"
TINTA = "#16202E"
GRIS = "#5B6675"
LINEA = "#E3E6EB"
FONDO = "#F5F6F8"

_CSS = f"""
<style>
/* ---------- Contenido ---------- */
[data-testid="stMainBlockContainer"] {{
    max-width: 1200px;
    padding-top: 2.5rem;
}}
[data-testid="stHeaderActionElements"] {{ display: none; }}

/* ---------- Encabezado de página ---------- */
.encabezado {{ margin: 0 0 1.75rem 0; }}
.encabezado .ruta {{ font-size: 0.82rem; color: {GRIS}; }}
.encabezado h1 {{
    font-size: 1.75rem; font-weight: 600; color: {TINTA};
    margin: 0.25rem 0 0.35rem 0; padding: 0; letter-spacing: -0.01em;
}}
.encabezado p {{ font-size: 0.95rem; color: {GRIS}; margin: 0; }}

/* ---------- Etiquetas de carga ---------- */
.upload-label {{
    font-size: 0.85rem; font-weight: 600; color: {TINTA};
    margin: 0.75rem 0 0.35rem 0;
}}
.upload-label.optional, .upload-label.secondary {{ font-weight: 500; color: {GRIS}; }}
.upload-label.optional::after {{ content: " · opcional"; font-weight: 400; }}
.section-title, .section-label {{
    font-size: 0.78rem; font-weight: 600; letter-spacing: 0.08em;
    text-transform: uppercase; color: {GRIS}; margin: 1.25rem 0 0.25rem 0;
}}
.toggle-box {{ margin: 0.5rem 0; }}

/* ---------- Zona de carga de archivos ---------- */
[data-testid="stFileUploaderDropzone"] {{
    background: #FFFFFF; border: 1px dashed #C9CFD8;
}}
[data-testid="stFileUploaderDropzone"]:hover {{ border-color: {AZUL}; }}

/* ---------- Botones de acción ---------- */
[data-testid="stMainBlockContainer"] .stButton > button {{
    min-height: 2.75rem; font-weight: 600;
}}
[data-testid="stMainBlockContainer"] .stButton > button[kind="secondary"]:not(:disabled) {{
    background: {AZUL}; border-color: {AZUL}; color: #FFFFFF;
}}
[data-testid="stMainBlockContainer"] .stButton > button[kind="secondary"]:not(:disabled):hover {{
    background: #04108F; border-color: #04108F; color: #FFFFFF;
}}
[data-testid="stDownloadButton"] > button {{ min-height: 2.75rem; font-weight: 500; }}

/* ---------- Indicadores ---------- */
.metric-row {{ display: flex; gap: 1rem; margin: 1.25rem 0; flex-wrap: wrap; }}
.metric-card {{
    flex: 1; min-width: 150px; background: #FFFFFF;
    border: 1px solid {LINEA}; border-radius: 10px; padding: 1rem 1.15rem;
    display: flex; flex-direction: column-reverse; gap: 0.35rem;
}}
.metric-card .metric-value {{
    font-family: 'IBM Plex Mono', monospace; font-size: 1.5rem;
    font-weight: 500; color: {TINTA}; line-height: 1.15;
}}
.metric-card .metric-label {{ font-size: 0.82rem; color: {GRIS}; }}
.metric-card.ok .metric-value {{ color: #1E6B3E; }}
.metric-card.warn .metric-value {{ color: #8A4307; }}
.metric-card.error .metric-value {{ color: #B42318; }}

.counter-box {{
    background: #FFFFFF; border: 1px solid {LINEA}; border-radius: 10px;
    padding: 1rem 1.15rem; margin: 1rem 0; display: flex; align-items: baseline; gap: 0.6rem;
}}
.counter-box .counter-num {{
    font-family: 'IBM Plex Mono', monospace; font-size: 1.5rem; font-weight: 500; color: {AZUL};
}}
.counter-box .counter-label {{ font-size: 0.9rem; color: {GRIS}; }}

[data-testid="stMetricValue"] {{
    font-family: 'IBM Plex Mono', monospace; font-size: 1.45rem; font-weight: 500;
}}
[data-testid="stMetricLabel"] p {{ color: {GRIS}; }}

/* ---------- Bloques de detalle ---------- */
.liq-card {{
    background: #FFFFFF; border: 1px solid {LINEA}; border-radius: 10px;
    padding: 0.9rem 1.15rem; margin: 0.6rem 0; font-size: 0.88rem;
}}
.liq-card .liq-id {{ font-weight: 600; color: {TINTA}; font-size: 0.92rem; }}
.liq-card .liq-detail {{ color: {GRIS}; margin-top: 0.3rem; }}
.liq-card.warn, .liq-card.naranja {{ background: #FFF7ED; border-color: #F3D3AE; }}
.liq-card.warn .liq-id, .liq-card.naranja .liq-id {{ color: #7A3B06; }}
.liq-card.warn .liq-detail, .liq-card.naranja .liq-detail {{ color: #7A3B06; }}

.pdf-list {{
    background: #FFFFFF; border: 1px solid {LINEA}; border-radius: 10px;
    padding: 0.5rem 1rem; margin: 0.75rem 0; max-height: 200px; overflow-y: auto;
}}
.pdf-item {{
    font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; color: {GRIS};
    padding: 0.35rem 0; border-bottom: 1px solid #EEF0F3;
}}
.pdf-item:last-child {{ border-bottom: none; }}

.divider {{ border: none; border-top: 1px solid {LINEA}; margin: 1.75rem 0; }}

/* ---------- Tarjetas del inicio (toda la tarjeta es el enlace) ---------- */
div[class*="st-key-card_"] {{ position: relative; height: 100%; }}
div[class*="st-key-card_"] .tarjeta {{
    background: #FFFFFF; border: 1px solid {LINEA}; border-radius: 10px;
    padding: 1.2rem 1.25rem; min-height: 8.5rem; box-sizing: border-box;
    display: flex; flex-direction: column; gap: 0.5rem;
    transition: border-color 0.15s, box-shadow 0.15s;
}}
div[class*="st-key-card_"] .tarjeta-icono {{ color: {AZUL}; line-height: 0; }}
div[class*="st-key-card_"] .tarjeta-titulo {{ font-weight: 600; font-size: 1rem; color: {TINTA}; }}
div[class*="st-key-card_"] .tarjeta-desc {{ font-size: 0.88rem; color: {GRIS}; }}
div[class*="st-key-card_"]:hover .tarjeta {{
    border-color: {AZUL}; box-shadow: 0 2px 8px rgba(5, 22, 187, 0.08);
}}
div[class*="st-key-card_"] [data-testid="stElementContainer"]:has([data-testid="stPageLink"]) {{
    position: absolute !important; inset: 0 !important; margin: 0 !important; z-index: 2;
    width: 100% !important; height: 100% !important;
}}
div[class*="st-key-card_"] [data-testid="stPageLink"],
div[class*="st-key-card_"] [data-testid="stPageLink"] a {{
    position: absolute !important; inset: 0 !important; width: 100% !important;
    height: 100% !important; margin: 0 !important; opacity: 0 !important;
}}
</style>
"""


def aplicar_estilos():
    st.markdown(_CSS, unsafe_allow_html=True)


def encabezado(titulo, descripcion="", ruta=""):
    """Encabezado de página: ruta de navegación, título y descripción."""
    partes = ['<div class="encabezado">']
    if ruta:
        partes.append(f'<div class="ruta">{html.escape(ruta)}</div>')
    partes.append(f'<h1>{html.escape(titulo)}</h1>')
    if descripcion:
        partes.append(f'<p>{html.escape(descripcion)}</p>')
    partes.append('</div>')
    st.markdown("".join(partes), unsafe_allow_html=True)


# Íconos de línea para las tarjetas del inicio (SVG en línea, color heredado).
ICONOS = {
    "compras": '<path d="M6 2h9l5 5v15H6z"/><path d="M14 2v6h6M9 13h8M9 17h6"/>',
    "ventas": '<path d="M4 20V10M10 20V4M16 20v-7M22 20H2"/>',
    "tesoreria": '<rect x="3" y="6" width="18" height="13" rx="2"/><path d="M3 10h18M16 15h2"/>',
    "bancos": '<path d="M3 10 12 4l9 6M5 10v8M9.5 10v8M14.5 10v8M19 10v8M3 21h18"/>',
    "rappi": '<circle cx="6" cy="17" r="3"/><circle cx="18" cy="17" r="3"/><path d="M9 17h6l-3-8h4M6 14l3-5"/>',
    "pedidosya": '<path d="M4 7h16l-1.5 13h-13z"/><path d="M9 7V5a3 3 0 0 1 6 0v2"/>',
    "impuestos": '<path d="M12 3 4 6v6c0 4.5 3.4 8 8 9 4.6-1 8-4.5 8-9V6z"/><path d="m9 12 2 2 4-4"/>',
    "pdfs": '<path d="M6 2h9l5 5v15H6z"/><path d="M14 2v6h6M9 14l2 2 4-4"/>',
}


def tarjeta(clave, icono, titulo, descripcion, pagina):
    """Tarjeta del inicio. Toda la superficie es un enlace a la página."""
    with st.container(key=f"card_{clave}"):
        st.markdown(
            f'<div class="tarjeta">'
            f'<div class="tarjeta-icono"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" '
            f'stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">'
            f'{ICONOS[icono]}</svg></div>'
            f'<div class="tarjeta-titulo">{html.escape(titulo)}</div>'
            f'<div class="tarjeta-desc">{html.escape(descripcion)}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        st.page_link(pagina, label=f"{titulo}: {descripcion}")
