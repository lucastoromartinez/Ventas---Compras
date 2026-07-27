import streamlit as st

st.set_page_config(
    page_title="Sistema de Cruces",
    page_icon="🧮",
    layout="centered",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.stApp { background-color: #0f0f0f; color: #e8e8e8; }
.hero {
    text-align: center;
    padding: 3rem 0 2rem 0;
}
.hero h1 {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 2rem; font-weight: 600;
    color: #ffffff; margin: 0; letter-spacing: -1px;
}
.hero p {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.8rem; color: #444;
    margin: 0.5rem 0 0 0;
    letter-spacing: 2px; text-transform: uppercase;
}

/* --- Cards que son, a la vez, el botón --- */
.card-btn {
    position: relative;
    margin-bottom: 1.2rem;
}
.card-btn .card-visual {
    background: #1a1a1a;
    border: 1px solid #2a2a2a;
    border-radius: 10px;
    padding: 1.6rem 1rem;
    text-align: center;
    height: 100%;
    pointer-events: none;
    transition: border-color 0.15s, transform 0.15s;
}
.card-btn .card-icon { font-size: 2rem; margin-bottom: 0.8rem; }
.card-btn .card-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.85rem; font-weight: 600;
    color: #ffffff; margin-bottom: 0.4rem;
}
.card-btn .card-desc { font-size: 0.7rem; color: #666; line-height: 1.4; }

.card-btn [data-testid="stButton"] {
    position: absolute; inset: 0; margin: 0; z-index: 2;
}
.card-btn [data-testid="stButton"] > button {
    position: absolute; inset: 0;
    width: 100%; height: 100%;
    opacity: 0; cursor: pointer; padding: 0; border: none;
}
.card-btn:hover .card-visual { transform: translateY(-3px); }
.card-btn.acc-compras:hover .card-visual { border-color: #00ff87; }
.card-btn.acc-ventas:hover .card-visual  { border-color: #00aaff; }
.card-btn.acc-concil:hover .card-visual  { border-color: #ff6b35; }
.card-btn.acc-pdfs:hover .card-visual    { border-color: #c084fc; }
.card-btn.acc-rappi:hover .card-visual   { border-color: #FF441F; }

.footer {
    text-align: center;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.65rem; color: #333;
    margin-top: 3rem; letter-spacing: 1px;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="hero">
    <h1>🧮 Sistema de Cruces</h1>
    <p>Contabilidad Enter</p>
</div>
""", unsafe_allow_html=True)


def card(col, key, accent, icon, title, desc, page):
    with col:
        st.markdown(f'<div class="card-btn acc-{accent}">', unsafe_allow_html=True)
        st.markdown(f"""
        <div class="card-visual">
            <div class="card-icon">{icon}</div>
            <div class="card-title">{title}</div>
            <div class="card-desc">{desc}</div>
        </div>
        """, unsafe_allow_html=True)
        clicked = st.button(f"{title} - {desc}", key=key)
        st.markdown('</div>', unsafe_allow_html=True)
        if clicked:
            st.switch_page(page)


row1 = st.columns(3, gap="medium")
card(row1[0], "nav_compras", "compras", "🧾", "Compras", "Comprobantes recibidos vs ARCA", "pages/compras.py")
card(row1[1], "nav_ventas", "ventas", "📊", "Ventas", "Comprobantes emitidos vs ARCA", "pages/ventas.py")
card(row1[2], "nav_concil", "concil", "🏦", "Conciliaciones", "Mayor vs extracto bancario", "pages/conciliaciones.py")

row2 = st.columns([1, 2, 2, 1], gap="medium")
card(row2[1], "nav_pdfs", "pdfs", "📷", "Lector PDFs", "Liquidaciones Payway a Excel", "pages/lector_pdfs.py")
card(row2[2], "nav_rappi", "rappi", "🛵", "Rappi", "Liquidaciones y conciliación Atalaya", "pages/rappi.py")

st.markdown("""
<div class="footer">Seleccioná un proceso para comenzar</div>
""", unsafe_allow_html=True)
