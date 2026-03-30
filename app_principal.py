import streamlit as st

st.set_page_config(
    page_title="Sistema de Cruces",
    page_icon="⚡",
    layout="centered",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}
.stApp {
    background-color: #0f0f0f;
    color: #e8e8e8;
}
.hero {
    text-align: center;
    padding: 3rem 0 2rem 0;
}
.hero h1 {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 2rem;
    font-weight: 600;
    color: #ffffff;
    margin: 0;
    letter-spacing: -1px;
}
.hero p {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.8rem;
    color: #444;
    margin: 0.5rem 0 0 0;
    letter-spacing: 2px;
    text-transform: uppercase;
}
.card-row {
    display: flex;
    gap: 1.5rem;
    margin: 3rem 0;
    justify-content: center;
}
.card {
    flex: 1;
    background: #1a1a1a;
    border: 1px solid #2a2a2a;
    border-radius: 10px;
    padding: 2rem 1.5rem;
    text-align: center;
    transition: border-color 0.2s, transform 0.2s;
    cursor: pointer;
    max-width: 260px;
}
.card:hover {
    border-color: #00aaff;
    transform: translateY(-2px);
}
.card .card-icon {
    font-size: 2.5rem;
    margin-bottom: 1rem;
}
.card .card-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1rem;
    font-weight: 600;
    color: #ffffff;
    margin-bottom: 0.5rem;
}
.card .card-desc {
    font-size: 0.78rem;
    color: #555;
    line-height: 1.5;
}
.card.compras:hover { border-color: #00ff87; }
.card.ventas:hover  { border-color: #00aaff; }

.stButton > button {
    font-family: 'IBM Plex Mono', monospace !important;
    font-weight: 600 !important;
    font-size: 0.85rem !important;
    letter-spacing: 1px !important;
    border: none !important;
    border-radius: 6px !important;
    padding: 0.8rem 2rem !important;
    width: 100% !important;
    transition: opacity 0.2s !important;
}
.stButton > button:hover { opacity: 0.85 !important; }

.footer {
    text-align: center;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.65rem;
    color: #333;
    margin-top: 4rem;
    letter-spacing: 1px;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# HERO
# ─────────────────────────────────────────────
st.markdown("""
<div class="hero">
    <h1>⚡ Sistema de Cruces</h1>
    <p>ARCA &nbsp;×&nbsp; Sistema Interno</p>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# CARDS
# ─────────────────────────────────────────────
st.markdown("""
<div class="card-row">
    <div class="card compras">
        <div class="card-icon">🧾</div>
        <div class="card-title">Cruce de Compras</div>
        <div class="card-desc">Cruza comprobantes recibidos del sistema contra ARCA</div>
    </div>
    <div class="card ventas">
        <div class="card-icon">📊</div>
        <div class="card-title">Cruce de Ventas</div>
        <div class="card-desc">Cruza comprobantes emitidos del sistema contra ARCA</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# BOTONES DE NAVEGACIÓN
# ─────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    if st.button("🧾  IR A COMPRAS", use_container_width=True):
        st.switch_page("pages/compras.py")

with col2:
    if st.button("📊  IR A VENTAS", use_container_width=True):
        st.switch_page("pages/ventas.py")

# ─────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────
st.markdown("""
<div class="footer">
    Seleccioná un proceso para comenzar
</div>
""", unsafe_allow_html=True)
