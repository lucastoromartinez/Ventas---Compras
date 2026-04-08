import streamlit as st

st.set_page_config(
    page_title="Sistema de Cruces",
    page_icon="⚡",
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
.card-row {
    display: flex; gap: 1rem;
    margin: 2.5rem 0 1rem 0; justify-content: center;
    flex-wrap: wrap;
}
.card {
    flex: 1; background: #1a1a1a;
    border: 1px solid #2a2a2a; border-radius: 10px;
    padding: 1.5rem 1rem; text-align: center;
    max-width: 180px; min-width: 130px;
}
.card .card-icon { font-size: 2rem; margin-bottom: 0.8rem; }
.card .card-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.8rem; font-weight: 600;
    color: #ffffff; margin-bottom: 0.4rem;
}
.card .card-desc { font-size: 0.7rem; color: #555; line-height: 1.4; }
.stButton > button {
    font-family: 'IBM Plex Mono', monospace !important;
    font-weight: 600 !important; font-size: 0.8rem !important;
    letter-spacing: 1px !important; border: none !important;
    border-radius: 6px !important; padding: 0.7rem 1rem !important;
    width: 100% !important; transition: opacity 0.2s !important;
}
.stButton > button:hover { opacity: 0.85 !important; }
.btn-compras > div > button { background: #00ff87 !important; color: #0f0f0f !important; }
.btn-ventas  > div > button { background: #00aaff !important; color: #0f0f0f !important; }
.btn-concil  > div > button { background: #ff6b35 !important; color: #0f0f0f !important; }
.btn-pdfs    > div > button { background: #c084fc !important; color: #0f0f0f !important; }
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
    <h1>⚡ Sistema de Cruces</h1>
    <p>ARCA &nbsp;×&nbsp; Sistema Interno</p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="card-row">
    <div class="card">
        <div class="card-icon">🧾</div>
        <div class="card-title">Compras</div>
        <div class="card-desc">Comprobantes recibidos vs ARCA</div>
    </div>
    <div class="card">
        <div class="card-icon">📊</div>
        <div class="card-title">Ventas</div>
        <div class="card-desc">Comprobantes emitidos vs ARCA</div>
    </div>
    <div class="card">
        <div class="card-icon">🏦</div>
        <div class="card-title">Conciliaciones</div>
        <div class="card-desc">Mayor vs extracto bancario</div>
    </div>
    <div class="card">
        <div class="card-icon">📷</div>
        <div class="card-title">Lector PDFs</div>
        <div class="card-desc">Liquidaciones Payway a Excel</div>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown('<div class="btn-compras">', unsafe_allow_html=True)
    if st.button("🧾 COMPRAS", use_container_width=True):
        st.switch_page("pages/compras.py")
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="btn-ventas">', unsafe_allow_html=True)
    if st.button("📊 VENTAS", use_container_width=True):
        st.switch_page("pages/ventas.py")
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    st.markdown('<div class="btn-concil">', unsafe_allow_html=True)
    if st.button("🏦 CONCILIACIONES", use_container_width=True):
        st.switch_page("pages/conciliaciones.py")
    st.markdown('</div>', unsafe_allow_html=True)

with col4:
    st.markdown('<div class="btn-pdfs">', unsafe_allow_html=True)
    if st.button("📷 LECTOR PDFs", use_container_width=True):
        st.switch_page("pages/lector_pdfs.py")
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown("""
<div class="footer">Seleccioná un proceso para comenzar</div>
""", unsafe_allow_html=True)
