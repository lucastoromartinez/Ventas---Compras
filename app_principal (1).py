import streamlit as st

pg = st.navigation([
    st.Page("app_home.py",              title="Inicio",         icon="⚡", default=True),
    st.Page("pages/compras.py",         title="Compras",        icon="🧾"),
    st.Page("pages/ventas.py",          title="Ventas",         icon="📊"),
    st.Page("pages/conciliaciones.py",  title="Conciliaciones", icon="🏦"),
    st.Page("pages/lector_pdfs.py",     title="Lector PDFs",    icon="📷"),
])
pg.run()
