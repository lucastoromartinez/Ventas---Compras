import streamlit as st

pg = st.navigation([
    st.Page("app_home.py",      title="Inicio",  icon="⚡", default=True),
    st.Page("pages/compras.py", title="Compras", icon="🧾"),
    st.Page("pages/ventas.py",  title="Ventas",  icon="📊"),
])
pg.run()
