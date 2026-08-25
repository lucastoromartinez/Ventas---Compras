import streamlit as st

# Se guardan los objetos Page (no solo las rutas) en session_state para que
# cada página pueda navegar con st.switch_page(objeto) en vez de un string:
# switch_page con string resuelve la ruta contra el filesystem del servidor
# y en Streamlit Cloud eso puede fallar ("Could not find page"); con el
# objeto Page no hace falta esa resolución.
PAGES = {
    "home":           st.Page("app_home.py",              title="Inicio",         icon="⚡", default=True),
    "compras":        st.Page("pages/compras.py",         title="Compras",        icon="🧾"),
    "ventas":         st.Page("pages/ventas.py",          title="Ventas",         icon="📊"),
    "tesoreria":      st.Page("pages/tesoreria.py",       title="Tesorería",      icon="💰"),
    "conciliaciones": st.Page("pages/conciliaciones.py",  title="Conciliaciones", icon="🏦"),
    "lector_pdfs":    st.Page("pages/lector_pdfs.py",     title="Lector PDFs",    icon="📷"),
    "rappi":          st.Page("pages/rappi.py",           title="Rappi",          icon="🛵"),
    "impuestos":      st.Page("pages/impuestos.py",       title="Impuestos",      icon="👮"),
}
st.session_state["_pages"] = PAGES

pg = st.navigation(list(PAGES.values()))
pg.run()
