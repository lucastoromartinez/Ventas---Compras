import streamlit as st

from estilos import LOGO_COMPLETO, LOGO_ICONO

st.set_page_config(
    page_title="Sistema de Cruces",
    page_icon=LOGO_ICONO,
    layout="wide",
)

# Se guardan los objetos Page (no solo las rutas) en session_state para que
# cada página pueda navegar con st.switch_page(objeto) o st.page_link(objeto)
# en vez de un string: con string la ruta se resuelve contra el filesystem del
# servidor y en Streamlit Cloud eso puede fallar ("Could not find page"); con
# el objeto Page no hace falta esa resolución.
# url_path fijo: las direcciones de cada módulo no cambian aunque se renombre
# un archivo.
PAGES = {
    "home":           st.Page("app_home.py",             title="Inicio",          icon=":material/home:",                   url_path="", default=True),
    "compras":        st.Page("pages/compras.py",        title="Compras",         icon=":material/receipt_long:",           url_path="compras"),
    "ventas":         st.Page("pages/ventas.py",         title="Ventas",          icon=":material/bar_chart:",              url_path="ventas"),
    "conciliaciones": st.Page("pages/conciliaciones.py", title="Bancos",          icon=":material/account_balance:",        url_path="conciliaciones"),
    "tesoreria":      st.Page("pages/tesoreria.py",      title="Tesorería",       icon=":material/account_balance_wallet:", url_path="tesoreria"),
    "rappi":          st.Page("pages/rappi.py",          title="Rappi",           icon=":material/two_wheeler:",            url_path="rappi"),
    "pedidosya":      st.Page("pages/pedidosya.py",      title="Pedidos Ya",      icon=":material/shopping_bag:",           url_path="pedidosya"),
    "impuestos":      st.Page("pages/impuestos.py",      title="Impuestos",       icon=":material/policy:",                 url_path="impuestos"),
    "lector_pdfs":    st.Page("pages/lector_pdfs.py",    title="Lector de PDFs",  icon=":material/picture_as_pdf:",         url_path="lector_pdfs"),
}
st.session_state["_pages"] = PAGES

st.logo(LOGO_COMPLETO, icon_image=LOGO_ICONO, size="large")

pg = st.navigation({
    "": [PAGES["home"]],
    "Comprobantes": [PAGES["compras"], PAGES["ventas"]],
    "Conciliaciones": [PAGES["conciliaciones"], PAGES["tesoreria"], PAGES["rappi"], PAGES["pedidosya"]],
    "Impuestos": [PAGES["impuestos"]],
    "Herramientas": [PAGES["lector_pdfs"]],
})
pg.run()
