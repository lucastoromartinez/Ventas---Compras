import streamlit as st

from estilos import aplicar_estilos, encabezado, tarjeta

aplicar_estilos()
encabezado("Inicio", "Seleccione el proceso a ejecutar.")

# Si "_pages" no está en session_state (se entró directo a esta página sin
# pasar por app_principal), se usa la ruta del archivo.
PAGES = st.session_state.get("_pages", {})


def _pagina(clave, ruta):
    return PAGES.get(clave, ruta)


SECCIONES = [
    ("Comprobantes", [
        ("compras", "compras", "Compras", "Comprobantes recibidos contra ARCA", "pages/compras.py"),
        ("ventas", "ventas", "Ventas", "Comprobantes emitidos contra ARCA", "pages/ventas.py"),
    ]),
    ("Conciliaciones", [
        ("conciliaciones", "bancos", "Bancos", "Mayor contra extracto bancario", "pages/conciliaciones.py"),
        ("tesoreria", "tesoreria", "Tesorería", "Caja Central contra contabilidad", "pages/tesoreria.py"),
        ("rappi", "rappi", "Rappi", "Liquidaciones, conciliación y Atalaya", "pages/rappi.py"),
        ("pedidosya", "pedidosya", "Pedidos Ya", "Conciliación cuenta recaudación", "pages/pedidosya.py"),
    ]),
    ("Impuestos y herramientas", [
        ("impuestos", "impuestos", "Impuestos", "Percepciones y retenciones contra sistema", "pages/impuestos.py"),
        ("lector_pdfs", "pdfs", "Lector de PDFs", "Liquidaciones Payway a Excel", "pages/lector_pdfs.py"),
    ]),
]

COLUMNAS = 4
for titulo, tarjetas in SECCIONES:
    st.markdown(f'<div class="section-title">{titulo}</div>', unsafe_allow_html=True)
    for inicio in range(0, len(tarjetas), COLUMNAS):
        cols = st.columns(COLUMNAS, gap="medium")
        for col, (clave, icono, nombre, desc, ruta) in zip(cols, tarjetas[inicio:inicio + COLUMNAS]):
            with col:
                tarjeta(clave, icono, nombre, desc, _pagina(clave, ruta))
