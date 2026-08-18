"""
Base de datos centralizada de proveedores.

Un único lugar para cargar/editar proveedores en vez de tener un
diccionario hardcodeado en cada logica_*.py. Cualquier módulo de la app
(Impuestos, Compras, Ventas, y los que se agreguen a futuro) puede hacer:

    from proveedores import buscar_por_cuit, buscar_por_nombre

Para agregar o corregir un proveedor, editá el diccionario PROVEEDORES
de abajo y hacé commit.

Formato de cada entrada (clave = CUIT sin guiones ni espacios):
    "<CUIT>": {
        "nombre": "<Razón social de referencia>",
        "alias": ["<variantes del nombre que puedan aparecer en ARCA o en el sistema>"],
    }
"""

from rapidfuzz import fuzz, process, utils

PROVEEDORES: dict[str, dict] = {
    # Ejemplo (dejalo como referencia de formato o borralo cuando cargues los reales):
    # "30500001735": {
    #     "nombre": "BANCO DE GALICIA Y BUENOS AIRES S.A.",
    #     "alias": ["BANCO GALICIA", "GALICIA"],
    # },
}


def normalizar_cuit(cuit) -> str:
    """Deja solo dígitos, para poder comparar CUITs sin importar guiones/espacios."""
    return "".join(ch for ch in str(cuit) if ch.isdigit())


def buscar_por_cuit(cuit) -> dict | None:
    """Devuelve la info del proveedor para un CUIT, o None si no está cargado."""
    return PROVEEDORES.get(normalizar_cuit(cuit))


def buscar_por_nombre(nombre: str, score_minimo: int = 85) -> tuple[str, dict] | None:
    """
    Busca, por nombre o alias, el proveedor más parecido a `nombre` (fuzzy matching).
    Devuelve (cuit, info) del mejor candidato, o None si ninguno supera score_minimo.
    """
    if not nombre or not PROVEEDORES:
        return None

    candidatos = {}
    for cuit, info in PROVEEDORES.items():
        candidatos[cuit] = info["nombre"]
        for alias in info.get("alias", []):
            candidatos[f"{cuit}::{alias}"] = alias

    match = process.extractOne(
        nombre, candidatos, scorer=fuzz.token_sort_ratio, processor=utils.default_process
    )
    if match is None:
        return None

    _, score, key = match
    if score < score_minimo:
        return None

    cuit = key.split("::")[0]
    return cuit, PROVEEDORES[cuit]
