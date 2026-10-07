#!/usr/bin/env python3
"""
Funciones compartidas: configuracion por entidad, normalizacion de columnas,
lectura de los sistemas internos (Dean, HIO, Atalaya) y de la Lista de PeYa.

Todo lo que depende de la entidad sale de config/entidades.yaml. Los scripts
reciben --entidad y de ahi deducen que sistemas cruzar y con que criterio.
"""
from __future__ import annotations

import os
import re
import unicodedata

import numpy as np
import pandas as pd
import yaml

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG = os.path.join(RAIZ, "config", "entidades.yaml")

NETA = "Monto de Venta Neta ($)"
BRUTA = "Monto bruto de la venta"
PED = "Número de pedido"
FPED = "Fecha de pedido"
SUC = "Sucursal"
MET = "Método de pago"
REINT = "Monto neto a reintegrar"
MPED = "Monto del Pedido"


# ----------------------------------------------------------------- helpers
def norm(x) -> str:
    """minusculas, sin acentos, sin simbolos. Para comparar encabezados y nombres."""
    x = unicodedata.normalize("NFKD", str(x)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", x.lower())


def col(df: pd.DataFrame, *palabras):
    """Primera columna cuyo encabezado normalizado contiene todas las palabras."""
    for c in df.columns:
        n = norm(c)
        if all(norm(p) in n for p in palabras):
            return c
    return None


def ids(serie: pd.Series) -> pd.Series:
    """Identificadores a texto sin decimales (2220387818.0 -> '2220387818')."""
    return pd.to_numeric(serie.astype(str).str.strip(), errors="coerce").astype("Int64").astype(str)


def fecha(serie: pd.Series, dayfirst=False) -> pd.Series:
    return pd.to_datetime(serie, dayfirst=dayfirst, errors="coerce").dt.normalize()


def num(serie: pd.Series) -> pd.Series:
    return pd.to_numeric(serie, errors="coerce").fillna(0.0).astype(float)


def mes(serie: pd.Series) -> pd.Series:
    return pd.to_datetime(serie, errors="coerce").dt.to_period("M").astype(str)


def rango_txt(a: pd.Timestamp, b: pd.Timestamp) -> str:
    return f"{a:%d/%m} al {b:%d/%m}"


def periodo_txt(desde: str, hasta: str) -> str:
    """'23/02/2026','01/03/2026' -> '23/02-01/03' (como lo nombra el usuario)."""
    return f"{desde[:5]}-{hasta[:5]}"


def leer_config(entidad: str) -> dict:
    cfg = yaml.safe_load(open(CONFIG, encoding="utf-8"))
    if entidad not in cfg:
        raise SystemExit(f"Entidad '{entidad}' no esta en {CONFIG}. Disponibles: "
                         f"{', '.join(k for k in cfg if isinstance(cfg[k], dict) and 'sistemas' in cfg[k])}")
    c = cfg[entidad]
    c["id"] = entidad
    return c


def detectar_entidad(carpeta_zips: str) -> str | None:
    """Deduce la entidad por el numero de local que traen los nombres de los ZIP."""
    cfg = yaml.safe_load(open(CONFIG, encoding="utf-8"))
    nums = {str(v.get("local_peya")): k for k, v in cfg.items() if isinstance(v, dict) and "sistemas" in v}
    vistos = set()
    for f in os.listdir(carpeta_zips):
        for m in re.findall(r"(?<!\d)(\d{6})(?!\d)", f):
            if m in nums:
                vistos.add(nums[m])
    return vistos.pop() if len(vistos) == 1 else None


# ----------------------------------------------------------- PeYa (Lista)
def local_peya(suc: pd.Series, sistema: dict) -> pd.Series:
    """Mapea la Sucursal de la Lista al local del sistema (clave de 'locales')."""
    locs = sistema.get("locales") or {}
    out = pd.Series([None] * len(suc), index=suc.index, dtype=object)
    s = suc.astype(str).map(norm)
    for clave, d in locs.items():
        pat = norm(d.get("peya", clave))
        out = out.where(~s.str.contains(pat, na=False, regex=False), clave)
    return out


def peya_del_sistema(lista: pd.DataFrame, sistema: dict) -> pd.DataFrame:
    """Pedidos de la Lista que corresponden a las sucursales del sistema."""
    p = lista[lista[SUC].astype(str).str.contains(sistema["sucursal_peya"], case=False, na=False, regex=True)].copy()
    p["ped"] = ids(p[PED])
    p["F"] = fecha(p[FPED])
    p["loc"] = local_peya(p[SUC], sistema)
    p["fuera"] = p[MET].astype(str).str.contains("fuera", case=False, na=False)
    return p


def base_facturacion(p: pd.DataFrame) -> pd.Series:
    """BASE = bruta - dto comercial PeYa x1,21 - dto local - cupon local."""
    C = lambda c: num(p[c]) if c in p.columns else 0.0
    return (C(BRUTA) - C("Descuento comercial neto otorgado x PeYa") * 1.21
            - C("Descuento otorgado por el local") - C("Cupon otorgado por el local")).round(2)


def correcto_facturacion(p: pd.DataFrame) -> pd.Series:
    """CORRECTO = BASE - dto neto PeYa a usuarios x1,21."""
    C = lambda c: num(p[c]) if c in p.columns else 0.0
    return (base_facturacion(p) - C("Descuento neto otorgado Peya a usuarios") * 1.21).round(2)


def importe_criterio(p: pd.DataFrame, criterio: str) -> pd.Series:
    if criterio == "fecha_local_bruta":
        return num(p[BRUTA]).round(2)
    if criterio == "fecha_local_base":
        return base_facturacion(p)
    if criterio == "fecha_facturacion":
        # pago fuera de la app se factura bien (CORRECTO); pago en la app por BASE
        return pd.Series(np.where(p["fuera"], correcto_facturacion(p), base_facturacion(p)),
                         index=p.index).round(2)
    raise ValueError(f"criterio desconocido: {criterio}")


# ------------------------------------------------------ sistemas internos
def leer_dean(ruta: str) -> pd.DataFrame:
    """Dean: una fila por transaccion. Devuelve F, id, importe, medio, mas las originales."""
    d = pd.read_excel(ruta)
    if "Servicio" in d.columns:
        d = d[d["Servicio"].astype(str).str.contains("PedidosYa", na=False)].copy()
    d["F"] = fecha(d["Fecha"], dayfirst=True)
    for c in ("Online", "Efectivo"):
        d[c] = num(d[c])
    d["id"] = d["Nº Transacción"].astype(str).str.strip()
    d["importe"] = d["Online"] + d["Efectivo"]
    d["medio"] = np.where(d["Efectivo"] != 0, "efectivo", "online")
    d["Importe"] = d["importe"]
    d["Metodo de Pago"] = np.where(d["Efectivo"] != 0, "Efectivo", "Online")
    d["loc"] = None
    return d


def leer_hio(ruta_doc: str, ruta_met: str, sistema: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """HIO: combina el reporte por documento con el de metodo de pago.

    Devuelve (combinado, metodo_pedidosya). El combinado trae F, id (localizador),
    importe (Venta), medio y loc. El reporte por metodo es la fuente de referencia
    de las ventas (es el que coincide con el mayor).
    """
    doc = pd.read_excel(ruta_doc)
    met = pd.read_excel(ruta_met)
    met.columns = [str(c).strip() for c in met.columns]
    sn_d, sn_m = col(doc, "serie"), col(met, "serie")
    mp = col(met, "medio", "pago")
    met["sn"] = met[sn_m].astype(str).str.strip()
    py = met[met[mp].astype(str).str.contains("PEDIDOS", na=False)].copy()
    py["F"] = fecha(py[col(py, "fecha")])
    py["medio"] = np.where(py[mp].astype(str).str.contains("EFECTIVO", case=False), "efectivo", "online")
    py["importe"] = num(py[col(py, "total")])

    doc["sn"] = doc[sn_d].astype(str).str.strip()
    vac = {"", "nan", "none", "nat", "<na>"}
    doc = doc[~doc["sn"].str.lower().isin(vac)]
    comb = doc[doc["sn"].isin(set(py["sn"]))].copy()
    comb["F"] = fecha(comb[col(comb, "fecha")])
    comb["id"] = ids(comb[col(comb, "localizador")])
    comb["importe"] = num(comb[col(comb, "venta")])
    g = py.groupby("sn")
    comb["medio"] = comb["sn"].map(g["medio"].first())
    comb["Medio Pago"] = comb["sn"].map(g[mp].first())
    # el reporte por metodo es el que coincide con el mayor; el documento es el que
    # trae el localizador y alimenta los cruces. Cuando difieren hay que nombrarlo.
    comb["Importe por metodo"] = comb["sn"].map(g["importe"].sum())
    est = col(comb, "establecimiento")
    comb["loc"] = None
    e = comb[est].astype(str).map(norm)
    for clave, d in (sistema.get("locales") or {}).items():
        pat = norm(d.get("hio", clave))
        comb.loc[e.str.contains(pat, na=False, regex=False), "loc"] = clave
    comb["Local"] = comb["loc"]
    return comb, py


def leer_atalaya(ruta: str, sistema: dict) -> pd.DataFrame:
    a = pd.read_excel(ruta)
    cmp = next((c for c in a.columns if norm(c) == "mediopago"), None) or col(a, "medio", "pago")
    # el reporte puede traer todos los medios de pago (Tarjeta, Efectivo, Rappi...):
    # solo cuentan los de PedidosYa ("Pedidos Ya", "Peya Efectivo", "PEDIDOS YA TARJETA")
    mp = a[cmp].astype(str).map(norm)
    a = a[mp.str.contains("pedidosya") | mp.str.contains("peya")].copy()
    a["F"] = fecha(a[col(a, "fecha")])
    a["id"] = None
    a["importe"] = num(a[col(a, "con iva")])
    a["medio"] = np.where(a[cmp].astype(str).str.contains("efectivo", case=False, na=False), "efectivo", "online")
    a["loc"] = list(sistema.get("locales") or {"Atalaya": {}})[0]
    return a


def leer_sistema(sistema: dict, args) -> dict:
    """Devuelve {'tickets': df, 'ventas': df} segun el tipo del sistema."""
    t = sistema["tipo"]
    if t == "dean":
        if not args.dean:
            raise SystemExit("Falta --dean")
        d = leer_dean(args.dean)
        return {"tickets": d, "ventas": d}
    if t == "hio":
        if not (args.hio_documento and args.hio_metodo):
            raise SystemExit("Faltan --hio-documento y --hio-metodo")
        comb, py = leer_hio(args.hio_documento, args.hio_metodo, sistema)
        return {"tickets": comb, "ventas": py}
    if t == "atalaya":
        if not args.atalaya:
            raise SystemExit("Falta --atalaya")
        a = leer_atalaya(args.atalaya, sistema)
        return {"tickets": a, "ventas": a}
    raise ValueError(f"tipo de sistema desconocido: {t}")


def en_scope(df: pd.DataFrame, ini, fin, c="F") -> pd.DataFrame:
    return df[(df[c] >= ini) & (df[c] <= fin)].copy()


def scope_liquidaciones(liq: list) -> tuple[pd.Timestamp, pd.Timestamp]:
    f = lambda s: pd.Timestamp(f"{s[6:]}-{s[3:5]}-{s[:2]}")
    return f(liq[0]["desde"]), f(liq[-1]["hasta"])


def args_sistemas(ap):
    """Argumentos de archivos de los sistemas internos (todos opcionales)."""
    ap.add_argument("--dean", help="ventas Dean (EASA)")
    ap.add_argument("--hio-documento", help="reporte HIO por documento")
    ap.add_argument("--hio-metodo", help="reporte HIO por metodo de pago")
    ap.add_argument("--atalaya", help="tickets Atalaya (Ronda)")
