#!/usr/bin/env python3
"""
Cruza la Lista de Pedidos de PedidosYa contra los sistemas internos de la
entidad (los que define config/entidades.yaml).

Por sistema:
  1) cruce por identificador (Nº Transaccion en Dean, Localizador en HIO),
     uno a uno. Atalaya no tiene identificador: saltea este paso.
  2) de los tickets que quedaron sin pedido, se apartan los que tienen un
     reintegro (pedido rechazado: PeYa lo saca de la Lista y lo manda a
     Reintegros). Si no se apartan antes del cruce 2, se matchean por fecha y
     monto contra otro pedido y el reintegro queda sin su venta.
  3) cruce secundario por fecha (+ local) + importe, uno a uno, con el criterio
     de importe del YAML (bruta / BASE / criterio de facturacion). Dean no lo
     tiene: siempre integra, y buscar por fecha+monto produce matches espurios.
  4) en Atalaya los reintegros se identifican despues del cruce, por fecha y
     Monto del Pedido contra los tickets que quedaron sin pedido.

Todo se acota al scope de las liquidaciones (fechas de liquidaciones.json).

USO
---
    python cruzar.py --entidad easa --trabajo ./trabajo --dean Dean.xlsx \\
        --hio-documento hio_doc.xlsx --hio-metodo hio_met.xlsx
    python cruzar.py --entidad ronda --trabajo ./trabajo \\
        --hio-documento hio_doc.xlsx --hio-metodo hio_met.xlsx --atalaya atalaya.xlsx

Genera en la carpeta de trabajo cruce_<sistema>.xlsx (una hoja por resultado)
y cruces.pkl (lo que lee conciliar.py).
"""
from __future__ import annotations

import argparse
import json
import os

import numpy as np
import pandas as pd

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import comun as K  # noqa: E402


def cruce_id(p: pd.DataFrame, t: pd.DataFrame, nombre_imp: str):
    """Uno a uno por identificador. Recorre los tickets en su orden y toma el
    primer pedido libre con ese numero."""
    libres = {}
    for ped, idx in zip(p["ped"], p.index):
        libres.setdefault(ped, []).append(idx)
    pares = []
    for i, tid in zip(t.index, t["id"]):
        cand = libres.get(tid)
        if cand:
            pares.append((cand.pop(0), i))
    ip = [a for a, _ in pares]
    it = [b for _, b in pares]
    mp = p.loc[ip].copy()
    mp[nombre_imp] = t.loc[it, "importe"].values
    return mp, t.loc[it].copy(), p.drop(index=ip), t.drop(index=it)


def cruce_fecha(p: pd.DataFrame, t: pd.DataFrame, criterio: str, usar_local: bool, nombre_imp: str,
                redondeo: float = 0):
    """Uno a uno por fecha (+ local) + importe. Recorre los pedidos en su orden
    y toma el primer ticket libre que coincida. Primero por el importe exacto del
    criterio; si el sistema redondea el ticket hacia abajo (redondeo = 100: a la
    centena), una segunda pasada con el importe redondeado. La diferencia queda
    en la linea "Diferencia ... match", como cualquier otra."""
    p = p.copy()
    t = t.copy()
    p["_m"] = K.importe_criterio(p, criterio)
    t["_m"] = t["importe"].round(2)
    pool = {}
    for i, f, l, m in zip(t.index, t["F"], t["loc"], t["_m"]):
        pool.setdefault((f, l if usar_local else None, m), []).append(i)
    pares = []
    pasadas = [lambda m: m] + ([lambda m: float(np.floor(m / redondeo) * redondeo)] if redondeo else [])
    hechos = set()
    for paso in pasadas:
        for j, f, l, m in zip(p.index, p["F"], p["loc"], p["_m"]):
            if j in hechos:
                continue
            cand = pool.get((f, l if usar_local else None, paso(m)))
            if cand:
                pares.append((j, cand.pop(0)))
                hechos.add(j)
    ip = [a for a, _ in pares]
    it = [b for _, b in pares]
    mp = p.loc[ip].drop(columns="_m")
    mp[nombre_imp] = t.loc[it, "importe"].values
    return mp, t.loc[it].drop(columns="_m"), p.drop(index=ip).drop(columns="_m"), t.drop(index=it).drop(columns="_m")


def cruzar_sistema(sis: dict, lista: pd.DataFrame, rein: pd.DataFrame, tickets: pd.DataFrame) -> dict:
    nombre_imp = f"Importe {sis['etiqueta_corta'] if sis['tipo'] != 'hio' else 'Hio'}"
    if sis["tipo"] == "dean":
        nombre_imp = "Importe Dean"
    if sis["tipo"] == "atalaya":
        nombre_imp = "Importe Atalaya"
    p = K.peya_del_sistema(lista, sis)
    r = rein[rein[K.SUC].astype(str).str.contains(sis["sucursal_peya"], case=False, na=False, regex=True)].copy()
    r["ped"] = K.ids(r[K.PED])
    r["F"] = K.fecha(r["Fecha del pedido"])
    r["monto"] = K.num(r[K.MPED]).round(2)
    r["loc"] = K.local_peya(r[K.SUC], sis)
    print(f"\n{sis['nombre']}: {len(p)} pedidos PeYa | {len(tickets)} tickets | {len(r)} reintegros")

    vacio_p = p.iloc[0:0]
    vacio_t = tickets.iloc[0:0]
    tiene_id = tickets["id"].notna().any()
    if tiene_id:
        m1p, m1t, rp, rt = cruce_id(p, tickets, nombre_imp)
        print(f"  cruce 1 (identificador): {len(m1p)}")
        # tickets con reintegro: pedido rechazado, salen del pool antes del cruce 2
        en_r = rt["id"].isin(set(r["ped"]))
        expl, rt = rt[en_r].copy(), rt[~en_r].copy()
        print(f"  tickets explicados por reintegro: {len(expl)}")
        # si el local ya anulo el ticket por sistema (ticket negativo por el mismo
        # importe), la venta neta interna es cero: no corresponde NC por el
        # complemento, el reintegro es un ingreso sin venta (ND). El par sale
        # del pool para que no infle "Falta PeYa".
        neg = rt[rt["importe"] < 0]
        pool = {}
        for i, f, l, m in zip(neg.index, neg["F"], neg["loc"], neg["importe"].round(2)):
            pool.setdefault((l, -m), []).append((f, i))
        anul, anul_neg = [], []
        for i, f, l, m in zip(expl.index, expl["F"], expl["loc"], expl["importe"].round(2)):
            cand = [x for x in pool.get((l, m), []) if x[0] >= f]
            if cand:
                pool[(l, m)].remove(cand[0])
                anul.append(i)
                anul_neg.append(cand[0][1])
        anulados = pd.concat([expl.loc[anul], rt.loc[anul_neg]])
        expl, rt = expl.drop(index=anul), rt.drop(index=anul_neg)
        if anul:
            print(f"  tickets anulados por sistema (ticket + contra-ticket): {len(anul)} pares")
    else:
        m1p, m1t, rp, rt, expl, anulados = vacio_p, vacio_t, p, tickets, vacio_t, vacio_t

    crit = sis.get("cruce_secundario")
    if crit:
        usar_local = sis["tipo"] != "atalaya"
        m2p, m2t, fs, fp = cruce_fecha(rp, rt, crit, usar_local, nombre_imp, sis.get("redondeo_ticket", 0) or 0)
        print(f"  cruce 2 ({crit}" + (f", ticket redondeado a {sis['redondeo_ticket']}" if sis.get("redondeo_ticket") else "") + f"): {len(m2p)}")
    else:
        m2p, m2t, fs, fp = vacio_p, vacio_t, rp, rt

    if not tiene_id:
        # Atalaya: el reintegro se reconoce por fecha + monto entre los tickets sin pedido
        pool = {}
        for i, f, m in zip(fp.index, fp["F"], fp["importe"].round(2)):
            pool.setdefault((f, m), []).append(i)
        usados = []
        for f, m in zip(r["F"], r["monto"]):
            cand = pool.get((f, m))
            if cand:
                usados.append(cand.pop(0))
        expl, fp = fp.loc[usados].copy(), fp.drop(index=usados)
        print(f"  tickets explicados por reintegro: {len(expl)}")

    print(f"  falta {sis['etiqueta_corta']} (PeYa liquido, no hay ticket): {len(fs)} | "
          f"falta PeYa (ticket sin pedido): {len(fp)}")
    return {"peya": p, "tickets": tickets, "reint": r, "m1_peya": m1p, "m1_tk": m1t,
            "m2_peya": m2p, "m2_tk": m2t, "falta_sistema": fs, "falta_peya": fp,
            "explicados": expl, "anulados": anulados, "nombre_imp": nombre_imp}


def limpiar(df: pd.DataFrame) -> pd.DataFrame:
    """Saca las columnas auxiliares antes de escribir una hoja."""
    aux = [c for c in ("ped", "F", "loc", "fuera", "id", "importe", "medio", "sn", "monto") if c in df.columns]
    return df.drop(columns=aux)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--entidad", required=True)
    ap.add_argument("--trabajo", required=True)
    K.args_sistemas(ap)
    a = ap.parse_args()
    cfg = K.leer_config(a.entidad)

    liq = json.load(open(os.path.join(a.trabajo, "liquidaciones.json")))
    ini, fin = K.scope_liquidaciones(liq)
    print(f"Entidad {cfg['nombre']} · scope {ini:%d/%m/%Y} al {fin:%d/%m/%Y}")

    xp = pd.ExcelFile(os.path.join(a.trabajo, "archivos_peya.xlsx"))
    lista, rein = xp.parse("Lista Pedidos"), xp.parse("Reintegros")
    lista = lista[(K.fecha(lista[K.FPED]) >= ini) & (K.fecha(lista[K.FPED]) <= fin)].copy()
    # los reintegros se toman por la liquidacion que los trae (los ZIP del scope),
    # no por la fecha del pedido: un pedido de la semana anterior puede reintegrarse
    # en esta. Su ticket ya quedo como "Falta PeYa" en el cierre anterior, asi que
    # aca va como ND (reintegro sin venta en el scope).
    rein = rein.copy()

    out = {"entidad": a.entidad, "ini": ini, "fin": fin, "sistemas": {}}
    for sis in cfg["sistemas"]:
        datos = K.leer_sistema(sis, a)
        tk = K.en_scope(datos["tickets"], ini, fin)
        res = cruzar_sistema(sis, lista, rein, tk)
        res["ventas"] = K.en_scope(datos["ventas"], ini, fin)
        # los reportes completos (en cierre mensual vienen desde el 1° del mes):
        # de aca salen la venta del mes contra el mayor y las hojas de venta
        res["ventas_todo"] = datos["ventas"]
        res["tickets_todo"] = datos["tickets"]
        out["sistemas"][sis["id"]] = res
        h = sis["hojas"]
        ruta = os.path.join(a.trabajo, f"cruce_{sis['id']}.xlsx")
        with pd.ExcelWriter(ruta, engine="openpyxl") as w:
            limpiar(res["m1_peya"]).to_excel(w, sheet_name=h["match"][:31], index=False)
            if "match2_peya" in h:
                limpiar(res["m2_peya"]).to_excel(w, sheet_name=h["match2_peya"][:31], index=False)
                limpiar(res["m2_tk"]).to_excel(w, sheet_name=h["match2_sistema"][:31], index=False)
            limpiar(res["falta_sistema"]).to_excel(w, sheet_name=h["falta_sistema"][:31], index=False)
            limpiar(res["falta_peya"]).to_excel(w, sheet_name=h["falta_peya"][:31], index=False)
            limpiar(res["explicados"]).to_excel(w, sheet_name="Tickets c-pedido cancelado", index=False)
            if len(res["anulados"]):
                limpiar(res["anulados"]).to_excel(w, sheet_name="Anulados por sistema", index=False)
            limpiar(res["reint"]).to_excel(w, sheet_name=h["reintegros"][:31], index=False)
        print(f"  -> {ruta}")
    pd.to_pickle(out, os.path.join(a.trabajo, "cruces.pkl"))
    print(f"\nListo: cruces.pkl en {a.trabajo}")


if __name__ == "__main__":
    main()
