#!/usr/bin/env python3
"""
Arma la conciliacion completa: parte del saldo del mayor y llega al saldo
final, una linea por concepto, y deja todo listo para generar_conciliacion.py.

USO
---
    python conciliar.py --entidad easa --trabajo ./trabajo --mayor recaudacion.xlsx \\
        [--anterior Conciliacion_mes_anterior.xlsx] [--salida conceptos.json]

Necesita en la carpeta de trabajo lo que dejan armar_peya.py (liquidaciones.json,
facturas.json, archivos_peya.xlsx) y cruzar.py (cruces.pkl).

QUE CALCULA (todo deterministico)
---------------------------------
A. Ajustes de registracion, leidos del mayor contra las liquidaciones:
   - asientos que no son venta / acreditacion / factura / retencion: si el
     importe coincide con una retencion de alguna liquidacion es una retencion
     duplicada; si no, un ajuste sin respaldo que se revierte (flag)
   - acreditaciones registradas por un importe distinto al liquidado
   - facturas pagadas por un importe distinto al del PDF
   - ventas de cada sistema registradas de mas o de menos, mes por mes
   - diferencia entre el reporte HIO por metodo y por documento
B. Devengamiento: ventas internas del tramo del mes siguiente que entra en la
   ultima liquidacion, por sistema y medio de pago.
C. Pendientes al cierre: una linea por acreditacion no cobrada, una por factura
   no registrada, retenciones no registradas, ventas con pago fuera de la app,
   liquidaciones que no cierran (comprobante faltante o descuento facturado en
   otra semana) y ajustes de liquidacion (cargos adicionales, aminoraciones).
   >>> subtotal: residuo operativo
D. Cruces por sistema: diferencias en matcheadas, faltas, NC por complemento,
   ND por reintegros sin comprobante y recupero de cargos por reclamo.

Lo que no se puede nombrar queda en "pendiente de definicion", fuera del saldo.
Nunca se mete un importe para que cierre.

Genera: conceptos.json, det/*.csv (hojas de detalle), det/historico.csv
(Diferencias por mes) y _controles.json.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import comun as K  # noqa: E402

TOL = 1.00          # tolerancia general, en pesos
N = K.NETA


# ------------------------------------------------------------------ mayor
def leer_mayor(ruta: str, cfg: dict) -> pd.DataFrame:
    m = pd.read_excel(ruta)
    m.columns = [str(c).strip() for c in m.columns]
    for c in ("Debe", "Haber"):
        m[c] = K.num(m[c])
    m["F"] = K.fecha(m[K.col(m, "fecha")])
    m["com"] = m["Comentario"].astype(str).str.strip()
    sf = m["Su factura"] if "Su factura" in m.columns else pd.Series([None] * len(m), index=m.index)
    m["factura"] = sf.astype(str).str.strip().where(sf.notna(), None)
    com = m["com"]
    tipo = np.where(m["factura"].notna() | com.str.contains(r"^Pago S/F|^PAGO", case=False, regex=True), "factura",
           np.where(com.str.contains("Acreditaci", case=False), "acreditacion",
           np.where(com.str.contains(r"^Ret\b|^Ret\.|Retenci", case=False, regex=True), "retencion",
           np.where(com.str.contains(r"^Ventas", case=False, regex=True), "venta",
           np.where(com.str.contains("Saldo anterior", case=False), "saldo_anterior", "otro")))))
    m["tipo"] = tipo
    m["sistema"] = None
    m["medio"] = None
    for sis in cfg["sistemas"]:
        sel = (m["tipo"] == "venta") & com.str.contains(re.escape(sis["mayor"]), case=False)
        m.loc[sel, "sistema"] = sis["id"]
    m.loc[m["tipo"] == "venta", "medio"] = np.where(
        com[m["tipo"] == "venta"].str.contains("efectivo", case=False), "efectivo", "online")
    m["mes"] = K.mes(m["F"])
    return m


def asignar_acreditaciones(filas: list, totales: list):
    """Particion de las liquidaciones (consecutivas, en orden) entre los asientos
    de acreditacion del mayor, minimizando la suma de |asiento - liquidaciones|.
    Devuelve [(j0, j1)] por asiento y el indice de la primera liquidacion pendiente."""
    R, n = len(filas), len(totales)
    acum = [0.0]
    for t in totales:
        acum.append(acum[-1] + t)
    INF = float("inf")
    dp = [[INF] * (n + 1) for _ in range(R + 1)]
    prev = [[None] * (n + 1) for _ in range(R + 1)]
    dp[0][0] = 0.0
    for r in range(1, R + 1):
        for i in range(n + 1):
            best, arg = INF, None
            for j in range(i + 1):
                if dp[r - 1][j] == INF:
                    continue
                c = dp[r - 1][j] + abs(acum[i] - acum[j] - filas[r - 1])
                if c < best - 1e-9:
                    best, arg = c, j
            dp[r][i], prev[r][i] = best, arg
    i = min(range(n + 1), key=lambda k: (round(dp[R][k], 2), -k))
    fin_i = i
    asig = []
    for r in range(R, 0, -1):
        j = prev[r][i]
        asig.append((j, i))
        i = j
    return asig[::-1], fin_i


# ------------------------------------------------------ bloques de lineas
class Lineas:
    def __init__(self):
        self.L = []

    def add(self, concepto, importe, nota=None, flag=False, casos=None):
        """importe con signo de efecto sobre el saldo: + Debe, - Haber."""
        importe = round(float(importe), 2)
        if importe == 0 and not flag:
            return
        ln = {"concepto": concepto}
        if importe > 0:
            ln["debe"] = importe
        elif importe < 0:
            ln["haber"] = -importe
        if nota:
            ln["nota"] = nota
        if flag:
            ln["flag"] = True
        if casos is not None:
            ln["casos"] = int(casos)
        self.L.append(ln)

    def subtotal(self, texto):
        self.L.append({"subtotal": texto})

    def saldo(self, inicial):
        s = inicial
        for ln in self.L:
            if "subtotal" in ln:
                continue
            s += ln.get("debe", 0) - ln.get("haber", 0)
        return round(s, 2) + 0.0


# ------------------------------------------------------- ventas internas
def ventas_por_rango(cr: dict, cfg: dict, a, b, todo=False) -> dict:
    """{(sistema, medio): importe} de las ventas internas entre a y b.
    todo=True usa el reporte completo (no acotado al scope de las liquidaciones)."""
    out = {}
    for sis in cfg["sistemas"]:
        s = cr["sistemas"][sis["id"]]
        v = s["ventas_todo"] if todo and "ventas_todo" in s else s["ventas"]
        v = v[(v["F"] >= a) & (v["F"] <= b)]
        for medio in ("online", "efectivo"):
            out[(sis["id"], medio)] = round(v[v["medio"] == medio]["importe"].sum(), 2)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--entidad", required=True)
    ap.add_argument("--trabajo", required=True)
    ap.add_argument("--mayor", required=True)
    ap.add_argument("--anterior", help="conciliacion del mes anterior (cierre mensual)")
    ap.add_argument("--salida", default=None)
    a = ap.parse_args()
    cfg = K.leer_config(a.entidad)
    T = a.trabajo
    det = os.path.join(T, "det")
    os.makedirs(det, exist_ok=True)

    liq = json.load(open(os.path.join(T, "liquidaciones.json")))
    fac = json.load(open(os.path.join(T, "facturas.json")))
    cr = pd.read_pickle(os.path.join(T, "cruces.pkl"))
    ini, fin = cr["ini"], cr["fin"]
    TL = lambda k: round(sum(x[k] for x in liq), 2)
    P = lambda d: K.periodo_txt(d["desde"], d["hasta"])
    ctl = {"scope": [str(ini.date()), str(fin.date())], "liquidaciones": len(liq)}
    print(f"Entidad {cfg['nombre']} · scope {ini:%d/%m/%Y} al {fin:%d/%m/%Y} ({len(liq)} liquidaciones)")

    mayor = leer_mayor(a.mayor, cfg)
    saldo = round(mayor["Debe"].sum() - mayor["Haber"].sum(), 2)
    fecha_mayor = mayor["F"].max()
    print(f"Saldo del mayor al {fecha_mayor:%d/%m/%Y}: {saldo:,.2f}")

    arr = None
    liq_ac = liq                      # liquidaciones a asignar contra los asientos de acreditacion
    if a.anterior:
        import mensual
        arr = mensual.arrastre(a.anterior, mayor, cfg, {v for s_ in cfg["sistemas"] for v in s_["lineas"].values()})
        # los asientos que ya estaban en la Recaudacion anterior fueron conciliados
        # en ese cierre: de aca en adelante solo cuentan los nuevos (menos los que
        # resolvieron un pendiente). Las ventas se controlan sobre el mayor completo.
        ven_mayor = arr["mayor"][arr["mayor"]["tipo"] == "venta"]
        mayor = arr["mayor"][arr["nuevos"].reindex(arr["mayor"].index)]
        # las acreditaciones pendientes del cierre anterior van delante de las
        # liquidaciones nuevas para la asignacion contra los asientos nuevos
        liq_ac = arr["acred_pend"] + liq
        print(f"\nCIERRE MENSUAL — {arr['etiqueta']} · {int(arr['nuevos'].sum())} asientos nuevos en el mayor")
        for x in arr["resueltos"]:
            print(f"  OK  {x}")
        for x in arr["acred_pend"]:
            print(f"  ..  acreditacion pendiente anterior {x['periodo']} {x['total']:,.2f}: se asigna con las nuevas")
        for x in arr["lineas"]:
            print(f"  --  se arrastra: {x['concepto']}")
    ACUM = (arr or {}).get("acumulados", {})
    DEV = (arr or {}).get("devengado", [])
    mensual_ = arr is not None

    L = Lineas()

    # ============ CONTROL 1: ecuacion de cada liquidacion ============
    print("\nCONTROL 1 — ecuacion de cada liquidacion")
    falta_comp, desc_otra = [], []
    filas_liq = []
    for d in liq:
        f = round(sum(fac.get(d["periodo"], {}).values()), 2)
        dif = round((d["ventas_netas"] + d["reint_canc"] + d["ajustes"])
                    - (d["total"] + f + d["sirtac"] + d["fuera_app"]), 2)
        filas_liq.append({"Periodo": d["periodo"], "Ventas netas": d["ventas_netas"], "Acreditacion": d["total"],
                          "Facturas": f, "Retencion SIRTAC": d["sirtac"], "Ventas fuera app": d["fuera_app"],
                          "Reintegro cancelaciones": d["reint_canc"], "Ajustes": d["ajustes"],
                          "Cargos adicionales": d.get("cargos_adic", 0), "Aminoracion": d.get("aminoracion", 0),
                          "Control": dif})
        if abs(dif) <= TOL:
            continue
        if dif > 0:
            falta_comp.append((d, dif, f))
            print(f"  {d['periodo']}: falta comprobante por {dif:,.2f}" + ("  (sin factura aportada)" if f == 0 else ""))
        else:
            desc_otra.append((d, dif))
    print(f"  cierran {len(liq) - len(falta_comp) - len(desc_otra)}/{len(liq)}")
    pd.DataFrame(filas_liq).to_csv(os.path.join(det, "liquidaciones.csv"), index=False)
    ctl["liq_no_cierran"] = [(d["periodo"], x) for d, x, _ in falta_comp] + [(d["periodo"], x) for d, x in desc_otra]

    # ============ CONTROL 2: ventas internas vs mayor, mes por mes ============
    print("\nCONTROL 2 — ventas internas vs mayor")
    meses = pd.period_range(ini, fin, freq="M")
    ultimo_parcial = fin.day < 28
    meses_control = [str(m) for m in meses[:-1]] if ultimo_parcial else [str(m) for m in meses]
    ven = ven_mayor if mensual_ else mayor[mayor["tipo"] == "venta"]
    ajustes_ventas, no_registradas = [], []
    # en cierre mensual los reportes vienen desde el 1° del mes: la venta del mes
    # es el mes completo del reporte (el tramo anterior al scope ya se cruzo en
    # el cierre anterior y se controla aparte contra lo devengado entonces)
    rep_desde = min(cr["sistemas"][s["id"]]["ventas_todo"]["F"].min() for s in cfg["sistemas"]) if mensual_ else ini
    for ms in meses_control:
        m0, m1 = pd.Period(ms).start_time.normalize(), min(pd.Period(ms).end_time.normalize(), fin)
        a_ = m0 if (mensual_ and rep_desde <= m0) else max(m0, ini)
        real = ventas_por_rango(cr, cfg, a_, m1, todo=mensual_)
        reg_mes = ven[ven["mes"] == ms]
        if reg_mes.empty and sum(real.values()) > 0:
            for sis in cfg["sistemas"]:
                for medio in ("online", "efectivo"):
                    v = real[(sis["id"], medio)]
                    if v:
                        et = sis[f"etiqueta_{medio}"].format(rango=ms[5:] + "-" + ms[:4])
                        no_registradas.append((ms, sis, medio, v, et, a_, m1))
            print(f"  {ms}: el mayor no tiene ventas registradas ({sum(real.values()):,.2f}) -> se genera la venta del mes")
            continue
        for sis in cfg["sistemas"]:
            r = real[(sis["id"], "online")] + real[(sis["id"], "efectivo")]
            g = round(reg_mes[reg_mes["sistema"] == sis["id"]]["Debe"].sum(), 2)
            dif = round(r - g, 2)
            if abs(dif) > TOL:
                ajustes_ventas.append((ms, sis, dif, r, g))
                print(f"  {ms} {sis['etiqueta_corta']}: reporte {r:,.2f} vs mayor {g:,.2f} -> {dif:,.2f}")
    if not ajustes_ventas and not no_registradas:
        print("  todos los meses coinciden")
    ctl["ventas_vs_mayor"] = [(ms, s["id"], d) for ms, s, d, _, _ in ajustes_ventas]

    # ============ CONTROL 2b: devengado en el cierre anterior vs reporte nuevo ============
    # El cierre anterior devengo las ventas de los primeros dias del mes con el
    # reporte de entonces; el reporte nuevo trae ese mismo tramo. Si difieren,
    # la venta del mes (que sale del reporte nuevo) no cuadra con lo ya cruzado.
    dif_deveng = []
    if DEV:
        print("\nCONTROL 2b — devengado del cierre anterior vs reporte nuevo")
        for sid, medio, a_, b_, v_ant in DEV:
            s = cr["sistemas"].get(sid)
            if s is None or s["ventas_todo"]["F"].min() > a_:
                print(f"  {sid} {medio} {a_:%d/%m}-{b_:%d/%m}: el reporte nuevo no cubre el tramo, no se controla")
                continue
            v_new = ventas_por_rango(cr, cfg, a_, b_, todo=True)[(sid, medio)]
            dif = round(v_new - v_ant, 2)
            sis = next(x for x in cfg["sistemas"] if x["id"] == sid)
            print(f"  {sis['etiqueta_corta']} {medio} {a_:%d/%m}-{b_:%d/%m}: devengado {v_ant:,.2f} vs reporte nuevo {v_new:,.2f}"
                  + (f" -> {dif:,.2f}" if abs(dif) > 0.005 else " OK"))
            if abs(dif) > 0.005:
                dif_deveng.append((sis, medio, a_, b_, v_ant, v_new, dif))
    ctl["devengado_vs_reporte"] = [(s["id"], m, d) for s, m, _, _, _, _, d in dif_deveng]

    # ============ acreditaciones: mayor vs liquidaciones ============
    # El mayor puede registrar varias semanas en un asiento. Se asignan
    # liquidaciones consecutivas a cada asiento (en orden cronologico) buscando
    # la particion que minimiza las diferencias; lo que queda sin asignar al
    # final son las acreditaciones pendientes de cobro.
    ac_rows = mayor[mayor["tipo"] == "acreditacion"].sort_values(["F", "Haber"])
    ac_mayor = round(ac_rows["Haber"].sum(), 2)
    asig, pend_ac = asignar_acreditaciones([r["Haber"] for _, r in ac_rows.iterrows()], [d["total"] for d in liq_ac])
    # Los asientos pueden agrupar semanas de forma distinta a las liquidaciones
    # (son movimientos bancarios): diferencias consecutivas que se compensan
    # entre si no son errores. Una diferencia es real cuando el bloque de
    # asientos que la contiene no vuelve a cero antes del siguiente asiento exacto.
    ac_dif, redondeo = [], 0.0
    bloque = []            # [(row, usados, dif)]
    def cerrar_bloque():
        nonlocal redondeo
        if not bloque:
            return
        neto = round(sum(b[2] for b in bloque), 2)
        if abs(neto) <= TOL:
            redondeo += neto
        else:
            filas = [b[0] for b in bloque]
            usados = [u for b in bloque for u in b[1]]
            ac_dif.append((filas, usados, neto))
        bloque.clear()
    for (_, row), (j0, j1) in zip(ac_rows.iterrows(), asig):
        usados = liq_ac[j0:j1]
        dif = round(sum(u["total"] for u in usados) - row["Haber"], 2)   # >0 registrada de menos
        if abs(dif) <= TOL:
            cerrar_bloque()
            redondeo += dif
        else:
            bloque.append((row, usados, dif))
    cerrar_bloque()
    pend_ac = liq_ac[pend_ac:]
    ctl["acreditaciones"] = {"liq": round(sum(d["total"] for d in liq_ac), 2), "mayor": ac_mayor,
                             "pendientes": [P(d) for d in pend_ac]}
    print("\nACREDITACIONES — asientos del mayor vs liquidaciones")
    for (_, row), (j0, j1) in zip(ac_rows.iterrows(), asig):
        print(f"  {row['F']:%d/%m/%Y} {row['Haber']:>16,.2f} <- {', '.join(P(d) for d in liq_ac[j0:j1]) or 'sin liquidacion'}")
    for d in pend_ac:
        print(f"  pendiente de cobro {P(d)} {d['total']:,.2f}")

    # ============ facturas: por numero contra el mayor ============
    pagadas = {}
    for _, row in mayor[mayor["tipo"] == "factura"].iterrows():
        pagadas.setdefault(row["factura"] or f"s/n {row['com']}", []).append(row)
    ids_fac = {ident for dd in fac.values() for ident in dd}
    pend_fac, dif_fac, huerfanas = [], [], []
    for per, dd in fac.items():
        for ident, imp in dd.items():
            alt = [ident, ident.replace("0026", "0023", 1), ident.replace("0013", "0023", 1)]
            hit = next((x for x in alt if x in pagadas), None)
            if not hit:
                pend_fac.append((per, ident, imp))
            else:
                pag = round(sum(r["Haber"] for r in pagadas[hit]), 2)
                if abs(pag - imp) > 0.005:
                    dif_fac.append((per, ident, imp, pag))
    for k, rows in pagadas.items():
        if k not in ids_fac and k.replace("0023", "0026", 1) not in ids_fac:
            huerfanas.extend(rows)
    # pendiente cuyo importe coincide con un pago huerfano: numero mal tipeado
    tipeadas = []
    for per, ident, imp in list(pend_fac):
        h = next((r for r in huerfanas if abs(r["Haber"] - imp) <= TOL), None)
        if h is not None:
            huerfanas = [r for r in huerfanas if r is not h]
            pend_fac.remove((per, ident, imp))
            tipeadas.append((ident, h, imp))
    ctl["facturas"] = {"pendientes": [x[1] for x in pend_fac], "con_diferencia": [x[1] for x in dif_fac],
                       "tipeadas": [x[0] for x in tipeadas], "huerfanas": [r["com"] for r in huerfanas]}

    # ============ retenciones ============
    ret_rows = mayor[mayor["tipo"] == "retencion"].copy()
    libres = list(ret_rows.index)
    pend_ret = []
    for d in liq:
        if d["sirtac"] <= 0:
            continue
        hit = next((j for j in libres if abs(ret_rows.loc[j, "Haber"] - d["sirtac"]) <= TOL), None)
        if hit is None:
            pend_ret.append(d)
        else:
            libres.remove(hit)
    ret_sin_liq = ret_rows.loc[libres]
    sirtac = {round(d["sirtac"], 2): d for d in liq if d["sirtac"] > 0}

    # ============ otros asientos ============
    otros = mayor[mayor["tipo"] == "otro"].copy()
    otros["neto"] = otros["Debe"] - otros["Haber"]
    # pares que se cancelan entre si (un asiento y su reversion) no son una diferencia
    cancelados = set()
    for j, r in otros.iterrows():
        if j in cancelados:
            continue
        k = next((q for q, s in otros.iterrows() if q != j and q not in cancelados
                  and abs(s["neto"] + r["neto"]) <= 0.005), None)
        if k is not None:
            cancelados |= {j, k}
    otros = otros.drop(index=list(cancelados))

    # ============ HIO: metodo vs documento ============
    dif_hio = []
    for sis in cfg["sistemas"]:
        if sis["tipo"] != "hio":
            continue
        s = cr["sistemas"][sis["id"]]
        met, docv = round(s["ventas"]["importe"].sum(), 2), round(s["tickets"]["importe"].sum(), 2)
        t = s["tickets"]
        d = t[(K.num(t["Importe por metodo"]) - K.num(t["importe"])).abs() > 0.005].copy()
        if abs(met - docv) > TOL or len(d):
            d["Diferencia"] = (K.num(d["Importe por metodo"]) - K.num(d["importe"])).round(2)
            cols = {K.col(d, "serie"): "Serie / Número", K.col(d, "fecha"): "Fecha",
                    K.col(d, "establecimiento"): "Establecimiento", K.col(d, "localizador"): "Localizador"}
            det_d = d[[c for c in cols if c] + ["Medio Pago", "Importe por metodo", "importe", "Diferencia"]].rename(
                columns={**cols, "Importe por metodo": "Importe reporte método", "importe": "Importe reporte documento"})
            det_d = det_d.sort_values("Diferencia", key=abs, ascending=False)
            dif_hio.append((sis, met - docv, det_d))

    # ================================================================
    # A. AJUSTES DE REGISTRACION
    # ================================================================
    for _, r in otros.iterrows():
        imp = round(r["Haber"], 2) if r["Haber"] else round(r["Debe"], 2)
        rev = -r["neto"]                       # revertir el asiento
        if imp in sirtac and r["Haber"]:
            d = sirtac[imp]
            L.add(f"Retencion IIBB {P(d)} Duplicada", rev,
                  f"Asiento {r['Asiento']} '{r['com']}', mismo importe que la retención de la liquidación")
        else:
            L.add(f"Ajuste sin respaldo en liquidaciones: {r['com']}", rev,
                  f"Asiento {r['Asiento']} del {r['F']:%d/%m/%Y} — se revierte", flag=True)
    for _, r in ret_sin_liq.iterrows():
        L.add(f"Retención sin liquidación que la respalde: {r['com']}", round(r["Haber"] - r["Debe"], 2),
              f"Asiento {r['Asiento']} del {r['F']:%d/%m/%Y}", flag=True)
    for filas, usados, dif in ac_dif:
        reg = sum(r["Haber"] for r in filas)
        if usados:
            rango = f"{P(usados[0])}" if len(usados) == 1 else f"{usados[0]['desde'][:5]}-{usados[-1]['hasta'][:5]}"
            tipo = "de más" if dif < 0 else "de menos"
            L.add(f"Acreditación Registrada {tipo} {rango}", -dif,
                  f"Registrado {reg:,.2f} vs liquidado {sum(u['total'] for u in usados):,.2f} "
                  f"(asiento{'s' if len(filas) > 1 else ''} {', '.join(str(r['Asiento']) for r in filas)})")
        else:
            L.add(f"Acreditación sin liquidación que la respalde ({filas[0]['F']:%d/%m/%Y})", round(reg, 2),
                  f"Asiento {filas[0]['Asiento']} '{filas[0]['com']}'", flag=True)
    if abs(redondeo) > 0.005:
        L.add("Diferencia de redondeo en acreditaciones registradas", -redondeo)
    for d, dif, f in falta_comp:
        if f == 0:
            continue                           # va como factura pendiente (bloque C)
        L.add(f"Servicio Pago en Linea (No facturado) {P(d)}", -dif,
              "La ecuación de la semana no cierra: falta un comprobante (típicamente la factura de PagosYa)", flag=True)
    for per, ident, imp, pag in dif_fac:
        L.add(f"Factura {ident} pagada con diferencia", -(imp - pag),
              f"Factura {imp:,.2f} · pagado {pag:,.2f} · período {per} · revisar percepción IIBB")
    for ident, h, imp in tipeadas:
        L.add(f"Factura {ident} registrada como {h['factura'] or h['com']} en el mayor", round(h["Haber"] - imp, 2),
              f"Pagada el {h['F']:%d/%m} por {h['Haber']:,.2f} — solo corregir el número", flag=True)
    for ms, sis, dif, r, g in ajustes_ventas:
        tipo = "de menos" if dif > 0 else "de más"
        L.add(f"Ventas {sis['etiqueta_corta']} {ms[5:]}-{ms[:4]} registradas {tipo}", dif,
              f"Reporte {r:,.2f} vs mayor {g:,.2f}")
    if arr and arr.get("redondeo_fac"):
        L.add("Diferencia de redondeo en facturas registradas", arr["redondeo_fac"],
              "Centavos entre las facturas pendientes del cierre anterior y lo pagado")
    for sis, medio, a_, b_, v_ant, v_new, dif in dif_deveng:
        L.add(f"{sis[f'etiqueta_{medio}'].format(rango=K.rango_txt(a_, b_))} — reporte nuevo vs devengado en el cierre anterior",
              dif, f"Devengado {v_ant:,.2f} en el cierre anterior · reporte nuevo {v_new:,.2f}", flag=True)
    for ms, sis, medio, v, et, a_, b_ in no_registradas:
        # venta del mes sin asiento: se genera con las mismas palabras del mayor
        L.add(et, v, f"Venta del mes no registrada en el mayor · reporte {sis['etiqueta_corta']} "
                     f"{a_:%d/%m} al {b_:%d/%m} ({medio}) — asiento a registrar")
    for sis, dif, det_d in dif_hio:
        L.add(f"Diferencia {sis['etiqueta_corta']} reporte método vs documento", -dif,
              f"{len(det_d)} ticket{'s' if len(det_d) != 1 else ''} donde los dos reportes difieren · "
              f"uno por uno en la hoja 'Dif {sis['etiqueta_corta']} método vs documento' · definir cuál manda", flag=True)

    # ================================================================
    # B. DEVENGAMIENTO (tramo del mes siguiente dentro de la ultima liquidacion)
    # ================================================================
    if ultimo_parcial:
        a_ = pd.Timestamp(fin.year, fin.month, 1)
        real = ventas_por_rango(cr, cfg, a_, fin)
        rango = K.rango_txt(a_, fin)
        for sis in cfg["sistemas"]:
            for medio in ("online", "efectivo"):
                L.add(sis[f"etiqueta_{medio}"].format(rango=rango), real[(sis["id"], medio)])

    # ================================================================
    # C. PENDIENTES AL CIERRE
    # ================================================================
    for d in pend_ac:
        L.add(f"Acreditacion {P(d)}", -d["total"], "Se cobra ~11 días después del cierre")
    for per, ident, imp in pend_fac:
        L.add(f"Factura Pendiente {ident}", -imp, f"Período {per}")
    for d, dif, f in falta_comp:
        if f == 0:
            L.add(f"Factura Pendiente {P(d)} (No aportada)", -dif,
                  "Importe deducido de la ecuación de la liquidación", flag=True)
    acp = lambda pref: ACUM.get(pref, 0.0)
    L.add("Ventas con pago por fuera de la aplicación ya cobradas", -TL("fuera_app") + acp("Ventas con pago por fuera de la aplicación"),
          f"Las {len(liq)} liquidaciones" + (" + acumulado anterior" if acp("Ventas con pago por fuera de la aplicación") else ""))
    for d in pend_ret:
        L.add(f"Retencion {P(d)}", -d["sirtac"])
    if desc_otra or acp("Diferencia Descuentos facturados"):
        L.add("Diferencia Descuentos facturados en semana distinta", -sum(x for _, x in desc_otra) + acp("Diferencia Descuentos facturados"),
              f"{len(desc_otra)} liquidaci{'ones' if len(desc_otra) > 1 else 'ón'} ({', '.join(P(d) for d, _ in desc_otra)}) · "
              "lo facturado supera lo liquidado: descuento facturado en otra semana o comprobante (NC) faltante")
    # ajustes de liquidacion: entran en la ecuacion y no tienen venta de contrapartida
    amin = [d for d in liq if abs(d.get("aminoracion", 0)) > 0.5]
    carg = [d for d in liq if abs(d.get("cargos_adic", 0)) > 0.5]
    t_carg = round(sum(d["cargos_adic"] for d in carg), 2)
    t_amin = round(TL("ajustes") - t_carg, 2)      # absorbe el redondeo del renglon "Ajustes" del PDF
    if carg or acp("Cargos / Reintegros adicionales"):
        L.add(f"Cargos / Reintegros adicionales de liquidación (liq. {', '.join(P(d) for d in carg) or 'anteriores'})",
              t_carg + acp("Cargos / Reintegros adicionales"), "Ajustes de liquidación sin venta de contrapartida")
    if abs(t_amin) > 0.5 or acp("Aminoración descuento comercial"):
        L.add(f"Aminoración descuento comercial (liq. {', '.join(P(d) for d in amin) or 'anteriores'})",
              t_amin + acp("Aminoración descuento comercial"), "PeYa lo reconoce en la liquidación y no corresponde a ninguna venta")
    if arr:
        for ln in arr["lineas"]:
            L.L.append(ln)
    L.subtotal("Subtotal — residuo operativo a explicar")
    residuo = L.saldo(saldo)

    # ================================================================
    # D. CRUCES Y REINTEGROS, POR SISTEMA
    # ================================================================
    hist_rows, detalles = [], {}
    # primer dia del mes que se cierra: desde ahi van los papeles de trabajo
    mes_desde = pd.Timestamp(ini.year, ini.month, 1)
    # etiqueta de la columna "Cierre" de los papeles de trabajo: de que cierre viene cada fila
    cierre_act = f"{fecha_mayor:%Y-%m}"
    cierre_ant = (arr["anterior"]["fecha"][6:] + "-" + arr["anterior"]["fecha"][3:5]) if mensual_ else None
    recl = pd.ExcelFile(os.path.join(T, "archivos_peya.xlsx")).parse("Reclamos")
    recl_ped = set(K.ids(recl[K.PED])) if len(recl) else set()
    por_local = cfg.get("apertura_historico") == "mes_y_local"
    nombres_loc = {}
    for sis in cfg["sistemas"]:
        for k, d in (sis.get("locales") or {}).items():
            nombres_loc[k] = d.get("nombre", k)
    comp_cruces = 0.0
    reint_total_lineas = 0.0
    n_reint = 0
    prev_hist = (arr or {}).get("historico")
    def prev_acum(con):
        """acumulado del concepto en la conciliacion anterior (hoja Diferencias por mes)."""
        if prev_hist is None or prev_hist.empty:
            return 0.0
        h = prev_hist[prev_hist.iloc[:, 0].astype(str) == con]
        mc = [c for c in h.columns if re.match(r"\d{4}-\d{2}$", str(c)) or str(c).startswith("Reclasif")]
        return round(float(h[mc].fillna(0).sum().sum()), 2) if len(h) else 0.0

    def nota_acum(con, texto):
        """nota de una linea de cruce: lo nuevo, y el acumulado anterior si lo hay."""
        pa = prev_acum(con)
        return texto + (f" · acumulado anterior {pa:,.2f}" if pa else "")

    def hist(con, df, valores, fcol="F"):
        if not len(df):
            return
        t = pd.DataFrame({"m": K.mes(df[fcol]).values, "v": np.asarray(valores, dtype=float),
                          "l": (df["loc"].map(nombres_loc).fillna("").values if (por_local and "loc" in df.columns) else "")})
        for (mm, ll), g in t.groupby(["m", "l"]):
            hist_rows.append({"Concepto": con, "Local": ll, "Mes": mm, "Importe": round(g["v"].sum(), 2)})

    for sis in cfg["sistemas"]:
        s = cr["sistemas"][sis["id"]]
        ln, imp = sis["lineas"], s["nombre_imp"]
        m1, m2p, m2t, fs, fp, expl, r = (s["m1_peya"], s["m2_peya"], s["m2_tk"], s["falta_sistema"],
                                         s["falta_peya"], s["explicados"], s["reint"].copy())
        # --- reintegros: clasificacion ---
        r["pct100"] = r["% de Reintegro"].astype(str).str.startswith("100")
        r["en_recl"] = r["ped"].isin(recl_ped)
        r["reint"] = K.num(r[K.REINT]).round(2)
        if sis["tipo"] == "atalaya":
            # ticket explicado por fecha + monto, uno a uno
            pool = {}
            for i, f, m in zip(expl.index, expl["F"], expl["importe"].round(2)):
                pool.setdefault((f, m), []).append(i)
            venta = []
            for f, m in zip(r["F"], r["monto"]):
                c = pool.get((f, m))
                venta.append(expl.loc[c.pop(0), "importe"] if c else np.nan)
            r["venta"] = venta
        else:
            vmap = dict(zip(expl["id"], expl["importe"]))
            r["venta"] = r["ped"].map(vmap)
        r["trat"] = np.where(r["pct100"] & r["en_recl"], "Recupero cargo reclamo (100%)",
                    np.where(r["venta"].notna(), "NC complemento", "ND sin comprobante"))
        r["NC"] = np.where(r["trat"] == "NC complemento", (r["venta"].fillna(0) - r["reint"]).round(2), 0.0)
        nc, nd, rc = (r[r["trat"] == t] for t in ("NC complemento", "ND sin comprobante", "Recupero cargo reclamo (100%)"))
        reint_total_lineas += r["reint"].sum()
        n_reint += len(r)

        # --- lineas ---
        if "match2" not in ln and not len(m1):
            m1, m2p = m2p, m2p.iloc[0:0]        # sin identificador: el unico cruce es la linea "match"
        d1 = -(K.num(m1[imp]) - K.num(m1[N])) if len(m1) else pd.Series(dtype=float)
        mal = int(((K.num(m1[imp]) - K.num(m1[N])).abs() > 0.02).sum()) if len(m1) else 0
        L.add(ln["match"], d1.sum() + prev_acum(ln["match"]), nota_acum(ln["match"], f"{len(m1)} pedidos · {mal} con diferencia"), casos=len(m1))
        hist(ln["match"], m1, d1)
        if "match2" in ln:
            d2 = -(K.num(m2p[imp]) - K.num(m2p[N])) if len(m2p) else pd.Series(dtype=float)
            L.add(ln["match2"], d2.sum() + prev_acum(ln["match2"]), nota_acum(ln["match2"], f"{len(m2p)} pedidos"), casos=len(m2p))
            hist(ln["match2"], m2p, d2)
        vfs = K.num(fs[N])
        L.add(ln["falta_sistema"], vfs.sum() + prev_acum(ln["falta_sistema"]), nota_acum(ln["falta_sistema"], f"{len(fs)} pedido{'s' if len(fs) != 1 else ''} que PeYa liquidó y {sis['etiqueta_corta']} no ticketeó"), casos=len(fs))
        hist(ln["falta_sistema"], fs, vfs)
        vfp = -K.num(fp["importe"])
        L.add(ln["falta_peya"], vfp.sum() + prev_acum(ln["falta_peya"]), nota_acum(ln["falta_peya"], f"{len(fp)} ticket{'s' if len(fp) != 1 else ''} {sis['etiqueta_corta']} sin pedido en la Lista"), casos=len(fp))
        hist(ln["falta_peya"], fp, vfp)
        L.add(ln["nc"], -nc["NC"].sum() + prev_acum(ln["nc"]),
              nota_acum(ln["nc"], f"{len(nc)} casos · venta {nc['venta'].sum():,.2f} − reintegro {nc['reint'].sum():,.2f}"), casos=len(nc))
        hist(ln["nc"], nc, -nc["NC"])
        L.add(ln["nd50"], nd["reint"].sum() + prev_acum(ln["nd50"]), nota_acum(ln["nd50"], f"{len(nd)} casos"), casos=len(nd))
        hist(ln["nd50"], nd, nd["reint"])
        L.add(ln["nd100"], rc["reint"].sum() + prev_acum(ln["nd100"]), nota_acum(ln["nd100"], f"{len(rc)} casos · recupero de cargo por reclamo"), casos=len(rc))
        hist(ln["nd100"], rc, rc["reint"])
        d2s = (K.num(m2p[imp]).sum() - K.num(m2p[N]).sum()) if len(m2p) else 0.0
        comp_cruces += -d1.sum() + d2s - vfs.sum() + K.num(fp["importe"]).sum() + nc["venta"].sum() - r["reint"].sum()

        # --- hojas de detalle ---
        h = sis["hojas"]
        v = s["tickets"]          # para HIO es el combinado documento+metodo; para los demas, el ticket
        if mensual_:
            # papeles de trabajo del cierre mensual: el mes completo del reporte
            # (incluye los primeros dias, que se cruzaron en el cierre anterior)
            v = s["tickets_todo"]
            v = v[v["F"] >= mes_desde].copy()
            v.insert(0, "Cierre", np.where(v["F"] < ini, cierre_ant, cierre_act))
        detalles[h["venta"]] = v.drop(columns=[c for c in ("sn", "medio", "importe", "id", "loc", "F", "Local") if c in v.columns])
        detalles[h["match"]] = m1
        if "match2_peya" in h:
            detalles[h["match2_peya"]] = m2p
            detalles[h["match2_sistema"]] = m2t
        detalles[h["falta_sistema"]] = fs
        detalles[h["falta_peya"]] = fp
        cols = [c for c in ["Tipo de Reintegro", K.PED, "Estado del pedido", "Orden entregada al repartidor", K.SUC,
                            "Fecha del pedido", K.MPED, "Servicios PedidosYa", "% de Reintegro", K.REINT,
                            "venta", "trat", "NC"] if c in r.columns]
        detalles[h["reintegros"]] = r[cols].rename(columns={"venta": "Venta interna", "trat": "Tratamiento"})
        for s_, _, det_d in dif_hio:
            if s_["id"] == sis["id"]:
                detalles[f"Dif {sis['etiqueta_corta']} método vs documento"] = det_d
        print(f"\n{sis['nombre']}: match {len(m1)}" + (f" + {len(m2p)}" if len(m2p) else "") +
              f" | falta {sis['etiqueta_corta']} {len(fs)} | falta PeYa {len(fp)} | "
              f"reintegros NC {len(nc)} / ND {len(nd)} / 100% {len(rc)}")

    final = L.saldo(saldo)

    # ============ CONTROL 3: reintegros y verificacion independiente ============
    print("\nCONTROL 3 — reintegros")
    print(f"  lineas {reint_total_lineas:,.2f} ({n_reint} casos) vs liquidaciones {TL('reint_canc'):,.2f}")
    ctl["reintegros"] = {"lineas": round(reint_total_lineas, 2), "liquidaciones": TL("reint_canc"), "casos": n_reint}
    vi = sum(ventas_por_rango(cr, cfg, ini, fin).values())
    haber = round(TL("ventas_netas") + TL("reint_canc") + TL("ajustes"), 2)
    res_ind = round(vi - haber, 2)
    print("\nCONTROL 4 — verificacion independiente")
    print(f"  ventas internas {vi:,.2f} − (ventas netas + reintegros + ajustes) {haber:,.2f} = {res_ind:,.2f}")
    if mensual_:
        orden_ = [sis["lineas"][k] for sis in cfg["sistemas"] for k in ("match", "match2", "falta_sistema", "falta_peya", "nc", "nd50", "nd100") if k in sis["lineas"]]
        acum_ant = round(sum(prev_acum(c) for c in orden_), 2)
        print(f"  residuo operativo de la cadena: {residuo:,.2f} = acumulado de cruces anterior {-acum_ant:,.2f} "
              f"+ residuo nuevo {residuo + acum_ant:,.2f} · cruces nuevos explican {comp_cruces:,.2f}")
    else:
        print(f"  residuo operativo de la cadena: {residuo:,.2f} · cruces explican {comp_cruces:,.2f}")
    print(f"\nSaldo mayor {saldo:,.2f} -> SALDO FINAL {final:,.2f}")
    ctl["verificacion"] = {"ventas_internas": round(vi, 2), "peya": haber, "residuo_independiente": res_ind,
                           "residuo_cadena": residuo, "cruces": round(comp_cruces, 2)}
    ctl["saldo_mayor"], ctl["saldo_final"] = saldo, final

    # ============ salida ============
    abierto = []
    if abs(final) > TOL:
        abierto.append({"concepto": "Diferencia sin identificar", "debe" if final > 0 else "haber": abs(final),
                        "nota": f"{abs(final) / max(vi, 1) * 100:.3f}% sobre {vi / 1e6:,.1f} millones de ventas internas"})
    if arr:
        abierto.extend(arr["abiertas"])
    hist_df = pd.DataFrame(hist_rows, columns=["Concepto", "Local", "Mes", "Importe"])
    if arr and arr.get("historico") is not None:
        hist_df = mensual.acumular_historico(arr["historico"], hist_df, por_local)
    piv = hist_df.pivot_table(index=["Concepto", "Local"] if por_local else ["Concepto"], columns="Mes",
                              values="Importe", aggfunc="sum", fill_value=0).reset_index()
    if not por_local and "Local" in piv.columns:
        piv = piv.drop(columns="Local")
    orden = []
    for sis in cfg["sistemas"]:
        orden += [sis["lineas"][k] for k in ("match", "match2", "falta_sistema", "falta_peya", "nc", "nd50", "nd100") if k in sis["lineas"]]
    piv["_o"] = piv["Concepto"].map({c: i for i, c in enumerate(orden)}).fillna(99)
    piv = piv.sort_values(["_o"] + (["Local"] if por_local else [])).drop(columns="_o")
    piv = piv[piv.drop(columns=["Concepto"] + (["Local"] if por_local else [])).abs().sum(axis=1) > 0]
    mcols = [c for c in piv.columns if re.match(r"\d{4}-\d{2}", str(c)) or str(c).startswith("Reclasif")]
    piv = piv[["Concepto"] + (["Local"] if por_local else []) + sorted(c for c in mcols if not str(c).startswith("Reclasif"))
              + [c for c in mcols if str(c).startswith("Reclasif")]]
    piv["Acumulado"] = piv[mcols].sum(axis=1).round(2)
    if "Local" in piv.columns:
        piv["Local"] = piv["Local"].fillna("")
    piv.to_csv(os.path.join(det, "historico.csv"), index=False)

    pd.read_excel(a.mayor).to_csv(os.path.join(det, "recaudacion.csv"), index=False)
    lista = pd.ExcelFile(os.path.join(T, "archivos_peya.xlsx")).parse("Lista Pedidos")
    lista = lista.drop(columns=[c for c in lista.columns if str(c).startswith("_zip")], errors="ignore")
    if mensual_:
        # Papeles de trabajo: el mes completo. Lo que se cruza ahora + los
        # movimientos de los primeros dias del mes, que entraron en el cierre
        # anterior y se traen de sus hojas (columna "Cierre" dice de cual viene).
        ant_ = arr["anterior"]
        def con_anterior(nombre, df_nuevo):
            prev = mensual.detalle_anterior(ant_, nombre, mes_desde, ini - pd.Timedelta(days=1))
            df_nuevo = df_nuevo.copy()
            df_nuevo.insert(0, "Cierre", cierre_act)
            if prev is None or prev.empty:
                return df_nuevo
            prev = prev.copy()
            if "Cierre" in prev.columns:
                prev = prev.drop(columns="Cierre")
            prev.insert(0, "Cierre", cierre_ant)
            return pd.concat([prev, df_nuevo], ignore_index=True)
        lista = con_anterior("PeYa Lista Pedidos", lista[K.fecha(lista[K.FPED]) >= mes_desde])
        for nombre in list(detalles):
            if "Cierre" not in detalles[nombre].columns:
                detalles[nombre] = con_anterior(nombre, detalles[nombre])
        # liquidaciones: la anterior que termino dentro del mes (trae los primeros dias)
        lq = pd.read_csv(os.path.join(det, "liquidaciones.csv"))
        lq.insert(0, "Cierre", cierre_act)
        xl_ant = ant_.get("hojas_detalle", {}).get("Liquidaciones")
        if xl_ant is not None:
            la = xl_ant.parse("Liquidaciones")
            la = la[la["Periodo"].astype(str).str.match(r"\d{2}/\d{2}-\d{2}/\d{2}")]
            hasta = la["Periodo"].astype(str).str[6:11].map(lambda x: pd.Timestamp(f"{ini.year}-{x[3:5]}-{x[:2]}") if x else pd.NaT)
            la = la[hasta >= mes_desde].copy()
            if "Cierre" in la.columns:
                la = la.drop(columns="Cierre")
            la.insert(0, "Cierre", cierre_ant)
            lq = pd.concat([la, lq], ignore_index=True)
        lq.to_csv(os.path.join(det, "liquidaciones.csv"), index=False)
    lista.to_csv(os.path.join(det, "peya_lista.csv"), index=False)
    hojas = {"Recaudacion": "recaudacion.csv"}
    for sis in cfg["sistemas"]:
        hojas[sis["hojas"]["venta"]] = None
    hojas["PeYa Lista Pedidos"] = "peya_lista.csv"
    hojas["Liquidaciones"] = "liquidaciones.csv"
    for nombre, df in detalles.items():
        f = re.sub(r"[^a-z0-9]+", "_", nombre.lower()).strip("_") + ".csv"
        d = df.drop(columns=[c for c in ("ped", "F", "loc", "fuera", "id", "importe", "medio", "sn", "monto",
                                          "pct100", "en_recl", "reint") if c in df.columns], errors="ignore")
        # columnas vacias que arrastran los reportes exportados ("Unnamed: 15" y siguientes)
        d = d.drop(columns=[c for c in d.columns if str(c).startswith("Unnamed") and d[c].isna().all()])
        d.to_csv(os.path.join(det, f), index=False)
        hojas[nombre] = f
    hojas = {k: os.path.join(det, v) for k, v in hojas.items() if v}

    cfg_out = {"titulo": f"CONCILIACIÓN CUENTA RECAUDACIÓN PEDIDOSYA — {cfg['nombre']}",
               "subtitulo": f"Saldo del mayor al {fecha_mayor:%d/%m/%Y} · Scope de liquidaciones "
                            f"{ini:%d/%m/%Y} al {fin:%d/%m/%Y} ({len(liq)} semanas)",
               "saldo_inicial": saldo, "lineas": L.L, "abierto": abierto,
               "nota_final": f"Verificación independiente: ventas internas {vi:,.2f} − (ventas netas PeYa + reintegros + "
                             f"ajustes) {haber:,.2f} = {res_ind:,.2f}; el residuo operativo de la cadena es {residuo:,.2f} "
                             f"y los cruces explican {comp_cruces:,.2f}.",
               "historico": os.path.join(det, "historico.csv"), "detalles": hojas}
    salida = a.salida or os.path.join(T, "conceptos.json")
    json.dump(cfg_out, open(salida, "w", encoding="utf-8"), ensure_ascii=False, indent=1, default=str)
    json.dump(ctl, open(os.path.join(T, "_controles.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=1, default=str)
    print(f"\nConceptos en {salida} · controles en {T}/_controles.json")


if __name__ == "__main__":
    main()
