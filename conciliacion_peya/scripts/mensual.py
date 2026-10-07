#!/usr/bin/env python3
"""
Cierre mensual: la conciliacion del mes anterior entra como INPUT y de ahi
sale todo lo que hace falta para continuar. Nada se recuerda entre corridas.

Lo usa conciliar.py cuando recibe --anterior. Tambien se puede correr solo
para ver que se resuelve y que se arrastra:

    python mensual.py --entidad easa --anterior Conciliacion_ago.xlsx --mayor recaudacion_sep.xlsx

QUE SE CONSIDERA "NUEVO" EN EL MAYOR
------------------------------------
Los asientos del mayor que NO estan en la hoja "Recaudacion" de la conciliacion
anterior (clave: asiento, apunte, fecha, debe, haber). No se filtra por fecha:
las facturas pendientes suelen registrarse con fecha del mes anterior y aun asi
son asientos nuevos. Solo los asientos nuevos pueden resolver un pendiente.

QUE HACE CON CADA LINEA DE LA CONCILIACION ANTERIOR
---------------------------------------------------
Acreditacion dd/mm-dd/mm        se devuelve como liquidacion pendiente de cobro
                                (periodo + importe). conciliar.py la pone delante
                                de las liquidaciones nuevas y asigna los asientos
                                de acreditacion nuevos sobre esa lista completa:
                                el mayor registra varias semanas en un asiento y
                                una pendiente anterior puede caer junto con una
                                semana nueva.
Factura Pendiente NNNN-NNNN     por numero en "Su factura" entre las facturas
                                nuevas. Si se pago con otro importe: diferencia
                                mayor a TOL_IMPORTE -> "pagada con diferencia";
                                menor -> va a la linea de redondeo de facturas.
Factura Pendiente dd/mm-dd/mm (No aportada)
                                el PDF no se aporto y el importe salio de la
                                ecuacion: se busca por importe entre las
                                facturas nuevas que no resolvieron otra linea.
Retencion dd/mm-dd/mm           por importe entre las retenciones nuevas.
Ventas dd/mm al dd/mm ...       devengamiento del cierre anterior: no se arrastra.
                                Se devuelve (sistema, medio, desde, hasta, importe)
                                para que conciliar.py lo compare con lo que dice
                                el reporte nuevo de ese tramo (que ahora viene
                                desde el 1° del mes) y nombre la diferencia si la hay.
Ventas <mes> <sistema> ...      venta de un mes que el cierre anterior dejo sin
(linea con las palabras del     registrar: si el mayor nuevo ya la tiene, se
mayor, o "... NO REGISTRADAS")  resuelve y queda la diferencia si la hay; si no,
                                se arrastra.
Lineas de cruce                 no se arrastran como lineas: sus acumulados vienen
                                de la hoja "Diferencias por mes" y se suman a los
                                valores del mes nuevo.
Conceptos acumulativos          ventas fuera de la app, descuentos facturados en
                                otra semana, aminoraciones, cargos adicionales: el
                                importe anterior se suma a la linea nueva.
Cualquier otra linea            (retencion duplicada, ajuste sin respaldo, ventas
                                registradas de mas/menos, etc.) se arrastra tal cual,
                                salvo que el mayor nuevo tenga un asiento que la
                                corrija por el mismo importe con signo contrario.
SALDO FINAL anterior            si no era exactamente cero (redondeo), se arrastra
                                como "Residuo del cierre anterior" para que la
                                cadena nueva explique solo lo nuevo.
PENDIENTE DE DEFINICION         se arrastra tal cual, menos la "Diferencia sin
                                identificar", que el cierre nuevo vuelve a calcular.

Tolerancia de importe: TOL_IMPORTE (1 peso).
"""
from __future__ import annotations

import argparse
import os
import re
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import comun as K  # noqa: E402
import regresion  # noqa: E402

TOL_IMPORTE = 1.00

RE_FACTURA = re.compile(r"^Factura Pendiente\s+(\d{4}-\d{6,8})")
RE_FAC_NOAP = re.compile(r"^Factura Pendiente\s+(\d{2}/\d{2})-(\d{2}/\d{2})")
RE_ACRED = re.compile(r"^Acreditacion\s+(\d{2}/\d{2})-(\d{2}/\d{2})")
RE_RETEN = re.compile(r"^Retencion\s+(\d{2}/\d{2})-(\d{2}/\d{2})")
RE_DEVENG = re.compile(r"^Ventas\b.*?(\d{2}/\d{2}) al (\d{2}/\d{2})")
RE_NOREG = re.compile(r"^Ventas .*?(\d{2})-(\d{4})")
ACUMULATIVOS = ("Ventas con pago por fuera de la aplicación", "Diferencia Descuentos facturados",
                "Aminoración descuento comercial", "Cargos / Reintegros adicionales")


def _re_etiqueta(plantilla: str, rango: str) -> re.Pattern:
    """'Ventas D&D {rango} (online TC)' -> regex con el grupo del rango."""
    return re.compile("^" + re.escape(plantilla).replace(re.escape("{rango}"), rango) + "$")


def sistema_medio(concepto: str, cfg: dict, rango=r"(.+?)"):
    """(sistema, medio, m) si el concepto es una linea de ventas de un sistema."""
    for sis in cfg["sistemas"]:
        for medio in ("online", "efectivo"):
            m = _re_etiqueta(sis[f"etiqueta_{medio}"], rango).match(concepto)
            if m:
                return sis, medio, m
    return None, None, None


def _fecha(txt: str, anio: int) -> pd.Timestamp:
    return pd.Timestamp(f"{anio}-{txt[3:5]}-{txt[:2]}")


def leer_anterior(ruta: str) -> dict:
    """Lineas, abiertas, historico, hoja Recaudacion y hojas de detalle de la anterior."""
    d = regresion.leer(ruta)
    xl = pd.ExcelFile(ruta)
    hd = next((h for h in xl.sheet_names if "diferencia" in K.norm(h)), None)
    d["historico"] = xl.parse(hd) if hd else None
    hr = next((h for h in xl.sheet_names if K.norm(h) == "recaudacion"), None)
    d["recaudacion"] = xl.parse(hr) if hr else None
    d["hojas_detalle"] = {h: xl for h in xl.sheet_names if h not in (xl.sheet_names[0], hd, hr)}
    m = re.search(r"Saldo del mayor al (\d{2}/\d{2}/\d{4})", str(xl.parse(xl.sheet_names[0], header=None).iloc[:4, 1].tolist()))
    d["fecha"] = m.group(1) if m else "?"
    m = re.search(r"Scope de liquidaciones (\d{2}/\d{2}/\d{4}) al (\d{2}/\d{2}/\d{4})",
                  str(xl.parse(xl.sheet_names[0], header=None).iloc[:4, 1].tolist()))
    d["scope"] = (pd.Timestamp(f"{m.group(1)[6:]}-{m.group(1)[3:5]}-{m.group(1)[:2]}"),
                  pd.Timestamp(f"{m.group(2)[6:]}-{m.group(2)[3:5]}-{m.group(2)[:2]}")) if m else (None, None)
    return d


def _clave(df: pd.DataFrame) -> set:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    df = df[pd.to_numeric(df["Asiento"], errors="coerce").notna()]
    return set(zip(pd.to_numeric(df["Asiento"]).astype(int), pd.to_numeric(df["Apunte"]).astype(int),
                   pd.to_datetime(df[K.col(df, "fecha")]).dt.strftime("%Y-%m-%d"),
                   K.num(df["Debe"]).round(2), K.num(df["Haber"]).round(2)))


def asientos_nuevos(mayor: pd.DataFrame, ant: dict) -> pd.Series:
    """Mascara de los asientos del mayor que no estaban en la Recaudacion anterior."""
    if ant.get("recaudacion") is None:
        f = ant["fecha"]
        return mayor["F"] > pd.Timestamp(f"{f[6:]}-{f[3:5]}-{f[:2]}")
    prev = _clave(ant["recaudacion"])
    col_f = K.col(mayor, "fecha")
    k = list(zip(mayor["Asiento"].astype(int), mayor[K.col(mayor, "apunte")].astype(int),
                 pd.to_datetime(mayor[col_f]).dt.strftime("%Y-%m-%d"), mayor["Debe"].round(2), mayor["Haber"].round(2)))
    return pd.Series([x not in prev for x in k], index=mayor.index)


def arrastre(ruta_anterior: str, mayor: pd.DataFrame, cfg: dict, conceptos_cruce: set) -> dict:
    """Resuelve contra el mayor nuevo y devuelve lo que sigue abierto.

    Devuelve dict con:
      mayor         el mayor sin los asientos que resolvieron pendientes anteriores
      nuevos        mascara (sobre el mayor original) de los asientos nuevos
      lineas        lineas a arrastrar tal cual (con nota de arrastre)
      acred_pend    liquidaciones pendientes de cobro [{periodo, desde, hasta, total}]
      redondeo_fac  centavos entre factura pendiente y lo pagado (+ debe)
      acumulados    {prefijo de concepto: importe anterior} para sumar a la linea nueva
      devengado     [(sistema_id, medio, desde, hasta, importe)] del cierre anterior
      abiertas      lineas de "pendiente de definicion" que siguen
      historico     DataFrame de "Diferencias por mes" anterior
      anterior      lo que leyo leer_anterior (para los papeles de trabajo)
      resueltos     textos de lo que se resolvio (para el reporte)
    """
    ant = leer_anterior(ruta_anterior)
    etiqueta = f"Arrastre de la conciliación al {ant['fecha']}"
    anio = int(ant["fecha"][6:]) if ant["fecha"] != "?" else pd.Timestamp.today().year
    mayor = mayor.copy()
    es_nuevo = asientos_nuevos(mayor, ant)
    nuevos = mayor[es_nuevo]
    usados = set()
    lineas, resueltos, acum, deveng, noreg, acred_pend = [], [], {}, [], {}, []
    redondeo_fac = 0.0
    otros = nuevos[nuevos["tipo"] == "otro"]

    # acreditacion registrada dos veces: un asiento nuevo con el mismo importe y el
    # mismo comentario que una acreditacion que ya estaba en la Recaudacion anterior
    viejas = mayor[(~es_nuevo) & (mayor["tipo"] == "acreditacion")]
    for j, r in nuevos[nuevos["tipo"] == "acreditacion"].iterrows():
        dup = viejas[((viejas["Haber"] - r["Haber"]).abs() <= 0.005) & (viejas["com"] == r["com"])]
        if len(dup):
            usados.add(j)
            d0 = dup.iloc[0]
            lineas.append({"concepto": f"Acreditación duplicada: '{r['com']}' asiento {r['Asiento']} del {r['F']:%d/%m/%Y}",
                           "debe": round(r["Haber"], 2), "flag": True,
                           "nota": f"Mismo importe y comentario que el asiento {d0['Asiento']} del {d0['F']:%d/%m/%Y}, ya conciliado — se revierte"})
            resueltos.append(f"acreditacion duplicada: asiento {r['Asiento']} ({r['Haber']:,.2f}) repite al {d0['Asiento']}")

    def fila_por_importe(tipo, importe):
        sub = nuevos[(nuevos["tipo"] == tipo) & (~nuevos.index.isin(usados))]
        hit = sub[(sub["Haber"] - importe).abs() <= TOL_IMPORTE]
        return hit.index[0] if len(hit) else None

    def resolver_factura(c, ident, imp, hit):
        nonlocal redondeo_fac
        usados.update(hit.index)
        pag = round(hit["Haber"].sum(), 2)
        dif = round(pag - (-imp), 2)          # + pagado de mas (debe), - pagado de menos (haber)
        if abs(dif) > TOL_IMPORTE:
            lineas.append({"concepto": f"Factura {ident} pagada con diferencia", "debe" if dif > 0 else "haber": abs(dif),
                           "nota": f"Factura {-imp:,.2f} · pagado {pag:,.2f} · revisar percepción IIBB", "flag": True})
            resueltos.append(f"{c}: pagada con diferencia ({dif:,.2f})")
        else:
            redondeo_fac += dif
            resueltos.append(f"{c}: pagada el {hit['F'].iloc[0]:%d/%m/%Y}" + (f" (redondeo {dif:,.2f})" if dif else ""))

    for ln in ant["lineas"]:
        c, imp = ln["concepto"], ln["importe"]          # imp: + debe, - haber
        if c in conceptos_cruce:
            continue
        m = RE_ACRED.match(c)
        if m:
            a, b = _fecha(m.group(1), anio), _fecha(m.group(2), anio)
            if b < a:
                b = _fecha(m.group(2), anio + 1)
            acred_pend.append({"periodo": f"{m.group(1)}-{m.group(2)}", "desde": f"{a:%d/%m/%Y}",
                               "hasta": f"{b:%d/%m/%Y}", "total": round(-imp, 2), "anterior": True})
            continue
        m = RE_FACTURA.match(c)
        if m:
            ident = m.group(1)
            alt = [ident, ident.replace("0026", "0023", 1), ident.replace("0013", "0023", 1)]
            sub = nuevos[(nuevos["tipo"] == "factura") & (~nuevos.index.isin(usados))]
            hit = sub[sub["factura"].isin(alt)]
            if len(hit):
                resolver_factura(c, ident, imp, hit)
                continue
            # numero mal tipeado: pagada por el importe exacto bajo otro numero
            hit = sub[(sub["Haber"] - (-imp)).abs() <= TOL_IMPORTE].iloc[:1]
            if len(hit):
                otro = hit["factura"].iloc[0] or hit["com"].iloc[0]
                resolver_factura(c, ident, imp, hit)
                lineas.append({"concepto": f"Factura {ident} registrada como {otro} en el mayor",
                               "nota": f"Pagada el {hit['F'].iloc[0]:%d/%m} por {hit['Haber'].iloc[0]:,.2f} — solo corregir el número",
                               "flag": True})
                resueltos[-1] += f" · registrada como {otro}"
            else:
                lineas.append({"concepto": c, "haber": -imp, "nota": f"{etiqueta} — sigue sin registrar", "flag": True})
            continue
        m = RE_FAC_NOAP.match(c)
        if m or c.startswith("Factura Pendiente"):
            sub = nuevos[(nuevos["tipo"] == "factura") & (~nuevos.index.isin(usados))]
            hit = sub[(sub["Haber"] - (-imp)).abs() <= TOL_IMPORTE]
            if len(hit):
                hit = hit.iloc[:1]
                resolver_factura(c, hit["factura"].iloc[0] or hit["com"].iloc[0], imp, hit)
                resueltos[-1] += f" · factura {hit['factura'].iloc[0] or hit['com'].iloc[0]}"
            else:
                lineas.append({"concepto": c, "haber": -imp, "nota": f"{etiqueta} — sigue sin registrar", "flag": True})
            continue
        if RE_RETEN.match(c):
            j = fila_por_importe("retencion", -imp)
            if j is not None:
                usados.add(j)
                resueltos.append(f"{c}: registrada el {mayor.loc[j, 'F']:%d/%m/%Y}")
            else:
                lineas.append({"concepto": c, "haber": -imp, "nota": f"{etiqueta} — sigue sin registrar", "flag": True})
            continue
        m = RE_DEVENG.match(c)
        if m:
            sis, medio, _ = sistema_medio(c, cfg)
            a, b = _fecha(m.group(1), anio), _fecha(m.group(2), anio)
            if b < a:
                b = _fecha(m.group(2), anio + 1)
            deveng.append((sis["id"] if sis else None, medio, a, b, imp))
            continue
        sis, medio, m = sistema_medio(c.replace(" — NO REGISTRADAS", ""), cfg, rango=r"(\d{2})-(\d{4})")
        if sis is None:
            m = RE_NOREG.match(c) if "NO REGISTRADAS" in c else None
            if m:
                sis = next((s for s in cfg["sistemas"] if s["mayor"].lower() in c.lower()), None)
                medio = "efectivo" if "efectivo" in c.lower() else "online"
        if m and sis is not None:
            ms = f"{m.group(2)}-{m.group(1)}"
            sub = nuevos[(nuevos["tipo"] == "venta") & (nuevos["mes"] == ms) & (nuevos["sistema"] == sis["id"])]
            if len(sub):
                usados |= set(sub.index)
                noreg[(sis["id"], ms)] = noreg.get((sis["id"], ms), [0.0, 0.0, sis])
                noreg[(sis["id"], ms)][0] += imp
                noreg[(sis["id"], ms)][1] = round(sub["Debe"].sum(), 2)
                resueltos.append(f"{c}: registrada en el mayor nuevo")
            else:
                lineas.append({"concepto": c, "debe": imp, "nota": f"{etiqueta} — sigue sin registrar", "flag": True})
            continue
        pref = next((p for p in ACUMULATIVOS if c.startswith(p)), None)
        if pref:
            acum[pref] = acum.get(pref, 0.0) + imp
            continue
        # cualquier otra: se arrastra salvo que el mayor nuevo la corrija
        corr = otros[(~otros.index.isin(usados)) & ((otros["Debe"] - otros["Haber"] + imp).abs() <= TOL_IMPORTE)]
        if len(corr):
            usados.add(corr.index[0])
            resueltos.append(f"{c}: corregida con el asiento {corr.iloc[0]['Asiento']}")
        else:
            lineas.append({"concepto": c, "debe" if imp > 0 else "haber": abs(imp), "nota": etiqueta, "flag": True})

    # ventas de un mes que el cierre anterior dejo sin registrar y el mayor nuevo ya tiene
    for (sid, ms), (real, reg, sis) in noreg.items():
        dif = round(real - reg, 2)
        if abs(dif) > TOL_IMPORTE:
            lineas.append({"concepto": f"Ventas {sis['etiqueta_corta']} {ms[5:]}-{ms[:4]} registradas {'de menos' if dif > 0 else 'de más'}",
                           "debe" if dif > 0 else "haber": abs(dif), "nota": f"Reporte {real:,.2f} vs mayor {reg:,.2f} ({etiqueta})"})
    # saldo final anterior distinto de cero (redondeo): se arrastra con nombre
    # el SALDO FINAL anterior sigue dentro del mayor nuevo: la linea es su contrapartida
    fin_ant = round(ant.get("final") or 0.0, 2)
    if fin_ant:
        lineas.append({"concepto": "Residuo del cierre anterior (redondeo)", "haber" if fin_ant > 0 else "debe": abs(fin_ant),
                       "nota": f"SALDO FINAL {fin_ant:,.2f} de la conciliación al {ant['fecha']}", "flag": True})
    abiertas = [{"concepto": x["concepto"], "debe" if x["importe"] > 0 else "haber": abs(x["importe"]),
                 "nota": etiqueta} for x in ant["abierto"] if not x["concepto"].startswith("Diferencia sin identificar")]
    return {"mayor": mayor.drop(index=list(usados)), "nuevos": es_nuevo, "lineas": lineas, "acred_pend": acred_pend,
            "redondeo_fac": round(redondeo_fac, 2), "acumulados": acum, "devengado": deveng, "abiertas": abiertas,
            "historico": normalizar_historico(ant["historico"], cfg, ant["lineas"], f"Reclasif. cierre {ant['fecha']}"),
            "anterior": ant, "resueltos": resueltos, "etiqueta": etiqueta}


def _clave_concepto(con: str):
    """match2 / match / falta_peya / falta_sistema / nc / nd100 / nd50 segun el texto."""
    n = K.norm(con)
    if n.startswith("diferencia") and "match" in n:
        return "match2" if ("2do" in n or "segundo" in n or "fecha" in n) else "match"
    if n.startswith("faltapeya"):
        return "falta_peya"
    if n.startswith("falta"):
        return "falta_sistema"
    if n.startswith("nc"):
        return "nc"
    if n.startswith("nd"):
        return "nd100" if "100" in n else "nd50"
    return None


def _sistema_de(con: str, local: str, cfg: dict):
    """Sistema al que pertenece una fila del historico: por local, si no por el texto."""
    for sis in cfg["sistemas"]:
        for k, d in (sis.get("locales") or {}).items():
            if local and K.norm(local) in (K.norm(k), K.norm(d.get("nombre", k))):
                return sis
    n = K.norm(con)
    for sis in cfg["sistemas"]:
        if any(K.norm(x) in n for x in (sis["etiqueta_corta"], sis["nombre"], sis["id"], sis["mayor"]) if x):
            return sis
    return None


def normalizar_historico(prev: pd.DataFrame, cfg: dict, lineas_ant: list, etiqueta_ajuste: str) -> pd.DataFrame:
    """Lleva la hoja "Diferencias por mes" anterior a los nombres del YAML.

    Las conciliaciones viejas pueden traer nombres abreviados u otra descomposicion
    (por ejemplo, los tickets con reintegro en "Falta PeYa" y la NC con el
    reintegro positivo). Las lineas de la conciliacion anterior son lo aprobado:
    si el total de un concepto en el historico no ata con su linea, la diferencia
    queda en una columna propia ("Reclasif. ...") para que cada fila sume lo que
    dice la linea. Nada se reparte por mes a mano.
    """
    if prev is None or prev.empty:
        return prev
    p = prev.copy()
    p = p[~p.iloc[:, 0].astype(str).str.upper().eq("TOTAL")]
    if "Local" not in p.columns:
        p["Local"] = ""
    p["Local"] = p["Local"].fillna("")
    nombres = {v for s_ in cfg["sistemas"] for v in s_["lineas"].values()}
    nuevos = []
    for con, loc in zip(p["Concepto"].astype(str), p["Local"].astype(str)):
        if con in nombres:
            nuevos.append(con)
            continue
        sis, k = _sistema_de(con, loc, cfg), _clave_concepto(con)
        nuevos.append(sis["lineas"].get(k, con) if (sis and k) else con)
    p["Concepto"] = nuevos
    mcols = [c for c in p.columns if re.match(r"\d{4}-\d{2}$", str(c)) or str(c).startswith("Reclasif")]
    tot = p.groupby("Concepto")[mcols].sum().sum(axis=1)
    lin = {x["concepto"]: x["importe"] for x in lineas_ant}
    filas = []
    for con in nombres:
        dif = round(lin.get(con, 0.0) - float(tot.get(con, 0.0)), 2)
        if abs(dif) > 0.005 and (con in lin or con in tot.index):
            filas.append({"Concepto": con, "Local": "", etiqueta_ajuste: dif})
    if filas:
        p = pd.concat([p, pd.DataFrame(filas)], ignore_index=True)
    return p


def acumular_historico(prev: pd.DataFrame, nuevo: pd.DataFrame, por_local: bool) -> pd.DataFrame:
    """Une el historico anterior (ancho) con las filas nuevas (largo: Concepto, Local, Mes, Importe)."""
    if prev is None or prev.empty:
        return nuevo
    p = prev.copy()
    p = p[~p.iloc[:, 0].astype(str).str.upper().eq("TOTAL")]
    if "Local" not in p.columns:
        p["Local"] = ""
    mcols = [c for c in p.columns if re.match(r"\d{4}-\d{2}$", str(c)) or str(c).startswith("Reclasif")]
    largo = p.melt(id_vars=["Concepto", "Local"], value_vars=mcols, var_name="Mes", value_name="Importe")
    largo["Local"] = largo["Local"].fillna("")
    largo = largo[largo["Importe"].fillna(0) != 0]
    return pd.concat([largo, nuevo], ignore_index=True)


def detalle_anterior(ant: dict, hoja: str, desde: pd.Timestamp, hasta: pd.Timestamp) -> pd.DataFrame | None:
    """Filas de una hoja de detalle de la conciliacion anterior con fecha entre desde y hasta.

    Es lo que mantiene en los papeles de trabajo los movimientos de los primeros
    dias del mes, que entraron en el cierre anterior.
    """
    xl = ant.get("hojas_detalle", {}).get(hoja)
    if xl is None:
        return None
    df = xl.parse(hoja)
    df = df[~df.iloc[:, 0].astype(str).str.upper().eq("TOTAL")]
    cf = K.col(df, "fecha de pedido") or K.col(df, "fecha del pedido") or K.col(df, "fecha")
    if cf is None:
        return None
    f = pd.to_datetime(df[cf], errors="coerce").dt.normalize()
    return df[(f >= desde) & (f <= hasta)].copy()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--anterior", required=True)
    ap.add_argument("--mayor", required=True)
    ap.add_argument("--entidad", required=True)
    a = ap.parse_args()
    import conciliar
    cfg = K.leer_config(a.entidad)
    mayor = conciliar.leer_mayor(a.mayor, cfg)
    cruce = {v for s in cfg["sistemas"] for v in s["lineas"].values()}
    r = arrastre(a.anterior, mayor, cfg, cruce)
    print(f"ASIENTOS NUEVOS EN EL MAYOR: {int(r['nuevos'].sum())}")
    print("\nRESUELTOS:")
    for x in r["resueltos"]:
        print("  OK ", x)
    print("\nACREDITACIONES PENDIENTES DEL CIERRE ANTERIOR (van a la asignacion con las liquidaciones nuevas):")
    for x in r["acred_pend"]:
        print(f"  {x['periodo']}  {x['total']:,.2f}")
    print("\nSE ARRASTRAN:")
    for x in r["lineas"]:
        print(f"  --  {x['concepto']}  {x.get('debe', 0) - x.get('haber', 0):,.2f}")
    print("\nACUMULADOS QUE SE SUMAN A LA LINEA NUEVA:")
    for k, v in r["acumulados"].items():
        print(f"  {k}: {v:,.2f}")
    print("\nDEVENGADO DEL CIERRE ANTERIOR (se compara con el reporte nuevo del mismo tramo):")
    for sid, medio, a_, b_, v in r["devengado"]:
        print(f"  {sid} {medio} {a_:%d/%m}-{b_:%d/%m}: {v:,.2f}")
    if r["redondeo_fac"]:
        print(f"\nREDONDEO DE FACTURAS: {r['redondeo_fac']:,.2f}")


if __name__ == "__main__":
    main()
