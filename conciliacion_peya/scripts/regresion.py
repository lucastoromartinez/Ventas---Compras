#!/usr/bin/env python3
"""
Test de regresion: compara una conciliacion generada contra una de referencia
(o contra un JSON de valores esperados) linea por linea.

USO
---
    # contra otro Excel (por ejemplo la conciliacion aprobada por el usuario)
    python regresion.py --generado Conciliacion_nueva.xlsx --referencia Conciliacion_aprobada.xlsx

    # guardar lo esperado a partir de un Excel aprobado
    python regresion.py --guardar tests/easa_2026-08.json --referencia Conciliacion_aprobada.xlsx

    # contra lo esperado (lo que se corre despues de tocar los scripts)
    python regresion.py --generado Conciliacion_nueva.xlsx --esperado tests/easa_2026-08.json

Que compara: saldo del mayor, saldo final, cada linea de la hoja
"conciliacion" por concepto (importe con tolerancia de 1 peso), las lineas
que estan en un solo lado, la lista de hojas y la cantidad de filas de cada
una. Devuelve codigo 1 si algo no coincide.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata

import openpyxl

TOL = 1.00


def _n(x: str) -> str:
    x = unicodedata.normalize("NFKD", str(x)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", x).strip()


def leer(ruta: str) -> dict:
    wb = openpyxl.load_workbook(ruta)
    ws = next(w for w in wb.worksheets if _n(w.title).startswith("concilia"))
    fila0 = next(r for r in range(1, ws.max_row + 1) if _n(ws.cell(r, 2).value) == "concepto")
    out = {"saldo": None, "lineas": [], "abierto": [], "hojas": {w.title: max(w.max_row - 2, 0) for w in wb.worksheets}}
    s, en_abierto = None, False
    for r in range(fila0 + 1, ws.max_row + 1):
        c = ws.cell(r, 2).value
        if c is None:
            continue
        c = str(c).strip()
        d, h = ws.cell(r, 3).value, ws.cell(r, 4).value
        d = float(d) if isinstance(d, (int, float)) else 0.0
        h = float(h) if isinstance(h, (int, float)) else 0.0
        if c == "Saldo":
            s = float(ws.cell(r, 5).value)
            out["saldo"] = s
            continue
        if "PENDIENTE DE DEFINICI" in c.upper():
            en_abierto = True
            continue
        if c.startswith("SALDO FINAL"):
            out["final"] = round(s, 2)
            continue
        if c.lower().startswith("subtotal") or c.lower().startswith("verificaci"):
            continue
        if en_abierto:
            if d or h:
                out["abierto"].append({"concepto": c, "importe": round(d - h, 2)})
            continue
        s = s + d - h
        out["lineas"].append({"concepto": c, "importe": round(d - h, 2)})
    out["final"] = round(s, 2) if "final" not in out else out["final"]
    return out


def comparar(gen: dict, ref: dict) -> int:
    errores = 0
    print(f"{'':<70}{'referencia':>16}{'generado':>16}{'dif':>14}")
    print("-" * 116)
    for k, lab in (("saldo", "Saldo del mayor"), ("final", "SALDO FINAL")):
        a, b = ref.get(k), gen.get(k)
        ok = a is not None and b is not None and abs(a - b) <= TOL
        errores += 0 if ok else 1
        print(f"{lab:<70}{a or 0:>16,.2f}{b or 0:>16,.2f}{(b or 0) - (a or 0):>14,.2f} {'' if ok else '  <-- DIFIERE'}")
    print()
    rg = {_n(x["concepto"]): x for x in gen["lineas"]}
    rr = {_n(x["concepto"]): x for x in ref["lineas"]}
    comunes = [k for k in rr if k in rg]
    for k in comunes:
        a, b = rr[k]["importe"], rg[k]["importe"]
        ok = abs(a - b) <= TOL
        errores += 0 if ok else 1
        print(f"{rr[k]['concepto'][:69]:<70}{a:>16,.2f}{b:>16,.2f}{b - a:>14,.2f} {'' if ok else '  <-- DIFIERE'}")
    solo_ref = [rr[k] for k in rr if k not in rg]
    solo_gen = [rg[k] for k in rg if k not in rr]
    if solo_ref:
        print("\nSolo en la referencia:")
        for x in solo_ref:
            print(f"  {x['concepto'][:80]:<82}{x['importe']:>16,.2f}")
    if solo_gen:
        print("\nSolo en la generada:")
        for x in solo_gen:
            print(f"  {x['concepto'][:80]:<82}{x['importe']:>16,.2f}")
    if solo_ref or solo_gen:
        sr, sg = sum(x["importe"] for x in solo_ref), sum(x["importe"] for x in solo_gen)
        print(f"\n  suma solo referencia {sr:,.2f} · suma solo generada {sg:,.2f} · neto {sg - sr:,.2f}")
        errores += 1
    print("\nHojas:")
    for h in dict.fromkeys(list(ref["hojas"]) + list(gen["hojas"])):
        a, b = ref["hojas"].get(h), gen["hojas"].get(h)
        mark = "" if (a is not None and b is not None) else "  <-- solo " + ("referencia" if b is None else "generada")
        if mark:
            errores += 1
        print(f"  {h:<34}{'' if a is None else a:>8}{'' if b is None else b:>8}{mark}")
    print(f"\n{'OK: coincide' if errores == 0 else f'{errores} diferencia(s)'}")
    return 1 if errores else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--generado")
    ap.add_argument("--referencia")
    ap.add_argument("--esperado")
    ap.add_argument("--guardar")
    a = ap.parse_args()
    if a.guardar:
        json.dump(leer(a.referencia or a.generado), open(a.guardar, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print(f"Esperado guardado en {a.guardar}")
        return
    gen = leer(a.generado)
    ref = leer(a.referencia) if a.referencia else json.load(open(a.esperado, encoding="utf-8"))
    sys.exit(comparar(gen, ref))


if __name__ == "__main__":
    main()
