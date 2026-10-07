#!/usr/bin/env python3
"""
Valida la ecuacion interna de cada liquidacion de PedidosYa:

    Ventas netas + Reintegros = Acreditacion + Facturas + Retencion SIRTAC + Ventas fuera de app

Una liquidacion que no cierra casi siempre significa que falta un comprobante
(tipicamente la factura de PAGOS YA, que es chica y a veces no se descarga).
El importe que falta suele aparecer despues disfrazado de "acreditacion
registrada de mas" en el mayor, ya compensado.

USO
---
    python validar_liquidaciones.py --carpeta ./liquidaciones

Extrae los conceptos de cada PDF de estado de cuenta y corre la ecuacion.
Las facturas se pasan aparte porque hay que asociarlas al periodo:

    python validar_liquidaciones.py --carpeta ./liq --facturas facturas.json

facturas.json:
    {"23/02-01/03": [468454.82, 67838.69], "02/03-08/03": [1818757.78]}

SALIDA
------
Una linea por liquidacion con izquierda, derecha y diferencia. Diferencia
distinta de cero = falta un comprobante de ese importe.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import subprocess
import sys

# Conceptos que se buscan en el PDF. Las claves son las que usa el resto
# del pipeline; los valores son fragmentos de texto tal como aparecen.
CONCEPTOS = {
    "ventas_netas": ["ventas netas", "venta neta"],
    "acreditacion": ["total liquidado", "subtotal", "total a acreditar"],
    "fuera_app": ["fuera de la aplicaci", "fuera de la app"],
    "retencion": ["sirtac", "retenciones fiscales"],
    "reintegros": ["reintegros por pedidos", "reintegro por pedidos"],
}


def _num(txt: str) -> float:
    """'1.234.567,89' -> 1234567.89 ; tambien soporta negativos con parentesis."""
    txt = txt.strip().replace("ARS", "").strip()
    neg = txt.startswith("(") or txt.startswith("-")
    txt = txt.strip("()-").replace(".", "").replace(",", ".")
    try:
        v = float(txt)
    except ValueError:
        return 0.0
    return -v if neg else v


def extraer_pdf(path: str) -> dict:
    """Extrae los conceptos de un PDF de estado de cuenta."""
    try:
        txt = subprocess.run(
            ["pdftotext", "-layout", path, "-"],
            capture_output=True, text=True, check=True,
        ).stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        print(f"  no se pudo leer {os.path.basename(path)}", file=sys.stderr)
        return {}

    out = {k: 0.0 for k in CONCEPTOS}
    for linea in txt.split("\n"):
        bajo = linea.lower()
        montos = re.findall(r"-?[\d\.]+,\d{2}", linea)
        if not montos:
            continue
        for clave, patrones in CONCEPTOS.items():
            if out[clave]:            # primera aparicion gana
                continue
            if any(p in bajo for p in patrones):
                out[clave] = abs(_num(montos[-1]))

    # El periodo sale del nombre de archivo: YYYYMMDD_YYYYMMDD_...
    m = re.search(r"(\d{4})(\d{2})(\d{2})_(\d{4})(\d{2})(\d{2})", os.path.basename(path))
    out["periodo"] = (
        f"{m.group(3)}/{m.group(2)}-{m.group(6)}/{m.group(5)}" if m
        else os.path.basename(path)[:20]
    )
    return out


def validar(carpeta: str, facturas: dict | None = None, tolerancia: float = 1.0) -> list:
    facturas = facturas or {}
    pdfs = sorted(glob.glob(os.path.join(carpeta, "**", "*.pdf"), recursive=True))
    pdfs = [p for p in pdfs if "factura" not in os.path.basename(p).lower()]

    if not pdfs:
        print(f"No se encontraron PDFs de liquidacion en {carpeta}", file=sys.stderr)
        return []

    filas, abiertas = [], []
    print(f"{'PERIODO':<16}{'IZQUIERDA':>16}{'DERECHA':>16}{'DIFERENCIA':>14}")
    print("-" * 62)

    for p in pdfs:
        d = extraer_pdf(p)
        if not d:
            continue
        fact = sum(facturas.get(d["periodo"], []))
        izq = d["ventas_netas"] + d["reintegros"]
        der = d["acreditacion"] + fact + d["retencion"] + d["fuera_app"]
        dif = round(izq - der, 2)
        d.update(facturas=fact, izquierda=izq, derecha=der, diferencia=dif)
        filas.append(d)

        flag = "" if abs(dif) <= tolerancia else "  <-- falta comprobante"
        if flag:
            abiertas.append(d)
        print(f"{d['periodo']:<16}{izq:>16,.2f}{der:>16,.2f}{dif:>14,.2f}{flag}")

    print("-" * 62)
    if abiertas:
        print(f"\n{len(abiertas)} liquidacion(es) sin cerrar:")
        for d in abiertas:
            print(f"  {d['periodo']}: falta un comprobante por {abs(d['diferencia']):,.2f}")
        print("\nBuscar la factura de PAGOS YA de ese periodo. Si el mayor registro")
        print("la acreditacion por un importe mayor al real, probablemente ya este")
        print("compensado y no haya que ajustar nada.")
    else:
        print("\nTodas las liquidaciones cierran.")

    return filas


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--carpeta", required=True, help="carpeta con los PDF de estado de cuenta")
    ap.add_argument("--facturas", help="JSON con las facturas por periodo")
    ap.add_argument("--tolerancia", type=float, default=1.0)
    ap.add_argument("--salida", help="guardar el resultado como JSON")
    a = ap.parse_args()

    fact = json.load(open(a.facturas, encoding="utf-8")) if a.facturas else None
    filas = validar(a.carpeta, fact, a.tolerancia)

    if a.salida:
        with open(a.salida, "w", encoding="utf-8") as f:
            json.dump(filas, f, indent=2, ensure_ascii=False)
        print(f"\nGuardado en {a.salida}")


if __name__ == "__main__":
    main()
