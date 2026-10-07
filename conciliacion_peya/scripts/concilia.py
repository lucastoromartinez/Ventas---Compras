#!/usr/bin/env python3
"""
Corre la conciliacion completa de punta a punta: ZIPs y facturas -> cruces ->
cadena de conceptos -> Excel.

USO
---
    # con una carpeta que respeta la convencion (abajo)
    python concilia.py --carpeta ~/PeYa/ronda/2026-09 [--entidad ronda] [--anterior Conciliacion_ago.xlsx]

    # o con cada archivo a mano
    python concilia.py --entidad easa --zips ./zips --facturas ./facturas --mayor recaudacion.xlsx \\
        --dean Dean.xlsx --hio-documento hio_doc.xlsx --hio-metodo hio_met.xlsx [--anterior ...]

CONVENCION DE CARPETA
---------------------
    <carpeta>/
        zips/              estados de cuenta semanales (.zip), sin huecos
        facturas/          PDFs de facturas PEDIDOSYA (0026) y PAGOS YA (0013)
        *recaud*.xlsx      el mayor de la cuenta (o *mayor*.xlsx)
        *dean*.xlsx        ventas Dean                      (EASA)
        *documento*.xlsx   reporte HIO por documento
        *metodo*.xlsx      reporte HIO por metodo de pago
        *atalaya*.xlsx     tickets Atalaya                  (Ronda)
        anterior/          la conciliacion del mes anterior (cierre mensual), opcional
        salida/            aca queda el Excel y la carpeta de trabajo

La entidad se deduce del numero de local que traen los ZIP (411335 = EASA,
409994 = Ronda); --entidad la fija a mano si hace falta.
"""
from __future__ import annotations

import argparse
import glob
import os
import re
import subprocess
import sys

AQUI = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, AQUI)
import comun as K  # noqa: E402


def buscar(carpeta, *pats, ext="xlsx"):
    for p in pats:
        hits = sorted(f for f in glob.glob(os.path.join(carpeta, f"*.{ext}"))
                      if re.search(p, os.path.basename(f), re.I) and not os.path.basename(f).startswith("~$"))
        if hits:
            return hits[0]
    return None


def correr(script, *args):
    cmd = [sys.executable, "-I", os.path.join(AQUI, script)] + [str(x) for x in args if x is not None]
    print(f"\n$ {' '.join(os.path.basename(c) if i < 3 else c for i, c in enumerate(cmd))}")
    r = subprocess.run(cmd)
    if r.returncode != 0:
        raise SystemExit(f"{script} termino con error")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--carpeta", help="carpeta con la convencion de arriba")
    ap.add_argument("--entidad", help="easa | ronda (se deduce de los ZIP si se omite)")
    ap.add_argument("--zips")
    ap.add_argument("--facturas")
    ap.add_argument("--mayor")
    ap.add_argument("--anterior", help="conciliacion del mes anterior (cierre mensual)")
    ap.add_argument("--salida", help="carpeta de salida (default: <carpeta>/salida o ./salida)")
    ap.add_argument("--esperado", help="JSON de regresion para comparar al final")
    K.args_sistemas(ap)
    a = ap.parse_args()

    if a.carpeta:
        c = a.carpeta
        a.zips = a.zips or os.path.join(c, "zips")
        a.facturas = a.facturas or os.path.join(c, "facturas")
        a.mayor = a.mayor or buscar(c, r"recaud", r"mayor")
        a.dean = a.dean or buscar(c, r"dean")
        a.hio_documento = a.hio_documento or buscar(c, r"documento")
        a.hio_metodo = a.hio_metodo or buscar(c, r"metodo")
        a.atalaya = a.atalaya or buscar(c, r"atalaya")
        if not a.anterior and os.path.isdir(os.path.join(c, "anterior")):
            a.anterior = buscar(os.path.join(c, "anterior"), r"concil", r".")
        a.salida = a.salida or os.path.join(c, "salida")
    if not (a.zips and a.facturas and a.mayor):
        raise SystemExit("Faltan --zips, --facturas o --mayor (o una --carpeta con la convencion)")
    a.salida = a.salida or "./salida"
    os.makedirs(a.salida, exist_ok=True)
    trabajo = os.path.join(a.salida, "trabajo")

    entidad = a.entidad or K.detectar_entidad(a.zips)
    if not entidad:
        raise SystemExit("No pude deducir la entidad de los ZIP: indicar --entidad easa|ronda")
    cfg = K.leer_config(entidad)
    print(f"Entidad: {cfg['nombre']} ({entidad})")
    print(f"  zips: {a.zips}\n  facturas: {a.facturas}\n  mayor: {a.mayor}")
    for k in ("dean", "hio_documento", "hio_metodo", "atalaya", "anterior"):
        if getattr(a, k):
            print(f"  {k}: {getattr(a, k)}")

    correr("armar_peya.py", "--zips", a.zips, "--facturas", a.facturas, "--salida", trabajo)
    sis = []
    for k, flag in (("dean", "--dean"), ("hio_documento", "--hio-documento"), ("hio_metodo", "--hio-metodo"),
                    ("atalaya", "--atalaya")):
        if getattr(a, k):
            sis += [flag, getattr(a, k)]
    correr("cruzar.py", "--entidad", entidad, "--trabajo", trabajo, *sis)
    correr("conciliar.py", "--entidad", entidad, "--trabajo", trabajo, "--mayor", a.mayor,
           *(["--anterior", a.anterior] if a.anterior else []))

    import json
    cfg_out = json.load(open(os.path.join(trabajo, "conceptos.json"), encoding="utf-8"))
    m = re.search(r"al (\d{2})/(\d{2})/(\d{4})", cfg_out["subtitulo"])
    periodo = f"{m.group(3)}-{m.group(2)}" if m else "periodo"
    excel = os.path.join(a.salida, f"Conciliacion_PeYa_{cfg['nombre']}_{periodo}.xlsx")
    correr("generar_conciliacion.py", "--json", os.path.join(trabajo, "conceptos.json"), "--salida", excel)
    recalc = "/mnt/skills/public/xlsx/scripts/recalc.py"
    if os.path.exists(recalc):
        subprocess.run([sys.executable, recalc, excel], capture_output=True)
    print(f"\nListo: {excel}")
    if a.esperado:
        correr("regresion.py", "--generado", excel, "--esperado", a.esperado)


if __name__ == "__main__":
    main()
