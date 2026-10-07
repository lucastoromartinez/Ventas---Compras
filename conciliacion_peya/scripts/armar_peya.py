#!/usr/bin/env python3
"""
Arma archivos_peya.xlsx a partir de los ZIP de estado de cuenta, y extrae
los conceptos de cada liquidacion (PDF) y de cada factura (PDF).

Cada ZIP trae los xls del periodo: estado-de-cuenta (lista de pedidos),
cargos-por-reclamos, cargos-por-cancelaciones, reintegros; mas el PDF de
la liquidacion.

USO
---
    python armar_peya.py --zips ./zips --facturas ./facturas --salida ./trabajo

Genera en la carpeta de salida:
    archivos_peya.xlsx   Lista Pedidos / Reclamos / Cancelaciones / Reintegros
    liquidaciones.json   conceptos de cada liquidacion
    facturas.json        total de cada factura, por periodo

DEPURACION
----------
Las columnas identificadoras (Numero de pedido, Localizador, Nro Transaccion)
pasan a texto sin decimales: vienen como float (2220387818.0) y del otro lado
del cruce son texto, asi que sin esto el merge da cero matches sin avisar.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import subprocess
import unicodedata
import zipfile

import pandas as pd

IDS = {"numerodepedido", "nrodepedido", "ndepedido", "localizador",
       "nrotransaccion", "numerodetransaccion", "ntransaccion"}

# PedidosYa cambio los encabezados en el medio: el formato nuevo usa otros
# nombres para las mismas tres columnas. Sin renombrar, al concatenar quedan
# como columnas separadas y medio archivo cae en la copia equivocada.
RENOMBRAR = {
    "Numero de Pedido": "Número de pedido",
    "Fecha de Pedido": "Fecha de pedido",
    "Monto neto de la Venta ($)": "Monto de Venta Neta ($)",
}

TIPOS = {"estado-de-cuenta": "Lista Pedidos", "cargos-por-reclamos": "Reclamos",
         "cargos-por-cancelaciones": "Cancelaciones", "reintegros": "Reintegros"}

# Conceptos que se leen del PDF de liquidacion. El reintegro que entra en la
# ecuacion es el de cancelaciones: el "descuento neto otorgado" ya viene
# restado dentro del neto de la factura, sumarlo lo contaria dos veces.
PAT = {
    "ventas_netas": r"^Ventas netas\s+ARS\s+(-?[\d\.]+,\d{2})",
    "fuera_app":    r"^Ventas con pago fuera de la App cobradas\s+ARS\s+(-?[\d\.]+,\d{2})",
    "reint_canc":   r"^Reintegro por pedidos que cancelaron los usuarios\*?\s+ARS\s+(-?[\d\.]+,\d{2})",
    "desc_otorg":   r"^Descuento neto otorgado Peya a usuarios\s+ARS\s+(-?[\d\.]+,\d{2})",
    "desc_com":     r"^Descuento comercial neto otorgado por PedidosYa\s+ARS\s+(-?[\d\.]+,\d{2})",
    "sirtac":       r"^Retenciones? Fiscales? \(SIRTAC\)\s+ARS\s+(-?[\d\.]+,\d{2})",
    "ajustes":      r"^Ajustes de liquidación\s+ARS\s+(-?[\d\.]+,\d{2})",
    "cargos_adic":  r"^Cargos / Reintegros adicionales\s+ARS\s+(-?[\d\.]+,\d{2})",
    "aminoracion":  r"^Aminoración descuento comercial\s+ARS\s+(-?[\d\.]+,\d{2})",
}


def _norm(x: str) -> str:
    x = unicodedata.normalize("NFKD", str(x)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", x.lower())


def _ars(t: str) -> float:
    neg = t.strip().startswith("-")
    v = float(t.strip().lstrip("-").replace(".", "").replace(",", "."))
    return -v if neg else v


def depurar(df: pd.DataFrame) -> pd.DataFrame:
    """Fechas a datetime, floats a 2 decimales, identificadores a texto limpio."""
    df = df.copy()
    for c in df.columns:
        n = _norm(c)
        if "fecha" in n:
            df[c] = pd.to_datetime(df[c], dayfirst=True, errors="coerce")
        elif n in IDS:
            df[c] = pd.to_numeric(df[c].astype(str).str.strip(),
                                  errors="coerce").astype("Int64").astype(str)
    f = df.select_dtypes(include="float64").columns
    df[f] = df[f].round(2)
    return df


def extraer_zips(carpeta_zips: str, tmp: str) -> None:
    os.makedirs(tmp, exist_ok=True)
    zs = sorted(glob.glob(os.path.join(carpeta_zips, "*.zip")))
    if not zs:
        raise SystemExit(f"No hay ZIPs en {carpeta_zips}")
    for z in zs:
        with zipfile.ZipFile(z) as f:
            f.extractall(tmp)
    print(f"  {len(zs)} ZIPs extraidos")


def _destino(nombre: str) -> str | None:
    """Mapea un nombre de hoja o de archivo al destino de archivos_peya."""
    n = re.sub(r"[^a-z0-9]", "", _norm(nombre))
    if "cancelacion" in n:
        return "Cancelaciones"
    if "reclamo" in n:
        return "Reclamos"
    if "reintegro" in n:
        return "Reintegros"
    if "listadepedidos" in n or "estadodecuenta" in n or n == "sheet1":
        return "Lista Pedidos"
    return None


def control_cobertura(carpeta_zips: str, tmp: str) -> None:
    """Avisa qué semanas no traen cada tipo de dato.

    PedidosYa cambió el formato en el medio: al principio cada concepto venía en
    su propio .xls dentro del ZIP, y después pasaron a venir como hojas del
    mismo estado de cuenta. Hay que contemplar los dos, o los períodos nuevos
    parecen no traer reintegros ni reclamos cuando en realidad sí los traen.
    """
    zs = sorted(glob.glob(os.path.join(carpeta_zips, "*.zip")))
    faltan = {h: [] for h in TIPOS.values()}
    for z in zs:
        sem = os.path.basename(z)[:8]
        tiene = set()
        with zipfile.ZipFile(z) as f:
            for n in f.namelist():
                if not n.lower().endswith(".xls"):
                    continue
                d = _destino(os.path.basename(n))
                if d:
                    tiene.add(d)
                if "estado-de-cuenta" in n.lower():
                    ruta = os.path.join(tmp, os.path.basename(n))
                    if os.path.exists(ruta):
                        try:
                            for h in pd.ExcelFile(ruta, engine="openpyxl").sheet_names:
                                d = _destino(h)
                                if d:
                                    tiene.add(d)
                        except Exception:
                            pass
        for h in faltan:
            if h not in tiene:
                faltan[h].append(sem)
    # Cancelaciones puede faltar legítimamente: hay semanas sin ninguna
    for h in ("Reintegros", "Reclamos"):
        if faltan[h]:
            print(f"\n  AVISO: {len(faltan[h])} de {len(zs)} ZIPs no traen '{h}'")
            print(f"    semanas: {', '.join(faltan[h][:6])}" + (" ..." if len(faltan[h]) > 6 else ""))
            print("    -> volver a descargar esos estados de cuenta, o aportar")
            print("       un archivos_peya.xlsx ya armado. Si no, los reintegros")
            print("       quedan subestimados y el cierre no cierra.")


def armar_archivos_peya(tmp: str, salida: str) -> dict:
    """Junta cada concepto venga como .xls propio o como hoja del estado de cuenta."""
    partes = {h: [] for h in TIPOS.values()}
    origen = {h: 0 for h in TIPOS.values()}
    for f in sorted(glob.glob(os.path.join(tmp, "*.xls"))):
        base = os.path.basename(f)
        try:
            xl = pd.ExcelFile(f, engine="openpyxl")
        except Exception as e:
            print(f"  aviso: no se pudo leer {base} ({e})")
            continue
        d_arch = _destino(base)
        if d_arch is None:
            continue
        for hoja in xl.sheet_names:
            # Un .xls especifico (reintegros, reclamos, cancelaciones) va entero
            # a su destino. Solo en el estado de cuenta se mira hoja por hoja,
            # porque ahi conviven los cuatro conceptos.
            dest = (_destino(hoja) or d_arch) if d_arch == "Lista Pedidos" else d_arch
            try:
                partes[dest].append(xl.parse(hoja).rename(columns=RENOMBRAR))
                origen[dest] += 1
            except Exception as e:
                print(f"  aviso: {base}[{hoja}] ({e})")

    hojas = {}
    for h, ds in partes.items():
        hojas[h] = depurar(pd.concat(ds, ignore_index=True)) if ds else pd.DataFrame()
        print(f"  {h:<16}{len(hojas[h]):>7} filas  ({origen[h]} orígenes)")

    ruta = os.path.join(salida, "archivos_peya.xlsx")
    with pd.ExcelWriter(ruta, engine="openpyxl") as w:
        for hoja, df in hojas.items():
            df.to_excel(w, sheet_name=hoja, index=False)
    print(f"  -> {ruta}")
    return hojas


def extraer_liquidaciones(tmp: str, salida: str) -> list:
    out = []
    for f in sorted(glob.glob(os.path.join(tmp, "*estado-de-cuenta*.pdf"))):
        txt = subprocess.run(["pdftotext", "-layout", f, "-"],
                             capture_output=True, text=True).stdout
        d = {k: 0.0 for k in PAT}
        for linea in txt.split("\n"):
            s = re.sub(r"\s+", " ", linea).strip()
            for k, p in PAT.items():
                if d[k] == 0.0:
                    m = re.match(p, s)
                    if m:
                        # los ajustes conservan signo; el resto entra en positivo
                        d[k] = _ars(m.group(1)) if k in ("ajustes", "cargos_adic", "aminoracion") \
                            else abs(_ars(m.group(1)))
        m = re.search(r"Total liquidado del (\d{2}/\d{2}/\d{4}) al (\d{2}/\d{2}/\d{4})"
                      r"\s+ARS\s+(-?[\d\.]+,\d{2})", re.sub(r"\s+", " ", txt))
        if not m:
            print(f"  aviso: sin 'Total liquidado' en {os.path.basename(f)}")
            continue
        d["desde"], d["hasta"], d["total"] = m.group(1), m.group(2), _ars(m.group(3))
        b = os.path.basename(f)
        d["periodo"] = f"{b[6:8]}/{b[4:6]}-{b[15:17]}/{b[13:15]}"
        out.append(d)
    out.sort(key=lambda x: (x["desde"][6:], x["desde"][3:5], x["desde"][:2]))
    json.dump(out, open(os.path.join(salida, "liquidaciones.json"), "w"), indent=1)
    print(f"  {len(out)} liquidaciones")
    return out


def extraer_facturas(carpeta: str, salida: str) -> dict:
    out = {}
    for f in sorted(glob.glob(os.path.join(carpeta, "*.pdf"))):
        t = re.sub(r"\s+", " ", subprocess.run(["pdftotext", "-layout", f, "-"],
                                               capture_output=True, text=True).stdout)
        per = re.search(r"Periodo Facturado Desde:\s*(\d{2}/\d{2}/\d{4})\s*Hasta:\s*(\d{2}/\d{2}/\d{4})", t)
        tot = re.search(r"Importe Total:\s*([\d]+\.\d{2})", t)
        nro = re.search(r"Punto de Venta:\s*(\d{4})\s*Comp\.Nro:\s*(\d+)", t)
        if not (per and tot):
            print(f"  aviso: no se pudo parsear {os.path.basename(f)}")
            continue
        d, h = per.group(1), per.group(2)
        key = f"{d[:2]}/{d[3:5]}-{h[:2]}/{h[3:5]}"
        ident = f"{nro.group(1)}-{nro.group(2)}" if nro else os.path.basename(f)
        out.setdefault(key, {})[ident] = float(tot.group(1))
    json.dump(out, open(os.path.join(salida, "facturas.json"), "w"), indent=1)
    n = sum(len(v) for v in out.values())
    print(f"  {n} facturas en {len(out)} periodos")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--zips", required=True)
    ap.add_argument("--facturas", required=True)
    ap.add_argument("--salida", default="./trabajo")
    a = ap.parse_args()
    os.makedirs(a.salida, exist_ok=True)
    tmp = os.path.join(a.salida, "_extraido")

    print("Extrayendo ZIPs...")
    extraer_zips(a.zips, tmp)
    control_cobertura(a.zips, tmp)
    print("\nArmando archivos_peya.xlsx...")
    armar_archivos_peya(tmp, a.salida)
    print("\nLeyendo liquidaciones...")
    extraer_liquidaciones(tmp, a.salida)
    print("\nLeyendo facturas...")
    extraer_facturas(a.facturas, a.salida)
    print(f"\nListo. Todo en {a.salida}")


if __name__ == "__main__":
    main()
