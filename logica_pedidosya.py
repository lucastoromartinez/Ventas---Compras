"""
Conciliación de la cuenta recaudación PedidosYa (EASA y Ronda).

Capa de servicio sobre el motor de conciliacion_peya/: arma en una carpeta
temporal la convención de insumos que espera scripts/concilia.py (zips/,
facturas/, mayor, reportes internos, anterior/), corre el pipeline completo y
devuelve el Excel generado, los controles y los papeles de trabajo.

El motor no se reimplementa acá: las reglas contables viven en
conciliacion_peya/references/criterios.md y en los scripts. Este módulo solo
traslada los archivos subidos y lee lo que el motor deja en salida/.
"""
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile

import yaml

RAIZ_MOTOR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "conciliacion_peya")
SCRIPT_CONCILIA = os.path.join(RAIZ_MOTOR, "scripts", "concilia.py")
CONFIG_ENTIDADES = os.path.join(RAIZ_MOTOR, "config", "entidades.yaml")

ENTIDADES = ("easa", "ronda")

# Archivos que exige cada entidad además de ZIPs, facturas y mayor.
REQUERIDOS_POR_ENTIDAD = {
    "easa": ("dean", "hio_documento", "hio_metodo"),
    "ronda": ("hio_documento", "hio_metodo", "atalaya"),
}

TOLERANCIA_CIERRE = 1.0


def _nombre_seguro(nombre, por_defecto):
    """Basename del archivo subido, sin rutas ni caracteres problemáticos.
    Se respeta el nombre original: armar_peya.py toma la semana de los
    primeros 8 caracteres del ZIP y concilia.py busca los reportes por nombre."""
    base = os.path.basename(str(nombre or "")).strip()
    base = re.sub(r"[^\w.\-() ]", "_", base)
    return base or por_defecto


def _leer_bytes(archivo):
    if hasattr(archivo, "getvalue"):
        return archivo.getvalue()
    if hasattr(archivo, "read"):
        if hasattr(archivo, "seek"):
            archivo.seek(0)
        return archivo.read()
    with open(archivo, "rb") as f:
        return f.read()


def _guardar(archivo, carpeta, nombre_fijo=None, por_defecto="archivo"):
    nombre = nombre_fijo or _nombre_seguro(getattr(archivo, "name", None), por_defecto)
    ruta = os.path.join(carpeta, nombre)
    with open(ruta, "wb") as f:
        f.write(_leer_bytes(archivo))
    return ruta


def _local_peya(entidad):
    with open(CONFIG_ENTIDADES, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return str(cfg[entidad]["local_peya"]), cfg[entidad]["nombre"]


def validar_insumos(entidad, archivos_zip, archivos_facturas):
    """Validaciones previas recomendadas por la especificación del módulo.
    Devuelve una lista de advertencias (no bloquean la corrida)."""
    advertencias = []
    local, nombre = _local_peya(entidad)

    nombres_zip = [os.path.basename(getattr(z, "name", "")) for z in archivos_zip]
    ajenos = [n for n in nombres_zip if local not in n]
    if ajenos:
        advertencias.append(
            f"{len(ajenos)} ZIP no traen el local {local} de {nombre} en el nombre: "
            + ", ".join(ajenos[:5]) + ("…" if len(ajenos) > 5 else "")
        )
    repetidos = sorted({n for n in nombres_zip if nombres_zip.count(n) > 1})
    if repetidos:
        advertencias.append("ZIP repetidos: " + ", ".join(repetidos))

    if len(archivos_facturas) < len(archivos_zip):
        advertencias.append(
            f"Hay {len(archivos_zip)} estados de cuenta y {len(archivos_facturas)} facturas: "
            "se espera una factura por semana (dos si hay PAGOS YA). Las faltantes se "
            "estiman por la ecuación de la liquidación y quedan como 'No aportada'."
        )
    return advertencias


def _zip_carpeta(carpeta):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for raiz, _, archivos in os.walk(carpeta):
            for a in sorted(archivos):
                ruta = os.path.join(raiz, a)
                z.write(ruta, os.path.relpath(ruta, carpeta))
    buf.seek(0)
    return buf.getvalue()


def _resumen_conceptos(conceptos):
    """Líneas en naranja (deducidas o arrastradas) y pendiente de definición."""
    naranja = []
    for ln in conceptos.get("lineas", []):
        if ln.get("flag") and "concepto" in ln:
            importe = (ln.get("debe") or 0) - (ln.get("haber") or 0)
            naranja.append({"concepto": ln["concepto"], "importe": round(importe, 2)})
    abierto = []
    for ln in conceptos.get("abierto", []) or []:
        importe = (ln.get("debe") or 0) - (ln.get("haber") or 0)
        abierto.append({"concepto": ln.get("concepto", ""), "importe": round(importe, 2),
                        "nota": ln.get("nota", "")})
    return naranja, abierto


def correr_conciliacion_peya(entidad, archivos_zip, archivos_facturas, archivo_mayor,
                             archivo_dean=None, archivo_hio_documento=None,
                             archivo_hio_metodo=None, archivo_atalaya=None,
                             archivo_anterior=None):
    """Corre la conciliación PeYa de punta a punta.

    Sin archivo_anterior es un cierre inicial; con él, un cierre mensual que
    arrastra los pendientes y el histórico de la conciliación anterior (el
    Excel generado por este mismo módulo el mes previo).

    Devuelve (excel_bytes, nombre_excel, trabajo_zip_bytes, stats).
    """
    entidad = str(entidad).lower()
    if entidad not in ENTIDADES:
        raise ValueError(f"Entidad desconocida: {entidad}. Opciones: {', '.join(ENTIDADES)}")
    if not archivos_zip:
        raise ValueError("Faltan los ZIP de estado de cuenta de PedidosYa.")
    if not archivos_facturas:
        raise ValueError("Faltan las facturas PDF de PedidosYa.")
    if archivo_mayor is None:
        raise ValueError("Falta el mayor de la cuenta recaudación PedidosYa.")

    sistemas = {"dean": archivo_dean, "hio_documento": archivo_hio_documento,
                "hio_metodo": archivo_hio_metodo, "atalaya": archivo_atalaya}
    faltan = [k for k in REQUERIDOS_POR_ENTIDAD[entidad] if sistemas[k] is None]
    if faltan:
        raise ValueError("Faltan reportes internos: " + ", ".join(faltan))
    if shutil.which("pdftotext") is None:
        raise RuntimeError(
            "No se encontró 'pdftotext' (poppler-utils) en el servidor: es necesario para "
            "leer los PDF de liquidaciones y facturas."
        )

    advertencias = validar_insumos(entidad, archivos_zip, archivos_facturas)

    with tempfile.TemporaryDirectory(prefix="peya_") as tmp:
        carpeta = os.path.join(tmp, "insumos")
        d_zips = os.path.join(carpeta, "zips")
        d_fact = os.path.join(carpeta, "facturas")
        salida = os.path.join(carpeta, "salida")
        os.makedirs(d_zips)
        os.makedirs(d_fact)

        for i, z in enumerate(archivos_zip):
            _guardar(z, d_zips, por_defecto=f"estado_{i:03d}.zip")
        for i, f in enumerate(archivos_facturas):
            _guardar(f, d_fact, por_defecto=f"factura_{i:03d}.pdf")

        # Nombres fijos para los archivos sueltos: se pasan explícitos al motor,
        # así no depende de cómo vengan nombrados.
        args = [
            "--entidad", entidad,
            "--zips", d_zips,
            "--facturas", d_fact,
            "--mayor", _guardar(archivo_mayor, carpeta, "recaudacion.xlsx"),
            "--salida", salida,
        ]
        for clave, flag, nombre in (("dean", "--dean", "dean.xlsx"),
                                    ("hio_documento", "--hio-documento", "hio_documento.xlsx"),
                                    ("hio_metodo", "--hio-metodo", "hio_metodo.xlsx"),
                                    ("atalaya", "--atalaya", "atalaya.xlsx")):
            if sistemas[clave] is not None:
                args += [flag, _guardar(sistemas[clave], carpeta, nombre)]
        if archivo_anterior is not None:
            d_ant = os.path.join(carpeta, "anterior")
            os.makedirs(d_ant)
            args += ["--anterior", _guardar(archivo_anterior, d_ant, "conciliacion_anterior.xlsx")]

        # PYTHONUNBUFFERED: que la salida de cada etapa quede en orden en el log.
        proc = subprocess.run(
            [sys.executable, "-I", SCRIPT_CONCILIA] + args,
            cwd=tmp, capture_output=True, text=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        log = (proc.stdout or "") + (("\n" + proc.stderr) if proc.stderr else "")
        if proc.returncode != 0:
            ruta_liq = os.path.join(salida, "trabajo", "liquidaciones.json")
            if os.path.exists(ruta_liq):
                with open(ruta_liq, encoding="utf-8") as f:
                    if not json.load(f):
                        raise RuntimeError(
                            "No se pudo leer ninguna liquidación de los ZIP: cada estado de cuenta "
                            "debe traer el PDF de liquidación con el 'Total liquidado del … al …'."
                        )
            cola = "\n".join(log.strip().splitlines()[-25:])
            raise RuntimeError(f"El motor de conciliación terminó con error.\n\n{cola}")

        excels = sorted(f for f in os.listdir(salida)
                        if f.startswith("Conciliacion_PeYa_") and f.endswith(".xlsx"))
        if not excels:
            raise RuntimeError("El motor terminó sin generar el Excel de conciliación.")
        nombre_excel = excels[-1]
        with open(os.path.join(salida, nombre_excel), "rb") as f:
            excel_bytes = f.read()

        trabajo = os.path.join(salida, "trabajo")
        with open(os.path.join(trabajo, "_controles.json"), encoding="utf-8") as f:
            controles = json.load(f)
        with open(os.path.join(trabajo, "conceptos.json"), encoding="utf-8") as f:
            conceptos = json.load(f)
        with open(os.path.join(trabajo, "log.txt"), "w", encoding="utf-8") as f:
            f.write(log)
        trabajo_zip = _zip_carpeta(trabajo)

    m = re.search(r"_(\d{4}-\d{2})\.xlsx$", nombre_excel)
    periodo = m.group(1) if m else ""
    naranja, abierto = _resumen_conceptos(conceptos)

    saldo_final = controles.get("saldo_final")
    verif = controles.get("verificacion") or {}
    verificacion_ok = (
        verif.get("residuo_independiente") is not None and verif.get("cruces") is not None
        and abs(verif["residuo_independiente"] - verif["cruces"]) <= TOLERANCIA_CIERRE
    )
    cierre_ok = saldo_final is not None and abs(saldo_final) <= TOLERANCIA_CIERRE

    stats = {
        "entidad": entidad,
        "periodo": periodo,
        "modo": "mensual" if archivo_anterior is not None else "inicial",
        "titulo": conceptos.get("titulo", ""),
        "subtitulo": conceptos.get("subtitulo", ""),
        "nota_final": conceptos.get("nota_final", ""),
        "controles": controles,
        "cierre_ok": cierre_ok,
        "verificacion_ok": verificacion_ok,
        "valido": cierre_ok and verificacion_ok,
        "lineas_naranja": naranja,
        "pendiente_definicion": abierto,
        # Solo para mostrar en pantalla: la misma cadena que va al Excel.
        "cadena": conceptos.get("lineas", []),
        "advertencias": advertencias,
        "log": log,
    }
    return excel_bytes, nombre_excel, trabajo_zip, stats
