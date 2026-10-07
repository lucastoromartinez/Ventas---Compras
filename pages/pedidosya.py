import pandas as pd
import streamlit as st

from estilos import aplicar_estilos, encabezado
from logica_pedidosya import correr_conciliacion_peya

aplicar_estilos()
encabezado(
    "Conciliación cuenta recaudación Pedidos Ya",
    "Parte del saldo del mayor y lo lleva a cero, explicando cada importe con un concepto "
    "trazable a su registro original.",
    "Conciliaciones / Pedidos Ya",
)

ENTIDADES = {
    "EASA": {
        "id": "easa", "local": "411335", "sistemas": "Dean y HIOffice",
        "reportes": [
            ("dean", "Ventas Dean", "Excel · filtra el servicio PedidosYa"),
            ("hio_documento", "HIO por documento", "Excel · trae el localizador"),
            ("hio_metodo", "HIO por método de pago", "Excel · referencia de ventas"),
        ],
    },
    "Ronda": {
        "id": "ronda", "local": "409994", "sistemas": "HIOffice y Atalaya",
        "reportes": [
            ("hio_documento", "HIO por documento", "Excel · trae el localizador"),
            ("hio_metodo", "HIO por método de pago", "Excel · referencia de ventas"),
            ("atalaya", "Tickets Atalaya", "Excel · fecha, medio de pago, CON IVA"),
        ],
    },
}


def _pesos(valor):
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return "—"
    return f"$ {valor:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")


# ───────────────────────── Selección ─────────────────────────
col_ent, col_tipo, col_info = st.columns([1.1, 1.3, 3], vertical_alignment="bottom")
with col_ent:
    nombre_ent = st.segmented_control(
        "Entidad", list(ENTIDADES), default="EASA", key="peya_entidad_sel",
    ) or "EASA"
with col_tipo:
    tipo = st.segmented_control(
        "Tipo de cierre", ["Inicial", "Mensual"], default="Mensual", key="peya_tipo_sel",
        help="Inicial: sin conciliación anterior, procesa todo el rango de ZIP. "
             "Mensual: arrastra pendientes e histórico de la conciliación del mes anterior.",
    ) or "Mensual"
ent = ENTIDADES[nombre_ent]
with col_info:
    st.caption(f"Local PeYa {ent['local']} · Sistemas: {ent['sistemas']}")

pre = f"peya_{ent['id']}"
mensual = tipo == "Mensual"


# ───────────────────────── Insumos ─────────────────────────
def _insumo(clave, titulo, detalle, tipos, multiple=False):
    with st.container(border=True):
        st.markdown(f"**{titulo}**")
        st.caption(detalle)
        valor = st.file_uploader(
            titulo, type=tipos, accept_multiple_files=multiple,
            label_visibility="collapsed", key=f"{pre}_{clave}",
        )
        if valor:
            texto = f"{len(valor)} archivo{'s' if len(valor) != 1 else ''}" if multiple else "Cargado"
            st.badge(texto, icon=":material/check:", color="green")
        else:
            st.badge("Pendiente", icon=":material/upload:", color="gray")
    return valor


insumos = [
    ("zips", "Estados de cuenta PeYa", "ZIP semanales, sin huecos", ["zip"], True, "los ZIP de estado de cuenta"),
    ("facturas", "Facturas PEDIDOSYA / PAGOS YA", "PDF · pto. vta. 0026 y 0013", ["pdf"], True, "las facturas PDF"),
    ("mayor", "Mayor cuenta recaudación", "Excel · completo desde el origen", ["xlsx"], False, "el mayor de recaudación"),
] + [
    (clave, titulo, detalle, ["xlsx"], False, f"el reporte {titulo}") for clave, titulo, detalle in ent["reportes"]
]
if mensual:
    insumos.append(("anterior", "Conciliación del mes anterior", "Excel generado por este módulo",
                    ["xlsx"], False, "la conciliación del mes anterior"))

st.markdown('<div class="section-title">Insumos</div>', unsafe_allow_html=True)
archivos, faltantes = {}, []
for inicio in range(0, len(insumos), 2):
    cols = st.columns(2, gap="medium")
    for col, (clave, titulo, detalle, tipos, multiple, falta) in zip(cols, insumos[inicio:inicio + 2]):
        with col:
            archivos[clave] = _insumo(clave, titulo, detalle, tipos, multiple)
        if not archivos[clave]:
            faltantes.append(falta)

if faltantes:
    st.info("Falta cargar: " + ", ".join(faltantes) + ".", icon=":material/info:")

_, col_boton = st.columns([3, 1])
with col_boton:
    boton = st.button(
        f"Conciliar {nombre_ent}", type="primary", icon=":material/play_arrow:",
        disabled=bool(faltantes), use_container_width=True, key=f"btn_{pre}",
    )

clave_estado = f"resultado_{pre}"
if boton and not faltantes:
    st.session_state.pop(clave_estado, None)
    with st.status(f"Conciliando Pedidos Ya {nombre_ent}…", expanded=False) as estado:
        try:
            st.write("Leyendo estados de cuenta y facturas, cruzando contra sistemas y armando la cadena.")
            excel, nombre_excel, trabajo, stats = correr_conciliacion_peya(
                ent["id"], archivos["zips"], archivos["facturas"], archivos["mayor"],
                archivo_dean=archivos.get("dean"),
                archivo_hio_documento=archivos.get("hio_documento"),
                archivo_hio_metodo=archivos.get("hio_metodo"),
                archivo_atalaya=archivos.get("atalaya"),
                archivo_anterior=archivos.get("anterior") if mensual else None,
            )
            st.session_state[clave_estado] = {
                "excel": excel, "nombre": nombre_excel, "trabajo": trabajo, "stats": stats,
            }
            estado.update(label="Conciliación generada", state="complete")
        except Exception as e:
            estado.update(label="La conciliación no pudo completarse", state="error", expanded=True)
            st.error(f"Error al procesar: {e}")


# ───────────────────────── Resultado ─────────────────────────
def _estilo_estado(valor):
    if valor in ("Cierran", "Coincide", "Sin diferencias"):
        return "color: #1E6B3E; font-weight: 600"
    return "color: #8A4307; font-weight: 600"


IMPORTE = st.column_config.TextColumn(alignment="right")


def _tabla_controles(c):
    v = c.get("verificacion") or {}
    ac = c.get("acreditaciones") or {}
    re_ = c.get("reintegros") or {}
    fa = c.get("facturas") or {}
    n_pend = len(ac.get("pendientes") or [])
    n_fac = len(fa.get("pendientes") or [])
    filas = [
        ("Ecuación de cada liquidación", f"{c.get('liquidaciones', '—')} liquidaciones", "—",
         "Cierran" if not c.get("liq_no_cierran") else f"{len(c['liq_no_cierran'])} no cierran"),
        ("Verificación independiente", _pesos(v.get("peya")), _pesos(v.get("ventas_internas")),
         "Coincide" if v.get("residuo_independiente") is not None and v.get("cruces") is not None
         and abs(v["residuo_independiente"] - v["cruces"]) <= 1 else "Difiere"),
        (f"Reintegros ({re_.get('casos', '—')} casos)", _pesos(re_.get("liquidaciones")), _pesos(re_.get("lineas")),
         "Coincide" if re_.get("lineas") is not None and re_.get("liquidaciones") is not None
         and abs(re_["lineas"] - re_["liquidaciones"]) <= 1 else "Difiere"),
        ("Acreditaciones", _pesos(ac.get("liq")), _pesos(ac.get("mayor")),
         f"{n_pend} pendiente{'s' if n_pend != 1 else ''}" if n_pend else "Coincide"),
        ("Facturas contra el mayor", f"{n_fac} pendiente{'s' if n_fac != 1 else ''}",
         f"{len(fa.get('con_diferencia') or [])} con diferencia",
         "Sin diferencias" if not (fa.get("con_diferencia") or fa.get("huerfanas")) else "Revisar"),
        ("Ventas internas contra mayor", "—", "—",
         "Sin diferencias" if not c.get("ventas_vs_mayor") else f"{len(c['ventas_vs_mayor'])} diferencias"),
    ]
    df = pd.DataFrame(filas, columns=["Control", "Liquidaciones / PeYa", "Mayor / sistemas", "Estado"])
    st.dataframe(
        df.style.map(_estilo_estado, subset=["Estado"]), hide_index=True, use_container_width=True,
        column_config={"Liquidaciones / PeYa": IMPORTE, "Mayor / sistemas": IMPORTE},
    )


def _tabla_cadena(lineas, saldo_inicial):
    """Misma cadena que la hoja 'conciliacion' del Excel, con el saldo acumulado."""
    filas, tipos = [("Saldo del mayor", "", "", _pesos(saldo_inicial))], ["total"]
    saldo = saldo_inicial or 0.0
    for ln in lineas:
        if "subtotal" in ln:
            filas.append((ln["subtotal"], "", "", _pesos(saldo)))
            tipos.append("total")
            continue
        debe, haber = ln.get("debe") or 0.0, ln.get("haber") or 0.0
        saldo += debe - haber
        filas.append((ln.get("concepto", ""), _pesos(debe) if debe else "", _pesos(haber) if haber else "",
                      _pesos(saldo)))
        tipos.append("flag" if ln.get("flag") else "")
    filas.append(("SALDO FINAL", "", "", _pesos(saldo)))
    tipos.append("total")
    df = pd.DataFrame(filas, columns=["Concepto", "Debe", "Haber", "Saldo"])
    colores = {"total": "font-weight: 600; background-color: #F1F3F6", "flag": "background-color: #FFF7ED"}
    estilo = df.style.apply(lambda f: [colores.get(tipos[f.name], "")] * len(f), axis=1)
    st.dataframe(
        estilo, hide_index=True, use_container_width=True, height=min(520, 35 * (len(df) + 1) + 3),
        column_config={"Debe": IMPORTE, "Haber": IMPORTE, "Saldo": IMPORTE},
    )


if clave_estado in st.session_state:
    r = st.session_state[clave_estado]
    s = r["stats"]
    c = s["controles"]
    st.divider()

    col_tit, col_desc = st.columns([3, 2], vertical_alignment="bottom")
    with col_tit:
        st.subheader(f"{nombre_ent} · {s['periodo']}")
        st.caption(f"Cierre {s['modo']} · {s['subtitulo']}")
    with col_desc:
        d1, d2 = st.columns(2)
        d1.download_button(
            "Papeles de trabajo", data=r["trabajo"],
            file_name=r["nombre"].replace(".xlsx", "_trabajo.zip"), mime="application/zip",
            icon=":material/folder_zip:", use_container_width=True, key=f"dl_trabajo_{pre}",
        )
        d2.download_button(
            "Conciliación", data=r["excel"], file_name=r["nombre"], type="primary",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            icon=":material/download:", use_container_width=True, key=f"dl_{pre}",
        )

    if s["valido"]:
        st.success("**Cierre válido.** El saldo final está dentro de la tolerancia de 1 peso y la "
                   "verificación independiente coincide con los cruces.", icon=":material/check_circle:")
    else:
        st.warning("**El cierre no cumple las condiciones de validez** (saldo final de ±1 peso y "
                   "verificación independiente coincidente). Revisar los controles.", icon=":material/error:")
    st.caption("Guardar el Excel de conciliación sin modificar: es el insumo del cierre del mes siguiente.")

    v = c.get("verificacion") or {}
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Saldo del mayor", _pesos(c.get("saldo_mayor")), border=True)
    m2.metric("Saldo final", _pesos(c.get("saldo_final")), border=True)
    m3.metric("Residuo explicado por cruces", _pesos(v.get("cruces")), border=True)
    m4.metric("Liquidaciones procesadas", c.get("liquidaciones", "—"), border=True)

    revisar = (
        [{"Concepto": x["concepto"], "Importe": x["importe"], "Tipo": "Deducida o arrastrada"}
         for x in s["lineas_naranja"]]
        + [{"Concepto": x["concepto"], "Importe": x["importe"], "Tipo": "Pendiente de definición"}
           for x in s["pendiente_definicion"]]
    )
    t_ctl, t_rev, t_cad, t_log = st.tabs([
        ":material/fact_check: Controles",
        f":material/flag: A revisar ({len(revisar)})",
        ":material/format_list_numbered: Cadena de conceptos",
        ":material/terminal: Registro del proceso",
    ])
    with t_ctl:
        _tabla_controles(c)
        for adv in s["advertencias"]:
            st.warning(adv, icon=":material/warning:")
    with t_rev:
        if revisar:
            st.caption("Líneas que requieren una decisión. El cálculo no cambia con esa decisión; "
                       "lo pendiente de definición queda fuera del saldo.")
            df_rev = pd.DataFrame(revisar)
            df_rev["Importe"] = df_rev["Importe"].map(_pesos)
            st.dataframe(df_rev, hide_index=True, use_container_width=True,
                         column_config={"Importe": IMPORTE})
        else:
            st.caption("No hay líneas deducidas, arrastradas ni pendientes de definición.")
    with t_cad:
        _tabla_cadena(s.get("cadena") or [], c.get("saldo_mayor"))
        if s["nota_final"]:
            st.caption(s["nota_final"])
    with t_log:
        st.code(s["log"] or "(sin salida)", language="text")
