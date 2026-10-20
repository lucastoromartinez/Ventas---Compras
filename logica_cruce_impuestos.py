import re

import pandas as pd
from rapidfuzz import fuzz

from proveedores import mapa_nombre_a_cuit


# ─────────────────────────────────────────────
# NORMALIZACIÓN
# ─────────────────────────────────────────────

_FORMAS_SOCIETARIAS = [
    (r"\bSOCIEDAD ANONIMA UNIPERSONAL\b", "SAU"),
    (r"\bSOCIEDAD ANONIMA\b", "SA"),
    (r"\bSOCIEDAD DE RESPONSABILIDAD LIMITADA\b", "SRL"),
    (r"\bSOCIEDAD POR ACCIONES SIMPLIFICADA\b", "SAS"),
    (r"\bS A U\b", "SAU"),
    (r"\bS A S\b", "SAS"),
    (r"\bS R L\b", "SRL"),
    (r"\bS A\b", "SA"),
]


def _nombre_comparable(nombre) -> str:
    """
    Unifica la forma societaria para la comparación fuzzy: 'STYLUS SOCIEDAD
    ANONIMA', 'STYLUS S.A.' y 'STYLUS SA' quedan todos como 'STYLUS SA'.
    """
    s = str(nombre).upper()
    s = re.sub(r"[.,\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for patron, reemplazo in _FORMAS_SOCIETARIAS:
        s = re.sub(patron, reemplazo, s)
    return s


def _texto(valor) -> str:
    """Nombre de proveedor como texto; vacío si no viene."""
    if valor is None or (not isinstance(valor, str) and pd.isna(valor)):
        return ""
    s = str(valor).strip()
    return "" if s.upper() in {"", "NAN", "NONE"} else s


def normalizar_cuit(serie: pd.Series) -> pd.Series:
    """Deja solo los dígitos del CUIT; vacío si no viene."""
    return serie.apply(lambda x: re.sub(r"\D", "", _texto(x)))


def cuit_por_tercero(terceros: pd.Series, padron: dict) -> pd.Series:
    """
    Busca el CUIT de cada 'Tercero' en el padrón de proveedores: primero por
    nombre exacto (sin importar mayúsculas) y, si no aparece, comparando con
    la forma societaria unificada ('BANCO X SAU' contra 'Banco X S A U').
    Ante nombres repetidos queda el primer CUIT encontrado.
    """
    nombre_a_cuit = mapa_nombre_a_cuit(padron)
    comparable_a_cuit = {}
    for nombre, cuit in nombre_a_cuit.items():
        comparable_a_cuit.setdefault(_nombre_comparable(nombre), cuit)

    def _buscar(tercero):
        t = _texto(tercero)
        if not t:
            return None
        return nombre_a_cuit.get(t.upper()) or comparable_a_cuit.get(_nombre_comparable(t))

    return terceros.apply(_buscar)


# ─────────────────────────────────────────────
# CRUCE (mismo criterio para todos los módulos de impuestos)
# ─────────────────────────────────────────────

def cruzar_impuestos(
    df_arca: pd.DataFrame,
    df_sistema: pd.DataFrame,
    *,
    col_cuit_arca: str,
    col_importe_arca: str,
    col_importe_sistema: str,
    col_cuit_sistema: str | None = None,
    col_nombre_arca: str | None = None,
    col_nombre_sistema: str | None = None,
    padron: dict | None = None,
    tolerancia_importe: float = 1.0,
    score_nombre_min: int = 70,
    cols_comprobante: tuple[str, str, str, str] | None = None,
    cols_fecha: tuple[str, str] | None = None,
    solo_agrupado: set | None = None,
    revisar_comprobante: bool = False,
):
    """
    Cruza las líneas del organismo (df_arca) contra el sistema. Es el mismo
    criterio para todos los módulos de impuestos:

    Uno a uno (mismo importe, con tolerancia_importe):
      PASO 1 - CUIT + Importe.
      PASO 2 - CUIT vía padrón + Importe: el nombre que el padrón tiene para
               el CUIT del organismo coincide con 'Tercero' del sistema.
      PASO 3 - Nombre + Importe: score >= score_nombre_min. Las líneas sin
               nombre no participan.
      PASO 4 - (solo si revisar_comprobante) CUIT + mismo comprobante con
               importe distinto: no es match, va a revisar.

    Agrupado (todas las líneas del proveedor que siguen faltando):
      PASO 5 - Varias líneas del organismo contra una del sistema.
      PASO 6 - Varias líneas del sistema contra una del organismo.
      En los dos, el proveedor se identifica por CUIT, después por CUIT vía
      padrón y por último por nombre (score >= score_nombre_min, solo si la
      línea trae nombre). Se suma todo lo que sigue faltando de ese
      proveedor y se compara contra el importe de la otra línea.
      PASO 7 - Todas las líneas del sistema contra todas las del organismo
               del mismo proveedor (varias contra varias), con la misma
               identificación del proveedor.
      PASO 8 - Línea del sistema sin proveedor (sin CUIT ni nombre) contra
               el total de cada proveedor del organismo: matchea solo si
               cierra un único proveedor.

    Si hay más de un candidato uno a uno, desempata por el mismo comprobante
    (cols_comprobante = pto venta / número del organismo y del sistema),
    después por la fecha más cercana (cols_fecha = organismo, sistema) y
    por último por el comprobante más parecido.

    Las líneas del sistema en solo_agrupado no participan del uno a uno.

    Retorna:
        (matches, revisar)
        - matches: lista de ([idx organismo], [idx sistema], tipo, score)
        - revisar: lista de (idx organismo, idx sistema, comentario)
    """
    if padron is None:
        padron = {}
    solo_agrupado = set(solo_agrupado or ())

    tol = tolerancia_importe
    imp_a = pd.to_numeric(df_arca[col_importe_arca], errors="coerce").fillna(0).round(2)
    imp_s = pd.to_numeric(df_sistema[col_importe_sistema], errors="coerce").fillna(0).round(2)

    cuit_a = normalizar_cuit(df_arca[col_cuit_arca])
    cuit_s = (
        normalizar_cuit(df_sistema[col_cuit_sistema]) if col_cuit_sistema
        else pd.Series("", index=df_sistema.index)
    )

    def _comparables(df, col):
        if col is None:
            return pd.Series("", index=df.index)
        return df[col].apply(lambda x: _nombre_comparable(_texto(x)) if _texto(x) else "")

    nombre_a = _comparables(df_arca, col_nombre_arca)
    nombre_s = _comparables(df_sistema, col_nombre_sistema)

    padron_comparable = {}
    for cuit, nombre in padron.items():
        padron_comparable[re.sub(r"\D", "", str(cuit))] = _nombre_comparable(nombre)

    def _cierra(total, importe):
        return abs(round(total, 2) - importe) <= tol

    # ── desempate ──
    def _norm_digitos(serie):
        def limpiar(x):
            s = re.sub(r"\D", "", _texto(x))
            return str(int(s)) if s else ""
        return serie.apply(limpiar)

    if cols_comprobante:
        pv_a = _norm_digitos(df_arca[cols_comprobante[0]])
        nc_a = _norm_digitos(df_arca[cols_comprobante[1]])
        pv_s = _norm_digitos(df_sistema[cols_comprobante[2]])
        nc_s = _norm_digitos(df_sistema[cols_comprobante[3]])

    if cols_fecha:
        fecha_a = pd.to_datetime(df_arca[cols_fecha[0]], errors="coerce")
        fecha_s = pd.to_datetime(df_sistema[cols_fecha[1]], errors="coerce")

    def _desempatar(a, candidatos):
        candidatos = sorted(candidatos)
        if len(candidatos) <= 1:
            return candidatos[0]

        if cols_comprobante:
            pa, na = pv_a.loc[a], nc_a.loc[a]
            for nivel in (
                [i for i in candidatos if na and pv_s.loc[i] == pa and nc_s.loc[i] == na],
                [i for i in candidatos if na and nc_s.loc[i] == na],
                [i for i in candidatos if pa and pv_s.loc[i] == pa],
            ):
                if nivel:
                    return nivel[0]

        if cols_fecha and pd.notna(fecha_a.loc[a]):
            def _distancia(i):
                f = fecha_s.loc[i]
                return (pd.isna(f), abs((f - fecha_a.loc[a]).days) if pd.notna(f) else 0, i)
            return min(candidatos, key=_distancia)

        if cols_comprobante:
            concat_a = f"{pa}{na}"
            return max(candidatos, key=lambda i: (fuzz.ratio(concat_a, f"{pv_s.loc[i]}{nc_s.loc[i]}"), -i))

        return candidatos[0]

    matches = []
    revisar = []
    a_libres = set(df_arca.index)
    s_libres = set(df_sistema.index)

    def _registrar(a_lista, s_lista, tipo, score):
        matches.append((list(a_lista), list(s_lista), tipo, score))
        a_libres.difference_update(a_lista)
        s_libres.difference_update(s_lista)

    def _uno_a_uno(condicion, tipo):
        for a in sorted(a_libres):
            candidatos = [
                s for s in s_libres
                if s not in solo_agrupado
                and abs(imp_s.loc[s] - imp_a.loc[a]) <= tol
                and condicion(a, s)
            ]
            if candidatos:
                _registrar([a], [_desempatar(a, candidatos)], tipo, 100)

    # PASO 1: CUIT + Importe
    _uno_a_uno(lambda a, s: cuit_a.loc[a] != "" and cuit_s.loc[s] == cuit_a.loc[a], "cuit")

    # PASO 2: CUIT vía padrón + Importe
    _uno_a_uno(
        lambda a, s: bool(padron_comparable.get(cuit_a.loc[a]))
        and nombre_s.loc[s] == padron_comparable[cuit_a.loc[a]],
        "cuit_padron",
    )

    # PASO 3: Nombre + Importe
    for a in sorted(a_libres):
        if not nombre_a.loc[a]:
            continue
        candidatos = [
            s for s in s_libres
            if s not in solo_agrupado
            and nombre_s.loc[s]
            and abs(imp_s.loc[s] - imp_a.loc[a]) <= tol
        ]
        if not candidatos:
            continue
        puntajes = {s: fuzz.token_sort_ratio(nombre_a.loc[a], nombre_s.loc[s]) for s in candidatos}
        mejor = max(puntajes.values())
        if mejor < score_nombre_min:
            continue
        empatados = [s for s, sc in puntajes.items() if sc == mejor]
        _registrar([a], [_desempatar(a, empatados)], "nombre", mejor)

    # PASO 4: CUIT + mismo comprobante, importe distinto (error de carga)
    if revisar_comprobante and cols_comprobante:
        for a in sorted(a_libres):
            if not (cuit_a.loc[a] and pv_a.loc[a] and nc_a.loc[a]):
                continue
            candidatos = sorted(
                s for s in s_libres
                if cuit_s.loc[s] == cuit_a.loc[a]
                and pv_s.loc[s] == pv_a.loc[a]
                and nc_s.loc[s] == nc_a.loc[a]
            )
            if candidatos:
                revisar.append((a, candidatos[0], "Importe"))
                a_libres.discard(a)
                s_libres.discard(candidatos[0])

    # ── agrupado ──
    def _grupo_del_proveedor(cuit, nombre, libres, cuit_libres, nombre_libres, importe_libres, importe):
        """
        Busca, entre las líneas libres del otro lado, todas las del mismo
        proveedor cuya suma cierre 'importe'. Devuelve (grupo, tipo, score).
        """
        if cuit:
            grupo = [i for i in libres if cuit_libres.loc[i] == cuit]
            if grupo and _cierra(importe_libres.loc[grupo].sum(), importe):
                return grupo, "cuit", 100

        # CUIT vía padrón: el nombre del padrón para el CUIT de un lado
        # coincide con el nombre del otro lado
        if nombre:
            por_cuit = {}
            for i in libres:
                if cuit_libres.loc[i] and padron_comparable.get(cuit_libres.loc[i]) == nombre:
                    por_cuit.setdefault(cuit_libres.loc[i], []).append(i)
            for grupo in por_cuit.values():
                if _cierra(importe_libres.loc[grupo].sum(), importe):
                    return grupo, "cuit_padron", 100
        if cuit and padron_comparable.get(cuit):
            grupo = [i for i in libres if nombre_libres.loc[i] == padron_comparable[cuit]]
            if grupo and _cierra(importe_libres.loc[grupo].sum(), importe):
                return grupo, "cuit_padron", 100

        # Nombre: el proveedor del otro lado se agrupa por CUIT o, sin CUIT,
        # por nombre; las líneas sin nombre no participan
        if nombre:
            grupos = {}
            for i in libres:
                if not nombre_libres.loc[i]:
                    continue
                clave = cuit_libres.loc[i] or nombre_libres.loc[i]
                grupos.setdefault(clave, []).append(i)

            puntuados = []
            for grupo in grupos.values():
                score = max(fuzz.token_sort_ratio(nombre, nombre_libres.loc[i]) for i in grupo)
                if score >= score_nombre_min:
                    puntuados.append((score, grupo))
            for score, grupo in sorted(puntuados, key=lambda x: -x[0]):
                if _cierra(importe_libres.loc[grupo].sum(), importe):
                    return grupo, "nombre", score

        return None, None, None

    # PASO 5: varias líneas del organismo contra una del sistema
    for s in sorted(s_libres):
        if imp_s.loc[s] == 0:
            continue
        grupo, tipo, score = _grupo_del_proveedor(
            cuit_s.loc[s], nombre_s.loc[s], sorted(a_libres), cuit_a, nombre_a, imp_a, imp_s.loc[s]
        )
        if grupo:
            _registrar(grupo, [s], f"{tipo}_sumarizado", score)

    # PASO 6: varias líneas del sistema contra una del organismo
    for a in sorted(a_libres):
        if imp_a.loc[a] == 0:
            continue
        grupo, tipo, score = _grupo_del_proveedor(
            cuit_a.loc[a], nombre_a.loc[a], sorted(s_libres), cuit_s, nombre_s, imp_s, imp_a.loc[a]
        )
        if grupo:
            _registrar([a], grupo, f"{tipo}_sumarizado_sistema", score)

    # PASO 7: todas las líneas del sistema contra todas las del organismo del
    # mismo proveedor (varias contra varias)
    grupos_sistema = {}
    for s in sorted(s_libres):
        clave = cuit_s.loc[s] or nombre_s.loc[s]
        if clave:
            grupos_sistema.setdefault(clave, []).append(s)
    for grupo_s in grupos_sistema.values():
        if len(grupo_s) < 2 or not set(grupo_s) <= s_libres:
            continue
        total_s = round(imp_s.loc[grupo_s].sum(), 2)
        if total_s == 0:
            continue
        primera = grupo_s[0]
        grupo_a, tipo, score = _grupo_del_proveedor(
            cuit_s.loc[primera], nombre_s.loc[primera], sorted(a_libres), cuit_a, nombre_a, imp_a, total_s
        )
        if grupo_a and len(grupo_a) >= 2:
            _registrar(grupo_a, grupo_s, f"{tipo}_sumarizado_total", score)

    # PASO 8: línea del sistema sin proveedor (sin CUIT ni nombre) contra el
    # total de un único proveedor
    for s in sorted(s_libres):
        if cuit_s.loc[s] or nombre_s.loc[s] or imp_s.loc[s] == 0:
            continue
        por_cuit = {}
        for a in sorted(a_libres):
            if cuit_a.loc[a]:
                por_cuit.setdefault(cuit_a.loc[a], []).append(a)
        que_cierran = [g for g in por_cuit.values() if _cierra(imp_a.loc[g].sum(), imp_s.loc[s])]
        if len(que_cierran) == 1:
            _registrar(que_cierran[0], [s], "importe_proveedor_sumarizado", 100)

    return matches, revisar


def armar_resultados(
    df_arca: pd.DataFrame,
    df_sistema: pd.DataFrame,
    matches: list,
    excluidos_arca: set | None = None,
    excluidos_sistema: set | None = None,
):
    """
    Arma las hojas del cruce a partir de los matches de cruzar_impuestos.

    Retorna:
        (df_match_sistema, df_match_arca, df_falta_sistema, df_falta_arca)
        - df_falta_sistema: líneas del organismo sin match
        - df_falta_arca: líneas del sistema sin match
    """
    cols_extra = ["id_match", "match_tipo", "match_score"]
    filas_sistema, filas_arca = [], []
    for id_match, (a_lista, s_lista, tipo, score) in enumerate(matches, start=1):
        for df, lista, filas in ((df_sistema, s_lista, filas_sistema), (df_arca, a_lista, filas_arca)):
            for i in lista:
                fila = df.loc[i].copy()
                fila["id_match"] = id_match
                fila["match_tipo"] = tipo
                fila["match_score"] = score
                filas.append(fila)

    def _df(filas, df):
        if not filas:
            return pd.DataFrame(columns=list(df.columns) + cols_extra)
        out = pd.DataFrame(filas).reset_index(drop=True)
        return out[[c for c in out.columns if c not in cols_extra] + cols_extra]

    usados_a = {i for a_lista, _, _, _ in matches for i in a_lista} | set(excluidos_arca or ())
    usados_s = {i for _, s_lista, _, _ in matches for i in s_lista} | set(excluidos_sistema or ())

    return (
        _df(filas_sistema, df_sistema),
        _df(filas_arca, df_arca),
        df_arca.loc[[i for i in df_arca.index if i not in usados_a]].reset_index(drop=True),
        df_sistema.loc[[i for i in df_sistema.index if i not in usados_s]].reset_index(drop=True),
    )


# ─────────────────────────────────────────────
# REVISAR Y NETEOS (sobre los faltantes del cruce)
# ─────────────────────────────────────────────

def tabla_revisar_comprobante(
    df_arca: pd.DataFrame, df_sistema: pd.DataFrame, revisar: list, etiqueta: str = "AGIP"
) -> pd.DataFrame:
    """
    Arma la hoja de comprobantes a revisar (mismo comprobante, importe
    distinto): columnas del organismo y del sistema en la misma fila. Las que
    se repiten en ambos lados llevan sufijo para no pisarse.
    """
    repetidas = (set(df_arca.columns) & set(df_sistema.columns)) | {"Comentario"}
    cols_arca_rev = [f"{c} {etiqueta}" if c in repetidas else c for c in df_arca.columns]
    cols_sistema_rev = [f"{c} Sistema" if c in repetidas else c for c in df_sistema.columns]
    cols_revisar = cols_arca_rev + cols_sistema_rev + ["Comentario"]

    filas_revisar = [
        list(df_arca.loc[a_idx]) + list(df_sistema.loc[s_idx]) + [comentario]
        for a_idx, s_idx, comentario in revisar
    ]
    return pd.DataFrame(filas_revisar, columns=cols_revisar)


def netear_faltantes(
    df: pd.DataFrame,
    col_importe: str,
    col_fecha: str | None,
    col_nombre: str | None = None,
    tolerancia_dias: int = 2,
    tolerancia_importe: float = 0.01,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Saca de los faltantes los pares de líneas del mismo proveedor que se
    cancelan entre sí (una positiva y una negativa por el mismo importe)
    con fechas a no más de tolerancia_dias de distancia. Por ejemplo, una
    factura y la nota de crédito que la anula. Si el archivo no trae fecha
    (col_fecha None), alcanza con el mismo proveedor y el mismo importe.

    El proveedor se identifica por CUIT; si la línea no tiene CUIT, por
    col_nombre. Para cada línea negativa se elige la positiva de fecha más
    cercana.

    Retorna:
        (df sin los pares neteados, df con los pares y su 'id_neteo')
    """
    df = df.reset_index(drop=True)
    vacio = df.iloc[0:0].assign(id_neteo=pd.Series(dtype="int64"))
    if df.empty or col_importe not in df.columns:
        return df, vacio
    usar_fecha = col_fecha is not None
    if usar_fecha and col_fecha not in df.columns:
        return df, vacio

    col_cuit = next(
        (c for c in df.columns if str(c).replace(".", "").strip().upper() == "CUIT"),
        None
    )

    def _clave(i):
        if col_cuit is not None:
            cuit = re.sub(r"\D", "", _texto(df.loc[i, col_cuit]))
            if cuit:
                return cuit
        if col_nombre is not None and col_nombre in df.columns and _texto(df.loc[i, col_nombre]):
            return re.sub(r"\s+", "", _texto(df.loc[i, col_nombre])).upper()
        return ""

    claves = {i: _clave(i) for i in df.index}
    importes = pd.to_numeric(df[col_importe], errors="coerce")
    if usar_fecha:
        fechas = pd.to_datetime(df[col_fecha], errors="coerce", dayfirst=True)

    def _dias(pos, neg):
        return abs((fechas.loc[pos] - fechas.loc[neg]).days) if usar_fecha else 0

    usados = set()
    pares = []
    for neg in df.index:
        if importes.loc[neg] >= 0 or not claves[neg]:
            continue
        if usar_fecha and pd.isna(fechas.loc[neg]):
            continue
        candidatos = [
            pos for pos in df.index
            if pos not in usados
            and importes.loc[pos] > 0
            and claves[pos] == claves[neg]
            and abs(importes.loc[pos] + importes.loc[neg]) <= tolerancia_importe
            and (not usar_fecha or (pd.notna(fechas.loc[pos]) and _dias(pos, neg) <= tolerancia_dias))
        ]
        if not candidatos:
            continue
        pos = min(candidatos, key=lambda i: (_dias(i, neg), i))
        usados.update([pos, neg])
        pares.append((pos, neg))

    filas = []
    for id_neteo, (pos, neg) in enumerate(pares, start=1):
        for i in (pos, neg):
            fila = df.loc[i].copy()
            fila["id_neteo"] = id_neteo
            filas.append(fila)

    df_neteos = pd.DataFrame(filas).reset_index(drop=True) if filas else vacio
    df_restante = df.loc[[i for i in df.index if i not in usados]].reset_index(drop=True)
    return df_restante, df_neteos


def separar_diferencias(
    df_falta_sistema: pd.DataFrame,
    df_falta_arca: pd.DataFrame,
    *,
    col_cuit_arca: str,
    col_importe_arca: str,
    col_nombre_arca: str | None,
    col_cuit_sistema: str = "CUIT",
    col_importe_sistema: str = "Importe",
    col_nombre_sistema: str | None = "Tercero",
    etiqueta: str = "AGIP",
    score_nombre_min: int = 70,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Toma los faltantes del cruce y separa los proveedores (por CUIT) que
    tienen líneas sin match de ambos lados: el organismo informa y el
    sistema registró, pero los importes no cierran. Esos casos salen de
    los faltantes y pasan a revisar. Una línea del sistema sin CUIT se
    asigna al proveedor del organismo de nombre más parecido (score >=
    score_nombre_min); si no trae nombre, queda en faltantes.

    df_falta_sistema son líneas del organismo y df_falta_arca líneas del
    sistema.

    Retorna:
        - df_falta_sistema y df_falta_arca sin esos proveedores
        - df_revisar: una fila por proveedor con 'Importe Sistema',
          'Monto <etiqueta>', 'Diferencia' (Sistema - organismo) y
          'Comentario' ('Dif: Registrado de Más' / 'Dif: Registrado de Menos')
        - df_revisar_sistema / df_revisar_arca: el detalle de cada lado,
          ligado al resumen por 'id_revisar'
    """
    col_monto = f"Monto {etiqueta}"
    col_lineas = f"Líneas {etiqueta}"
    cols_revisar = [
        "id_revisar", "CUIT", "Tercero", "Razon Social",
        "Líneas Sistema", "Importe Sistema", col_lineas, col_monto,
        "Diferencia", "Comentario",
    ]
    vacio = (
        df_falta_sistema, df_falta_arca, pd.DataFrame(columns=cols_revisar),
        df_falta_arca.iloc[0:0].assign(id_revisar=pd.Series(dtype="int64")),
        df_falta_sistema.iloc[0:0].assign(id_revisar=pd.Series(dtype="int64")),
    )
    if df_falta_sistema.empty or df_falta_arca.empty:
        return vacio

    cuit_arca = normalizar_cuit(df_falta_sistema[col_cuit_arca])
    cuit_sistema = normalizar_cuit(df_falta_arca[col_cuit_sistema])

    # Líneas del sistema sin CUIT: proveedor del organismo por nombre
    if col_nombre_arca and col_nombre_sistema:
        nombres_arca = {}
        for i, cuit in cuit_arca.items():
            nombre = _texto(df_falta_sistema.loc[i, col_nombre_arca])
            if cuit and nombre:
                nombres_arca.setdefault(cuit, _nombre_comparable(nombre))
        for i in cuit_sistema.index[cuit_sistema == ""]:
            nombre = _texto(df_falta_arca.loc[i, col_nombre_sistema])
            if not nombre or not nombres_arca:
                continue
            puntajes = {
                cuit: fuzz.token_sort_ratio(_nombre_comparable(nombre), nombre_a)
                for cuit, nombre_a in nombres_arca.items()
            }
            mejor = max(puntajes, key=puntajes.get)
            if puntajes[mejor] >= score_nombre_min:
                cuit_sistema.loc[i] = mejor

    cuits_ambos = [c for c in pd.unique(cuit_sistema) if c and c in set(cuit_arca)]
    if not cuits_ambos:
        return vacio

    def _primer_nombre(lineas, col):
        if col is None or col not in lineas.columns:
            return ""
        nombres = [_texto(v) for v in lineas[col] if _texto(v)]
        return nombres[0] if nombres else ""

    filas_revisar, det_sistema, det_arca = [], [], []
    for id_revisar, cuit in enumerate(cuits_ambos, start=1):
        lineas_s = df_falta_arca[cuit_sistema == cuit]
        lineas_a = df_falta_sistema[cuit_arca == cuit]

        importe_s = round(pd.to_numeric(lineas_s[col_importe_sistema], errors="coerce").sum(), 2)
        monto_a = round(pd.to_numeric(lineas_a[col_importe_arca], errors="coerce").sum(), 2)
        diferencia = round(importe_s - monto_a, 2)
        if diferencia > 0:
            comentario = "Dif: Registrado de Más"
        elif diferencia < 0:
            comentario = "Dif: Registrado de Menos"
        else:
            comentario = "Sin diferencia"

        filas_revisar.append({
            "id_revisar": id_revisar,
            "CUIT": cuit,
            "Tercero": _primer_nombre(lineas_s, col_nombre_sistema),
            "Razon Social": _primer_nombre(lineas_a, col_nombre_arca),
            "Líneas Sistema": len(lineas_s),
            "Importe Sistema": importe_s,
            col_lineas: len(lineas_a),
            col_monto: monto_a,
            "Diferencia": diferencia,
            "Comentario": comentario,
        })
        det_sistema.append(lineas_s.assign(id_revisar=id_revisar))
        det_arca.append(lineas_a.assign(id_revisar=id_revisar))

    en_revisar_s = cuit_sistema.isin(cuits_ambos)
    en_revisar_a = cuit_arca.isin(cuits_ambos)

    return (
        df_falta_sistema[~en_revisar_a].reset_index(drop=True),
        df_falta_arca[~en_revisar_s].reset_index(drop=True),
        pd.DataFrame(filas_revisar, columns=cols_revisar),
        pd.concat(det_sistema).reset_index(drop=True),
        pd.concat(det_arca).reset_index(drop=True),
    )


def revisar_faltantes(
    df_falta_sistema: pd.DataFrame,
    df_falta_arca: pd.DataFrame,
    *,
    col_cuit_arca: str,
    col_importe_arca: str,
    col_fecha_arca: str | None,
    col_nombre_arca: str | None,
    col_fecha_sistema: str | None = "Fecha",
    col_cuit_sistema: str = "CUIT",
    col_importe_sistema: str = "Importe",
    col_nombre_sistema: str | None = "Tercero",
    etiqueta: str = "AGIP",
    df_revisar_comprobante: pd.DataFrame | None = None,
    score_nombre_min: int = 70,
):
    """
    Mismo tratamiento de los faltantes para todos los módulos de impuestos:
    1. Netea, de cada lado, la factura y la nota de crédito del mismo
       proveedor que se cancelan entre sí.
    2. Separa los proveedores con faltantes de ambos lados (Revisar por
       proveedor).

    df_falta_sistema son líneas del organismo y df_falta_arca líneas del
    sistema.

    Retorna:
        (df_falta_sistema, df_falta_arca, hojas, stats)
        - hojas: dict nombre de hoja -> DataFrame con Revisar y Neteos (solo
          las que tienen filas)
        - stats: 'revisar_comprobante', 'revisar_proveedor', 'neteos'
    """
    df_falta_arca, df_neteos_sistema = netear_faltantes(
        df_falta_arca, col_importe=col_importe_sistema, col_fecha=col_fecha_sistema, col_nombre=col_nombre_sistema
    )
    df_falta_sistema, df_neteos_arca = netear_faltantes(
        df_falta_sistema, col_importe=col_importe_arca, col_fecha=col_fecha_arca, col_nombre=col_nombre_arca
    )

    df_falta_sistema, df_falta_arca, df_revisar, df_revisar_sistema, df_revisar_arca = separar_diferencias(
        df_falta_sistema, df_falta_arca,
        col_cuit_arca=col_cuit_arca, col_importe_arca=col_importe_arca, col_nombre_arca=col_nombre_arca,
        col_cuit_sistema=col_cuit_sistema, col_importe_sistema=col_importe_sistema,
        col_nombre_sistema=col_nombre_sistema, etiqueta=etiqueta, score_nombre_min=score_nombre_min,
    )

    if df_revisar_comprobante is None:
        df_revisar_comprobante = pd.DataFrame()

    hojas = {
        "Revisar_Comprobante": df_revisar_comprobante,
        "Revisar_Proveedor": df_revisar,
        "Revisar_Prov_Sistema": df_revisar_sistema,
        f"Revisar_Prov_{etiqueta}": df_revisar_arca,
        "Neteos_Sistema": df_neteos_sistema,
        f"Neteos_{etiqueta}": df_neteos_arca,
    }
    hojas = {nombre: df for nombre, df in hojas.items() if not df.empty}

    stats = {
        "revisar_comprobante": len(df_revisar_comprobante),
        "revisar_proveedor": len(df_revisar),
        "neteos": len(df_neteos_sistema) + len(df_neteos_arca),
    }
    return df_falta_sistema, df_falta_arca, hojas, stats
