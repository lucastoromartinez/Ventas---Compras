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
      PASO 7 - Línea del sistema sin CUIT contra el total de cada proveedor
               del organismo: matchea solo si cierra un único proveedor.
               Las líneas sin nombre no participan.

    Si hay más de un candidato uno a uno, desempata por comprobante
    (cols_comprobante = pto venta / número del organismo y del sistema) o
    por la fecha más cercana (cols_fecha = organismo, sistema).

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
            concat_a = f"{pa}{na}"
            return max(candidatos, key=lambda i: (fuzz.ratio(concat_a, f"{pv_s.loc[i]}{nc_s.loc[i]}"), -i))

        if cols_fecha and pd.notna(fecha_a.loc[a]):
            def _distancia(i):
                f = fecha_s.loc[i]
                return (pd.isna(f), abs((f - fecha_a.loc[a]).days) if pd.notna(f) else 0, i)
            return min(candidatos, key=_distancia)

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

    # PASO 7: línea del sistema sin CUIT contra el total de un único proveedor
    for s in sorted(s_libres):
        if cuit_s.loc[s] or not nombre_s.loc[s] or imp_s.loc[s] == 0:
            continue
        por_cuit = {}
        for a in sorted(a_libres):
            if cuit_a.loc[a] and nombre_a.loc[a]:
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
