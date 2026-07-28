"""
logica_cupones.py — Conciliación Cupones (Nave vs Extracto Banco)
Código extraído del notebook Cupones.ipynb. Cruza el reporte de
acreditaciones de Nave contra el extracto bancario (que ya llega
normalizado y categorizado, con la columna "conciliacion" cargada).
Solo se agregó:
  - imports al tope
  - loaders adaptados para Streamlit (file-like en vez de Path)
  - correr_conciliacion_cupones() como pipeline de entrada para la app
"""

from io import BytesIO
from itertools import combinations

import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────────────────────────────────────────

def importar_extracto_banco(archivo) -> pd.DataFrame:
    """
    El extracto de banco ya llega normalizado y categorizado (fecha,
    debitos/creditos/saldo/importe tipados y columna "conciliacion"
    cargada), así que se importa tal cual.
    """
    return pd.read_excel(archivo)


def importar_reporte_nave(
    archivo,
    hoja: str | int = 0,
    columna_busqueda: int = 0,
    texto_encabezado: str = "Fecha de operación",
    max_filas_busqueda: int = 50,
) -> pd.DataFrame:
    raw = pd.read_excel(archivo, sheet_name=hoja, header=None, nrows=max_filas_busqueda)

    mascara = raw.iloc[:, columna_busqueda].astype(str).str.strip() == texto_encabezado
    filas_encontradas = raw.index[mascara].tolist()
    if not filas_encontradas:
        raise ValueError(
            f"No se encontró '{texto_encabezado}' en las primeras "
            f"{max_filas_busqueda} filas de la columna {columna_busqueda}."
        )
    fila_header = filas_encontradas[0]

    df = pd.read_excel(archivo, sheet_name=hoja, header=fila_header)
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)

    mascara_total = df.iloc[:, 0].astype(str).str.strip() == "Total"
    filas_total = df.index[mascara_total].tolist()
    if filas_total:
        df = df.loc[:filas_total[0] - 1]
    df.reset_index(drop=True, inplace=True)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# DEPURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

def depurar_leyenda(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deja "leyenda adicional1" reducida al código de operación puro, sacando
    los prefijos que agrega el banco ("Devolución - ", "Operación ",
    "Grupo de acreditación ") y los sufijos "PCT"/"TCTD". Solo se saca el
    guion pegado a "Devolución": el resto de los guiones se respeta porque
    los códigos de grupo también los usan (p.ej. "LS-21825349").
    """
    df = df.copy()
    df["leyenda adicional1"] = (
        df["leyenda adicional1"]
        .astype(str)
        .str.replace(r"^Devolución\s*-\s*", "", regex=True)
        .str.replace(r"^Operación\s+", "", regex=True)
        .str.replace(r"^Grupo de acreditación\s+", "", regex=True)
        .str.replace(" PCT", "", regex=False)
        .str.replace(" TCTD", "", regex=False)
        .str.strip()
    )
    return df


def depurar_nave(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # --- Detectar nombre de columna de operación ---
    if "Número de operación" in df.columns:
        col_operacion = "Número de operación"
    elif "Código de operación" in df.columns:
        col_operacion = "Código de operación"
    else:
        raise ValueError("No se encontró 'Número de operación' ni 'Código de operación' en el DataFrame.")

    # --- Fecha de operación ---
    fecha_op = pd.to_datetime(
        df["Fecha de operación"].astype(str).str.strip(),
        format="%d/%m/%Y %H:%M",
        errors="coerce"
    )
    # 00:00 a 00:05 → corresponde al día anterior
    mask_anterior = (fecha_op.dt.hour == 0) & (fecha_op.dt.minute <= 5)
    fecha_op = fecha_op.where(~mask_anterior, fecha_op - pd.Timedelta(days=1))
    df["Fecha de operación"] = fecha_op.dt.normalize()

    # --- Fecha de acreditación ---
    df["Fecha de acreditación"] = pd.to_datetime(
        df["Fecha de acreditación"].astype(str).str.strip(),
        format="%d/%m/%Y",
        errors="coerce"
    )

    # --- Grupo de acreditación ---
    mask_vacio = (
        df["Grupo de acreditación"].isna() |
        df["Grupo de acreditación"].astype(str).str.strip().isin(["", "nan", "None", "-"])
    )
    # Tipo de acreditación: si Nave nunca agrupó la operación (grupo vacío/"-", se
    # rellena con el número de operación) vs. si vino con un código de lote real.
    df["Tipo de acreditación"] = mask_vacio.map({True: "Individual", False: "Grupo"})

    # col_operacion puede ser numérico (arrastra ".0", ej. 127585397.0) o alfanumérico
    # (ej. "JIU213094047", formato que Nave empezó a usar en meses más recientes): si
    # es numérico, se limpia el ".0"; si no, se usa el texto tal cual.
    raw_str = df[col_operacion].astype(str).str.strip()
    numeros_operacion = pd.to_numeric(df[col_operacion], errors="coerce")
    limpio = numeros_operacion.astype("Int64").astype(str)
    limpio = limpio.where(numeros_operacion.notna(), raw_str)
    df.loc[mask_vacio, "Grupo de acreditación"] = limpio.loc[mask_vacio]
    df["Grupo de acreditación"] = df["Grupo de acreditación"].astype(str).str.strip()

    # --- Redondeo de columnas float (algunos meses no traen todas) ---
    cols_float = [
        "Monto bruto",
        "Costo del servicio",
        "IVA del costo",
        "Retención IIBB CABA",
        "Monto neto",
        "Propina",
    ]
    for col in cols_float:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = df[col].round(2)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# CRUCE
# ─────────────────────────────────────────────────────────────────────────────

def cruce_nave_banco(
    df_nave_dep: pd.DataFrame,
    df_banco_acred: pd.DataFrame,
    tolerancia: float = 1.0,
    columnas_ajuste_candidatas: list[str] = [
        "Retención IIBB CABA",
        "IVA del costo",
        "Costo del servicio",
        "Propina",
    ],  # agregar acá otras deducciones si aparecen
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Cruza nave vs banco por Grupo de acreditación ↔ leyenda adicional1.

    Primero intenta matchear cada grupo contra "Monto neto" tal cual. Si no
    cierra dentro de la tolerancia, prueba restarle -una por una, y después
    combinadas- las columnas de columnas_ajuste_candidatas (deducciones que a veces no
    están contempladas en "Monto neto", como la Retención IIBB CABA) hasta
    encontrar la que hace que el importe coincida con el banco. La columna
    "Diferencia Identificada" en match_nave/match_banco indica qué hubo que
    restar para que matcheara ("" si matcheó directo contra Monto neto).

    Si un código se repite en banco (p.ej. acreditación + devolución), se
    toma como match la combinación con menor diferencia y el resto queda
    faltante.

    falta_banco y falta_nave incluyen columna "comentario":
        - "Falta Cupón" (solo en falta_banco): el grupo no tiene contraparte
          en el banco y es "Individual" (Nave nunca lo agrupó).
        - "Falta Grupo" (solo en falta_banco): el grupo no tiene contraparte
          en el banco y es un lote real ("Tipo de acreditación" = "Grupo").
        - "Falta Cupón" (en falta_nave): el código de banco no tiene
          contraparte del lado de Nave.
        - "Diferencia de Importe": el código está en ambos lados pero ningún
          ajuste probado hizo que el monto cierre dentro de la tolerancia.
        - "Falta Cancelación" (solo en falta_nave): fila de banco duplicada
          de un código ya matcheado (p.ej. la devolución de una
          acreditación que sí reconcilió).

    Retorna: (match_nave, match_banco, falta_banco, falta_nave)
    """
    columnas_ajuste = [c for c in columnas_ajuste_candidatas if c in df_nave_dep.columns]

    # 1. Sumarizar nave por grupo: Monto neto + cada columna de ajuste disponible
    nave_sum = (
        df_nave_dep
        .groupby("Grupo de acreditación", as_index=False)[["Monto neto"] + columnas_ajuste]
        .sum()
    )

    # 2. Banco con índice propio para tracking
    banco = df_banco_acred.copy().reset_index(drop=True)
    banco["_banco_idx"] = banco.index

    # 3. Merge outer con indicator
    merged = pd.merge(
        nave_sum,
        banco[["_banco_idx", "leyenda adicional1", "importe"]],
        left_on="Grupo de acreditación",
        right_on="leyenda adicional1",
        how="outer",
        indicator=True,
    )

    # 4. Por cada par (grupo, fila de banco) buscar el ajuste que hace matchear
    def buscar_ajuste(row):
        base = row["Monto neto"]
        importe = row["importe"]

        candidatos = [("", base)]  # sin ajuste
        for r in range(1, len(columnas_ajuste) + 1):
            for combo in combinations(columnas_ajuste, r):
                etiqueta = " + ".join(combo)
                ajustado = base - sum(row[c] for c in combo)
                candidatos.append((etiqueta, ajustado))

        mejor_etiqueta = mejor_monto = mejor_diff = None
        for etiqueta, monto in candidatos:
            diff = abs(monto - importe)
            if diff <= tolerancia:
                return pd.Series([monto, diff, etiqueta])
            if mejor_diff is None or diff < mejor_diff:
                mejor_etiqueta, mejor_monto, mejor_diff = etiqueta, monto, diff

        return pd.Series([mejor_monto, mejor_diff, mejor_etiqueta])

    both = merged[merged["_merge"] == "both"].copy()
    if len(both) > 0:
        ajustes = both.apply(buscar_ajuste, axis=1)
        ajustes.columns = ["monto_neto_suma", "diferencia", "diferencia_identificada"]
        both = both.join(ajustes)
    else:
        both["monto_neto_suma"] = both.get("Monto neto")
        both["diferencia"] = pd.Series(dtype=float)
        both["diferencia_identificada"] = pd.Series(dtype=str)

    # 5. Si un código se repite en banco, nos quedamos con la combinación de
    #    menor diferencia como el match real; el resto son duplicados.
    both = both.sort_values("diferencia")
    elegidos = both.drop_duplicates(subset="Grupo de acreditación", keep="first")
    duplicados_idx = set(both["_banco_idx"]) - set(elegidos["_banco_idx"])

    mask_match      = elegidos["diferencia"] <= tolerancia
    mask_left_only  = merged["_merge"] == "left_only"
    mask_right_only = merged["_merge"] == "right_only"

    # Ajuste probado (matcheado o no) para cada grupo/fila de banco: se usa tanto
    # en los matches (qué hubo que restar para que cierre) como en "Diferencia de
    # Importe" (qué combinación se acercó más, aunque no haya cerrado).
    ajuste_por_grupo_todos = elegidos.set_index("Grupo de acreditación")["diferencia_identificada"].to_dict()
    ajuste_por_idx_todos   = elegidos.set_index("_banco_idx")["diferencia_identificada"].to_dict()

    # 6. Asignar match_id a los matches
    matches = elegidos[mask_match].copy().reset_index(drop=True)
    matches["match_id"] = [f"MATCH-{i+1:04d}" for i in range(len(matches))]

    # 7. match_nave: filas de nave de grupos matcheados, con el ajuste identificado
    grupo_a_id     = matches.set_index("Grupo de acreditación")["match_id"].to_dict()
    grupo_a_ajuste = matches.set_index("Grupo de acreditación")["diferencia_identificada"].to_dict()
    nave_out = df_nave_dep.copy()
    nave_out["match_id"] = nave_out["Grupo de acreditación"].map(grupo_a_id)
    nave_out["Diferencia Identificada"] = nave_out["Grupo de acreditación"].map(grupo_a_ajuste)
    match_nave = nave_out[nave_out["match_id"].notna()].reset_index(drop=True)

    # 8. match_banco: filas de banco matcheadas, con el ajuste identificado
    idx_a_id     = matches.set_index("_banco_idx")["match_id"].to_dict()
    idx_a_ajuste = matches.set_index("_banco_idx")["diferencia_identificada"].to_dict()
    banco_out = banco.copy()
    banco_out["match_id"] = banco_out["_banco_idx"].map(idx_a_id)
    banco_out["Diferencia Identificada"] = banco_out["_banco_idx"].map(idx_a_ajuste)
    match_banco = (
        banco_out[banco_out["match_id"].notna()]
        .drop(columns=["_banco_idx"])
        .reset_index(drop=True)
    )

    # 9. falta_banco: grupos de nave sin match, con motivo
    grupos_falta_cupon = set(merged.loc[mask_left_only, "Grupo de acreditación"].dropna())
    grupos_diferencia  = set(elegidos.loc[~mask_match, "Grupo de acreditación"])

    falta_banco = (
        df_nave_dep[df_nave_dep["Grupo de acreditación"].isin(grupos_falta_cupon | grupos_diferencia)]
        .copy()
        .reset_index(drop=True)
    )
    def comentario_nave(row):
        if row["Grupo de acreditación"] in grupos_diferencia:
            return "Diferencia de Importe"
        return "Falta Grupo" if row["Tipo de acreditación"] == "Grupo" else "Falta Cupón"

    falta_banco["comentario"] = falta_banco.apply(comentario_nave, axis=1)
    # Para "Diferencia de Importe": qué combinación de columnas se acercó más al
    # importe del banco (aunque no haya cerrado dentro de la tolerancia).
    falta_banco["Diferencia Identificada"] = (
        falta_banco["Grupo de acreditación"].map(ajuste_por_grupo_todos).fillna("")
    )
    falta_banco.loc[
        (falta_banco["comentario"] == "Diferencia de Importe") & (falta_banco["Diferencia Identificada"] == ""),
        "Diferencia Identificada",
    ] = "Sin explicar (ninguna combinación de deducciones se acerca)"

    # 10. falta_nave: filas de banco sin match, con motivo
    idx_falta_cupon = set(merged.loc[mask_right_only, "_banco_idx"].dropna().astype(int))
    idx_diferencia  = set(elegidos.loc[~mask_match, "_banco_idx"].dropna().astype(int))

    def comentario_banco(idx: int) -> str:
        if idx in duplicados_idx:
            return "Falta Cancelación"
        if idx in idx_diferencia:
            return "Diferencia de Importe"
        return "Falta Cupón"

    idx_sin_match = idx_falta_cupon | idx_diferencia | duplicados_idx
    falta_nave = banco[banco["_banco_idx"].isin(idx_sin_match)].copy()
    falta_nave["comentario"] = falta_nave["_banco_idx"].map(comentario_banco)
    # Para "Diferencia de Importe": qué combinación de columnas se acercó más al
    # importe del banco (aunque no haya cerrado dentro de la tolerancia).
    falta_nave["Diferencia Identificada"] = (
        falta_nave["_banco_idx"].map(ajuste_por_idx_todos).fillna("")
    )
    falta_nave.loc[
        (falta_nave["comentario"] == "Diferencia de Importe") & (falta_nave["Diferencia Identificada"] == ""),
        "Diferencia Identificada",
    ] = "Sin explicar (ninguna combinación de deducciones se acerca)"
    falta_nave = falta_nave.drop(columns=["_banco_idx"]).reset_index(drop=True)

    return match_nave, match_banco, falta_banco, falta_nave


def generar_tabla_resumen(falta_banco: pd.DataFrame, falta_nave: pd.DataFrame) -> pd.DataFrame:
    """
    Junta falta_banco y falta_nave (salida de cruce_nave_banco) en una sola
    tabla resumen por código, al estilo de la tabla dinámica que arma el
    equipo a mano (Grupo/N° Cupón, Importe Banco, Importe Nave, Diferencia).

    - "Grupo/N°Cupón": el código — "Grupo de acreditación" de falta_banco o
      "leyenda adicional1" de falta_nave (mismo namespace, por eso se unen).
    - "Importe Banco": suma de "importe" (falta_nave) por código.
    - "Importe Nave": suma de "Monto neto" (falta_banco) por código.
    - "Diferencia": Importe Banco - Importe Nave.
    - "Diferencia Identificada", "comentario", "Tipo de acreditación": una
      sola columna cada una (no duplicada). Cuando el código está en ambos
      lados coinciden; cuando está solo de un lado (Falta Cupón/Falta Grupo
      solo en Nave, o Falta Cupón/Falta Cancelación solo en Banco), se toma
      el valor del lado que exista. "Tipo de acreditación" solo existe del
      lado de Nave, así que queda vacío para códigos que están solo en Banco.
    """
    nave_por_grupo = (
        falta_banco
        .groupby("Grupo de acreditación")
        .agg(
            **{
                "Importe Nave": ("Monto neto", "sum"),
                "comentario_nave": ("comentario", "first"),
                "dif_id_nave": ("Diferencia Identificada", "first"),
                "Tipo de acreditación": ("Tipo de acreditación", "first"),
            }
        )
    )

    banco_por_codigo = (
        falta_nave
        .groupby("leyenda adicional1")
        .agg(
            **{
                "Importe Banco": ("importe", "sum"),
                "comentario_banco": ("comentario", "first"),
                "dif_id_banco": ("Diferencia Identificada", "first"),
            }
        )
    )

    tabla_resumen = nave_por_grupo.join(banco_por_codigo, how="outer")
    tabla_resumen.index.name = "Grupo/N°Cupón"
    tabla_resumen = tabla_resumen.reset_index()

    tabla_resumen["Importe Banco"] = tabla_resumen["Importe Banco"].fillna(0.0)
    tabla_resumen["Importe Nave"]  = tabla_resumen["Importe Nave"].fillna(0.0)
    tabla_resumen["Diferencia"] = tabla_resumen["Importe Banco"] - tabla_resumen["Importe Nave"]

    tabla_resumen["comentario"] = tabla_resumen["comentario_nave"].fillna(tabla_resumen["comentario_banco"])
    tabla_resumen["Diferencia Identificada"] = tabla_resumen["dif_id_nave"].fillna(tabla_resumen["dif_id_banco"])

    tabla_resumen = tabla_resumen.drop(columns=["comentario_nave", "comentario_banco", "dif_id_nave", "dif_id_banco"])
    tabla_resumen = tabla_resumen[
        ["Grupo/N°Cupón", "Importe Banco", "Importe Nave", "Diferencia",
         "Diferencia Identificada", "comentario", "Tipo de acreditación"]
    ]

    return tabla_resumen


def resolver_con_mes_anterior(
    df_nave_actual_dep: pd.DataFrame,
    df_banco_actual_acred: pd.DataFrame,
    df_nave_anterior_dep: pd.DataFrame,
    df_banco_anterior_acred: pd.DataFrame,
    tabla_resumen: pd.DataFrame,
    tolerancia: float = 1.0,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Cruza los pendientes del mes anterior contra los datos frescos del mes
    actual, y reevalúa las "Diferencia de Importe" del mes actual sumando el
    Nave completo del mes anterior. No hace falta el mes siguiente: cada mes
    se resuelve solo con su propio par (actual, anterior).

    1. Corre el cruce base del mes anterior (Nave anterior vs Banco anterior)
       para sacar su propio falta_banco al vuelo (no hace falta que esté
       guardado de una corrida previa).
    2. Corre el cruce base del mes actual (Nave actual vs Banco actual), el
       de siempre.
    3. Resuelve el falta_banco del mes anterior contra el mes actual:
        - "Falta Cupón" (individual): busca el código en el banco actual,
          match exacto (tolerancia) contra Monto neto. No suma nada, es una
          operación atómica.
        - "Falta Grupo" (lote): suma Monto neto del mes anterior + Monto
          neto del mes actual (mismo grupo) y compara contra el banco
          actual, por si el lote siguió sumando ventas del mes actual antes
          de cerrarse.
    4. Reevalúa "Diferencia de Importe" del mes actual sumando el Monto neto
       del mes anterior COMPLETO (no solo su pendiente) al del mes actual,
       para el mismo grupo, contra el banco actual — el lote arrancó antes
       del mes actual, así que la pieza que falta está del lado del Nave
       anterior completo.
    5. Recorre tabla_resumen (la del mes actual, la que ya tenías armada de
       antes con generar_tabla_resumen — se pasa como parámetro) y agrega la
       columna "Acreditado", mirando hacia atrás contra el Nave completo del
       mes anterior (no contra tabla_resumen_anterior — el cruce va en el
       sentido opuesto al del paso 3/4: acá partimos de febrero y miramos si
       se explica con enero, no al revés):
        - Código que está solo del lado banco en el mes actual ("Falta
          Cupón" con "Tipo de acreditación" vacío, porque no tiene ninguna
          fila de Nave este mes): se busca ese código en el Nave del mes
          anterior completo (por si es una operación de un mes atrás recién
          acreditada ahora). Si el Monto neto de esa búsqueda cierra contra
          "Importe Banco", "Acreditado"; si no, "Por acreditar".
        - "Diferencia de Importe" (código con datos de ambos lados este mes,
          pero no cierra): se suma el Nave del mes anterior completo al
          "Importe Nave" de este mes y se compara contra "Importe Banco" —
          si cierra, "Acreditado"; si no, "Por acreditar".
        - "Falta Cupón"/"Falta Grupo" con "Tipo de acreditación" poblado
          (hay Nave este mes pero no banco todavía): son ventas del mes
          actual esperando su propio crédito, no se explican mirando para
          atrás — siempre "Por acreditar".

    Retorna:
        match_nave, match_banco: del cruce base del mes actual, sin tocar.
        falta_nave: del cruce base del mes actual, sin tocar (falta_nave del
            mes anterior tampoco se toca, no se devuelve — ver nota abajo).
        falta_banco_actual_actualizado: falta_banco del mes actual, sacando
            las "Diferencia de Importe" que cerraron sumando el mes
            anterior.
        falta_banco_anterior_pendiente: lo que quedó pendiente del mes
            anterior después de intentar resolverlo con el mes actual (para
            seguir arrastrando al mes que viene).
        mes_anterior_acreditado: filas que se resolvieron en esta pasada
            (pendientes del mes anterior que ya se acreditaron, o
            diferencias del mes actual que cerraron sumando el mes
            anterior), con columna "resolucion" indicando el motivo.
        tabla_resumen_actualizada: tabla_resumen del mes actual (misma
            forma que la de entrada), con la columna nueva "Acreditado"
            ("Acreditado"/"Por acreditar").

    Nota: falta_nave del mes anterior no se cruza para resolver Falta
    Cupón/Falta Grupo (paso 3) — sus propios "Falta Cupón" se explicarían
    con el Nave de un mes *más* anterior todavía (dos meses atrás de
    "actual"), fuera del alcance de esta función.
    """
    # 1. Cruce base del mes anterior (para sacar su propio falta_banco)
    _, _, falta_banco_anterior, _ = cruce_nave_banco(
        df_nave_anterior_dep, df_banco_anterior_acred, tolerancia=tolerancia
    )

    # 2. Cruce base del mes actual (el de siempre)
    match_nave, match_banco, falta_banco_actual, falta_nave_actual = cruce_nave_banco(
        df_nave_actual_dep, df_banco_actual_acred, tolerancia=tolerancia
    )

    banco_actual_by_leyenda  = df_banco_actual_acred.groupby("leyenda adicional1")["importe"].sum()
    nave_actual_by_grupo     = df_nave_actual_dep.groupby("Grupo de acreditación")["Monto neto"].sum()
    nave_anterior_by_grupo   = df_nave_anterior_dep.groupby("Grupo de acreditación")["Monto neto"].sum()

    # 3. Resolver falta_banco del mes anterior contra el mes actual
    def resolver_fila_anterior(row):
        grupo = row["Grupo de acreditación"]
        if row["comentario"] == "Falta Cupón":
            monto = row["Monto neto"]
            if grupo in banco_actual_by_leyenda.index and abs(monto - banco_actual_by_leyenda[grupo]) <= tolerancia:
                return "Acreditado en el mes actual"
            return None
        if row["comentario"] == "Falta Grupo":
            monto_total = nave_anterior_by_grupo.get(grupo, 0) + nave_actual_by_grupo.get(grupo, 0)
            if grupo in banco_actual_by_leyenda.index and abs(monto_total - banco_actual_by_leyenda[grupo]) <= tolerancia:
                return "Cierra sumando el mes actual"
            return None
        return None  # "Diferencia de Importe" del mes anterior no se toca acá

    falta_banco_anterior = falta_banco_anterior.copy()
    falta_banco_anterior["resolucion"] = falta_banco_anterior.apply(resolver_fila_anterior, axis=1)

    mes_anterior_acreditado_anterior = falta_banco_anterior[falta_banco_anterior["resolucion"].notna()].copy()
    falta_banco_anterior_pendiente = (
        falta_banco_anterior[falta_banco_anterior["resolucion"].isna()]
        .drop(columns=["resolucion"])
        .reset_index(drop=True)
    )

    # 4. Reevaluar "Diferencia de Importe" del mes actual sumando el Nave
    #    COMPLETO del mes anterior (no solo su pendiente)
    di_mask = falta_banco_actual["comentario"] == "Diferencia de Importe"
    grupos_di = falta_banco_actual.loc[di_mask, "Grupo de acreditación"].unique()

    def reevaluar_diferencia(grupo):
        monto_total = nave_anterior_by_grupo.get(grupo, 0) + nave_actual_by_grupo.get(grupo, 0)
        importe_banco = banco_actual_by_leyenda.get(grupo, None)
        if importe_banco is not None and abs(monto_total - importe_banco) <= tolerancia:
            return "Cierra sumando el mes anterior"
        return None

    resolucion_di = {g: reevaluar_diferencia(g) for g in grupos_di}

    falta_banco_actual = falta_banco_actual.copy()
    falta_banco_actual["resolucion"] = falta_banco_actual["Grupo de acreditación"].map(resolucion_di)

    mes_anterior_acreditado_actual = falta_banco_actual[falta_banco_actual["resolucion"].notna()].copy()
    falta_banco_actual_actualizado = (
        falta_banco_actual[falta_banco_actual["resolucion"].isna()]
        .drop(columns=["resolucion"])
        .reset_index(drop=True)
    )

    mes_anterior_acreditado = pd.concat([mes_anterior_acreditado_anterior, mes_anterior_acreditado_actual], ignore_index=True)

    # 5. tabla_resumen (mes actual) mirando hacia atrás contra el Nave completo
    #    del mes anterior
    def estado_acreditado(row):
        codigo = row["Grupo/N°Cupón"]
        if pd.isna(row["Tipo de acreditación"]):
            # Falta Cupón solo del lado banco (sin Nave este mes): puede ser
            # una operación de un mes atrás recién acreditada.
            monto_anterior = nave_anterior_by_grupo.get(codigo, 0.0)
            diferencia_residual = row["Importe Banco"] - monto_anterior
        elif row["comentario"] == "Diferencia de Importe":
            monto_total = row["Importe Nave"] + nave_anterior_by_grupo.get(codigo, 0.0)
            diferencia_residual = row["Importe Banco"] - monto_total
        else:
            # Falta Cupón/Falta Grupo con Nave este mes pero sin banco todavía:
            # venta del mes actual esperando su propio crédito, no se explica
            # mirando para atrás.
            return pd.Series(["Por acreditar", None])

        if abs(diferencia_residual) <= tolerancia:
            return pd.Series(["Acreditado", None])
        return pd.Series(["Por acreditar", diferencia_residual])

    tabla_resumen_actualizada = tabla_resumen.copy()
    tabla_resumen_actualizada[["Acreditado", "Diferencia Residual"]] = (
        tabla_resumen_actualizada.apply(estado_acreditado, axis=1)
    )

    return (
        match_nave, match_banco, falta_nave_actual, falta_banco_actual_actualizado,
        falta_banco_anterior_pendiente, mes_anterior_acreditado, tabla_resumen_actualizada,
    )


# ─────────────────────────────────────────────────────────────────────────────
# EXPORTAR EN MEMORIA
# ─────────────────────────────────────────────────────────────────────────────

def generar_excel_en_memoria_cupones(match_nave, match_banco, falta_banco, falta_nave, tabla_resumen) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        match_nave.to_excel(writer,    sheet_name="Match Nave",    index=False)
        match_banco.to_excel(writer,   sheet_name="Match Banco",   index=False)
        falta_banco.to_excel(writer,   sheet_name="Falta Banco",   index=False)
        falta_nave.to_excel(writer,    sheet_name="Falta Nave",    index=False)
        tabla_resumen.to_excel(writer, sheet_name="Tabla Resumen", index=False)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE COMPLETO
# ─────────────────────────────────────────────────────────────────────────────

def correr_conciliacion_cupones(archivo_banco, archivo_nave, tolerancia: float = 1.0):
    # 1. Cargar
    df_banco = importar_extracto_banco(archivo_banco)
    df_nave  = importar_reporte_nave(archivo_nave)

    # 2. Depurar
    df_banco_acred = depurar_leyenda(df_banco)
    df_nave_dep    = depurar_nave(df_nave)

    # 3. Cruzar
    match_nave, match_banco, falta_banco, falta_nave = cruce_nave_banco(
        df_nave_dep, df_banco_acred, tolerancia=tolerancia
    )

    # 4. Tabla resumen (por código, al estilo de la tabla dinámica del equipo)
    tabla_resumen = generar_tabla_resumen(falta_banco, falta_nave)

    match_ajustado = int((match_banco["Diferencia Identificada"].fillna("") != "").sum())

    stats = {
        "grupos_matcheados": len(match_banco),
        "match_ajustado":    match_ajustado,
        "falta_banco":       len(falta_banco),
        "falta_nave":        len(falta_nave),
    }

    buf = generar_excel_en_memoria_cupones(match_nave, match_banco, falta_banco, falta_nave, tabla_resumen)
    return buf, stats


def generar_excel_en_memoria_cupones_con_anterior(
    match_nave, match_banco, falta_nave, falta_banco,
    falta_banco_mes_anterior, mes_anterior_acreditado, tabla_resumen_actualizada,
) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        match_nave.to_excel(writer,               sheet_name="Match Nave",              index=False)
        match_banco.to_excel(writer,               sheet_name="Match Banco",             index=False)
        falta_banco.to_excel(writer,               sheet_name="Falta Banco",             index=False)
        falta_nave.to_excel(writer,                sheet_name="Falta Nave",              index=False)
        falta_banco_mes_anterior.to_excel(writer,  sheet_name="Pendiente Mes Anterior",  index=False)
        mes_anterior_acreditado.to_excel(writer,   sheet_name="Acreditado Mes Anterior", index=False)
        tabla_resumen_actualizada.to_excel(writer, sheet_name="Tabla Resumen",           index=False)
    return buf.getvalue()


def correr_conciliacion_cupones_con_anterior(
    archivo_banco_actual, archivo_nave_actual,
    archivo_banco_anterior, archivo_nave_anterior,
    tolerancia: float = 1.0,
):
    # 1. Cargar los 4 adjuntos
    df_banco_actual    = importar_extracto_banco(archivo_banco_actual)
    df_nave_actual     = importar_reporte_nave(archivo_nave_actual)
    df_banco_anterior  = importar_extracto_banco(archivo_banco_anterior)
    df_nave_anterior   = importar_reporte_nave(archivo_nave_anterior)

    # 2. Depurar
    df_banco_actual_acred    = depurar_leyenda(df_banco_actual)
    df_nave_actual_dep       = depurar_nave(df_nave_actual)
    df_banco_anterior_acred  = depurar_leyenda(df_banco_anterior)
    df_nave_anterior_dep     = depurar_nave(df_nave_anterior)

    # 3. tabla_resumen del mes actual (insumo de resolver_con_mes_anterior)
    _, _, falta_banco_actual0, falta_nave_actual0 = cruce_nave_banco(
        df_nave_actual_dep, df_banco_actual_acred, tolerancia=tolerancia
    )
    tabla_resumen = generar_tabla_resumen(falta_banco_actual0, falta_nave_actual0)

    # 4. Cruzar mes actual + resolver contra mes anterior
    (
        match_nave, match_banco, falta_nave, falta_banco,
        falta_banco_mes_anterior, mes_anterior_acreditado, tabla_resumen_actualizada,
    ) = resolver_con_mes_anterior(
        df_nave_actual_dep, df_banco_actual_acred,
        df_nave_anterior_dep, df_banco_anterior_acred,
        tabla_resumen,
        tolerancia=tolerancia,
    )

    match_ajustado = int((match_banco["Diferencia Identificada"].fillna("") != "").sum())

    stats = {
        "grupos_matcheados":        len(match_banco),
        "match_ajustado":           match_ajustado,
        "falta_banco":              len(falta_banco),
        "falta_nave":               len(falta_nave),
        "pendiente_mes_anterior":   len(falta_banco_mes_anterior),
        "acreditado_mes_anterior":  len(mes_anterior_acreditado),
    }

    buf = generar_excel_en_memoria_cupones_con_anterior(
        match_nave, match_banco, falta_nave, falta_banco,
        falta_banco_mes_anterior, mes_anterior_acreditado, tabla_resumen_actualizada,
    )
    return buf, stats
