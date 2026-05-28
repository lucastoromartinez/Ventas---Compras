import io
import re
import pandas as pd
import openpyxl
import pdfplumber


# ─────────────────────────────────────────────
# IMPORTAR LIQUIDACIÓN (Excel)
# ─────────────────────────────────────────────

def importar_liquidacion(archivo):
    wb = openpyxl.load_workbook(archivo, data_only=True)
    ws = wb['Resumen']

    merged_ranges = list(ws.merged_cells.ranges)
    for rango in merged_ranges:
        if rango.min_col == 2:
            valor = ws.cell(rango.min_row, 2).value
            ws.unmerge_cells(str(rango))
            for fila in range(rango.min_row, rango.max_row + 1):
                ws.cell(fila, 2).value = valor

    registros = []
    for row in ws.iter_rows(min_row=10, values_only=True):
        if not any(c is not None for c in row):
            continue
        registros.append({
            'Grupo'  : row[1],
            'Valores': row[2],
            'Total'  : row[3],
        })

    df = pd.DataFrame(registros)
    df['Grupo'] = df['Grupo'].ffill()
    df['Valores'] = df['Valores'].ffill()
    df['Total'] = pd.to_numeric(df['Total'], errors='coerce')
    df = df.dropna(subset=['Valores']).reset_index(drop=True)
    return df


def depurar_liquidacion(df):
    excluir = ['SUM of Venta Bruta', 'SUM of Descuento de Producto', 'SUM of Subtotal antes de impuestos']
    df = df[~df['Valores'].isin(excluir)]
    df = df[df['Grupo'] != 'Valor total a transferir']
    df = df[df['Total'].notna()]
    df = df[df['Total'] != 0]
    df['Total'] = df['Total'].round(2)
    return df.reset_index(drop=True)


# ─────────────────────────────────────────────
# IMPORTAR FACTURAS (PDFs)
# ─────────────────────────────────────────────

def _parse_factura(file_obj):
    with pdfplumber.open(file_obj) as pdf:
        words = pdf.pages[0].extract_words()

    COL = {'cod':(36,115), 'cant':(115,158), 'desc':(158,379),
           'imp':(379,440), 'punit':(440,522), 'ptotal':(520,600)}
    IMP = {'codigo':(200,257), 'alicuota':(316,363), 'impuesto':(363,520)}

    def x_in(w, rng): return rng[0] <= w['x0'] < rng[1]

    header_top = next(w['top'] for w in words if w['text']=='Cod.' and 50<w['x0']<70)
    obs_top    = next(w['top'] for w in words if w['text']=='Observaciones' and w['x0']<120)
    det_top    = next(w['top'] for w in words if w['text']=='Código' and w['x0']>200)
    total_top  = next(w['top'] for w in words if w['text']=='Total' and 460<w['x0']<510)

    tbl_words = [w for w in words if header_top < w['top'] < obs_top]

    ptotal_anchors = sorted(
        [w for w in tbl_words if x_in(w, COL['ptotal']) and re.match(r'^-?\d', w['text'])],
        key=lambda w: w['top']
    )
    tops = [w['top'] for w in ptotal_anchors] + [obs_top]

    def collect_col(row_words, col_key):
        ws_ = sorted([w for w in row_words if x_in(w, COL[col_key])], key=lambda w:(w['top'],w['x0']))
        lines, cur_t, cur = [], None, []
        for w in ws_:
            if cur_t is None or abs(w['top']-cur_t) > 4:
                if cur: lines.append(' '.join(cur))
                cur, cur_t = [w['text']], w['top']
            else:
                cur.append(w['text'])
        if cur: lines.append(' '.join(cur))
        return ' '.join(lines).strip()

    rows = []
    for i, anchor in enumerate(ptotal_anchors):
        t_start = tops[i] - 3
        t_end   = tops[i+1] - 3
        rw = [w for w in tbl_words if t_start <= w['top'] < t_end]
        cant_vals = [w['text'] for w in rw if x_in(w, COL['cant']) and re.match(r'^\d', w['text'])]
        rows.append({
            'Cod':         collect_col(rw, 'cod'),
            'Cant':        cant_vals[0] if cant_vals else None,
            'Descripción': collect_col(rw, 'desc'),
            'Imp':         collect_col(rw, 'imp'),
            'P.Unit':      collect_col(rw, 'punit'),
            'P.Total':     anchor['text'],
        })
    df_main = pd.DataFrame(rows)

    imp_words = [w for w in words if det_top < w['top'] < total_top]
    ali_anchors = sorted(
        [w for w in imp_words if x_in(w, IMP['alicuota']) and '%' in w['text']],
        key=lambda w: w['top']
    )
    ali_tops = [w['top'] for w in ali_anchors] + [total_top]

    def collect_imp(row_words, col_key):
        ws_ = sorted([w for w in row_words if x_in(w, IMP[col_key])], key=lambda w:(w['top'],w['x0']))
        lines, cur_t, cur = [], None, []
        for w in ws_:
            if cur_t is None or abs(w['top']-cur_t) > 4:
                if cur: lines.append(' '.join(cur))
                cur, cur_t = [w['text']], w['top']
            else:
                cur.append(w['text'])
        if cur: lines.append(' '.join(cur))
        return ' '.join(lines).strip()

    imp_rows = []
    for i, anchor in enumerate(ali_anchors):
        t_start = ali_tops[i-1]+1 if i > 0 else det_top
        t_end   = ali_tops[i+1]-1
        rw = [w for w in imp_words if t_start <= w['top'] <= t_end]
        imp_rows.append({
            'Cod':         'Detalle de Impuesto',
            'Cant':        None,
            'Descripción': collect_imp(rw, 'codigo'),
            'Imp':         collect_imp(rw, 'alicuota'),
            'P.Unit':      collect_imp(rw, 'impuesto'),
            'P.Total':     collect_imp(rw, 'impuesto'),
        })

    return pd.concat([df_main, pd.DataFrame(imp_rows)], ignore_index=True)


def importar_facturas(archivos_pdf):
    COLUMNAS = ['Cod', 'Cant', 'Descripción', 'Imp', 'P.Unit', 'P.Total']
    df_pub, df_srv = [], []
    for f in archivos_pdf:
        df = _parse_factura(f)
        primera_cod = df['Cod'].iloc[0].replace(' ', '').lower()
        if 'serviciosdepublicidad' in primera_cod:
            df_pub.append(df)
        else:
            df_srv.append(df)
    df_publicidad = pd.concat(df_pub, ignore_index=True) if df_pub else pd.DataFrame(columns=COLUMNAS)
    df_servicios  = pd.concat(df_srv, ignore_index=True) if df_srv else pd.DataFrame(columns=COLUMNAS)
    return df_publicidad, df_servicios


def depurar_facturas(df):
    if df.empty:
        return df
    for col in ['P.Unit', 'P.Total']:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace('$', '', regex=False)
            .str.replace('.', '', regex=False)
            .str.replace(',', '.', regex=False)
            .pipe(pd.to_numeric, errors='coerce')
            .round(2)
        )
    df = df[df['P.Total'].abs() > 0.5].reset_index(drop=True)
    return df


# ─────────────────────────────────────────────
# CRUCES
# ─────────────────────────────────────────────

def cruce_publicidad_liquidacion(df_publicidad, df_liquidacion):
    TOLERANCIA = 0.5

    if df_publicidad.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), df_liquidacion.copy().reset_index(drop=True), {
            'match_pub': 0, 'match_liq': 0, 'falta_liq': 0, 'falta_pub': len(df_liquidacion)
        }

    disponibles_liq = list(df_liquidacion.index)
    idx_match_pub = []
    idx_match_liq = []

    mask_pub = df_publicidad['Cod'] == 'Servicios de Publicidad Index Accion Rappi'
    idx_pub  = df_publicidad[mask_pub].index.tolist()

    if idx_pub:
        suma_pub = df_publicidad.loc[idx_pub, 'P.Total'].sum()
        for j in disponibles_liq:
            if abs(abs(suma_pub) - abs(df_liquidacion.loc[j, 'Total'])) <= TOLERANCIA:
                idx_match_pub.extend(idx_pub)
                idx_match_liq.append(j)
                disponibles_liq.remove(j)
                break

    for i in df_publicidad[~mask_pub].index.tolist():
        val = abs(df_publicidad.loc[i, 'P.Total'])
        for j in disponibles_liq:
            if abs(val - abs(df_liquidacion.loc[j, 'Total'])) <= TOLERANCIA:
                idx_match_pub.append(i)
                idx_match_liq.append(j)
                disponibles_liq.remove(j)
                break

    match_publicidad  = df_publicidad.loc[idx_match_pub].reset_index(drop=True)
    match_liquidacion = df_liquidacion.loc[idx_match_liq].reset_index(drop=True)
    falta_pub1        = df_liquidacion.drop(index=idx_match_liq).reset_index(drop=True)
    falta_liq1        = df_publicidad.drop(index=idx_match_pub).reset_index(drop=True)

    stats = {
        'match_pub': len(match_publicidad),
        'match_liq': len(match_liquidacion),
        'falta_liq': len(falta_liq1),
        'falta_pub': len(falta_pub1),
    }
    return match_publicidad, match_liquidacion, falta_liq1, falta_pub1, stats


def cruzar_liquidacion_servicio(df_servicios, falta_publicidad1):
    TOLERANCIA = 0.5

    if df_servicios.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), falta_publicidad1.copy().reset_index(drop=True), {
            'paso1': 0, 'paso2': 0, 'paso3': 0,
            'match_srv': 0, 'match_liq': 0, 'falta_fac': len(falta_publicidad1), 'falta_liq2': 0
        }

    disponibles_srv = list(df_servicios.index)
    disponibles_liq = list(falta_publicidad1.index)
    idx_match_srv = []
    idx_match_liq = []

    # Paso 1: match 1 a 1
    for i in list(disponibles_liq):
        val = abs(falta_publicidad1.loc[i, 'Total'])
        for j in disponibles_srv:
            if abs(val - abs(df_servicios.loc[j, 'P.Total'])) <= TOLERANCIA:
                idx_match_liq.append(i)
                idx_match_srv.append(j)
                disponibles_liq.remove(i)
                disponibles_srv.remove(j)
                break

    paso1 = len(idx_match_liq)

    # Paso 2: IVA plataforma + Descuento vs 1 entrada
    resto_liq = falta_publicidad1.loc[disponibles_liq].copy()
    mask_iva = resto_liq['Valores'].str.contains('IVA Uso y alquiler de plataforma Rappi', na=False)
    mask_dsc = resto_liq['Valores'].str.contains('Descuento por inversión de Rappi a aplicar sobre el IVA Uso y alquiler', na=False)
    idx_iva_dsc = resto_liq[mask_iva | mask_dsc].index.tolist()

    paso2 = 0
    if idx_iva_dsc:
        suma_iva = resto_liq.loc[idx_iva_dsc, 'Total'].sum()
        for j in disponibles_srv:
            if abs(abs(suma_iva) - abs(df_servicios.loc[j, 'P.Total'])) <= TOLERANCIA:
                idx_match_liq.extend(idx_iva_dsc)
                idx_match_srv.append(j)
                disponibles_liq = [i for i in disponibles_liq if i not in idx_iva_dsc]
                disponibles_srv.remove(j)
                paso2 = len(idx_iva_dsc)
                break

    # Paso 3: suma por grupo
    resto_liq = falta_publicidad1.loc[disponibles_liq].copy()
    grupos_sum = resto_liq.groupby('Grupo')['Total'].sum()
    idx_match_srv2  = []
    idx_grupo_match = []

    for grupo, suma in grupos_sum.items():
        for j in disponibles_srv:
            if abs(abs(suma) - abs(df_servicios.loc[j, 'P.Total'])) <= TOLERANCIA:
                idx_match_srv2.append(j)
                idx_grupo_match.append(grupo)
                disponibles_srv.remove(j)
                break

    idx_liq_grupo = resto_liq[resto_liq['Grupo'].isin(idx_grupo_match)].index.tolist()

    todos_match_liq = idx_match_liq + idx_liq_grupo
    todos_match_srv = idx_match_srv + idx_match_srv2

    match_servicios    = df_servicios.loc[todos_match_srv].reset_index(drop=True)
    match_liquidacion  = falta_publicidad1.loc[todos_match_liq].reset_index(drop=True)
    falta_factura      = falta_publicidad1.drop(index=todos_match_liq).reset_index(drop=True)
    falta_liquidacion2 = df_servicios.drop(index=todos_match_srv).reset_index(drop=True)

    stats = {
        'paso1': paso1, 'paso2': paso2, 'paso3': len(idx_liq_grupo),
        'match_srv': len(match_servicios),
        'match_liq': len(match_liquidacion),
        'falta_fac': len(falta_factura),
        'falta_liq2': len(falta_liquidacion2),
    }
    return match_servicios, match_liquidacion, falta_liquidacion2, falta_factura, stats


# ─────────────────────────────────────────────
# FUNCIÓN PRINCIPAL
# ─────────────────────────────────────────────

def correr_rappi(archivo_liquidacion, archivos_pdf):
    df_liq = importar_liquidacion(archivo_liquidacion)
    df_liq = depurar_liquidacion(df_liq)

    df_pub_raw, df_srv_raw = importar_facturas(archivos_pdf)
    df_publicidad = depurar_facturas(df_pub_raw)
    df_servicios  = depurar_facturas(df_srv_raw)

    (match_pub, match_liq_pub,
     falta_liq1, falta_pub1, stats_pub) = cruce_publicidad_liquidacion(df_publicidad, df_liq)

    (match_srv, match_liq_srv,
     falta_liq2, falta_fac, stats_srv) = cruzar_liquidacion_servicio(df_servicios, falta_pub1)

    stats = {**stats_pub, **stats_srv}

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df_liq.to_excel(writer, sheet_name='Liquidación depurada', index=False)
        df_publicidad.to_excel(writer, sheet_name='Facturas publicidad', index=False)
        df_servicios.to_excel(writer, sheet_name='Facturas servicios', index=False)
        match_pub.to_excel(writer, sheet_name='Match publicidad', index=False)
        match_liq_pub.to_excel(writer, sheet_name='Match liq. publicidad', index=False)
        match_srv.to_excel(writer, sheet_name='Match servicios', index=False)
        match_liq_srv.to_excel(writer, sheet_name='Match liq. servicios', index=False)
        falta_fac.to_excel(writer, sheet_name='Sin factura', index=False)
        falta_liq2.to_excel(writer, sheet_name='Sin liquidación', index=False)
    buf.seek(0)
    return buf, stats
