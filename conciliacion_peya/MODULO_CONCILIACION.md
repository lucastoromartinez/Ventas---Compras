# Módulo de conciliación — Cuenta recaudación PedidosYa

Especificación técnica para integrar el motor de conciliación en una aplicación.
El motor ya existe y está probado (scripts Python en `scripts/`). Este documento
explica qué hace, qué recibe, qué devuelve y qué reglas no se pueden cambiar.

> Objetivo del proceso: partir del saldo del mayor contable de la cuenta
> "Recaudación PedidosYa" y llegar a **cero (tolerancia 1 peso)**, explicando cada
> peso con un concepto trazable a un registro original. Todo es **determinístico**:
> mismos archivos de entrada producen exactamente el mismo Excel, celda por celda.

---

## 1. Contenido del paquete

```
conciliacion-peya/
├── MODULO_CONCILIACION.md      este documento
├── SKILL.md                    instrucciones operativas (formato agente)
├── requirements.txt
├── config/entidades.yaml       todo lo que cambia entre entidades
├── references/criterios.md     criterios contables (signos, reintegros, descuentos…)
├── scripts/
│   ├── concilia.py             orquestador: corre todo de punta a punta
│   ├── armar_peya.py           etapa 1: ZIP + PDF → archivos PeYa normalizados
│   ├── cruzar.py               etapa 2: cruce PeYa vs sistemas internos
│   ├── conciliar.py            etapa 3: cadena de conceptos, controles, histórico
│   ├── mensual.py              arrastre del cierre anterior (lo usa conciliar.py)
│   ├── generar_conciliacion.py etapa 4: Excel final
│   ├── comun.py                config, lectores de Dean / HIO / Atalaya, helpers
│   ├── regresion.py            compara una conciliación contra lo aprobado
│   └── validar_liquidaciones.py  utilitario: ecuación de cada liquidación
└── tests/
    ├── README.md
    ├── esperado_easa_2026-08.json   cierre inicial EASA aprobado
    ├── esperado_ronda_2026-08.json  cierre inicial Ronda aprobado
    ├── esperado_easa_2026-09.json   cierre mensual EASA septiembre
    └── esperado_ronda_2026-09.json  cierre mensual Ronda septiembre
```

## 2. Dependencias

| Componente | Versión probada | Uso |
|---|---|---|
| Python | 3.11+ (probado en 3.13) | |
| pandas | 3.0 | todo |
| numpy | 2.x | todo |
| openpyxl | 3.1 | lectura de xlsx/xls de PeYa y escritura del Excel |
| PyYAML | 6.x | `config/entidades.yaml` |
| poppler-utils (`pdftotext`) | 24.x | lectura de PDFs de liquidación y facturas |

`pdftotext` es un binario del sistema (no pip). En Windows: poppler para Windows
en el PATH. Los ZIP de PeYa traen archivos `.xls` que en realidad son xlsx; se leen
con `engine="openpyxl"` (no hace falta xlrd).

## 3. Entidades

`config/entidades.yaml` define cada entidad. Agregar una entidad = copiar un bloque;
los scripts no se tocan.

| | EASA | Ronda |
|---|---|---|
| Local PeYa (en nombres de ZIP/facturas) | 411335 | 409994 |
| Sistemas internos | Dean (D&D) + HIOffice | HIOffice + Atalaya |
| Cruce por identificador | Dean: Nº Transacción · HIO: Localizador | HIO: Localizador |
| Cruce sin identificador | HIO: fecha + local + monto bruto | HIO: fecha + local + BASE · Atalaya: fecha + criterio de facturación, 2ª pasada con BASE redondeada a la centena |
| Apertura del histórico | por mes | por mes **y local** (9 locales) |

Claves relevantes por sistema en el YAML: `sucursal_peya` (regex sobre la Sucursal
de la Lista de PeYa), `cruce_secundario`, `redondeo_ticket`, `mayor` (texto que
identifica sus ventas en el mayor), `etiqueta_online` / `etiqueta_efectivo`
(plantillas del nombre de la línea de ventas, **con las mismas palabras que usa el
mayor**), `lineas` (nombres de las líneas de cruce) y `hojas` (nombres de hojas).

## 4. Entradas

Convención de carpeta (el orquestador la resuelve sola):

```
<carpeta>/
├── zips/        estados de cuenta semanales de PeYa (*_estado-de-cuenta_<local>.zip), sin huecos
├── facturas/    PDFs de facturas PEDIDOSYA (pto. vta. 0026) y PAGOS YA (0013)
├── *recaud*.xlsx    mayor de la cuenta recaudación, COMPLETO desde el origen
├── *dean*.xlsx      ventas Dean                         (EASA)
├── *documento*.xlsx reporte HIO por documento (trae localizador)
├── *metodo*.xlsx    reporte HIO por método de pago (fuente de referencia de ventas)
├── *atalaya*.xlsx   tickets Atalaya                     (Ronda)
├── anterior/        conciliación del mes anterior (Excel generado por este motor)
└── salida/          lo genera el motor
```

Archivos `~$*.xlsx` (bloqueos de Excel abierto) se ignoran.

### Columnas que se leen

| Archivo | Columnas usadas |
|---|---|
| Mayor | `Asiento`, `Apunte`, `Fecha`, `Comentario`, `Debe`, `Haber`, `Su factura` (los encabezados pueden traer espacios al final) |
| Dean | `Servicio` (filtra "PedidosYa"), `Fecha` (dd/mm/aaaa hh:mm:ss), `Nº Transacción`, `Online`, `Efectivo`, `Nº Factura` |
| HIO método | serie/número, fecha, medio de pago (se filtran los que contienen "PEDIDOS"; "EFECTIVO" → efectivo), total |
| HIO documento | serie/número, fecha, localizador, venta, establecimiento |
| Atalaya | `Fecha`, `Medio Pago` (se filtran los que contienen "pedidos ya" o "peya"; puede venir con todos los medios), `CON IVA` |
| ZIP PeYa | Lista de pedidos, Reclamos, Cancelaciones, Reintegros (dos formatos: .xls por concepto u hojas de un mismo archivo) + PDF de liquidación |

Los encabezados se buscan normalizados (sin acentos, minúsculas, sin símbolos), así
que toleran variaciones menores. Los identificadores (Nº pedido, localizador, Nº
transacción) se pasan a texto sin decimales antes de cruzar.

### Modos

- **Cierre inicial**: sin `anterior/`. Procesa todo el rango de ZIPs.
- **Cierre mensual** (lo habitual): con `anterior/`. Ver sección 7.

## 5. Uso

```bash
python scripts/concilia.py --carpeta <carpeta> --entidad easa|ronda [--anterior <xlsx>] [--esperado tests/<json>]
```

`--entidad` se deduce del número de local en los ZIP si se omite.
Archivos sueltos en vez de carpeta: `--zips --facturas --mayor --dean --hio-documento --hio-metodo --atalaya --salida`.

Código de salida distinto de 0 si alguna etapa falla o si `--esperado` no coincide.

## 6. Pipeline

```
ZIPs + PDFs ──armar_peya──► trabajo/archivos_peya.xlsx, liquidaciones.json, facturas.json
                                │
Dean / HIO / Atalaya ──cruzar──► trabajo/cruces.pkl, cruce_<sistema>.xlsx
                                │
Mayor + conciliación anterior ──conciliar (+ mensual)──► trabajo/conceptos.json, _controles.json, det/*.csv
                                │
                       generar_conciliacion──► salida/Conciliacion_PeYa_<ENTIDAD>_<AAAA-MM>.xlsx
```

### Etapa 1 — `armar_peya.py`
- Extrae los ZIP, une Lista de Pedidos / Reclamos / Cancelaciones / Reintegros
  (renombra encabezados del formato nuevo al viejo).
- De cada PDF de liquidación (vía `pdftotext`) lee: ventas netas, ventas fuera de
  app, reintegro por cancelaciones, descuentos, retención SIRTAC, ajustes, cargos
  adicionales, aminoración, acreditación.
- De cada PDF de factura lee número y total, y lo asocia al período por nombre de archivo.

### Etapa 2 — `cruzar.py` (por sistema)
1. **Cruce por identificador**, uno a uno (Dean: Nº Transacción; HIO: Localizador).
2. Tickets sin pedido que tienen un **reintegro** (pedido rechazado: PeYa lo saca de
   la Lista) salen del pool antes del cruce 2.
3. **Anulaciones por sistema**: ticket + contra-ticket por el mismo importe salen del pool.
4. **Cruce por fecha (+ local) + importe**, uno a uno, con el criterio del YAML.
   Atalaya: segunda pasada con el importe redondeado hacia abajo a `redondeo_ticket`.
   Dean no tiene cruce 2 (siempre integra; fecha+monto produce matches espurios).
5. Atalaya (sin identificador): reintegros por fecha + monto entre los tickets sin pedido.

Scope: los pedidos se acotan a las fechas de las liquidaciones; **los reintegros se
toman todos los que traen los ZIP del scope** (un pedido de la semana anterior puede
reintegrarse en esta). Los reportes internos se guardan también completos
(`ventas_todo`, `tickets_todo`) para la venta del mes y los papeles de trabajo.

Desempates: siempre el primero en el orden del archivo. Determinístico mientras los
reportes se exporten igual.

### Etapa 3 — `conciliar.py`
Arma la cadena de líneas desde el saldo del mayor. Bloques, en orden:

**A. Ajustes de registración** (mayor vs liquidaciones)
- asientos "otros" que no son venta/acreditación/factura/retención: si el importe
  coincide con una retención de liquidación → "Retención IIBB … Duplicada"; si no →
  "Ajuste sin respaldo en liquidaciones" (pares asiento/reversión se cancelan)
- acreditaciones: asignación de liquidaciones **consecutivas** a cada asiento por
  programación dinámica (minimiza Σ|asiento − liquidaciones|). Diferencias de un bloque
  que vuelve a cero = redondeo; si no, "Acreditación registrada de más/menos"
- facturas: por número en `Su factura` (acepta 0026↔0023, 0013↔0023); pagada por otro
  importe → "pagada con diferencia"; pendiente cuyo importe coincide con un pago
  huérfano → "registrada como NNNN en el mayor" (número mal tipeado)
- ventas internas vs mayor, mes por mes y por sistema (ver venta del mes, sección 7)
- diferencia HIO reporte método vs documento

**B. Devengamiento**: ventas internas de los primeros días del mes siguiente que
entran en la última liquidación, por sistema y medio de pago.

**C. Pendientes al cierre**: acreditaciones no cobradas, facturas no registradas,
facturas no aportadas (estimadas por la ecuación), retenciones no registradas, ventas
con pago fuera de la app (acumulativo), descuentos facturados en otra semana, cargos
adicionales, aminoración, más las líneas arrastradas del cierre anterior.
→ **Subtotal: residuo operativo.**

**D. Cruces por sistema**: diferencia en matcheados, match 2do cruce, falta sistema
(PeYa liquidó, no hay ticket), falta PeYa (ticket sin pedido), NC por complemento
(venta − reintegro), ND reintegro 50% sin comprobante, ND reintegro 100% (recupero de
cargo por reclamo). En cierre mensual cada línea = valor nuevo + acumulado anterior.

**→ SALDO FINAL.** Lo que no se puede nombrar va a "PENDIENTE DE DEFINICIÓN", fuera
del saldo. **Nunca se mete un importe para que cierre.**

Signos: importe con signo de efecto sobre el saldo. Debe (+) = falta en el mayor;
Haber (−) = sobra en el mayor. Ver `references/criterios.md` §6.

### Etapa 4 — `generar_conciliacion.py`
Escribe el Excel desde `conceptos.json`. El saldo de cada línea es **fórmula**
(`=saldo_anterior + Debe − Haber`). Líneas deducidas o pendientes de definición
van en naranja (`flag`).

## 7. Cierre mensual — reglas fijas

Implementadas en `mensual.py` + `conciliar.py`. Son el contrato del módulo:

1. **ZIP y facturas arrancan donde terminó el cierre anterior** (semana siguiente a
   su última liquidación) y llegan hasta la semana que termina en los primeros días
   del mes siguiente. Los primeros días del mes ya se cruzaron en el cierre anterior.
2. **Reportes internos desde el 1° del mes**, solo para la venta del mes. Si el mayor
   no tiene ventas de ese mes → se genera la línea con las palabras del mayor
   (`Ventas D&D 09-2026 (online TC)`, `Ventas 09-2026 HIO PY efectivo`…) por el mes
   completo del reporte, por sistema y medio. Si las tiene → se compara y se genera
   "registradas de más/menos". **Control 2b**: el tramo devengado en el cierre anterior
   se compara contra el reporte nuevo; si difiere, línea propia.
3. **Asientos nuevos** = los que no están en la hoja `Recaudacion` de la anterior
   (clave: asiento + apunte + fecha + debe + haber). **No se filtra por fecha**: las
   facturas pendientes suelen registrarse con fecha del mes anterior. Los asientos ya
   conciliados no vuelven a entrar en la cadena; las ventas se controlan sobre el mayor completo.
4. **Pendientes anteriores**:
   - facturas: por número; si no, por importe exacto (→ línea informativa
     "registrada como …"); las "No aportada", por importe
   - acreditaciones: se ponen delante de las liquidaciones nuevas y se asignan
     contra los asientos de acreditación nuevos
   - retenciones: por importe
   - lo que no aparece se arrastra en naranja
5. **Acreditación duplicada**: asiento de acreditación nuevo con el mismo importe y
   comentario que uno ya conciliado → se revierte (Debe) y no entra en la asignación.
6. **Diferencias de cruce**: nuevas + acumulado anterior. La hoja "Diferencias por mes"
   se trae entera; si los nombres anteriores no coinciden con el YAML se normalizan, y
   si el total de un concepto no ata con su línea anterior la diferencia va a la
   columna "Reclasif. cierre dd/mm/aaaa".
7. **Residuo anterior**: si el SALDO FINAL anterior no era 0,00 exacto, se arrastra su
   contrapartida ("Residuo del cierre anterior (redondeo)").
8. **Conceptos acumulativos** (ventas fuera de app, descuentos en otra semana,
   aminoración, cargos adicionales): el importe anterior se suma a la línea nueva.
9. **Cualquier otra línea anterior** se arrastra tal cual salvo que un asiento nuevo
   la corrija por el mismo importe con signo contrario.

## 8. Controles (en `_controles.json` y en consola)

1. **Ecuación de cada liquidación**: ventas netas + reintegros + ajustes =
   acreditación + facturas + SIRTAC + ventas fuera de app.
2. **Ventas internas vs mayor**, mes por mes y sistema; **2b** devengado anterior vs reporte nuevo.
3. **Reintegros**: suma y cantidad de casos de las líneas = liquidaciones.
4. **Verificación independiente**: ventas internas − (ventas netas + reintegros +
   ajustes) = residuo nuevo de la cadena = lo que explican los cruces.
5. **Facturas por número** contra el mayor (pendientes, con diferencia, tipeadas, huérfanas).
6. **Acreditaciones**: qué liquidaciones cayeron en cada asiento y cuáles quedan pendientes.

Un cierre es válido cuando SALDO FINAL ≈ 0 (±1 peso) **y** el control 4 coincide.

`_controles.json` (ejemplo EASA 2026-09):
```json
{
 "scope": ["2026-09-07", "2026-10-04"],
 "liquidaciones": 4,
 "liq_no_cierran": [],
 "ventas_vs_mayor": [],
 "devengado_vs_reporte": [],
 "acreditaciones": {"liq": 41663311.91, "mayor": 26190835.94, "pendientes": ["14/09-20/09", "21/09-27/09", "28/09-04/10"]},
 "facturas": {"pendientes": ["0026-02830513", "..."], "con_diferencia": [], "tipeadas": [], "huerfanas": []},
 "reintegros": {"lineas": 289147.51, "liquidaciones": 289147.51, "casos": 28},
 "verificacion": {"ventas_internas": 40085334.7, "peya": 39144865.48, "residuo_independiente": 940469.22, "cruces": 940469.22},
 "saldo_mayor": 33522248.07,
 "saldo_final": 0.0
}
```

## 9. Salida — Excel `Conciliacion_PeYa_<ENTIDAD>_<AAAA-MM>.xlsx`

| Hoja | Contenido |
|---|---|
| `conciliacion` | Concepto / Debe / Haber / Saldo (fórmula) / Observaciones. Saldo del mayor → bloques A-D → SALDO FINAL → PENDIENTE DE DEFINICIÓN → nota de verificación |
| `Diferencias por mes` | una fila por concepto de cruce (Ronda: y local), una columna por mes, columna "Reclasif." si corresponde, Acumulado. Perdura entre cierres |
| `Recaudacion` | el mayor nuevo completo (de acá sale la clave de "asientos nuevos" del próximo cierre — **no modificar**) |
| `Venta <sistema>` | reporte interno del mes completo; HIO = combinado documento+método |
| `PeYa Lista Pedidos`, `Liquidaciones` | mes + primeros días del siguiente |
| `Match …`, `Falta …`, `Reintegros …` | papeles de trabajo del cruce |

Papeles de trabajo en cierre mensual: solo el mes y los primeros días del siguiente.
Columna `Cierre` (`AAAA-MM`): de qué cierre viene cada fila (los primeros días del mes
se traen de las hojas del cierre anterior).

**El Excel generado es el input del cierre siguiente**: la app debe guardarlo tal cual.
`mensual.py` lee de él la hoja `conciliacion` (líneas, saldo final, fecha y scope del
subtítulo "Saldo del mayor al dd/mm/aaaa · Scope de liquidaciones dd/mm/aaaa al dd/mm/aaaa"),
`Diferencias por mes`, `Recaudacion` y las hojas de detalle.

## 10. Determinismo

- Sin aleatoriedad, sin fechas del sistema, sin intervención manual.
- Verificado: dos corridas desde cero producen las 17 hojas idénticas celda por celda.
- Tolerancias fijas: 1 peso para resolver pendientes y para el cierre; 0,005 para
  "al peso" en controles de ventas/devengado.
- Desempates por orden de archivo (cruces) y por orden cronológico (asientos).

## 11. Regresión

```bash
python scripts/regresion.py --generado nueva.xlsx --esperado tests/esperado_easa_2026-09.json
python scripts/regresion.py --generado nueva.xlsx --referencia aprobada.xlsx
python scripts/regresion.py --guardar tests/esperado_<ent>_<per>.json --referencia aprobada.xlsx
```
Compara saldo del mayor, saldo final, cada línea (±1 peso), hojas y filas.
Los insumos de prueba no viajan en el paquete (tamaño); hay que tenerlos aparte.

Resultados de referencia:

| Cierre | Saldo mayor | Saldo final | Liquidaciones |
|---|---|---|---|
| EASA 2026-08 (inicial) | 83.178.161,80 | 0,61 | 28 |
| Ronda 2026-08 (inicial) | −1.556.352,77 | 0,29 | 28 |
| EASA 2026-09 (mensual) | 33.522.248,07 | 0,00 | 4 |
| Ronda 2026-09 (mensual) | −26.068.909,25 | 0,00 | 4 |

## 12. Sugerencias de integración

- Envolver `concilia.py` en una función de servicio:
  `conciliar(entidad, carpeta_insumos, conciliacion_anterior) -> {excel, controles, saldo_final}`.
  Hoy las etapas se comunican por archivos en `salida/trabajo/`; se puede mantener
  (simple, auditable) o pasar a llamadas en memoria importando `main()` de cada script.
- Guardar por entidad y período: el Excel generado (input del próximo cierre),
  `_controles.json` y la carpeta `trabajo/` (auditoría).
- Validaciones previas recomendadas en la UI: ZIPs consecutivos sin huecos, una
  factura por semana (dos si hay PAGOS YA), mayor que contenga todos los asientos de
  la `Recaudacion` anterior, reportes internos que empiecen el 1° del mes.
- Mostrar al usuario: controles (sección 8), la cadena, y aparte las líneas en naranja
  (arrastres y deducidos) y "pendiente de definición". Esas son las que requieren una
  decisión humana; el cálculo no cambia con esa decisión.
- No reimplementar los criterios contables: leer `references/criterios.md` antes de
  tocar reintegros, signos o descuentos.
