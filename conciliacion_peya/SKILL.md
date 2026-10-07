---
name: "conciliacion-peya"
description: Concilia la cuenta de recaudación de PedidosYa de cualquier entidad (EASA, Ronda) partiendo del saldo del mayor hasta llegar a cero. Arma los archivos de cruce desde los ZIP de estado de cuenta, cruza contra los sistemas internos de cada entidad (Dean, HIOffice, Atalaya) y produce un Excel línea por línea con cada factura, cada acreditación pendiente y cada grupo de reintegros, más la apertura por mes (y por local en Ronda). INVOCACIÓN EXPLÍCITA — ejecutar el workflow completo únicamente cuando el usuario escriba "concilia peya", "conciliá peya", "concilia PeYa", "concilia easa", "concilia ronda" o "/concilia_peya easa" o "/concilia_peya ronda". Si el usuario sube archivos de PedidosYa (zips de estado de cuenta, facturas de Delivery Hero, reportes de Dean, HIO o Atalaya, mayor de recaudación) sin dar ese comando, confirmar la recepción y esperar; no conciliar por cuenta propia.
---

# Conciliación cuenta recaudación PedidosYa

Llevar el saldo de la cuenta desde lo que dice el mayor hasta cero, explicando
cada peso con un concepto trazable a registros originales. Cero es cero con
tolerancia de 1 peso: los centavos no se persiguen, pero se nombran para que no
se acumulen cierre tras cierre.

## Disparo y argumentos

**Correr el workflow solo con el comando explícito.** El usuario suele subir los
archivos en varias tandas; al recibir archivos sin comando: acusar recibo, decir
qué llegó y qué falta, y esperar.

```
/concilia_peya easa                      cierre inicial de EASA
/concilia_peya ronda 2026-09             cierre mensual de Ronda del período 2026-09
/concilia_peya ronda --carpeta RUTA    insumos en una carpeta con la convención de abajo
```

- **entidad**: `easa` o `ronda`. Si se omite se deduce del número que traen los
  ZIP (411335 = EASA, 409994 = Ronda); si hay duda, preguntar.
- **período** (`AAAA-MM`): el mes que se cierra. Si hay conciliación del mes
  anterior (adjunta o en `anterior/` de la carpeta) es cierre mensual; si no,
  cierre inicial.
- **carpeta**: con el usuario conectado por Cowork, la carpeta reemplaza la
  subida de archivos. Los archivos no pasan por el contexto: los scripts los
  leen en disco.

## Entidades

`config/entidades.yaml` define lo que cambia entre una y otra. Los scripts lo
leen con `--entidad`; para una entidad nueva se copia un bloque.

| | EASA | Ronda |
|---|---|---|
| Sistemas | Dean + HIO | HIO + Atalaya |
| Cruce por identificador | Nº Transacción · Localizador | Localizador |
| Cruce sin identificador | fecha + local + bruta (HIO) | fecha + local + BASE (HIO) · fecha + criterio de facturación (Atalaya, con segunda pasada por la BASE redondeada a la centena: `redondeo_ticket: 100`, desde el 11/09/2026) |
| Locales | 3 | 9 |
| Apertura del histórico | por mes | por mes **y local** |
| Reintegros al 100% con reclamo | sí (Dean, HIO) | no existen |

## Entradas

| Insumo | Entidad | Qué es |
|---|---|---|
| ZIPs de estado de cuenta | ambas | uno por semana, sin huecos; traen los xls y el PDF de la liquidación |
| PDFs de facturas | ambas | PEDIDOSYA (pto. vta. 0026) y PAGOS YA (0013) |
| Mayor de recaudación | ambas | los asientos de la cuenta |
| `hio_metodo` + `hio_documento` | ambas | el reporte por método es la **fuente de referencia** de ventas; el documento trae localizador |
| `Dean_*.xlsx` | EASA | transacción, fecha, online, efectivo, Nº factura |
| `atalaya_*.xlsx` | Ronda | tickets con fecha, medio de pago, CON IVA (sin localizador); puede venir con todos los medios de pago, el lector se queda con los de PedidosYa |
| Conciliación del mes anterior | cierre mensual | entra como input; de ahí salen los pendientes y los acumulados |

Convención de carpeta (`scripts/concilia.py` la resuelve sola):

```
CARPETA/zips/  facturas/  *recaud*.xlsx  *dean*.xlsx  *documento*.xlsx  *metodo*.xlsx  *atalaya*.xlsx  anterior/  salida/
```

Las facturas de PAGOS YA son chicas y se saltean fácil: si falta una, la ecuación
de esa semana no cierra y el control lo marca.

## Dos modos

**Cierre inicial** — no hay conciliación previa. Se procesa todo el rango de ZIPs.
Es lo que se corrió una vez para EASA y Ronda (23/02 al 06/09/2026).

**Cierre mensual** — lo habitual de ahora en más. Reglas fijas (las aplican los
scripts, nada se decide a mano):

1. **ZIP y facturas arrancan donde terminó el cierre anterior** (semana siguiente a
   su última liquidación) y llegan hasta la semana que termina entrados los
   primeros días del mes siguiente. Los primeros días del mes ya se cruzaron en el
   cierre anterior y no se vuelven a cruzar.
2. **Dean / HIO / Atalaya vienen desde el 1° del mes** solo para la venta del mes.
   Antes de generar la línea se verifica que el mayor no tenga ventas de ese mes;
   si las tiene, no se genera y se compara (registradas de más / de menos). Si no,
   la línea lleva **las mismas palabras del mayor** (`Ventas D&D 09-2026 (online TC)`,
   `Ventas 09-2026 HIO PY efectivo`...) por el mes completo del reporte, por sistema
   y medio. El tramo de los primeros días se controla contra lo devengado en el
   cierre anterior (control 2b); si difiere, línea propia en naranja.
3. **Asientos nuevos** = los que no están en la hoja `Recaudacion` de la anterior
   (asiento + apunte + fecha + debe + haber), sin filtrar por fecha: las facturas
   pendientes suelen registrarse con fecha del mes anterior. Solo un asiento nuevo
   resuelve un pendiente.
4. **Pendientes anteriores**: facturas por número (las "No aportada" por importe);
   si están en el mayor, salen. Acreditaciones pendientes van delante de las
   liquidaciones nuevas y se asignan contra los asientos de acreditación nuevos
   (el mayor agrupa semanas). Retenciones por importe. Lo que no aparece se
   arrastra en naranja.
5. **Diferencias de cruce** (match, faltas, NC, ND): las nuevas se suman a las
   anteriores; `Diferencias por mes` viene entera, con cada importe en su mes.
6. **Hojas**: `Recaudacion` trae el mayor nuevo. Los papeles de trabajo traen solo
   el mes y los primeros días del siguiente: lo cruzado ahora más los movimientos
   de los primeros días del mes, traídos de las hojas del cierre anterior. La
   columna `Cierre` dice de qué cierre viene cada fila.
7. Si el SALDO FINAL anterior no era cero exacto, se arrastra su contrapartida como
   "Residuo del cierre anterior (redondeo)".
8. Una factura pendiente que no aparece por número pero sí por importe exacto entre
   las facturas nuevas se da por pagada, con la línea informativa "registrada como
   NNNN en el mayor" (número mal tipeado).
9. Un asiento de acreditación nuevo con el mismo importe y comentario que uno que ya
   estaba en la Recaudación anterior es una acreditación duplicada: se revierte
   (Debe) en naranja y no entra en la asignación.
10. La hoja "Diferencias por mes" anterior se lleva a los nombres del YAML. Si el
    total de un concepto no ata con su línea del cierre anterior (otra
    descomposición en cierres viejos), la diferencia queda en la columna
    "Reclasif. cierre dd/mm/aaaa" para que cada fila sume lo que dice la línea.
11. Los reintegros se toman por la liquidación que los trae (los ZIP del scope), no
    por la fecha del pedido: un pedido de la semana anterior reintegrado en esta
    va como ND (su ticket ya quedó como "Falta PeYa" en el cierre anterior).

La conciliación anterior es **input, no memoria**. `scripts/mensual.py` documenta
qué hace con cada línea.

## Workflow

```
python scripts/concilia.py --carpeta CARPETA [--entidad easa|ronda] [--anterior Conciliacion_ant.xlsx]
```

Equivale a correr en orden:

1. `armar_peya.py` — ZIPs → `archivos_peya.xlsx` (Lista Pedidos, Reclamos,
   Cancelaciones, Reintegros), `liquidaciones.json`, `facturas.json`. Contempla
   los dos formatos de ZIP.
2. `cruzar.py --entidad` — por sistema: cruce por identificador, apartado de los
   tickets con reintegro, cruce por fecha + importe, anulaciones por sistema.
   Deja `cruce_SISTEMA.xlsx` y `cruces.pkl`.
3. `conciliar.py --entidad --mayor [--anterior]` — toda la cadena, deterministica:
   controles, ajustes de registración, devengamiento, pendientes, cruces y
   reintegros, histórico. Deja `conceptos.json` y `det/`.
4. `generar_conciliacion.py` — el Excel.
5. `regresion.py --esperado tests/...` — opcional, compara con lo aprobado.

Lo único que no decide un script es la lectura del resultado: qué queda en
"pendiente de definición", qué preguntas van al usuario, qué pasa con los
asientos marcados en naranja. Leer `references/criterios.md` antes de opinar
sobre reintegros, signos o descuentos.

## Estructura del Excel

`Conciliacion_PeYa_ENTIDAD_AAAA-MM.xlsx`, hoja `conciliacion` con columnas
**Concepto / Debe / Haber / Saldo / Observaciones**, saldo arrastrado con fórmula:

```
Saldo                                          (del mayor)
--- ajustes de registración ---
Retencion IIBB dd/mm-dd/mm Duplicada
Ajuste sin respaldo en liquidaciones: ASIENTO
Acreditación Registrada de más/menos dd/mm-dd/mm
Diferencia de redondeo en acreditaciones registradas
Servicio Pago en Linea (No facturado) dd/mm-dd/mm
Factura NNNN-NNNNNNNN pagada con diferencia
Factura NNNN registrada como NNNN en el mayor
Diferencia de redondeo en facturas registradas
Ventas SISTEMA MM-AAAA registradas de más/menos
Ventas SISTEMA MM-AAAA (medio)               venta del mes sin asiento, palabras del mayor
Ventas SISTEMA dd/mm al dd/mm — reporte nuevo vs devengado en el cierre anterior
Diferencia HIO reporte método vs documento
--- devengamiento ---
Ventas dd/mm al dd/mm SISTEMA (medio)       una por sistema y medio de pago
--- pendientes al cierre ---
Acreditacion dd/mm-dd/mm                      una por liquidación no cobrada
Factura Pendiente NNNN-NNNNNNNN               una por factura no registrada
Factura Pendiente dd/mm-dd/mm (No aportada)   estimada por la ecuación, en naranja
Ventas con pago por fuera de la aplicación ya cobradas
Retencion dd/mm-dd/mm
Diferencia Descuentos facturados en semana distinta
Cargos / Reintegros adicionales de liquidación
Aminoración descuento comercial
(líneas arrastradas del cierre anterior, en naranja)
Residuo del cierre anterior (redondeo)
    >>> Subtotal — residuo operativo a explicar
--- cruces, por sistema ---
Diferencia SIST match · match 2do cruce · Falta SIST · Falta PeYa sobra SIST · NC complemento · ND 50% · ND 100%
SALDO FINAL
PENDIENTE DE DEFINICIÓN                        (fuera del cierre)
```

Signos: Debe = falta en el mayor (ventas no registradas, PeYa liquidó y no se
ticketeó, reintegro sin venta); Haber = sobra en el mayor (acreditación o factura
pendiente, ticket sin pedido, NC por complemento, venta registrada de más).
Las NC van por el **complemento** (venta − reintegro). Lo deducido y no
verificado va en naranja (`flag`).

### Hoja "Diferencias por mes"

Una fila por concepto de cruce (en Ronda, por concepto **y local**), una columna
por mes, más el acumulado. Los valores llevan el signo de efecto sobre el saldo
(Debe +, Haber −), así cada fila suma lo que dice la línea de la conciliación.
Perdura entre cierres: en cierre mensual se unen las columnas anteriores con la
del mes nuevo.

### Hojas de detalle

| EASA | Ronda |
|---|---|
| Recaudacion · Venta Dean · Venta Combinada HIO · PeYa Lista Pedidos · Liquidaciones | Recaudacion · Venta HIO · Venta Atalaya · PeYa Lista Pedidos · Liquidaciones |
| Match Dean PeYa · Falta Dean · Falta PeYa (sobra Dean) · Reintegros Dean | Match HIO 1 · Match HIO 2 (Lado PeYa) · Match HIO 2 (Lado HIO) · Falta HIO (sobra PeYa) · Falta PeYa (sobra HIO) · Reintegros HIO |
| Match HIO 1 · Match HIO 2 (Lado PeYa) · Match HIO 2 (Lado HIO) · Falta HIO (sobra PeYa) · Falta PeYa (sobra HIO) · Reintegros HIO | Match Atalaya · Falta Atalaya (sobra PeYa) · Falta PeYa (sobra Atalaya) · Reintegros Atalaya |

Más **Dif HIO método vs documento**: un renglón por ticket donde los dos reportes
de HIO difieren, con fecha, establecimiento, localizador, medio de pago, los dos
importes y la diferencia. Es la apertura de esa línea de la conciliación.

En cierre mensual llevan el mes y los primeros días del siguiente, con columna `Cierre`. La hoja de venta de HIO es el
**combinado** (documento filtrado por los medios PEDIDOS YA del reporte por
método), con las columnas `Medio Pago` e `Importe por metodo`: el total de `Venta`
es lo que usan los cruces y el de `Importe por metodo` es lo que ata al mayor.
Las hojas de reintegros traen **Venta interna**, **Tratamiento** (NC complemento /
ND sin comprobante / Recupero cargo reclamo) y **NC** (el complemento).
`cruce_SISTEMA.xlsx` en la carpeta de trabajo trae además "Tickets c-pedido
cancelado" y "Anulados por sistema".

## Principio que ordena el trabajo

> Un cierre en cero apoyado en una línea sin explicación no es un cierre.

Si aparece una diferencia que no se puede nombrar, va a "pendiente de definición"
**fuera del saldo**, nunca repartida dentro de otra línea. El residuo de EASA que
antes quedaba "sin identificar" (63.143,42) se explicó aplicando los mismos
criterios que a Ronda: método vs documento, ajustes de liquidación y un ticket
interno sin pedido (criterios.md §14). Si vuelve a aparecer un residuo, buscar
primero qué entra en la ecuación de la liquidación y no tiene línea.

## Controles de integridad

Los corre `conciliar.py` y los imprime; mirarlos antes que la cadena:

1. **Ecuación de cada liquidación**: ventas netas + reintegros + ajustes =
   acreditación + facturas + SIRTAC + ventas fuera de app. Si no cierra con
   sobrante, falta un comprobante; con faltante, descuento facturado en otra
   semana o NC faltante.
2. **Ventas internas vs mayor, mes por mes y por sistema**, al peso. Un mes sin
   asientos genera la venta del mes con las palabras del mayor. **2b**: tramo
   devengado en el cierre anterior vs reporte nuevo.
3. **Reintegros**: la suma de las líneas y la cantidad de casos = total de las
   liquidaciones.
4. **Verificación independiente**: ventas internas − (ventas netas + reintegros
   + ajustes) debe coincidir con el residuo operativo de la cadena, y los cruces
   deben explicarlo. Si los dos caminos coinciden, el cierre es real.
5. **Cada factura contra el mayor, por número**: detecta las pagadas por otro
   importe (típico: sin la percepción de IIBB) y los números mal tipeados.
6. **Cobertura**: cada transacción interna del scope está en match, en falta o
   explicada por un reintegro. "Cubierto" no es una categoría.

## Presentar el resultado

Mostrar primero los controles, después la cadena, y por separado lo que queda
abierto y lo que está en naranja. Las decisiones que dependen del criterio del
usuario (qué reporte manda cuando método y documento difieren, si una factura
"pagada con diferencia" es percepción de IIBB) se plantean como preguntas, no se
resuelven por cuenta propia.

Cuando el usuario corrija un criterio, recalcular y decir si el saldo se movió.
Muchas reclasificaciones no lo mueven (NC complemento vs Falta PeYa + ND, por
ejemplo) y conviene decirlo para que no parezca que se está acomodando el número.

## Errores frecuentes

1. Ventas internas subregistradas en el tramo del mes siguiente.
2. Debe que no entra en la fórmula del saldo.
3. Snapshots viejos duplicados junto a la fila recalculada.
4. Etiquetas equivocadas: un importe correcto con nombre errado impide saber su signo.
5. Acreditación "registrada de más" que en realidad compensa una factura no registrada.
6. Asientos duplicados por reapertura de período.
7. Signos invertidos en las faltas cruzadas.
8. Plugs heredados de cierres anteriores que nadie nombró.
9. Facturas pagadas sin la percepción de IIBB, o con el número mal tipeado.
10. Tickets con reintegro que se cuelan en el cruce 2 y matchean con otro pedido.
11. NC pedida sobre una venta que el local ya anuló por sistema.
12. En cierre mensual, filtrar los asientos nuevos por fecha: las facturas pendientes
    se registran con fecha del mes anterior y quedan como "sigue sin registrar".
13. Asignar las liquidaciones contra todos los asientos de acreditación del mayor en
    vez de solo los nuevos.
14. Arrastrar el residuo anterior con el signo del saldo: la línea es su contrapartida.

## Archivos

- `config/entidades.yaml` — sistemas, claves, nombres de líneas y hojas por entidad
- `references/criterios.md` — mecánica de PeYa, reintegros, signos, descuentos, anulaciones
- `scripts/concilia.py` — corre todo (carpeta o archivos sueltos)
- `scripts/armar_peya.py` · `cruzar.py` · `conciliar.py` · `generar_conciliacion.py`
- `scripts/mensual.py` — arrastre del cierre anterior
- `scripts/regresion.py` + `tests/` — comparación contra lo aprobado
