# Criterios de conciliación PedidosYa

Contenido:
1. Mecánica de PedidosYa
2. La regla que explica casi todos los casos raros
3. Criterio de reintegros
4. NC por el complemento vs por el total
5. Reintegros al 100% con cargo por reclamo
6. Tabla de signos
7. Cuándo un match dudoso importa y cuándo no
8. Nombres del cruce 2 de HIO
9. El formato de los ZIP cambia en el medio
10. El descuento de PeYa a usuarios no se resta en los pagos en la app
11. El medio de pago del ticket no sirve para filtrar el match
12. Tickets con reintegro: salen del pool antes del cruce 2
13. Anulaciones por sistema: no hay NC que emitir
14. Lo que explicaba el residuo "sin identificar" de EASA
15. Acreditaciones: los asientos no respetan las semanas
16. Dónde aparece la discordancia entre los dos reportes de HIO

---

## 1. Mecánica de PedidosYa

Identidad que cumple cada liquidación semanal:

```
Ventas netas + Reintegros = Acreditación + Facturas + Retención SIRTAC + Ventas fuera de app
```

- La **acreditación neta** se deposita ~11 días después del cierre de semana.
- Las **facturas** se pagan por transferencia aparte y van al Haber de la cuenta recaudación.
  Suelen ser dos: PEDIDOSYA (pto. vta. 0026) y PAGOS YA (0013, servicio de pago en línea).
- Los **cargos por reclamos** viajan dentro de las facturas, en su propio renglón.
- Las **ventas con pago fuera de la app** las cobró el local; PeYa no las paga.

Por el desfasaje de ~11 días, las liquidaciones de la última quincena se acreditan al mes
siguiente. Eso no es una diferencia: es timing, y va como pendiente.

**Si una liquidación no cierra, falta un comprobante.** El caso típico es la factura de
PAGOS YA, que a veces no se descarga porque es chica. Si además el mayor registró la
acreditación por un importe mayor al real, probablemente ya esté compensado y no haya que
ajustar nada.

---

## 2. La regla que explica casi todos los casos raros

**Cuando un pedido se rechaza, PedidosYa lo saca de la Lista de Pedidos** y lo manda a
Reintegros.

Consecuencia: un pedido facturado internamente puede no aparecer en ninguna hoja del cruce.
No es integrador apagado ni error de clave — es el comportamiento normal de PeYa.

Por eso **la Lista de Pedidos no sirve como criterio para clasificar reintegros**. Si se usa,
todos los pedidos rechazados quedan mal clasificados.

Si al correr los controles de cobertura aparecen ventas internas "sin cruzar", buscarlas en
Reintegros antes de crear una categoría residual. Casi siempre son estas.

---

## 3. Criterio de reintegros

La pregunta correcta es: **¿emitimos comprobante interno?**

### Búsqueda del comprobante, en orden

1. Identificador de transacción en el sistema con integrador confiable → hay comprobante
2. Localizador en el sistema de tickets → hay comprobante
3. Sin localizador: fecha + importe en el sistema de tickets → candidato
4. Nada → no hay comprobante

**El paso 3 solo aplica donde el integrador puede fallar.** En un sistema que siempre integra
(Dean), si no matchea por Nº de transacción, no está facturado y punto. Buscar por fecha+monto
ahí produce matches espurios — incluso contra tickets de otro local.

Simétricamente: **la ausencia de match por fecha+monto no prueba ausencia de comprobante**. Si
el integrador estaba caído, el ticket existe pero su importe puede no coincidir con el monto
del pedido. Tratar la ausencia como certeza mientras se trata la presencia como "presunto" es
al revés de lo que corresponde.

Para confirmar que el integrador estaba caído: mirar si *todos* los tickets de ese local en
esa fecha tienen localizador vacío.

### Tratamiento

| Situación | Tratamiento |
|---|---|
| Hay comprobante y el ticket **no** está en Falta PeYa | NC por el complemento |
| Hay comprobante y el ticket **sí** está en Falta PeYa | Nada extra — ver punto 7 |
| No hay comprobante | ND por el reintegro |
| Reintegro al 100% con cargo por reclamo | Recupero de gasto — ver punto 5 |

---

## 4. NC por el complemento vs por el total

En pedidos rechazados PeYa suele reintegrar el **50%**. Si la venta se facturó entera, hay
dos formas de ajustarla:

**Por el complemento** (venta − reintegro): deja la venta neta igual a la acreditación en un
solo movimiento. Homologa directo.

**Por el total**: anula la venta completa, pero después hay que hacer un segundo asiento que
reconozca el reintegro contra otros ingresos. Si ese asiento se omite, la cuenta queda
descolgada por el importe del reintegro, porque el reintegro se acredita igual.

**Los dos dan el mismo saldo en la conciliación.** La diferencia es de registración, y el
complemento es preferible porque no depende de que nadie se acuerde del segundo asiento.

El saldo es ciego a esta elección. Conviene decirlo cuando se plantea, para que quede claro
que no se está acomodando el número.

---

## 5. Reintegros al 100% con cargo por reclamo

Circuito distinto: la venta sale en la Lista por el total y se acredita normalmente. Después
aparece en Reclamos (cargo) y en Reintegros (100%), y ambos se cancelan entre sí.

**No hay venta que ajustar**, pero la línea debe figurar en la conciliación, porque el cargo
por reclamo ya está restado vía facturas y quedaría sin contrapartida.

Imputación sugerida (confirmar con el contador):

```
Debe   Recaudación PeYa          X
Haber  Gastos - Cargos reclamos  X   (recupero)
```

Nombrarla **"Recupero de cargos por reclamos"**, no "reintegro a registrar": con el nombre
equivocado parece un asiento nuevo y el usuario la va a rechazar con razón.

Atención: reintegro y cargo pueden caer en **semanas distintas** — el reintegro va por fecha
de pedido y el cargo por fecha de reclamo. Se compensan a lo largo del tiempo pero pueden
quedar patas sueltas en los bordes del período.

Este circuito puede existir en un sistema y no en otro. No asumirlo simétrico.

---

## 6. Tabla de signos

La regla: **¿en qué lado sobra o falta respecto del Debe?**

| Concepto | Significado | Va al |
|---|---|---|
| Falta PeYa (sobra interno) | Ticket interno sin pedido en PeYa | **Haber** — sobra en el Debe |
| Falta interno (sobra PeYa) | PeYa liquidó, no se ticketeó | **Debe** — falta en el Debe |
| Diferencia en matcheadas | Registrado de más (descuento sin aplicar) | **Haber** |
| Reintegro ND | Ingreso sin venta previa | **Debe** |
| NC complemento | Anula parte de una venta facturada | **Haber** |

Estas dos primeras se confunden fácil y el error vale el doble (invierte el signo). Si el
usuario anotó "diferencia que no se le encuentra sentido" al lado de alguna, empezar por ahí.

---

## 7. Cuándo un match dudoso importa y cuándo no

Si el ticket de un reintegro **ya está dentro de "Falta PeYa"**, su venta ya se restó ahí.
Sumar el reintegro como ND da:

```
Falta PeYa + ND   = −venta + reintegro
NC complemento    = −(venta − reintegro) = −venta + reintegro
```

Idéntico. **Validar ese match no cambia el saldo** — solo define si corresponde emitir NC,
que es una decisión fiscal y puede resolverse después.

Vale la pena verificarlo antes de pedirle al usuario que investigue: si el ticket está en
Falta PeYa, no hace falta.

El único escenario problemático es que el ticket exista pero **no** esté en Falta PeYa,
porque habría matcheado con otro pedido y la venta quedaría sin restar. Ese sí hay que
chequearlo.

---

## 8. Nombres del cruce 2 de HIO

En el pipeline heredado las hojas del segundo cruce estan invertidas: la que se
llama `Math Peya2` trae tickets de HIO (columnas `Serie / Número`, `Venta`) y la
que se llama `Match HIO2` trae pedidos de PeYa (`Número de pedido`,
`Monto de Venta Neta ($)`).

Viene de que en el codigo original `falta_doc` son los **pedidos de PeYa** sin
ticket y `falta_peya` son los **tickets de HIO** sin pedido — al reves de lo que
sugieren los nombres.

`cruzar.py` ya escribe cada lado en la hoja que le corresponde:

| Hoja | Contenido |
|---|---|
| `Match HIO 2 (Lado PeYa)` | pedidos de PeYa matcheados por fecha+monto |
| `Match HIO 2 (Lado HIO)` | tickets de HIO matcheados por fecha+monto |
| `Falta HIO (sobra PeYa)` | pedidos de PeYa sin ticket |
| `Falta PeYa (sobra HIO)` | tickets de HIO sin pedido |

Al leer archivos generados por el pipeline viejo, **verificar el contenido por sus
columnas antes de confiar en el nombre de la hoja.**

---

## 9. El formato de los ZIP cambia en el medio

PedidosYa cambio como entrega los conceptos dentro del ZIP:

| Formato | Contenido del ZIP |
|---|---|
| Viejo | un `.xls` por concepto: `estado-de-cuenta`, `reintegros`, `cargos-por-reclamos`, `cargos-por-cancelaciones`. Cada uno con una sola hoja (`Sheet1`) |
| Nuevo | un solo `.xls` de estado de cuenta con varias hojas: `Lista de Pedidos`, `Cargos por reclamos`, `Reintegros`, a veces `Cargos por cancelaciones` |

`armar_peya.py` contempla los dos. La regla: si el nombre del archivo ya dice el
concepto (reintegros, reclamos, cancelaciones), todo el archivo va ahi; solo en el
estado de cuenta se mira hoja por hoja, porque ahi conviven los cuatro.

**Buscar archivos `*reintegros*.xls` sueltos hace parecer que los periodos nuevos
no traen reintegros.** En el caso real eso daba 115 reintegros en vez de 215 y 458
reclamos en vez de 861, sin que nada fallara: el cierre simplemente no cerraba y la
diferencia aparecia como residuo sin nombre.

---

## 10. El descuento de PeYa a usuarios no se resta en los pagos en la app

La venta debería registrarse como PeYa calcula su venta neta:

```
Venta = Monto bruto
      − Descuento comercial neto otorgado x PeYa × 1,21
      − Descuento otorgado por el local
      − Cupón otorgado por el local
      − Descuento neto otorgado PeYa a usuarios × 1,21
```

Los descuentos propios se restan tal cual; los de PeYa vienen netos de IVA y por
eso el ×1,21. Cualquier descuento nuevo sigue la misma regla.

**Lo que se factura hoy omite el último término en los pagos en la app.** Verificado
en Ronda sobre los casos donde las dos fórmulas difieren:

| | Bien (restó) | Mal (no restó) |
|---|---:|---:|
| HIO — pago en la app | 0 | 279 |
| HIO — pago fuera de la app | 4 | 0 |
| Atalaya — pago en la app | 0 | 93 |
| Atalaya — pago fuera de la app | 8 | 0 |

Sin excepciones en ninguno de los dos sistemas. Sobrefacturación del período:
$5.907.970,77.

**Para cruzar** hay que usar el criterio con el que se factura (BASE en los pagos
en la app, la fórmula completa en los que van fuera), porque es lo que dice el
ticket. **Para conciliar**, la diferencia entre las dos fórmulas es un concepto
propio: venta registrada de más, va al Haber.

Un dato que valida el factor: PeYa trae ese mismo importe ya con IVA en la columna
"Descuentos PedidosYa a cobrar", y coincide al centavo con el descuento × 1,21.

### El integrador apagado no cambia el criterio

En los pedidos que van al segundo cruce (sin localizador), el importe registrado
coincide con BASE en 42 de 42. Lo que cambia sin localizador es la fragilidad del
match, no cómo se factura.

### Los descuentos que se anulan entre sí

En algunos períodos el descuento comercial, el del local y el cupón son cero en
todos los pedidos. Ahí BASE se reduce al bruto y los dos criterios dan idéntico:
**esos datos no pueden decidir entre uno y otro**. Usar BASE igual, porque si en
otro mes aparece un cupón la bruta empieza a fallar sin aviso.

---

## 11. El medio de pago del ticket no sirve para filtrar el match

Tentador pero equivocado: el medio de pago que informa el sistema interno y el
método de pago que informa PeYa coinciden solo en el 95%, y usarlo como filtro
adicional **empeora** el match.

En Ronda, sobre 797 matches por fecha e importe:

| | Interno no-efectivo | Interno efectivo |
|---|---:|---:|
| PeYa en app | 722 | 17 |
| PeYa fuera de app | 23 | 35 |

40 casos cruzados, más un tercer medio ("PEDIDOS YA TARJETA") que no entra en la
dicotomía. Dejarlo como columna informativa para revisar dudosos, no como criterio.

---

## 12. Tickets con reintegro: salen del pool antes del cruce 2

Un ticket interno cuyo localizador figura en Reintegros es un pedido rechazado:
PeYa lo sacó de la Lista. Si ese ticket queda en el pool del cruce 2, matchea por
fecha + local + importe contra **otro** pedido del mismo monto, y entonces:

- el pedido verdadero de ese otro ticket queda en "Falta interno",
- el ticket rechazado aparece como "homologado" con un pedido que no es el suyo,
- y el reintegro queda sin su venta (NC por el complemento imposible de armar).

Por eso `cruzar.py` aparta esos tickets después del cruce 1 y antes del cruce 2.
Con eso el cruce reproduce exactamente los resultados del pipeline del usuario
(EASA: 889 pares en el cruce 2, 322 Falta HIO, 398 Falta PeYa, 56 tickets explicados).

Un reintegro cuyo ticket **sí** matcheó en el cruce 1 (el pedido sigue en la Lista)
no lleva NC: la venta ya está homologada contra un pedido liquidado, y el reintegro
es un ingreso sin venta → ND.

---

## 13. Anulaciones por sistema: no hay NC que emitir

Algunos locales (Nacha, Hell's) anulan el ticket por sistema: queda el ticket
original y un contra-ticket negativo por el mismo importe. Si el original tiene
reintegro, la venta neta interna ya es cero: **no corresponde NC por el
complemento**, y el reintegro va como ND (ingreso sin venta).

`cruzar.py` aparea cada ticket explicado por reintegro con un negativo del mismo
local e importe, fecha igual o posterior, y saca el par del pool (hoja "Anulados
por sistema"). Si no se hace, "Falta PeYa" se infla con el negativo y la línea de NC
pide emitir una nota de crédito sobre una venta que ya no existe. El saldo no cambia
(−venta + reintegro en los dos casos), la diferencia es qué se le pide al usuario.

---

## 14. Lo que explicaba el residuo "sin identificar" de EASA

La conciliación de EASA al 31/08/2026 cerraba con 63.143,42 sin identificar.
Aplicando a EASA los mismos criterios que a Ronda, el residuo se descompone en
tres cosas con nombre y baja a 0,61:

| Concepto | Importe | Dónde estaba |
|---|---:|---|
| Diferencia HIO reporte método vs documento (8 tickets: 5 de Nacha en marzo por 12.000 y 3 de Hell's) | 15.500,00 | Las ventas salían del método y los cruces del documento; nadie lo lineaba |
| Ajustes de liquidación: cargos adicionales −45.401,14 y aminoración +28.476,33 | −16.924,81 | Entraban en la ecuación de cada semana pero no en la cadena |
| Ticket Dean 2239317910 del 15/08 (fact. B 0055-00010523) sin pedido en PeYa y sin reintegro | 30.718,00 | El pipeline lo daba por "cubierto" y no iba a ninguna línea |

Regla general que sale de acá: **todo lo que entra en la ecuación de la liquidación
tiene que tener su línea**, y toda transacción interna del scope tiene que estar en
match, en falta o explicada por un reintegro. "Cubierto" no es una categoría.

---

## 15. Acreditaciones: los asientos no respetan las semanas
16. Dónde aparece la discordancia entre los dos reportes de HIO

El mayor registra las acreditaciones como llegan al banco: un asiento puede
agrupar varias semanas, y dos asientos pueden partir una semana. Comparar asiento
por asiento contra liquidaciones da diferencias que se compensan entre sí y no son
errores.

`conciliar.py` asigna liquidaciones consecutivas a cada asiento buscando la
partición que minimiza las diferencias, y después junta en un bloque las
diferencias consecutivas: si el bloque vuelve a cero antes del siguiente asiento
exacto, era un tema de agrupación y se absorbe como redondeo; si no, es una
diferencia real y se nombra con el rango de semanas que abarca ("Acreditación
Registrada de más 09/03-15/03"). Lo que queda sin asignar al final son las
acreditaciones pendientes de cobro.

---

## 16. Dónde aparece la discordancia entre los dos reportes de HIO

El reporte **por método de pago** es el que coincide con el mayor (de ahí salen las
ventas registradas). El reporte **por documento** es el que trae el localizador y
alimenta los cruces. Cuando los dos difieren para el mismo Serie/Número, la venta
registrada y la venta homologada no son el mismo importe, y eso tiene que tener
línea propia: "Diferencia HIO reporte método vs documento", con una hoja que los
lista uno por uno.

El patrón de los casos reales (8 en EASA por 15.500, 1 en Ronda por 400):

- todos son comprobantes **FC N** de serie con letra **G** (8 de 356 en EASA, 1 de 12 en Ronda);
- todos son **PEDIDOS YA EFECTIVO** y todos **sin localizador**;
- el método **siempre informa más** que el documento, y siempre un número redondo
  (20.000 contra 19.000, 23.000 contra 22.800, 30.000 contra 26.900);
- al no tener localizador, entran al cruce 2 y matchean por fecha + local + importe
  **del documento**: la venta homologada es la del documento y la registrada es la
  del método.

Compatible con un error de caja en efectivo (se carga el billete recibido y no el
vuelto). No decidirlo por cuenta propia: preguntarle al usuario cuál reporte manda,
porque de eso depende si hay que corregir la venta registrada o el ticket.
