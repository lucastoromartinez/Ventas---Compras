---
name: auditar-tesoreria
description: >
  Revisa con criterio humano el reporte_tesoreria.xlsx que produce
  correr_conciliacion_tesoreria (logica_tesoreria.py). No vuelve a correr
  el cruce ni lo modifica: lee la hoja "Auditoría" (los hallazgos
  determinísticos ya calculados) y las hojas "Falta Contabilidad" /
  "Falta Tesorería", y busca lo que un score de texto o una tolerancia
  numérica no puede ver por sí solos -nombres que son la misma persona
  aunque el texto no se parezca, errores de tipeo de un dígito, quién es
  quién cuando el tercero es genérico pero el comentario trae el nombre
  real-. Invocación explícita con /auditar-tesoreria, adjuntando el
  Excel del reporte. También aplica si el usuario adjunta un
  reporte_tesoreria.xlsx (o el nombre de archivo empieza con
  "reporte_tesoreria") y pide auditarlo, revisarlo, o encontrar qué
  falta sin haber tipeado el comando.
---

# Auditar Tesorería

## Qué es esto y qué NO es

Este es el segundo par de ojos sobre una conciliación que ya corrió.
`logica_tesoreria.py` decide los matches con reglas deterministas y
`auditoria_tesoreria.py` ya corrió cuatro chequeos automáticos sobre ese
resultado (reconciliación de totales, materialidad, asignaciones
ambiguas, patrones repetidos) — eso está en la hoja **"Auditoría"** del
Excel, calculado, no hay que rehacerlo.

El trabajo de esta skill es el paso siguiente, el que necesita criterio:

- Leer los hallazgos de "Auditoría" con contexto y decir cuáles importan.
- Mirar lo que quedó en "Falta Contabilidad" / "Falta Tesorería" con más
  flexibilidad que cualquier score de texto, buscando coincidencias que
  el código no pudo ver.

**Nunca edita el Excel ni el resultado del cruce.** No corre
`cruzar_caja` de nuevo, no cambia ninguna fila de "Match". Todo lo que
esta skill produce es una **propuesta para que la persona confirme**,
nunca un hecho consumado. Si algo resulta ser un patrón sistemático y
confirmado (no un caso aislado), se lo dice al usuario y sugiere llevarlo
como pedido de cambio a `logica_tesoreria.py` — eso lo decide el usuario,
la skill no toca código de matching por su cuenta.

## Procedimiento

### 1. Leer el archivo completo

Con la skill de xlsx (o `pd.read_excel` si hay entorno Python a mano),
leer **todas** las hojas: `Auditoría`, `Falta Contabilidad`,
`Falta Tesorería`, `Match Contabilidad`, `Match Tesorería`, `Revisar`,
`Neteos Contabilidad`, `Comisiones`, `Diferencia Arqueo`. Las hojas de
Match dan el contexto necesario para juzgar los hallazgos de Auditoría
(qué tipo de texto suele usar cada tercero, qué conceptos son comunes).

### 2. Recorrer la hoja "Auditoría", por categoría

**Materialidad** — para cada fila, mirar el `id`, ir a las hojas de
Match correspondientes (mismo `id`), leer `Comentario` y `Detalle`
completos (no solo lo que resume la hoja Auditoría) y juzgar: ¿la
diferencia es explicable (redondeo de tipo de cambio, centavos de una
tolerancia porcentual en una línea grande) o amerita preguntarle a
contabilidad? Las diferencias de más de un par de miles de pesos en
líneas chicas, o cualquier diferencia con signo y magnitud sospechosamente
"redonda" (exactamente 1.000, 5.000, 10.000), pesan más.

**Asignación ambigua** — estos son los casos más accionables. Cuando el
tercero es genérico (`AREA MARKETING`, `CRIS PROFETA` repetido) pero el
`Comentario` de esa fila de contabilidad trae el nombre real de la
persona (pasa seguido: el campo Comentario a veces es más específico que
el Tercero), comparar ese nombre contra el `Detalle` de tesorería de los
candidatos empatados y proponer la reasignación correcta, citando el
texto exacto de ambos lados. Si no hay forma de desambiguar con el texto
disponible, decirlo explícitamente en vez de forzar una propuesta.

**Patrón repetido** — mirar la lista de diferencias del grupo. Si
alternan de signo y de magnitud (ej. -5, +2, +4), es ruido de redondeo,
no un patrón real — decirlo así, no lo escales. Si son consistentemente
del mismo signo y una magnitud parecida, sí es candidato a un cargo
sistemático no contemplado (una comisión, una tasa) y amerita mencionarlo
como hallazgo para preguntar a contabilidad o tesorería.

### 3. Buscar en "Falta Contabilidad" y "Falta Tesorería" lo que el cruce no encontró

Con las dos tablas completas a la vista, buscar activamente:

- **Errores de tipeo o de captura**: mismo conjunto de dígitos en otro
  orden, un cero de más o de menos (multiplica o divide por 10 el
  importe), un signo invertido. Estos casi nunca están dentro de ninguna
  tolerancia razonable, así que el cruce determinista nunca los va a
  encontrar — es trabajo exclusivo de esta revisión.
- **Nombres que son la misma entidad sin parecerse como texto**: apodos
  (Charo → Rosario), un tercero registrado con la razón social completa
  contra un detalle que usa el nombre de fantasía, abreviaturas, o el
  mismo proveedor escrito con errores de tipeo distintos cada vez.
- **Relaciones de importe o fecha que un algoritmo ciego a contexto no
  ve**: por ejemplo, un conjunto de líneas sueltas de un lado que en
  conjunto arman un número redondo del otro lado, aunque cada una
  individualmente no cierre con nada.

Para cada candidato encontrado así, mostrar el razonamiento completo
(fecha, importe, texto de ambos lados, y por qué se cree que son la
misma operación) para que se pueda verificar en segundos, no solo
afirmar que matchea.

**Importante**: no inventar coincidencias para justificar cada línea de
falta. La mayoría de las veces la conclusión correcta y honesta es "no
encontré nada, parece un faltante real" — decirlo así, sin forzar una
propuesta débil solo para tener algo que reportar. Antes de proponer un
match, buscar también si ya se intentó por otros caminos (ver el
contexto de la conversación o el CLAUDE.md del repo si hay un historial
de intentos), para no repetir una búsqueda que ya dio negativo.

### 4. Entregar el informe

Formato: una lista priorizada, no un volcado de todas las filas.

```
## Hallazgos que ameritan confirmar

1. [reasignación] id=253 — el Comentario dice "Federico Cardone" pero
   quedó asignado a "Cris Profeta" en tesorería. Candidato correcto:
   la línea de tesorería del 17/06 "Federico Cardone" -800.000, hoy
   asignada a otro id ambiguo del mismo grupo.

2. [posible error de carga] Falta Contabilidad 13/08 -4.627.748 vs
   Falta Tesorería "Compra 29760 usd tc 1555" -46.277.748 (mismo día,
   mismos dígitos, un cero de diferencia — 90% de magnitud).

## Patrones a preguntar

3. [patrón repetido, confirmar con contabilidad] CUCCINET SA: 2 matches
   con diferencia de -4 pesos aprox cada uno — ¿hay un redondeo de tipo
   de cambio aplicado de un solo lado?

## Sin hallazgo

- Los 6 movimientos de la serie A3TZ que quedan en Falta Contabilidad
  (sin tercero cargado) no tienen candidato razonable en el remanente de
  tesorería, ni por importe ni por combinación de hasta 6 líneas. Parecen
  faltantes reales, no un problema de cruce.
```

Cerrar preguntando si alguno de los hallazgos de "reasignación" o "error
de carga" se confirma, y si algún "patrón repetido" resulta sistemático
tras confirmarlo -en ese caso, ofrecer proponer el ajuste correspondiente
a `logica_tesoreria.py` como paso siguiente, no aplicarlo de una.
