# Regresión

`esperado_<entidad>_<período>.json` guarda el saldo del mayor, el saldo final, cada
línea de la hoja `conciliacion` y la lista de hojas de la conciliación aprobada:

- EASA al 31/08/2026 (scope 23/02–06/09, 28 liquidaciones): saldo final 0,61
- Ronda al 31/08/2026 (mismo scope): saldo final 0,00

Los insumos (ZIP, facturas, mayores, Dean, HIO, Atalaya) no viajan con la skill
por tamaño. Con la carpeta de insumos armada según la convención de
`scripts/concilia.py`:

```
python scripts/concilia.py --carpeta <carpeta_easa>  --esperado tests/esperado_easa_2026-08.json
python scripts/concilia.py --carpeta <carpeta_ronda> --esperado tests/esperado_ronda_2026-08.json
```

El test compara con tolerancia de 1 peso y devuelve código 1 si alguna línea,
saldo u hoja no coincide. Correrlo después de tocar cualquier script.

Para comparar contra otro Excel (por ejemplo una conciliación hecha a mano):

```
python scripts/regresion.py --generado Conciliacion_nueva.xlsx --referencia Conciliacion_aprobada.xlsx
```

Cuando el usuario aprueba una conciliación nueva, se actualiza el esperado:

```
python scripts/regresion.py --guardar tests/esperado_easa_2026-09.json --referencia Conciliacion_PeYa_EASA_2026-09.xlsx
```
