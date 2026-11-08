# Seudonimización (respuesta a la P9 de Iván, 25-10-2026)

## Qué se usó en la corrida

| Identificador | Dónde | Cómo | Dónde está la sal |
| --- | --- | --- | --- |
| `ID_CLIENTE` | `0300_CleanTransformParquet_8`, pos. 9 | `md5(ID_CLIENTE || SAL_CLIENTE)` en DuckDB | Escrita en el cuaderno (`SAL_CLIENTE = "CAMBIAR_ESTA_SAL"`) |
| `guia` | `0330_BaseAnaliticaV3_3` | `md5(guía || sal)` en DuckDB | `SEGURIDAD/sal_guia.txt` en el Volume, fuera de Git |
| `guia_seud` | `0310_AuditoriaDesenlace` (solo auditoría) | `md5(guía || SAL)` | Escrita en el cuaderno |

El número de guía real, nombres, teléfonos y direcciones no salen de 0300 (columnas en `NULL` o fuera de la selección).

## El problema

`md5(valor || sal)` es un hash sin clave. Quien conozca la sal puede recalcular el seudónimo de cualquier guía o cliente que ya tenga y comprobar si está en la tabla. En 0300 la sal del cliente está en el propio cuaderno, así que para quien lea este repositorio ese md5 no protege nada. Además md5 es rápido de calcular, lo que facilita probar valores por fuerza bruta cuando el espacio de identificadores es pequeño, como el de clientes.

## Qué se hace ahora

`0500_SeudonimizacionHMAC` (con `MODULOS/seudonimo.py`) toma cada tabla por guía que vaya a salir del equipo y reemplaza `guia` e `ID_CLIENTE` por `HMAC-SHA256(clave, valor)`:

- La clave tiene 32 bytes aleatorios. Se lee de un secreto de Databricks (`proyectogrado/clave_hmac`) o de `SEGURIDAD/clave_hmac.txt` en el Volume. Nunca se imprime, no va a Git y no se entrega con las tablas.
- Sin la clave no se puede recalcular ni comprobar ningún seudónimo, aunque se conozca la sal del md5.
- La misma clave da el mismo seudónimo en todas las tablas, así que los cruces entre tablas compartidas siguen funcionando.
- Se aplica sobre el md5 que ya está en la tabla. **No cambia ningún resultado** y no obliga a volver a correr el pipeline.
- Controles: mismo número de valores distintos antes y después (sin colisiones), ningún valor original en la copia y ninguna columna de datos personales.

## Si el pipeline se vuelve a correr desde cero

Lo recomendable es seudonimizar con HMAC-SHA256 desde el origen y no depender de 0500:

1. En 0300, quitar `SAL_CLIENTE` del cuaderno y calcular `ID_CLIENTE` con HMAC en pandas (`MODULOS/seudonimo.py`, función `seudonimizar`) sobre los clientes distintos, o registrar `hmac_sha256` como función de DuckDB con `con.create_function`.
2. En 0330, hacer lo mismo con la guía en lugar de `md5(guia || sal)`.
3. En 0310, igual para `guia_seud`.

No se hizo para esta entrega porque cambiaría los identificadores con los que se corrieron 0411 a 0440 y obligaría a repetir la prueba final, que por diseño se corre una sola vez.

## Pendiente fuera del código

- Autorización escrita de Servienvios para usar los datos en el trabajo de grado y para compartir tablas seudonimizadas con el jurado.
- Repositorio privado mientras no exista esa autorización.
