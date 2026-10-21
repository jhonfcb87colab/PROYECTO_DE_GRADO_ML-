# Cuadernos v3 · SLA en días hábiles (21-10-2026)

Versiones nuevas; los originales no se modificaron. Todas las salidas van a `LOG_CONSOLIDADO/REVISION_2026_10/V3/`, así que la corrida v2 queda intacta. Los cuadernos van sin salidas.

## Orden de corrida

1. `0330_BaseAnaliticaV3`: primero el **Paso 0** (solo conteos: tipos de día del calendario, cobertura, qué cambia en las claves repetidas de RED). Si los textos de `TIPO_DIA` no son `DIA HABIL`, ajustar `CONDICION_DIA_HABIL`. Luego el resto: base, controles, sensibilidad, balance y manifiesto.
2. `0160_ClimaAlineado_2`.
3. `0210_AnalisisExploratorioV3`.
4. `0410_EvaluacionTemporal_3`: primero rápido (`N_CONFIGS_BUSQUEDA = 3`, `B_BOOTSTRAP = 200`) y después completo. Decisión y prueba final, una sola vez, como antes.
5. `0420_Interpretabilidad_2`.

`0300_CleanTransformParquet_7` solo hace falta si se vuelve a correr 0300.

## Qué cambia

| Cuaderno | Cambio |
|---|---|
| 0330 v3 | SLA en días hábiles (`DIM.VW_TIEMPO_CALENDARIO_Y_FESTIVOS` + `TIEMPO_ENTREGA_COMERCIAL_DIA`); vence al final del n-ésimo día hábil sin contar el de admisión. Una fila de RED por clave: la ruta activa más reciente. `origen_sla`: `SLA_UNICO`, `SLA_POR_REGLA`, `SLA_CON_SUSTITUCION`, `SIN_SLA`. Devoluciones (`ENTREGADO A REMITENTE`) y siniestros con etiqueta y última milla nulas. `guia` = md5(guía + sal), con la sal en `SEGURIDAD/sal_guia.txt` del Volume (se crea sola la primera vez; no subirla a GitHub). `ID_CLIENTE` (ya seudonimizado por 0300 v6) viaja en la base. Tabla de sensibilidad con 4 variantes más la regla v2 de horas continuas. |
| 0160_2 | Lee `GOLD_V3_ENVIO`, escribe `GOLD_V3_ENVIO_CLIMA`. |
| 0210 v3 | Celda de carga activa (la corrida del 20-10 falló por `NameError: dev`). Población: `SLA_UNICO` + `SLA_POR_REGLA`, sin devoluciones ni siniestros; imprime la sensibilidad con solo `SLA_UNICO`. Usa la `cargar_festivos` del equipo. |
| 0410_3 | Mismo modelado. Misma población que 0210, con `SOLO_SLA_UNICO` para la sensibilidad. Ya no cruza con `LOG_GUIAS_BOGOTA` por la guía real; las predicciones de validación guardan el seudónimo. Conserva `# !pip` y la `cargar_festivos` del equipo. |
| 0420_2 | Solo la carpeta `V3/`. |
| 0300_7 | Las vistas previas de las posiciones 21, 23 y 25 vuelven a `LIMIT 0` y muestran solo el conteo (en Databricks estaban en `LIMIT 1` y el HTML dejó ver una guía y su cliente). |

## Regla de lectura

La variante principal (días hábiles, sin contar el día de admisión, ruta activa más reciente) se fijó antes de ver la prevalencia real. Las otras variantes se reportan tal cual, y no se elige ninguna por quedar más cerca del 20 % que reporta la empresa.

## Prueba

Probados de punta a punta (0330 → 0160 → 0210 → 0410 con decisión y prueba final → 0420) con datos sintéticos. 0330 acierta los 11 casos armados a mano: sábado hábil, domingo y festivo que no cuentan, envío en domingo, ruta activa frente a inactiva más nueva, dos activas con plazos distintos, devolución, siniestro, abierto vencido y abierto no vencido. No se corrieron con datos reales.
