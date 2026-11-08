# Proyecto Servienvios · cuadernos corregidos (versión del 20-10-2026, integrada con el repositorio)

## Qué reemplaza a qué

| Archivo que tenían | Qué hacer | Archivo nuevo |
|---|---|---|
| 0200_AnalisisExploratorio | Archivar (se entrega sin salidas como _ARCHIVAR) | 0210_AnalisisExploratorioV2 |
| 0300_CleanTransformParquet (repositorio) / _4 / _5 | Reemplazar | 0300_CleanTransformParquet_6 (minimización del repositorio + v5; pos. 38–45 desactivadas) |
| 0100_ExtraccionInformacion | Mantener (solo se borraron sus salidas) | + 0110_ExtraccionComplementaria (nuevo, no lo modifica) |
| 0150_ClimaBogota | Reemplazar | 0150_ClimaBogota_2 (solo descarga) + 0160_ClimaAlineado |
| 0400_ModeladoPrediccion_1 | Reemplazar; queda solo como antecedente | 0400_ModeladoPrediccion_2 (sin salidas) |
| 0410_EvaluacionTemporal_1 (del asesor) | Archivar: está contenido, sin cambios en sus funciones, en | 0410_EvaluacionTemporal_2 |
| — | Nuevo | 0420_Interpretabilidad |
| 0310, 0320, 0330, 0160 (del asesor) | Usar tal como los envió (probados, funcionan) | — |

Repositorio: reemplazar README.md y .gitignore por los de la carpeta REPOSITORIO, borrar READ.MD y MODULOS/__pycache__. Antes de subir a GitHub/OneDrive: borrar del repositorio las versiones antiguas de 0300 y 0400 **con salidas** (tienen datos personales; también en el historial de git).

## Orden de ejecución en Databricks

| Paso | Cuaderno | Qué configurar | Compuerta |
|---|---|---|---|
| 0 | 0110_ExtraccionComplementaria, paso 1 (en el equipo con SQL Server) | Ninguno | Lista de procesos: responde la pregunta 1 del asesor |
| 1 | 0300_CleanTransformParquet_6, hasta pos. 37 | Rutas y `SAL_CLIENTE` | Crea LOG_GUIAS_BOGOTA y LOG_BASE_BOGOTA; la pos. 16 muestra movimientos por mes (deben estar enero–agosto) |
| 2 | 0310_AuditoriaDesenlace | `SAL` (guardarla fuera del cuaderno y de GitHub) | Enviar la muestra de 60 guías a Servienvios |
| 3 | 0320_DiagnosticoFanout | Rutas | Nota: qué tabla y columna duplican filas |
| 4 | — | Responder las 3 preguntas del asesor y decidir A o B | Acta con el asesor |
| 5 | 0330_BaseAnaliticaV2 | `CAMPO_ENTREGA`, `INCUMPLE_SI_IGUAL`, `CORTE_OBSERVACION` | Controles: filas = guías; etiqueta en {0,1} |
| 6 | 0150_ClimaBogota_2 | Ninguno (necesita internet) | Crea CLIMA_BOGOTA_PUNTOS_HORARIO |
| 7 | 0160_ClimaAlineado | Ninguno | Revisar el diagnóstico de zona horaria |
| 8 | 0210_AnalisisExploratorioV2 | Columnas del calendario si no las detecta | Tablas del OE1 |
| 9 | 0410_EvaluacionTemporal_2, secciones 1–8 | `ALERTAS_POR_DIA` (con la empresa); primera corrida rápida: `N_CONFIGS_BUSQUEDA = 3`, `B_BOOTSTRAP = 200` | Revisar y aprobar la decisión (`DECISION_APROBADA = True`) |
| 10 | 0410_EvaluacionTemporal_2, sección 9 | `EJECUTAR_PRUEBA_FINAL = True` **una sola vez** | Se niega a repetirse |
| 11 | 0420_Interpretabilidad | Ninguno | Tablas del OE5 |

Todas las salidas quedan en `LOG_CONSOLIDADO/REVISION_2026_10/` (subcarpetas EDA, MODELOS, INTERPRETACION). Ninguna muestra guías individuales.

## Verificación hecha

La cadena completa (0310 → 0320 → 0330 → 0160 → 0210 → 0410_2 → 0420) se ejecutó de principio a fin con datos **sintéticos** que reproducen los nombres de columnas reales y defectos conocidos (regional duplicada, documentos repetidos, re-despachos, entregas sin hora). Se probó con Python 3.13/pandas 3 y con Python 3.11/pandas 2.2/scikit-learn 1.4 (versiones típicas de Databricks), con y sin clima, y con los cuatro tipos de modelo final. 0150 no se pudo probar (descarga de internet) y no cambió en su lógica. Ningún resultado proviene de los datos de Servienvios.
