# Predicción de tiempos de entrega e incumplimiento de SLA · Servienvios (Bogotá)

Universidad Santo Tomás · Facultad de Estadística · Maestría en Ciencia de Datos · Trabajo de grado

John Fernando Cruz Becerra · Oscar Javier Huertas Campo · Mauricio Alejandro Gaviria Alzate

## Qué hace

Construye una base analítica con **una fila por guía** (envíos con destino Bogotá, procesos 5, 6 y 9, enero a agosto de 2026) y predice, en el momento de la **primera salida a zona urbana (T0)**:

- el **tiempo de última milla** (horas de T0 a la entrega efectiva), y
- la **probabilidad de incumplir el SLA**, con el plazo de la ruta contado en días hábiles.

La evaluación es temporal: validación de origen móvil en abril, mayo y junio, y una **prueba final única** en julio y agosto, corrida el 24-10-2026 (hora de Bogotá). Las fechas y el número de guías de cada partición están en [`entrega/particiones.csv`](entrega/particiones.csv).

## Estructura

| Carpeta | Qué hay |
| --- | --- |
| raíz | Cuadernos vigentes (`.ipynb`, sin salidas), numerados en el orden de corrida |
| `MODULOS/` | Funciones que importan los cuadernos. Debe quedar **junto a los cuadernos** (en la misma carpeta del workspace) |
| `entrega/` | Archivos para el documento y el asesor: particiones y tabla zona→localidad versionada. Solo conteos y nombres de zona, sin guías |
| `herramientas/` | Script que arma la tabla zona→localidad v2 desde la v1 de 0430 |
| `docs/` | Seudonimización, limpieza del historial y documentos anteriores (`docs/historial/`) |
| `archivo/` | Versiones reemplazadas que se conservan como antecedente. **No usar para resultados** |

Para usarlos en Databricks: *Workspace → Import → File* con cada `.ipynb`, o abrir el repositorio como *Git folder*. `MODULOS/` va en la misma carpeta que los cuadernos.

## Orden de corrida

Todas las rutas parten de `PATH_DATABRICKS = "/Volumes/proyectogrado/source/source/"` y las salidas quedan en `LOG_CONSOLIDADO/REVISION_2026_10/V3/`. En Databricks serverless se usa `%pip install "duckdb<2"` (la versión 2 quitó `fetchone`).

| # | Cuaderno | Dónde | Depende de | Qué produce |
| --- | --- | --- | --- | --- |
| 1 | `0100_ExtraccionInformacion` | Equipo con SQL Server | Variables `SQL_SERVIDOR` y `SQL_BASE` (ver `MODULOS/conexion_sql.py`) | Parquet de guías, movimientos (procesos 5, 6 y 9), documentos y paramétricas, por lotes mensuales |
| 2 | `0110_ExtraccionComplementaria` (opcional) | Equipo con SQL Server | 0100 | Diagnóstico de procesos; movimientos de entrega y meses faltantes |
| 3 | `0300_CleanTransformParquet_8` | Databricks | 0100 subido al Volume | `LOG_GUIAS_BOGOTA` y `LOG_BASE_BOGOTA`, sin datos personales; `ID_CLIENTE` seudonimizado |
| 4 | `0310_AuditoriaDesenlace` | Databricks | 0300 | Validación de la fuente de la entrega efectiva |
| 5 | `0320_DiagnosticoFanout` | Databricks | 0300 | Diagnóstico de la multiplicación de filas en las uniones |
| 6 | `0330_BaseAnaliticaV3_3` | Databricks | 0300 | `GOLD_V3_ENVIO` (una fila por guía, SLA en días hábiles, `guia` seudonimizada) y la promesa calendario como sensibilidad |
| 7 | `0150_ClimaBogota_2` | Databricks (con internet) | — | Clima horario de Bogotá (Open-Meteo, reanálisis) |
| 8 | `0160_ClimaAlineado_3` | Databricks | 0330, 0150 | `GOLD_V3_ENVIO_CLIMA` (clima de horas anteriores a T0) |
| 9 | `0210_AnalisisExploratorioV3` | Databricks | 0330 | Caracterización (OE1) |
| 10 | `0340_VerificacionSupuestos` | Databricks | 0330 | Verificación de supuestos con los datos (`VERIFICACION/P6_*`); control: 49,4 % de incumplimiento |
| 11 | `0350_RutaVigenteAlAdmitir` | Databricks | 0330 | Sensibilidad del plazo con la ruta vigente al admitir (`P6r_*`) |
| 12 | `0411_Preparacion_Modelado` | Databricks | 0160 | Base de modelado, historia sin futuro por bloque y configuración (`MODELADO/`) |
| 13 | `0412_Modelos_Base` · `0413_RandomForest` · `0414_XGBoost` · `0415_LightGBM` | Databricks | 0411 | Validación de cada modelo (independientes entre sí) |
| 14 | `0416_Comparacion_PruebaFinal` | Databricks | 0412–0415 | Comparación, H1/H2, decisión (`P4b_decision.json`) y **prueba final, sección 7, una sola vez** |
| 15 | `0420_Interpretabilidad_4` | Databricks | 0416 | Permutación, SHAP y lectura operativa (OE5) |
| 16 | `0417_Sensibilidad` | Databricks | 0411, 0416 (sección 1) | Hueco p99 y regresión con todas las guías, solo en validación (`P4c_*`) |
| 17 | `0418_EfectoSabado` | Databricks | 0413, 0416, 0420 | Efecto sábado en validación (`SABADO/P9_*`) |
| 18 | `0419_ArchivoParticiones` | Databricks | 0411 | `P10_particiones.csv`; se detiene si no coincide con los conteos de 0411/0416 |
| 19 | `0430_AsignacionZonas` | Databricks | 0340, capas de IDECA en `ESPACIAL/capas/` | Zona → sector catastral → UPZ (`ESPACIAL/P7_*`) y matrices de vecindad |
| 20 | `0440_MoranResiduos` | Databricks | 0411, 0416 (sección 1), 0430 | I de Moran de los residuos de validación por zona×mes (`ESPACIAL/P8_*`) |
| 21 | `0500_SeudonimizacionHMAC` | Databricks | Tablas que se vayan a compartir | Copias con `guia` e `ID_CLIENTE` en HMAC-SHA256 (`COMPARTIR/`) |

Los pasos 4, 5, 9, 10, 11 y 16 a 20 no cambian la base de modelado ni la prueba final; se pueden correr en cualquier momento después de sus dependencias.

**La prueba final no se repite.** La sección 7 de 0416 tiene una guarda que la detiene si ya existen los archivos `P4b_prueba_final_*`.

## Archivos de `entrega/`

| Archivo | Qué es |
| --- | --- |
| `particiones.csv` | Fechas (inicio incluido, fin excluido) y guías de cada partición, por tarea. Cifras tomadas de las salidas de 0411 (celdas 19 y 26) y 0416 (celdas 10 y 41) de la corrida del 24/25-10-2026. 0419 las regenera y las comprueba |
| `zona_localidad_v1.csv` | Tabla que dejó 0430 el 25-10-2026, sin cambios |
| `zona_localidad_v2.csv` | v1 con la localidad completada desde la UPZ, 9 correcciones manuales y la marca de revisión cruzada. Ubica el 61,3 % de las guías en una localidad |
| `zona_localidad_v2_por_localidad.csv` | Zonas y guías por localidad en la v2 |
| `upz_localidad.csv` | UPZ → localidad (POT, Decreto 190 de 2004). Las cuatro UPR rurales quedan sin localidad porque abarcan más de una |
| `asignacion_manual_v2.csv` | Correcciones a mano con su justificación. Se puede subir como `V3/ESPACIAL/asignacion_manual.csv` para que 0430 las aplique |
| `revision_cruzada_muestra_v2.csv` | Revisión de las 30 zonas de la muestra de 0430: 27 de acuerdo y 3 dudosas. Faltan las columnas `revisor_integrante` y `veredicto_integrante`, que llena un integrante del equipo |

## Privacidad y reglas del repositorio

- Los cuadernos se suben **sin salidas**. Antes de cada commit: *Clear all outputs* en Databricks (o `jupyter nbconvert --clear-output --inplace *.ipynb`). Nunca subir `.html` ni cuadernos con salidas: muestran filas de guías y rutas del workspace.
- No se suben datos (`*.parquet`), modelos, la carpeta `SEGURIDAD/` ni las sales o claves (ver `.gitignore`). Los únicos CSV permitidos son los de `entrega/`.
- El servidor y la base de SQL Server se leen de variables de entorno; no se escriben en el código.
- La seudonimización que se usó en la corrida es `md5(valor || sal)`. Para cualquier tabla por guía que salga del equipo se aplica antes `0500_SeudonimizacionHMAC` (HMAC-SHA256 con clave secreta). Detalle en [`docs/seudonimizacion.md`](docs/seudonimizacion.md).
- El historial de Git tiene versiones antiguas de cuadernos con salidas (correos de empleados de la empresa en `0200_AnalisisExploratorio.ipynb`, entre otros). Los pasos para limpiarlo están en [`docs/limpieza_historial.md`](docs/limpieza_historial.md); requieren reescribir el historial y debe hacerlo el dueño del repositorio.
- El repositorio debe quedar **privado** mientras no exista autorización escrita de Servienvios para publicar.

## Tecnología

Databricks serverless (Volume `/Volumes/proyectogrado/source/source/`), DuckDB sobre Parquet ZSTD, pandas, scikit-learn, LightGBM, XGBoost, SHAP y GeoPandas para la parte espacial (el I de Moran se calcula en el cuaderno, sin PySAL). Clima: Open-Meteo Historical Weather API. Capas espaciales: IDECA, Datos Abiertos Bogotá.
