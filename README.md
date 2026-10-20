# Predicción de tiempos de entrega e incumplimiento de SLA · Servienvios (Bogotá)

Universidad Santo Tomás · Facultad de Estadística · Maestría en Ciencia de Datos · Trabajo de grado

John Fernando Cruz Becerra · Oscar Javier Huertas Campo · Mauricio Alejandro Gaviria Alzate

## Qué hace

Construye una base analítica con **una fila por guía** (envíos con destino Bogotá, enero–agosto de 2026) y predice, en el momento de la **primera salida a zona urbana (T0)**:
- el **tiempo de última milla** (horas de T0 a la entrega efectiva), y
- la **probabilidad de incumplir el SLA** (tiempo de tránsito comprometido por ruta).

La evaluación usa validación temporal de origen móvil (febrero–junio) y una prueba final única (julio–agosto, o un mes de reserva posterior).

## Cuadernos y orden de ejecución

| Orden | Cuaderno | Dónde | Qué produce |
|---|---|---|---|
| 1 | 0100_ExtraccionInformacion | Equipo con SQL Server | Parquet de guías, movimientos (procesos 5, 6, 9), documentos y paramétricas |
| 1b | 0110_ExtraccionComplementaria | Equipo con SQL Server | Diagnóstico de procesos; opcional: movimientos de entrega, meses faltantes y mes de reserva |
| 2 | 0300_CleanTransformParquet_6 (hasta pos. 37) | Databricks | LOG_GUIAS_BOGOTA y LOG_BASE_BOGOTA, sin datos personales |
| 3 | 0310_AuditoriaDesenlace | Databricks | Validación de la fuente de la entrega |
| 4 | 0320_DiagnosticoFanout | Databricks | Origen de la multiplicación de filas |
| 5 | 0330_BaseAnaliticaV2 | Databricks | GOLD_V2_ENVIO (una fila por guía, con controles y manifiesto) |
| 6 | 0150_ClimaBogota_2 → 0160_ClimaAlineado | Databricks | Clima de horas anteriores a T0 |
| 7 | 0210_AnalisisExploratorioV2 | Databricks | Caracterización (OE1) |
| 8 | 0410_EvaluacionTemporal_2 | Databricks | Comparación de modelos, H1/H2, decisión y prueba final (OE3, OE4) |
| 9 | 0420_Interpretabilidad | Databricks | Permutación, SHAP y lectura operativa (OE5) |

Archivados (no usar para resultados): `0200_AnalisisExploratorio_ARCHIVAR`, `0400_ModeladoPrediccion_2` (antecedente diagnóstico).

## Tecnología

Databricks (Volume `/Volumes/proyectogrado/source/sourcedb/`), DuckDB sobre Parquet ZSTD, pandas, scikit-learn, LightGBM, XGBoost, SHAP. Clima: Open-Meteo Historical Weather API (reanálisis retrospectivo).

## Reglas del repositorio

- **Nunca** subir cuadernos con salidas que muestren filas de guías, movimientos o documentos: antes de cada commit, *Clear all outputs*.
- No subir datos (`*.parquet`, `*.csv`), modelos ni la carpeta `PRIVADO/` (ver `.gitignore`).
- Las sales (`SAL`, `SAL_CLIENTE`) se guardan fuera del repositorio.
