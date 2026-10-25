# Databricks notebook source
# MAGIC %md
# MAGIC # 0160 · Clima alineado al despacho (propuesta P5) · versión 2
# MAGIC
# MAGIC **Qué hace.** Rehace las variables de clima para que cada hora use solo información de horas **anteriores**, sin convertir los faltantes en 0, y las cruza con la hora de **T0** (primera salida a zona urbana), no con la hora de admisión en la ciudad de origen.
# MAGIC
# MAGIC **Alcance.** Es un escenario **retrospectivo**: Open-Meteo *best match* es una reconstrucción que no estaba disponible al despachar. Sirve para medir si el clima aporta información, no para afirmar una mejora operativa.
# MAGIC
# MAGIC **Requisitos.** Base v3 de `0330_BaseAnaliticaV3` aprobada y el archivo horario por punto que ya produce 0150 (posición 5, `CLIMA_BOGOTA_PUNTOS_HORARIO.parquet`).
# MAGIC
# MAGIC **Cómo usarlo.** Ejecutar todo. Revisar primero el diagnóstico de zona horaria: si T0 no está en hora local, el cruce queda desplazado.

# COMMAND ----------

# MAGIC %md
# MAGIC > **Versión 2 (21-10-2026).** Solo cambia la entrada y la salida: lee `V3/GOLD_V3_ENVIO.parquet` (SLA en días hábiles) y escribe `V3/GOLD_V3_ENVIO_CLIMA.parquet` y sus CSV en la carpeta `V3/`. El cálculo del clima es el mismo de la versión 1. La columna `guia` ya llega seudonimizada desde `0330_BaseAnaliticaV3`.
# MAGIC
# MAGIC > **Versión 3 (24-10-2026).** Igual a la anterior, salvo: ruta nueva del Volume (`/Volumes/proyectogrado/source/source/`), `%pip install duckdb` activo y la función `copy_a_volume` (celda 3), que hace que cada `COPY ... TO` de DuckDB escriba primero en `/tmp` y luego copie el parquet al Volume. Así se evita el error `Async write failed ... Input/output error (errno 5)` de Databricks serverless.

# COMMAND ----------

# Si el entorno no tiene DuckDB, quitar el comentario y ejecutar:
%pip install duckdb

# COMMAND ----------

# CONFIGURACIÓN
import os, pandas as pd, duckdb
PATH_DATABRICKS = "/Volumes/proyectogrado/source/source/"
CARPETA_SALIDAS = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3/"   # v3: carpeta propia, no pisa la v2
RUTA_GOLD_V3        = f"{CARPETA_SALIDAS}GOLD_V3_ENVIO.parquet"                      # 0330
RUTA_CLIMA_PUNTOS   = f"{PATH_DATABRICKS}EXTERNAS/CLIMA_BOGOTA_PUNTOS_HORARIO.parquet"   # 0150, pos. 5
RUTA_CLIMA_REZAGADO = f"{CARPETA_SALIDAS}CLIMA_CIUDAD_REZAGADO.parquet"
RUTA_GOLD_V3_CLIMA  = f"{CARPETA_SALIDAS}GOLD_V3_ENVIO_CLIMA.parquet"

# ESCRITURA EN EL VOLUME (v6, 24-10-2026) · en Databricks serverless el FUSE de /Volumes no acepta las
# escrituras asíncronas de DuckDB ("Async write failed ... errno 5"). El COPY escribe primero en disco
# local y después el archivo se copia completo al Volume.
import os, re, shutil, tempfile, uuid

def copy_a_volume(sql_copy, con=None):
    """Ejecuta un COPY (...) TO '<ruta>' de DuckDB escribiendo en /tmp y copiando el resultado a <ruta>."""
    destino_m = list(re.finditer(r"\bTO\s+'([^']+)'", sql_copy, flags=re.IGNORECASE))[-1]
    destino = destino_m.group(1)
    local = os.path.join(tempfile.gettempdir(), f"duckdb_{uuid.uuid4().hex}_{os.path.basename(destino)}")
    sql_local = sql_copy[:destino_m.start(1)] + local + sql_copy[destino_m.end(1):]
    try:
        (con or duckdb).execute(sql_local)
        os.makedirs(os.path.dirname(destino), exist_ok=True)
        try:
            shutil.copyfile(local, destino)
        except OSError:
            dbutils.fs.cp(f"file:{local}", destino)
    finally:
        if os.path.exists(local):
            os.remove(local)
    print(f"Escrito en {destino}")
    return destino

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funciones y consultas (no es necesario modificarlas)

# COMMAND ----------

# =====================================================================================
# P5 · CLIMA ALINEADO AL INSTANTE DE PREDICCIÓN (T0 = salida a zona urbana) Y SIN
#      INFORMACIÓN DE LA MISMA HORA NI CEROS INVENTADOS
# Ubicación sugerida: 0150, reemplaza la posición 7 (id 0849a63b) y la posición 9 (id 1af8c9da).
# Escenario que habilita: comparación RETROSPECTIVA (reanálisis "best match"), no un sistema
# operativo: el reanálisis no estaba publicado en T0. Para un escenario operativo se
# necesitarían pronósticos archivados emitidos antes de T0 (Historical Forecast API).
# =====================================================================================
import duckdb, pandas as pd

def clima_ciudad_rezagado(df_puntos):
    """df_puntos: una fila por punto y hora local (salida de la posición 5).
    Cada fila FECHA_HORA = h contiene SOLO información de horas < h."""
    c = (df_puntos.groupby("FECHA_HORA")
         .agg(precip_prom=("PRECIPITACION_MM", "mean"), precip_max=("PRECIPITACION_MM", "max"),
              puntos_validos=("PRECIPITACION_MM", "count"),
              temp=("TEMPERATURA_C", "mean"), hum=("HUMEDAD_PCT", "mean"))
         .sort_index())
    # Rejilla horaria completa: una hora ausente queda NaN, no 0
    rejilla = pd.date_range(c.index.min(), c.index.max(), freq="h")
    c = c.reindex(rejilla)
    out = pd.DataFrame(index=rejilla)
    out["PRECIP_PREV_1H"] = c["precip_prom"].shift(1)
    out["PRECIP_MAX_PREV_1H"] = c["precip_max"].shift(1)
    for v in (3, 24, 72):   # exige ventana completa: min_periods = v
        out[f"LLUVIA_{v}H_PREV"] = c["precip_prom"].shift(1).rolling(v, min_periods=v).sum()
    out["TEMP_PREV_1H"] = c["temp"].shift(1)
    out["HUM_PREV_1H"] = c["hum"].shift(1)
    out["PUNTOS_VALIDOS_PREV_1H"] = c["puntos_validos"].shift(1)
    out["CLIMA_COMPLETO"] = out[["PRECIP_PREV_1H", "LLUVIA_72H_PREV"]].notna().all(axis=1)
    return out.rename_axis("FECHA_HORA").reset_index()      # SIN fillna(0)

SQL_CRUCE = """
-- Cruce por la hora de T0 (no por FECHA_ENVIO, que es la admisión en la ciudad de origen).
-- Requiere confirmar antes que t0 y FECHA_HORA están en la misma zona horaria (hora local).
SELECT B.*, C.* EXCLUDE (FECHA_HORA)
FROM read_parquet('{RUTA_GOLD_V3}') B
LEFT JOIN read_parquet('{RUTA_CLIMA_REZAGADO}') C
  ON C.FECHA_HORA = date_trunc('hour', B.t0)
"""

SQL_COBERTURA = """
SELECT COUNT(*) AS guias,
       COUNT(*) FILTER (WHERE PRECIP_PREV_1H IS NULL)     AS sin_clima,
       COUNT(*) FILTER (WHERE NOT CLIMA_COMPLETO)         AS clima_incompleto,
       MIN(t0) AS t0_min, MAX(t0) AS t0_max
FROM read_parquet('{RUTA_GOLD_V3_CLIMA}')
"""

SQL_ZONA_HORARIA = """
-- Diagnóstico de zona horaria y resolución (solo agregados).
SELECT any_value(typeof(FECHA_ENVIO)) AS tipo_fecha_envio, any_value(typeof(t0)) AS tipo_t0,
       COUNT(*) FILTER (WHERE CAST(FECHA_ENVIO AS TIME) = TIME '00:00:00') AS envio_a_medianoche,
       histogram(hour(t0)) AS hist_hora_t0          -- se espera concentración en horario de despacho local
FROM read_parquet('{RUTA_GOLD_V3}')
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Diagnóstico de zona horaria

# COMMAND ----------

zh = duckdb.sql(SQL_ZONA_HORARIA.format(RUTA_GOLD_V3=RUTA_GOLD_V3)).df()
display(zh)
# Se espera que la hora de T0 se concentre en el horario local de despacho (mañana). Si aparece
# desplazada unas 5 horas, las marcas están en UTC y hay que convertirlas antes del cruce.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clima rezagado y cruce con T0

# COMMAND ----------

df_puntos = pd.read_parquet(RUTA_CLIMA_PUNTOS)
clima = clima_ciudad_rezagado(df_puntos)
clima.to_parquet(RUTA_CLIMA_REZAGADO, index=False)
copy_a_volume(f"""COPY ({SQL_CRUCE.format(RUTA_GOLD_V3=RUTA_GOLD_V3, RUTA_CLIMA_REZAGADO=RUTA_CLIMA_REZAGADO)})
               TO '{RUTA_GOLD_V3_CLIMA}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
cobertura = duckdb.sql(SQL_COBERTURA.format(RUTA_GOLD_V3_CLIMA=RUTA_GOLD_V3_CLIMA)).df()
display(cobertura); cobertura.to_csv(f"{CARPETA_SALIDAS}P5_cobertura.csv", index=False)

# COMMAND ----------

# Cobertura por mes: cuántas guías quedan fuera de la cohorte con clima
por_mes = duckdb.sql(f"""SELECT date_trunc('month', t0) AS mes, COUNT(*) AS guias,
                                COUNT(*) FILTER (WHERE NOT CLIMA_COMPLETO OR CLIMA_COMPLETO IS NULL) AS sin_clima_completo
                         FROM read_parquet('{RUTA_GOLD_V3_CLIMA}') GROUP BY 1 ORDER BY 1""").df()
display(por_mes); por_mes.to_csv(f"{CARPETA_SALIDAS}P5_cobertura_mes.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué entregar y siguiente paso
# MAGIC
# MAGIC Los CSV `P5_cobertura` y `P5_cobertura_mes`, y el resultado del diagnóstico de zona horaria. Para contrastar H2, dejar en `0410_EvaluacionTemporal_3` la ruta `RUTA_GOLD_V3_CLIMA` con la ruta de este archivo y volver a ejecutarlo: comparará las dos versiones sobre la misma cohorte.
# MAGIC
# MAGIC Si no se consigue una fuente de tráfico con cobertura retrospectiva, H2 se reformula como «clima» (texto en la sección 6 del documento).