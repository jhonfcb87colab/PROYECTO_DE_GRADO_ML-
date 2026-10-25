# Databricks notebook source
# MAGIC %md
# MAGIC # 0310 · Auditoría del desenlace (propuesta P1)
# MAGIC
# MAGIC **Qué hace.** Mide, para cada fuente candidata de la «entrega efectiva», cuántas guías la tienen y si es coherente con la salida a zona urbana: `FECHA_ENTREGA`, el estado de la guía, la existencia de receptor y los movimientos de proceso 9. Usa **todos** los movimientos, no solo el último.
# MAGIC
# MAGIC **Por qué.** Hoy `tiempo_entrega_horas` mide hasta la SALIDA A ZONA URBANA (proceso 9), que no es una entrega. Sin validar el desenlace no se puede construir ninguna etiqueta (ver P1 del documento de realimentación).
# MAGIC
# MAGIC **Requisitos.** Haber ejecutado 0300 hasta la posición 10, que crea `LOG_GUIAS_BOGOTA.parquet`. Tener acceso a los Parquet de seguimiento y a las parametrías.
# MAGIC
# MAGIC **Cómo usarlo.**
# MAGIC 1. Revisar las rutas de la celda de configuración y cambiar `SAL` por un texto propio que se guarde fuera del cuaderno.
# MAGIC 2. Ejecutar todas las celdas en orden (*Run all*). La lectura de todos los movimientos puede tardar varios minutos.
# MAGIC 3. Leer los resultados con la guía de la sección «Cómo leer los resultados».
# MAGIC 4. Entregar lo indicado en «Qué entregar».
# MAGIC
# MAGIC **Salidas.** Tablas agregadas a1–a4 (pantalla y CSV) y la muestra de 60 guías en dos versiones: con la guía real (solo para la empresa) y seudonimizada (para el proyecto).

# COMMAND ----------

# Si el entorno no tiene DuckDB, quitar el comentario y ejecutar:
!pip install duckdb

# COMMAND ----------

# CONFIGURACIÓN · revisar las rutas antes de ejecutar (son las mismas de 0300)
import os
PATH_DATABRICKS = "/Volumes/proyectogrado/source/source/"
RUTA_GUIAS      = f"{PATH_DATABRICKS}LOG_GUIAS/RAW/LOG_GUIAS_BOGOTA.parquet"                 # la crea 0300, pos. 10
RUTA_MOVS_GLOB  = f"{PATH_DATABRICKS}LOG_SEGUIMIENTO_ENVIO/LOG_SEGUIMIENTO_ENVIO*.parquet"    # TODOS los movimientos
RUTA_DOCS_GLOB  = f"{PATH_DATABRICKS}LOG_DOCUMENTOS/LOG_DOCUMENTOS*.parquet"                 # TODOS los documentos
RUTA_TABLA_BASE = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/LOG_BASE_BOGOTA.parquet"                # la crea 0300, pos. 30
PAR = f"{PATH_DATABRICKS}PARAMETRICAS/"
P = {"CIUDADES":   f"{PAR}PAR.CIUDADES.parquet",       "REGIONALES": f"{PAR}PAR.REGIONALES_PAIS.parquet",
     "EST_ENVIO":  f"{PAR}PAR.ESTADOSENVIO.parquet",   "EST_GUIA":   f"{PAR}PAR.ESTADOSGUIA.parquet",
     "RED":        f"{PAR}PAR.RED_OPERATIVA_OFERTA.parquet", "PROCESOS": f"{PAR}PAR.PROCESOS.parquet",
     "ZONAS":      f"{PAR}PAR.ZONASURBANAS.parquet"}
# Salidas nuevas de esta revisión: nada de lo existente se sobrescribe
CARPETA_SALIDAS = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/"
os.makedirs(CARPETA_SALIDAS, exist_ok=True)

ID_SALIDA_ZONA = 9                 # SALIDA A ZONA URBANA según PAR.PROCESOS (0300, pos. 36)
SAL = "CAMBIAR_ESTA_SAL"           # texto propio; guardarlo fuera del cuaderno
# Carpeta del entorno autorizado para el archivo con guías reales (no se comparte en el proyecto)
CARPETA_PRIVADA = f"{CARPETA_SALIDAS}PRIVADO/"
os.makedirs(CARPETA_PRIVADA, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funciones (no es necesario modificarlas)

# COMMAND ----------

# =====================================================================================
# P1 · AUDITORÍA DE LAS FUENTES CANDIDATAS DEL DESENLACE ("entrega efectiva")
# Ubicación sugerida: celda nueva en 0300, después de la celda de diagnóstico de procesos
# (posición 36) y ANTES de fijar ID_PROCESO_ENTREGA (posición 37).
# Entradas: LOG_GUIAS_BOGOTA.parquet (filtrada en pos. 10), LOG_SEGUIMIENTO_ENVIO*.parquet
#           (TODOS los movimientos, no solo el último), PAR.ESTADOSGUIA, PAR.ESTADOSENVIO.
# Salidas: solo tablas AGREGADAS (no imprime guías, nombres ni direcciones).
# =====================================================================================
import duckdb

def auditoria_desenlace(RUTA_GUIAS, RUTA_MOVS_GLOB, RUTA_EST_GUIA, RUTA_EST_ENVIO,
                        ID_SALIDA_ZONA=9, SAL="CAMBIAR_ESTA_SAL"):
    con = duckdb.connect()
    con.execute(f"""
    CREATE OR REPLACE TEMP VIEW AUD AS
    WITH G AS (
        SELECT CAST(NUMERO_GUIA AS BIGINT) AS guia, FECHA_ENVIO, FECHA_ENTREGA,
               ID_ESTADOGUIA, ID_ESTADOENVIO,
               (IDEN_PERS_RECIBE IS NOT NULL OR NOMB_PERS_RECIBE IS NOT NULL) AS hay_receptor
        FROM read_parquet('{RUTA_GUIAS}')
    ),
    M AS (
        SELECT CAST(NUMERO_GUIA AS BIGINT) AS guia,
               COUNT(*)                                                     AS n_movs,
               COUNT(*) FILTER (WHERE ID_PROCESO = {ID_SALIDA_ZONA})        AS n_salidas_zona,
               MIN(FECHA_HORA_MOVIMIENTO) FILTER (WHERE ID_PROCESO = {ID_SALIDA_ZONA}) AS primera_salida_zona,
               MAX(FECHA_HORA_MOVIMIENTO)                                   AS ultimo_mov,
               arg_max(ID_PROCESO, FECHA_HORA_MOVIMIENTO)                   AS proceso_ultimo_mov
        FROM read_parquet('{RUTA_MOVS_GLOB}')
        WHERE CAST(NUMERO_GUIA AS BIGINT) IN (SELECT guia FROM G)   -- solo guías de la población
        GROUP BY 1
    )
    SELECT G.*, M.* EXCLUDE (guia),
           EG.NOMBRE_ESTADOGUIA, EE.NOMBRE_ESTADOENVIO,
           (epoch_ms(G.FECHA_ENTREGA) - epoch_ms(M.primera_salida_zona)) / 3600000.0 AS h_salida_a_entrega,
           (epoch_ms(G.FECHA_ENTREGA) - epoch_ms(G.FECHA_ENVIO))         / 3600000.0 AS h_envio_a_entrega
    FROM G
    LEFT JOIN M  ON M.guia = G.guia
    LEFT JOIN read_parquet('{RUTA_EST_GUIA}')  EG ON EG.ID_ESTADOGUIA  = G.ID_ESTADOGUIA
    LEFT JOIN read_parquet('{RUTA_EST_ENVIO}') EE ON EE.ID_ESTADOENVIO = G.ID_ESTADOENVIO
    """)

    # A1. Cobertura global de cada fuente candidata (una fila)
    a1 = con.sql("""
        SELECT COUNT(*)                                             AS guias,
               COUNT(*) FILTER (WHERE n_movs IS NULL)               AS sin_movimientos,
               COUNT(*) FILTER (WHERE n_salidas_zona > 0)           AS con_salida_zona,
               COUNT(*) FILTER (WHERE n_salidas_zona > 1)           AS con_varias_salidas_zona,
               COUNT(*) FILTER (WHERE FECHA_ENTREGA IS NOT NULL)    AS con_fecha_entrega,
               COUNT(*) FILTER (WHERE hay_receptor)                 AS con_receptor,
               COUNT(*) FILTER (WHERE FECHA_ENTREGA < primera_salida_zona) AS entrega_antes_de_salida,
               COUNT(*) FILTER (WHERE FECHA_ENTREGA < FECHA_ENVIO)  AS entrega_antes_de_envio,
               COUNT(*) FILTER (WHERE FECHA_ENTREGA IS NOT NULL
                                  AND CAST(FECHA_ENTREGA AS TIME) = TIME '00:00:00') AS entrega_sin_hora
        FROM AUD
    """).df()

    # A2. Concordancia estado de la guía × existencia de FECHA_ENTREGA × receptor
    a2 = con.sql("""
        SELECT NOMBRE_ESTADOGUIA, NOMBRE_ESTADOENVIO,
               COUNT(*) AS guias,
               ROUND(AVG((FECHA_ENTREGA IS NOT NULL)::INT), 4) AS pct_con_fecha_entrega,
               ROUND(AVG(hay_receptor::INT), 4)                 AS pct_con_receptor,
               ROUND(AVG((n_salidas_zona > 0)::INT), 4)         AS pct_con_salida_zona
        FROM AUD GROUP BY ALL ORDER BY guias DESC
    """).df()

    # A3. Cuantiles de los dos intervalos candidatos (solo guías con ambas marcas)
    a3 = con.sql("""
        SELECT 'salida_zona -> entrega' AS intervalo, COUNT(h_salida_a_entrega) AS n,
               quantile_cont(h_salida_a_entrega, [0.01,0.10,0.50,0.90,0.99]) AS q01_q10_q50_q90_q99
        FROM AUD
        UNION ALL
        SELECT 'envio -> entrega', COUNT(h_envio_a_entrega),
               quantile_cont(h_envio_a_entrega, [0.01,0.10,0.50,0.90,0.99])
        FROM AUD
    """).df()

    # A4. Proceso del último movimiento disponible (cuántas guías excluye la regla "último = 9")
    a4 = con.sql("""
        SELECT proceso_ultimo_mov, COUNT(*) AS guias,
               COUNT(*) FILTER (WHERE n_salidas_zona > 0) AS de_ellas_con_alguna_salida_zona
        FROM AUD GROUP BY 1 ORDER BY 2 DESC
    """).df()

    # A5. Muestra de 60 guías para que Servienvios la contraste en su sistema.
    #     Trae la guía real (solo para el entorno autorizado y la empresa) y su seudónimo
    #     (lo único que circula en el proyecto). La sal se guarda aparte.
    a5 = con.sql(f"""
        SELECT guia, md5(CAST(guia AS VARCHAR) || '{SAL}') AS guia_seud,
               NOMBRE_ESTADOGUIA, FECHA_ENVIO, primera_salida_zona, FECHA_ENTREGA, n_salidas_zona
        FROM AUD
        WHERE n_salidas_zona > 0
        USING SAMPLE reservoir(60 ROWS) REPEATABLE (42)
    """).df()
    con.close()
    return a1, a2, a3, a4, a5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejecución

# COMMAND ----------

a1, a2, a3, a4, a5 = auditoria_desenlace(RUTA_GUIAS, RUTA_MOVS_GLOB, P["EST_GUIA"], P["EST_ENVIO"],
                                         ID_SALIDA_ZONA=ID_SALIDA_ZONA, SAL=SAL)
for nombre, tabla in [("a1_cobertura", a1), ("a2_estados", a2), ("a3_cuantiles", a3), ("a4_ultimo_proceso", a4)]:
    print(nombre); display(tabla)
    tabla.to_csv(f"{CARPETA_SALIDAS}P1_{nombre}.csv", index=False)

# Muestra: la versión con guía real SOLO va a la carpeta privada (para enviarla a Servienvios);
# al proyecto solo llega la seudonimizada. No se muestra en pantalla.
a5.to_csv(f"{CARPETA_PRIVADA}P1_a5_muestra_con_guia_real.csv", index=False)
a5.drop(columns="guia").to_csv(f"{CARPETA_SALIDAS}P1_a5_muestra_seudonimizada.csv", index=False)
print("Muestra guardada:", len(a5), "guías")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo leer los resultados
# MAGIC
# MAGIC | Tabla | Qué mirar | Qué se espera si `FECHA_ENTREGA` sirve |
# MAGIC | --- | --- | --- |
# MAGIC | a1 | `con_fecha_entrega` frente a `guias`; `entrega_antes_de_salida`, `entrega_antes_de_envio`, `entrega_sin_hora` | Cobertura alta; las tres incoherencias cercanas a cero |
# MAGIC | a2 | `pct_con_fecha_entrega` por estado de la guía | Alto en ENTREGADO, bajo en estados abiertos |
# MAGIC | a3 | Cuantiles de horas salida → entrega y envío → entrega | Valores positivos y plausibles para un tramo urbano |
# MAGIC | a4 | Proceso del último movimiento | Cuántas guías excluye la regla actual «último movimiento = 9» |
# MAGIC
# MAGIC Si `FECHA_ENTREGA` falla, la otra vía es reextraer en 0100 todos los procesos de seguimiento. Si ninguna fuente sirve, aplica la alternativa B del documento.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué entregar
# MAGIC
# MAGIC 1. Los cuatro CSV `P1_a1` a `P1_a4` de la carpeta `REVISION_2026_10`.
# MAGIC 2. La respuesta de Servienvios sobre `P1_a5_muestra_con_guia_real.csv`: para cada guía, si la fecha de entrega coincide con su sistema. Al proyecto solo llega la versión seudonimizada.
# MAGIC 3. Antes de compartir este cuaderno, comprobar que ninguna salida muestre guías individuales.