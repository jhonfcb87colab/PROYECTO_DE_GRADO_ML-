# Databricks notebook source
# MAGIC %md
# MAGIC > **Versión 6 (20-10-2026) · versión 5 + minimización de datos personales del repositorio**
# MAGIC > - **Integra lo que el equipo ya había hecho en el repositorio** (celdas 10, 15 y 18 con columnas explícitas y `NULL` en nombres, teléfonos, direcciones y usuarios) y lo completa: también quedan nulos los identificadores de remitente, destinatario, depositante y empleados, el contenido, las observaciones y la dirección estándar.
# MAGIC > - **Receptor:** `IDEN_PERS_RECIBE` y `NOMB_PERS_RECIBE` se guardan solo como indicador (`'S'` si había receptor, nulo si no). `0310_AuditoriaDesenlace` sigue funcionando igual, porque solo pregunta si el campo es nulo.
# MAGIC > - **Cliente:** `ID_CLIENTE` se guarda seudonimizado (`md5` con `SAL_CLIENTE`, pos. 9) en lugar de nulo, para que `0410_EvaluacionTemporal_2` pueda usar la historia por cliente sin conocer el cliente real.
# MAGIC > - **Control de meses (pos. 16):** cuenta guías y movimientos por mes en los archivos crudos, para verificar que hay movimientos de enero a agosto.
# MAGIC > - **Se mantiene de la versión 5:** sin salidas guardadas, `LIMIT 0` en las vistas previas, tabla base sin direcciones y celdas 38–45 desactivadas (`EJECUTAR_GOLD_ANTIGUA = False`). La base válida es la de `0330_BaseAnaliticaV2`.
# MAGIC > - La numeración de posiciones coincide con la del documento del asesor: esta celda de aviso es la posición 0.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > **Versión 7 (21-10-2026).** Igual a la versión 6, salvo las vistas previas de las posiciones 21, 23, 25 y 31: ahora muestran solo la estructura (`LIMIT 0`) y el número de filas. En la corrida del 20-10 esas celdas tenían `LIMIT 1` y el HTML exportado dejó ver una guía real y su cliente.
# MAGIC
# MAGIC > **Versión 8 (24-10-2026).** Igual a la anterior, salvo: ruta nueva del Volume (`/Volumes/proyectogrado/source/source/`), `%pip install duckdb` activo y la función `copy_a_volume` (celda 5), que hace que cada `COPY ... TO` de DuckDB escriba primero en `/tmp` y luego copie el parquet al Volume. Así se evita el error `Async write failed ... Input/output error (errno 5)` de Databricks serverless.

# COMMAND ----------

# MAGIC %md
# MAGIC # LIMPIEZA  DE DATOS

# COMMAND ----------

# MAGIC %md
# MAGIC ### IMPORTAR LIBRERIAS

# COMMAND ----------

# MAGIC %pip install duckdb
# MAGIC

# COMMAND ----------

import datetime
import duckdb
import pandas as pd
horainicio = datetime.datetime.now()

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
# MAGIC ### RUTAS PRINCIPALES

# COMMAND ----------

# RUTA RAIZ PRINCIPAL DEL PROYECTO EN DATABRICKS CLOUD
PATH_DATABRICKS = "/Volumes/proyectogrado/source/source/"

PATH_PROJECT = f"{PATH_DATABRICKS}/"

# COMMAND ----------

## VERIFICAR LA EXISTENCIA DE LA RUTA COMPARTIDA  AÑADIDA DATA-BRICKS CLOUD
archivos_y_carpetas = dbutils.fs.ls(PATH_DATABRICKS)
# Para imprimir cada elemento:
for elemento in archivos_y_carpetas:
    print(elemento.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIMPIEZA Y OBTENCION CONSOLIDADO DE DATOS  TBL GUI

# COMMAND ----------

### ENSAMBLE DE RUTAS PARA LOS ARCHIVOS PARQUET
RUTA_BATCH_LOG_GUIAS = f"{PATH_PROJECT}LOG_GUIAS/LOG_GUIAS*.parquet"
RUTA_FINAL_LOG_GUIAS = f"{PATH_PROJECT}LOG_GUIAS/RAW/LOG_GUIAS_BOGOTA.parquet"
SAL_CLIENTE = "CAMBIAR_ESTA_SAL"   # texto propio para seudonimizar ID_CLIENTE; guardarlo fuera del cuaderno y de GitHub


# COMMAND ----------

# Versión 6: columnas personales en NULL; receptor solo como indicador 'S'; ID_CLIENTE seudonimizado
# TRANSPORTE Y SEGMENTACION DE GUIAS DE BOGOTA (ID_CIUDAD_DESTINO = 10) Y CON NUMERO DE GUIA >= 8 Y ESTADO DE ENVIO >= 3
con = duckdb.connect()

copy_a_volume(f"""
    COPY (
        SELECT 
            NUMERO_GUIA,ID_TIPOGUIA,md5(CAST(ID_CLIENTE AS VARCHAR) || '{SAL_CLIENTE}') AS ID_CLIENTE,ID_CANAL,ID_DIVISION_PROV,ID_1,ID_2,ID_PAIS_ORIGEN,ID_CIUDAD_ORIGEN,
            ID_PAIS_DESTINO,ID_CIUDAD_DESTINO,ID_TIPO_ZONA_URBA,ID_PAIS_ZONA_URB_ENTR,ID_CIUDAD_ZONA_URB_ENTR,ID_ZONA_URBA,ID_ESTADOENVIO,
            ID_ESTADOGUIA,ID_CARACT_ENVIO,ID_TIEMPO,ID_MEDTRANS,ID_PRODUCTO,ID_SUBPRODUCTO,ID_BODEPROV,ID_PAIS_UBICACION,ID_CIUDAD_UBICACION,
            ID_DIVISION_UBICACION,FECHA_ENVIO,NULL AS ID_REMITE, NULL AS ID_DESTINA,null as  NOMBRE_REMITE_GUIA,null as  APELLIDO_REMITE_GUIA,
            null as  NOMBRE_DESTINA_GUIA,null as  APELLIDO_DESTINA_GUIA,null as  TELEFONO_REMITE_GUIA,null as  TELEFONO_DESTINA_GUIA,
            null as  DIRECCION_REMITE_GUIA,null as  DIRECCION_DESTINA_GUIA,NULL AS IDEN_RECEP_SERVI, NULL AS IDEN_ENTR_SERVI, NULL AS CONTENIDO,LARGO_ENVIO,
            ALTO_ENVIO,ANCHO_ENVIO,LARGO_ENV_OPERA,ALTO_ENV_OPERA,ANCHO_ENV_OPERA,PESO_GUIA,PESO_FISICO_GUIA,PESO_VOLU_GUIA,
            PESO_FISICO_OPERA,PESO_VOLU_OPERA,PESO_FACTURADO,FECHA_ENTREGA,NUMERO_PIEZAS,CASE WHEN NULLIF(TRIM(CAST(IDEN_PERS_RECIBE AS VARCHAR)), '') IS NOT NULL THEN 'S' END AS IDEN_PERS_RECIBE, CASE WHEN NULLIF(TRIM(CAST(NOMB_PERS_RECIBE AS VARCHAR)), '') IS NOT NULL THEN 'S' END AS NOMB_PERS_RECIBE,
            ID_REMPLAZA,NULL AS IDEN_DEPOSITANTE, NULL AS NOMB_DEPOSITANTE,
            FECHA_ULTIMA_MOD,NULL AS DIRECCION_ESTANDAR, NULL AS OBSERVACIONES,GUIA_EXTERNA,ID_DOCU_ULT_MOVI,NU_DOCU_ULT_MOVI,CONS_DOCU_ULT_MOVI,ID_FACTURADO,VALOR_DECLARADO_GUIA,VALOR_SEG_MAN_GUIAS,VALOR_FLETES_GUIAS,VALOR_DESCUENTO_GUIAS,VALOR_PEDIDO_GUIAS,VALOR_SEG_MAN_GUIAS_FACT,VALOR_FLETES_GUIAS_FACT,ID_REQUIERE_EMBALAJE,null as  LOGIN_EMPLEADO,FECHA_CREACION,FECHA_MODIFICACION,FECHA_MODIFICACION_DG,FECHA_EXTRACCION_DW
        FROM read_parquet('{RUTA_BATCH_LOG_GUIAS}')
        WHERE ID_CIUDAD_DESTINO = 10
        AND LEN(NUMERO_GUIA::VARCHAR) >= 8
        AND ID_ESTADOENVIO >= 3
    )
    TO '{RUTA_FINAL_LOG_GUIAS}'
    (
        FORMAT PARQUET,
        COMPRESSION ZSTD
    )
""", con)

print("Parquet consolidado correctamente.")

con.close()

# COMMAND ----------

df_resultado = duckdb.sql(f"""
    SELECT COUNT(1) AS TOTAL_REGISTROS
    FROM read_parquet('{ RUTA_FINAL_LOG_GUIAS }')
""").df()
display(df_resultado)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIMPIEZA Y OBTENCION CONSOLIDADO DE DATOS  TBL MOVIMIENTOS

# COMMAND ----------

### ENSAMBLE DE RUTAS PARA LOS ARCHIVOS PARQUET
RUTA_BATCH_LOG_SEGUIMIENTO_ENVIO = f"{PATH_PROJECT}LOG_SEGUIMIENTO_ENVIO/LOG_SEGUIMIENTO_ENVIO*.parquet"
RUTA_FINAL_LOG_SEGUIMIENTO_ENVIO = f"{PATH_PROJECT}LOG_SEGUIMIENTO_ENVIO/RAW/LOG_SEGUIMIENTO_ENVIO_BOGOTA.parquet"
RUTA_RELACION = f"{PATH_PROJECT}LOG_GUIAS/RAW/LOG_GUIAS_BOGOTA.parquet"

# COMMAND ----------

# pRUEBAS SCRIPT MOVIMIENTO SEGUIMIENTO DE GUIAS
df_resultado = duckdb.sql(f"""
        with CTE AS(
        SELECT
        	ROW_NUMBER ( )
				OVER (
					PARTITION BY G.NUMERO_GUIA
					order by S.FECHA_HORA_MOVIMIENTO desc
					)  AS ROW_ID_SE --> ULTIMO MOVIMIENTO
        ,S.*
        FROM read_parquet('{RUTA_RELACION}') G
        INNER JOIN read_parquet('{RUTA_BATCH_LOG_SEGUIMIENTO_ENVIO}') S
        ON G.NUMERO_GUIA = S.NUMERO_GUIA
        )
        SELECT
            COUNT(1)
        FROM CTE WHERE ROW_ID_SE = 1
           --LIMIT 5
""").df()
display(df_resultado)

# COMMAND ----------

# TRANSPORTE Y SEGMENTACION DE LOS MOVIMIENTOS DE GUIAS DE BOGOTA
con = duckdb.connect()

copy_a_volume(f"""
    COPY (
        with CTE AS(
        SELECT
        	ROW_NUMBER ( )
				OVER (
					PARTITION BY G.NUMERO_GUIA
					order by S.FECHA_HORA_MOVIMIENTO desc
					)  AS ROW_ID_SE, --> ULTIMO MOVIMIENTO
                S.ID_PROCESO, S.NUMERO_MOVIMI, S.CONSECUT_MOVIMI, S.NUMERO_GUIA,
                S.ID_CONCEPTO, NULL AS ID_EMPLEADO, S.ID_ESTA_FISICO, S.ID_PRODUCTO,
                S.ID_SUBPRODUCTO, S.ID_TIPOENVIO, S.ID_CONSECUTIVO_SEGUIMIENTO,
                S.ID_ENVIO_CRUZADO, S.PESO_ENVIO, S.LARGO_ENVIO, S.ANCHO_ENVIO,
                S.ALTO_ENVIO, S.HORA_INICIAL_ENTREGA, S.HORA_FINAL_ENTREGA,
                S.FECHA_HORA_MOVIMIENTO, S.ID_MODO_REGISTRO, S.NUMERO_PIEZAS,
                S.ID_EST_EMBALAJE, S.ID_TIPOENVIO_PRODUCTIVIDAD, S.DEVOLUCION_REGIONAL,
                NULL AS LOGIN_EMPLEADO, S.FECHA_CREACION, S.FECHA_MODIFICACION,
                S.ID_REPLICADO, S.FECHA_EXTRACCION_DW
        FROM read_parquet('{RUTA_RELACION}') G
        INNER JOIN read_parquet('{RUTA_BATCH_LOG_SEGUIMIENTO_ENVIO}') S
        ON G.NUMERO_GUIA = S.NUMERO_GUIA
        )SELECT * FROM CTE WHERE ROW_ID_SE = 1
    )
    TO '{RUTA_FINAL_LOG_SEGUIMIENTO_ENVIO}'
    (
        FORMAT PARQUET,
        COMPRESSION ZSTD
    )
""", con)

print("Parquet consolidado correctamente.")

con.close()

# COMMAND ----------

# PRUEBAS SCRIPT VALIDAR NUMERO DE GUIAS POR MOVIMIENTO SEGUIMIENTO DE GUIAS
df_resultado = duckdb.sql(f"""
        SELECT
            COUNT(1)
        FROM read_parquet('{RUTA_FINAL_LOG_SEGUIMIENTO_ENVIO}')
""").df()
display(df_resultado)

# Control (versión 6): guías y movimientos por mes en los archivos crudos.
# Deben aparecer movimientos de enero a agosto; si faltan meses, volver a correr 0100 para esos periodos.
display(duckdb.sql(f"""
    SELECT strftime(FECHA_HORA_MOVIMIENTO, '%Y-%m') AS mes, ID_PROCESO, COUNT(*) AS movimientos
    FROM read_parquet('{RUTA_BATCH_LOG_SEGUIMIENTO_ENVIO}') GROUP BY ALL ORDER BY ALL
""").df())
display(duckdb.sql(f"""
    SELECT strftime(FECHA_ENVIO, '%Y-%m') AS mes, COUNT(*) AS guias_bogota
    FROM read_parquet('{RUTA_FINAL_LOG_GUIAS}') GROUP BY 1 ORDER BY 1
""").df())


# COMMAND ----------

### ENSAMBLE DE RUTAS PARA LOS ARCHIVOS PARQUET
RUTA_BATCH_LOG_DOCUMENTOS = f"{PATH_PROJECT}LOG_DOCUMENTOS/LOG_DOCUMENTOS*.parquet"
RUTA_FINAL_LOG_DOCUMENTOS = f"{PATH_PROJECT}LOG_DOCUMENTOS/RAW/LOG_DOCUMENTOS_BOGOTA.parquet"
RUTA_RELACION_SE = f"{PATH_PROJECT}LOG_SEGUIMIENTO_ENVIO/RAW/LOG_SEGUIMIENTO_ENVIO_BOGOTA.parquet"

# COMMAND ----------

# TRANSPORTE Y SEGMENTACION DE LOS MANIFIESTOS DE BOGOTA
con = duckdb.connect()

copy_a_volume(f"""
    COPY (
        with CTE AS(
        SELECT
        	ROW_NUMBER ( )
				OVER (
					PARTITION BY
                                SEG.ID_PROCESO,
                                SEG.NUMERO_MOVIMI,
                                SEG.CONSECUT_MOVIMI
					order by DOC.FECHA_HORA_MOVIMI desc
					)  AS ROW_ID_SE --> ULTIMO MOVIMIENTO    
                    ,DOC.ID_PROCESO,DOC.NUMERO_MOVIMI,DOC.CONSECUT_MOVIMI,DOC.ID_TIPO_ZONA_URBA,
                        DOC.ID_PAIS_ZONA_URBA,DOC.ID_CIUDAD_ZONA_URBA,DOC.ID_ZONA_URBA,
                        DOC.ID_PAIS_DIVISION_ORIGEN,DOC.ID_CIUDAD_DIVISION_ORIGEN,
                        DOC.ID_DIVISION_ORIGEN, DOC.ID_PAIS_DESTINO,DOC.ID_CIUDAD_DESTINO,
                        DOC.ID_DIVISION_DESTINO, NULL AS ID_COURIER_CONDUCTOR,DOC.ID_PAIS_OPERATIVO,
                        DOC.ID_CIUDAD_OPERATIVO, DOC.ID_OPERATIVO,DOC.ID_RUTA,DOC.ID_PAIS_CANAL,
                        DOC.ID_CIUDAD_CANAL, DOC.ID_CANAL, NULL AS ID_CLIENTE,DOC.ID_SUCURSAL,
                        DOC.ID_DEPENDEN, DOC.ID_TIPOCONTENE, NULL AS ID_AUXILIAR1_VEHICULO,
                        DOC.ID_OPERADOR_LOGISTIC, DOC.ID_TIPO_LOGISTICA,DOC.ID_TIPO_DOCUMENTO,
                        DOC.FECHA_HORA_MOVIMI, DOC.FECHA_HORA_CIERRE,DOC.TOTAL_ENVIOS,DOC.PESO_TOTAL,
                        NULL AS PLACA_VEHICULO_DOCU, DOC.ESTADO_MOVIMI,DOC.NUME_SELLO_SEGURIDAD,
                        DOC.ID_TIPO_MOVIMIENTO, DOC.ID_ESTADO_TRANS_ZONA, NULL AS ID_AUXILIAR2_VEHICULO,
                        DOC.TOTAL_BOLSAS, NULL AS LOGIN_EMPLEADO, DOC.FECHA_CREACION,
                        DOC.FECHA_MODIFICACION, DOC.ID_REPLICADO, DOC.FECHA_MODIFICACION_QV,DOC.FECHA_EXTRACCION_DW
        FROM read_parquet('{RUTA_RELACION_SE}') AS SEG
        INNER JOIN read_parquet('{RUTA_BATCH_LOG_DOCUMENTOS}') AS DOC
	         ON SEG.ID_PROCESO      =   DOC.ID_PROCESO
            AND SEG.NUMERO_MOVIMI   =   DOC.NUMERO_MOVIMI
            AND SEG.CONSECUT_MOVIMI =   DOC.CONSECUT_MOVIMI
        )
        SELECT DISTINCT
            *
            FROM CTE WHERE ROW_ID_SE = 1
    )
    TO '{RUTA_FINAL_LOG_DOCUMENTOS}'
    (
        FORMAT PARQUET,
        COMPRESSION ZSTD
    )
""", con)

print("Parquet consolidado correctamente.")

con.close()

# COMMAND ----------

# PRUEBAS SCRIPT VALIDAR NUMERO DE GUIAS POR MOVIMIENTO SEGUIMIENTO DE GUIAS
df_resultado = duckdb.sql(f"""
        SELECT
            COUNT(1)
        FROM read_parquet('{RUTA_FINAL_LOG_DOCUMENTOS}')
""").df()
display(df_resultado)

# COMMAND ----------

# MAGIC %md
# MAGIC # TRANSFORMACION E INTEGRACION DE DATOS

# COMMAND ----------

# v7: solo estructura y conteos; ninguna fila real (realimentación, Acción inmediata).
# No cambiar LIMIT 0 por LIMIT 1: en la corrida del 20-10 eso dejó a la vista una guía y su cliente.
df_GUIAS = duckdb.sql(f"SELECT * FROM '{RUTA_FINAL_LOG_GUIAS}' LIMIT 0").df()          # columnas, sin filas
display(duckdb.sql(f"SELECT COUNT(*) AS filas FROM '{RUTA_FINAL_LOG_GUIAS}'").df())


# COMMAND ----------

print(df_GUIAS.columns)

# COMMAND ----------

# v7: solo estructura y conteos; ninguna fila real (realimentación, Acción inmediata).
# No cambiar LIMIT 0 por LIMIT 1: en la corrida del 20-10 eso dejó a la vista una guía y su cliente.
df_MOV = duckdb.sql(f"SELECT * FROM '{RUTA_FINAL_LOG_SEGUIMIENTO_ENVIO}' LIMIT 0").df()          # columnas, sin filas
display(duckdb.sql(f"SELECT COUNT(*) AS filas FROM '{RUTA_FINAL_LOG_SEGUIMIENTO_ENVIO}'").df())


# COMMAND ----------

print(df_MOV.columns)

# COMMAND ----------

# v7: solo estructura y conteos; ninguna fila real (realimentación, Acción inmediata).
# No cambiar LIMIT 0 por LIMIT 1: en la corrida del 20-10 eso dejó a la vista una guía y su cliente.
df_DOC = duckdb.sql(f"SELECT * FROM '{RUTA_FINAL_LOG_DOCUMENTOS}' LIMIT 0").df()          # columnas, sin filas
display(duckdb.sql(f"SELECT COUNT(*) AS filas FROM '{RUTA_FINAL_LOG_DOCUMENTOS}'").df())


# COMMAND ----------

print(df_DOC.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformacion contruccion Tabla Base de trabajo  

# COMMAND ----------

# CONFIGURAR RUTA FINAL BASE CONSOLIDADA DE LOS 3 PARQUETS
RUTA_FINAL_TABLA_BASE = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/LOG_BASE_BOGOTA.parquet"  # antes: ruta absoluta fija, ya no sigue PATH_DATABRICKS si este cambia
print(RUTA_FINAL_TABLA_BASE)

# COMMAND ----------

# PRUEBAS SCRIPT VALORES DUPLICADOS
'''
RUTA_FINAL_LOG_GUIAS
RUTA_FINAL_LOG_SEGUIMIENTO_ENVIO
RUTA_FINAL_LOG_DOCUMENTOS
'''

df_resultado = duckdb.sql(f"""
        SELECT
            NUMERO_GUIA, COUNT(1) AS TOTAL_REGISTROS
        FROM '{RUTA_FINAL_LOG_SEGUIMIENTO_ENVIO}'
        GROUP BY NUMERO_GUIA
        HAVING COUNT(1) > 1
""").df()
if df_resultado.empty:
    print("No se encontraron registros duplicados.")
else:
    print(df_resultado)
#--------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------
df_resultado = duckdb.sql(f"""
        SELECT
            ID_PROCESO, NUMERO_MOVIMI, CONSECUT_MOVIMI, COUNT(1) AS TOTAL_REGISTROS
        FROM '{RUTA_FINAL_LOG_DOCUMENTOS}'
        GROUP BY ID_PROCESO, NUMERO_MOVIMI, CONSECUT_MOVIMI
        HAVING COUNT(1) > 1
""").df()
if df_resultado.empty:
    print("No se encontraron registros duplicados.")
else:
    print(df_resultado)
#--------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------
df_resultado = duckdb.sql(f"""
        SELECT SEG.NUMERO_GUIA, COUNT(1) AS TOTAL_REGISTROS
                FROM read_parquet('{RUTA_FINAL_LOG_GUIAS}') AS GUI
                INNER JOIN read_parquet('{RUTA_FINAL_LOG_SEGUIMIENTO_ENVIO}') AS SEG
                    ON SEG.NUMERO_GUIA = GUI.NUMERO_GUIA
                GROUP BY SEG.NUMERO_GUIA
                HAVING COUNT(1) > 1
                    """).df()
if df_resultado.empty:
    print("No se encontraron registros duplicados.")
else:
    print(df_resultado)
#--------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------
df_resultado = duckdb.sql(f"""
        SELECT
                 DOC.ID_PROCESO
                ,DOC.NUMERO_MOVIMI
                ,DOC.CONSECUT_MOVIMI
                ,COUNT(1) AS TOTAL_REGISTROS
                FROM read_parquet('{RUTA_FINAL_LOG_SEGUIMIENTO_ENVIO}') AS SEG
                    INNER JOIN read_parquet('{RUTA_FINAL_LOG_DOCUMENTOS}') AS DOC
                        ON DOC.ID_PROCESO   = SEG.ID_PROCESO
                    AND DOC.NUMERO_MOVIMI   = SEG.NUMERO_MOVIMI
                    AND DOC.CONSECUT_MOVIMI = SEG.CONSECUT_MOVIMI
                GROUP BY DOC.ID_PROCESO, DOC.NUMERO_MOVIMI, DOC.CONSECUT_MOVIMI
                HAVING COUNT(1) > 1
                    """).df()
# En lugar de display(df_resultado), usa esto:
if df_resultado.empty:
    print("No se encontraron registros duplicados.")
else:
    print(df_resultado)

# COMMAND ----------

# TRANSPORTE Y SEGMENTACION DE LOS MANIFIESTOS DE BOGOTA
con = duckdb.connect()
copy_a_volume(f"""
    COPY (
                SELECT DISTINCT
                        ---------------------------------->Tikets Guia,
                        GUI.NUMERO_GUIA AS G_NUMERO_GUIA,
                        GUI.ID_TIPOGUIA AS G_ID_TIPOGUIA,
                        GUI.ID_PAIS_ORIGEN AS G_ID_PAIS_ORIGEN,
                        GUI.FECHA_ENVIO AS G_FECHA_ENVIO ,
                        GUI.ID_CIUDAD_ORIGEN  AS G_ID_CIUDAD_ORIGEN ,
                        GUI.ID_PAIS_DESTINO  AS G_ID_PAIS_DESTINO ,
                        GUI.ID_CIUDAD_DESTINO AS G_ID_CIUDAD_DESTINO,
                        GUI.ID_ESTADOENVIO  AS G_ID_ESTADOENVIO ,
                        GUI.ID_ESTADOGUIA AS G_ID_ESTADOGUIA,
                        GUI.ID_TIEMPO  AS G_ID_TIEMPO ,
                        GUI.ID_MEDTRANS  AS G_ID_MEDTRANS ,
                        GUI.ID_PRODUCTO  AS G_ID_PRODUCTO ,
                        GUI.ID_SUBPRODUCTO AS G_ID_SUBPRODUCTO,
                        -- GUI.DIRECCION_ESTANDAR retirada (dato personal; realimentación S0)
                        -- GUI.DIRECCION_REMITE_GUIA retirada (dato personal; realimentación S0)
                        -- GUI.DIRECCION_DESTINA_GUIA retirada (dato personal; realimentación S0)
                        ---------------------------------->MOVS,
                        SEG.ID_PROCESO  AS SEG_ID_PROCESO ,
                        SEG.NUMERO_MOVIMI  AS SEG_NUMERO_MOVIMI ,
                        SEG.CONSECUT_MOVIMI AS SEG_CONSECUT_MOVIMI,
                        SEG.FECHA_HORA_MOVIMIENTO AS SEG_FECHA_HORA_MOVIMIENTO,
                        SEG.ID_CONCEPTO AS SEG_ID_CONCEPTO,
                        SEG.ID_TIPOENVIO AS SEG_ID_TIPOENVIO,
                        SEG.PESO_ENVIO AS SEG_PESO_ENVIO,
                        SEG.LARGO_ENVIO  AS SEG_LARGO_ENVIO ,
                        SEG.ANCHO_ENVIO  AS SEG_ANCHO_ENVIO ,
                        SEG.ALTO_ENVIO AS SEG_ALTO_ENVIO,
                        SEG.NUMERO_PIEZAS AS SEG_NUMERO_PIEZAS,
                        ---------------------------------->DOCS,
                        DOC.ID_TIPO_ZONA_URBA  AS DOC_ID_TIPO_ZONA_URBA ,
                        DOC.ID_PAIS_ZONA_URBA  AS DOC_ID_PAIS_ZONA_URBA ,
                        DOC.ID_CIUDAD_ZONA_URBA AS DOC_ID_CIUDAD_ZONA_URBA,
                        DOC.ID_ZONA_URBA  AS DOC_ID_ZONA_URBA ,
                        DOC.ID_PAIS_DIVISION_ORIGEN  AS DOC_ID_PAIS_DIVISION_ORIGEN ,
                        DOC.ID_CIUDAD_DIVISION_ORIGEN AS DOC_ID_CIUDAD_DIVISION_ORIGEN,
                        DOC.ID_DIVISION_ORIGEN  AS DOC_ID_DIVISION_ORIGEN ,
                        DOC.ID_PAIS_DESTINO  AS DOC_ID_PAIS_DESTINO ,
                        DOC.ID_CIUDAD_DESTINO AS DOC_ID_CIUDAD_DESTINO,
                        DOC.ID_DIVISION_DESTINO AS DOC_ID_DIVISION_DESTINO
                FROM read_parquet('{RUTA_FINAL_LOG_GUIAS}') AS GUI
                INNER JOIN read_parquet('{RUTA_FINAL_LOG_SEGUIMIENTO_ENVIO}') AS SEG
                        ON SEG.NUMERO_GUIA = GUI.NUMERO_GUIA
                INNER JOIN read_parquet('{RUTA_FINAL_LOG_DOCUMENTOS}') AS DOC
                        ON DOC.ID_PROCESO = SEG.ID_PROCESO
                    AND DOC.NUMERO_MOVIMI = SEG.NUMERO_MOVIMI
                    AND DOC.CONSECUT_MOVIMI = SEG.CONSECUT_MOVIMI
    )
    TO '{RUTA_FINAL_TABLA_BASE}'
    (
        FORMAT PARQUET,
        COMPRESSION ZSTD
    )
""", con)

print("Parquet consolidado correctamente.")

con.close()

# COMMAND ----------

# v7: ya mostraba el conteo; se deja en LIMIT 0 (no cambiar por LIMIT 1: mostraría una fila real).
# PRUEBAS SCRIPT VALIDAR NUMERO DE REGISTROS EN LA TABLA BASE CONSOLIDADA
df_resultado = duckdb.sql(f"""
        SELECT
         COUNT(1) AS TOTAL_REGISTROS
           FROM '{RUTA_FINAL_TABLA_BASE}'
""").df()
display(df_resultado)

df_resultado = duckdb.sql(f"""
        SELECT *
           FROM '{RUTA_FINAL_TABLA_BASE}'
           LIMIT 0
""").df()
print(df_resultado.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Llamado de dimenisiones

# COMMAND ----------

# Carpeta secundaria de los archivos parametricos
Archivo_PAR_REGIONALES_PAIS     = f'{PATH_DATABRICKS}PARAMETRICAS/PAR.REGIONALES_PAIS.parquet'
Archivo_PAR_CIUDADES            = f'{PATH_DATABRICKS}PARAMETRICAS/PAR.CIUDADES.parquet'
Archivo_PAR_ESTADOSENVIO        = f'{PATH_DATABRICKS}PARAMETRICAS/PAR.ESTADOSENVIO.parquet'
Archivo_PAR_PROCESOS            = f'{PATH_DATABRICKS}PARAMETRICAS/PAR.PROCESOS.parquet'
Archivo_PAR_ESTADOSGUIA         = f'{PATH_DATABRICKS}PARAMETRICAS/PAR.ESTADOSGUIA.parquet'
Archivo_PAR_ZONASURBANAS        = f'{PATH_DATABRICKS}PARAMETRICAS/PAR.ZONASURBANAS.parquet'
Archivo_DIM_CALENDARIO          = f'{PATH_DATABRICKS}PARAMETRICAS/DIM.VW_TIEMPO_CALENDARIO_Y_FESTIVOS.parquet'
Archivo_PAR_RED_OPERATIVA_OFERTA = f'{PATH_DATABRICKS}PARAMETRICAS/PAR.RED_OPERATIVA_OFERTA.parquet'


# COMMAND ----------

# PRUEBA TABLAS
df_resultado = duckdb.sql(f"""
        SELECT * FROM read_parquet('{Archivo_PAR_ZONASURBANAS}') LIMIT 0
        """).df()
print(df_resultado.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnóstico de procesos presentes en el seguimiento
# MAGIC
# MAGIC **Corrección (realimentación P1):** en los datos reales los procesos presentes son 5 ENTRADA PUNTO DE SERVICIO, 6 ENTRADA DE ZONA URBANA y **9 SALIDA A ZONA URBANA**. El proceso 9 **no es una entrega**: es el despacho del envío hacia su zona de reparto en Bogotá. En el proyecto corregido se usa como **instante de predicción T0** (inicio de la última milla), y la entrega efectiva se toma de la fuente validada en `0310_AuditoriaDesenlace` (candidata: `FECHA_ENTREGA` de la guía).
# MAGIC
# MAGIC La celda siguiente se conserva como diagnóstico.

# COMMAND ----------

# DIAGNÓSTICO: nombres reales de los ID_PROCESO presentes en LOG_SEGUIMIENTO_ENVIO,
# para confirmar cuál corresponde a la entrega efectiva al destinatario.
df_diag_procesos = duckdb.sql(f"""
    SELECT DISTINCT
        SEG.ID_PROCESO,
        PROC.NOMBRE_PROCESO_LOG
    FROM read_parquet('{RUTA_FINAL_LOG_SEGUIMIENTO_ENVIO}') AS SEG
    LEFT JOIN read_parquet('{Archivo_PAR_PROCESOS}') AS PROC
        ON SEG.ID_PROCESO = PROC.ID_PROCESO
    ORDER BY SEG.ID_PROCESO
""").df()
display(df_diag_procesos)

# COMMAND ----------

# CONFIGURACIÓN (versión 5)
# ID_PROCESO_ENTREGA = [9] era un supuesto equivocado: el 9 es SALIDA A ZONA URBANA (T0), no la entrega.
# Se conserva solo para que las celdas de referencia 38-45 sigan siendo legibles.
ID_PROCESO_ENTREGA = [9]
SLA_DIAS_DEFAULT = 3

# Las posiciones 38-45 (base dimensionada, Gold antigua y partición 80/20) quedan como referencia.
# La base válida es GOLD_V2_ENVIO.parquet, que construye 0330_BaseAnaliticaV2.
EJECUTAR_GOLD_ANTIGUA = False


# COMMAND ----------

if not EJECUTAR_GOLD_ANTIGUA:
    print('Celda de referencia desactivada (EJECUTAR_GOLD_ANTIGUA = False). Usar 0330_BaseAnaliticaV2.')
else:
    # CONSTRUCCIÓN DE LA BASE DIMENSIONADA: TABLA BASE + TODAS LAS DIMENSIONES PARAMÉTRICAS
    RUTA_FINAL_BASE_DIMENSIONADA = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/BASE_DIMENSIONADA.parquet"

    con = duckdb.connect()
    copy_a_volume(f"""
        COPY (
            SELECT 
                TB_PR.G_NUMERO_GUIA,
                TB_PR.G_ID_TIPOGUIA,
                TB_PR.G_FECHA_ENVIO, 
                TB_PR.G_ID_CIUDAD_ORIGEN,
                TB_PR.G_ID_CIUDAD_DESTINO,
                TB_PAR_CD_ORG.NOMBRE_CIUDAD AS G_NOMBRE_CIUDAD_ORIGEN,
                TB_PAR_RG_ORG.NOMBRE_REGIONAL AS G_NOMBRE_REGIONAL_ORIGEN, 
                TB_PAR_CD_DST.NOMBRE_CIUDAD AS G_NOMBRE_CIUDAD_DESTINO,
                TB_PAR_RG_DST.NOMBRE_REGIONAL AS G_NOMBRE_REGIONAL_DESTINO,
                ESTADOENV.NOMBRE_ESTADOENVIO AS G_ESTADOENVIO,
                ESTADOGUIA.NOMBRE_ESTADOGUIA AS G_ESTADOGUIA, 
                REDOPE.TIEMPO_ENTREGA_COMERCIAL_HOR,
                REDOPE.TIEMPO_ENTREGA_COMERCIAL_DIA,
                TB_PROCESOS.NOMBRE_PROCESO_LOG AS SEG_NOMBRE_PROCESO_LOGISTICO,
                TB_PR.SEG_ID_PROCESO,
                TB_PR.SEG_NUMERO_MOVIMI,
                TB_PR.SEG_CONSECUT_MOVIMI, 
                TB_PR.SEG_FECHA_HORA_MOVIMIENTO, 
                TB_PR.SEG_NUMERO_PIEZAS,
                TB_PR.SEG_PESO_ENVIO,
                TB_PR.SEG_LARGO_ENVIO,
                TB_PR.SEG_ANCHO_ENVIO,
                TB_PR.SEG_ALTO_ENVIO,
                ZNURB.NOMBRE_ZONA_URBA AS DOC_NOMBRE_ZONA_URBANA,
                ZNURB.CATEGORIA_ZONA_URBA AS CATEGORIA_ZONA_URBA
               FROM read_parquet('{RUTA_FINAL_TABLA_BASE}') AS TB_PR 
               INNER JOIN read_parquet('{Archivo_PAR_CIUDADES}') AS TB_PAR_CD_ORG 
                    ON TB_PR.G_ID_PAIS_ORIGEN = TB_PAR_CD_ORG.ID_PAIS
                    AND TB_PR.G_ID_CIUDAD_ORIGEN = TB_PAR_CD_ORG.ID_CIUDAD
                INNER JOIN read_parquet('{Archivo_PAR_REGIONALES_PAIS}') AS TB_PAR_RG_ORG ON  TB_PAR_RG_ORG.ID_REGIONAL = TB_PAR_CD_ORG.ID_REGIONAL    
                INNER JOIN read_parquet('{Archivo_PAR_CIUDADES}') AS TB_PAR_CD_DST 
                    ON TB_PR.G_ID_PAIS_DESTINO = TB_PAR_CD_DST.ID_PAIS
                    AND TB_PR.G_ID_CIUDAD_DESTINO = TB_PAR_CD_DST.ID_CIUDAD   
                INNER JOIN read_parquet('{Archivo_PAR_REGIONALES_PAIS}') AS TB_PAR_RG_DST ON  TB_PAR_RG_DST.ID_REGIONAL = TB_PAR_CD_DST.ID_REGIONAL       
                INNER JOIN read_parquet('{Archivo_PAR_ESTADOSENVIO}') AS ESTADOENV ON TB_PR.G_ID_ESTADOENVIO = ESTADOENV.ID_ESTADOENVIO
            INNER JOIN read_parquet('{Archivo_PAR_ESTADOSGUIA}') AS ESTADOGUIA ON TB_PR.G_ID_ESTADOGUIA = ESTADOGUIA.ID_ESTADOGUIA
            LEFT JOIN read_parquet('{Archivo_PAR_RED_OPERATIVA_OFERTA}') AS REDOPE 
               ON TB_PR.G_ID_CIUDAD_ORIGEN = REDOPE.ID_CIUDAD_ORIGEN
              AND TB_PR.G_ID_CIUDAD_DESTINO = REDOPE.ID_CIUDAD_DESTINO
              AND CASE WHEN COALESCE(TB_PR.G_ID_TIEMPO,0) <> 0 THEN TB_PR.G_ID_TIEMPO ELSE 1 END = REDOPE.ID_TIEMPO
              AND CASE WHEN COALESCE(TB_PR.G_ID_MEDTRANS,0) <> 0 THEN TB_PR.G_ID_MEDTRANS ELSE 1 END = REDOPE.ID_MEDTRANS
            INNER JOIN read_parquet('{Archivo_PAR_PROCESOS}') AS TB_PROCESOS ON TB_PR.SEG_ID_PROCESO = TB_PROCESOS.ID_PROCESO  
        	LEFT JOIN READ_PARQUET('{Archivo_PAR_ZONASURBANAS}') AS ZNURB
                ON(ZNURB.ID_TIPO_ZONA_URBA=TB_PR.DOC_ID_TIPO_ZONA_URBA 
                    AND ZNURB.ID_ZONA_URBA=TB_PR.DOC_ID_ZONA_URBA 
                    AND ZNURB.ID_CIUDAD=TB_PR.DOC_ID_CIUDAD_ZONA_URBA 
                    AND ZNURB.ID_PAIS=1)
        )
        TO '{RUTA_FINAL_BASE_DIMENSIONADA}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    """, con)
    print("Parquet BASE_DIMENSIONADA consolidado correctamente.")
    con.close()


# COMMAND ----------

if not EJECUTAR_GOLD_ANTIGUA:
    print('Celda de referencia desactivada (EJECUTAR_GOLD_ANTIGUA = False). Usar 0330_BaseAnaliticaV2.')
else:
    # VALIDACIÓN: número de registros y muestra de la base dimensionada persistida, y guías que no encontraron
    # tiempo comercial de referencia en PAR.RED_OPERATIVA_OFERTA (LEFT JOIN -> TIEMPO_ENTREGA_COMERCIAL_HOR nulo).
    df_resultado = duckdb.sql(f"""
        SELECT COUNT(1) AS TOTAL_REGISTROS
        FROM read_parquet('{RUTA_FINAL_BASE_DIMENSIONADA}')
    """).df()
    display(df_resultado)

    df_sin_tiempo_comercial = duckdb.sql(f"""
        SELECT COUNT(1) AS GUIAS_SIN_TIEMPO_COMERCIAL_REF
        FROM read_parquet('{RUTA_FINAL_BASE_DIMENSIONADA}')
        WHERE TIEMPO_ENTREGA_COMERCIAL_HOR IS NULL
    """).df()
    display(df_sin_tiempo_comercial)
    print("Si este número es alto, revisar cobertura real de PAR.RED_OPERATIVA_OFERTA "
          "(combinaciones ciudad_origen/ciudad_destino/ID_TIEMPO/ID_MEDTRANS) antes de confiar en el indicador de incumplimiento.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## (Referencia) Etapa 2 de la versión anterior: tiempo de entrega, SLA y Gold antigua
# MAGIC
# MAGIC **No usar para resultados.** Estas celdas calculaban `tiempo_entrega_horas` hasta la salida a zona urbana (no hasta la entrega), con `DATEDIFF` (que cuenta cambios de hora, no horas transcurridas) y sobre una base con más filas que guías. Las reemplaza `0330_BaseAnaliticaV2`.

# COMMAND ----------

if not EJECUTAR_GOLD_ANTIGUA:
    print('Celda de referencia desactivada (EJECUTAR_GOLD_ANTIGUA = False). Usar 0330_BaseAnaliticaV2.')
else:
    # TIEMPO DE ENTREGA E INCUMPLIMIENTO DE SLA
    # tiempo_entrega_horas se calcula SOLO para el registro de movimiento que corresponde a la entrega
    # efectiva (SEG_ID_PROCESO en ID_PROCESO_ENTREGA); para los demás movimientos queda NULL.
    # incumplimiento_sla compara ese tiempo contra el tiempo comercial de la ruta (TIEMPO_ENTREGA_COMERCIAL_HOR);
    # si la ruta no tiene tiempo comercial parametrizado (LEFT JOIN nulo), se usa SLA_DIAS_DEFAULT*24 horas
    # como respaldo, y se deja marcada la columna SLA_ES_APROXIMADO para trazabilidad.
    ids_entrega_sql = ",".join(str(i) for i in ID_PROCESO_ENTREGA)

    df_resultado = duckdb.sql(f"""
        SELECT
            COUNT(1) AS TOTAL_ENTREGAS_DETECTADAS
        FROM read_parquet('{RUTA_FINAL_BASE_DIMENSIONADA}')
        WHERE SEG_ID_PROCESO IN ({ids_entrega_sql})
    """).df()
    display(df_resultado)
    print("Si este número es 0 o muy bajo frente al total de guías, ID_PROCESO_ENTREGA no está bien configurado: "
          "revisar la celda de diagnóstico de PAR.PROCESOS de arriba.")


# COMMAND ----------

if not EJECUTAR_GOLD_ANTIGUA:
    print('Celda de referencia desactivada (EJECUTAR_GOLD_ANTIGUA = False). Usar 0330_BaseAnaliticaV2.')
else:
    # VISTA PREVIA (10 filas) de tiempo_entrega_horas e incumplimiento_sla por envio individual.
    # IMPORTANTE (bug corregido): esta celda SI tenia antes un GROUP BY sobre columnas descriptivas
    # (ciudad, fecha, zona, etc.) con SUM(DATEDIFF(...)) y SUM(CASE ...) para las dos variables objetivo.
    # Eso esta mal: BASE_DIMENSIONADA ya tiene una fila por G_NUMERO_GUIA (SEG quedó deduplicado a una
    # fila por guia en la celda de MOVIMIENTOS de este mismo notebook), asi que cada fila de aqui ya es un
    # envio individual (sección 4.2 del anteproyecto: "unidad de análisis = envío individual"). Agregar
    # (GROUP BY/SUM) tiempo_entrega_horas o incumplimiento_sla entre varias guias que comparten
    # ciudad/fecha/zona da un numero sin sentido (el total de horas o el conteo de incumplidas del grupo,
    # no el de una guia). Esto se detecto porque, al correr contra datos reales, incumplimiento_sla salio
    # con 34 valores distintos en vez de 0/1 (visible en 0400_ModeladoPrediccion_1.ipynb, que tuvo que
    # "binarizar" incumplimiento_sla como parche porque el problema real estaba aqui, en la capa Gold).
    df_resultado = duckdb.sql(f"""
        SELECT
            G_NUMERO_GUIA
            ,G_ID_TIPOGUIA
            ,CAST(G_FECHA_ENVIO AS DATE) AS FECHA_ENVIO_FECHA
            ,G_NOMBRE_CIUDAD_ORIGEN
            ,G_NOMBRE_REGIONAL_ORIGEN
            ,G_NOMBRE_CIUDAD_DESTINO
            ,G_NOMBRE_REGIONAL_DESTINO
            ,G_ESTADOENVIO
            ,G_ESTADOGUIA
            ,TIEMPO_ENTREGA_COMERCIAL_HOR
            ,TIEMPO_ENTREGA_COMERCIAL_DIA
            ,SEG_NOMBRE_PROCESO_LOGISTICO
            ,CAST(SEG_FECHA_HORA_MOVIMIENTO AS DATE) AS FECHA_MOVIMIENTO_FECHA
            ,DOC_NOMBRE_ZONA_URBANA
            ,CATEGORIA_ZONA_URBA
            ,SEG_NUMERO_PIEZAS AS TOTAL_PIEZAS
            ,SEG_PESO_ENVIO AS TOTAL_PESO_ENVIO
            ,(COALESCE(SEG_LARGO_ENVIO, 0) * COALESCE(SEG_ANCHO_ENVIO, 0) * COALESCE(SEG_ALTO_ENVIO, 0)) AS VOLUMEN_ENVIO_CM3
            ,DATEDIFF('hour', G_FECHA_ENVIO, SEG_FECHA_HORA_MOVIMIENTO) AS tiempo_entrega_horas
            ,CASE
                WHEN TIEMPO_ENTREGA_COMERCIAL_HOR IS NOT NULL
                    THEN (DATEDIFF('hour', G_FECHA_ENVIO, SEG_FECHA_HORA_MOVIMIENTO) > TIEMPO_ENTREGA_COMERCIAL_HOR)::INT
                ELSE (DATEDIFF('hour', G_FECHA_ENVIO, SEG_FECHA_HORA_MOVIMIENTO) > {SLA_DIAS_DEFAULT} * 24)::INT
            END AS incumplimiento_sla
        FROM read_parquet('{RUTA_FINAL_BASE_DIMENSIONADA}')
        WHERE SEG_ID_PROCESO IN ({ids_entrega_sql})
        LIMIT 10
    """).df()

    display(df_resultado)


# COMMAND ----------

if not EJECUTAR_GOLD_ANTIGUA:
    print('Celda de referencia desactivada (EJECUTAR_GOLD_ANTIGUA = False). Usar 0330_BaseAnaliticaV2.')
else:
    # CONSTRUCCION DE LA BASE ANALITICA DEFINITIVA (equivalente a la capa "Gold")
    # Una fila por envio individual (G_NUMERO_GUIA) -- SIN GROUP BY/SUM, por la razon explicada en la celda
    # anterior. Esta es la correccion del bug real: la version anterior agregaba por columnas descriptivas
    # y sumaba las variables objetivo entre guias, lo cual rompia la unidad de analisis exigida por el
    # anteproyecto (seccion 4.2) y corrompia tanto los targets (tiempo_entrega_horas, incumplimiento_sla)
    # como las variables TOTAL_PIEZAS/TOTAL_PESO_ENVIO/VOLUMEN_ENVIO_CM3 (quedaban sumadas entre varias
    # guias en vez de ser el dato de una sola guia).
    RUTA_BASE_ANALITICA_DEFINITIVA = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/BASE_ANALITICA_DEFINITIVA.parquet"

    con = duckdb.connect()
    copy_a_volume(f"""
        COPY (
         SELECT
            G_NUMERO_GUIA
            ,G_ID_TIPOGUIA
            ,G_FECHA_ENVIO  -- se deja con hora completa (no se trunca a DATE): 0150_ClimaBogota la necesita para cruzar el clima por hora exacta de envio
            ,G_NOMBRE_CIUDAD_ORIGEN
            ,G_NOMBRE_REGIONAL_ORIGEN
            ,G_NOMBRE_CIUDAD_DESTINO
            ,G_NOMBRE_REGIONAL_DESTINO
            ,G_ESTADOENVIO
            ,G_ESTADOGUIA
            ,TIEMPO_ENTREGA_COMERCIAL_HOR
            ,TIEMPO_ENTREGA_COMERCIAL_DIA
            ,SEG_NOMBRE_PROCESO_LOGISTICO
            ,CAST(SEG_FECHA_HORA_MOVIMIENTO AS DATE) AS FECHA_MOVIMIENTO_FECHA
            ,DOC_NOMBRE_ZONA_URBANA
            ,CATEGORIA_ZONA_URBA
            ,SEG_NUMERO_PIEZAS AS TOTAL_PIEZAS
            ,SEG_PESO_ENVIO AS TOTAL_PESO_ENVIO
            ,(COALESCE(SEG_LARGO_ENVIO, 0) * COALESCE(SEG_ANCHO_ENVIO, 0) * COALESCE(SEG_ALTO_ENVIO, 0)) AS VOLUMEN_ENVIO_CM3
            ,DATEDIFF('hour', G_FECHA_ENVIO, SEG_FECHA_HORA_MOVIMIENTO) AS tiempo_entrega_horas
            ,CASE
                WHEN TIEMPO_ENTREGA_COMERCIAL_HOR IS NOT NULL
                    THEN (DATEDIFF('hour', G_FECHA_ENVIO, SEG_FECHA_HORA_MOVIMIENTO) > TIEMPO_ENTREGA_COMERCIAL_HOR)::INT
                ELSE (DATEDIFF('hour', G_FECHA_ENVIO, SEG_FECHA_HORA_MOVIMIENTO) > {SLA_DIAS_DEFAULT} * 24)::INT
            END AS incumplimiento_sla
            ,(TIEMPO_ENTREGA_COMERCIAL_HOR IS NULL) AS SLA_ES_APROXIMADO
            ,DATE_PART('dow', G_FECHA_ENVIO) AS DIA_SEMANA_ENVIO
            ,DATE_PART('month', G_FECHA_ENVIO) AS MES_ENVIO
            ,CASE
                WHEN DATE_PART('hour', G_FECHA_ENVIO) BETWEEN 6 AND 11 THEN 'MANANA'
                WHEN DATE_PART('hour', G_FECHA_ENVIO) BETWEEN 12 AND 17 THEN 'TARDE'
                WHEN DATE_PART('hour', G_FECHA_ENVIO) BETWEEN 18 AND 22 THEN 'NOCHE'
                ELSE 'MADRUGADA'
            END AS FRANJA_HORARIA_ENVIO
        FROM read_parquet('{RUTA_FINAL_BASE_DIMENSIONADA}')
        WHERE SEG_ID_PROCESO IN ({ids_entrega_sql})
        )
        TO '{RUTA_BASE_ANALITICA_DEFINITIVA}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    """, con)
    print("BASE_ANALITICA_DEFINITIVA (Gold) consolidada correctamente.")
    con.close()

    # VALIDACION CRITICA: debe haber exactamente una fila por guia (unidad de analisis = envio individual,
    # seccion 4.2), e incumplimiento_sla debe ser binaria. Si esto falla, algun JOIN de la celda de
    # BASE_DIMENSIONADA esta generando fan-out (candidato mas probable: PAR.RED_OPERATIVA_OFERTA, si la
    # combinacion ciudad_origen/ciudad_destino/ID_TIEMPO/ID_MEDTRANS no es unica alli) y hay que revisarlo
    # antes de seguir -- NO continuar con el notebook de modelado si estas aserciones fallan.
    df_check = duckdb.sql(f"""
        SELECT COUNT(*) AS TOTAL_FILAS, COUNT(DISTINCT G_NUMERO_GUIA) AS GUIAS_UNICAS,
               COUNT(DISTINCT incumplimiento_sla) AS VALORES_DISTINTOS_INCUMPLIMIENTO_SLA
        FROM read_parquet('{RUTA_BASE_ANALITICA_DEFINITIVA}')
    """).df()
    display(df_check)
    assert df_check.loc[0, "TOTAL_FILAS"] == df_check.loc[0, "GUIAS_UNICAS"], (
        "BASE_ANALITICA_DEFINITIVA tiene mas filas que guias unicas: hay fan-out en algun JOIN de "
        "BASE_DIMENSIONADA (revisar sobre todo PAR.RED_OPERATIVA_OFERTA)."
    )
    assert df_check.loc[0, "VALORES_DISTINTOS_INCUMPLIMIENTO_SLA"] <= 2, (
        "incumplimiento_sla deberia ser binaria (0/1): si tiene mas de 2 valores distintos, algo esta "
        "sumando/agregando sobre varias guias de nuevo."
    )
    print("OK: una fila por guia, incumplimiento_sla es binaria.")


# COMMAND ----------

if not EJECUTAR_GOLD_ANTIGUA:
    print('Celda de referencia desactivada (EJECUTAR_GOLD_ANTIGUA = False). Usar 0330_BaseAnaliticaV2.')
else:
    # LIMPIEZA DE ATÍPICOS Y NULOS EN LA BASE ANALÍTICA DEFINITIVA
    # (criterios de exclusión, sección 4.2.2: tiempos negativos o físicamente imposibles se tratan como atípicos)
    df_diag = duckdb.sql(f"""
        SELECT
            COUNT(1) AS TOTAL,
            SUM(CASE WHEN tiempo_entrega_horas < 0 THEN 1 ELSE 0 END) AS NEGATIVOS,
            SUM(CASE WHEN tiempo_entrega_horas > 720 THEN 1 ELSE 0 END) AS MAYOR_30_DIAS,
            SUM(CASE WHEN G_NOMBRE_CIUDAD_DESTINO IS NULL THEN 1 ELSE 0 END) AS SIN_CIUDAD_DESTINO,
            SUM(CASE WHEN tiempo_entrega_horas IS NULL THEN 1 ELSE 0 END) AS SIN_TIEMPO_ENTREGA
        FROM read_parquet('{RUTA_BASE_ANALITICA_DEFINITIVA}')
    """).df()
    display(df_diag)

    con = duckdb.connect()
    copy_a_volume(f"""
        COPY (
            SELECT *
            FROM read_parquet('{RUTA_BASE_ANALITICA_DEFINITIVA}')
            WHERE tiempo_entrega_horas IS NOT NULL
              AND tiempo_entrega_horas >= 0
              AND tiempo_entrega_horas <= 720
              AND G_NOMBRE_CIUDAD_DESTINO IS NOT NULL
        )
        TO '{RUTA_BASE_ANALITICA_DEFINITIVA}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    """, con)
    print("Atípicos depurados y BASE_ANALITICA_DEFINITIVA sobrescrita sin ellos.")
    con.close()


# COMMAND ----------

if not EJECUTAR_GOLD_ANTIGUA:
    print('Celda de referencia desactivada (EJECUTAR_GOLD_ANTIGUA = False). Usar 0330_BaseAnaliticaV2.')
else:
    # PARTICIÓN TEMPORAL 80/20 (sección 4.6: la validación debe respetar el orden cronológico, nunca aleatoria)
    df_fechas = duckdb.sql(f"""
        SELECT G_FECHA_ENVIO
        FROM read_parquet('{RUTA_BASE_ANALITICA_DEFINITIVA}')
        ORDER BY G_FECHA_ENVIO
    """).df()
    fecha_corte = df_fechas["G_FECHA_ENVIO"].quantile(0.8, interpolation="nearest")
    print("Fecha de corte train/test (percentil 80 por fecha de envío):", fecha_corte)

    con = duckdb.connect()
    copy_a_volume(f"""
        COPY (
            SELECT *, CASE WHEN G_FECHA_ENVIO < TIMESTAMP '{fecha_corte}' THEN 'train' ELSE 'test' END AS SPLIT
            FROM read_parquet('{RUTA_BASE_ANALITICA_DEFINITIVA}')
        )
        TO '{RUTA_BASE_ANALITICA_DEFINITIVA}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    """, con)
    con.close()

    df_conteo_split = duckdb.sql(f"""
        SELECT SPLIT, COUNT(1) AS TOTAL, AVG(incumplimiento_sla) AS TASA_INCUMPLIMIENTO
        FROM read_parquet('{RUTA_BASE_ANALITICA_DEFINITIVA}')
        GROUP BY SPLIT
    """).df()
    display(df_conteo_split)
    print("BASE_ANALITICA_DEFINITIVA lista, con columna SPLIT ('train'/'test'), para el notebook de modelado.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Historial
# MAGIC - **Versión 4:** se retiró el `GROUP BY`/`SUM` de la Gold. La realimentación del asesor mostró que eso no bastaba: la base dimensionada ya tenía más filas que guías (4.945.549 frente a 3.053.448) por claves repetidas en las parametrías (ver `0320_DiagnosticoFanout`).
# MAGIC - **Versión 5:** salidas sin datos personales y celdas 38–45 desactivadas. La construcción válida está en `0330_BaseAnaliticaV2`.

# COMMAND ----------

horafin = datetime.datetime.now()
Duracion = horafin - horainicio
print(f"Tiempo de ejecución: {Duracion}")