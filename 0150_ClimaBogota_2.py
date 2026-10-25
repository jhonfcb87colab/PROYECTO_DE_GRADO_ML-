# Databricks notebook source
# MAGIC %md
# MAGIC # 0110 · Extracción complementaria (se corre en el equipo con acceso a SQL Server, igual que 0100)
# MAGIC
# MAGIC **Qué hace.** Agrega a lo que ya extrajo `0100_ExtraccionInformacion` **sin modificarlo ni sobrescribir sus archivos**:
# MAGIC
# MAGIC 1. **Diagnóstico de procesos** (solo conteos): qué `ID_PROCESO` existen en `LOG.SEGUIMIENTO_ENVIO` y cómo se llaman. Responde la pregunta 1 del asesor: 0100 extrae solo los procesos 5, 6 y 9 (`WHERE ID_PROCESO IN (5, 6, 9)`), así que ningún proceso de entrega llegó a los datos.
# MAGIC 2. **Movimientos de entrega** (opcional): extrae los procesos de entrega que se identifiquen en el paso 1, de enero a agosto, en una carpeta **aparte** (`LOG_SEGUIMIENTO_ENTREGA/`). Es la segunda vía de la alternativa A si `FECHA_ENTREGA` no resulta confiable.
# MAGIC 3. **Meses faltantes de movimientos** (opcional): la celda de movimientos de 0100 lista solo 202601–202605, aunque las guías van hasta 202608. Si en el Volume faltan los movimientos de junio a agosto (lo verifica `0300` versión 6, posición 16), este paso los extrae con el mismo nombre de archivo que usa 0100.
# MAGIC 4. **Periodo de reserva** (opcional, recomendado): extrae septiembre de 2026 en una carpeta **aparte** (`RESERVA_POSTERIOR/`). Julio y agosto ya se usaron para elegir modelos en el 0400 antiguo; un mes nunca visto permite una prueba final realmente independiente (realimentación P4).
# MAGIC
# MAGIC **Datos personales.** Todas las consultas usan listas explícitas de columnas: no traen nombres, teléfonos, direcciones, identificaciones, contenido, observaciones, usuarios, conductores ni placas. Del receptor solo se trae un indicador (`'S'` si existe). Así los Parquet nuevos ya salen minimizados desde la fuente (realimentación, «Acción inmediata», punto 2).
# MAGIC
# MAGIC **Cómo usarlo.** Ejecutar el paso 1 y revisar la tabla. Activar los pasos 2–4 con sus banderas solo si se necesitan. Subir los Parquet generados al Volume respetando las carpetas indicadas.

# COMMAND ----------

# Si faltan librerías:
 %pip install pyodbc sqlalchemy duckdb pandas

# COMMAND ----------

import os, datetime
import pandas as pd, duckdb
from MODULOS.conexion_sql import get_connection

CARPETA_LOCAL = "D:/Datos/"     # misma raíz que usa 0100

# Banderas: activar solo lo que se necesite
EXTRAER_MOVS_ENTREGA   = False
EXTRAER_MOVS_FALTANTES = False
EXTRAER_RESERVA        = False

PERIODOS_DESARROLLO = [202601, 202602, 202603, 202604, 202605, 202606, 202607, 202608]
PERIODOS_FALTANTES  = [202606, 202607, 202608]     # solo los que no estén en el Volume
PERIODOS_RESERVA    = [202609]                      # agregar 202610 cuando el mes esté cerrado
ID_PROCESOS_ENTREGA = []                            # completar con lo que muestre el diagnóstico (paso 1)

# COMMAND ----------

def SQLServer_to_Parquet(ScriptSQL, ruta_parquet):
    """Igual a la función de 0100; además crea la carpeta de destino."""
    os.makedirs(os.path.dirname(ruta_parquet), exist_ok=True)
    conn = get_connection()
    if conn is None:
        print("No se pudo conectar a SQL Server"); return
    try:
        df = pd.read_sql(ScriptSQL, conn)
        print(f"Registros obtenidos: {len(df):,}")
        duckdb.sql(f"COPY (SELECT * FROM df) TO '{ruta_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        print("Parquet generado:", ruta_parquet)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

def rango(periodo):
    return f"""DECLARE @FECHA_INI DATETIME = CONVERT(DATETIME, CONCAT(CAST({periodo} AS VARCHAR(6)), '01'));
            DECLARE @FECHA_FIN DATETIME = DATEADD(MONTH, 1, @FECHA_INI);"""

COLUMNAS_GUIAS = """NUMERO_GUIA,
                ID_TIPOGUIA,
                ID_CLIENTE,
                ID_CANAL,
                ID_DIVISION_PROV,
                ID_1,
                ID_2,
                ID_PAIS_ORIGEN,
                ID_CIUDAD_ORIGEN,
                ID_PAIS_DESTINO,
                ID_CIUDAD_DESTINO,
                ID_TIPO_ZONA_URBA,
                ID_PAIS_ZONA_URB_ENTR,
                ID_CIUDAD_ZONA_URB_ENTR,
                ID_ZONA_URBA,
                ID_ESTADOENVIO,
                ID_ESTADOGUIA,
                ID_CARACT_ENVIO,
                ID_TIEMPO,
                ID_MEDTRANS,
                ID_PRODUCTO,
                ID_SUBPRODUCTO,
                ID_BODEPROV,
                ID_PAIS_UBICACION,
                ID_CIUDAD_UBICACION,
                ID_DIVISION_UBICACION,
                FECHA_ENVIO,
                LARGO_ENVIO,
                ALTO_ENVIO,
                ANCHO_ENVIO,
                LARGO_ENV_OPERA,
                ALTO_ENV_OPERA,
                ANCHO_ENV_OPERA,
                PESO_GUIA,
                PESO_FISICO_GUIA,
                PESO_VOLU_GUIA,
                PESO_FISICO_OPERA,
                PESO_VOLU_OPERA,
                PESO_FACTURADO,
                FECHA_ENTREGA,
                NUMERO_PIEZAS,
                CASE WHEN NULLIF(LTRIM(RTRIM(CAST(IDEN_PERS_RECIBE AS VARCHAR(200)))), '') IS NOT NULL THEN 'S' END AS IDEN_PERS_RECIBE,
                CASE WHEN NULLIF(LTRIM(RTRIM(CAST(NOMB_PERS_RECIBE AS VARCHAR(200)))), '') IS NOT NULL THEN 'S' END AS NOMB_PERS_RECIBE,
                ID_REMPLAZA,
                FECHA_ULTIMA_MOD,
                GUIA_EXTERNA,
                ID_DOCU_ULT_MOVI,
                NU_DOCU_ULT_MOVI,
                CONS_DOCU_ULT_MOVI,
                ID_FACTURADO,
                VALOR_DECLARADO_GUIA,
                VALOR_SEG_MAN_GUIAS,
                VALOR_FLETES_GUIAS,
                VALOR_DESCUENTO_GUIAS,
                VALOR_PEDIDO_GUIAS,
                VALOR_SEG_MAN_GUIAS_FACT,
                VALOR_FLETES_GUIAS_FACT,
                ID_REQUIERE_EMBALAJE,
                FECHA_CREACION,
                FECHA_MODIFICACION,
                FECHA_MODIFICACION_DG,
                FECHA_EXTRACCION_DW"""
COLUMNAS_MOVIMIENTOS = """ID_PROCESO,
                NUMERO_MOVIMI,
                CONSECUT_MOVIMI,
                NUMERO_GUIA,
                ID_CONCEPTO,
                ID_ESTA_FISICO,
                ID_PRODUCTO,
                ID_SUBPRODUCTO,
                ID_TIPOENVIO,
                ID_CONSECUTIVO_SEGUIMIENTO,
                ID_ENVIO_CRUZADO,
                PESO_ENVIO,
                LARGO_ENVIO,
                ANCHO_ENVIO,
                ALTO_ENVIO,
                HORA_INICIAL_ENTREGA,
                HORA_FINAL_ENTREGA,
                FECHA_HORA_MOVIMIENTO,
                ID_MODO_REGISTRO,
                NUMERO_PIEZAS,
                ID_EST_EMBALAJE,
                ID_TIPOENVIO_PRODUCTIVIDAD,
                DEVOLUCION_REGIONAL,
                FECHA_CREACION,
                FECHA_MODIFICACION,
                ID_REPLICADO,
                FECHA_EXTRACCION_DW"""
COLUMNAS_DOCUMENTOS = """ID_PROCESO,
                NUMERO_MOVIMI,
                CONSECUT_MOVIMI,
                ID_TIPO_ZONA_URBA,
                ID_PAIS_ZONA_URBA,
                ID_CIUDAD_ZONA_URBA,
                ID_ZONA_URBA,
                ID_PAIS_DIVISION_ORIGEN,
                ID_CIUDAD_DIVISION_ORIGEN,
                ID_DIVISION_ORIGEN,
                ID_PAIS_DESTINO,
                ID_CIUDAD_DESTINO,
                ID_DIVISION_DESTINO,
                ID_PAIS_OPERATIVO,
                ID_CIUDAD_OPERATIVO,
                ID_OPERATIVO,
                ID_RUTA,
                ID_PAIS_CANAL,
                ID_CIUDAD_CANAL,
                ID_CANAL,
                ID_SUCURSAL,
                ID_DEPENDEN,
                ID_TIPOCONTENE,
                ID_OPERADOR_LOGISTIC,
                ID_TIPO_LOGISTICA,
                ID_TIPO_DOCUMENTO,
                FECHA_HORA_MOVIMI,
                FECHA_HORA_CIERRE,
                TOTAL_ENVIOS,
                PESO_TOTAL,
                ESTADO_MOVIMI,
                NUME_SELLO_SEGURIDAD,
                ID_TIPO_MOVIMIENTO,
                ID_ESTADO_TRANS_ZONA,
                TOTAL_BOLSAS,
                FECHA_CREACION,
                FECHA_MODIFICACION,
                ID_REPLICADO,
                FECHA_MODIFICACION_QV,
                FECHA_EXTRACCION_DW"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Diagnóstico de procesos (solo conteos, un mes)

# COMMAND ----------

conn = get_connection()
diag = pd.read_sql(f"""
    {rango(202605)}
    SELECT S.ID_PROCESO, P.NOMBRE_PROCESO_LOG, COUNT(*) AS MOVIMIENTOS, COUNT(DISTINCT S.NUMERO_GUIA) AS GUIAS
    FROM LOG.SEGUIMIENTO_ENVIO S WITH(NOLOCK)
    LEFT JOIN PAR.PROCESOS P WITH(NOLOCK) ON P.ID_PROCESO = S.ID_PROCESO
    WHERE S.FECHA_HORA_MOVIMIENTO >= @FECHA_INI AND S.FECHA_HORA_MOVIMIENTO < @FECHA_FIN
    GROUP BY S.ID_PROCESO, P.NOMBRE_PROCESO_LOG
    ORDER BY MOVIMIENTOS DESC
    OPTION(RECOMPILE);
""", conn)
conn.close()
display(diag)
os.makedirs(f"{CARPETA_LOCAL}DIAGNOSTICO/", exist_ok=True)
diag.to_csv(f"{CARPETA_LOCAL}DIAGNOSTICO/procesos_seguimiento_202605.csv", index=False)
print("Buscar procesos de ENTREGA, ENTREGADO, DEVOLUCIÓN, NOVEDAD o INTENTO FALLIDO y escribirlos en ID_PROCESOS_ENTREGA.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Movimientos de entrega (carpeta aparte: no cambia lo que leen 0300, 0310 y 0330)

# COMMAND ----------

if EXTRAER_MOVS_ENTREGA:
    assert ID_PROCESOS_ENTREGA, "Completar ID_PROCESOS_ENTREGA con el resultado del paso 1"
    ids = ", ".join(str(i) for i in ID_PROCESOS_ENTREGA)
    for periodo in PERIODOS_DESARROLLO:
        SQLServer_to_Parquet(f"""
            {rango(periodo)}
            SELECT {COLUMNAS_MOVIMIENTOS}
            FROM LOG.SEGUIMIENTO_ENVIO WITH(NOLOCK)
            WHERE ID_PROCESO IN ({ids})
              AND FECHA_HORA_MOVIMIENTO >= @FECHA_INI AND FECHA_HORA_MOVIMIENTO < @FECHA_FIN
            OPTION(RECOMPILE);""",
            f"{CARPETA_LOCAL}LOG_SEGUIMIENTO_ENTREGA/LOG_SEGUIMIENTO_ENTREGA-{periodo}.parquet")
else:
    print("Paso 2 desactivado (EXTRAER_MOVS_ENTREGA = False).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Meses faltantes de movimientos 5, 6 y 9 (mismo nombre de archivo que 0100)

# COMMAND ----------

if EXTRAER_MOVS_FALTANTES:
    for periodo in PERIODOS_FALTANTES:
        SQLServer_to_Parquet(f"""
            {rango(periodo)}
            SELECT {COLUMNAS_MOVIMIENTOS}
            FROM LOG.SEGUIMIENTO_ENVIO WITH(NOLOCK)
            WHERE ID_PROCESO IN (5, 6, 9)
              AND FECHA_HORA_MOVIMIENTO >= @FECHA_INI AND FECHA_HORA_MOVIMIENTO < @FECHA_FIN
            OPTION(RECOMPILE);""",
            f"{CARPETA_LOCAL}LOG_SEGUIMIENTO_ENVIO/LOG_SEGUIMIENTO_ENVIO-{periodo}.parquet")
else:
    print("Paso 3 desactivado (EXTRAER_MOVS_FALTANTES = False).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Periodo de reserva (carpeta aparte; no entra en la base de desarrollo)
# MAGIC
# MAGIC Se extraen guías, movimientos y documentos del mes de reserva con las mismas columnas que esperan `0300` versión 6 y `0330`. Para usarlo como prueba final independiente, se construye su base apuntando las rutas a `RESERVA_POSTERIOR/` y se evalúa una sola vez, al final, con el modelo ya fijado.

# COMMAND ----------

if EXTRAER_RESERVA:
    ids_mov = ", ".join(str(i) for i in [5, 6, 9] + list(ID_PROCESOS_ENTREGA))
    for periodo in PERIODOS_RESERVA:
        base = f"{CARPETA_LOCAL}RESERVA_POSTERIOR/"
        SQLServer_to_Parquet(f"""
            {rango(periodo)}
            SELECT {COLUMNAS_GUIAS}
            FROM LOG.GUIAS WITH(NOLOCK)
            WHERE FECHA_ENVIO >= @FECHA_INI AND FECHA_ENVIO < @FECHA_FIN
            OPTION(RECOMPILE);""", f"{base}LOG_GUIAS/LOG_GUIAS-{periodo}.parquet")
        SQLServer_to_Parquet(f"""
            {rango(periodo)}
            SELECT {COLUMNAS_MOVIMIENTOS}
            FROM LOG.SEGUIMIENTO_ENVIO WITH(NOLOCK)
            WHERE ID_PROCESO IN ({ids_mov})
              AND FECHA_HORA_MOVIMIENTO >= @FECHA_INI AND FECHA_HORA_MOVIMIENTO < @FECHA_FIN
            OPTION(RECOMPILE);""", f"{base}LOG_SEGUIMIENTO_ENVIO/LOG_SEGUIMIENTO_ENVIO-{periodo}.parquet")
        SQLServer_to_Parquet(f"""
            {rango(periodo)}
            SELECT {COLUMNAS_DOCUMENTOS}
            FROM LOG.DOCUMENTOS WITH(NOLOCK)
            WHERE FECHA_HORA_MOVIMI >= @FECHA_INI AND FECHA_HORA_MOVIMI < @FECHA_FIN
            OPTION(RECOMPILE);""", f"{base}LOG_DOCUMENTOS/LOG_DOCUMENTOS-{periodo}.parquet")
else:
    print("Paso 4 desactivado (EXTRAER_RESERVA = False).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué entregar
# MAGIC
# MAGIC - `DIAGNOSTICO/procesos_seguimiento_202605.csv` (solo conteos): con él se responde por escrito la pregunta 1 del asesor.
# MAGIC - Si se activaron los pasos 2–4: subir las carpetas `LOG_SEGUIMIENTO_ENTREGA/`, `LOG_SEGUIMIENTO_ENVIO/` (meses faltantes) y `RESERVA_POSTERIOR/` al Volume `/Volumes/proyectogrado/source/source/`, sin mezclar `RESERVA_POSTERIOR` con las carpetas de desarrollo.