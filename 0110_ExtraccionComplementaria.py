# Databricks notebook source
# !pip install duckdb
# !pip install sqlalchemy
# !pip install pyodbc
# !pip install pandas
# !pip install polars


# COMMAND ----------

import datetime 
from MODULOS.conexion_sql import get_connection
from MODULOS.conexion_sql import get_sqlalchemy_engine
import duckdb
import pandas as pd

# COMMAND ----------

def SQLServer_to_Parquet(ScriptSQL, ruta_parquet):
    
    conn = get_connection()
    if conn is None:
        print("No se pudo conectar a SQL Server")
        return
    try:
        query = f"""
                {ScriptSQL}
        """
        df = pd.read_sql(query, conn)
        print(f"Registros obtenidos: {len(df):,}")
        duckdb.sql(f"""
            COPY (
                SELECT *
                FROM df
            )
            TO '{ruta_parquet}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD
            )
        """)
        print("Parquet generado correctamente")
        print(ruta_parquet)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

# COMMAND ----------

# RUTA_PARQUET = "D:/Datos/PAR_ESTADOSENVIO.parquet"
# SCRIPT_SQL = "SELECT * FROM PAR.ESTADOSENVIO WITH(NOLOCK)"
# SQLServer_to_Parquet(
#     SCRIPT_SQL,
#     RUTA_PARQUET
# )

# df_resultado = duckdb.sql(f"""
#     SELECT *
#     FROM '{RUTA_PARQUET}'
#     LIMIT 5
# """).df()
# display(df_resultado)

# COMMAND ----------

ARCHIVO_PARQUET = "LOG_SEGUIMIENTO_ENVIO"

periodos = [
    202605,
    202604,
    202603,
    202602,
    202601
]

for periodo in periodos:
    print(f"Generando parquet para el periodo: {periodo}")
    RUTA_PARQUET = f"D:/Datos/{ARCHIVO_PARQUET}/{ARCHIVO_PARQUET}-{periodo}.parquet"
    
    # Se agregó la 'f' al inicio de la triple comilla
    SCRIPT_SQL = f"""
            DECLARE @FECHAID INT = {periodo};
            -- Se usa CAST(@FECHAID AS VARCHAR(6)) para asegurar el formato correcto antes de concatenar '01'
            DECLARE @FECHA_INI DATETIME = CONVERT(DATETIME, CONCAT(CAST(@FECHAID AS VARCHAR(6)), '01'));
            DECLARE @FECHA_FIN DATETIME = DATEADD(MONTH, 1, @FECHA_INI);
            
            SELECT 
                * 
            FROM LOG.SEGUIMIENTO_ENVIO WITH(NOLOCK) 
            WHERE 
                [ID_PROCESO] IN (5, 6, 9)
                AND FECHA_HORA_MOVIMIENTO >= @FECHA_INI 
                AND FECHA_HORA_MOVIMIENTO < @FECHA_FIN
            OPTION(RECOMPILE);
    """
    
    SQLServer_to_Parquet(
        SCRIPT_SQL,
        RUTA_PARQUET
    )
    print(f"Finaliza periodo: {periodo}")


# COMMAND ----------

df_resultado = duckdb.sql(f"""
    SELECT *
    FROM '{RUTA_PARQUET}'
    LIMIT 5
""").df()
display(df_resultado)

# COMMAND ----------

ARCHIVO_PARQUET = "LOG_GUIAS"

periodos = [
    202608,
    202607,
    202606,
    202605,
    202604,
    202603,
    202602,
    202601
]

for periodo in periodos:
    print(f"Generando parquet para el periodo: {periodo}")
    RUTA_PARQUET = f"D:/Datos/{ARCHIVO_PARQUET}/{ARCHIVO_PARQUET}-{periodo}.parquet"
    
    # Se agregó la 'f' al inicio de la triple comilla
    SCRIPT_SQL = f"""
            DECLARE @FECHAID INT = {periodo};
            -- Se usa CAST(@FECHAID AS VARCHAR(6)) para asegurar el formato correcto antes de concatenar '01'
            DECLARE @FECHA_INI DATETIME = CONVERT(DATETIME, CONCAT(CAST(@FECHAID AS VARCHAR(6)), '01'));
            DECLARE @FECHA_FIN DATETIME = DATEADD(MONTH, 1, @FECHA_INI);
            
            SELECT 
                * 
            FROM LOG.GUIAS WITH(NOLOCK) 
            WHERE 
                    FECHA_ENVIO >= @FECHA_INI 
                AND FECHA_ENVIO < @FECHA_FIN
            OPTION(RECOMPILE);
    """
    
    SQLServer_to_Parquet(
        SCRIPT_SQL,
        RUTA_PARQUET
    )
    print(f"Finaliza periodo: {periodo}")


# COMMAND ----------

ARCHIVO_PARQUET = "LOG_DOCUMENTOS"

periodos = [
    202608,
    202607,
    202606,
    202605,
    202604,
    202603,
    202602,
    202601
]

for periodo in periodos:
    print(f"Generando parquet para el periodo: {periodo}")
    RUTA_PARQUET = f"D:/Datos/{ARCHIVO_PARQUET}/{ARCHIVO_PARQUET}-{periodo}.parquet"
    
    # Se agregó la 'f' al inicio de la triple comilla
    SCRIPT_SQL = f"""
            DECLARE @FECHAID INT = {periodo};
            -- Se usa CAST(@FECHAID AS VARCHAR(6)) para asegurar el formato correcto antes de concatenar '01'
            DECLARE @FECHA_INI DATETIME = CONVERT(DATETIME, CONCAT(CAST(@FECHAID AS VARCHAR(6)), '01'));
            DECLARE @FECHA_FIN DATETIME = DATEADD(MONTH, 1, @FECHA_INI);
            
            SELECT 
                * 
            FROM LOG.DOCUMENTOS WITH(NOLOCK) 
            WHERE 
                    FECHA_HORA_MOVIMI >= @FECHA_INI 
                AND FECHA_HORA_MOVIMI < @FECHA_FIN
            OPTION(RECOMPILE);
    """
    
    SQLServer_to_Parquet(
        SCRIPT_SQL,
        RUTA_PARQUET
    )
    print(f"Finaliza periodo: {periodo}")


# COMMAND ----------

#  PARAMETRICAS GENERALES BASICAS 
ARCHIVO_PARQUET = "PARAMETRICAS"

TBL_PARAMETRICAS = [
 'PAR.REGIONALES_PAIS'
,'PAR.CIUDADES'
,'PAR.ESTADOSENVIO'
,'PAR.PROCESOS'
,'PAR.ESTADOSGUIA'
,'PAR.ZONASURBANAS'
,'DIM.VW_TIEMPO_CALENDARIO_Y_FESTIVOS'
]

for tabla in TBL_PARAMETRICAS:
    print(f"Generando Tabla: {tabla}")
    RUTA_PARQUET = f"D:/Datos/{ARCHIVO_PARQUET}/{tabla}.parquet"
    SCRIPT_SQL = f"""
            SELECT 
                * 
            FROM {tabla} WITH(NOLOCK) 
            OPTION(RECOMPILE);
    """
    
    SQLServer_to_Parquet(
        SCRIPT_SQL,
        RUTA_PARQUET
    )
    print(f"Finaliza carga de {tabla}: {RUTA_PARQUET}")


# COMMAND ----------

#  PARAMETRICAS RED OPERATIVA
ARCHIVO_PARQUET = "PARAMETRICAS"
NOMBRE_TABLA = "PAR.RED_OPERATIVA_OFERTA"
SQLServer_Script = f"""
SELECT * FROM [TMP].[MPCCO_VJ_PARAMETROS_RED_OPERATIVA] WITH(NOLOCK)
"""

print(f"Generando Tabla: {NOMBRE_TABLA}")
RUTA_PARQUET = f"D:/Datos/{ARCHIVO_PARQUET}/{NOMBRE_TABLA}.parquet"
SCRIPT_SQL = SQLServer_Script

SQLServer_to_Parquet(
    SCRIPT_SQL,
    RUTA_PARQUET
)
print(f"Finaliza carga de {NOMBRE_TABLA}: {RUTA_PARQUET}")