# Databricks notebook source
# MAGIC %md
# MAGIC # 0320 · Diagnóstico de la multiplicación de filas (propuesta P2)
# MAGIC
# MAGIC **Qué hace.** Repite, una a una, las uniones de la celda de `BASE_DIMENSIONADA` (0300, pos. 38) y cuenta filas y guías únicas después de cada una. También mide cuántas veces se repite la clave en cada parametría.
# MAGIC
# MAGIC **Por qué.** La tabla base tiene 3.053.448 filas (una por guía) y `BASE_DIMENSIONADA` 4.945.549. En algún paso se multiplican las filas; retirar el `GROUP BY` no lo corrige (ver P2 del documento).
# MAGIC
# MAGIC **Requisitos.** Haber ejecutado 0300 hasta la posición 30, que crea `LOG_BASE_BOGOTA.parquet`.
# MAGIC
# MAGIC **Cómo usarlo.** Revisar las rutas y ejecutar todo. El conteo incremental hace diez consultas sobre unos 3 millones de filas: puede tardar.
# MAGIC
# MAGIC **Salidas.** Dos tablas agregadas y, para cada parametría con claves repetidas, hasta 10 ejemplos de claves repetidas (sin columnas de personas ni placas).

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funciones (no es necesario modificarlas)

# COMMAND ----------

# =====================================================================================
# P2 · DIAGNÓSTICO DE LA MULTIPLICACIÓN DE FILAS (fan-out) EN BASE_DIMENSIONADA
# =====================================================================================
import duckdb, pandas as pd

CLAVES = {  # la clave que usa la celda 38 de 0300
    "CIUDADES": ("CIUDADES", "ID_PAIS, ID_CIUDAD"), "REGIONALES_PAIS": ("REGIONALES", "ID_REGIONAL"),
    "ESTADOSENVIO": ("EST_ENVIO", "ID_ESTADOENVIO"), "ESTADOSGUIA": ("EST_GUIA", "ID_ESTADOGUIA"),
    "RED_OPERATIVA_OFERTA": ("RED", "ID_CIUDAD_ORIGEN, ID_CIUDAD_DESTINO, ID_TIEMPO, ID_MEDTRANS"),
    "PROCESOS": ("PROCESOS", "ID_PROCESO"),
    "ZONASURBANAS": ("ZONAS", "ID_PAIS, ID_CIUDAD, ID_TIPO_ZONA_URBA, ID_ZONA_URBA")}

def multiplicidad_claves(P):
    return pd.concat([duckdb.sql(f"""
        WITH c AS (SELECT {k}, COUNT(*) n FROM read_parquet('{P[r]}') GROUP BY ALL)
        SELECT '{t}' tabla, SUM(n) filas, COUNT(*) claves_distintas,
               COUNT(*) FILTER (WHERE n > 1) claves_repetidas, MAX(n) max_repeticion FROM c""").df()
        for t, (r, k) in CLAVES.items()], ignore_index=True)

def conteo_incremental(RUTA_TABLA_BASE, P):
    pasos = [
      ("+ CIUDAD origen",   f"JOIN read_parquet('{P['CIUDADES']}') CO ON TB.G_ID_PAIS_ORIGEN=CO.ID_PAIS AND TB.G_ID_CIUDAD_ORIGEN=CO.ID_CIUDAD"),
      ("+ REGIONAL origen", f"JOIN read_parquet('{P['REGIONALES']}') RO ON RO.ID_REGIONAL=CO.ID_REGIONAL"),
      ("+ CIUDAD destino",  f"JOIN read_parquet('{P['CIUDADES']}') CD ON TB.G_ID_PAIS_DESTINO=CD.ID_PAIS AND TB.G_ID_CIUDAD_DESTINO=CD.ID_CIUDAD"),
      ("+ REGIONAL destino",f"JOIN read_parquet('{P['REGIONALES']}') RD ON RD.ID_REGIONAL=CD.ID_REGIONAL"),
      ("+ ESTADOSENVIO",    f"JOIN read_parquet('{P['EST_ENVIO']}') EE ON TB.G_ID_ESTADOENVIO=EE.ID_ESTADOENVIO"),
      ("+ ESTADOSGUIA",     f"JOIN read_parquet('{P['EST_GUIA']}') EG ON TB.G_ID_ESTADOGUIA=EG.ID_ESTADOGUIA"),
      ("+ RED_OPERATIVA (LEFT)", f"""LEFT JOIN read_parquet('{P['RED']}') RE ON TB.G_ID_CIUDAD_ORIGEN=RE.ID_CIUDAD_ORIGEN
          AND TB.G_ID_CIUDAD_DESTINO=RE.ID_CIUDAD_DESTINO
          AND CASE WHEN COALESCE(TB.G_ID_TIEMPO,0)<>0 THEN TB.G_ID_TIEMPO ELSE 1 END=RE.ID_TIEMPO
          AND CASE WHEN COALESCE(TB.G_ID_MEDTRANS,0)<>0 THEN TB.G_ID_MEDTRANS ELSE 1 END=RE.ID_MEDTRANS"""),
      ("+ PROCESOS",        f"JOIN read_parquet('{P['PROCESOS']}') PR ON TB.SEG_ID_PROCESO=PR.ID_PROCESO"),
      ("+ ZONAS (LEFT)",    f"""LEFT JOIN read_parquet('{P['ZONAS']}') ZU ON ZU.ID_TIPO_ZONA_URBA=TB.DOC_ID_TIPO_ZONA_URBA
          AND ZU.ID_ZONA_URBA=TB.DOC_ID_ZONA_URBA AND ZU.ID_CIUDAD=TB.DOC_ID_CIUDAD_ZONA_URBA AND ZU.ID_PAIS=1""")]
    
    q = f"SELECT COUNT(*), COUNT(DISTINCT TB.G_NUMERO_GUIA) FROM read_parquet('{RUTA_TABLA_BASE}') TB"
    
    # CORREGIDO: Usamos .df().iloc[0].values para extraer los dos valores numéricos de forma segura
    base_vals = duckdb.sql(q).df().iloc[0].values
    filas, joins = [("tabla base", base_vals[0], base_vals[1])], ""
    
    for nombre, j in pasos:
        joins += "\n" + j
        step_vals = duckdb.sql(q + joins).df().iloc[0].values
        filas.append((nombre, step_vals[0], step_vals[1]))
        
    out = pd.DataFrame(filas, columns=["paso", "filas", "guias_unicas"])
    out["filas_por_guia"] = (out.filas / out.guias_unicas).round(4)
    out["guias_perdidas_vs_base"] = out.guias_unicas.iloc[0] - out.guias_unicas
    return out

def ver_claves_repetidas(tabla, max_claves=10):
    """Ejemplos de claves repetidas en una parametría, sin columnas de personas, placas o logins."""
    r, k = CLAVES[tabla]
    return duckdb.sql(f"""
        WITH rep AS (SELECT {k} FROM read_parquet('{P[r]}') GROUP BY ALL HAVING COUNT(*) > 1 LIMIT {max_claves})
        SELECT COLUMNS(c -> NOT regexp_matches(c, 'LOGIN|PLACA|CONDUCTOR|AUXILIAR|NOMB_|TELEF|DIRECC|IDEN_'))
        FROM read_parquet('{P[r]}') T JOIN rep USING ({k}) ORDER BY ALL""").df()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejecución

# COMMAND ----------

multiplicidad = multiplicidad_claves(P)
display(multiplicidad)
multiplicidad.to_csv(f"{CARPETA_SALIDAS}P2_multiplicidad_claves.csv", index=False)

incremental = conteo_incremental(RUTA_TABLA_BASE, P)
display(incremental)
incremental.to_csv(f"{CARPETA_SALIDAS}P2_conteo_incremental.csv", index=False)

# import shutil
# import time

# # --- PASO 1: Multiplicidad de claves ---
# multiplicidad = multiplicidad_claves(P)
# display(multiplicidad)
# multiplicidad.to_csv(f"{CARPETA_SALIDAS}P2_multiplicidad_claves.csv", index=False)


# # --- PASO 2: Conteo incremental ---
# incremental = conteo_incremental(RUTA_TABLA_BASE, P)
# display(incremental)
# incremental.to_csv(f"{CARPETA_SALIDAS}P2_conteo_incremental.csv", index=False)


# # --- PASO 3: Clima y Cruce con DuckDB (Protegido contra errores de E/S en Volumes) ---
# df_puntos = pd.read_parquet(RUTA_CLIMA_PUNTOS)
# clima = clima_ciudad_rezagado(df_puntos)

# # Guardar temporalmente el archivo intermedio en el disco local del nodo (/tmp)
# timestamp = int(time.time())
# ruta_local_clima = f"/tmp/tmp_CLIMA_{timestamp}.parquet"
# clima.to_parquet(ruta_local_clima, index=False)

# # Ruta local temporal para la salida de DuckDB antes de pasarla al Volume
# ruta_local_gold_v2_clima = f"/tmp/tmp_GOLD_V2_ENVIO_CLIMA_{timestamp}.parquet"

# # Ejecutamos la consulta en DuckDB apuntando al archivo local
# duckdb.sql(f"""
#     COPY ({SQL_CRUCE.format(RUTA_GOLD_V2=RUTA_GOLD_V2, RUTA_CLIMA_REZAGADO=ruta_local_clima)})
#     TO '{ruta_local_gold_v2_clima}' (FORMAT PARQUET, COMPRESSION ZSTD)
# """)

# # Copiamos el resultado final ya consolidado hacia tu Volume de Unity Catalog de forma segura
# shutil.copy(ruta_local_gold_v2_clima, RUTA_GOLD_V2_CLIMA)


# # --- PASO 4: Cobertura y Salida final ---
# cobertura = duckdb.sql(SQL_COBERTURA.format(RUTA_GOLD_V2_CLIMA=RUTA_GOLD_V2_CLIMA)).df()
# display(cobertura)
# cobertura.to_csv(f"{CARPETA_SALIDAS}P5_cobertura.csv", index=False)


# COMMAND ----------

# Ejemplos de claves repetidas en cada parametría que las tenga
for tabla in multiplicidad.loc[multiplicidad.claves_repetidas > 0, "tabla"]:
    print(tabla); display(ver_claves_repetidas(tabla))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo leer los resultados
# MAGIC
# MAGIC 1. En `conteo_incremental`, buscar el primer paso donde `filas_por_guia` deja de ser 1,00: esa unión multiplica filas. Si `guias_perdidas_vs_base` sube, una unión interna está perdiendo guías.
# MAGIC 2. En `multiplicidad_claves`, la tabla de ese paso debería tener `claves_repetidas > 0`.
# MAGIC 3. En los ejemplos de claves repetidas, ver qué columna distingue las filas repetidas (estado activo/inactivo, fecha de vigencia, versión). Esa columna define la regla para quedarse con una sola fila, y esa regla debe confirmarla Servienvios.
# MAGIC
# MAGIC La corrección **no** es un `DISTINCT`: es reducir la parametría a una fila por clave con la regla confirmada (así lo hace la base v2 del cuaderno 0330).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué entregar
# MAGIC
# MAGIC Los CSV `P2_multiplicidad_claves` y `P2_conteo_incremental`, y una nota que diga qué tabla y qué columna explican la repetición.