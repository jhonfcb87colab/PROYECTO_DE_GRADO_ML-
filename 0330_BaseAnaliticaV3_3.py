# Databricks notebook source
# MAGIC %md
# MAGIC # 0330 · Base analítica v3: SLA en días hábiles (una fila por guía)
# MAGIC
# MAGIC **Qué cambia frente a la v2.** La corrida del 20-10-2026 de `0330_BaseAnaliticaV2` pasó los controles de unidad, pero dio cerca de 80 % de incumplimiento (la empresa reporta alrededor de 20 %), 58 % de guías ya vencidas antes de T0 y solo 38 % de guías con SLA único, porque `RED_OPERATIVA_OFERTA` repite la clave de ruta hasta 5 veces. Esta versión aplica las decisiones del equipo del 21-10-2026:
# MAGIC
# MAGIC 1. **El SLA se cuenta en días hábiles** con el calendario corporativo `DIM.VW_TIEMPO_CALENDARIO_Y_FESTIVOS` y `TIEMPO_ENTREGA_COMERCIAL_DIA`. El vencimiento es el **final del n-ésimo día hábil** después de la admisión (sin contar el día de admisión; contarlo queda como sensibilidad).
# MAGIC 2. **Una fila de RED_OPERATIVA por clave con regla explícita**: la ruta activa más reciente (`ID_ESTADO_RUTA`, `FECHA_MODIFICACION`). La alternativa (el plazo más holgado) se reporta como sensibilidad.
# MAGIC 3. **Devoluciones y siniestros aparte**: las guías `ENTREGADO A REMITENTE` y `SINIESTRADO` no cuentan como entregas; su etiqueta y su última milla quedan nulas.
# MAGIC 4. **Guía seudonimizada**: la columna `guia` de la salida es `md5(guía || sal)`; la sal se guarda en el Volume, fuera del cuaderno y de GitHub. `ID_CLIENTE` se toma de `LOG_GUIAS_BOGOTA`, donde `0300` versión 6 ya lo guarda seudonimizado, para que `0410` no tenga que volver a cruzar con la guía real.
# MAGIC
# MAGIC **Qué no cambia.** T0 = primera salida a zona urbana (proceso 9), `FECHA_ENTREGA` como desenlace (alternativa A, validada en `0310`), deduplicación de documentos y parametrías, y los controles que detienen el cuaderno si hay más filas que guías.
# MAGIC
# MAGIC **Salidas.** Todo va a la carpeta nueva `REVISION_2026_10/V3/`; la base v2 y sus CSV no se tocan.
# MAGIC
# MAGIC **Cómo usarlo.**
# MAGIC 1. Ejecutar la sección **Paso 0** (solo conteos) y revisar que la condición de día hábil y la regla de RED tengan sentido con lo que muestra.
# MAGIC 2. Ejecutar el resto. La celda de controles se detiene si falla la unidad o el dominio de la etiqueta.
# MAGIC 3. Entregar los CSV `P3_*`, la tabla de sensibilidad y el manifiesto.
# MAGIC
# MAGIC > **Versión 3.2 (24-10-2026).** Igual a la anterior, salvo: ruta nueva del Volume (`/Volumes/proyectogrado/source/source/`), `%pip install "duckdb<2"` activo y la función `copy_a_volume` (celda 2), que hace que cada `COPY ... TO` de DuckDB escriba primero en `/tmp` y luego copie el parquet al Volume. Así se evita el error `Async write failed ... Input/output error (errno 5)` de Databricks serverless.
# MAGIC
# MAGIC > **Versión 3.3 (25-10-2026) · copia de trabajo.** Añade al SELECT final de `construir_gold_v3` cuatro columnas nuevas: `fecha_promesa_entrega` (admisión + `TIEMPO_ENTREGA_COMERCIAL_DIA` en **días calendario**, la regla recalculada en `0200` celda 23), `dias_entrega_real` (días calendario de admisión a entrega), `dias_vs_promesa` (> 0: retraso respecto a la promesa) y `cumplimiento_promesa` (1/0/NULL: entregada dentro del día promesa, con el mismo dominio que `incumplimiento_sla`). Todo va a la carpeta propia `REVISION_2026_10/V3_3/`: la base `V3/GOLD_V3_ENVIO.parquet` y los CSV `V3/P3_*` de la corrida anterior no se tocan.

# COMMAND ----------

# MAGIC %pip install "duckdb<2"

# COMMAND ----------

# CONFIGURACIÓN · revisar las rutas antes de ejecutar (son las mismas de 0300 y 0330 v2)
import os
PATH_DATABRICKS = "/Volumes/proyectogrado/source/source/"
RUTA_GUIAS      = f"{PATH_DATABRICKS}LOG_GUIAS/RAW/LOG_GUIAS_BOGOTA.parquet"                 # la crea 0300, pos. 10
RUTA_MOVS_GLOB  = f"{PATH_DATABRICKS}LOG_SEGUIMIENTO_ENVIO/LOG_SEGUIMIENTO_ENVIO*.parquet"    # TODOS los movimientos
RUTA_DOCS_GLOB  = f"{PATH_DATABRICKS}LOG_DOCUMENTOS/LOG_DOCUMENTOS*.parquet"                 # TODOS los documentos
PAR = f"{PATH_DATABRICKS}PARAMETRICAS/"
P = {"CIUDADES":   f"{PAR}PAR.CIUDADES.parquet",       "REGIONALES": f"{PAR}PAR.REGIONALES_PAIS.parquet",
     "EST_ENVIO":  f"{PAR}PAR.ESTADOSENVIO.parquet",   "EST_GUIA":   f"{PAR}PAR.ESTADOSGUIA.parquet",
     "RED":        f"{PAR}PAR.RED_OPERATIVA_OFERTA.parquet", "PROCESOS": f"{PAR}PAR.PROCESOS.parquet",
     "ZONAS":      f"{PAR}PAR.ZONASURBANAS.parquet"}
RUTA_CALENDARIO = f"{PAR}DIM.VW_TIEMPO_CALENDARIO_Y_FESTIVOS.parquet"
# Sal para seudonimizar la guía: archivo en el Volume, nunca en el cuaderno ni en GitHub
RUTA_SAL_GUIA   = f"{PATH_DATABRICKS}SEGURIDAD/sal_guia.txt"
# Salidas: carpeta propia de la v3.3 (copia de trabajo); NADA de la carpeta V3/ se sobrescribe
# v3.3 · carpeta nueva para no dañar la corrida original de la 3.2
CARPETA_SALIDAS = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3_3/"
os.makedirs(CARPETA_SALIDAS, exist_ok=True)
RUTA_GOLD_V3 = f"{CARPETA_SALIDAS}GOLD_V3_ENVIO.parquet"

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
# MAGIC ## Paso 0 · Diagnóstico corto (solo conteos)
# MAGIC
# MAGIC Antes de construir la base: qué valores trae el calendario, si cubre todos los vencimientos posibles y qué cambia entre las filas repetidas de `RED_OPERATIVA_OFERTA`. Nada de esto muestra guías.

# COMMAND ----------

import duckdb
cal = f"read_parquet('{RUTA_CALENDARIO}')"
print("Columnas del calendario:", duckdb.sql(f"DESCRIBE SELECT * FROM {cal}").df()[["column_name", "column_type"]].values.tolist())

# a) Tipos de día en 2026 y si algún día marcado como hábil tiene fecha festiva
d0a = duckdb.sql(f"""
    SELECT TIPO_DIA, ID_TIPO_DIA, COUNT(*) AS dias,
           COUNT(*) FILTER (WHERE FECHA_FESTIVA IS NOT NULL) AS con_fecha_festiva,
           list_distinct(list(dayofweek(CAST("DATE" AS DATE)))) AS dias_semana   -- 0 = domingo
    FROM {cal} WHERE CAST("DATE" AS DATE) BETWEEN DATE '2026-01-01' AND DATE '2026-12-31'
    GROUP BY ALL ORDER BY ALL""").df()
display(d0a)

# b) Cobertura: el calendario debe llegar más allá del último vencimiento posible (envío + plazo máximo)
d0b = duckdb.sql(f"""SELECT MIN(CAST("DATE" AS DATE)) AS primera_fecha, MAX(CAST("DATE" AS DATE)) AS ultima_fecha,
                            COUNT(*) AS filas, COUNT(DISTINCT CAST("DATE" AS DATE)) AS fechas_distintas
                     FROM {cal}""").df()
display(d0b)

# c) RED_OPERATIVA: en las claves repetidas, ¿qué columnas cambian?
red = f"read_parquet('{P['RED']}')"
print("Tipo de ID_ESTADO_RUTA:", duckdb.sql(f"SELECT any_value(typeof(ID_ESTADO_RUTA)) FROM {red}").fetchall()[0][0])
d0c = duckdb.sql(f"""
    WITH g AS (
        SELECT ID_CIUDAD_ORIGEN, ID_CIUDAD_DESTINO, ID_TIEMPO, ID_MEDTRANS, COUNT(*) AS n,
               COUNT(DISTINCT TIEMPO_ENTREGA_COMERCIAL_DIA) AS n_dia, COUNT(DISTINCT ID_ESTADO_RUTA) AS n_estado,
               COUNT(DISTINCT ID_IMPEDANCIA) AS n_imp, COUNT(DISTINCT ID_FILTRO_CRITICIDAD) AS n_filtro,
               COUNT(DISTINCT FECHA_MODIFICACION) AS n_fmod,
               COUNT(*) FILTER (WHERE COALESCE(CAST(ID_ESTADO_RUTA AS INT), 0) = 1) AS n_activas
        FROM {red} GROUP BY ALL HAVING COUNT(*) > 1)
    SELECT COUNT(*) AS claves_repetidas,
           AVG((n_dia > 1)::INT)    AS pct_con_sla_distinto,
           AVG((n_estado > 1)::INT) AS pct_estado_distinto,
           AVG((n_imp > 1)::INT)    AS pct_impedancia_distinta,
           AVG((n_filtro > 1)::INT) AS pct_filtro_distinto,
           AVG((n_fmod > 1)::INT)   AS pct_fecha_modificacion_distinta,
           AVG((n_activas = 0)::INT) AS pct_sin_ruta_activa,
           AVG((n_activas > 1)::INT) AS pct_varias_activas
    FROM g""").df()
display(d0c)

# d) Distribución del plazo en días (y si hay plazos no enteros)
d0d = duckdb.sql(f"""SELECT TIEMPO_ENTREGA_COMERCIAL_DIA AS sla_dias, COUNT(*) AS filas_red,
                            AVG((TIEMPO_ENTREGA_COMERCIAL_HOR = TIEMPO_ENTREGA_COMERCIAL_DIA * 24)::INT) AS pct_hor_igual_dia_x24
                     FROM {red} GROUP BY 1 ORDER BY 1""").df()
display(d0d)
for n, t in {"0a_tipos_dia": d0a, "0b_cobertura_calendario": d0b, "0c_red_repetidas": d0c, "0d_sla_dias_red": d0d}.items():
    t.to_csv(f"{CARPETA_SALIDAS}P3_{n}.csv", index=False)

# COMMAND ----------

import duckdb
cal = f"read_parquet('{RUTA_CALENDARIO}')"
print("Columnas del calendario:", duckdb.sql(f"DESCRIBE SELECT * FROM {cal}").df()[["column_name", "column_type"]].values.tolist())

# a) Tipos de día en 2026 y si algún día marcado como hábil tiene fecha festiva
d0a = duckdb.sql(f"""
    SELECT TIPO_DIA, ID_TIPO_DIA, COUNT(*) AS dias,
           COUNT(*) FILTER (WHERE FECHA_FESTIVA IS NOT NULL) AS con_fecha_festiva,
           list_distinct(list(dayofweek(CAST("DATE" AS DATE)))) AS dias_semana   -- 0 = domingo
    FROM {cal} WHERE CAST("DATE" AS DATE) BETWEEN DATE '2026-01-01' AND DATE '2026-12-31'
    GROUP BY ALL ORDER BY ALL""").df()
display(d0a)

# b) Cobertura: el calendario debe llegar más allá del último vencimiento posible (envío + plazo máximo)
d0b = duckdb.sql(f"""SELECT MIN(CAST("DATE" AS DATE)) AS primera_fecha, MAX(CAST("DATE" AS DATE)) AS ultima_fecha,
                            COUNT(*) AS filas, COUNT(DISTINCT CAST("DATE" AS DATE)) AS fechas_distintas
                     FROM {cal}""").df()
display(d0b)

# c) RED_OPERATIVA: en las claves repetidas, ¿qué columnas cambian?
red = f"read_parquet('{P['RED']}')"
print("Tipo de ID_ESTADO_RUTA:", duckdb.sql(f"SELECT any_value(typeof(ID_ESTADO_RUTA)) FROM {red}").fetchall()[0][0])
d0c = duckdb.sql(f"""
    WITH g AS (
        SELECT ID_CIUDAD_ORIGEN, ID_CIUDAD_DESTINO, ID_TIEMPO, ID_MEDTRANS, COUNT(*) AS n,
               COUNT(DISTINCT TIEMPO_ENTREGA_COMERCIAL_DIA) AS n_dia, COUNT(DISTINCT ID_ESTADO_RUTA) AS n_estado,
               COUNT(DISTINCT ID_IMPEDANCIA) AS n_imp, COUNT(DISTINCT ID_FILTRO_CRITICIDAD) AS n_filtro,
               COUNT(DISTINCT FECHA_MODIFICACION) AS n_fmod,
               COUNT(*) FILTER (WHERE COALESCE(CAST(ID_ESTADO_RUTA AS INT), 0) = 1) AS n_activas
        FROM {red} GROUP BY ALL HAVING COUNT(*) > 1)
    SELECT COUNT(*) AS claves_repetidas,
           AVG((n_dia > 1)::INT)    AS pct_con_sla_distinto,
           AVG((n_estado > 1)::INT) AS pct_estado_distinto,
           AVG((n_imp > 1)::INT)    AS pct_impedancia_distinta,
           AVG((n_filtro > 1)::INT) AS pct_filtro_distinto,
           AVG((n_fmod > 1)::INT)   AS pct_fecha_modificacion_distinta,
           AVG((n_activas = 0)::INT) AS pct_sin_ruta_activa,
           AVG((n_activas > 1)::INT) AS pct_varias_activas
    FROM g""").df()
display(d0c)

# d) Distribución del plazo en días (y si hay plazos no enteros)
d0d = duckdb.sql(f"""SELECT TIEMPO_ENTREGA_COMERCIAL_DIA AS sla_dias, COUNT(*) AS filas_red,
                            AVG((TIEMPO_ENTREGA_COMERCIAL_HOR = TIEMPO_ENTREGA_COMERCIAL_DIA * 24)::INT) AS pct_hor_igual_dia_x24
                     FROM {red} GROUP BY 1 ORDER BY 1""").df()
display(d0d)
for n, t in {"0a_tipos_dia": d0a, "0b_cobertura_calendario": d0b, "0c_red_repetidas": d0c, "0d_sla_dias_red": d0d}.items():
    t.to_csv(f"{CARPETA_SALIDAS}P3_{n}.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parámetros
# MAGIC
# MAGIC Lo que sí está decidido (21-10-2026): días hábiles, sin esperar la confirmación de Servienvios, y alternativa A. Lo demás queda como parámetro, con su sensibilidad al final. La variante principal se fija **antes** de ver la prevalencia y no se cambia por acercarse al 20 % que reporta la empresa.
# MAGIC
# MAGIC `CONDICION_DIA_HABIL` es una expresión SQL sobre las columnas del calendario. El valor propuesto toma como hábil lo que el calendario marca `DIA HABIL` (incluye el sábado) y que no tenga fecha festiva. Si el Paso 0 muestra otros textos en `TIPO_DIA`, se ajusta aquí.

# COMMAND ----------

print(duckdb.sql(f"""SELECT MAX(FECHA_EXTRACCION_DW) AS ultima_extraccion_guias, MAX(FECHA_ENVIO) AS ultimo_envio
                      FROM read_parquet('{RUTA_GUIAS}')""").df())

# COMMAND ----------

CAMPO_ENTREGA       = "FECHA_ENTREGA"        # columna de LOG_GUIAS validada en 0310 como entrega efectiva
CORTE_OBSERVACION   = "2026-08-31 23:59:59"  # último instante observado en la extracción
ID_SALIDA_ZONA      = 9
SLA_EN_DIAS_HABILES = True                   # decisión 21-10; False reproduce la regla v2 (horas continuas) para comparar
CUENTA_DIA_ADMISION = False                  # False: el plazo empieza el día hábil siguiente a la admisión (principal)
REGLA_RED           = "ACTIVA_MAS_RECIENTE"  # principal; sensibilidad: "MAXIMO" (el plazo más holgado de las rutas activas)
COL_FECHA_CALENDARIO = "DATE"
CONDICION_DIA_HABIL = "UPPER(TRIM(TIPO_DIA)) = 'DIA HABIL' AND FECHA_FESTIVA IS NULL"   # ajustar con el Paso 0
ESTADOS_DEVOLUCION  = ["ENTREGADO A REMITENTE"]
ESTADOS_SINIESTRO   = ["SINIESTRADO"]
INCUMPLE_SI_IGUAL   = False                  # solo aplica con SLA_EN_DIAS_HABILES = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funciones (no es necesario modificarlas)

# COMMAND ----------

# =====================================================================================
# BASE ANALÍTICA V3: UNA FILA POR GUÍA, T0 EXPLÍCITO, SLA EN DÍAS HÁBILES CON REGLA DE RED
# EXPLÍCITA, DEVOLUCIONES Y SINIESTROS APARTE Y GUÍA SEUDONIMIZADA.
# Parte de construir_gold_v2 (0330 v2). Cambios marcados con «v3».
# =====================================================================================
import duckdb, secrets

def leer_o_crear_sal(ruta):
    """La sal vive en el Volume (no en el cuaderno ni en GitHub). Si no existe, se crea una vez."""
    if not os.path.exists(ruta):
        os.makedirs(os.path.dirname(ruta), exist_ok=True)
        with open(ruta, "w") as f:
            f.write(secrets.token_hex(16))
        print(f"Sal nueva creada en {ruta}. No copiarla al cuaderno ni subirla al repositorio.")
    with open(ruta) as f:
        sal = f.read().strip()
    assert len(sal) >= 16 and "'" not in sal, "La sal debe tener al menos 16 caracteres y no llevar comillas"
    return sal

def _lista_sql(valores):
    return ", ".join("'" + v.upper().replace("'", "''") + "'" for v in valores)

def sql_calendario(ruta_calendario, col_fecha, condicion_habil):
    """CTE del calendario hábil: cada día hábil recibe un número consecutivo (n_hab)."""
    return f"""
    CAL AS (
        SELECT CAST("{col_fecha}" AS DATE) AS fecha, bool_and(COALESCE({condicion_habil}, FALSE)) AS es_habil
        FROM read_parquet('{ruta_calendario}') GROUP BY 1
    ),
    HAB AS (SELECT fecha, es_habil, SUM(es_habil::INT) OVER (ORDER BY fecha) AS n_hab FROM CAL),
    HABIL AS (SELECT fecha, n_hab FROM HAB WHERE es_habil)"""

def sql_limite_habil(alias_h0, alias_hv, col_envio, col_dias, cuenta_dia_admision):
    """JOIN que ubica el día hábil del vencimiento. El límite es el inicio del día siguiente (fin del día hábil)."""
    ajuste = f"CASE WHEN {alias_h0}.es_habil THEN 1 ELSE 0 END" if cuenta_dia_admision else "0"
    return f"""LEFT JOIN HAB {alias_h0} ON {alias_h0}.fecha = CAST({col_envio} AS DATE)
        LEFT JOIN HABIL {alias_hv} ON {alias_hv}.n_hab = {alias_h0}.n_hab + {col_dias} - {ajuste}"""

def construir_gold_v3(R, P, RUTA_SALIDA, RUTA_CALENDARIO, SAL_GUIA, CAMPO_ENTREGA="FECHA_ENTREGA", ID_SALIDA_ZONA=9,
                      CORTE_OBSERVACION="2026-08-31 23:59:59", SLA_EN_DIAS_HABILES=True, CUENTA_DIA_ADMISION=False,
                      REGLA_RED="ACTIVA_MAS_RECIENTE", COL_FECHA_CALENDARIO="DATE",
                      CONDICION_DIA_HABIL="UPPER(TRIM(TIPO_DIA)) = 'DIA HABIL' AND FECHA_FESTIVA IS NULL",
                      ESTADOS_DEVOLUCION=("ENTREGADO A REMITENTE",), ESTADOS_SINIESTRO=("SINIESTRADO",),
                      INCUMPLE_SI_IGUAL=False):
    assert REGLA_RED in ("ACTIVA_MAS_RECIENTE", "MAXIMO"), "REGLA_RED debe ser ACTIVA_MAS_RECIENTE o MAXIMO"
    # Con días hábiles el límite es el inicio del día siguiente: entregar en ese instante o después incumple
    op = ">=" if (SLA_EN_DIAS_HABILES or INCUMPLE_SI_IGUAL) else ">"
    col_sla = "sla_dias_reciente" if REGLA_RED == "ACTIVA_MAS_RECIENTE" else "sla_dias_max"
    limite = ("epoch_ms(CAST(HV.fecha AS TIMESTAMP) + INTERVAL 1 DAY)" if SLA_EN_DIAS_HABILES
              else "epoch_ms(B.FECHA_ENVIO) + CAST(B.sla_dias * 24 * 3600000 AS BIGINT)")
    con = duckdb.connect()
    copy_a_volume(f"""
    COPY (
    WITH
    GUI AS (
        SELECT CAST(NUMERO_GUIA AS BIGINT) AS guia, ID_TIPOGUIA, ID_CLIENTE, ID_PAIS_ORIGEN, ID_CIUDAD_ORIGEN,
               ID_PAIS_DESTINO, ID_CIUDAD_DESTINO, ID_TIEMPO, ID_MEDTRANS, ID_PRODUCTO, ID_SUBPRODUCTO,
               ID_ESTADOGUIA, ID_ESTADOENVIO, FECHA_ENVIO, {CAMPO_ENTREGA} AS fecha_entrega,
               NUMERO_PIEZAS AS piezas_declaradas, PESO_GUIA AS peso_declarado_kg,
               LARGO_ENVIO, ANCHO_ENVIO, ALTO_ENVIO
        FROM read_parquet('{R["GUIAS"]}')
    ),
    -- T0 = PRIMERA salida a zona urbana (desempate determinista); se conservan cuántas hubo
    SAL AS (
        SELECT CAST(NUMERO_GUIA AS BIGINT) AS guia, ID_PROCESO, NUMERO_MOVIMI, CONSECUT_MOVIMI,
               FECHA_HORA_MOVIMIENTO AS t0,
               COUNT(*) OVER (PARTITION BY CAST(NUMERO_GUIA AS BIGINT)) AS n_salidas_zona
        FROM read_parquet('{R["MOVS_GLOB"]}')
        WHERE ID_PROCESO = {ID_SALIDA_ZONA}
          AND CAST(NUMERO_GUIA AS BIGINT) IN (SELECT guia FROM GUI)   -- solo guías de la población
        QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(NUMERO_GUIA AS BIGINT)
                                   ORDER BY FECHA_HORA_MOVIMIENTO, NUMERO_MOVIMI, CONSECUT_MOVIMI) = 1
    ),
    -- Documento del movimiento T0, deduplicado por su clave (el conteo previo va en el control)
    DOC AS (
        SELECT ID_PROCESO, NUMERO_MOVIMI, CONSECUT_MOVIMI, ID_TIPO_ZONA_URBA, ID_CIUDAD_ZONA_URBA, ID_ZONA_URBA
        FROM read_parquet('{R["DOCS_GLOB"]}')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ID_PROCESO, NUMERO_MOVIMI, CONSECUT_MOVIMI
                                   ORDER BY FECHA_EXTRACCION_DW DESC, FECHA_MODIFICACION DESC) = 1
    ),
    -- v3: RED_OPERATIVA reducida a UNA fila por clave con regla explícita.
    --     Se prefieren las rutas activas; entre ellas, la de FECHA_MODIFICACION más reciente.
    REDR AS (
        SELECT ID_CIUDAD_ORIGEN, ID_CIUDAD_DESTINO, ID_TIEMPO, ID_MEDTRANS,
               TIEMPO_ENTREGA_COMERCIAL_DIA AS dia, TIEMPO_ENTREGA_COMERCIAL_HOR AS hor, FECHA_MODIFICACION,
               COALESCE(CAST(ID_ESTADO_RUTA AS INT), 0) AS activa,
               MAX(COALESCE(CAST(ID_ESTADO_RUTA AS INT), 0))
                   OVER (PARTITION BY ID_CIUDAD_ORIGEN, ID_CIUDAD_DESTINO, ID_TIEMPO, ID_MEDTRANS) AS hay_activa
        FROM read_parquet('{P["RED"]}')
    ),
    RED AS (
        SELECT ID_CIUDAD_ORIGEN, ID_CIUDAD_DESTINO, ID_TIEMPO, ID_MEDTRANS,
               COUNT(*) AS n_red,
               COUNT(DISTINCT dia) AS n_valores_sla,
               MAX(hay_activa) = 0 AS red_sin_ruta_activa,
               first(dia ORDER BY activa DESC, FECHA_MODIFICACION DESC NULLS LAST, dia DESC) AS dia_reciente,
               first(hor ORDER BY activa DESC, FECHA_MODIFICACION DESC NULLS LAST, dia DESC) AS hor_reciente,
               MAX(dia) FILTER (WHERE activa = hay_activa) AS dia_max
        FROM REDR GROUP BY ALL
    ),
    ZON AS (
        SELECT ID_CIUDAD, ID_TIPO_ZONA_URBA, ID_ZONA_URBA, COUNT(*) AS n_zona,
               CASE WHEN COUNT(DISTINCT NOMBRE_ZONA_URBA) = 1 THEN MIN(NOMBRE_ZONA_URBA) END AS zona_urbana,
               CASE WHEN COUNT(DISTINCT CATEGORIA_ZONA_URBA) = 1 THEN MIN(CATEGORIA_ZONA_URBA) END AS categoria_zona
        FROM read_parquet('{P["ZONAS"]}') WHERE ID_PAIS = 1 GROUP BY ALL
    ),
    CIU AS (
        SELECT ID_PAIS, ID_CIUDAD, COUNT(*) AS n_ciu,
               CASE WHEN COUNT(DISTINCT NOMBRE_CIUDAD) = 1 THEN MIN(NOMBRE_CIUDAD) END AS ciudad,
               CASE WHEN COUNT(DISTINCT ID_REGIONAL)   = 1 THEN MIN(ID_REGIONAL)   END AS id_regional
        FROM read_parquet('{P["CIUDADES"]}') GROUP BY ALL
    ),
    REG AS (
        SELECT ID_REGIONAL, CASE WHEN COUNT(DISTINCT NOMBRE_REGIONAL) = 1 THEN MIN(NOMBRE_REGIONAL) END AS regional
        FROM read_parquet('{P["REGIONALES"]}') GROUP BY ALL
    ),
    -- v3: estado del envío para separar devoluciones y siniestros
    EEN AS (
        SELECT ID_ESTADOENVIO,
               CASE WHEN COUNT(DISTINCT NOMBRE_ESTADOENVIO) = 1 THEN UPPER(TRIM(MIN(NOMBRE_ESTADOENVIO))) END AS estado_envio
        FROM read_parquet('{P["EST_ENVIO"]}') GROUP BY ALL
    ),
    {sql_calendario(RUTA_CALENDARIO, COL_FECHA_CALENDARIO, CONDICION_DIA_HABIL)},
    BASE AS (
        SELECT G.*, S.t0, S.n_salidas_zona,
               D.ID_TIPO_ZONA_URBA, D.ID_ZONA_URBA,
               Z.zona_urbana, Z.categoria_zona, Z.n_zona,
               CO.ciudad AS ciudad_origen, RO.regional AS regional_origen,
               E.estado_envio,
               R2.n_red, R2.n_valores_sla, R2.red_sin_ruta_activa,
               CAST(CEIL(R2.dia_reciente) AS INT) AS sla_dias_reciente,
               CAST(CEIL(R2.dia_max) AS INT)      AS sla_dias_max,
               R2.hor_reciente                    AS sla_h_continuas,      -- valor en horas de la fila elegida (comparación v2)
               (COALESCE(G.ID_TIEMPO,0) = 0 OR COALESCE(G.ID_MEDTRANS,0) = 0) AS sla_con_sustitucion_1
        FROM GUI G
        JOIN SAL S USING (guia)                                   -- población: guías con salida a zona
        LEFT JOIN DOC D ON D.ID_PROCESO = S.ID_PROCESO AND D.NUMERO_MOVIMI = S.NUMERO_MOVIMI
                       AND D.CONSECUT_MOVIMI = S.CONSECUT_MOVIMI
        LEFT JOIN ZON Z ON Z.ID_CIUDAD = D.ID_CIUDAD_ZONA_URBA AND Z.ID_TIPO_ZONA_URBA = D.ID_TIPO_ZONA_URBA
                       AND Z.ID_ZONA_URBA = D.ID_ZONA_URBA
        LEFT JOIN CIU CO ON CO.ID_PAIS = G.ID_PAIS_ORIGEN AND CO.ID_CIUDAD = G.ID_CIUDAD_ORIGEN
        LEFT JOIN REG RO ON RO.ID_REGIONAL = CO.id_regional
        LEFT JOIN EEN E  ON E.ID_ESTADOENVIO = G.ID_ESTADOENVIO
        LEFT JOIN RED R2 ON R2.ID_CIUDAD_ORIGEN = G.ID_CIUDAD_ORIGEN AND R2.ID_CIUDAD_DESTINO = G.ID_CIUDAD_DESTINO
                        AND R2.ID_TIEMPO   = CASE WHEN COALESCE(G.ID_TIEMPO,0)   <> 0 THEN G.ID_TIEMPO   ELSE 1 END
                        AND R2.ID_MEDTRANS = CASE WHEN COALESCE(G.ID_MEDTRANS,0) <> 0 THEN G.ID_MEDTRANS ELSE 1 END
    ),
    BASE2 AS (SELECT *, {col_sla} AS sla_dias FROM BASE),
    -- v3: vencimiento al final del n-ésimo día hábil (o en horas continuas si SLA_EN_DIAS_HABILES = False)
    VEN AS (
        SELECT B.*, {limite} AS limite_ms
        FROM BASE2 B
        {sql_limite_habil("H0", "HV", "B.FECHA_ENVIO", "B.sla_dias", CUENTA_DIA_ADMISION)}
    ),
    ETI AS (
        SELECT *,
            estado_envio IN ({_lista_sql(ESTADOS_DEVOLUCION)}) AS es_devolucion,
            estado_envio IN ({_lista_sql(ESTADOS_SINIESTRO)})  AS es_siniestro,
            (epoch_ms(t0)            - epoch_ms(FECHA_ENVIO)) / 3600000.0 AS h_admision_a_t0,
            (epoch_ms(fecha_entrega) - epoch_ms(t0))          / 3600000.0 AS h_ultima_milla_bruta,
            (epoch_ms(fecha_entrega) - epoch_ms(FECHA_ENVIO)) / 3600000.0 AS h_total,
            epoch_ms(TIMESTAMP '{CORTE_OBSERVACION}')                       AS corte_ms
        FROM VEN
    ),
    FIN AS (
        SELECT *,
            COALESCE(es_devolucion OR es_siniestro, FALSE) AS fuera_de_desenlace,
            sla_dias IS NOT NULL AND limite_ms IS NULL     AS vencimiento_sin_calendario
        FROM ETI
    )
    SELECT md5(CAST(guia AS VARCHAR) || '{SAL_GUIA}') AS guia,                 -- v3: seudónimo, nunca la guía real
        * EXCLUDE (guia, corte_ms, limite_ms, es_devolucion, es_siniestro, sla_con_sustitucion_1, fuera_de_desenlace),
        CAST(sla_dias * 24 AS DOUBLE)                                       AS sla_h,   -- plazo nominal en horas (variable)
        make_timestamp((limite_ms - 1000) * 1000)                           AS vencimiento_sla,   -- 23:59:59 del último día
        (limite_ms - epoch_ms(t0)) / 3600000.0                              AS margen_sla_en_t0_h,  -- predictor válido en T0
        -- v3.3: fecha promesa de entrega en días CALENDARIO (regla de 0200 celda 23: admisión + TIEMPO_ENTREGA_COMERCIAL_DIA)
        FECHA_ENVIO + INTERVAL (sla_dias) DAY                               AS fecha_promesa_entrega,
        date_diff('day', CAST(FECHA_ENVIO AS DATE), CAST(fecha_entrega AS DATE)) AS dias_entrega_real,
        date_diff('day', CAST(FECHA_ENVIO + INTERVAL (sla_dias) DAY AS DATE), CAST(fecha_entrega AS DATE)) AS dias_vs_promesa,  -- > 0: retraso en días calendario
        -- v3.3: cumplimiento contra la promesa calendario (1 = entregada dentro del día promesa); mismo dominio que incumplimiento_sla
        CASE WHEN fuera_de_desenlace OR sla_dias IS NULL THEN NULL
             WHEN fecha_entrega IS NOT NULL THEN (CAST(fecha_entrega AS DATE) <= CAST(FECHA_ENVIO + INTERVAL (sla_dias) DAY AS DATE))::INT
             WHEN CAST(FECHA_ENVIO + INTERVAL (sla_dias) DAY AS DATE) < CAST(TIMESTAMP '{CORTE_OBSERVACION}' AS DATE) THEN 0
             ELSE NULL END                                                  AS cumplimiento_promesa,
        CASE WHEN fuera_de_desenlace THEN NULL ELSE h_ultima_milla_bruta END AS h_ultima_milla,
        CASE WHEN sla_dias IS NULL THEN 'SIN_SLA'
             WHEN sla_con_sustitucion_1 THEN 'SLA_CON_SUSTITUCION'
             WHEN n_valores_sla > 1 THEN 'SLA_POR_REGLA'
             ELSE 'SLA_UNICO' END                                           AS origen_sla,
        CASE WHEN es_devolucion THEN 'DEVUELTO'
             WHEN es_siniestro  THEN 'SINIESTRADO'
             WHEN fecha_entrega IS NOT NULL                     THEN 'ENTREGADO'
             WHEN limite_ms IS NOT NULL AND limite_ms <= corte_ms THEN 'ABIERTO_VENCIDO'
             ELSE 'ABIERTO_NO_VENCIDO' END                                  AS estado_desenlace,
        -- Etiqueta: 1/0 cuando es identificable; NULL si no (abierto no vencido, sin SLA, sin calendario,
        -- devolución o siniestro)
        CASE WHEN fuera_de_desenlace OR limite_ms IS NULL       THEN NULL
             WHEN fecha_entrega IS NOT NULL THEN (epoch_ms(fecha_entrega) {op} limite_ms)::INT
             WHEN limite_ms <= corte_ms                         THEN 1
             ELSE NULL END                                                  AS incumplimiento_sla,
        -- Momento en que cada etiqueta quedó conocida (para cortes y agregados sin futuro)
        CASE WHEN NOT fuera_de_desenlace
             THEN make_timestamp(LEAST(epoch_ms(fecha_entrega), limite_ms) * 1000) END AS t_etiqueta_clf,
        CASE WHEN NOT fuera_de_desenlace THEN fecha_entrega END             AS t_etiqueta_reg,
        (epoch_ms(t0) >= limite_ms)                                         AS vencido_antes_de_t0
    FROM FIN
    ) TO '{RUTA_SALIDA}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """, con)
    con.close()

def controles_gold_v3(RUTA, CORTE_OBSERVACION):
    """Pruebas de propiedades. Falla con mensaje claro; imprime solo agregados."""
    q = lambda s: duckdb.sql(s.replace("{T}", f"read_parquet('{RUTA}')")).df()
    principal = "origen_sla IN ('SLA_UNICO', 'SLA_POR_REGLA')"
    c = q(f"""SELECT COUNT(*) filas, COUNT(DISTINCT guia) guias,
                    COUNT(*) FILTER (WHERE incumplimiento_sla NOT IN (0,1)) etiqueta_fuera_dominio,
                    COUNT(*) FILTER (WHERE incumplimiento_sla IS NULL) etiqueta_nula,
                    COUNT(*) FILTER (WHERE h_ultima_milla < 0) ultima_milla_negativa,
                    COUNT(*) FILTER (WHERE h_admision_a_t0 < 0) t0_antes_de_admision,
                    COUNT(*) FILTER (WHERE vencimiento_sin_calendario) sin_vencimiento_por_calendario,
                    COUNT(*) FILTER (WHERE vencimiento_sla < FECHA_ENVIO) vencimiento_antes_de_admision,
                    COUNT(*) FILTER (WHERE vencido_antes_de_t0) vencidas_antes_de_t0,
                    COUNT(*) FILTER (WHERE estado_desenlace = 'DEVUELTO') devueltas,
                    COUNT(*) FILTER (WHERE estado_desenlace = 'SINIESTRADO') siniestradas,
                    COUNT(*) FILTER (WHERE {principal}) con_sla_principal,
                    AVG(incumplimiento_sla) FILTER (WHERE {principal}) prevalencia_principal
             FROM {{T}}""")
    assert c.loc[0, "filas"] == c.loc[0, "guias"], "Hay más filas que guías: fan-out sin resolver"
    assert c.loc[0, "etiqueta_fuera_dominio"] == 0, "incumplimiento_sla fuera de {0,1}"
    tablas = {
        "control": c,
        "etiqueta_por_estado": q("""SELECT estado_desenlace, origen_sla, COUNT(*) guias,
                                      AVG(incumplimiento_sla) prevalencia, COUNT(incumplimiento_sla) n_etiquetadas
                               FROM {T} GROUP BY ALL ORDER BY guias DESC"""),
        "por_mes": q(f"""SELECT date_trunc('month', FECHA_ENVIO) mes, COUNT(*) guias,
                          AVG((estado_desenlace='ENTREGADO')::INT) pct_entregado,
                          AVG((vencido_antes_de_t0)::INT) pct_vencido_antes_de_t0,
                          AVG(incumplimiento_sla) FILTER (WHERE {principal}) prevalencia_principal,
                          AVG(incumplimiento_sla) FILTER (WHERE origen_sla = 'SLA_UNICO') prevalencia_solo_sla_unico
                        FROM {{T}} GROUP BY 1 ORDER BY 1"""),
        "sla_dias": q(f"""SELECT sla_dias, COUNT(*) guias, AVG(incumplimiento_sla) prevalencia,
                           median(h_admision_a_t0) mediana_h_admision_a_t0, median(margen_sla_en_t0_h) mediana_margen_h
                         FROM {{T}} WHERE {principal} GROUP BY 1 ORDER BY 1"""),
    }
    return tablas

def sensibilidad_sla(RUTA, RUTA_CALENDARIO, COL_FECHA_CALENDARIO, CONDICION_DIA_HABIL, CORTE_OBSERVACION):
    """Prevalencia y % vencido antes de T0 en las variantes de la regla del SLA, sobre la misma población
    (SLA único o por regla, sin devoluciones ni siniestros). La variante 1 es la principal."""
    variantes = [(1, False, "sla_dias_reciente"), (2, True, "sla_dias_reciente"),
                 (3, False, "sla_dias_max"),      (4, True, "sla_dias_max")]
    partes = []
    for k, cuenta, col in variantes:
        partes.append(f"""
        SELECT {k} AS variante, 'días hábiles' AS unidad, {cuenta} AS cuenta_dia_admision,
               '{'ACTIVA_MAS_RECIENTE' if col == 'sla_dias_reciente' else 'MAXIMO'}' AS regla_red,
               epoch_ms(CAST(HV.fecha AS TIMESTAMP) + INTERVAL 1 DAY) AS lim, B.*
        FROM POB B {sql_limite_habil("H0", "HV", "B.FECHA_ENVIO", "B." + col, cuenta)}""")
    partes.append("""
        SELECT 0, 'horas continuas (regla v2)', FALSE, 'ACTIVA_MAS_RECIENTE',
               epoch_ms(B.FECHA_ENVIO) + CAST(B.sla_dias_reciente * 24 * 3600000 AS BIGINT), B.*
        FROM POB B""")
    sql = f"""
    WITH {sql_calendario(RUTA_CALENDARIO, COL_FECHA_CALENDARIO, CONDICION_DIA_HABIL).strip()},
    POB AS (SELECT FECHA_ENVIO, fecha_entrega, t0, sla_dias_reciente, sla_dias_max
            FROM read_parquet('{RUTA}')
            WHERE origen_sla IN ('SLA_UNICO', 'SLA_POR_REGLA') AND estado_desenlace NOT IN ('DEVUELTO', 'SINIESTRADO')),
    V AS ({" UNION ALL ".join(partes)})
    SELECT variante, unidad, cuenta_dia_admision, regla_red, COUNT(*) AS guias,
           AVG(CASE WHEN lim IS NULL THEN NULL
                    WHEN fecha_entrega IS NOT NULL THEN (epoch_ms(fecha_entrega) >= lim)::INT
                    WHEN lim <= epoch_ms(TIMESTAMP '{CORTE_OBSERVACION}') THEN 1 END) AS prevalencia,
           AVG((epoch_ms(t0) >= lim)::INT) AS pct_vencido_antes_de_t0,
           COUNT(*) FILTER (WHERE lim IS NULL) AS sin_vencimiento
    FROM V GROUP BY ALL ORDER BY variante"""
    return duckdb.sql(sql).df()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construcción y controles

# COMMAND ----------

SAL_GUIA = leer_o_crear_sal(RUTA_SAL_GUIA)
R = {"GUIAS": RUTA_GUIAS, "MOVS_GLOB": RUTA_MOVS_GLOB, "DOCS_GLOB": RUTA_DOCS_GLOB}
construir_gold_v3(R, P, RUTA_GOLD_V3, RUTA_CALENDARIO, SAL_GUIA, CAMPO_ENTREGA=CAMPO_ENTREGA,
                  ID_SALIDA_ZONA=ID_SALIDA_ZONA, CORTE_OBSERVACION=CORTE_OBSERVACION,
                  SLA_EN_DIAS_HABILES=SLA_EN_DIAS_HABILES, CUENTA_DIA_ADMISION=CUENTA_DIA_ADMISION,
                  REGLA_RED=REGLA_RED, COL_FECHA_CALENDARIO=COL_FECHA_CALENDARIO,
                  CONDICION_DIA_HABIL=CONDICION_DIA_HABIL, ESTADOS_DEVOLUCION=ESTADOS_DEVOLUCION,
                  ESTADOS_SINIESTRO=ESTADOS_SINIESTRO, INCUMPLE_SI_IGUAL=INCUMPLE_SI_IGUAL)
del SAL_GUIA
tablas = controles_gold_v3(RUTA_GOLD_V3, CORTE_OBSERVACION)   # se detiene con error si falla la unidad o el dominio
for nombre, t in tablas.items():
    print(nombre); display(t); t.to_csv(f"{CARPETA_SALIDAS}P3_{nombre}.csv", index=False)

# COMMAND ----------

# DBTITLE 1,Control columnas nuevas v3.3
# v3.3 · Control de las columnas nuevas (promesa en días calendario): dominio y comparación con el SLA en hábiles
T = f"read_parquet('{RUTA_GOLD_V3}')"
chk = duckdb.sql(f"""
    SELECT COUNT(*) AS filas,
           COUNT(*) FILTER (WHERE cumplimiento_promesa NOT IN (0,1)) AS promesa_fuera_de_dominio,
           COUNT(cumplimiento_promesa)                                   AS con_cumplimiento_promesa,
           COUNT(*) FILTER (WHERE sla_dias IS NOT NULL AND fecha_promesa_entrega IS NULL) AS promesa_nula_con_sla,
           COUNT(*) FILTER (WHERE dias_entrega_real < 0)                AS entrega_antes_de_admision,
           AVG(cumplimiento_promesa) FILTER (WHERE origen_sla IN ('SLA_UNICO','SLA_POR_REGLA')
                                             AND estado_desenlace = 'ENTREGADO') AS pct_cumple_promesa,
           AVG(incumplimiento_sla)   FILTER (WHERE origen_sla IN ('SLA_UNICO','SLA_POR_REGLA')
                                             AND estado_desenlace = 'ENTREGADO') AS prevalencia_sla_entregadas
    FROM {T}""").df()
display(chk); chk.to_csv(f"{CARPETA_SALIDAS}P3_control_promesa.csv", index=False)
assert int(chk.loc[0, "promesa_fuera_de_dominio"]) == 0, "cumplimiento_promesa fuera de {0,1}"
assert int(chk.loc[0, "promesa_nula_con_sla"]) == 0, "hay guías con SLA y sin fecha promesa"
if int(chk.loc[0, "entrega_antes_de_admision"]) > 0:
    print(f"AVISO: {int(chk.loc[0, 'entrega_antes_de_admision']):,} entregas anteriores a la admisión (auditar, no se excluyen)")
print("Muestra de estructura (fechas truncadas a día; sin guías):")
display(duckdb.sql(f"""SELECT CAST(FECHA_ENVIO AS DATE)            AS admision,
                              sla_dias,
                              CAST(fecha_promesa_entrega AS DATE)   AS promesa_calendario,
                              CAST(vencimiento_sla AS DATE)         AS vencimiento_habiles,
                              CAST(fecha_entrega AS DATE)           AS entrega,
                              dias_entrega_real, dias_vs_promesa, cumplimiento_promesa, incumplimiento_sla
                       FROM {T} WHERE estado_desenlace = 'ENTREGADO' AND origen_sla = 'SLA_UNICO' LIMIT 5""").df())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sensibilidad de la regla del SLA
# MAGIC
# MAGIC Misma población en todas las filas (SLA único o por regla, sin devoluciones ni siniestros). La variante 1 es la principal y se fijó antes de ver estos números; las demás se reportan tal cual. La fila 0 reproduce la regla de la v2 (horas continuas desde la admisión) con la misma fila de RED, para ver cuánto explica el cambio de unidad.

# COMMAND ----------

sens = sensibilidad_sla(RUTA_GOLD_V3, RUTA_CALENDARIO, COL_FECHA_CALENDARIO, CONDICION_DIA_HABIL, CORTE_OBSERVACION)
display(sens); sens.to_csv(f"{CARPETA_SALIDAS}P3_sensibilidad_sla.csv", index=False)

# COMMAND ----------

# DBTITLE 1,Sensibilidad v3.3 · promesa calendario
# v3.3 · Sensibilidad ampliada: la PROMESA EN DÍAS CALENDARIO (regla de 0200 celda 23) frente a las reglas del SLA.
# Misma población que la tabla anterior (SLA único o por regla, sin devoluciones ni siniestros).
import pandas as pd
prom = duckdb.sql(f"""
    SELECT 5 AS variante, 'promesa calendario (v3.3)' AS unidad, FALSE AS cuenta_dia_admision,
           'ADMITIDA + TIEMPO_ENTREGA_COMERCIAL_DIA (días calendario)' AS regla_red,
           COUNT(*) AS guias,
           AVG(1 - cumplimiento_promesa)             AS prevalencia,
           AVG((epoch_ms(t0) >= epoch_ms(CAST(fecha_promesa_entrega AS DATE) + INTERVAL 1 DAY))::INT) AS pct_vencido_antes_de_t0,
           0 AS sin_vencimiento
    FROM read_parquet('{RUTA_GOLD_V3}')
    WHERE origen_sla IN ('SLA_UNICO', 'SLA_POR_REGLA')
      AND estado_desenlace NOT IN ('DEVUELTO', 'SINIESTRADO')""").df()
abiertas = duckdb.sql(f"""SELECT COUNT(*) AS abiertas_no_vencidas
    FROM read_parquet('{RUTA_GOLD_V3}')
    WHERE origen_sla IN ('SLA_UNICO', 'SLA_POR_REGLA')
      AND estado_desenlace NOT IN ('DEVUELTO', 'SINIESTRADO')
      AND cumplimiento_promesa IS NULL""").df().loc[0, "abiertas_no_vencidas"]
print(f"Nota: {int(abiertas):,} guías abiertas aún no vencidas quedan con etiqueta nula en TODAS las variantes (igual que en la tabla anterior).")
sens_v3_3 = pd.concat([sens, prom], ignore_index=True)
display(sens_v3_3)
sens_v3_3.to_csv(f"{CARPETA_SALIDAS}P3_sensibilidad_sla_v3_3.csv", index=False)

# COMMAND ----------

# DBTITLE 1,Sensibilidad promesa calendario v3.3
# v3.3 · Sensibilidad de la regla de la FECHA PROMESA en días calendario (columnas nuevas).
# Misma población que sensibilidad_sla (SLA principal, sin devoluciones ni siniestros):
# las prevalencias son comparables fila a fila con la tabla anterior.
CORTE_MS = f"epoch_ms(TIMESTAMP '{CORTE_OBSERVACION}')"
T = f"read_parquet('{RUTA_GOLD_V3}')"
sens_promesa = duckdb.sql(f"""
    WITH POB AS (
        SELECT FECHA_ENVIO, fecha_entrega, t0, sla_dias_reciente, sla_dias_max
        FROM {T}
        WHERE origen_sla IN ('SLA_UNICO', 'SLA_POR_REGLA')
          AND estado_desenlace NOT IN ('DEVUELTO', 'SINIESTRADO')
    ),
    V AS (
        SELECT 1 AS variante, 'promesa calendario (día completo)' AS regla, 'ACTIVA_MAS_RECIENTE' AS regla_red,
               epoch_ms(CAST(CAST(FECHA_ENVIO AS DATE) + (sla_dias_reciente + 1) AS TIMESTAMP)) AS lim, POB.* FROM POB
        UNION ALL
        SELECT 2, 'promesa calendario (día completo)', 'MAXIMO',
               epoch_ms(CAST(CAST(FECHA_ENVIO AS DATE) + (sla_dias_max + 1) AS TIMESTAMP)) AS lim, POB.* FROM POB
        UNION ALL
        SELECT 3, 'promesa calendario · cuenta día de admisión', 'ACTIVA_MAS_RECIENTE',
               epoch_ms(CAST(CAST(FECHA_ENVIO AS DATE) + sla_dias_reciente AS TIMESTAMP)), POB.* FROM POB
        UNION ALL
        SELECT 4, 'promesa calendario · cuenta día de admisión', 'MAXIMO',
               epoch_ms(CAST(CAST(FECHA_ENVIO AS DATE) + sla_dias_max AS TIMESTAMP)), POB.* FROM POB
        UNION ALL
        SELECT 5, 'promesa calendario estricta (misma hora de admisión)', 'ACTIVA_MAS_RECIENTE',
               epoch_ms(FECHA_ENVIO + INTERVAL (sla_dias_reciente) DAY), POB.* FROM POB
    )
    SELECT variante, regla, regla_red, COUNT(*) AS guias,
           AVG(CASE WHEN fecha_entrega IS NOT NULL THEN (epoch_ms(fecha_entrega) >= lim)::INT
                    WHEN lim <= {CORTE_MS} THEN 1 END) AS prevalencia,
           AVG(CASE WHEN fecha_entrega IS NOT NULL THEN (epoch_ms(fecha_entrega) >= lim)::INT END) AS prevalencia_entregadas,
           AVG((epoch_ms(t0) >= lim)::INT) AS pct_vencido_antes_de_t0
    FROM V GROUP BY ALL ORDER BY variante""").df()
display(sens_promesa); sens_promesa.to_csv(f"{CARPETA_SALIDAS}P3_sensibilidad_promesa.csv", index=False)
print("Referencia: la variante 1 equivale a 1 - cumplimiento_promesa en la población entregada de la celda de control v3.3.")

# COMMAND ----------

# Balance de registros: de LOG_GUIAS_BOGOTA a la base v3 (solo conteos)
T = f"read_parquet('{RUTA_GOLD_V3}')"
balance = duckdb.sql(f"""
    SELECT 'Guías en LOG_GUIAS_BOGOTA' AS etapa, COUNT(*) AS guias FROM read_parquet('{RUTA_GUIAS}')
    UNION ALL SELECT 'Base v3: con salida a zona urbana', COUNT(*) FROM {T}
    UNION ALL SELECT '  con SLA único', COUNT(*) FROM {T} WHERE origen_sla = 'SLA_UNICO'
    UNION ALL SELECT '  con SLA por regla (clave repetida con plazos distintos)', COUNT(*) FROM {T} WHERE origen_sla = 'SLA_POR_REGLA'
    UNION ALL SELECT '  con SLA por sustitución', COUNT(*) FROM {T} WHERE origen_sla = 'SLA_CON_SUSTITUCION'
    UNION ALL SELECT '  sin SLA', COUNT(*) FROM {T} WHERE origen_sla = 'SIN_SLA'
    UNION ALL SELECT '  devoluciones (aparte)', COUNT(*) FROM {T} WHERE estado_desenlace = 'DEVUELTO'
    UNION ALL SELECT '  siniestros (aparte)', COUNT(*) FROM {T} WHERE estado_desenlace = 'SINIESTRADO'
    UNION ALL SELECT '  con etiqueta identificable', COUNT(incumplimiento_sla) FROM {T}
    UNION ALL SELECT '  ya vencidas antes de T0', COUNT(*) FILTER (WHERE vencido_antes_de_t0) FROM {T}
    UNION ALL SELECT '  última milla negativa (auditar)', COUNT(*) FILTER (WHERE h_ultima_milla < 0) FROM {T}
""").df()
display(balance); balance.to_csv(f"{CARPETA_SALIDAS}P3_balance.csv", index=False)

# Documentos repetidos por clave en la fuente (la base se queda con uno por clave)
display(duckdb.sql(f"""SELECT COUNT(*) AS claves_documento_repetidas FROM (
    SELECT ID_PROCESO, NUMERO_MOVIMI, CONSECUT_MOVIMI FROM read_parquet('{RUTA_DOCS_GLOB}')
    GROUP BY ALL HAVING COUNT(*) > 1)""").df())

# COMMAND ----------

# Manifiesto de la corrida: entradas, parámetros, versiones y conteos (la sal NO se guarda)
import json, glob, sys, datetime
def archivos(patron):
    return [{"archivo": f, "bytes": os.path.getsize(f), "modificado": datetime.datetime.fromtimestamp(os.path.getmtime(f)).isoformat()}
            for f in sorted(glob.glob(patron))]
manifiesto = {
    "fecha_corrida": datetime.datetime.now().isoformat(), "python": sys.version.split()[0], "duckdb": duckdb.__version__,
    "entradas": {"guias": archivos(RUTA_GUIAS), "movimientos": archivos(RUTA_MOVS_GLOB), "documentos": archivos(RUTA_DOCS_GLOB),
                 "calendario": archivos(RUTA_CALENDARIO), "parametricas": {k: archivos(v) for k, v in P.items()}},
    "parametros": {"CAMPO_ENTREGA": CAMPO_ENTREGA, "CORTE_OBSERVACION": CORTE_OBSERVACION, "ID_SALIDA_ZONA": ID_SALIDA_ZONA,
                   "SLA_EN_DIAS_HABILES": SLA_EN_DIAS_HABILES, "CUENTA_DIA_ADMISION": CUENTA_DIA_ADMISION,
                   "REGLA_RED": REGLA_RED, "CONDICION_DIA_HABIL": CONDICION_DIA_HABIL,
                   "ESTADOS_DEVOLUCION": ESTADOS_DEVOLUCION, "ESTADOS_SINIESTRO": ESTADOS_SINIESTRO,
                   "INCUMPLE_SI_IGUAL": INCUMPLE_SI_IGUAL, "seudonimo_guia": "md5(guia || sal), sal en el Volume"},
    "salida": RUTA_GOLD_V3, "control": tablas["control"].to_dict("records"), "balance": balance.to_dict("records"),
    "sensibilidad": sens.to_dict("records")}
with open(f"{CARPETA_SALIDAS}P3_manifiesto.json", "w") as f:
    json.dump(manifiesto, f, indent=2, default=str)
print("Manifiesto guardado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo leer los resultados
# MAGIC
# MAGIC - **Paso 0**: confirma qué cuenta como día hábil y que el calendario cubre los vencimientos. Si `sin_vencimiento_por_calendario` sale mayor que cero, el calendario no llega hasta la fecha de vencimiento de esas guías.
# MAGIC - `control`: `filas` igual a `guias` y `etiqueta_fuera_dominio` en 0 (si no, la celda ya se detuvo). `prevalencia_principal` es la tasa de incumplimiento en la población del análisis principal.
# MAGIC - `etiqueta_por_estado`: prevalencia por estado del desenlace y origen del SLA. Devoluciones y siniestros aparecen con etiqueta nula.
# MAGIC - `por_mes`: prevalencia y porcentaje vencido antes de T0 por mes; si los últimos meses tienen menos entregadas, hay envíos sin madurar.
# MAGIC - `sla_dias`: prevalencia por plazo comprometido; un plazo con prevalencia cercana a 1 indica que ese valor de RED probablemente no es el que usa la operación.
# MAGIC - `sensibilidad_sla`: cuánto cambia la prevalencia con cada decisión. Se reporta completa en el documento.
# MAGIC - `balance`: cuántas guías quedan en cada paso.
# MAGIC
# MAGIC ## Qué entregar
# MAGIC
# MAGIC Los CSV `P3_*` de la carpeta `V3/` y `P3_manifiesto.json`. Con los controles aprobados se pasa a `0160_ClimaAlineado_2`, `0210_AnalisisExploratorioV3` y `0410_EvaluacionTemporal_3`.