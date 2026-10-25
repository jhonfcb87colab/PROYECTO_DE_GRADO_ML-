# Databricks notebook source
# MAGIC %md
# MAGIC # 0340 · Verificación empírica de los supuestos
# MAGIC
# MAGIC **Para qué.** Servienvios no entregará más información ni confirmaciones (24-10-2026). Este cuaderno responde con los datos que ya tenemos las preguntas que antes eran para la empresa. Cada sección termina en un CSV `P6_*` y en una regla de lectura fijada **antes** de ver el resultado: qué patrón respalda el supuesto y cuál lo contradice.
# MAGIC
# MAGIC | Sección | Pregunta | Supuesto |
# MAGIC | --- | --- | --- |
# MAGIC | V1 | ¿`FECHA_ENTREGA` marca una entrega real o un cierre administrativo? | S1 |
# MAGIC | V2 | ¿El proceso 9 es la salida a reparto? | S2 |
# MAGIC | V3 | ¿Las horas están en hora local de Bogotá? | S2 |
# MAGIC | V4 | ¿Qué son las entregas anteriores a la salida y las que no tienen hora? | S1 |
# MAGIC | V5 | ¿Las devoluciones se comportan distinto de las entregas? | S6 |
# MAGIC | V6 | ¿La operación trabaja los sábados? | S4 |
# MAGIC | V7 | ¿Qué significan los plazos fraccionarios de RED_OPERATIVA? | S5 |
# MAGIC | V8 | ¿Con qué definición del SLA se obtiene el ~20 % que reporta la empresa? | S3, S10 |
# MAGIC | V9 | ¿Los nombres de zona sirven para ubicarlas en localidades? | S8 |
# MAGIC
# MAGIC **Lee** la base v3 (`GOLD_V3_ENVIO.parquet`), `LOG_GUIAS_BOGOTA`, los movimientos, `RED_OPERATIVA_OFERTA`, `ZONASURBANAS` y el calendario. **No escribe** nada en las rutas de 0330. Todas las salidas son conteos o porcentajes agregados; ninguna muestra guías, clientes ni personas.
# MAGIC
# MAGIC **Regla para V8.** La variante principal de 0330 (días hábiles, sin contar el día de admisión, ruta activa más reciente) **no se cambia** por acercarse al 20 %. V8 sirve para explicar la diferencia, no para elegir la regla.

# COMMAND ----------

# MAGIC %pip install "duckdb<2"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# CONFIGURACIÓN · mismas rutas de 0330_BaseAnaliticaV3_2
import os, duckdb, pandas as pd
PATH_DATABRICKS = "/Volumes/proyectogrado/source/source/"
RUTA_GUIAS      = f"{PATH_DATABRICKS}LOG_GUIAS/RAW/LOG_GUIAS_BOGOTA.parquet"
RUTA_MOVS_GLOB  = f"{PATH_DATABRICKS}LOG_SEGUIMIENTO_ENVIO/LOG_SEGUIMIENTO_ENVIO*.parquet"
PAR             = f"{PATH_DATABRICKS}PARAMETRICAS/"
RUTA_RED        = f"{PAR}PAR.RED_OPERATIVA_OFERTA.parquet"
RUTA_ZONAS      = f"{PAR}PAR.ZONASURBANAS.parquet"
RUTA_CALENDARIO = f"{PAR}DIM.VW_TIEMPO_CALENDARIO_Y_FESTIVOS.parquet"
RUTA_GOLD_V3    = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3/GOLD_V3_ENVIO.parquet"
CARPETA_SALIDAS = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3/VERIFICACION/"
os.makedirs(CARPETA_SALIDAS, exist_ok=True)

CORTE_OBSERVACION = "2026-08-31 23:59:59"
ID_CIUDAD_BOGOTA  = 10
COL_FECHA_CAL     = "DATE"
HABIL_PRINCIPAL   = "UPPER(TRIM(TIPO_DIA)) = 'DIA HABIL' AND FECHA_FESTIVA IS NULL"    # igual que 0330
POBLACION_SLA     = "origen_sla IN ('SLA_UNICO', 'SLA_POR_REGLA') AND estado_desenlace NOT IN ('DEVUELTO', 'SINIESTRADO')"

# COMMAND ----------

# Utilidades: consulta, guardado y vista
G   = f"read_parquet('{RUTA_GOLD_V3}')"
CAL = f"read_parquet('{RUTA_CALENDARIO}')"
try:
    display
except NameError:          # fuera de Databricks
    display = print

def q(sql):
    return duckdb.sql(sql).df()

def guardar(nombre, df):
    df.to_csv(f"{CARPETA_SALIDAS}P6_{nombre}.csv", index=False)
    display(df)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## V1 · ¿`FECHA_ENTREGA` es una entrega real?
# MAGIC
# MAGIC **Respalda S1** si las entregas se reparten en horario de reparto (aprox. 7–20 h, de lunes a sábado), sin un pico en un minuto fijo, y si casi todas ocurren el mismo día de la salida o el siguiente.
# MAGIC **Lo contradice** si una parte grande cae a las 00:00:00 o en un mismo minuto todos los días (cierre por lote), o si la entrega suele ser días después de la salida.

# COMMAND ----------

# V1.a Hora del día de la entrega (solo entregadas)
v1a = q(f'''
    SELECT hour(fecha_entrega) AS hora, COUNT(*) AS guias,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM {G} WHERE estado_desenlace = 'ENTREGADO'
    GROUP BY 1 ORDER BY 1''')
guardar("v1a_hora_entrega", v1a)

# COMMAND ----------

# V1.b Concentración: minuto exacto más frecuente, medianoche exacta y segundos en cero
v1b = q(f'''
    WITH E AS (SELECT fecha_entrega AS f FROM {G} WHERE estado_desenlace = 'ENTREGADO'),
    M AS (SELECT strftime(f, '%H:%M') AS minuto, COUNT(*) AS n FROM E GROUP BY 1)
    SELECT (SELECT COUNT(*) FROM E) AS entregadas,
           ROUND(100.0 * (SELECT COUNT(*) FROM E WHERE strftime(f, '%H:%M:%S') = '00:00:00') / (SELECT COUNT(*) FROM E), 2) AS pct_medianoche_exacta,
           ROUND(100.0 * (SELECT COUNT(*) FROM E WHERE second(f) = 0) / (SELECT COUNT(*) FROM E), 2) AS pct_segundos_en_cero,
           (SELECT minuto FROM M ORDER BY n DESC LIMIT 1) AS minuto_mas_frecuente,
           ROUND(100.0 * (SELECT MAX(n) FROM M) / (SELECT COUNT(*) FROM E), 2) AS pct_minuto_mas_frecuente''')
guardar("v1b_concentracion_entrega", v1b)

# COMMAND ----------

# V1.c Día de la entrega frente al día de la salida (T0)
v1c = q(f'''
    SELECT CASE WHEN h_ultima_milla_bruta < 0 THEN 'antes de la salida'
                WHEN CAST(fecha_entrega AS DATE) = CAST(t0 AS DATE) THEN 'mismo día'
                WHEN date_diff('day', CAST(t0 AS DATE), CAST(fecha_entrega AS DATE)) = 1 THEN 'día siguiente'
                WHEN date_diff('day', CAST(t0 AS DATE), CAST(fecha_entrega AS DATE)) <= 3 THEN '2 o 3 días'
                ELSE 'más de 3 días' END AS entrega_frente_a_salida,
           COUNT(*) AS guias, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM {G} WHERE estado_desenlace = 'ENTREGADO'
    GROUP BY 1 ORDER BY guias DESC''')
guardar("v1c_entrega_vs_salida", v1c)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V2 · ¿El proceso 9 es la salida a reparto?
# MAGIC
# MAGIC Se miran los movimientos de cada guía entregada (procesos 5, 6 y 9, los únicos extraídos) frente a su `FECHA_ENTREGA`.
# MAGIC
# MAGIC **Respalda S2** si en la gran mayoría el proceso 9 es el **último** movimiento antes de la entrega, viene **después** de 5 o 6 y la entrega llega pocas horas después.
# MAGIC **Lo contradice** si el 9 suele ir seguido de otro 5 o 6 (ida y vuelta entre bodegas) o si muchas entregas no tienen ningún 9 antes.

# COMMAND ----------

# V2.a Último proceso registrado antes de la entrega
v2a = q(f'''
    WITH GU AS (SELECT CAST(NUMERO_GUIA AS BIGINT) AS guia, FECHA_ENTREGA AS fe
                FROM read_parquet('{RUTA_GUIAS}') WHERE FECHA_ENTREGA IS NOT NULL),
    MV AS (SELECT CAST(NUMERO_GUIA AS BIGINT) AS guia, ID_PROCESO, FECHA_HORA_MOVIMIENTO AS fm
           FROM read_parquet('{RUTA_MOVS_GLOB}')),
    U AS (SELECT GU.guia, arg_max(MV.ID_PROCESO, MV.fm) AS ultimo_proceso
          FROM GU JOIN MV ON MV.guia = GU.guia AND MV.fm <= GU.fe GROUP BY GU.guia)
    SELECT COALESCE(CAST(ultimo_proceso AS VARCHAR), 'sin movimiento antes de la entrega') AS ultimo_proceso_antes_de_entrega,
           COUNT(*) AS guias, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM GU LEFT JOIN U USING (guia) GROUP BY 1 ORDER BY guias DESC''')
guardar("v2a_ultimo_proceso_antes_de_entrega", v2a)

# COMMAND ----------

# V2.b Secuencia: ¿qué viene después de la primera salida (9)?
v2b = q(f'''
    WITH MV AS (SELECT CAST(NUMERO_GUIA AS BIGINT) AS guia, ID_PROCESO, FECHA_HORA_MOVIMIENTO AS fm
                FROM read_parquet('{RUTA_MOVS_GLOB}')),
    S AS (SELECT guia, MIN(fm) AS t9 FROM MV WHERE ID_PROCESO = 9 GROUP BY guia),
    R AS (SELECT S.guia,
                 bool_or(MV.ID_PROCESO IN (5, 6) AND MV.fm < S.t9) AS hubo_entrada_antes,
                 bool_or(MV.ID_PROCESO IN (5, 6) AND MV.fm > S.t9) AS hubo_entrada_despues,
                 COUNT(*) FILTER (WHERE MV.ID_PROCESO = 9) AS salidas
          FROM S JOIN MV USING (guia) GROUP BY S.guia)
    SELECT hubo_entrada_antes, hubo_entrada_despues, salidas > 1 AS mas_de_una_salida,
           COUNT(*) AS guias, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM R GROUP BY ALL ORDER BY guias DESC''')
guardar("v2b_secuencia_tras_salida", v2b)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V3 · ¿Las horas están en hora local?
# MAGIC
# MAGIC **Respalda hora local** si la admisión y la salida tienen su pico en la mañana (T0 entre 8 y 10 h ya se vio en 0160) y casi no hay actividad entre 0 y 5 h.
# MAGIC **Indica UTC** si los picos aparecen unas 5 horas más tarde (salidas hacia las 13–15 h) y hay actividad nocturna que en hora local sería de madrugada.

# COMMAND ----------

# V3 Hora del día de admisión, salida (T0) y entrega, en la misma tabla
v3 = q(f'''
    WITH B AS (SELECT FECHA_ENVIO, t0, fecha_entrega FROM {G} WHERE estado_desenlace = 'ENTREGADO')
    SELECT h.hora,
           ROUND(100.0 * COUNT(*) FILTER (WHERE hour(FECHA_ENVIO)   = h.hora) / COUNT(*), 2) AS pct_admision,
           ROUND(100.0 * COUNT(*) FILTER (WHERE hour(t0)            = h.hora) / COUNT(*), 2) AS pct_salida_t0,
           ROUND(100.0 * COUNT(*) FILTER (WHERE hour(fecha_entrega) = h.hora) / COUNT(*), 2) AS pct_entrega
    FROM B, (SELECT range AS hora FROM range(24)) h
    GROUP BY h.hora ORDER BY h.hora''')
guardar("v3_hora_local", v3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V4 · Entregas antes de la salida y entregas sin hora
# MAGIC
# MAGIC **Hipótesis a probar:** son errores de registro menores (entrega digitada sin hora, o reloj desfasado unos minutos), no entregas reales antes de salir a reparto.
# MAGIC **Respalda** si la mayoría tiene la hora en 00:00:00 o un desfase de menos de una hora, y si no se concentran en pocas zonas.
# MAGIC **Si no**, se reportan como inconsistencia y se mantienen excluidas de la regresión (como ya hace 0411).

# COMMAND ----------

# V4.a Tamaño del desfase y si la entrega viene sin hora
v4a = q(f'''
    SELECT CASE WHEN h_ultima_milla_bruta >= -1  THEN 'menos de 1 h'
                WHEN h_ultima_milla_bruta >= -24 THEN '1 a 24 h'
                ELSE 'más de 24 h' END AS desfase,
           strftime(fecha_entrega, '%H:%M:%S') = '00:00:00' AS entrega_sin_hora,
           COUNT(*) AS guias, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM {G} WHERE estado_desenlace = 'ENTREGADO' AND h_ultima_milla_bruta < 0
    GROUP BY ALL ORDER BY guias DESC''')
guardar("v4a_entregas_antes_de_salida", v4a)

# COMMAND ----------

# V4.b ¿Se concentran en pocas zonas o meses? (participación de las 10 zonas con más casos)
v4b = q(f'''
    WITH N AS (SELECT zona_urbana, date_trunc('month', t0) AS mes, COUNT(*) AS n,
                      COUNT(*) FILTER (WHERE h_ultima_milla_bruta < 0) AS negativas
               FROM {G} WHERE estado_desenlace = 'ENTREGADO' GROUP BY ALL)
    SELECT 'mes' AS por, CAST(mes AS VARCHAR) AS grupo, SUM(negativas) AS negativas,
           ROUND(100.0 * SUM(negativas) / SUM(n), 3) AS pct_del_grupo
    FROM N GROUP BY ALL
    UNION ALL
    SELECT 'top 10 zonas', 'participación en el total de negativas',
           SUM(negativas), ROUND(100.0 * SUM(negativas) / (SELECT SUM(negativas) FROM N), 2)
    FROM (SELECT zona_urbana, SUM(negativas) AS negativas FROM N GROUP BY 1 ORDER BY 2 DESC LIMIT 10)
    ORDER BY por, grupo''')
guardar("v4b_negativas_por_mes_y_zona", v4b)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V5 · Devoluciones
# MAGIC
# MAGIC **Respalda tratarlas aparte (S6)** si las `DEVUELTO` tardan mucho más, tienen más salidas a zona o un patrón de hora distinto al de las entregadas: su `FECHA_ENTREGA` sería la entrega al remitente, no al destinatario.

# COMMAND ----------

# V5 Comparación de devoluciones, siniestros y entregadas
v5 = q(f'''
    SELECT estado_desenlace, COUNT(*) AS guias,
           median(h_total) AS mediana_h_admision_a_entrega,
           quantile_cont(h_total, 0.9) AS p90_h_admision_a_entrega,
           median(h_ultima_milla_bruta) AS mediana_h_salida_a_entrega,
           AVG(n_salidas_zona) AS salidas_promedio,
           ROUND(100.0 * AVG((n_salidas_zona > 1)::INT), 2) AS pct_mas_de_una_salida
    FROM {G} WHERE estado_desenlace IN ('ENTREGADO', 'DEVUELTO', 'SINIESTRADO')
    GROUP BY 1 ORDER BY guias DESC''')
guardar("v5_devoluciones", v5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V6 · ¿La operación trabaja los sábados?
# MAGIC
# MAGIC **Respalda contar el sábado como hábil (S4)** si el volumen de salidas y entregas del sábado es comparable al de un día entre semana (por ejemplo, más de la mitad) y el domingo es marginal.
# MAGIC **Lo contradice** si el sábado se parece al domingo.
# MAGIC El calendario se revisa también: qué `TIPO_DIA` tiene cada día de la semana en 2026.

# COMMAND ----------

# V6.a Volumen promedio por día de la semana (0 = domingo)
v6a = q(f'''
    WITH S AS (SELECT CAST(t0 AS DATE) AS d, COUNT(*) AS salidas FROM {G} GROUP BY 1),
         E AS (SELECT CAST(fecha_entrega AS DATE) AS d, COUNT(*) AS entregas FROM {G}
               WHERE estado_desenlace = 'ENTREGADO' GROUP BY 1),
         C AS (SELECT CAST("{COL_FECHA_CAL}" AS DATE) AS d, bool_and(COALESCE({HABIL_PRINCIPAL}, FALSE)) AS habil
               FROM {CAL} GROUP BY 1)
    SELECT dayofweek(C.d) AS dia_semana, COUNT(*) AS dias,
           COUNT(*) FILTER (WHERE C.habil) AS dias_habiles_en_calendario,
           ROUND(AVG(COALESCE(S.salidas, 0)), 0) AS salidas_promedio_dia,
           ROUND(AVG(COALESCE(E.entregas, 0)), 0) AS entregas_promedio_dia
    FROM C LEFT JOIN S USING (d) LEFT JOIN E USING (d)
    WHERE C.d BETWEEN DATE '2026-01-01' AND DATE '2026-08-31'
    GROUP BY 1 ORDER BY 1''')
guardar("v6a_volumen_dia_semana", v6a)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V7 · Plazos fraccionarios en RED_OPERATIVA
# MAGIC
# MAGIC 0330 redondea hacia arriba (`CEIL`) los plazos de menos de un día. Aquí se ve qué valores son, cuántas guías los tienen y si las horas (`TIEMPO_ENTREGA_COMERCIAL_HOR`) los explican.
# MAGIC **Lectura:** si el valor en horas es pequeño (por ejemplo 4, 8 o 12 h) y esas guías se entregan casi siempre el mismo día, el plazo fraccionario significa **entrega el mismo día**, y la variante «mismo día» de V8 es la más realista para ellas.

# COMMAND ----------

# V7.a Valores no enteros del plazo en RED
v7a = q(f'''
    SELECT TIEMPO_ENTREGA_COMERCIAL_DIA AS plazo_dias, TIEMPO_ENTREGA_COMERCIAL_HOR AS plazo_horas, COUNT(*) AS filas_red
    FROM read_parquet('{RUTA_RED}')
    WHERE TIEMPO_ENTREGA_COMERCIAL_DIA <> FLOOR(TIEMPO_ENTREGA_COMERCIAL_DIA)
    GROUP BY ALL ORDER BY filas_red DESC LIMIT 30''')
guardar("v7a_plazos_fraccionarios_red", v7a)

# COMMAND ----------

# V7.b Guías con plazo en horas menor que el plazo redondeado, y cuándo se entregan
v7b = q(f'''
    SELECT sla_dias, sla_h_continuas < sla_dias * 24 AS plazo_original_fraccionario,
           COUNT(*) AS guias,
           ROUND(100.0 * AVG((CAST(fecha_entrega AS DATE) = CAST(FECHA_ENVIO AS DATE))::INT), 2) AS pct_entregada_mismo_dia_admision,
           median(h_total) AS mediana_h_admision_a_entrega
    FROM {G} WHERE {POBLACION_SLA} AND estado_desenlace = 'ENTREGADO'
    GROUP BY ALL ORDER BY sla_dias, plazo_original_fraccionario''')
guardar("v7b_guias_plazo_fraccionario", v7b)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V8 · ¿Con qué definición se obtiene el ~20 %?
# MAGIC
# MAGIC Primero, la **curva de cumplimiento**: qué porcentaje de las entregadas llega al final del día hábil k después de la admisión, por plazo comprometido. Muestra cuánta holgura haría falta para bajar al 20 %.
# MAGIC Después, una rejilla de variantes sobre la **misma población** (SLA único o por regla, sin devoluciones ni siniestros): unidad de día, si cuenta el día de admisión, sábado, plazo fraccionario, días de gracia, punto de inicio y origen local o nacional.
# MAGIC
# MAGIC **Lectura fijada antes de correr:** la variante 1 sigue siendo la principal. Si alguna variante razonable se acerca al 20 %, se reporta como la explicación más probable de la diferencia, con la advertencia de que la empresa no documentó su definición. Si ninguna se acerca, el 20 % se reporta como cifra no reproducible.

# COMMAND ----------

# V8.a Curva de cumplimiento: días hábiles usados desde la admisión, por plazo
v8a = q(f'''
    WITH C AS (SELECT CAST("{COL_FECHA_CAL}" AS DATE) AS fecha, bool_and(COALESCE({HABIL_PRINCIPAL}, FALSE)) AS es_habil
               FROM {CAL} GROUP BY 1),
    H AS (SELECT fecha, SUM(es_habil::INT) OVER (ORDER BY fecha) AS n_hab FROM C),
    B AS (SELECT LEAST(GREATEST(HE.n_hab - H0.n_hab, 0), 6) AS k, LEAST(sla_dias, 4) AS plazo
          FROM {G} G2
          JOIN H H0 ON H0.fecha = CAST(G2.FECHA_ENVIO AS DATE)
          JOIN H HE ON HE.fecha = CAST(G2.fecha_entrega AS DATE)
          WHERE {POBLACION_SLA} AND estado_desenlace = 'ENTREGADO')
    SELECT plazo AS plazo_dias_4_o_mas, k AS dias_habiles_usados_6_o_mas, COUNT(*) AS guias,
           ROUND(100.0 * SUM(COUNT(*)) OVER (PARTITION BY plazo ORDER BY k) / SUM(COUNT(*)) OVER (PARTITION BY plazo), 2) AS pct_acumulado
    FROM B GROUP BY plazo, k ORDER BY plazo, k''')
guardar("v8a_curva_cumplimiento", v8a)

# COMMAND ----------

# V8.b Función de prevalencia para una variante del SLA
def prevalencia_variante(nombre, condicion_habil=HABIL_PRINCIPAL, plazo="sla_dias", cuenta_dia_inicio=False,
                         gracia=0, inicio="FECHA_ENVIO", filtro="TRUE"):
    '''Vencimiento = fin del día hábil número (plazo + gracia) contado desde el día de inicio.
    plazo 0 = el mismo día de inicio (o el siguiente hábil si ese día no es hábil).'''
    ajuste = "CASE WHEN H0.es_habil THEN 1 ELSE 0 END" if cuenta_dia_inicio else "0"
    return q(f'''
    WITH C AS (SELECT CAST("{COL_FECHA_CAL}" AS DATE) AS fecha, bool_and(COALESCE({condicion_habil}, FALSE)) AS es_habil
               FROM {CAL} GROUP BY 1),
    HAB AS (SELECT fecha, es_habil, SUM(es_habil::INT) OVER (ORDER BY fecha) AS n_hab FROM C),
    HABIL AS (SELECT fecha, n_hab FROM HAB WHERE es_habil),
    POB AS (SELECT {inicio} AS ini, fecha_entrega, t0, CAST({plazo} AS INT) + {gracia} AS d
            FROM {G} WHERE {POBLACION_SLA} AND ({filtro})),
    V AS (SELECT P.*, epoch_ms(CAST(HV.fecha AS TIMESTAMP) + INTERVAL 1 DAY) AS lim
          FROM POB P
          LEFT JOIN HAB H0 ON H0.fecha = CAST(P.ini AS DATE)
          LEFT JOIN HABIL HV ON HV.n_hab = H0.n_hab + GREATEST(P.d, 0)
                                + CASE WHEN P.d <= 0 AND NOT H0.es_habil THEN 1 ELSE 0 END
                                - CASE WHEN P.d > 0 THEN {ajuste} ELSE 0 END)
    SELECT '{nombre}' AS variante, COUNT(*) AS guias,
           ROUND(100.0 * AVG(CASE WHEN lim IS NULL THEN NULL
                                  WHEN fecha_entrega IS NOT NULL THEN (epoch_ms(fecha_entrega) >= lim)::INT
                                  WHEN lim <= epoch_ms(TIMESTAMP '{CORTE_OBSERVACION}') THEN 1 END), 2) AS pct_incumplimiento,
           ROUND(100.0 * AVG((epoch_ms(t0) >= lim)::INT), 2) AS pct_vencido_antes_de_t0,
           COUNT(*) FILTER (WHERE lim IS NULL) AS sin_vencimiento
    FROM V''')

# COMMAND ----------

# V8.c Rejilla de variantes (la 1 es la principal de 0330 y debe reproducir su 49,4 %)
SIN_SABADO = HABIL_PRINCIPAL + f' AND dayofweek(CAST("{COL_FECHA_CAL}" AS DATE)) <> 6'
PLAZO_FRACC_MISMO_DIA = "CASE WHEN sla_h_continuas < sla_dias * 24 THEN FLOOR(sla_h_continuas / 24) ELSE sla_dias END"
LOCAL = f"ID_CIUDAD_ORIGEN = {ID_CIUDAD_BOGOTA}"
variantes = [
    ("1 principal (hábiles, sin día de admisión, CEIL)", {}),
    ("2 contando el día de admisión", {"cuenta_dia_inicio": True}),
    ("3 sábado no hábil", {"condicion_habil": SIN_SABADO}),
    ("4 días calendario", {"condicion_habil": "TRUE"}),
    ("5 plazo fraccionario = mismo día", {"plazo": PLAZO_FRACC_MISMO_DIA}),
    ("6 un día hábil de gracia", {"gracia": 1}),
    ("7 dos días hábiles de gracia", {"gracia": 2}),
    ("8 plazo contado desde la salida a zona (T0)", {"inicio": "t0"}),
    ("9 solo origen Bogotá", {"filtro": LOCAL}),
    ("10 solo origen fuera de Bogotá", {"filtro": f"NOT ({LOCAL})"}),
    ("11 solo plazo de 1 día", {"filtro": "sla_dias = 1"}),
    ("12 solo plazo de 2 días o más", {"filtro": "sla_dias >= 2"}),
]
v8c = pd.concat([prevalencia_variante(n, **kw) for n, kw in variantes], ignore_index=True)
guardar("v8c_variantes_sla", v8c)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V9 · ¿Los nombres de zona sirven para ubicarlas?
# MAGIC
# MAGIC Lista de zonas urbanas con su categoría y cuántas guías tienen (solo el nombre de la zona; no se leen conductores, placas ni usuarios de `ZONASURBANAS`). El CSV trae una columna `localidad` vacía para llenarla a mano con la cartografía de IDECA; esa tabla es la entrada de la evaluación espacial.
# MAGIC **Lectura:** si la mayoría de los nombres son barrios, UPZ o localidades reconocibles, la asignación manual es viable y se documenta; si son códigos internos, la unidad espacial queda en `categoria_zona`.

# COMMAND ----------

# V9 Plantilla zona urbana → localidad
v9 = q(f'''
    SELECT zona_urbana, categoria_zona, COUNT(*) AS guias,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 3) AS pct_guias,
           CAST(NULL AS VARCHAR) AS localidad, CAST(NULL AS VARCHAR) AS upz, CAST(NULL AS VARCHAR) AS nota_asignacion
    FROM {G} GROUP BY 1, 2 ORDER BY guias DESC''')
print(f"{len(v9):,} zonas; las 20 con más guías concentran {v9.pct_guias.head(20).sum():.1f} % de las guías")
guardar("v9_plantilla_zona_localidad", v9)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué entregar
# MAGIC
# MAGIC Los CSV `P6_*` de la carpeta `V3/VERIFICACION/`. Con ellos, cada supuesto del documento del Hito 4 pasa a una de tres categorías: **verificado con los datos**, **respaldado parcialmente** o **supuesto declarado con sensibilidad**.