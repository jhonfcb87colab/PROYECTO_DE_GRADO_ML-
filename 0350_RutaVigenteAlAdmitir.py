# Databricks notebook source
# MAGIC %md
# MAGIC # 0350 · Sensibilidad: plazo con la ruta vigente al admitir
# MAGIC
# MAGIC **Para qué.** Iván pidió (P8, 25-10) que el plazo de cada guía sea el de la ruta **vigente el día de la admisión**, no la fila más reciente de `RED_OPERATIVA_OFERTA`. 0330 usa la ruta activa con `FECHA_MODIFICACION` más reciente, aunque esa modificación sea posterior al envío. Este cuaderno cuenta **cuántas guías cambian de plazo** si solo se aceptan filas de RED modificadas en o antes de la fecha de envío, y cuánto cambian el incumplimiento, el porcentaje vencido antes de T0 y la población con margen que usa el modelo.
# MAGIC
# MAGIC **Regla de lectura, fijada antes de correr.**
# MAGIC - Si cambia el plazo de menos del 1 % de las guías y la prevalencia se mueve menos de 0,5 puntos, se deja la regla de 0330 y se reporta esta tabla como sensibilidad. No se reentrena nada.
# MAGIC - Si cambia más, se reporta la magnitud y se decide con Iván si se reconstruye la base. La prueba final ya corrida no se repite.
# MAGIC
# MAGIC **Limitación.** `RED_OPERATIVA_OFERTA` guarda las filas vigentes hoy, no el historial de versiones. Si una ruta cambió de plazo, la versión anterior puede no estar. Las guías sin ninguna fila anterior al envío conservan el plazo de 0330 y se cuentan aparte (`sin_fila_previa`). `ID_ESTADO_RUTA` también es el estado de hoy.
# MAGIC
# MAGIC **Lee** `GOLD_V3_ENVIO.parquet`, `RED_OPERATIVA_OFERTA` y el calendario. **Escribe** solo CSV agregados `P6r_*` en `V3/VERIFICACION/`. No modifica la base.

# COMMAND ----------

# MAGIC %pip install "duckdb<2"

# COMMAND ----------

# CONFIGURACIÓN · mismas rutas y reglas de 0330_BaseAnaliticaV3_2 y 0340
import os, duckdb, pandas as pd
PATH_DATABRICKS = "/Volumes/proyectogrado/source/source/"
PAR             = f"{PATH_DATABRICKS}PARAMETRICAS/"
RUTA_RED        = f"{PAR}PAR.RED_OPERATIVA_OFERTA.parquet"
RUTA_CALENDARIO = f"{PAR}DIM.VW_TIEMPO_CALENDARIO_Y_FESTIVOS.parquet"
RUTA_GOLD_V3    = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3/GOLD_V3_ENVIO.parquet"
CARPETA_SALIDAS = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3/VERIFICACION/"
os.makedirs(CARPETA_SALIDAS, exist_ok=True)

CORTE_OBSERVACION = "2026-08-31 23:59:59"
COL_FECHA_CAL     = "DATE"
HABIL_PRINCIPAL   = "UPPER(TRIM(TIPO_DIA)) = 'DIA HABIL' AND FECHA_FESTIVA IS NULL"    # igual que 0330
POBLACION_SLA     = "origen_sla IN ('SLA_UNICO', 'SLA_POR_REGLA') AND estado_desenlace NOT IN ('DEVUELTO', 'SINIESTRADO')"

G   = f"read_parquet('{RUTA_GOLD_V3}')"
CAL = f"read_parquet('{RUTA_CALENDARIO}')"
try:
    display
except NameError:          # fuera de Databricks
    display = print

def q(sql):
    return duckdb.sql(sql).df()

def guardar(nombre, df):
    df.to_csv(f"{CARPETA_SALIDAS}P6r_{nombre}.csv", index=False)
    display(df)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · ¿Cuándo se modificó RED?
# MAGIC
# MAGIC Si casi todas las filas de RED se modificaron antes de enero de 2026, la ruta vigente al admitir es la misma de 0330 para casi todas las guías y el cambio es mínimo.

# COMMAND ----------

# 1. Fecha de modificación de las filas de RED, por mes (solo conteos)
r1 = q(f'''
    SELECT CASE WHEN FECHA_MODIFICACION IS NULL THEN 'sin fecha'
                WHEN FECHA_MODIFICACION < TIMESTAMP '2026-01-01' THEN 'antes de 2026'
                ELSE strftime(date_trunc('month', FECHA_MODIFICACION), '%Y-%m') END AS modificada,
           COUNT(*) AS filas_red,
           COUNT(*) FILTER (WHERE COALESCE(CAST(ID_ESTADO_RUTA AS INT), 0) = 1) AS filas_activas
    FROM read_parquet('{RUTA_RED}') GROUP BY 1 ORDER BY 1''')
guardar("r1_red_por_fecha_modificacion", r1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Plazo con la ruta vigente al admitir
# MAGIC
# MAGIC Para cada guía de la población principal se toman solo las filas de RED de su clave (con la misma sustitución de 0330 cuando `ID_TIEMPO` o `ID_MEDTRANS` vienen en 0) cuya `FECHA_MODIFICACION` es igual o anterior a `FECHA_ENVIO`. Entre ellas se aplica la regla de 0330: activa primero, luego la más reciente y, en empate, el plazo mayor. El vencimiento se calcula igual que en 0330: fin del día hábil número `plazo`, sin contar el día de admisión.

# COMMAND ----------

# 2. Tabla temporal con el plazo de 0330 y el vigente al admitir (en memoria; no se guarda por guía)
con = duckdb.connect()
con.execute(f'''
CREATE TEMP TABLE RV AS
WITH
C AS (SELECT CAST("{COL_FECHA_CAL}" AS DATE) AS fecha, bool_and(COALESCE({HABIL_PRINCIPAL}, FALSE)) AS es_habil
      FROM {CAL} GROUP BY 1),
HAB AS (SELECT fecha, es_habil, SUM(es_habil::INT) OVER (ORDER BY fecha) AS n_hab FROM C),
HABIL AS (SELECT fecha, n_hab FROM HAB WHERE es_habil),
REDR AS (SELECT ID_CIUDAD_ORIGEN, ID_CIUDAD_DESTINO, ID_TIEMPO, ID_MEDTRANS, FECHA_MODIFICACION,
                TIEMPO_ENTREGA_COMERCIAL_DIA AS dia, COALESCE(CAST(ID_ESTADO_RUTA AS INT), 0) AS activa
         FROM read_parquet('{RUTA_RED}')),
POB AS (SELECT guia, FECHA_ENVIO, fecha_entrega, t0, sla_dias AS sla_0330, date_trunc('month', FECHA_ENVIO) AS mes,
               ID_CIUDAD_ORIGEN, ID_CIUDAD_DESTINO,
               CASE WHEN COALESCE(ID_TIEMPO,0)   <> 0 THEN ID_TIEMPO   ELSE 1 END AS k_tiempo,
               CASE WHEN COALESCE(ID_MEDTRANS,0) <> 0 THEN ID_MEDTRANS ELSE 1 END AS k_medtrans
        FROM {G} WHERE {POBLACION_SLA}),
VIG AS (SELECT P.guia,
               first(R.dia ORDER BY R.activa DESC, R.FECHA_MODIFICACION DESC NULLS LAST, R.dia DESC) AS dia_vigente
        FROM POB P JOIN REDR R
          ON R.ID_CIUDAD_ORIGEN = P.ID_CIUDAD_ORIGEN AND R.ID_CIUDAD_DESTINO = P.ID_CIUDAD_DESTINO
         AND R.ID_TIEMPO = P.k_tiempo AND R.ID_MEDTRANS = P.k_medtrans
         AND R.FECHA_MODIFICACION <= P.FECHA_ENVIO
        GROUP BY P.guia),
B AS (SELECT P.*, V.dia_vigente IS NULL AS sin_fila_previa,
             COALESCE(CAST(CEIL(V.dia_vigente) AS INT), P.sla_0330) AS sla_vigente
      FROM POB P LEFT JOIN VIG V USING (guia))
SELECT B.guia, B.mes, B.sla_0330, B.sla_vigente, B.sin_fila_previa,
       B.fecha_entrega IS NOT NULL AS entregada,
       epoch_ms(B.fecha_entrega) AS ent_ms, epoch_ms(B.t0) AS t0_ms,
       epoch_ms(CAST(H1.fecha AS TIMESTAMP) + INTERVAL 1 DAY) AS lim_0330,
       epoch_ms(CAST(H2.fecha AS TIMESTAMP) + INTERVAL 1 DAY) AS lim_vigente
FROM B
LEFT JOIN HAB H0 ON H0.fecha = CAST(B.FECHA_ENVIO AS DATE)
LEFT JOIN HABIL H1 ON H1.n_hab = H0.n_hab + B.sla_0330
LEFT JOIN HABIL H2 ON H2.n_hab = H0.n_hab + B.sla_vigente
''')
CORTE_MS = f"epoch_ms(TIMESTAMP '{CORTE_OBSERVACION}')"
def etiqueta(lim):
    return f"CASE WHEN {lim} IS NULL THEN NULL WHEN entregada THEN (ent_ms >= {lim})::INT WHEN {lim} <= {CORTE_MS} THEN 1 END"
print(con.sql("SELECT COUNT(*) AS guias FROM RV").df())

# COMMAND ----------

# 2.a Cuántas guías cambian de plazo
r2a = con.sql('''
    SELECT CASE WHEN sin_fila_previa THEN 'sin fila de RED anterior al envío (conserva el plazo de 0330)'
                WHEN sla_vigente = sla_0330 THEN 'mismo plazo'
                WHEN sla_vigente > sla_0330 THEN 'plazo vigente MAYOR'
                ELSE 'plazo vigente MENOR' END AS caso,
           COUNT(*) AS guias, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 3) AS pct
    FROM RV GROUP BY 1 ORDER BY guias DESC''').df()
guardar("r2a_guias_que_cambian", r2a)

# COMMAND ----------

# 2.b Matriz de transición del plazo (días): filas = 0330, columnas = vigente al admitir
r2b = con.sql('''SELECT LEAST(sla_0330, 6) AS plazo_0330_6_o_mas, LEAST(sla_vigente, 6) AS plazo_vigente_6_o_mas, COUNT(*) AS guias
                 FROM RV WHERE NOT sin_fila_previa GROUP BY ALL ORDER BY 1, 2''').df()
guardar("r2b_transicion_plazo", r2b.pivot(index="plazo_0330_6_o_mas", columns="plazo_vigente_6_o_mas", values="guias")
        .fillna(0).astype(int).reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Efecto en la etiqueta y en la población del modelo
# MAGIC
# MAGIC Misma población y misma regla de vencimiento; solo cambia el plazo. «Con margen en T0» es la población de modelado de 0411 (guías que aún no estaban vencidas al salir a reparto).

# COMMAND ----------

# 3.a Incumplimiento, vencidas antes de T0 e incumplimiento con margen: 0330 frente a vigente
partes = []
for regla, lim in [("0330: activa más reciente", "lim_0330"), ("vigente al admitir", "lim_vigente")]:
    partes.append(f'''
    SELECT '{regla}' AS regla, COUNT(*) AS guias,
           ROUND(100.0 * AVG({etiqueta(lim)}), 2) AS pct_incumplimiento,
           ROUND(100.0 * AVG((t0_ms >= {lim})::INT), 2) AS pct_vencido_antes_de_t0,
           COUNT(*) FILTER (WHERE t0_ms < {lim}) AS guias_con_margen_en_t0,
           ROUND(100.0 * AVG({etiqueta(lim)}) FILTER (WHERE t0_ms < {lim}), 2) AS pct_incumplimiento_con_margen
    FROM RV''')
r3a = con.sql(" UNION ALL ".join(partes)).df()
guardar("r3a_efecto_en_etiqueta", r3a)

# COMMAND ----------

# 3.b Población del modelo: guías que entran o salen de la cohorte con margen, y etiquetas que cambian
r3b = con.sql(f'''
    SELECT COUNT(*) FILTER (WHERE t0_ms <  lim_0330 AND t0_ms >= lim_vigente) AS salen_de_la_poblacion_con_margen,
           COUNT(*) FILTER (WHERE t0_ms >= lim_0330 AND t0_ms <  lim_vigente) AS entran_a_la_poblacion_con_margen,
           COUNT(*) FILTER (WHERE {etiqueta("lim_0330")} = 0 AND {etiqueta("lim_vigente")} = 1) AS etiqueta_pasa_de_0_a_1,
           COUNT(*) FILTER (WHERE {etiqueta("lim_0330")} = 1 AND {etiqueta("lim_vigente")} = 0) AS etiqueta_pasa_de_1_a_0,
           COUNT(*) FILTER (WHERE t0_ms < lim_0330 AND t0_ms < lim_vigente
                            AND {etiqueta("lim_0330")} IS DISTINCT FROM {etiqueta("lim_vigente")}) AS etiqueta_cambia_dentro_de_la_poblacion_con_margen
    FROM RV''').df()
guardar("r3b_efecto_en_poblacion", r3b)

# COMMAND ----------

# 3.c Por mes de envío (para ver si el cambio se concentra en algún periodo, p. ej. validación o prueba)
r3c = con.sql(f'''
    SELECT strftime(mes, '%Y-%m') AS mes, COUNT(*) AS guias,
           ROUND(100.0 * AVG((NOT sin_fila_previa AND sla_vigente <> sla_0330)::INT), 3) AS pct_cambia_plazo,
           ROUND(100.0 * AVG(sin_fila_previa::INT), 2) AS pct_sin_fila_previa,
           ROUND(100.0 * AVG({etiqueta("lim_0330")}), 2) AS pct_incumplimiento_0330,
           ROUND(100.0 * AVG({etiqueta("lim_vigente")}), 2) AS pct_incumplimiento_vigente
    FROM RV GROUP BY 1 ORDER BY 1''').df()
guardar("r3c_por_mes", r3c)

# COMMAND ----------

# 4. Lectura con la regla fijada arriba
cambia = r2a.loc[r2a.caso.str.startswith("plazo vigente"), "pct"].sum()
dif = abs(r3a.pct_incumplimiento.iloc[1] - r3a.pct_incumplimiento.iloc[0])
sin_previa = r2a.loc[r2a.caso.str.startswith("sin fila"), "pct"].sum()
print(f"Guías con plazo distinto: {cambia:.3f} % · cambio en la prevalencia: {dif:.2f} puntos · sin fila previa: {sin_previa:.2f} %")
if cambia < 1 and dif < 0.5:
    print("Lectura: efecto despreciable. Se mantiene la regla de 0330 y esta tabla va como sensibilidad.")
else:
    print("Lectura: el efecto supera el umbral fijado. Reportar la magnitud y decidir con Iván si se reconstruye la base.")
if sin_previa > 20:
    print("Aviso: muchas guías no tienen fila de RED anterior al envío; RED no guarda el historial y la comparación es parcial.")
con.close()