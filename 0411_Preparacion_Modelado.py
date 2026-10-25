# Databricks notebook source
# MAGIC %md
# MAGIC # 0411 · Preparación para el modelado (OE3, OE4)
# MAGIC
# MAGIC **Este es el primer cuaderno del modelado.** Se corre una vez; los cuadernos de cada modelo leen lo que deja aquí.
# MAGIC
# MAGIC | Cuaderno | Qué hace | Depende de |
# MAGIC | --- | --- | --- |
# MAGIC | **0411 Preparación** (este) | Datos, exclusiones, variables, historia sin futuro y pliegues | 0330 / 0160 |
# MAGIC | 0412 Modelos base | Referencia sin modelo y regresión lineal / logística | 0411 |
# MAGIC | 0413 Random Forest | Búsqueda de hiperparámetros y validación | 0411 |
# MAGIC | 0414 XGBoost | Búsqueda de hiperparámetros y validación | 0411 |
# MAGIC | 0415 LightGBM | Búsqueda de hiperparámetros y validación | 0411 |
# MAGIC | 0416 Comparación y prueba final | Junta los resultados, H1/H2, calibración, umbral, decisión y prueba final (una sola vez) | 0412 a 0415 |
# MAGIC | 0420 Interpretabilidad | Importancias y SHAP del modelo final | 0416 |
# MAGIC
# MAGIC 0412 a 0415 no dependen entre sí: se pueden correr en cualquier orden, o al mismo tiempo como *jobs* separados de Databricks.
# MAGIC
# MAGIC **Qué deja en `V3/MODELADO/`:** `base_modelado.parquet` (guías ya filtradas y con variables derivadas), `historia_<bloque>.parquet` (historia sin futuro de cada bloque de validación), `historia_final.parquet` (historia congelada para la prueba final) y `configuracion.json` (periodos, semilla, variables y tareas que usan los demás cuadernos). Además escribe `P4b_exclusiones.csv`.

# COMMAND ----------

# Si faltan librerías:
# !pip install duckdb lightgbm xgboost scikit-learn joblib

# COMMAND ----------

# MAGIC %pip install "duckdb<2" lightgbm xgboost shap
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuración
# MAGIC
# MAGIC Todo lo que comparten los cuadernos de modelado se fija aquí (periodos, semilla, población). Para cambiarlo, se cambia aquí y se vuelve a correr 0411 y los que siguen.

# COMMAND ----------

import os, json, datetime, warnings
import numpy as np, pandas as pd, duckdb
warnings.filterwarnings("ignore", category=UserWarning)

# MODULOS/ debe estar junto al cuaderno, como en 0100 (MODULOS/conexion_sql.py)
from MODULOS.modelado_temporal import (cargar_festivos, tabla_exclusiones, agregar_historia2, pliegues_origen_movil,
                                       guardar_preparacion)

# COMMAND ----------

# Rutas
PATH_DATABRICKS = "/Volumes/proyectogrado/source/source/"
CARPETA_SALIDAS  = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3/"
CARPETA_MODELADO = f"{CARPETA_SALIDAS}MODELADO/"                       # archivos que comparten 0411 a 0416
RUTA_GOLD_V3       = f"{CARPETA_SALIDAS}GOLD_V3_ENVIO.parquet"         # 0330
RUTA_GOLD_V3_CLIMA = f"{CARPETA_SALIDAS}GOLD_V3_ENVIO_CLIMA.parquet"   # 0160; si no existe, no se evalúa H2
RUTA_GOLD_V3_3     = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3_3/GOLD_V3_ENVIO.parquet"  # 0330 v3.3: promesa calendario
RUTA_CALENDARIO    = f"{PATH_DATABRICKS}PARAMETRICAS/DIM.VW_TIEMPO_CALENDARIO_Y_FESTIVOS.parquet"
COL_FECHA_CAL, COL_FESTIVO = None, None     # None = detección automática

# COMMAND ----------

# Periodos
INICIOS_VALIDACION = ["2026-04-01", "2026-05-01", "2026-06-01"]   # un bloque de validación por mes
FIN_DESARROLLO     = "2026-07-01"                                 # desde aquí: prueba final
FIN_PRUEBA         = "2026-09-01"
SEMILLA            = 42

# COMMAND ----------

# Población y variables
USAR_VARIABLES_CLIENTE   = True    # historia sin futuro por ID_CLIENTE (ya seudonimizado en la base v3)
SOLO_SLA_UNICO           = False   # True: sensibilidad con solo SLA_UNICO
EXCLUIR_ENTREGA_SIN_HORA = True    # FECHA_ENTREGA a las 00:00:00: el tiempo calculado no es confiable
REAJUSTAR_CON_CLIMA      = False   # True: tercera versión de H2, con hiperparámetros buscados aparte
USAR_PROMESA_COMO_TARGET = False   # v3.3: True -> el target clf pasa de incumplimiento_sla a incumplimiento_promesa (regla calendario de 0330 v3.3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Datos
# MAGIC
# MAGIC ### 1.1 Carga
# MAGIC
# MAGIC Lee la base de 0330 (o la de 0160 con clima, si existe) y el calendario de festivos. `ID_CLIENTE` ya viene seudonimizado; si está casi vacío, se desactivan las variables de cliente.

# COMMAND ----------

festivos = cargar_festivos(RUTA_CALENDARIO, COL_FECHA_CAL, COL_FESTIVO)
usa_clima = os.path.exists(RUTA_GOLD_V3_CLIMA)
ruta = RUTA_GOLD_V3_CLIMA if usa_clima else RUTA_GOLD_V3
# v3.3: columnas de la promesa calendario (0330_BaseAnaliticaV3_3) unidas por la guía seudonimizada.
# Ambas bases usan la MISMA sal (SEGURIDAD/sal_guia.txt), así que el md5 de cada guía coincide.
# margen_promesa_en_t0_h es la única nueva válida como FEATURE (conocida en T0);
# cumplimiento/incumplimiento_promesa, dias_entrega_real y dias_vs_promesa son desenlace (target), NO features.
todo = duckdb.sql(f"""
    SELECT B.*, P.fecha_promesa_entrega, P.cumplimiento_promesa,
           1 - P.cumplimiento_promesa                        AS incumplimiento_promesa,
           P.dias_entrega_real, P.dias_vs_promesa,
           (epoch_ms(CAST(P.fecha_promesa_entrega AS DATE) + INTERVAL 1 DAY) - epoch_ms(B.t0)) / 3600000.0
                                                         AS margen_promesa_en_t0_h,
           make_timestamp(LEAST(epoch_ms(B.fecha_entrega),
                                epoch_ms(CAST(P.fecha_promesa_entrega AS DATE) + INTERVAL 1 DAY)) * 1000)
                                                         AS t_etiqueta_promesa
    FROM read_parquet('{ruta}') B
    LEFT JOIN (SELECT guia, fecha_promesa_entrega, cumplimiento_promesa, dias_entrega_real, dias_vs_promesa
               FROM read_parquet('{RUTA_GOLD_V3_3}')) P USING (guia)""").df()
print(f"Base: {os.path.basename(ruta)} + promesa calendario (V3_3) · {len(todo):,} guías · con clima: {usa_clima}")
assert todo["margen_promesa_en_t0_h"].notna().sum() > 0, "El join con la base V3_3 no encontró la promesa: revisar RUTA_GOLD_V3_3"
print(f"Promesa presente en {todo['fecha_promesa_entrega'].notna().sum():,} guías ({todo['fecha_promesa_entrega'].notna().mean():.1%})")

# COMMAND ----------

m = todo.incumplimiento_promesa.notna() & todo.incumplimiento_sla.notna()
print(f"Guías con ambas etiquetas: {m.sum():,}")
print("Incumplimiento SLA (hábiles):", round(todo.loc[m, "incumplimiento_sla"].mean(), 3))
print("Incumplimiento promesa:", round(todo.loc[m, "incumplimiento_promesa"].mean(), 3))
display(pd.crosstab(todo.loc[m, "incumplimiento_sla"], todo.loc[m, "incumplimiento_promesa"], margins=True))

# COMMAND ----------

if not USAR_VARIABLES_CLIENTE:
    todo = todo.drop(columns="ID_CLIENTE")
elif todo["ID_CLIENTE"].notna().mean() < 0.5:
    print(f"AVISO: ID_CLIENTE está vacío en {todo['ID_CLIENTE'].isna().mean():.0%} de las guías. "
          "Se desactivan las variables de cliente. Para usarlas, ejecutar 0300 versión 6, que lo guarda seudonimizado.")
    USAR_VARIABLES_CLIENTE = False
    todo = todo.drop(columns="ID_CLIENTE")

# COMMAND ----------

for c in ["t0", "FECHA_ENVIO", "fecha_entrega", "t_etiqueta_reg", "t_etiqueta_clf"]:
    todo[c] = pd.to_datetime(todo[c])
todo["entrega_sin_hora"] = todo.fecha_entrega.notna() & (todo.fecha_entrega == todo.fecha_entrega.dt.normalize())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Exclusiones
# MAGIC
# MAGIC Cada filtro se aplica en orden y se cuenta cuántas guías saca y qué tasa de incumplimiento tenían. Nada se borra sin registro: la tabla queda en `P4b_exclusiones.csv`.

# COMMAND ----------

SLA_PRINCIPAL = ["SLA_UNICO"] if SOLO_SLA_UNICO else ["SLA_UNICO", "SLA_POR_REGLA"]
pasos = [("Base v3",                         pd.Series(True, index=todo.index)),
         (f"SLA en {' o '.join(SLA_PRINCIPAL)}", todo.origen_sla.isin(SLA_PRINCIPAL)),
         ("Sin devolución ni siniestro",     ~todo.estado_desenlace.isin(["DEVUELTO", "SINIESTRADO"])),
         ("No vencido antes de T0",          ~todo.vencido_antes_de_t0.fillna(False).astype(bool))]
if EXCLUIR_ENTREGA_SIN_HORA:
    pasos.append(("Entrega con hora (no 00:00:00)", ~todo.entrega_sin_hora))
if usa_clima:
    pasos.append(("Clima completo (misma cohorte para H1 y H2)", todo.CLIMA_COMPLETO.fillna(False).astype(bool)))

# COMMAND ----------

exclusiones, mask = tabla_exclusiones(todo, pasos)
display(exclusiones)
exclusiones.to_csv(f"{CARPETA_SALIDAS}P4b_exclusiones.csv", index=False)
df = todo[mask].copy()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Última milla negativa
# MAGIC
# MAGIC Si la entrega quedó antes de la salida a zona, las horas de última milla no son confiables: el objetivo de regresión pasa a nulo (la guía sigue en clasificación).

# COMMAND ----------

neg = df.h_ultima_milla < 0
print(f"Última milla negativa con hora: {int(neg.sum())} guías -> objetivo de regresión nulo (se auditan, no se borran)")
df.loc[neg, ["h_ultima_milla", "t_etiqueta_reg"]] = np.nan
df = df.sort_values("t0").reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Variables derivadas (todas conocidas en T0)
# MAGIC
# MAGIC Franja horaria y día de la semana de la salida, combinación zona×franja (para la historia), día calendario (para el bootstrap), festivo y volumen declarado (nulo si falta una dimensión, nunca 0).

# COMMAND ----------

df["franja"] = pd.cut(df.t0.dt.hour, [-1, 5, 11, 17, 23], labels=["MADRUGADA", "MANANA", "TARDE", "NOCHE"]).astype(str)
df["dia_semana"] = df.t0.dt.dayofweek.astype(str)
df["zona_franja"] = df["categoria_zona"].astype(str) + "_" + df["franja"]
df["dia"] = df.t0.dt.date
df["es_festivo"] = df.t0.dt.normalize().isin(festivos).astype(int)
vol = df.LARGO_ENVIO * df.ANCHO_ENVIO * df.ALTO_ENVIO
df["volumen_declarado_cm3"] = vol.where(vol > 0)
if USAR_VARIABLES_CLIENTE:
    df["ID_CLIENTE"] = df["ID_CLIENTE"].astype(str)
print(f"{len(df):,} guías; T0 de {df.t0.min()} a {df.t0.max()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Variables de los modelos
# MAGIC
# MAGIC - `CATEGORICAS` y `NUMERICAS`: variables internas. Las `hist_*` se calculan en la sección 2.
# MAGIC - `VERSIONES`: conjuntos de variables que se comparan. «internas» siempre; «internas+clima» si hay clima (H2).
# MAGIC - `TAREAS`: (tarea, columna objetivo, momento en que se conoce la etiqueta).

# COMMAND ----------

CATEGORICAS = ["regional_origen", "categoria_zona", "franja", "dia_semana", "ID_TIPOGUIA", "ID_PRODUCTO"]
NUMERICAS = ["sla_h", "margen_sla_en_t0_h", "h_admision_a_t0", "piezas_declaradas", "peso_declarado_kg",
             "volumen_declarado_cm3", "es_festivo", "hist_h_zona_franja", "hist_inc_zona_franja", "n_hist"]
if USAR_VARIABLES_CLIENTE:
    NUMERICAS += ["hist_h_cliente", "hist_inc_cliente", "n_hist_cliente"]
for c in CATEGORICAS: df[c] = df[c].astype(str)
VARIABLES_CLIMA = ["PRECIP_PREV_1H", "PRECIP_MAX_PREV_1H", "LLUVIA_3H_PREV", "LLUVIA_24H_PREV", "LLUVIA_72H_PREV",
                   "TEMP_PREV_1H", "HUM_PREV_1H"]

# COMMAND ----------

VERSIONES = {"internas": NUMERICAS}
if usa_clima:
    assert set(VARIABLES_CLIMA) <= set(df.columns), "La base con clima no trae las columnas esperadas"
    VERSIONES["internas+clima"] = NUMERICAS + VARIABLES_CLIMA
    if REAJUSTAR_CON_CLIMA: VERSIONES["internas+clima_reajustada"] = NUMERICAS + VARIABLES_CLIMA
print("Versiones:", list(VERSIONES))
# v3.3: target clf conmutable. incumplimiento_promesa = 1 - cumplimiento_promesa (regla calendario, 0330 v3.3),
# con el mismo sentido que incumplimiento_sla (1 = tarde). t_etiqueta_promesa = cuándo quedó conocida la etiqueta.
TAREA_CLF = (("clf", "incumplimiento_promesa", "t_etiqueta_promesa") if USAR_PROMESA_COMO_TARGET
             else ("clf", "incumplimiento_sla", "t_etiqueta_clf"))
TAREAS = [TAREA_CLF, ("reg", "h_ultima_milla", "t_etiqueta_reg")]
print("Target de clasificación:", TAREA_CLF[1])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Historia sin futuro y pliegues temporales
# MAGIC
# MAGIC ### 2.1 Historia sin futuro
# MAGIC
# MAGIC Para cada guía se calcula la media histórica de horas y de incumplimiento de su zona×franja y de su cliente, usando solo guías cuyo desenlace se conocía antes del inicio del bloque (función del asesor `media_historica_sin_futuro`). Se calcula una vez por bloque.

# COMMAND ----------

HIST = {pd.Timestamp(ini): agregar_historia2(df, ini, USAR_VARIABLES_CLIENTE) for ini in INICIOS_VALIDACION}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Pliegues de origen móvil
# MAGIC
# MAGIC Cada bloque de validación es un mes (abril, mayo, junio). Se entrena con todo lo anterior cuya etiqueta ya se conocía al inicio del mes y se valida con ese mes. La prueba final no se toca. Aquí solo se muestran los tamaños; cada cuaderno de modelo vuelve a armar los pliegues (es instantáneo).

# COMMAND ----------

PLIEGUES = {t: list(pliegues_origen_movil(df, INICIOS_VALIDACION, FIN_DESARROLLO, t_etiqueta=te)) for t, _, te in TAREAS}
for t, y_col, _ in TAREAS:
    for ini, tr, va in PLIEGUES[t]:
        print(f"{t} · bloque {ini.date()}: entrenamiento {df[y_col].iloc[tr].notna().sum():,}, validación {df[y_col].iloc[va].notna().sum():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Historia congelada para la prueba final
# MAGIC
# MAGIC La misma historia, pero con todo lo conocido antes de `FIN_DESARROLLO`. Solo la usa la prueba final de 0416.

# COMMAND ----------

HIST_FINAL = agregar_historia2(df, FIN_DESARROLLO, USAR_VARIABLES_CLIENTE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Guardar para los cuadernos de modelos

# COMMAND ----------

CONFIGURACION = {
    "INICIOS_VALIDACION": INICIOS_VALIDACION, "FIN_DESARROLLO": FIN_DESARROLLO, "FIN_PRUEBA": FIN_PRUEBA, "SEMILLA": SEMILLA,
    "USAR_VARIABLES_CLIENTE": USAR_VARIABLES_CLIENTE, "SOLO_SLA_UNICO": SOLO_SLA_UNICO,
    "EXCLUIR_ENTREGA_SIN_HORA": EXCLUIR_ENTREGA_SIN_HORA, "REAJUSTAR_CON_CLIMA": REAJUSTAR_CON_CLIMA,
    "USAR_PROMESA_COMO_TARGET": USAR_PROMESA_COMO_TARGET,
    "CATEGORICAS": CATEGORICAS, "VERSIONES": VERSIONES, "TAREAS": TAREAS,
    "usa_clima": usa_clima, "ruta_gold": ruta, "RUTA_CALENDARIO": RUTA_CALENDARIO,
    "exclusiones": exclusiones.to_dict("records"),
    "fecha_preparacion": datetime.datetime.now().isoformat()}
guardar_preparacion(CARPETA_MODELADO, df, HIST, HIST_FINAL, CONFIGURACION)
print("Preparación guardada en", CARPETA_MODELADO)
print(json.dumps({k: v for k, v in CONFIGURACION.items() if k not in ("VERSIONES", "exclusiones")}, indent=1, default=str))