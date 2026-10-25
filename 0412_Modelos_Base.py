# Databricks notebook source
# MAGIC %md
# MAGIC # 0412 · Modelos base: referencia y lineal / logística
# MAGIC
# MAGIC **Qué hace.** Valida los dos puntos de partida contra los que se comparan los ensambles (H1):
# MAGIC
# MAGIC | Modelo | Clasificación (`clf`) | Regresión (`reg`) |
# MAGIC | --- | --- | --- |
# MAGIC | **Referencia** (sin modelo) | Prevalencia de incumplimiento del entrenamiento, igual para todas las guías | Media histórica de horas de su zona×franja (o la media del entrenamiento si no hay historia) |
# MAGIC | **Lineal** | Regresión logística | Regresión lineal |
# MAGIC
# MAGIC Los dos usan las mismas variables y los mismos bloques de validación que los ensambles. No tienen hiperparámetros que buscar, así que este cuaderno es rápido.
# MAGIC
# MAGIC **No depende de los otros cuadernos de modelos.** Solo necesita que 0411 se haya corrido. La prueba final no se toca aquí.

# COMMAND ----------

# Si faltan librerías:
# !pip install duckdb lightgbm xgboost scikit-learn joblib

# COMMAND ----------

# MAGIC %pip install "duckdb<2" lightgbm xgboost shap
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuración

# COMMAND ----------

import os, time, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore", category=UserWarning)

# MODULOS/ debe estar junto al cuaderno, como en 0100 (MODULOS/conexion_sql.py)
from MODULOS.modelado_temporal import (cargar_preparacion, pliegues_origen_movil, partes_del_bloque, marco_predicciones,
                                       construir_modelo, predecir, tabla_metricas)

# COMMAND ----------

PATH_DATABRICKS = "/Volumes/proyectogrado/source/source/"
CARPETA_SALIDAS  = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3/"
CARPETA_MODELADO = f"{CARPETA_SALIDAS}MODELADO/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar la preparación de 0411

# COMMAND ----------

CFG, df, HIST = cargar_preparacion(CARPETA_MODELADO)
SEMILLA, CATEGORICAS, VERSIONES, TAREAS = CFG["SEMILLA"], CFG["CATEGORICAS"], CFG["VERSIONES"], CFG["TAREAS"]
PLIEGUES = {t: list(pliegues_origen_movil(df, CFG["INICIOS_VALIDACION"], CFG["FIN_DESARROLLO"], t_etiqueta=te))
            for t, _, te in TAREAS}
print(f"{len(df):,} guías · versiones: {list(VERSIONES)} · preparado el {CFG['fecha_preparacion']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validación de origen móvil
# MAGIC
# MAGIC En cada versión de variables, tarea y bloque: se arma el entrenamiento (antes del bloque) y la validación (el bloque), se calcula la referencia y se entrena el modelo lineal.

# COMMAND ----------

pred_ref, pred_lin = [], []
t_ini = time.time()
for version, numericas in VERSIONES.items():
    for tarea, y_col, _ in TAREAS:
        for ini, tr, va in PLIEGUES[tarea]:
            Dva, Xtr, ytr, Xva, yva = partes_del_bloque(df, HIST[ini], y_col, tr, va, CATEGORICAS + numericas)
            base = marco_predicciones(version, tarea, ini, Dva, yva)
            # Referencia: sin modelo, estimada solo con el entrenamiento
            ref = np.full(len(Dva), ytr.mean()) if tarea == "clf" else Dva["hist_h_zona_franja"].fillna(ytr.mean()).values
            pred_ref.append(base.assign(modelo="Referencia", pred=ref))
            # Lineal (regresión lineal o logística según la tarea)
            m = construir_modelo(tarea, "Lineal", numericas, CATEGORICAS, {}, SEMILLA).fit(Xtr, ytr)
            pred_lin.append(base.assign(modelo="Lineal", pred=predecir(m, Xva, tarea)))
            print(f"{version} · {tarea} · {ini.date()} listo ({time.time() - t_ini:.0f} s)")
pred_ref, pred_lin = pd.concat(pred_ref, ignore_index=True), pd.concat(pred_lin, ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Vista rápida
# MAGIC
# MAGIC Solo para revisar los modelos base. La comparación con los ensambles se hace en 0416.

# COMMAND ----------

display(tabla_metricas(pd.concat([pred_ref, pred_lin]), ["version", "tarea", "modelo"]).round(4))

# COMMAND ----------

display(tabla_metricas(pd.concat([pred_ref, pred_lin]), ["version", "tarea", "modelo", "bloque"]).round(4))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Guardar para 0416

# COMMAND ----------

pred_ref.to_parquet(f"{CARPETA_MODELADO}pred_val_Referencia.parquet", index=False)
pred_lin.to_parquet(f"{CARPETA_MODELADO}pred_val_Lineal.parquet", index=False)
print("Guardado en", CARPETA_MODELADO)