# Databricks notebook source
# MAGIC %md
# MAGIC # 0413 · RandomForest (ensamble)
# MAGIC
# MAGIC **Qué hace.** Entrena y valida solo RandomForest, en las dos tareas:
# MAGIC
# MAGIC | Tarea | Qué predice | Métrica para elegir |
# MAGIC | --- | --- | --- |
# MAGIC | `clf` | Probabilidad de incumplir el SLA | AP (mayor es mejor) |
# MAGIC | `reg` | Horas de última milla | MAE en horas (menor es mejor) |
# MAGIC
# MAGIC **Random Forest** promedia muchos árboles de decisión entrenados con muestras distintas de guías y de variables. Es el ensamble más simple de los tres y el más lento de predecir con muchas filas.
# MAGIC
# MAGIC **Pasos.** 1) carga lo que dejó 0411; 2) búsqueda aleatoria de hiperparámetros con validación de origen móvil; 3) validación con la mejor configuración en los tres bloques (abril, mayo, junio); 4) vista rápida de sus métricas; 5) guarda predicciones, importancias y parámetros para 0416.
# MAGIC
# MAGIC **No depende de los otros cuadernos de modelos.** Solo necesita que 0411 se haya corrido. La prueba final no se toca aquí.
# MAGIC
# MAGIC **Tiempo.** La búsqueda hace `N_CONFIGS_BUSQUEDA × 3 bloques × 2 tareas` ajustes sobre una submuestra de `MAX_FILAS_BUSQUEDA` filas. Para una primera corrida rápida, bajar `N_CONFIGS_BUSQUEDA` a 3.

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

import os, json, time, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore", category=UserWarning)

# MODULOS/ debe estar junto al cuaderno, como en 0100 (MODULOS/conexion_sql.py)
from MODULOS.modelado_temporal import (cargar_preparacion, pliegues_origen_movil, partes_del_bloque, marco_predicciones,
                                       construir_modelo, predecir, muestrear_configs, puntaje_config, mejores, params_de,
                                       importancia_pliegue, tabla_metricas)

# COMMAND ----------

MODELO = "RandomForest"
PATH_DATABRICKS = "/Volumes/proyectogrado/source/source/"
CARPETA_SALIDAS  = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3/"
CARPETA_MODELADO = f"{CARPETA_SALIDAS}MODELADO/"

N_CONFIGS_BUSQUEDA = 20        # configuraciones aleatorias por tarea
MAX_FILAS_BUSQUEDA = 300_000   # submuestra de entrenamiento en la búsqueda (no en la validación)
EJECUTAR_BUSQUEDA  = True      # False: usa los parámetros que ya guardó este cuaderno
N_PERMUTACION      = 10_000    # filas de validación para la importancia por permutación en cada bloque

# COMMAND ----------

# Hiperparámetros de base (si no hay búsqueda) y espacio de búsqueda registrado (realimentación P4)
PARAMS_BASE = dict(n_estimators=200, min_samples_leaf=20, max_features="sqrt", max_samples=0.2)
ESPACIO = dict(n_estimators=[100, 200, 300], max_depth=[None, 12, 20], min_samples_leaf=[1, 5, 20, 50],
               max_features=["sqrt", 0.3, 0.5], max_samples=[0.1, 0.2, 0.3])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar la preparación de 0411
# MAGIC
# MAGIC Base de modelado, historia sin futuro de cada bloque y configuración común (periodos, semilla, variables). Los pliegues se vuelven a armar con la misma función del asesor.

# COMMAND ----------

CFG, df, HIST = cargar_preparacion(CARPETA_MODELADO)
SEMILLA, CATEGORICAS, VERSIONES, TAREAS = CFG["SEMILLA"], CFG["CATEGORICAS"], CFG["VERSIONES"], CFG["TAREAS"]
PLIEGUES = {t: list(pliegues_origen_movil(df, CFG["INICIOS_VALIDACION"], CFG["FIN_DESARROLLO"], t_etiqueta=te))
            for t, _, te in TAREAS}
print(f"{len(df):,} guías · versiones: {list(VERSIONES)} · preparado el {CFG['fecha_preparacion']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Búsqueda aleatoria de hiperparámetros (solo en validación)
# MAGIC
# MAGIC Se prueban `N_CONFIGS_BUSQUEDA` configuraciones de `ESPACIO`. Cada una se entrena en cada bloque con una submuestra de hasta `MAX_FILAS_BUSQUEDA` filas y se evalúa en el bloque completo. Se elige la de mejor promedio entre bloques. Si la base trae la versión «internas+clima_reajustada», se busca también para ella.

# COMMAND ----------

def buscar(version, numericas):
    """Prueba todas las configuraciones de una versión de variables; una fila por configuración."""
    filas = []
    for tarea, y_col, _ in TAREAS:
        for k, cfg in enumerate(muestrear_configs(ESPACIO, N_CONFIGS_BUSQUEDA, SEMILLA + len(MODELO))):
            if tarea == "clf":
                cfg = {a: b for a, b in cfg.items() if a != "objetivo_reg"}   # solo aplica a regresión
            vals = puntaje_config(tarea, MODELO, cfg, k, PLIEGUES[tarea], HIST, df, y_col,
                                  CATEGORICAS, numericas, MAX_FILAS_BUSQUEDA, SEMILLA)
            filas.append({"version": version, "tarea": tarea, "modelo": MODELO, "config": k,
                          "metrica": "AP" if tarea == "clf" else "MAE_h", "promedio": np.mean(vals),
                          "por_bloque": [round(v, 4) for v in vals], **{f"p_{a}": b for a, b in cfg.items()}})
            print(f"{version} {tarea} config {k}: {np.mean(vals):.4f}")
    return pd.DataFrame(filas)

# COMMAND ----------

RUTA_PARAMS = f"{CARPETA_MODELADO}parametros_{MODELO}.json"
t_ini = time.time()
if EJECUTAR_BUSQUEDA:
    versiones_busqueda = ["internas"] + [v for v in VERSIONES if v == "internas+clima_reajustada"]
    tabla_busqueda = pd.concat([buscar(v, VERSIONES[v]) for v in versiones_busqueda], ignore_index=True)
    tabla_busqueda.to_pickle(f"{CARPETA_MODELADO}busqueda_{MODELO}.pkl")       # exacta, para 0416
    tabla_busqueda.to_csv(f"{CARPETA_MODELADO}busqueda_{MODELO}.csv", index=False)
    MEJORES = mejores(tabla_busqueda, {MODELO: ESPACIO})
else:
    MEJORES = json.load(open(RUTA_PARAMS))["mejores"]
print(f"Búsqueda: {time.time() - t_ini:.0f} s")
display(pd.DataFrame(MEJORES).T)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validación de origen móvil con la mejor configuración
# MAGIC
# MAGIC En cada versión de variables, tarea y bloque: se entrena con todo lo anterior al bloque, se predice el bloque y se guardan las predicciones. Con las variables internas se mide además la importancia por permutación (insumo de 0420).

# COMMAND ----------

pred, imp = [], []
t_ini = time.time()
for version, numericas in VERSIONES.items():
    for tarea, y_col, _ in TAREAS:
        params = params_de(MEJORES, {MODELO: PARAMS_BASE}, version, tarea, MODELO)
        for ini, tr, va in PLIEGUES[tarea]:
            Dva, Xtr, ytr, Xva, yva = partes_del_bloque(df, HIST[ini], y_col, tr, va, CATEGORICAS + numericas)
            m = construir_modelo(tarea, MODELO, numericas, CATEGORICAS, params, SEMILLA).fit(Xtr, ytr)
            pred.append(marco_predicciones(version, tarea, ini, Dva, yva).assign(modelo=MODELO, pred=predecir(m, Xva, tarea)))
            if version == "internas":
                etiquetas = {"tarea": tarea, "modelo": MODELO, "bloque": ini.date()}
                imp.append(importancia_pliegue(m, Xva, yva, tarea, N_PERMUTACION, SEMILLA, etiquetas))
            print(f"{version} · {tarea} · {ini.date()} listo ({time.time() - t_ini:.0f} s)")
pred, imp = pd.concat(pred, ignore_index=True), pd.concat(imp, ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Vista rápida de RandomForest
# MAGIC
# MAGIC Solo para revisar este modelo. La comparación con los demás se hace en 0416.

# COMMAND ----------

display(tabla_metricas(pred, ["version", "tarea", "modelo"]).round(4))
display(tabla_metricas(pred, ["version", "tarea", "modelo", "bloque"]).round(4))

# COMMAND ----------

# Variables con más peso en validación (promedio de los tres bloques)
display(imp.groupby(["tarea", "variable"]).importancia.mean().sort_values(ascending=False).groupby(level=0).head(8).round(4))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Guardar para 0416

# COMMAND ----------

pred.to_parquet(f"{CARPETA_MODELADO}pred_val_{MODELO}.parquet", index=False)
imp.to_parquet(f"{CARPETA_MODELADO}importancia_{MODELO}.parquet", index=False)
with open(RUTA_PARAMS, "w") as f:
    json.dump({"modelo": MODELO, "espacio": ESPACIO, "params_base": PARAMS_BASE, "semilla": SEMILLA,
               "n_configs": N_CONFIGS_BUSQUEDA, "max_filas": MAX_FILAS_BUSQUEDA, "mejores": MEJORES}, f, indent=2, default=str)
print("Guardado en", CARPETA_MODELADO)