# Databricks notebook source
# MAGIC %md
# MAGIC # 0420 · Interpretabilidad y hallazgos operativos · versión 4 (OE5)
# MAGIC
# MAGIC **Qué hace.** Interpreta los modelos finales que fijó `0416_Comparacion_PruebaFinal`, sin volver a ajustar nada.
# MAGIC
# MAGIC | Sección | Pregunta que responde | Salida |
# MAGIC | --- | --- | --- |
# MAGIC | 1. Estabilidad | ¿Pesan las mismas variables en abril, mayo y junio? | `01_*` |
# MAGIC | 2. Permutación en prueba | ¿Cuánto empeora el modelo final si se desordena cada variable? | `02_*` |
# MAGIC | 3. SHAP | ¿Qué variables mueven cada predicción y en qué dirección? | `03_*` |
# MAGIC | 4. Dependencia y categorías | ¿Cómo cambia el efecto con el valor de la variable o la categoría? | `04_*` |
# MAGIC | 5. Traducción operativa | Alertas por día, dónde se concentran, por qué se alerta y con qué margen de error se comunica el tiempo | `05_*` a `07_*` |
# MAGIC
# MAGIC **Lectura.** Las importancias y los SHAP describen **cómo usa el modelo** las variables; no son efectos causales (realimentación OE5). Sobre la prueba se interpreta **solo el modelo final ya fijado**.
# MAGIC
# MAGIC **Requisitos.** Haber ejecutado la prueba final de `0416_Comparacion_PruebaFinal` (carpeta `MODELOS/` y `P4b_decision.json`).
# MAGIC
# MAGIC **Salidas.** CSV y PNG en `REVISION_2026_10/V3/INTERPRETACION/`. Solo agregados; ninguna guía individual.

# COMMAND ----------

# MAGIC %md
# MAGIC > **Versión 4 (23-10-2026).** Igual a la versión 3; solo cambia el cuaderno del que lee: ahora el modelado está separado en 0411 a 0416 y los modelos finales los deja `0416_Comparacion_PruebaFinal`. El análisis y los archivos de salida son los mismos que en la versión 2.

# COMMAND ----------

# Si faltan librerías:
# %pip install shap lightgbm xgboost scikit-learn joblib

# COMMAND ----------

# MAGIC %pip install "duckdb<2" lightgbm xgboost shap
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuración

# COMMAND ----------

import os, json, numpy as np, pandas as pd, joblib, matplotlib.pyplot as plt, shap
from sklearn.inspection import permutation_importance

# MODULOS/ debe estar junto al cuaderno, como en 0100 (MODULOS/conexion_sql.py)
from MODULOS.interpretabilidad import (shap_modelo, agregar_por_variable, estabilidad_importancias, resumen_shap_tarea,
                                       efectos_por_categoria, alertas_por_segmento, barras_horizontales)

# COMMAND ----------

PATH_DATABRICKS = "/Volumes/proyectogrado/source/source/"
CARPETA_SALIDAS = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3/"
CARPETA_MODELOS = f"{CARPETA_SALIDAS}MODELOS/"
CARPETA_INT     = f"{CARPETA_SALIDAS}INTERPRETACION/"
os.makedirs(CARPETA_INT, exist_ok=True)
SEMILLA = 42
N_SHAP = 2000            # filas de prueba para SHAP (Random Forest es lento: bajar a 1000 si tarda)
N_PERMUTACION = 20_000   # filas de prueba para la importancia por permutación
TOP = 15                 # variables que se muestran en los gráficos

# COMMAND ----------

# Decisión registrada en 0416: modelos, umbral y variables
DEC = json.load(open(f"{CARPETA_SALIDAS}P4b_decision.json"))
CAT, NUM = DEC["variables_categoricas"], DEC["variables_numericas"]
print(json.dumps({k: DEC[k] for k in ["modelo_clf", "modelo_reg", "calibracion", "umbral", "alertas_por_dia_escenario"]}, indent=1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Estabilidad de la importancia entre bloques de validación
# MAGIC
# MAGIC Usa la importancia por permutación que midieron 0413 a 0415 en cada bloque. Para cada modelo y tarea compara el orden de las variables entre bloques con la correlación de Spearman. Cercano a 1: el orden se mantiene; por debajo de 0,5: interpretar con cautela.

# COMMAND ----------

imp = pd.read_csv(f"{CARPETA_SALIDAS}P4b_importancia_pliegues.csv")
filas = []
for (tarea, modelo), g in imp.groupby(["tarea", "modelo"]):
    piv, resumen = estabilidad_importancias(g)
    filas.append({"tarea": tarea, "modelo": modelo, **resumen})
    piv.assign(media=piv.mean(axis=1)).sort_values("media", ascending=False).round(5) \
       .to_csv(f"{CARPETA_INT}01_permutacion_validacion_{tarea}_{modelo}.csv")

# COMMAND ----------

estabilidad = pd.DataFrame(filas); display(estabilidad)
estabilidad.to_csv(f"{CARPETA_INT}01_estabilidad_importancias.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Importancia por permutación del modelo final (prueba)
# MAGIC
# MAGIC Se cargan el modelo final y los datos de prueba de cada tarea. Luego se desordena cada variable en una muestra de la prueba y se mide cuánto cae la AP (clasificación) o sube el MAE (regresión).

# COMMAND ----------

modelos, datos = {}, {}
for tarea in ["clf", "reg"]:
    modelos[tarea] = joblib.load(f"{CARPETA_MODELOS}modelo_final_{tarea}.joblib")
    datos[tarea] = pd.read_parquet(f"{CARPETA_MODELOS}prueba_{tarea}.parquet")
    print(f"{tarea}: {DEC['modelo_' + tarea]} · {len(datos[tarea]):,} guías de prueba")

# COMMAND ----------

perm = []
for tarea, scoring in [("clf", "average_precision"), ("reg", "neg_mean_absolute_error")]:
    d = datos[tarea].sample(min(N_PERMUTACION, len(datos[tarea])), random_state=SEMILLA)
    r = permutation_importance(modelos[tarea], d[CAT + NUM], d["y"], n_repeats=5, random_state=SEMILLA, scoring=scoring, n_jobs=1)
    t = pd.DataFrame({"tarea": tarea, "variable": CAT + NUM, "importancia": r.importances_mean, "desv": r.importances_std}) \
          .sort_values("importancia", ascending=False)
    perm.append(t)
perm = pd.concat(perm); perm.to_csv(f"{CARPETA_INT}02_permutacion_prueba.csv", index=False)
display(perm.groupby("tarea").head(10).round(4))

# COMMAND ----------

for tarea in ["clf", "reg"]:
    tt = perm[perm.tarea == tarea].head(TOP)[::-1]
    medida = "pérdida de AP" if tarea == "clf" else "aumento del MAE (h)"
    barras_horizontales(tt.variable, tt.importancia, f"Permutación en prueba · {DEC['modelo_' + tarea]} · {medida}",
                        f"{CARPETA_INT}02_permutacion_prueba_{tarea}.png", error=tt.desv)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Valores SHAP
# MAGIC
# MAGIC SHAP reparte cada predicción entre las variables. Para una variable categórica se suman sus columnas *one-hot*. En clasificación, los valores están en la escala del modelo (log-odds en los ensambles de *boosting* y en la logística; probabilidad en Random Forest). La **dirección** de una variable numérica es la correlación de Spearman entre su valor y su SHAP: positiva significa «a mayor valor, mayor riesgo o más horas».
# MAGIC
# MAGIC ### 3.1 Cálculo
# MAGIC
# MAGIC Una muestra de `N_SHAP` guías de prueba por tarea; el fondo (necesario para el modelo lineal) sale de la muestra de entrenamiento que guardó 0416.

# COMMAND ----------

SHAP, SHAP_COLUMNAS = {}, {}
for tarea in ["clf", "reg"]:
    d = datos[tarea].sample(min(N_SHAP, len(datos[tarea])), random_state=SEMILLA)
    fondo = pd.read_parquet(f"{CARPETA_MODELOS}muestra_entrenamiento_{tarea}.parquet").sample(500, random_state=SEMILLA)
    sv, nombres, Xt = shap_modelo(modelos[tarea], d[CAT + NUM], fondo[CAT + NUM], tarea)
    agg = agregar_por_variable(sv, nombres, CAT); agg.index = d.index
    SHAP[tarea] = (d, agg)                      # por variable original
    SHAP_COLUMNAS[tarea] = (sv, nombres, Xt)    # por columna transformada

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Resumen por variable

# COMMAND ----------

resumen_shap = pd.concat([resumen_shap_tarea(tarea, *SHAP[tarea], NUM) for tarea in ["clf", "reg"]])
resumen_shap.to_csv(f"{CARPETA_INT}03_shap_por_variable.csv", index=False)
display(resumen_shap.groupby("tarea").head(10).round(4))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Gráficos
# MAGIC
# MAGIC Barras: rojo si la variable aumenta el riesgo o las horas al crecer, azul si las disminuye, gris si es categórica. El gráfico de resumen muestra cada columna transformada.

# COMMAND ----------

for tarea in ["clf", "reg"]:
    tt = resumen_shap[resumen_shap.tarea == tarea].head(TOP)[::-1]
    colores = np.where(tt.direccion_spearman > 0, "#E45756", np.where(tt.direccion_spearman < 0, "#4C78A8", "#999"))
    barras_horizontales(tt.variable, tt.media_abs_shap,
                        f"SHAP medio absoluto · {DEC['modelo_' + tarea]} ({tarea})\nrojo: aumenta con la variable · azul: disminuye · gris: categórica",
                        f"{CARPETA_INT}03_shap_barras_{tarea}.png", color=colores)

# COMMAND ----------

for tarea in ["clf", "reg"]:
    sv, nombres, Xt = SHAP_COLUMNAS[tarea]
    plt.figure(); shap.summary_plot(sv, Xt, feature_names=nombres, max_display=TOP, show=False)
    plt.title(f"SHAP por columna transformada ({tarea})"); plt.tight_layout()
    plt.savefig(f"{CARPETA_INT}03_shap_resumen_{tarea}.png", dpi=130, bbox_inches="tight"); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dependencia y efecto por categoría
# MAGIC
# MAGIC ### 4.1 Dependencia de las tres variables numéricas con más peso
# MAGIC
# MAGIC Cada punto es una guía: valor de la variable (eje x) frente a su SHAP (eje y).

# COMMAND ----------

for tarea in ["clf", "reg"]:
    d, agg = SHAP[tarea]
    top_num = [v for v in resumen_shap[resumen_shap.tarea == tarea].variable if v in NUM][:3]
    fig, axs = plt.subplots(1, len(top_num), figsize=(5 * len(top_num), 3.6), squeeze=False)
    for ax, v in zip(axs[0], top_num):
        ax.scatter(d[v], agg[v], s=4, alpha=0.4, color="#4C78A8"); ax.axhline(0, color="k", lw=0.8)
        ax.set_xlabel(v); ax.set_ylabel("SHAP")
    fig.suptitle(f"Dependencia SHAP · {tarea}"); plt.tight_layout()
    fig.savefig(f"{CARPETA_INT}04_dependencia_{tarea}.png", dpi=130); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 SHAP medio por categoría (categorías con al menos 30 guías)

# COMMAND ----------

filas = []
for tarea in ["clf", "reg"]:
    filas += efectos_por_categoria(tarea, *SHAP[tarea], CAT)
efectos_cat = pd.DataFrame(filas).sort_values(["tarea", "variable", "shap_medio"])
efectos_cat.to_csv(f"{CARPETA_INT}04_shap_por_categoria.csv", index=False)
display(efectos_cat.round(4).head(30))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Traducción operativa
# MAGIC
# MAGIC ### 5.1 Carga de alertas con el umbral fijado en validación

# COMMAND ----------

c = datos["clf"].copy(); c["alerta"] = c.pred >= DEC["umbral"]
dias = c.dia.nunique()
alertas_dia = c.groupby("dia").alerta.sum()
carga = pd.DataFrame({"alertas_por_dia_media": [alertas_dia.mean()],
                      "alertas_por_dia_p90": [alertas_dia.quantile(0.9)],
                      "precision": [c.loc[c.alerta, "y"].mean()],
                      "exhaustividad": [c.loc[c.y == 1, "alerta"].mean()],
                      "incumplimientos_no_alertados_por_dia": [((c.y == 1) & ~c.alerta).sum() / dias],
                      "escenario_capacidad": [DEC["alertas_por_dia_escenario"]]})
display(carga.round(3)); carga.to_csv(f"{CARPETA_INT}05_carga_alertas.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Alertas por segmento
# MAGIC
# MAGIC Dónde se concentran las alertas y qué tan bien acierta el modelo en cada zona, franja, día de la semana y regional.

# COMMAND ----------

seg = pd.concat([alertas_por_segmento(c, col, dias) for col in ["categoria_zona", "franja", "dia_semana", "regional_origen"]])
display(seg.round(3)); seg.to_csv(f"{CARPETA_INT}05_alertas_por_segmento.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Motivos de las alertas
# MAGIC
# MAGIC Para cada guía alertada de la muestra SHAP, la variable que más empuja su riesgo hacia arriba.

# COMMAND ----------

d, agg = SHAP["clf"]
alertadas = d.pred >= DEC["umbral"]
if alertadas.any():
    motivo = agg[alertadas].idxmax(axis=1)
    motivos = motivo.value_counts(normalize=True).rename("proporcion_de_alertas").to_frame()
    motivos["n"] = motivo.value_counts()
    display(motivos.round(3)); motivos.to_csv(f"{CARPETA_INT}06_motivos_alertas.csv")
else:
    print("Ninguna guía de la muestra SHAP supera el umbral.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Margen de error del tiempo estimado
# MAGIC
# MAGIC Por categoría de zona y franja: horas reales, error medio, percentiles 80 y 90 del error y qué tan seguido el modelo se queda corto. Sirve para comunicar la estimación con su rango.

# COMMAND ----------

r = datos["reg"].copy(); r["error_abs"] = (r.y - r.pred).abs(); r["subestima"] = r.y > r.pred
banda = r.groupby(["categoria_zona", "franja"]).agg(guias=("y", "size"), horas_reales_mediana=("y", "median"),
          MAE_h=("error_abs", "mean"), P80_error_h=("error_abs", lambda s: s.quantile(0.8)),
          P90_error_h=("error_abs", lambda s: s.quantile(0.9)), pct_subestima=("subestima", "mean"))
banda = banda[banda.guias >= 30]
display(banda.round(2)); banda.to_csv(f"{CARPETA_INT}07_banda_error_tiempo.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo leer y qué entregar
# MAGIC
# MAGIC - **Estabilidad (01):** si una variable está en el top 5 en todos los bloques, su papel es robusto; si el Spearman entre bloques es bajo, la interpretación debe ser prudente.
# MAGIC - **Permutación (02) y SHAP (03–04):** las variables que coinciden en los dos métodos y en la estabilidad son las que se reportan como «factores de mayor peso predictivo». Comparar con el ordenamiento descriptivo de `0210` (sección 7).
# MAGIC - **Operación (05–07):** cuántas alertas por día generaría el modelo con el umbral fijado, dónde se concentran, cuántos incumplimientos se escapan, por qué se alerta y con qué margen de error se comunica el tiempo estimado.
# MAGIC - **No causal:** «el modelo asigna más riesgo cuando el margen de SLA en T0 es menor» es correcto; «reducir X causaría menos incumplimientos» no se puede afirmar con este diseño.
# MAGIC
# MAGIC **Entregar:** la carpeta `INTERPRETACION/` completa.