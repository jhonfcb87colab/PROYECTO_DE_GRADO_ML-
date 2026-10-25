# Databricks notebook source
# MAGIC %md
# MAGIC # 0416 · Comparación de modelos, decisión y prueba final (OE3, OE4, H1, H2)
# MAGIC
# MAGIC **Qué hace.** Junta lo que dejaron los cuadernos de cada modelo y, sin entrenar nada nuevo en validación, compara:
# MAGIC
# MAGIC | Sección | Qué pasa | Se toca algo |
# MAGIC | --- | --- | --- |
# MAGIC | 1. Juntar resultados | Predicciones de validación de los cinco modelos, importancias y búsqueda | No |
# MAGIC | 2. Resultados de validación | Tablas general, por bloque y por segmento | No |
# MAGIC | 3. H1 y H2 | Ensambles frente al lineal y clima frente a sin clima, con IC 95 % por días | No |
# MAGIC | 4. Modelo propuesto y calibración | Mejor modelo y si conviene calibrar | No |
# MAGIC | 5. Umbral | Umbral por capacidad de atención | No |
# MAGIC | 6. Decisión | Se registra antes de la prueba final | `DECISION_APROBADA = True` |
# MAGIC | 7. Prueba final | Julio–agosto, una sola vez | `EJECUTAR_PRUEBA_FINAL = True` |
# MAGIC | 8. Manifiesto | Versiones, parámetros y salidas | No |
# MAGIC
# MAGIC **Requisitos.** Haber corrido 0411 (preparación) y los cuatro cuadernos de modelos: 0412 (referencia y lineal), 0413 (Random Forest), 0414 (XGBoost) y 0415 (LightGBM). Si falta alguno, la sección 1 dice cuál.
# MAGIC
# MAGIC **Regla central.** Todo se decide con la validación de abril, mayo y junio. La prueba final (julio–agosto) se ejecuta **una sola vez**, al final, y después no se ajusta nada.
# MAGIC
# MAGIC **Salidas.** Las mismas `P4b_*` de siempre en `V3/`, y los modelos y datos de prueba en `V3/MODELOS/` para 0420.

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

import os, json, glob, sys, time, datetime, warnings
import numpy as np, pandas as pd, duckdb, joblib
import lightgbm as lgb, xgboost as xgb, sklearn
import matplotlib.pyplot as plt
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import (mean_absolute_error, average_precision_score, precision_score, recall_score, f1_score)
warnings.filterwarnings("ignore", category=UserWarning)

# MODULOS/ debe estar junto al cuaderno, como en 0100 (MODULOS/conexion_sql.py)
from MODULOS.modelado_temporal import (
    cargar_preparacion, leer_historia, ordenar, con_historia, construir_modelo, predecir, mejores, params_de,
    metricas, tabla_metricas, metricas_por_segmento, diferencias, diferencia_por_bloques, referencias_probabilidad,
    calibracion_cruzada, calibracion_tabla, tabla_umbrales, umbral_por_capacidad, huella)

# COMMAND ----------

PATH_DATABRICKS = "/Volumes/proyectogrado/source/source/"
CARPETA_SALIDAS  = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3/"
CARPETA_MODELADO = f"{CARPETA_SALIDAS}MODELADO/"   # lo que dejaron 0411 a 0415
CARPETA_MODELOS  = f"{CARPETA_SALIDAS}MODELOS/"    # modelos finales y datos de prueba para 0420
os.makedirs(CARPETA_MODELOS, exist_ok=True)

ALERTAS_POR_DIA = 500     # ESCENARIO de capacidad de atención, no un dato oficial: definirlo con la empresa
B_BOOTSTRAP     = 1000    # remuestreos por días para los IC
MODELOS   = ["Lineal", "RandomForest", "XGBoost", "LightGBM"]   # "Lineal" = regresión lineal o logística según la tarea
ENSAMBLES = ["RandomForest", "XGBoost", "LightGBM"]

# COMMAND ----------

# Configuración común que fijó 0411
CFG, df, _ = cargar_preparacion(CARPETA_MODELADO)
SEMILLA, CATEGORICAS, VERSIONES, TAREAS = CFG["SEMILLA"], CFG["CATEGORICAS"], CFG["VERSIONES"], CFG["TAREAS"]
INICIOS_VALIDACION, FIN_DESARROLLO, FIN_PRUEBA = CFG["INICIOS_VALIDACION"], CFG["FIN_DESARROLLO"], CFG["FIN_PRUEBA"]
USAR_VARIABLES_CLIENTE, SOLO_SLA_UNICO = CFG["USAR_VARIABLES_CLIENTE"], CFG["SOLO_SLA_UNICO"]
EXCLUIR_ENTREGA_SIN_HORA = CFG["EXCLUIR_ENTREGA_SIN_HORA"]
ruta, RUTA_CALENDARIO = CFG["ruta_gold"], CFG["RUTA_CALENDARIO"]
exclusiones = pd.DataFrame(CFG["exclusiones"])   # misma tabla que P4b_exclusiones.csv
print(f"{len(df):,} guías · versiones: {list(VERSIONES)} · preparado el {CFG['fecha_preparacion']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Juntar los resultados de cada modelo
# MAGIC
# MAGIC ### 1.1 ¿Están todos?

# COMMAND ----------

esperados = {"0412": ["pred_val_Referencia.parquet", "pred_val_Lineal.parquet"],
             "0413": ["pred_val_RandomForest.parquet", "importancia_RandomForest.parquet", "parametros_RandomForest.json"],
             "0414": ["pred_val_XGBoost.parquet", "importancia_XGBoost.parquet", "parametros_XGBoost.json"],
             "0415": ["pred_val_LightGBM.parquet", "importancia_LightGBM.parquet", "parametros_LightGBM.json"]}
faltan = {c: [f for f in fs if not os.path.exists(CARPETA_MODELADO + f)] for c, fs in esperados.items()}
faltan = {c: fs for c, fs in faltan.items() if fs}
assert not faltan, f"Faltan resultados; correr primero estos cuadernos: {faltan}"
print("Están los resultados de los cinco modelos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Predicciones de validación
# MAGIC
# MAGIC Una tabla con las predicciones de todos los modelos, en el mismo orden que tendría si todo se hubiera corrido en un solo cuaderno.

# COMMAND ----------

pred = pd.concat([pd.read_parquet(f"{CARPETA_MODELADO}pred_val_{m}.parquet") for m in ["Referencia"] + MODELOS], ignore_index=True)
pred = ordenar(pred, {"version": list(VERSIONES), "tarea": ["clf", "reg"], "bloque": None, "modelo": ["Referencia"] + MODELOS})
pred.to_parquet(f"{CARPETA_SALIDAS}P4b_predicciones_validacion.parquet", index=False)
print(pred.groupby(["version", "tarea", "modelo"]).size().unstack())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Importancia por permutación de cada bloque (insumo de 0420)

# COMMAND ----------

importancias_pliegues = pd.concat([pd.read_parquet(f"{CARPETA_MODELADO}importancia_{m}.parquet") for m in ENSAMBLES], ignore_index=True)
importancias_pliegues = ordenar(importancias_pliegues, {"tarea": ["clf", "reg"], "bloque": None, "modelo": ENSAMBLES})
importancias_pliegues.to_csv(f"{CARPETA_SALIDAS}P4b_importancia_pliegues.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Hiperparámetros elegidos en cada ensamble

# COMMAND ----------

PARAMETROS = {m: json.load(open(f"{CARPETA_MODELADO}parametros_{m}.json")) for m in ENSAMBLES}
PARAMS_BASE = {**{m: PARAMETROS[m]["params_base"] for m in ENSAMBLES}, "Lineal": {}}
ESPACIOS = {m: PARAMETROS[m]["espacio"] for m in ["LightGBM", "XGBoost", "RandomForest"]}
n_configs = {m: PARAMETROS[m]["n_configs"] for m in ENSAMBLES}   # un solo número si todos usaron el mismo
max_filas = {m: PARAMETROS[m]["max_filas"] for m in ENSAMBLES}
N_CONFIGS_BUSQUEDA = n_configs["LightGBM"] if len(set(n_configs.values())) == 1 else n_configs
MAX_FILAS_BUSQUEDA = max_filas["LightGBM"] if len(set(max_filas.values())) == 1 else max_filas

# COMMAND ----------

busquedas = [f"{CARPETA_MODELADO}busqueda_{m}.pkl" for m in ENSAMBLES]
if all(os.path.exists(f) for f in busquedas):
    tabla_busqueda = ordenar(pd.concat([pd.read_pickle(f) for f in busquedas], ignore_index=True),
                             {"version": list(VERSIONES), "tarea": ["clf", "reg"], "modelo": ENSAMBLES})
    tabla_busqueda = tabla_busqueda[[c for c in tabla_busqueda if c != "p_objetivo_reg"] + ["p_objetivo_reg"]]
    tabla_busqueda.to_csv(f"{CARPETA_SALIDAS}P4b_busqueda.csv", index=False)
    MEJORES = mejores(tabla_busqueda, ESPACIOS)
else:   # algún cuaderno se corrió con EJECUTAR_BUSQUEDA = False: se usan los parámetros que guardó
    MEJORES = dict(sorted({k: v for m in ENSAMBLES for k, v in PARAMETROS[m]["mejores"].items()}.items(),
                          key=lambda kv: tuple(kv[0].split("|"))))
with open(f"{CARPETA_SALIDAS}P4b_mejores_parametros.json", "w") as f:
    json.dump({"espacios": ESPACIOS, "semilla": SEMILLA, "n_configs": N_CONFIGS_BUSQUEDA,
               "max_filas": MAX_FILAS_BUSQUEDA, "mejores": MEJORES}, f, indent=2, default=str)
display(pd.DataFrame(MEJORES).T)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Resultados de validación
# MAGIC
# MAGIC ### 2.1 Tabla general (los tres bloques juntos)

# COMMAND ----------

tabla = tabla_metricas(pred, ["version", "tarea", "modelo"])
display(tabla.round(4))
tabla.to_csv(f"{CARPETA_SALIDAS}P4b_tabla_validacion.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Por bloque
# MAGIC
# MAGIC Sirve para ver si el orden de los modelos se mantiene de un mes a otro.

# COMMAND ----------

por_bloque = tabla_metricas(pred, ["version", "tarea", "modelo", "bloque"])
por_bloque.to_csv(f"{CARPETA_SALIDAS}P4b_validacion_por_bloque.csv", index=False)
principal = por_bloque.assign(metrica=np.where(por_bloque.tarea == "clf", por_bloque.AP, por_bloque.MAE_h)) \
                      .pivot_table(index=["version", "tarea", "modelo"], columns="bloque", values="metrica")
print("Métrica principal por bloque (AP en clasificación, MAE en horas en regresión):")
display(principal.round(4))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Por segmento
# MAGIC
# MAGIC Por categoría de zona, franja y, en regresión, decil de duración real (el decil 10 es el 10 % más lento).

# COMMAND ----------

v = pred[pred.version == "internas"].copy()
v["decil_duracion"] = (pd.qcut(v.y.where(v.tarea == "reg"), 10, labels=False, duplicates="drop") + 1).fillna(0).astype(int)
segmentos = metricas_por_segmento(v, ["categoria_zona", "franja", "decil_duracion"])
segmentos = segmentos[~((segmentos.segmento == "decil_duracion") & (segmentos.tarea == "clf"))]
segmentos.to_csv(f"{CARPETA_SALIDAS}P4b_validacion_por_segmento.csv", index=False)
display(segmentos[segmentos.segmento != "decil_duracion"].round(3).head(40))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. H1 y H2: diferencias con IC 95 % por bloques de día
# MAGIC
# MAGIC Cada comparación se hace sobre **las mismas guías**. En AP, una diferencia **positiva** favorece al primero; en MAE, una **negativa**. Si el IC no incluye el 0, la diferencia es apreciable en el periodo evaluado.
# MAGIC
# MAGIC - **H1:** cada ensamble frente al modelo lineal (logística en clasificación).
# MAGIC - **H2:** LightGBM con clima frente a LightGBM sin clima (solo si hay base con clima).

# COMMAND ----------

filas = []
for e in ENSAMBLES:
    etiqueta = lambda t, e=e: f"H1 · {e} - {'Logística' if t == 'clf' else 'Lineal'}"
    filas += diferencias(pred, "internas", e, "internas", "Lineal", etiqueta, B_BOOTSTRAP, SEMILLA)

# COMMAND ----------

for vclima in [x for x in VERSIONES if x.startswith("internas+clima")]:
    etiqueta = lambda t, vc=vclima: f"H2 · LightGBM {vc} - internas"
    filas += diferencias(pred, vclima, "LightGBM", "internas", "LightGBM", etiqueta, B_BOOTSTRAP, SEMILLA)

# COMMAND ----------

diferencias_val = pd.DataFrame(filas)
diferencias_val["favorece_al_primero"] = np.where(diferencias_val.metrica == "AP",
                                                  diferencias_val.ic95_inf > 0, diferencias_val.ic95_sup < 0)
display(diferencias_val.round(4))
diferencias_val.to_csv(f"{CARPETA_SALIDAS}P4b_diferencias_validacion.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Modelo propuesto y calibración
# MAGIC
# MAGIC ### 4.1 Mejor modelo en validación
# MAGIC
# MAGIC Se propone el de mejor métrica principal con variables internas. Aparte se anota el mejor ensamble, que es el que se contrasta con el lineal en la prueba final (H1 confirmatoria).

# COMMAND ----------

cand_clf = tabla[(tabla.version == "internas") & (tabla.tarea == "clf") & tabla.modelo.isin(MODELOS)].sort_values("AP", ascending=False)
cand_reg = tabla[(tabla.version == "internas") & (tabla.tarea == "reg") & tabla.modelo.isin(MODELOS)].sort_values("MAE_h")
MODELO_CLF_PROPUESTO = cand_clf.iloc[0].modelo
MODELO_REG_PROPUESTO = cand_reg.iloc[0].modelo
ENSAMBLE_H1 = {"clf": cand_clf[cand_clf.modelo.isin(ENSAMBLES)].iloc[0].modelo,
               "reg": cand_reg[cand_reg.modelo.isin(ENSAMBLES)].iloc[0].modelo}
print(f"Mejor en validación · clasificación: {MODELO_CLF_PROPUESTO} · regresión: {MODELO_REG_PROPUESTO}")
print(f"Mejor ensamble (contraste confirmatorio de H1) · clasificación: {ENSAMBLE_H1['clf']} · regresión: {ENSAMBLE_H1['reg']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 ¿Conviene calibrar?
# MAGIC
# MAGIC Calibración isotónica evaluada sin mirar el bloque evaluado: se ajusta con los bloques anteriores y se aplica al siguiente. Se propone calibrar solo si mejora a la vez Brier y pérdida logarítmica.

# COMMAND ----------

pc = pred[(pred.version == "internas") & (pred.tarea == "clf") & (pred.modelo == MODELO_CLF_PROPUESTO)]
cc = calibracion_cruzada(pc)
comp_cal = pd.DataFrame({
    "sin_calibrar": metricas("clf", cc.y, cc.pred),
    "isotonica_cruzada": metricas("clf", cc.y, cc.pred_cal)}).T[["n", "AP", "Brier", "LogLoss"]]
display(comp_cal.round(4))
comp_cal.to_csv(f"{CARPETA_SALIDAS}P4b_calibracion_comparacion.csv")
mejora_brier   = comp_cal.loc["isotonica_cruzada", "Brier"] < comp_cal.loc["sin_calibrar", "Brier"]
mejora_logloss = comp_cal.loc["isotonica_cruzada", "LogLoss"] < comp_cal.loc["sin_calibrar", "LogLoss"]
CALIBRACION_PROPUESTA = "isotonica" if (mejora_brier and mejora_logloss) else "ninguna"
print("Calibración propuesta:", CALIBRACION_PROPUESTA)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Calibrador y curva de calibración
# MAGIC
# MAGIC Si se propone calibrar, el calibrador se ajusta con **todas** las predicciones de validación del modelo propuesto y se guarda para la prueba final.

# COMMAND ----------

calibrador = None
if CALIBRACION_PROPUESTA == "isotonica":
    calibrador = IsotonicRegression(out_of_bounds="clip", y_min=0, y_max=1).fit(pc.pred, pc.y)
    joblib.dump(calibrador, f"{CARPETA_MODELOS}calibrador_clf_validacion.joblib")

# COMMAND ----------

fig, ax = plt.subplots(figsize=(4.5, 4.5))
for nombre, col in [("sin calibrar", "pred"), ("isotónica (cruzada)", "pred_cal")]:
    t = calibracion_tabla(cc.y.values, cc[col].values)
    ax.plot(t.p_media, t.y_obs, marker="o", label=nombre)
ax.plot([0, 1], [0, 1], "k--", lw=1); ax.set_xlabel("Probabilidad predicha"); ax.set_ylabel("Frecuencia observada")
ax.set_title(f"Curva de calibración · {MODELO_CLF_PROPUESTO}"); ax.legend()
plt.tight_layout(); fig.savefig(f"{CARPETA_SALIDAS}P4b_curva_calibracion.png", dpi=130); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Umbral por capacidad de atención (en validación)
# MAGIC
# MAGIC La tabla muestra, para cada umbral, cuántas alertas por día se generarían y qué tanto aciertan. El umbral propuesto es el que deja `ALERTAS_POR_DIA` alertas diarias en promedio (escenario, no un costo oficial).

# COMMAND ----------

p_val = calibrador.predict(pc.pred) if calibrador is not None else pc.pred.values
umbrales = tabla_umbrales(pc.y.values.astype(int), p_val, pc.dia.values)
umbrales["F1"] = 2 * umbrales.precision * umbrales.exhaustividad / (umbrales.precision + umbrales.exhaustividad).replace(0, np.nan)
display(umbrales.round(3))
umbrales.to_csv(f"{CARPETA_SALIDAS}P4b_tabla_umbrales.csv", index=False)

# COMMAND ----------

UMBRAL_PROPUESTO = float(umbral_por_capacidad(p_val, pc.dia.values, ALERTAS_POR_DIA))
a = p_val >= UMBRAL_PROPUESTO
print(f"Escenario de {ALERTAS_POR_DIA} alertas/día -> umbral {UMBRAL_PROPUESTO:.4f}; en validación: "
      f"exhaustividad {recall_score(pc.y, a):.3f}, precisión {precision_score(pc.y, a, zero_division=0):.3f}, "
      f"alertas/día {a.sum() / pc.dia.nunique():.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Decisión (antes de la prueba final)
# MAGIC
# MAGIC La propuesta sale de las reglas fijadas: mejor métrica principal en validación, calibración solo si mejora Brier y pérdida logarítmica, y umbral por capacidad. **El equipo puede cambiar cualquier valor**, pero solo con argumentos de validación. Al aprobar, se escribe `P4b_decision.json` y ya no se modifica.

# COMMAND ----------

DECISION = {
    "modelo_clf": MODELO_CLF_PROPUESTO, "modelo_reg": MODELO_REG_PROPUESTO, "version": "internas",
    "params_clf": params_de(MEJORES, PARAMS_BASE, "internas", "clf", MODELO_CLF_PROPUESTO),
    "params_reg": params_de(MEJORES, PARAMS_BASE, "internas", "reg", MODELO_REG_PROPUESTO),
    "ensamble_h1_clf": ENSAMBLE_H1["clf"], "ensamble_h1_reg": ENSAMBLE_H1["reg"],
    "params_h1_clf": params_de(MEJORES, PARAMS_BASE, "internas", "clf", ENSAMBLE_H1["clf"]),
    "params_h1_reg": params_de(MEJORES, PARAMS_BASE, "internas", "reg", ENSAMBLE_H1["reg"]),
    "calibracion": CALIBRACION_PROPUESTA, "umbral": UMBRAL_PROPUESTO, "alertas_por_dia_escenario": ALERTAS_POR_DIA,
    "variables_categoricas": CATEGORICAS, "variables_numericas": VERSIONES["internas"],
    "excluir_entrega_sin_hora": EXCLUIR_ENTREGA_SIN_HORA, "usar_variables_cliente": USAR_VARIABLES_CLIENTE,
    "fin_desarrollo": FIN_DESARROLLO, "fin_prueba": FIN_PRUEBA, "semilla": SEMILLA}
# Si H2 resultó favorable en validación y el equipo decide usar clima, cambiar "version" y "variables_numericas" aquí.
print(json.dumps(DECISION, indent=2, default=str))

# COMMAND ----------

DECISION_APROBADA = True      # cambiar a True cuando el equipo y el asesor estén de acuerdo
RUTA_DECISION = f"{CARPETA_SALIDAS}P4b_decision.json"
if not DECISION_APROBADA:
    print("Decisión NO registrada (DECISION_APROBADA = False).")
elif os.path.exists(RUTA_DECISION):
    print("Ya existe una decisión registrada; no se sobrescribe:", RUTA_DECISION)
else:
    DECISION["fecha_registro"] = datetime.datetime.now().isoformat()
    with open(RUTA_DECISION, "w") as f: json.dump(DECISION, f, indent=2, default=str)
    print("Decisión registrada en", RUTA_DECISION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Prueba final (ejecutar UNA sola vez)
# MAGIC
# MAGIC Usa exclusivamente lo registrado en `P4b_decision.json`. Los pasos:
# MAGIC
# MAGIC 1. **Guardas y datos:** comprueba que hay decisión y que la prueba no se ha corrido; usa la historia congelada en `FIN_DESARROLLO` que dejó 0411.
# MAGIC 2. **Entrenamiento:** para cada tarea entrena, con todo el desarrollo, el modelo elegido, el lineal y el mejor ensamble (H1).
# MAGIC 3. **Clasificación:** métricas, calibración (si se decidió), referencias y matriz de confusión con el umbral fijado.
# MAGIC 4. **Regresión:** métricas y referencias (media histórica y constante).
# MAGIC 5. **H1 confirmatoria:** mejor ensamble menos lineal, con IC 95 % por días.
# MAGIC 6. **Guardar para 0420:** modelos, datos de prueba (sin guía, con `zona_urbana`) y muestra de entrenamiento para SHAP.
# MAGIC 7. **Tablas finales.**
# MAGIC
# MAGIC Cada celda revisa `EJECUTAR_PRUEBA_FINAL`; con `False` no hace nada.

# COMMAND ----------

# 7.1 Guardas y datos
EJECUTAR_PRUEBA_FINAL = True      # cambiar a True UNA sola vez, con la decisión ya registrada
if EJECUTAR_PRUEBA_FINAL:
    assert os.path.exists(RUTA_DECISION), "Primero registrar la decisión (sección 6)"
    assert not glob.glob(f"{CARPETA_SALIDAS}P4b_prueba_final_*.csv"), "La prueba final ya se ejecutó: no se repite"
    DEC = json.load(open(RUTA_DECISION))
    num, cat = DEC["variables_numericas"], DEC["variables_categoricas"]
    d = con_historia(df, leer_historia(f"{CARPETA_MODELADO}historia_final.parquet"))   # congelada en 0411
    corte, fin = pd.Timestamp(DEC["fin_desarrollo"]), pd.Timestamp(DEC["fin_prueba"])
    PF = {}      # todo lo de la prueba final, por tarea
else:
    print("Prueba final no ejecutada (EJECUTAR_PRUEBA_FINAL = False).")

# COMMAND ----------

# 7.2 Entrenamiento: modelo elegido, lineal y mejor ensamble (H1), con todo el desarrollo
if EJECUTAR_PRUEBA_FINAL:
    for tarea, y_col, t_et in TAREAS:
        tr = d.index[(d.t0 < corte) & (d[t_et] < corte) & d[y_col].notna()]
        te = d.index[(d.t0 >= corte) & (d.t0 < fin) & d[y_col].notna()]
        Xtr, ytr, Xte = d.loc[tr, cat + num], d.loc[tr, y_col], d.loc[te, cat + num]
        nombre, ens = DEC[f"modelo_{tarea}"], DEC[f"ensamble_h1_{tarea}"]
        m = construir_modelo(tarea, nombre, num, cat, DEC[f"params_{tarea}"], SEMILLA).fit(Xtr, ytr)
        lin = construir_modelo(tarea, "Lineal", num, cat, {}, SEMILLA).fit(Xtr, ytr)
        p = predecir(m, Xte, tarea)
        p_ens = p if ens == nombre else \
            predecir(construir_modelo(tarea, ens, num, cat, DEC[f"params_h1_{tarea}"], SEMILLA).fit(Xtr, ytr), Xte, tarea)
        PF[tarea] = dict(y_col=y_col, tr=tr, te=te, Xtr=Xtr, ytr=ytr, Xte=Xte, y=d.loc[te, y_col].to_numpy(),
                         nombre=nombre, ens=ens, modelo=m, p=p, p_lin=predecir(lin, Xte, tarea), p_ens=p_ens)
        print(f"{tarea}: entrenamiento {len(tr):,} · prueba {len(te):,} · modelo {nombre} · ensamble H1 {ens}")

# COMMAND ----------

# 7.3 Clasificación: métricas, calibración, referencias y umbral
if EJECUTAR_PRUEBA_FINAL:
    c = PF["clf"]; y, p = c["y"], c["p"]
    fila = {"tarea": "clf", "modelo": c["nombre"], **metricas("clf", y, p)}
    if DEC["calibracion"] == "isotonica":
        assert DEC["modelo_clf"] == MODELO_CLF_PROPUESTO, "El calibrador se ajustó para otro modelo: volver a ejecutar la sección 4 con ese modelo"
        cal = joblib.load(f"{CARPETA_MODELOS}calibrador_clf_validacion.joblib")
        p = cal.predict(p); joblib.dump(cal, f"{CARPETA_MODELOS}calibrador_clf.joblib")
        fila.update({f"{k}_calibrado": v for k, v in metricas("clf", y, p).items() if k in ("Brier", "LogLoss", "AP")})
    y = y.astype(int); a = p >= DEC["umbral"]
    fila.update({f"ref_{k}": v for k, v in referencias_probabilidad(c["ytr"], y).items()})
    fila.update({"umbral": DEC["umbral"], "VP": int((a & (y == 1)).sum()), "FP": int((a & (y == 0)).sum()),
                 "FN": int((~a & (y == 1)).sum()), "VN": int((~a & (y == 0)).sum()),
                 "precision": precision_score(y, a, zero_division=0), "exhaustividad": recall_score(y, a),
                 "F1": f1_score(y, a), "alertas_por_dia": a.sum() / d.loc[c["te"], "dia"].nunique()})
    c.update(y=y, p_final=p, fila=fila)

# COMMAND ----------

# 7.4 Regresión: métricas y referencias
if EJECUTAR_PRUEBA_FINAL:
    r = PF["reg"]; y, p, ytr = r["y"], r["p"], r["ytr"]
    ref = d.loc[r["te"], "hist_h_zona_franja"].fillna(ytr.mean()).to_numpy()
    fila = {"tarea": "reg", "modelo": r["nombre"], **metricas("reg", y, p),
            "ref_MAE_media_historica_h": mean_absolute_error(y, ref),
            "ref_MAE_constante_h": mean_absolute_error(y, np.full(len(y), ytr.mean()))}
    r.update(p_final=p, fila=fila)

# COMMAND ----------

# 7.5 Lineal y H1 confirmatoria: mejor ensamble (sin calibrar) frente al lineal, IC 95 % por días
if EJECUTAR_PRUEBA_FINAL:
    difs = []
    for tarea, metrica in [("clf", average_precision_score), ("reg", mean_absolute_error)]:
        t = PF[tarea]
        t["fila"].update({f"lineal_{k}": v for k, v in metricas(tarea, t["y"], t["p_lin"]).items()
                          if k in ("AP", "MAE_h", "Brier", "AUC_ROC", "RMSE_h")})
        ic = diferencia_por_bloques(t["y"], t["p_ens"], t["p_lin"], d.loc[t["te"], "dia"].values, metrica,
                                    B=B_BOOTSTRAP, semilla=SEMILLA)
        difs.append({"tarea": tarea, "comparacion": f"H1 · {t['ens']} - {'Logística' if tarea == 'clf' else 'Lineal'} (prueba final)",
                     "metrica": "AP" if tarea == "clf" else "MAE_h", **ic})

# COMMAND ----------

# 7.6 Guardar para 0420: modelo, datos de prueba (sin guía; con zona_urbana para la evaluación espacial) y fondo para SHAP
if EJECUTAR_PRUEBA_FINAL:
    for tarea, t in PF.items():
        joblib.dump(t["modelo"], f"{CARPETA_MODELOS}modelo_final_{tarea}.joblib")
        X_guardar = t["Xte"].copy()
        X_guardar["y"], X_guardar["pred"] = t["y"], t["p_final"]
        X_guardar["dia"] = d.loc[t["te"], "dia"].astype(str).values
        X_guardar["zona_urbana"] = d.loc[t["te"], "zona_urbana"].values
        X_guardar.to_parquet(f"{CARPETA_MODELOS}prueba_{tarea}.parquet", index=False)
        s = t["Xtr"].sample(min(50_000, len(t["Xtr"])), random_state=SEMILLA)
        s[t["y_col"]] = t["ytr"].loc[s.index]
        s.to_parquet(f"{CARPETA_MODELOS}muestra_entrenamiento_{tarea}.parquet", index=False)

# COMMAND ----------

# 7.7 Tablas finales
if EJECUTAR_PRUEBA_FINAL:
    final = pd.DataFrame([PF["clf"]["fila"], PF["reg"]["fila"]]); display(final.T)
    final.to_csv(f"{CARPETA_SALIDAS}P4b_prueba_final_metricas.csv", index=False)
    difs = pd.DataFrame(difs); display(difs)
    difs.to_csv(f"{CARPETA_SALIDAS}P4b_prueba_final_diferencias.csv", index=False)
    preds_test = pd.concat([pd.DataFrame({"tarea": tarea, "modelo": t["nombre"], "y": t["y"], "pred": t["p_final"],
                                          "categoria_zona": d.loc[t["te"], "categoria_zona"].values,
                                          "franja": d.loc[t["te"], "franja"].values}) for tarea, t in PF.items()])
    seg = metricas_por_segmento(preds_test, ["categoria_zona", "franja"])
    seg.to_csv(f"{CARPETA_SALIDAS}P4b_prueba_final_por_segmento.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Manifiesto de la corrida
# MAGIC
# MAGIC Versiones de librerías, huella de los archivos de entrada, parámetros, exclusiones y lista de salidas. Se ejecuta siempre al final.

# COMMAND ----------

manifiesto = {"fecha_corrida": datetime.datetime.now().isoformat(), "python": sys.version.split()[0],
              "versiones": {"duckdb": duckdb.__version__, "pandas": pd.__version__, "numpy": np.__version__, "sklearn": sklearn.__version__,
                            "lightgbm": lgb.__version__, "xgboost": xgb.__version__},
              "entradas": {"gold": huella(ruta), "calendario": huella(RUTA_CALENDARIO)},
              "parametros": {k: globals()[k] for k in ["INICIOS_VALIDACION", "FIN_DESARROLLO", "FIN_PRUEBA", "ALERTAS_POR_DIA", "SEMILLA",
                             "USAR_VARIABLES_CLIENTE", "SOLO_SLA_UNICO", "EXCLUIR_ENTREGA_SIN_HORA", "N_CONFIGS_BUSQUEDA", "MAX_FILAS_BUSQUEDA", "B_BOOTSTRAP"]},
              "exclusiones": exclusiones.to_dict("records"), "versiones_evaluadas": list(VERSIONES),
              "salidas": sorted(os.path.basename(f) for f in glob.glob(f"{CARPETA_SALIDAS}P4b_*"))}
with open(f"{CARPETA_SALIDAS}P4b_manifiesto.json", "w") as f: json.dump(manifiesto, f, indent=2, default=str)
print("Manifiesto guardado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué entregar
# MAGIC
# MAGIC **Después de la validación (secciones 1–6):** `P4b_exclusiones.csv` (de 0411), `P4b_busqueda.csv`, `P4b_mejores_parametros.json`, `P4b_tabla_validacion.csv`, `P4b_validacion_por_bloque.csv`, `P4b_validacion_por_segmento.csv`, `P4b_diferencias_validacion.csv`, `P4b_calibracion_comparacion.csv`, `P4b_curva_calibracion.png`, `P4b_tabla_umbrales.csv` y la decisión `P4b_decision.json`.
# MAGIC
# MAGIC **Después de la prueba final (sección 7):** `P4b_prueba_final_metricas.csv`, `P4b_prueba_final_diferencias.csv`, `P4b_prueba_final_por_segmento.csv` y el manifiesto. Con eso se ejecuta `0420_Interpretabilidad_4`. Todo queda en la carpeta `V3/`.
# MAGIC
# MAGIC **Cómo leer H1.** H1 se sostiene en una tarea si el IC 95 % de la diferencia «ensamble − lineal» excluye el 0 en la dirección favorable, primero en validación y luego, de forma confirmatoria, en la prueba final. Si no, se reporta que no hay evidencia de mejora (H0 no se rechaza).
# MAGIC
# MAGIC **Recordatorio.** La prueba final de julio–agosto es la misma que se usó antes para elegir modelos en el 0400 antiguo. Debe declararse en el documento como una evaluación retrospectiva limitada, o bien reservarse datos posteriores a agosto si se vuelven a extraer (realimentación P4).