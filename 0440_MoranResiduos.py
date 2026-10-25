# Databricks notebook source
# MAGIC %md
# MAGIC # 0440 · Dependencia espacial de los residuos (I de Moran por zona×mes)
# MAGIC
# MAGIC **Para qué.** El docente preguntó cómo se maneja que el modelo supone guías independientes cuando hay vecindades. Iván respondió (P2, 25-10): calcular el **I de Moran sobre los residuos de VALIDACIÓN promediados por zona y mes**, y llevar la vecindad al modelo solo si hace falta. Este cuaderno hace exactamente eso, sin tocar la prueba final.
# MAGIC
# MAGIC **Qué usa.**
# MAGIC - Predicciones de validación de 0416 (`P4b_predicciones_validacion.parquet`): bloques abril, mayo y junio, con los modelos elegidos en `P4b_decision.json` y el modelo lineal como referencia.
# MAGIC - `base_modelado.parquet` de 0411, solo para unir guía (seudónimo) con `zona_urbana`.
# MAGIC - `P7_zona_localidad_v1.csv` de 0430: punto del barrio de cada zona y su UPZ.
# MAGIC
# MAGIC **Matrices de vecindad (fijadas antes de ver resultados).**
# MAGIC - **W1, zonas, k vecinos más cercanos** (k = 5, estandarizada por filas) sobre el punto del barrio. Es la principal.
# MAGIC - **W2, zonas, k = 10.** Sensibilidad.
# MAGIC - **W3, UPZ, contigüidad tipo reina** con los residuos promediados por UPZ y mes. Sensibilidad con unidades oficiales.
# MAGIC
# MAGIC **Regla de lectura, fijada antes de correr.**
# MAGIC - Se calcula I con prueba de permutación (999 permutaciones) en cada mes, tarea y modelo. Solo entran zonas con al menos `MIN_GUIAS` guías en ese mes.
# MAGIC - **Hay dependencia que tratar** si, con W1, el modelo elegido tiene I > 0 con p < 0,05 en al menos 2 de los 3 meses. En ese caso se propone agregar al modelo el rezago espacial de la historia (promedio de `hist_*` de las zonas vecinas, W·X, sin futuro) y reportar errores agrupados por zona. Se evalúa solo en validación.
# MAGIC - **Si no**, se reporta que los residuos no muestran dependencia espacial detectable a esta escala, y la independencia condicional queda como supuesto verificado. CAR o BYM quedan como trabajo futuro, como sugirió Iván.
# MAGIC
# MAGIC **Escribe** solo tablas agregadas `P8_*` en `V3/ESPACIAL/`. No muestra guías.

# COMMAND ----------

# MAGIC %pip install "duckdb<2" scikit-learn

# COMMAND ----------

# CONFIGURACIÓN
import os, json, numpy as np, pandas as pd
from sklearn.neighbors import NearestNeighbors
PATH_DATABRICKS  = "/Volumes/proyectogrado/source/source/"
CARPETA_V3       = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3/"
RUTA_PRED_VAL    = f"{CARPETA_V3}P4b_predicciones_validacion.parquet"        # 0416, sección 1
RUTA_DECISION    = f"{CARPETA_V3}P4b_decision.json"
RUTA_BASE        = f"{CARPETA_V3}MODELADO/base_modelado.parquet"             # 0411
CARPETA_ESPACIAL = f"{CARPETA_V3}ESPACIAL/"
RUTA_ZONAS       = f"{CARPETA_ESPACIAL}P7_zona_localidad_v1.csv"              # 0430
RUTA_VEC_UPZ     = f"{CARPETA_ESPACIAL}P7_vecindad_upz.csv"                   # 0430 (si no hubo capa de localidades)

MIN_GUIAS     = 30        # mínimo de guías de una zona en el mes para entrar al cálculo
K_PRINCIPAL   = 5
K_SENSIBLE    = 10
PERMUTACIONES = 999
SEMILLA       = 42
ALFA          = 0.05
try:
    display
except NameError:
    display = print

# COMMAND ----------

# 1. Residuos de validación por guía, con su zona
dec = json.load(open(RUTA_DECISION))
MODELOS = {"clf": [dec["modelo_clf"], "Lineal"], "reg": [dec["modelo_reg"], "Lineal"]}
print("Modelos evaluados:", MODELOS)
pred = pd.read_parquet(RUTA_PRED_VAL)
pred = pred[(pred.version == "internas") & pred.apply(lambda r: r.modelo in MODELOS[r.tarea], axis=1)]
base = pd.read_parquet(RUTA_BASE, columns=["guia", "zona_urbana"])
res = pred.merge(base, on="guia", how="left")
assert res.zona_urbana.notna().mean() > 0.95, "Muchas predicciones sin zona: revisar que 0411 y 0416 sean de la misma corrida"
res["residuo"] = res.y - res.pred                      # reg: horas reales − predichas; clf: 1/0 − probabilidad
res["mes"] = pd.to_datetime(res.bloque).dt.strftime("%Y-%m")
print(res.groupby(["tarea", "modelo", "mes"]).size().unstack())

# COMMAND ----------

# 2. Promedio por zona×mes y ubicación de la zona (0430)
zonas = pd.read_csv(RUTA_ZONAS)[["zona_urbana", "lon", "lat", "upz", "estado"]]
agg = (res.groupby(["tarea", "modelo", "mes", "zona_urbana"])
          .agg(guias=("residuo", "size"), residuo_medio=("residuo", "mean"), y_medio=("y", "mean")).reset_index()
          .merge(zonas, on="zona_urbana", how="left"))
cob = (agg.assign(ubicada=agg.lon.notna(), suficiente=agg.guias >= MIN_GUIAS)
          .groupby(["tarea", "modelo", "mes", "ubicada", "suficiente"]).agg(zonas=("guias", "size"), guias=("guias", "sum")).reset_index())
cob.to_csv(f"{CARPETA_ESPACIAL}P8_cobertura_moran.csv", index=False)
display(cob[(cob.tarea == "reg") & (cob.modelo == dec["modelo_reg"])])

# COMMAND ----------

# 3. I de Moran con prueba de permutación
def w_knn(xy_m, k):
    '''Matriz kNN estandarizada por filas (densa; cientos de zonas). Zonas con el mismo punto quedan vecinas a 0 m.'''
    n = len(xy_m); k = min(k, n - 1)
    _, idx = NearestNeighbors(n_neighbors=k + 1).fit(xy_m).kneighbors(xy_m)
    W = np.zeros((n, n))
    for i, fila in enumerate(idx):
        vecinos = [j for j in fila if j != i][:k]
        W[i, vecinos] = 1.0 / k
    return W

def w_contiguidad(unidades, pares):
    '''Matriz de contigüidad estandarizada por filas a partir de pares (a, b). Unidades sin vecinos quedan en cero.'''
    pos = {u: i for i, u in enumerate(unidades)}
    W = np.zeros((len(unidades), len(unidades)))
    for a, b in pares:
        if a in pos and b in pos:
            W[pos[a], pos[b]] = W[pos[b], pos[a]] = 1.0
    s = W.sum(1, keepdims=True)
    return np.divide(W, s, out=np.zeros_like(W), where=s > 0)

def moran(x, W, permutaciones=PERMUTACIONES, semilla=SEMILLA):
    z = np.asarray(x, float) - np.mean(x)
    S0 = W.sum()
    I = len(z) / S0 * (z @ W @ z) / (z @ z)
    rng = np.random.default_rng(semilla)
    sim = np.array([(lambda p: len(p) / S0 * (p @ W @ p) / (p @ p))(rng.permutation(z)) for _ in range(permutaciones)])
    p = (1 + np.sum(sim >= I)) / (permutaciones + 1)            # una cola: autocorrelación positiva
    return {"I": I, "E_I": -1 / (len(z) - 1), "p_permutacion": p, "z_sim": (I - sim.mean()) / sim.std()}

def a_metros(lon, lat):
    '''Proyección equirrectangular local (Bogotá): suficiente para vecinos más cercanos.'''
    return np.c_[np.radians(lon) * 6371000 * np.cos(np.radians(4.65)), np.radians(lat) * 6371000]

# COMMAND ----------

# 4. Moran por mes, tarea y modelo: W1 (k=5) y W2 (k=10) entre zonas
filas = []
for (tarea, modelo, mes), g in agg.groupby(["tarea", "modelo", "mes"]):
    g = g[(g.guias >= MIN_GUIAS) & g.lon.notna()].reset_index(drop=True)
    if len(g) < 20: continue
    xy = a_metros(g.lon.values, g.lat.values)
    for nombre_w, k in [("W1 zonas kNN k=5", K_PRINCIPAL), ("W2 zonas kNN k=10", K_SENSIBLE)]:
        filas.append({"tarea": tarea, "modelo": modelo, "mes": mes, "matriz": nombre_w, "unidades": len(g),
                      "guias": int(g.guias.sum()), **moran(g.residuo_medio.values, w_knn(xy, k))})
moran_zonas = pd.DataFrame(filas)

# COMMAND ----------

# 5. Sensibilidad W3: residuos por UPZ×mes con contigüidad tipo reina
filas = []
if os.path.exists(RUTA_VEC_UPZ):
    pares = pd.read_csv(RUTA_VEC_UPZ).itertuples(index=False, name=None)
    pares = list(pares)
    res_u = res.merge(zonas[["zona_urbana", "upz"]], on="zona_urbana", how="left").dropna(subset=["upz"])
    for (tarea, modelo, mes), g in res_u.groupby(["tarea", "modelo", "mes"]):
        u = g.groupby("upz").agg(guias=("residuo", "size"), residuo_medio=("residuo", "mean")).reset_index()
        u = u[u.guias >= MIN_GUIAS].reset_index(drop=True)
        if len(u) < 20: continue
        W = w_contiguidad(list(u.upz), pares)
        con_vecinos = W.sum(1) > 0
        u, W = u[con_vecinos].reset_index(drop=True), w_contiguidad(list(u.upz[con_vecinos]), pares)
        filas.append({"tarea": tarea, "modelo": modelo, "mes": mes, "matriz": "W3 UPZ reina", "unidades": len(u),
                      "guias": int(u.guias.sum()), **moran(u.residuo_medio.values, W)})
else:
    print("Sin P7_vecindad_upz.csv: se omite W3")
moran_tabla = pd.concat([moran_zonas, pd.DataFrame(filas)], ignore_index=True).round(4)
moran_tabla.to_csv(f"{CARPETA_ESPACIAL}P8_moran_residuos_validacion.csv", index=False)
display(moran_tabla)

# COMMAND ----------

# 6. Referencia: Moran del OBJETIVO (no del residuo) por zona×mes con W1
#    Si el objetivo tiene dependencia y el residuo no, el modelo ya la absorbe (zona×franja, historia).
filas = []
for (tarea, mes), g in agg[agg.modelo == "Lineal"].groupby(["tarea", "mes"]):
    g = g[(g.guias >= MIN_GUIAS) & g.lon.notna()].reset_index(drop=True)
    if len(g) < 20: continue
    filas.append({"tarea": tarea, "mes": mes, "matriz": "W1 zonas kNN k=5", "unidades": len(g),
                  **moran(g.y_medio.values, w_knn(a_metros(g.lon.values, g.lat.values), K_PRINCIPAL))})
moran_y = pd.DataFrame(filas).round(4)
moran_y.to_csv(f"{CARPETA_ESPACIAL}P8_moran_objetivo.csv", index=False)
display(moran_y)

# COMMAND ----------

# 7. Lectura con la regla fijada
for tarea in ["reg", "clf"]:
    elegido = dec[f"modelo_{tarea}"]
    t = moran_tabla[(moran_tabla.tarea == tarea) & (moran_tabla.modelo == elegido) & (moran_tabla.matriz == "W1 zonas kNN k=5")]
    meses_sig = int(((t.I > 0) & (t.p_permutacion < ALFA)).sum())
    veredicto = ("HAY dependencia espacial en los residuos: proponer rezago W·X de la historia y errores agrupados por zona (solo validación)"
                 if meses_sig >= 2 else "Sin dependencia espacial detectable en los residuos: se sostiene la independencia condicional")
    print(f"{tarea} · {elegido}: I por mes = {list(t.I.round(3))}, p = {list(t.p_permutacion.round(3))} → {meses_sig}/3 meses significativos")
    print("   ", veredicto)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué entregar
# MAGIC
# MAGIC - `P8_moran_residuos_validacion.csv`: I, valor esperado, p de permutación y z por mes, tarea, modelo y matriz. Es la tabla principal para el Hito 4.
# MAGIC - `P8_moran_objetivo.csv`: el mismo cálculo sobre el objetivo, para mostrar cuánto de la dependencia absorbe el modelo.
# MAGIC - `P8_cobertura_moran.csv`: cuántas zonas y guías entran (ubicadas y con al menos `MIN_GUIAS`).
# MAGIC
# MAGIC **Limitación que se declara.** Solo entran las zonas que 0430 pudo ubicar (alrededor del 54 % de las guías en la primera versión de la tabla). Las zonas no geográficas (aliados, oficinas) no tienen vecindad y quedan fuera por diseño.