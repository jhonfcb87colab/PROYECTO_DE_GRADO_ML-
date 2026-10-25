# Databricks notebook source
# MAGIC %md
# MAGIC # 0417 · Sensibilidad en validación: hueco temporal y regresión con todas las guías
# MAGIC
# MAGIC **Para qué.** Iván pidió dos cambios (25-10) que se evalúan aquí **solo en validación** (abril, mayo y junio). La prueba final ya se corrió una vez (24-10) y **no se repite**.
# MAGIC
# MAGIC | Sensibilidad | Qué cambia | Pregunta |
# MAGIC | --- | --- | --- |
# MAGIC | **S1 · Hueco p99** (P6) | Se entrena solo con guías cuyo T0 es anterior a `inicio del bloque − hueco`, donde el hueco es el percentil 99 de las horas de T0 a entrega en el desarrollo. | ¿El resultado depende de entrenar con guías muy cercanas al bloque? |
# MAGIC | **S2 · Regresión con todas** (P5) | La regresión se entrena y valida con todas las guías que salen a reparto, también las que ya estaban vencidas en T0. | ¿Cuánto cambia el error al incluir esa población y cómo se comporta en cada parte? |
# MAGIC
# MAGIC **Modelos.** Los de `P4b_decision.json` (clasificación y regresión), con sus hiperparámetros, y el lineal como referencia. No hay búsqueda nueva.
# MAGIC
# MAGIC **Regla de lectura, fijada antes de correr.**
# MAGIC - **S1:** el diseño original se sostiene si el IC 95 % por días de la diferencia (S1 − base) incluye 0 **o** el cambio es menor que el 2 % de la métrica base (con más de un millón de guías, diferencias mínimas salen significativas). Si empeora más que eso, se reporta y se discute con Iván.
# MAGIC - **S2:** es un análisis secundario. El modelo principal sigue siendo la población con margen (P5). Se reporta el MAE en todas, en las que tenían margen y en las vencidas, y la diferencia con la base sobre las mismas guías con margen.
# MAGIC
# MAGIC **Necesita:** que 0411 y 0416 (sección 1) se hayan corrido, `MODULOS/` junto al cuaderno (el mismo de los cuadernos v5) y `P4b_decision.json`. **Escribe** solo tablas `P4c_*` agregadas en `V3/`.

# COMMAND ----------

# Si faltan librerías:
# !pip install "duckdb<2" lightgbm xgboost scikit-learn

# COMMAND ----------

# MAGIC %pip install "duckdb<2" lightgbm xgboost
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, json, time, warnings
import numpy as np, pandas as pd, duckdb
from sklearn.metrics import average_precision_score, mean_absolute_error
warnings.filterwarnings("ignore", category=UserWarning)
from MODULOS.modelado_temporal import (cargar_preparacion, cargar_festivos, tabla_exclusiones, agregar_historia2,
                                       pliegues_origen_movil, partes_del_bloque, marco_predicciones, construir_modelo,
                                       predecir, tabla_metricas, diferencia_por_bloques)

PATH_DATABRICKS  = "/Volumes/proyectogrado/source/source/"
CARPETA_SALIDAS  = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3/"
CARPETA_MODELADO = f"{CARPETA_SALIDAS}MODELADO/"
RUTA_DECISION    = f"{CARPETA_SALIDAS}P4b_decision.json"
RUTA_PRED_VAL    = f"{CARPETA_SALIDAS}P4b_predicciones_validacion.parquet"      # 0416, sección 1

USAR_PRED_GUARDADAS = True     # True: la base son las predicciones de validación de 0416 (ahorra tiempo)
# Si S1 ya terminó y sus CSV están en V3/, se puede correr solo S2 (menos memoria): EJECUTAR_S1 = False
CUANTIL_HUECO       = 0.99     # P6 de Iván
B_BOOTSTRAP         = 1000
EJECUTAR_S1, EJECUTAR_S2 = True, True
try:
    display
except NameError:
    display = print

# COMMAND ----------

# 0. Preparación de 0411 y decisión registrada
CFG, df, HIST = cargar_preparacion(CARPETA_MODELADO)
DEC = json.load(open(RUTA_DECISION))
SEMILLA, CATEGORICAS, TAREAS = CFG["SEMILLA"], CFG["CATEGORICAS"], CFG["TAREAS"]
NUMERICAS = DEC["variables_numericas"]
assert DEC["version"] == "internas", "Este cuaderno supone la versión 'internas' registrada en la decisión"
MODELOS = {"clf": {DEC["modelo_clf"]: DEC["params_clf"], "Lineal": {}},
           "reg": {DEC["modelo_reg"]: DEC["params_reg"], "Lineal": {}}}
INICIOS = [pd.Timestamp(x) for x in CFG["INICIOS_VALIDACION"]]
print(f"{len(df):,} guías con margen · modelos: { {t: list(m) for t, m in MODELOS.items()} }")

# COMMAND ----------

# Utilidades
def validar(d, hist, tarea, y_col, pliegues, etiqueta, filtro_tr=None):
    '''Entrena y predice cada bloque con los modelos de la decisión. filtro_tr(ini) -> máscara booleana sobre d
    que restringe además el entrenamiento (S1). Devuelve la tabla de predicciones.'''
    out = []
    for ini, tr, va in pliegues:
        if filtro_tr is not None:
            tr = tr[filtro_tr(ini)[tr]]
        Dva, Xtr, ytr, Xva, yva = partes_del_bloque(d, hist[ini], y_col, tr, va, CATEGORICAS + NUMERICAS)
        for nombre, params in MODELOS[tarea].items():
            m = construir_modelo(tarea, nombre, NUMERICAS, CATEGORICAS, params, SEMILLA).fit(Xtr, ytr)
            out.append(marco_predicciones(etiqueta, tarea, ini, Dva, yva).assign(modelo=nombre, pred=predecir(m, Xva, tarea),
                                                                              n_entrenamiento=len(ytr)))
        print(f"{etiqueta} · {tarea} · {ini.date()}: entrenamiento {len(ytr):,} · validación {len(yva):,}")
    return pd.concat(out, ignore_index=True)

def comparar(a, b, tarea, etiqueta):
    '''Diferencia de la métrica (a − b) sobre las mismas guías, con IC 95 % por días.'''
    met = average_precision_score if tarea == "clf" else mean_absolute_error
    m = a.merge(b[["bloque", "guia", "modelo", "pred"]], on=["bloque", "guia", "modelo"], suffixes=("", "_b"))
    filas = []
    for modelo, g in m.groupby("modelo"):
        filas.append({"comparacion": etiqueta, "tarea": tarea, "modelo": modelo, "metrica": "AP" if tarea == "clf" else "MAE_h",
                      "guias": len(g), **diferencia_por_bloques(g.y.values, g.pred.values, g.pred_b.values, g.dia.values,
                                                                met, B=B_BOOTSTRAP, semilla=SEMILLA)})
    return filas

PLIEGUES = {t: list(pliegues_origen_movil(df, CFG["INICIOS_VALIDACION"], CFG["FIN_DESARROLLO"], t_etiqueta=te))
            for t, _, te in TAREAS}

# COMMAND ----------

# 1. Base: predicciones de validación de 0416 (o reentrenadas aquí con la misma configuración)
if USAR_PRED_GUARDADAS:
    p = pd.read_parquet(RUTA_PRED_VAL)
    base = pd.concat([p[(p.version == "internas") & (p.tarea == t) & p.modelo.isin(list(MODELOS[t]))] for t in MODELOS])
    base["version"] = "base"
else:
    base = pd.concat([validar(df, HIST, t, y, PLIEGUES[t], "base") for t, y, _ in TAREAS], ignore_index=True)
base["bloque"] = pd.to_datetime(base.bloque).dt.date
display(tabla_metricas(base, ["version", "tarea", "modelo"]).round(4))

# COMMAND ----------

# MAGIC %md
# MAGIC ## S1 · Hueco p99 entre entrenamiento y validación
# MAGIC
# MAGIC El hueco se calcula **solo con el desarrollo anterior al primer bloque** (T0 antes de abril): percentil 99 de las horas entre T0 y la entrega. El conjunto de validación no cambia; solo se quita del entrenamiento lo que salió a reparto dentro del hueco.

# COMMAND ----------

# S1.a Hueco
previo = df[(df.t0 < INICIOS[0]) & df.t_etiqueta_reg.notna()]
horas = (previo.t_etiqueta_reg - previo.t0).dt.total_seconds() / 3600
HUECO_H = float(np.quantile(horas[horas >= 0], CUANTIL_HUECO))
print(f"Hueco = p{int(CUANTIL_HUECO * 100)} de T0 → entrega en el desarrollo previo a abril: {HUECO_H:.1f} h ({HUECO_H / 24:.1f} días)")

# COMMAND ----------

# S1.b Validación con hueco
if EJECUTAR_S1:
    t_ini = time.time()
    hueco = pd.Timedelta(hours=HUECO_H)
    s1 = pd.concat([validar(df, HIST, t, y, PLIEGUES[t], "S1_hueco_p99",
                            filtro_tr=lambda ini: (df.t0 < ini - hueco).to_numpy())
                    for t, y, _ in TAREAS], ignore_index=True)
    s1["bloque"] = pd.to_datetime(s1.bloque).dt.date
    print(f"S1: {time.time() - t_ini:.0f} s")
    tabla_s1 = tabla_metricas(pd.concat([base, s1]), ["version", "tarea", "modelo"]).round(4)
    dif_s1 = pd.DataFrame(sum([comparar(s1[s1.tarea == t], base[base.tarea == t], t, "S1 hueco − base") for t in MODELOS], [])).round(4)
    tabla_s1.assign(hueco_h=round(HUECO_H, 1)).to_csv(f"{CARPETA_SALIDAS}P4c_S1_hueco_metricas.csv", index=False)
    dif_s1.to_csv(f"{CARPETA_SALIDAS}P4c_S1_hueco_diferencias.csv", index=False)
    display(tabla_s1); display(dif_s1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S2 · Regresión con todas las guías que salen a reparto
# MAGIC
# MAGIC Se arma la población como en 0411 **sin** el filtro «No vencido antes de T0» (los demás filtros iguales), se recalcula la historia sin futuro con esa población y se valida la regresión. Las métricas se reportan en tres grupos: todas, con margen en T0 y vencidas en T0.

# COMMAND ----------

# S2.a Población ampliada (mismos pasos y variables derivadas que 0411), cargada con poca memoria:
#      solo las columnas que usan los modelos, solo el desarrollo (T0 antes de FIN_DESARROLLO) y filtros en DuckDB.
if EJECUTAR_S2:
    import gc
    base = base[base.tarea == "reg"].copy()                      # S2 solo usa la regresión
    for nombre in ["df", "HIST", "PLIEGUES", "s1", "previo", "horas"]:
        globals().pop(nombre, None)
    gc.collect()
    cols_cat = [c for c in CATEGORICAS if c not in ("franja", "dia_semana")]
    cols_num = [c for c in NUMERICAS if not c.startswith(("hist_", "n_hist")) and c not in ("es_festivo", "volumen_declarado_cm3")]
    columnas = sorted(set(["guia", "t0", "fecha_entrega", "t_etiqueta_reg", "t_etiqueta_clf", "h_ultima_milla",
                           "incumplimiento_sla", "vencido_antes_de_t0", "LARGO_ENVIO", "ANCHO_ENVIO", "ALTO_ENVIO"]
                          + cols_cat + cols_num + (["ID_CLIENTE"] if CFG["USAR_VARIABLES_CLIENTE"] else [])))
    SLA_PRINCIPAL = ["SLA_UNICO"] if CFG["SOLO_SLA_UNICO"] else ["SLA_UNICO", "SLA_POR_REGLA"]
    filtros = [("SLA principal", "origen_sla IN (" + ", ".join(f"'{x}'" for x in SLA_PRINCIPAL) + ")"),
               ("Sin devolución ni siniestro", "estado_desenlace NOT IN ('DEVUELTO', 'SINIESTRADO')"),
               ("Desarrollo (T0 antes de la prueba final)", f"t0 < TIMESTAMP '{CFG['FIN_DESARROLLO']}'")]
    if CFG["EXCLUIR_ENTREGA_SIN_HORA"]:
        filtros.append(("Entrega con hora (no 00:00:00)", "fecha_entrega IS NULL OR fecha_entrega <> date_trunc('day', fecha_entrega)"))
    if CFG["usa_clima"]:
        filtros.append(("Clima completo", "COALESCE(CLIMA_COMPLETO, FALSE)"))
    G2 = f"read_parquet('{CFG['ruta_gold']}')"
    acumulado, filas = "TRUE", []
    for nombre, cond in filtros:                                  # tabla de exclusiones, solo conteos
        acumulado = f"{acumulado} AND ({cond})"
        filas.append({"paso": nombre, "guias": duckdb.sql(f"SELECT COUNT(*) FROM {G2} WHERE {acumulado}").fetchall()[0][0]})
    excl2 = pd.DataFrame(filas)
    d2 = duckdb.sql(f"SELECT {', '.join(columnas)} FROM {G2} WHERE {acumulado} ORDER BY t0").df()
    for c in d2.select_dtypes("float64").columns:
        d2[c] = d2[c].astype("float32")
    neg = d2.h_ultima_milla < 0
    d2.loc[neg, ["h_ultima_milla", "t_etiqueta_reg"]] = np.nan
    festivos = cargar_festivos(CFG["RUTA_CALENDARIO"])
    d2["franja"] = pd.cut(d2.t0.dt.hour, [-1, 5, 11, 17, 23], labels=["MADRUGADA", "MANANA", "TARDE", "NOCHE"]).astype(str)
    d2["dia_semana"] = d2.t0.dt.dayofweek.astype(str)
    d2["zona_franja"] = d2["categoria_zona"].astype(str) + "_" + d2["franja"]
    d2["dia"] = d2.t0.dt.date
    d2["es_festivo"] = d2.t0.dt.normalize().isin(festivos).astype("int8")
    vol = d2.LARGO_ENVIO * d2.ANCHO_ENVIO * d2.ALTO_ENVIO
    d2["volumen_declarado_cm3"] = vol.where(vol > 0)
    d2 = d2.drop(columns=["LARGO_ENVIO", "ANCHO_ENVIO", "ALTO_ENVIO"]); del vol
    if CFG["USAR_VARIABLES_CLIENTE"]:
        d2["ID_CLIENTE"] = d2["ID_CLIENTE"].astype(str)
    for c in CATEGORICAS: d2[c] = d2[c].astype(str)
    d2["vencida_en_t0"] = d2.vencido_antes_de_t0.fillna(False).astype(bool)
    gc.collect()
    display(excl2)
    print(f"Población ampliada (desarrollo): {len(d2):,} guías · vencidas en T0: {d2.vencida_en_t0.mean():.1%} · "
          f"memoria {d2.memory_usage(deep=True).sum() / 1e9:.1f} GB")

# COMMAND ----------

# S2.b Historia sin futuro con la población ampliada y validación de la regresión
if EJECUTAR_S2:
    t_ini = time.time()
    HIST2 = {ini: agregar_historia2(d2, ini, CFG["USAR_VARIABLES_CLIENTE"]) for ini in INICIOS}
    te_reg = dict((t, te) for t, _, te in TAREAS)["reg"]
    pl2 = list(pliegues_origen_movil(d2, CFG["INICIOS_VALIDACION"], CFG["FIN_DESARROLLO"], t_etiqueta=te_reg))
    s2 = validar(d2, HIST2, "reg", "h_ultima_milla", pl2, "S2_reg_todas")
    s2["bloque"] = pd.to_datetime(s2.bloque).dt.date
    s2 = s2.merge(d2[["guia", "vencida_en_t0"]], on="guia", how="left")
    del HIST2; gc.collect()
    s2["grupo"] = np.where(s2.vencida_en_t0, "vencidas en T0", "con margen en T0")
    print(f"S2: {time.time() - t_ini:.0f} s")

# COMMAND ----------

# S2.c Métricas por grupo y comparación con la base sobre las mismas guías con margen
if EJECUTAR_S2:
    tabla_s2 = pd.concat([tabla_metricas(s2.assign(grupo="todas"), ["version", "grupo", "tarea", "modelo"]),
                          tabla_metricas(s2, ["version", "grupo", "tarea", "modelo"]),
                          tabla_metricas(base[base.tarea == "reg"].assign(grupo="con margen en T0"),
                                         ["version", "grupo", "tarea", "modelo"])], ignore_index=True).round(4)
    con_margen = s2[s2.grupo == "con margen en T0"]
    dif_s2 = pd.DataFrame(comparar(con_margen, base[base.tarea == "reg"], "reg",
                                   "S2 (entrenado con todas) − base, en guías con margen")).round(4)
    tabla_s2.to_csv(f"{CARPETA_SALIDAS}P4c_S2_regresion_todas_metricas.csv", index=False)
    dif_s2.to_csv(f"{CARPETA_SALIDAS}P4c_S2_regresion_todas_diferencias.csv", index=False)
    excl2.to_csv(f"{CARPETA_SALIDAS}P4c_S2_exclusiones.csv", index=False)
    display(tabla_s2); display(dif_s2)

# COMMAND ----------

# 3. Lectura con la regla fijada
if EJECUTAR_S1:
    for r in dif_s1[dif_s1.modelo.isin([DEC["modelo_clf"], DEC["modelo_reg"]])].itertuples():
        peor = (r.diferencia > 0) if r.tarea == "reg" else (r.diferencia < 0)
        incluye_0 = r.ic95_inf <= 0 <= r.ic95_sup
        valor_base = tabla_s1[(tabla_s1.version == "base") & (tabla_s1.tarea == r.tarea) & (tabla_s1.modelo == r.modelo)]
        valor_base = float(valor_base["AP" if r.tarea == "clf" else "MAE_h"].iloc[0])
        pequeno = abs(r.diferencia) < 0.02 * valor_base
        lectura = ("se sostiene (sin diferencia o cambio menor al 2 %)" if incluye_0 or pequeno
                   else ("empeora: reportar y discutir con Iván" if peor else "mejora con el hueco: reportar"))
        print(f"S1 · {r.tarea} · {r.modelo}: {r.metrica} base {valor_base:.4f}, cambia {r.diferencia:+.4f} "
              f"[{r.ic95_inf:+.4f}; {r.ic95_sup:+.4f}] ({100 * r.diferencia / valor_base:+.1f} %) → {lectura}")
if EJECUTAR_S2:
    print("S2 (secundario):")
    display(tabla_s2[tabla_s2.modelo == DEC["modelo_reg"]][["version", "grupo", "n", "MAE_h", "MAE_10pct_mas_lento_h"]])