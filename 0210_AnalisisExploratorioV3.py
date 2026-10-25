# Databricks notebook source
# MAGIC %md
# MAGIC # 0210 · Análisis exploratorio v3 sobre la base por guía (OE1)
# MAGIC
# MAGIC **Qué hace.** Caracteriza los factores asociados al tiempo de última milla y al incumplimiento de SLA sobre la base v3 de `0330_BaseAnaliticaV3` (una fila por guía, T0 = primera salida a zona urbana). Reemplaza el exploratorio de `0200`, que usaba solo enero, la base antigua y el primer proceso 9 (realimentación S6).
# MAGIC
# MAGIC **Reglas que sigue.**
# MAGIC - **Solo periodo de desarrollo** (T0 antes de `FIN_DESARROLLO`): julio y agosto quedan reservados para la prueba final y no se miran aquí.
# MAGIC - **Cada tasa con su denominador** (`n`) y su intervalo de confianza del 95 % (Wilson). Los grupos con menos de `N_MIN` guías no se muestran.
# MAGIC - **Solo agregados**: no se muestra ninguna guía individual.
# MAGIC - Las asociaciones son **descriptivas, no causales**.
# MAGIC
# MAGIC **Requisitos.** `V3/GOLD_V3_ENVIO.parquet` aprobada en `0330_BaseAnaliticaV3`. Opcional: `GOLD_V3_ENVIO_CLIMA.parquet` de `0160` para la sección de clima.
# MAGIC
# MAGIC **Salidas.** CSV y PNG en `REVISION_2026_10/V3/EDA/`. Lo que se entrega está al final.

# COMMAND ----------

# MAGIC %md
# MAGIC > **Cambios frente a la v2 (21-10-2026).**
# MAGIC > - Lee la base v3 (SLA en días hábiles, una fila de RED por regla, devoluciones y siniestros aparte).
# MAGIC > - La población principal es `origen_sla` en `SLA_UNICO` o `SLA_POR_REGLA` (antes, solo `SLA_PARAMETRIZADO`), sin devoluciones ni siniestros. Solo `SLA_UNICO` queda como sensibilidad.
# MAGIC > - La celda de carga (sección 1) vuelve a estar activa: en la corrida del 20-10 quedó comentada y el cuaderno se detuvo con `NameError: dev`.
# MAGIC > - `cargar_festivos` es la versión que el equipo ajustó en `0410` (la columna de festivos ya trae las fechas), para que los dos cuadernos marquen los mismos días.
# MAGIC > - Se excluyen `guia` e `ID_CLIENTE` al cargar: este cuaderno no necesita identificadores.

# COMMAND ----------

# Si faltan librerías:
# %pip install duckdb matplotlib

# COMMAND ----------

# MAGIC %pip install "duckdb<2"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# CONFIGURACIÓN
import os, numpy as np, pandas as pd, duckdb, matplotlib.pyplot as plt
PATH_DATABRICKS = "/Volumes/proyectogrado/source/source/"
CARPETA_SALIDAS = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3/"   # v3: carpeta propia, no pisa la v2
CARPETA_EDA     = f"{CARPETA_SALIDAS}EDA/"
os.makedirs(CARPETA_EDA, exist_ok=True)
RUTA_GOLD_V3       = f"{CARPETA_SALIDAS}GOLD_V3_ENVIO.parquet"          # 0330
RUTA_GOLD_V3_CLIMA = f"{CARPETA_SALIDAS}GOLD_V3_ENVIO_CLIMA.parquet"    # 0160 (opcional)
RUTA_CALENDARIO    = f"{PATH_DATABRICKS}PARAMETRICAS/DIM.VW_TIEMPO_CALENDARIO_Y_FESTIVOS.parquet"

FIN_DESARROLLO = "2026-07-01"   # igual que en 0410: desde aquí es prueba final y no se explora
N_MIN = 30                      # grupos más pequeños no se muestran
SLA_PRINCIPAL = ["SLA_UNICO", "SLA_POR_REGLA"]   # v3: población principal; sensibilidad: solo SLA_UNICO
# Columnas del calendario: None = detección automática; si falla, escribir aquí sus nombres
COL_FECHA_CAL, COL_FESTIVO = None, None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funciones (no es necesario modificarlas)

# COMMAND ----------

def cargar_festivos(ruta, col_fecha=None, col_festivo=None):
    """Fechas festivas desde DIM.VW_TIEMPO_CALENDARIO_Y_FESTIVOS donde la columna de festivos ya contiene las fechas."""
    cal = pd.read_parquet(ruta)
    cols = list(cal.columns)
    
    if col_fecha is None:
        cand = [c for c in cols if "FECHA" in c.upper() and pd.api.types.is_datetime64_any_dtype(cal[c])] or \
               [c for c in cols if "FECHA" in c.upper()]
        col_fecha = cand[0] if cand else None
        
    if col_festivo is None:
        cand = [c for c in cols if "FESTIV" in c.upper() and "FECHA" not in c.upper()]
        if not cand:
            cand = [c for c in cols if "FESTIV" in c.upper()]
        col_festivo = cand[0] if cand else None
        
    if col_fecha is None or col_festivo is None:
        raise ValueError(f"No se identificaron fecha/festivo. Columnas: {cols}.")
    
    # Como la columna col_festivo ya contiene las fechas de los días festivos y NaT en los demás días,
    # solo necesitamos limpiar los valores nulos (NaT) y normalizar las fechas.
    v = cal[col_festivo]
    f = set(pd.to_datetime(v.dropna()).dt.normalize())
    
    assert len(f) > 0, f"El calendario no marca ningún festivo en la columna '{col_festivo}'"
    print(f"Festivos cargados exitosamente: {len(f)} (columnas: '{col_fecha}', '{col_festivo}')")
    return f

def wilson(k, n, z=1.96):
    k, n = np.asarray(k, float), np.asarray(n, float)
    p = np.divide(k, n, out=np.full_like(k, np.nan), where=n > 0)
    den = 1 + z**2 / n
    centro = (p + z**2 / (2 * n)) / den
    medio = z * np.sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / den
    return centro - medio, centro + medio

def perfil(df, por, n_min=N_MIN, orden=None):
    """Por grupo: guías, tasa de incumplimiento con IC95 (solo etiquetadas) y cuantiles de última milla."""
    g = df.groupby(por, observed=True, dropna=False)
    t = pd.DataFrame({
        "guias": g.size(),
        "n_etiquetadas": g["incumplimiento_sla"].count(),
        "incumplidas": g["incumplimiento_sla"].sum(),
        "n_con_ultima_milla": g["h_ultima_milla"].count(),
        "mediana_h": g["h_ultima_milla"].median(),
        "p90_h": g["h_ultima_milla"].quantile(0.9),
    })
    t["tasa_incumplimiento"] = t.incumplidas / t.n_etiquetadas
    t["ic95_inf"], t["ic95_sup"] = wilson(t.incumplidas, t.n_etiquetadas)
    t["pct_guias"] = t.guias / t.guias.sum()
    t = t[t.guias >= n_min]
    t = t.sort_index() if orden is None else t.sort_values(orden, ascending=False)
    return t.round(4)

def guardar(t, nombre):
    t.to_csv(f"{CARPETA_EDA}{nombre}.csv")

def grafico_tasa(t, titulo, nombre, rot=0):
    fig, ax = plt.subplots(figsize=(max(6, 0.45 * len(t) + 2), 3.6))
    x = np.arange(len(t))
    ax.bar(x, t.tasa_incumplimiento, color="#4C78A8")
    ax.errorbar(x, t.tasa_incumplimiento, yerr=[t.tasa_incumplimiento - t.ic95_inf, t.ic95_sup - t.tasa_incumplimiento],
                fmt="none", ecolor="#333", capsize=3, lw=1)
    ax.set_xticks(x); ax.set_xticklabels([str(i) for i in t.index], rotation=rot, ha="right" if rot else "center")
    ax.set_ylabel("Tasa de incumplimiento"); ax.set_title(titulo); ax.set_ylim(0, 1)
    for xi, n in zip(x, t.n_etiquetadas):
        ax.text(xi, 0.02, f"n={int(n):,}", ha="center", va="bottom", fontsize=7, rotation=90, color="white")
    plt.tight_layout(); fig.savefig(f"{CARPETA_EDA}{nombre}.png", dpi=130); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carga y variables conocidas en T0

# COMMAND ----------

festivos = cargar_festivos(RUTA_CALENDARIO, COL_FECHA_CAL, COL_FESTIVO)
usa_clima = os.path.exists(RUTA_GOLD_V3_CLIMA)
ruta = RUTA_GOLD_V3_CLIMA if usa_clima else RUTA_GOLD_V3
todo = duckdb.sql(f"SELECT * EXCLUDE (guia, ID_CLIENTE) FROM read_parquet('{ruta}')").df()   # sin identificadores
for c in ["t0", "FECHA_ENVIO", "fecha_entrega"]:
    todo[c] = pd.to_datetime(todo[c])
todo["entrega_sin_hora"] = todo.fecha_entrega.notna() & (todo.fecha_entrega == todo.fecha_entrega.dt.normalize())
todo["mes_t0"] = todo.t0.dt.to_period("M").astype(str)
todo["hora_t0"] = todo.t0.dt.hour
todo["franja"] = pd.cut(todo.hora_t0, [-1, 5, 11, 17, 23], labels=["MADRUGADA", "MANANA", "TARDE", "NOCHE"]).astype(str)
todo["dia_semana"] = todo.t0.dt.dayofweek.map(dict(enumerate(["1 lun", "2 mar", "3 mié", "4 jue", "5 vie", "6 sáb", "7 dom"])))
todo["festivo_t0"] = todo.t0.dt.normalize().isin(festivos).map({True: "festivo", False: "no festivo"})
todo["piezas_cat"] = pd.cut(todo.piezas_declaradas, [0, 1, 2, 5, np.inf], labels=["1", "2", "3-5", "6+"]).astype(str)
todo["peso_decil"] = pd.qcut(todo.peso_declarado_kg.rank(method="first"), 10, labels=[f"D{i}" for i in range(1, 11)]).astype(str)
todo["margen_cat"] = pd.cut(todo.margen_sla_en_t0_h, [-np.inf, 0, 6, 12, 24, 48, np.inf],
                            labels=["<0 (vencido)", "0-6", "6-12", "12-24", "24-48", ">48"]).astype(str)
print(f"Base completa: {len(todo):,} guías; T0 de {todo.t0.min()} a {todo.t0.max()}; clima: {usa_clima}")
dev = todo[todo.t0 < pd.Timestamp(FIN_DESARROLLO)].copy()
print(f"Periodo de desarrollo (T0 < {FIN_DESARROLLO}): {len(dev):,} guías")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Exclusiones y población de análisis
# MAGIC
# MAGIC Cada motivo se cuenta sobre el periodo de desarrollo y **con su tasa de incumplimiento**, para ver si excluir cambia la población. Los motivos no son excluyentes: una guía puede tener varios.

# COMMAND ----------

p999 = dev.h_ultima_milla.quantile(0.999)
motivos = {
    "SLA fuera del principal (sustitución o sin SLA)": ~dev.origen_sla.isin(SLA_PRINCIPAL),
    "Devolución o siniestro (desenlace aparte)": dev.estado_desenlace.isin(["DEVUELTO", "SINIESTRADO"]),
    "SLA por regla (clave de RED con plazos distintos; sensibilidad)": dev.origen_sla == "SLA_POR_REGLA",
    "Etiqueta no identificable (abierto sin vencer o sin SLA)": dev.incumplimiento_sla.isna(),
    "Ya vencido antes de T0": dev.vencido_antes_de_t0.fillna(False).astype(bool),
    "Entrega sin hora (00:00:00)": dev.entrega_sin_hora,
    "Última milla negativa": dev.h_ultima_milla < 0,
    f"Última milla > p99,9 ({p999:,.0f} h)": dev.h_ultima_milla > p999,
    "Re-despacho (más de una salida a zona)": dev.n_salidas_zona > 1,
}
filas = []
for m, mask in motivos.items():
    sub = dev[mask]
    filas.append({"motivo": m, "guias": int(mask.sum()), "pct": mask.mean(),
                  "tasa_incumplimiento": sub.incumplimiento_sla.mean(), "n_etiquetadas": sub.incumplimiento_sla.count()})
excl = pd.DataFrame(filas).set_index("motivo").round(4)
display(excl); guardar(excl, "01_exclusiones")

poblacion = (dev.origen_sla.isin(SLA_PRINCIPAL) & ~dev.estado_desenlace.isin(["DEVUELTO", "SINIESTRADO"])
             & ~dev.vencido_antes_de_t0.fillna(False).astype(bool) & ~dev.entrega_sin_hora)
d = dev[poblacion].copy()
d.loc[d.h_ultima_milla < 0, "h_ultima_milla"] = np.nan          # negativas: se auditan, no entran a los cuantiles
print(f"Población de análisis principal: {len(d):,} guías ({len(d)/len(dev):.1%} del desarrollo); "
      f"etiquetadas: {d.incumplimiento_sla.count():,}; prevalencia: {d.incumplimiento_sla.mean():.3f}")
# Sensibilidad: la misma población con solo SLA_UNICO
sens_unico = dev[poblacion & (dev.origen_sla == "SLA_UNICO")]
print(f"Sensibilidad solo SLA_UNICO: {len(sens_unico):,} guías; prevalencia {sens_unico.incumplimiento_sla.mean():.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Distribución de las variables objetivo

# COMMAND ----------

q = [0.01, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99, 0.999]
cuant = pd.DataFrame({
    "h_ultima_milla (T0 a entrega)": d.h_ultima_milla.quantile(q),
    "h_admision_a_t0 (tránsito previo)": d.h_admision_a_t0.quantile(q),
    "h_total (admisión a entrega)": d.h_total.quantile(q),
    "sla_h": d.sla_h.quantile(q)}).round(2)
cuant.index = [f"q{x:g}" for x in q]
display(cuant); guardar(cuant, "02_cuantiles_tiempos")
estado = d.groupby("estado_desenlace").agg(guias=("estado_desenlace", "size"), prevalencia=("incumplimiento_sla", "mean"))
display(estado); guardar(estado, "02b_estado_desenlace")

fig, axs = plt.subplots(1, 2, figsize=(12, 3.6))
x = d.h_ultima_milla.dropna(); x = x[x <= x.quantile(0.99)]
axs[0].hist(x, bins=60, color="#4C78A8"); axs[0].set_title("Horas de T0 a la entrega (hasta p99)"); axs[0].set_xlabel("horas")
axs[0].axvline(x.median(), color="k", ls="--", lw=1); axs[0].text(x.median(), axs[0].get_ylim()[1]*.9, f" mediana {x.median():.1f} h")
m = d.groupby("mes_t0").agg(n=("incumplimiento_sla", "count"), k=("incumplimiento_sla", "sum"), med=("h_ultima_milla", "median"))
lo, hi = wilson(m.k, m.n)
axs[1].errorbar(m.index, m.k / m.n, yerr=[m.k / m.n - lo, hi - m.k / m.n], marker="o", capsize=3)
axs[1].set_title("Tasa de incumplimiento por mes de T0"); axs[1].set_ylim(0, 1)
plt.tight_layout(); fig.savefig(f"{CARPETA_EDA}03_distribucion_y_mes.png", dpi=130); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Perfiles por factor
# MAGIC
# MAGIC Factores **temporales**, **espaciales**, **de operación** y **de servicio** conocidos en T0. Cada tabla trae guías, tasa con IC 95 %, mediana y percentil 90 de la última milla.

# COMMAND ----------

FACTORES = {
    "temporal · mes de T0": "mes_t0", "temporal · día de la semana": "dia_semana", "temporal · franja de T0": "franja",
    "temporal · hora de T0": "hora_t0", "temporal · festivo": "festivo_t0",
    "espacial · categoría de zona": "categoria_zona", "espacial · regional de origen": "regional_origen",
    "operación · piezas declaradas": "piezas_cat", "operación · decil de peso": "peso_decil",
    "operación · re-despachos": "n_salidas_zona",
    "servicio · producto": "ID_PRODUCTO", "servicio · tipo de guía": "ID_TIPOGUIA",
    "servicio · SLA comprometido (h)": "sla_h", "servicio · margen de SLA en T0 (h)": "margen_cat"}
perfiles = {}
for i, (titulo, col) in enumerate(FACTORES.items(), start=1):
    t = perfil(d, col); perfiles[col] = t
    print(titulo); display(t)
    guardar(t, f"04_{i:02d}_{col}")
    if 1 < len(t) <= 25:
        grafico_tasa(t, titulo, f"04_{i:02d}_{col}", rot=45 if len(t) > 8 else 0)

# COMMAND ----------

# Zonas urbanas: las 15 de mayor y menor tasa (con n >= N_MIN)
z = perfil(d, "zona_urbana", orden="tasa_incumplimiento")
display(pd.concat([z.head(15), z.tail(15)])); guardar(z, "04_zonas_urbanas")
print(f"{len(z)} zonas con al menos {N_MIN} guías; rango de tasas {z.tasa_incumplimiento.min():.2f}–{z.tasa_incumplimiento.max():.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Interacción zona × franja (mediana de horas de última milla)

# COMMAND ----------

tab = d.pivot_table(index="categoria_zona", columns="franja", values="h_ultima_milla", aggfunc="median", observed=True)
cnt = d.pivot_table(index="categoria_zona", columns="franja", values="h_ultima_milla", aggfunc="count", observed=True)
tab = tab.where(cnt >= N_MIN)
display(tab.round(1)); guardar(tab.round(2), "05_zona_x_franja_mediana")
fig, ax = plt.subplots(figsize=(6, 0.5 * len(tab) + 1.5))
im = ax.imshow(tab.values, cmap="Blues", aspect="auto")
ax.set_xticks(range(tab.shape[1])); ax.set_xticklabels(tab.columns); ax.set_yticks(range(tab.shape[0])); ax.set_yticklabels(tab.index)
for i in range(tab.shape[0]):
    for j in range(tab.shape[1]):
        if pd.notna(tab.values[i, j]): ax.text(j, i, f"{tab.values[i, j]:.1f}", ha="center", va="center", fontsize=8)
plt.colorbar(im, label="mediana (h)"); ax.set_title("Última milla: categoría de zona × franja de T0")
plt.tight_layout(); fig.savefig(f"{CARPETA_EDA}05_zona_x_franja.png", dpi=130); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Clima previo a T0 (solo si existe la base de 0160)

# COMMAND ----------

if usa_clima:
    dc = d[d.CLIMA_COMPLETO.fillna(False).astype(bool)].copy()
    dc["lluvia_24h_cat"] = pd.cut(dc.LLUVIA_24H_PREV, [-0.01, 0.0, 1, 5, 15, np.inf],
                                  labels=["0 mm", "0-1", "1-5", "5-15", ">15"]).astype(str)
    dc["lluvia_1h_cat"] = pd.cut(dc.PRECIP_PREV_1H, [-0.01, 0.0, 0.5, 2, np.inf], labels=["0 mm", "0-0,5", "0,5-2", ">2"]).astype(str)
    for col in ["lluvia_24h_cat", "lluvia_1h_cat"]:
        t = perfil(dc, col); display(t); guardar(t, f"06_{col}")
        grafico_tasa(t, f"Clima previo a T0 · {col}", f"06_{col}")
    print(f"Guías con clima completo: {len(dc):,} de {len(d):,}. Lectura descriptiva: el clima es reanálisis retrospectivo.")
else:
    print("No existe la base con clima de 0160: sección omitida.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Ordenamiento de factores por su relación con el retraso
# MAGIC
# MAGIC Medidas univariadas y descriptivas, comparables entre factores:
# MAGIC - **Incumplimiento:** AUC de la tasa del grupo usada como puntaje (0,5 = sin relación). Los grupos con menos de `N_MIN` se agrupan en «OTROS».
# MAGIC - **Última milla:** razón de correlación η² (proporción de la varianza de las horas explicada por el grupo).
# MAGIC
# MAGIC Para las variables continuas se usan sus categorías. No es una medida de importancia en un modelo multivariado (eso es 0420).

# COMMAND ----------

from sklearn.metrics import roc_auc_score
def eta2(y, g):
    m = y.groupby(g, observed=True).transform("mean")
    return float(((m - y.mean())**2).sum() / ((y - y.mean())**2).sum())
filas = []
candidatas = list(FACTORES.values()) + ["zona_urbana"] + (["lluvia_24h_cat"] if usa_clima else [])
base = dc if usa_clima else d
for col in candidatas:
    src = base if col.startswith("lluvia") else d
    g = src[col].astype(str)
    frec = g.map(g.value_counts()); g = g.where(frec >= N_MIN, "OTROS")
    e = src.incumplimiento_sla.notna()
    tasa = src.loc[e, "incumplimiento_sla"].groupby(g[e]).transform("mean")
    auc = roc_auc_score(src.loc[e, "incumplimiento_sla"], tasa) if src.loc[e, "incumplimiento_sla"].nunique() == 2 else np.nan
    r = src.h_ultima_milla.notna()
    filas.append({"factor": col, "categorias": g.nunique(), "AUC_incumplimiento": auc,
                  "eta2_ultima_milla": eta2(src.loc[r, "h_ultima_milla"], g[r]), "guias": len(src)})
rank = pd.DataFrame(filas).set_index("factor").sort_values("AUC_incumplimiento", ascending=False).round(4)
display(rank); guardar(rank, "07_ranking_factores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo leer y qué entregar
# MAGIC
# MAGIC - `01_exclusiones`: si un motivo tiene una tasa de incumplimiento muy distinta de la general, excluirlo cambia la población; debe decirse en 4.2.2.
# MAGIC - `02_cuantiles_tiempos`: cifra para el documento («la mediana de última milla es X h»), siempre con su `n`.
# MAGIC - `04_*` y `05_*`: tablas del OE1 con denominador e IC. No interpretar diferencias cuyos IC se solapan.
# MAGIC - `07_ranking_factores`: insumo para la discusión del OE1 y para comparar con la interpretabilidad del modelo (0420).
# MAGIC
# MAGIC **Entregar:** la carpeta `EDA/` completa (CSV y PNG). Ninguna salida contiene guías individuales.