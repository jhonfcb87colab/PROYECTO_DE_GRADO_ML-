"""Funciones auxiliares de los cuadernos de modelado (0411 a 0416).

Los cuadernos solo llaman a estas funciones; aquí no hay rutas, datos ni configuración.
Todo lo que se necesita se pasa como argumento.

Secciones:
  A. Funciones del asesor (copiadas sin cambios de 0410_EvaluacionTemporal)
  B. Datos: festivos, variables históricas, armado de cada pliegue
  C. Modelos: preprocesamiento, construcción, predicción, búsqueda de hiperparámetros
  D. Métricas, segmentos, calibración y comparaciones con IC
  E. Manifiesto
  F. Archivos compartidos entre cuadernos (preparación y resultados de cada modelo)
"""
import os, glob, json, datetime
import numpy as np, pandas as pd
import lightgbm as lgb, xgboost as xgb
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.inspection import permutation_importance
from sklearn.metrics import (mean_absolute_error, average_precision_score, roc_auc_score, brier_score_loss,
                             log_loss)


# =====================================================================================
# A. FUNCIONES DEL ASESOR (copiadas sin cambios de 0410_EvaluacionTemporal)
# =====================================================================================

# ---------- 1. Media histórica por grupo usando SOLO desenlaces conocidos antes de T0 ----------
def media_historica_sin_futuro(df, grupo, objetivo, t_pred="t0", t_desenlace="t_etiqueta_reg",
                               suavizado=20, historia_hasta=None):
    """Para cada fila: media suavizada del objetivo en su grupo, con las guías cuyo desenlace
    se conoció ESTRICTAMENTE antes del T0 de la fila. historia_hasta=<corte> congela la historia
    (agregado fijo, como si se hubiera calculado al cierre del entrenamiento); None = se actualiza
    con los desenlaces que van madurando (simulación de uso en línea).
    Devuelve (codificación, n_historia). Sin historia global -> NaN (periodo inicial)."""
    h = df.loc[df[objetivo].notna() & df[t_desenlace].notna(), [grupo, t_desenlace, objetivo]]
    if historia_hasta is not None:
        h = h[h[t_desenlace] < pd.Timestamp(historia_hasta)]
    h = h.sort_values(t_desenlace).rename(columns={t_desenlace: "_t"})
    h["_t"] = h["_t"].astype("datetime64[ns]")                   # misma precisión en ambos lados
    h["_n"] = h.groupby(grupo).cumcount() + 1
    h["_s"] = h.groupby(grupo)[objetivo].cumsum()
    hg = h[["_t", objetivo]].copy()
    hg["_ng"] = np.arange(1, len(hg) + 1); hg["_sg"] = hg[objetivo].cumsum()

    q = df[[grupo, t_pred]].copy(); q["_fila"] = np.arange(len(df))
    q[t_pred] = q[t_pred].astype("datetime64[ns]"); q = q.sort_values(t_pred)
    m = pd.merge_asof(q, h[[grupo, "_t", "_n", "_s"]], left_on=t_pred, right_on="_t", by=grupo,
                      allow_exact_matches=False, direction="backward")
    m = pd.merge_asof(m.sort_values(t_pred), hg[["_t", "_ng", "_sg"]].rename(columns={"_t": "_tg"}),
                      left_on=t_pred, right_on="_tg", allow_exact_matches=False, direction="backward")
    m = m.sort_values("_fila")
    prior = m["_sg"] / m["_ng"]
    n, s = m["_n"].fillna(0), m["_s"].fillna(0)
    enc = (s + suavizado * prior) / (n + suavizado)
    return enc.to_numpy(), n.to_numpy()

# ---------- 2. Pliegues de origen móvil por bloques de calendario ----------
def pliegues_origen_movil(df, inicios_validacion, fin_desarrollo, t_pred="t0",
                          t_etiqueta="t_etiqueta_clf"):
    """inicios_validacion: lista de fechas (inicio de cada bloque de validación).
    Entrena con T0 < inicio y SOLO con etiquetas ya conocidas en el inicio (madurez);
    valida con T0 en [inicio, siguiente inicio). Nunca toca la prueba final (> fin_desarrollo)."""
    limites = [pd.Timestamp(x) for x in inicios_validacion] + [pd.Timestamp(fin_desarrollo)]
    for ini, fin in zip(limites[:-1], limites[1:]):
        tr = (df[t_pred] < ini) & (df[t_etiqueta] < ini)
        va = (df[t_pred] >= ini) & (df[t_pred] < fin)
        yield ini, np.flatnonzero(tr), np.flatnonzero(va)

# ---------- 3. Referencias constantes estimadas SOLO con entrenamiento ----------
def referencias_probabilidad(y_train, y_eval):
    p = float(np.mean(y_train))
    pc = np.full(len(y_eval), p)
    return {"p_constante_train": p,
            "Brier_ref": brier_score_loss(y_eval, pc),
            "LogLoss_ref": log_loss(y_eval, pc, labels=[0, 1]),
            "AP_ref (= prevalencia evaluada)": float(np.mean(y_eval))}

# ---------- 4. Tabla de umbrales en VALIDACIÓN (no en prueba) ----------
def tabla_umbrales(y, p, dia, umbrales=np.round(np.arange(0.05, 0.96, 0.05), 2)):
    filas, n_dias = [], len(np.unique(dia))
    for u in umbrales:
        a = p >= u
        tp = int((a & (y == 1)).sum()); fp = int((a & (y == 0)).sum()); fn = int((~a & (y == 1)).sum())
        filas.append({"umbral": u, "alertas_por_dia": a.sum() / n_dias,
                      "exhaustividad": tp / max(tp + fn, 1), "precision": tp / max(tp + fp, 1),
                      "incumplimientos_omitidos": fn, "falsas_alertas": fp})
    return pd.DataFrame(filas)

def umbral_por_capacidad(p, dia, alertas_dia):
    """Umbral tal que, en promedio, se emitan a lo sumo 'alertas_dia' alertas por día (escenario,
    no costo oficial). Se elige en validación y se congela antes de la prueba final."""
    n_dias = len(np.unique(dia))
    k = int(alertas_dia * n_dias)
    return np.inf if k <= 0 else np.sort(p)[::-1][min(k, len(p)) - 1]

# ---------- 5. Diferencia entre dos modelos con bootstrap por bloques de día ----------
def diferencia_por_bloques(y, pred_a, pred_b, dia, metrica, B=2000, semilla=42):
    """metrica(y, pred) -> float. Remuestrea DÍAS completos (respeta la dependencia dentro de la
    jornada, del manifiesto y de la ruta). Incertidumbre de modelos FIJOS sobre el periodo evaluado."""
    y, pred_a, pred_b, dia = map(np.asarray, (y, pred_a, pred_b, dia))
    dias = np.unique(dia); idx = {d: np.flatnonzero(dia == d) for d in dias}
    rng = np.random.default_rng(semilla)
    obs = metrica(y, pred_a) - metrica(y, pred_b)
    reps = []
    for _ in range(B):
        ii = np.concatenate([idx[d] for d in rng.choice(dias, size=len(dias), replace=True)])
        reps.append(metrica(y[ii], pred_a[ii]) - metrica(y[ii], pred_b[ii]))
    lo, hi = np.percentile(reps, [2.5, 97.5])
    return {"diferencia": obs, "ic95_inf": lo, "ic95_sup": hi, "dias": len(dias)}


# =====================================================================================
# B. DATOS
# =====================================================================================

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

def tabla_exclusiones(todo, pasos):
    """Aplica los filtros en orden y cuenta cuántas guías salen en cada paso.
    pasos: lista de (nombre, máscara booleana). Devuelve (tabla, máscara final)."""
    mask, filas = pd.Series(True, index=todo.index), []
    for nombre, m in pasos:
        previo = mask.copy(); mask = mask & m
        filas.append({"paso": nombre, "guias": int(mask.sum()), "excluidas_en_el_paso": int((previo & ~m).sum()),
                      "tasa_incumplimiento_excluidas": todo.loc[previo & ~m, "incumplimiento_sla"].mean()})
    return pd.DataFrame(filas), mask

def agregar_historia2(d, historia_hasta, usar_variables_cliente):
    """Agregados sin futuro por zona×franja (función del asesor) y por cliente; congelados en 'historia_hasta'."""
    h = {}
    h["hist_h_zona_franja"], h["n_hist"] = media_historica_sin_futuro(d, "zona_franja", "h_ultima_milla", "t0", "t_etiqueta_reg", historia_hasta=historia_hasta)
    h["hist_inc_zona_franja"], _ = media_historica_sin_futuro(d, "zona_franja", "incumplimiento_sla", "t0", "t_etiqueta_clf", historia_hasta=historia_hasta)
    if usar_variables_cliente:
        h["hist_h_cliente"], h["n_hist_cliente"] = media_historica_sin_futuro(d, "ID_CLIENTE", "h_ultima_milla", "t0", "t_etiqueta_reg", historia_hasta=historia_hasta)
        h["hist_inc_cliente"], _ = media_historica_sin_futuro(d, "ID_CLIENTE", "incumplimiento_sla", "t0", "t_etiqueta_clf", historia_hasta=historia_hasta)
    return h

def con_historia(df, h):
    """Copia de df con las columnas históricas de un pliegue."""
    d = df.copy()
    for k, v in h.items(): d[k] = v
    return d

def partes_del_bloque(df, h, y_col, tr, va, columnas):
    """Entrenamiento y validación de un bloque, listos para ajustar: (Dva, Xtr, ytr, Xva, yva).
    Dva es la tabla completa de validación (para guardar guía, día, zona y franja)."""
    d, tr, va = datos_del_pliegue(df, h, y_col, tr, va)
    Dtr, Dva = d.iloc[tr], d.iloc[va]
    return Dva, Dtr[columnas], Dtr[y_col], Dva[columnas], Dva[y_col]

def marco_predicciones(version, tarea, ini, Dva, yva):
    """Columnas fijas de la tabla de predicciones de validación (falta 'modelo' y 'pred')."""
    return pd.DataFrame({"version": version, "tarea": tarea, "bloque": ini.date(), "guia": Dva["guia"].values,
                         "dia": Dva["dia"].values, "y": yva.values, "categoria_zona": Dva["categoria_zona"].values,
                         "franja": Dva["franja"].values})

def datos_del_pliegue(df, h, y_col, tr, va):
    """Tabla con la historia del pliegue y sus índices de entrenamiento y validación,
    sin las filas cuyo objetivo es nulo. Devuelve (d, tr, va)."""
    d = con_historia(df, h)
    tr = tr[d[y_col].iloc[tr].notna().to_numpy()]
    va = va[d[y_col].iloc[va].notna().to_numpy()]
    return d, tr, va


# =====================================================================================
# C. MODELOS
# =====================================================================================

def prep2(numericas, categoricas):
    return ColumnTransformer([
        ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")),
                          ("ohe", OneHotEncoder(handle_unknown="ignore", min_frequency=0.01))]), categoricas),
        ("num", Pipeline([("imp", SimpleImputer(strategy="median")), ("sc", StandardScaler())]), numericas)])

def construir_modelo(tarea, nombre, numericas, categoricas, params, semilla):
    """Pipeline (preprocesamiento + modelo). tarea: 'clf' o 'reg'. nombre: Lineal, LightGBM, XGBoost o RandomForest."""
    p = dict(params)
    obj = p.pop("objetivo_reg", None)
    if nombre == "Lineal":
        m = LogisticRegression(max_iter=1000) if tarea == "clf" else LinearRegression()
    elif nombre == "LightGBM":
        p.setdefault("subsample_freq", 1)
        m = lgb.LGBMClassifier(**p, random_state=semilla, verbose=-1) if tarea == "clf" else \
            lgb.LGBMRegressor(**p, objective=obj or "regression", random_state=semilla, verbose=-1)
    elif nombre == "XGBoost":
        m = xgb.XGBClassifier(**p, tree_method="hist", eval_metric="logloss", random_state=semilla, n_jobs=-1) if tarea == "clf" else \
            xgb.XGBRegressor(**p, tree_method="hist", objective=obj or "reg:squarederror", random_state=semilla, n_jobs=-1)
    elif nombre == "RandomForest":
        m = RandomForestClassifier(**p, random_state=semilla, n_jobs=-1) if tarea == "clf" else \
            RandomForestRegressor(**p, random_state=semilla, n_jobs=-1)
    return Pipeline([("prep", prep2(numericas, categoricas)), ("modelo", m)])

def predecir(m, X, tarea):
    """Probabilidad de incumplir (clf) u horas no negativas (reg)."""
    return m.predict_proba(X)[:, 1] if tarea == "clf" else np.maximum(m.predict(X), 0)

def muestrear_configs(espacio, n, semilla):
    """n configuraciones al azar del espacio de búsqueda {hiperparámetro: [valores]}."""
    rng = np.random.default_rng(semilla)
    return [{k: v[rng.integers(len(v))] for k, v in espacio.items()} for _ in range(n)]

def puntaje_config(tarea, nombre, cfg, k, pliegues, hist, df, y_col, categoricas, numericas, max_filas, semilla):
    """Entrena una configuración en cada pliegue (con submuestra de hasta max_filas) y devuelve
    la métrica de cada bloque de validación: AP en clasificación, MAE en regresión."""
    vals = []
    for ini, tr, va in pliegues:
        d, tr, va = datos_del_pliegue(df, hist[ini], y_col, tr, va)
        if len(tr) > max_filas:
            tr = np.sort(np.random.default_rng(semilla + k).choice(tr, max_filas, replace=False))
        m = construir_modelo(tarea, nombre, numericas, categoricas, cfg, semilla)
        m.fit(d.iloc[tr][categoricas + numericas], d.iloc[tr][y_col])
        p = predecir(m, d.iloc[va][categoricas + numericas], tarea)
        vals.append(average_precision_score(d.iloc[va][y_col], p) if tarea == "clf"
                    else mean_absolute_error(d.iloc[va][y_col], p))
    return vals

def mejores(tabla, espacios):
    """Mejor configuración por versión, tarea y modelo (mayor AP o menor MAE promedio)."""
    out = {}
    for (v, t, m), g in tabla.groupby(["version", "tarea", "modelo"]):
        g = g.sort_values("promedio", ascending=(t == "reg"))
        fila = g.iloc[0]
        out[f"{v}|{t}|{m}"] = {k[2:]: (None if (isinstance(val, float) and np.isnan(val)) else
                                      (val.item() if hasattr(val, "item") else val))
                               for k, val in fila.items() if k.startswith("p_") and k[2:] in espacios[m]}
    return out

def params_de(mejores_params, params_base, version, tarea, nombre):
    """Hiperparámetros a usar: los mejores de la búsqueda o, si no hay, los de base.
    'internas+clima' usa la MISMA configuración que 'internas'."""
    if nombre == "Lineal": return {}
    v = "internas+clima_reajustada" if version == "internas+clima_reajustada" else "internas"
    p = mejores_params.get(f"{v}|{tarea}|{nombre}")
    p = dict(params_base[nombre]) if p is None else dict(p)
    for k in ("n_estimators", "num_leaves", "min_child_samples", "min_child_weight", "min_samples_leaf", "max_depth"):
        if k in p and p[k] is not None: p[k] = int(p[k])
    if isinstance(p.get("max_features"), str) and p["max_features"] != "sqrt": p["max_features"] = float(p["max_features"])
    if tarea == "clf": p.pop("objetivo_reg", None)
    return p

def importancia_pliegue(m, Xva, yva, tarea, n_filas, semilla, etiquetas):
    """Importancia por permutación sobre una muestra del bloque de validación (insumo de 0420).
    etiquetas: dict con las columnas fijas que encabezan la tabla (tarea, modelo, bloque)."""
    s = Xva.sample(min(n_filas, len(Xva)), random_state=semilla)
    r = permutation_importance(m, s, yva.loc[s.index], n_repeats=3, random_state=semilla, n_jobs=1,
                               scoring="average_precision" if tarea == "clf" else "neg_mean_absolute_error")
    return pd.DataFrame({**etiquetas, "variable": s.columns, "importancia": r.importances_mean, "desv": r.importances_std})


# =====================================================================================
# D. MÉTRICAS
# =====================================================================================

def metricas(tarea, y, p):
    y, p = np.asarray(y, float), np.asarray(p, float)
    if tarea == "clf":
        return {"n": len(y), "prevalencia": y.mean(), "AP": average_precision_score(y, p), "AUC_ROC": roc_auc_score(y, p),
                "Brier": brier_score_loss(y, p), "LogLoss": log_loss(y, np.clip(p, 1e-6, 1 - 1e-6), labels=[0, 1])}
    e = np.abs(y - p); lento = y >= np.quantile(y, 0.9)
    return {"n": len(y), "MAE_h": e.mean(), "RMSE_h": np.sqrt(np.mean((y - p) ** 2)), "P90_error_abs_h": np.percentile(e, 90),
            "MAE_10pct_mas_lento_h": e[lento].mean(), "MAPE_pct_y>=1h": 100 * np.mean(e[y >= 1] / y[y >= 1]),
            "cobertura_MAPE": np.mean(y >= 1)}

def tabla_metricas(pred, por):
    """Métricas de cada grupo de 'pred' (columnas version, tarea, modelo, ...)."""
    return pd.DataFrame([{**dict(zip(por, claves)), **metricas(claves[1], g.y, g.pred)}
                         for claves, g in pred.groupby(por)])

def metricas_por_segmento(pr, seg_cols):
    filas = []
    for tarea in pr.tarea.unique():
        for col in seg_cols:
            sub = pr[pr.tarea == tarea]
            for (modelo, valor), g in sub.groupby(["modelo", col], observed=True):
                if len(g) < 30 or (tarea == "clf" and g.y.nunique() < 2): continue
                m = metricas(tarea, g.y, g.pred)
                filas.append({"tarea": tarea, "segmento": col, "valor": valor, "modelo": modelo, **m})
    return pd.DataFrame(filas)

def diferencias(pr, version_a, modelo_a, version_b, modelo_b, etiqueta, B, semilla):
    """Diferencia de métrica (AP o MAE) entre dos modelos sobre las mismas guías, con IC 95 % por días."""
    filas = []
    for tarea, metrica in [("clf", average_precision_score), ("reg", mean_absolute_error)]:
        a = pr[(pr.version == version_a) & (pr.tarea == tarea) & (pr.modelo == modelo_a)].sort_values(["bloque", "guia"])
        b = pr[(pr.version == version_b) & (pr.tarea == tarea) & (pr.modelo == modelo_b)].sort_values(["bloque", "guia"])
        assert (a.guia.values == b.guia.values).all(), "Las dos versiones no tienen las mismas guías"
        filas.append({"tarea": tarea, "metrica": "AP" if tarea == "clf" else "MAE_h", "comparacion": etiqueta(tarea),
                      **diferencia_por_bloques(a.y.values, a.pred.values, b.pred.values, a.dia.values, metrica, B=B, semilla=semilla)})
    return filas

def calibracion_tabla(y, p, bins=10):
    q = pd.qcut(pd.Series(p).rank(method="first"), bins, labels=False)
    return pd.DataFrame({"y": y, "p": p, "bin": q}).groupby("bin").agg(n=("y", "size"), p_media=("p", "mean"), y_obs=("y", "mean"))

def calibracion_cruzada(pr_modelo):
    """Isotónica ajustada con los bloques ANTERIORES y aplicada al siguiente (sin mirar el bloque evaluado)."""
    bloques = sorted(pr_modelo.bloque.unique()); out = []
    for i in range(1, len(bloques)):
        ant = pr_modelo[pr_modelo.bloque.isin(bloques[:i])]; act = pr_modelo[pr_modelo.bloque == bloques[i]]
        iso = IsotonicRegression(out_of_bounds="clip", y_min=0, y_max=1).fit(ant.pred, ant.y)
        out.append(act.assign(pred_cal=iso.predict(act.pred)))
    return pd.concat(out) if out else None


# =====================================================================================
# E. MANIFIESTO
# =====================================================================================

def huella(patron):
    """Nombre, tamaño y fecha de modificación de los archivos que coinciden con el patrón."""
    return [{"archivo": f, "bytes": os.path.getsize(f), "modificado": datetime.datetime.fromtimestamp(os.path.getmtime(f)).isoformat()}
            for f in sorted(glob.glob(patron))]


# =====================================================================================
# F. ARCHIVOS COMPARTIDOS ENTRE CUADERNOS
# =====================================================================================

def guardar_preparacion(carpeta, df, hist, hist_final, config):
    """0411 guarda: base de modelado, historia de cada bloque, historia congelada para la prueba y configuración."""
    os.makedirs(carpeta, exist_ok=True)
    df.to_parquet(f"{carpeta}base_modelado.parquet")
    for ini, h in hist.items():
        pd.DataFrame(h).to_parquet(f"{carpeta}historia_{pd.Timestamp(ini).date()}.parquet")
    pd.DataFrame(hist_final).to_parquet(f"{carpeta}historia_final.parquet")
    with open(f"{carpeta}configuracion.json", "w") as f: json.dump(config, f, indent=2, default=str)

def leer_historia(ruta):
    t = pd.read_parquet(ruta)
    return {c: t[c].to_numpy() for c in t.columns}

def cargar_preparacion(carpeta):
    """Lo que dejó 0411: (configuración, base de modelado, historia por bloque de validación)."""
    cfg = json.load(open(f"{carpeta}configuracion.json"))
    cfg["TAREAS"] = [tuple(t) for t in cfg["TAREAS"]]
    df = pd.read_parquet(f"{carpeta}base_modelado.parquet")
    hist = {pd.Timestamp(ini): leer_historia(f"{carpeta}historia_{pd.Timestamp(ini).date()}.parquet")
            for ini in cfg["INICIOS_VALIDACION"]}
    return cfg, df, hist

def ordenar(tabla, orden):
    """Orden estable por columnas con un orden dado, p. ej. {"tarea": ["clf", "reg"], "bloque": None}
    (None = orden natural). Deja las filas como si todo se hubiera corrido en un solo cuaderno."""
    claves = pd.DataFrame({c: (tabla[c].map({v: i for i, v in enumerate(vals)}) if vals is not None
                               else tabla[c].astype(str).rank(method="dense"))
                           for c, vals in orden.items()})
    return tabla.iloc[np.lexsort([claves[c].to_numpy() for c in reversed(list(orden))])].reset_index(drop=True)
