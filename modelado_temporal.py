"""Funciones auxiliares de 0420_Interpretabilidad_3.

El cuaderno solo llama a estas funciones; aquí no hay rutas ni configuración.
"""
import numpy as np, pandas as pd, shap
import matplotlib.pyplot as plt
from scipy.stats import spearmanr


def variable_original(nombre_transformado, categoricas):
    """'cat__categoria_zona_C' -> 'categoria_zona'; 'num__sla_h' -> 'sla_h'."""
    t, resto = nombre_transformado.split("__", 1)
    if t == "num": return resto
    return max([c for c in categoricas if resto.startswith(c + "_")], key=len)

def shap_modelo(pipe, X, X_fondo, tarea):
    """SHAP sobre las columnas transformadas; devuelve (matriz, nombres, datos transformados)."""
    prep, mod = pipe.named_steps["prep"], pipe.named_steps["modelo"]
    densa = lambda Z: Z.toarray() if hasattr(Z, "toarray") else np.asarray(Z)
    Xt = densa(prep.transform(X)).astype(float); nombres = list(prep.get_feature_names_out())
    if mod.__class__.__name__ in ("LogisticRegression", "LinearRegression"):
        exp = shap.LinearExplainer(mod, densa(prep.transform(X_fondo)).astype(float))
        sv = exp.shap_values(Xt)
    else:
        sv = shap.TreeExplainer(mod).shap_values(Xt, check_additivity=False)
    if isinstance(sv, list): sv = sv[1] if len(sv) == 2 else sv[0]
    sv = np.asarray(sv)
    if sv.ndim == 3: sv = sv[:, :, 1]
    return sv, nombres, Xt

def agregar_por_variable(sv, nombres, categoricas):
    """Suma las columnas one-hot de una misma variable: una columna de SHAP por variable original."""
    orig = [variable_original(n, categoricas) for n in nombres]
    df = pd.DataFrame(sv, columns=nombres)
    return df.T.groupby(orig).sum().T

def estabilidad_importancias(g):
    """Para un modelo y tarea: tabla variable×bloque y resumen de cuánto se parece el orden entre bloques."""
    piv = g.pivot_table(index="variable", columns="bloque", values="importancia")
    bloques = list(piv.columns)
    rhos = [spearmanr(piv[a], piv[b]).correlation for i, a in enumerate(bloques) for b in bloques[i + 1:]]
    top = piv.mean(axis=1).sort_values(ascending=False)
    en_top5 = (piv.rank(ascending=False) <= 5).sum(axis=1)
    resumen = {"spearman_min_entre_bloques": np.nanmin(rhos), "spearman_medio": np.nanmean(rhos),
               "top5_promedio": ", ".join(top.index[:5]),
               "variables_top5_en_todos_los_bloques": ", ".join(en_top5[en_top5 == len(bloques)].index)}
    return piv, resumen

def resumen_shap_tarea(tarea, d, agg, numericas):
    """SHAP medio absoluto por variable y dirección (Spearman entre valor y SHAP; solo numéricas)."""
    filas = []
    for v in agg.columns:
        direccion = spearmanr(d[v], agg[v], nan_policy="omit").correlation if v in numericas else np.nan
        filas.append({"tarea": tarea, "variable": v, "media_abs_shap": agg[v].abs().mean(), "direccion_spearman": direccion})
    return pd.DataFrame(filas).sort_values("media_abs_shap", ascending=False)

def efectos_por_categoria(tarea, d, agg, categoricas, minimo=30):
    """SHAP medio de cada categoría con al menos 'minimo' guías."""
    filas = []
    for v in categoricas:
        for cat, s in agg[v].groupby(d[v].astype(str)):
            if len(s) >= minimo: filas.append({"tarea": tarea, "variable": v, "categoria": cat, "n": len(s), "shap_medio": s.mean()})
    return filas

def alertas_por_segmento(c, col, dias):
    """Alertas por día, tasa real, precisión y exhaustividad por valor de un segmento (mínimo 30 guías)."""
    g = c.groupby(col)
    t = pd.DataFrame({"guias": g.size(), "alertas_por_dia": g.alerta.sum() / dias, "tasa_real": g.y.mean(),
                      "precision_alertas": g.apply(lambda x: x.loc[x.alerta, "y"].mean() if x.alerta.any() else np.nan),
                      "exhaustividad": g.apply(lambda x: x.loc[x.y == 1, "alerta"].mean() if (x.y == 1).any() else np.nan)})
    t = t[t.guias >= 30]; t.insert(0, "segmento", col)
    return t.reset_index().rename(columns={col: "valor"})

def barras_horizontales(nombres, valores, titulo, ruta_png, error=None, color="#4C78A8"):
    """Gráfico de barras horizontales (de mayor a menor, arriba la mayor) y lo guarda en PNG."""
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.barh(nombres, valores, xerr=error, color=color)
    ax.set_title(titulo)
    plt.tight_layout(); fig.savefig(ruta_png, dpi=130); plt.show()
