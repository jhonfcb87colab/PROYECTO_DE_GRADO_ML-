"""Arma entrega/zona_localidad_v2.csv a partir de la tabla v1 que dejó 0430 (P7_zona_localidad_v1.csv).

Qué cambia frente a v1 (03-11-2026):
1. La capa de UPZ que se usó no trae la localidad, así que en v1 las zonas ubicadas por barrio quedaron
   sin localidad. Aquí se completa con entrega/upz_localidad.csv (cada UPZ urbana pertenece a una sola
   localidad). Las UPR rurales no se asignan porque abarcan más de una localidad.
2. Se aplican las correcciones de entrega/asignacion_manual_v2.csv (zonas mal ubicadas en UPR rurales).
3. Se registra la revisión cruzada de la muestra de 30 zonas (entrega/revision_cruzada_muestra_v2.csv).

Uso:  python herramientas/zona_localidad_v2.py RUTA/P7_zona_localidad_v1.csv
"""
import sys
import pandas as pd

V1 = sys.argv[1] if len(sys.argv) > 1 else "P7_zona_localidad_v1.csv"
FECHA, VERSION = "2026-11-03", "v2"

t = pd.read_csv(V1)
upz = pd.read_csv("entrega/upz_localidad.csv")
man = pd.read_csv("entrega/asignacion_manual_v2.csv").fillna("")
rev = pd.read_csv("entrega/revision_cruzada_muestra_v2.csv").fillna("")

mapa = dict(zip(upz.upz, upz.localidad.fillna("")))
t["localidad_fuente"] = ""
t.loc[t.localidad.notna(), "localidad_fuente"] = "nombre de localidad (0430)"
llenar = t.localidad.isna() & t.upz.notna()
t.loc[llenar, "localidad"] = t.loc[llenar, "upz"].map(mapa).replace("", pd.NA)
t.loc[llenar & t.localidad.notna(), "localidad_fuente"] = "UPZ -> localidad (upz_localidad.csv)"
t.loc[llenar & t.localidad.isna(), "localidad_fuente"] = "UPR rural: sin localidad"

t["nota_v2"] = ""
for r in man.itertuples():
    m = t.zona_urbana.str.strip() == r.zona_urbana.strip()
    assert m.any(), f"No está en v1: {r.zona_urbana}"
    t.loc[m, "localidad"] = r.localidad or pd.NA
    t.loc[m, "upz"] = r.upz or pd.NA
    t.loc[m, "estado"] = "asignada_manual" if r.localidad else "sin_coincidencia"
    t.loc[m, "localidad_fuente"] = "manual (asignacion_manual_v2.csv)" if r.localidad else ""
    if r.borrar_coordenadas == "si":
        t.loc[m, ["lon", "lat"]] = pd.NA
    t.loc[m, "nota_v2"] = r.nota

t["revisado_por"] = ""
t["revision_cruzada"] = ""
for r in rev.itertuples():
    m = t.zona_urbana == r.zona_urbana
    assert m.sum() >= 1, f"No está en v1: {r.zona_urbana}"
    t.loc[m, "revisado_por"] = r.revisor
    t.loc[m, "revision_cruzada"] = r.veredicto

t["version_tabla"], t["fecha_asignacion"] = VERSION, FECHA
t.to_csv("entrega/zona_localidad_v2.csv", index=False)

# Resumen para el README y el documento
ub = t.localidad.notna()
print(f"Zonas: {len(t):,} · con localidad: {ub.sum():,} ({t.loc[ub, 'guias'].sum() / t.guias.sum():.1%} de las guías)")
print(t.groupby("localidad_fuente", dropna=False).agg(zonas=("zona_urbana", "size"), guias=("guias", "sum")))
print(t[ub].groupby("localidad").agg(zonas=("zona_urbana", "size"), guias=("guias", "sum")).sort_values("guias", ascending=False))

res = (t[ub].groupby("localidad").agg(zonas=("zona_urbana", "size"), guias=("guias", "sum"))
         .sort_values("guias", ascending=False).reset_index())
res["pct_guias"] = (100 * res.guias / t.guias.sum()).round(1)
res.to_csv("entrega/zona_localidad_v2_por_localidad.csv", index=False)
