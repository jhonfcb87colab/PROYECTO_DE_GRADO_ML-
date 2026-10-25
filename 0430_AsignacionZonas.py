# Databricks notebook source
# MAGIC %md
# MAGIC # 0430 · Asignación de zonas urbanas a barrio, UPZ y localidad
# MAGIC
# MAGIC **Para qué.** La evaluación espacial (I de Moran de los residuos, rezago de vecinos) necesita ubicar cada zona urbana de Servienvios. `ZONASURBANAS` no tiene coordenadas, pero la mayoría de las zonas se llama por un barrio («D HAYUELOS 1», «R&D CHAPINERO A»). Este cuaderno:
# MAGIC
# MAGIC 1. Clasifica las 2.386 zonas por su nombre: barrio, localidad, genérica, aliado, no geográfica o fuera de Bogotá.
# MAGIC 2. Busca el barrio en los **sectores catastrales** de Catastro Bogotá (IDECA) y ubica su centro en la **UPZ** y la **localidad**.
# MAGIC 3. Aplica las correcciones manuales de `asignacion_manual.csv`, si existe.
# MAGIC 4. Construye la **matriz de vecindad entre localidades** (contigüidad tipo reina) y la resume como lo pide Iván: pares, vecinos promedio, mínimo, máximo y unidades sin vecinos.
# MAGIC
# MAGIC **Lee** `V3/VERIFICACION/P6_v9_plantilla_zona_localidad.csv` (de 0340) y tres capas de Datos Abiertos Bogotá. **Escribe** solo en `V3/ESPACIAL/`. No toca la base ni los modelos.
# MAGIC
# MAGIC **Databricks serverless no tiene salida a internet**: las capas se bajan a mano del portal y se suben a `V3/ESPACIAL/capas/`. Basta con que el nombre del archivo contenga `sector`, `upz` o `localidad`, y los .zip se suben tal cual, sin descomprimir (GeoJSON, JSON, ZIP de shapefile, GPKG o KML). Solo es obligatoria la de sector catastral. Sin la capa de localidades, cada zona queda con su punto (suficiente para la vecindad kNN entre zonas, que es la que pide Iván para el Moran), con su UPZ si está esa capa, y la matriz de contigüidad se arma entre UPZ en vez de entre localidades.
# MAGIC
# MAGIC **Regla.** Una zona solo queda asignada si su barrio aparece en una sola localidad. Si aparece en varias (por ejemplo «SANTA BARBARA»), queda **ambigua** y se resuelve a mano; no se elige al azar.

# COMMAND ----------

# MAGIC %pip install "geopandas>=0.14" pyogrio

# COMMAND ----------

# CONFIGURACIÓN
import os, json, re, difflib, urllib.request
import numpy as np, pandas as pd, geopandas as gpd
PATH_DATABRICKS = "/Volumes/proyectogrado/source/source/"
CARPETA_V3      = f"{PATH_DATABRICKS}LOG_CONSOLIDADO/REVISION_2026_10/V3/"
RUTA_ZONAS      = f"{CARPETA_V3}VERIFICACION/P6_v9_plantilla_zona_localidad.csv"
CARPETA_SALIDAS = f"{CARPETA_V3}ESPACIAL/"
CARPETA_CAPAS   = f"{CARPETA_SALIDAS}capas/"
RUTA_MANUAL     = f"{CARPETA_SALIDAS}asignacion_manual.csv"     # opcional: zona_urbana, localidad, upz, nota
os.makedirs(CARPETA_CAPAS, exist_ok=True)

# Recursos de Datos Abiertos Bogotá (IDECA). El enlace de descarga se pide a la API del portal.
API_RECURSO = "https://datosabiertos.bogota.gov.co/api/3/action/resource_show?id="
CAPAS = {
    "sector_catastral.geojson": "428a5f4e-b9d4-4d91-88a7-28fb404a08ab",   # Sector catastral, Bogotá D.C. (GeoJSON)
    "upz.zip":                  "50a34bd9-ad86-405d-b0a8-8f98feb8ce3f",   # Polígonos de UPZ (SHP, versión 04.23)
    "localidades.geojson":      "38faae88-9c97-4ec8-90ca-bad251482838",   # Polígonos de localidad (GeoJSON 10.25)
}
DISPERSION_MAX_M = 2000     # solo sin capa de localidades: sectores homónimos más lejos que esto = ambigua
UMBRAL_PARECIDO = 0.88       # difflib: parecido mínimo para aceptar un nombre de barrio aproximado

# COMMAND ----------

# Clasificación de los nombres de ZONASURBANAS (misma función que usa 0430_AsignacionZonas)
import re, unicodedata
import pandas as pd

LOCALIDADES = ["USAQUEN", "CHAPINERO", "SANTA FE", "SAN CRISTOBAL", "USME", "TUNJUELITO", "BOSA", "KENNEDY",
               "FONTIBON", "ENGATIVA", "SUBA", "BARRIOS UNIDOS", "TEUSAQUILLO", "LOS MARTIRES", "ANTONIO NARINO",
               "PUENTE ARANDA", "LA CANDELARIA", "RAFAEL URIBE URIBE", "CIUDAD BOLIVAR", "SUMAPAZ"]
# Nombre de zona igual al de una localidad (algunos son también barrios: se marcan para revisión)
ALIAS_LOCALIDAD = {"MARTIRES": "LOS MARTIRES", "CANDELARIA": "LA CANDELARIA", "RAFAEL URIBE": "RAFAEL URIBE URIBE",
                   "ANTONIO NARINO": "ANTONIO NARINO", **{l: l for l in LOCALIDADES}}
MUNICIPIOS = ["SOACHA", "CAZUCA", "ZIPAQUIRA", "CHIA", "CAJICA", "COTA", "FUNZA", "MOSQUERA", "MADRID", "FACATATIVA",
              "SIBATE", "LA CALERA", "TOCANCIPA", "SOPO", "GACHANCIPA", "TENJO", "TABIO", "SUBACHOQUE", "FUSAGASUGA",
              "PUEBLOS"]
NO_GEOGRAFICA = ["ALIADO", "ALTO RIESGO", "VALORES", "GERENCIA", "OFIC", "VOLANTE", "LOGITECH", "ALM. CADENA",
                 "ALM CADENA", "AFORADOR", "SUPERCADE", "CENTRO DE ACOPIO", "BARRIOS", "CLIENTE", "CORPORATIVO",
                 "MENSAJERIA", "EMPRESARIAL", "CONVENIO", "NACIONAL"]
GENERICAS = ["NORTE", "SUR", "CENTRO", "OCCIDENTE", "ORIENTE", "LAGO", "COUNTRY", "INTERNACIONAL", "INDUSTRIAL",
             "Z INDUSTRIAL", "METRO", "AUTOPISTA", "INTERNACIONAL BAVARIA"]   # sector sin barrio preciso

PREFIJOS = r"^(?:D\s*NODO\s*II|R&D\s*HM|R&D\.?|RYD|D&R\.?|R\.O\.?|R\.?|D\.?|HM|CDS|CLS|ZONA)\s+"
SUFIJO = r"\s+(?:[A-H]|\d{1,2})$"

def normalizar(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().upper()
    return re.sub(r"\s+", " ", s).strip()

def clasificar(nombre):
    n = normalizar(nombre)
    if re.match(r"^\d+\s*-\s*ALIADO", n):
        return "aliado", None, False, None, "punto aliado: sin ubicación en el nombre"
    for k in NO_GEOGRAFICA:
        if k in n:
            return "no_geografica", None, False, None, f"contiene «{k}»"
    base = n
    for _ in range(3):                                   # quita prefijos encadenados («D NODO II», «D CP»)
        base = re.sub(PREFIJOS, "", base).strip()
        base = re.sub(r"^(?:CP|PM|NODO\s*II)\s+", "", base).strip()
    base = re.sub(SUFIJO, "", base).strip()
    base = re.sub(r"^(?:CARCEL)\s+", "CARCEL ", base)
    if not base:
        return "sin_nombre", None, False, None, "solo prefijo"
    for m in MUNICIPIOS:
        if re.search(rf"\b{m}\b", base):
            return "fuera_de_bogota", base, False, None, f"municipio «{m}»"
    if base in GENERICAS:
        return "generica", base, False, None, "sector genérico, sin barrio"
    loc = ALIAS_LOCALIDAD.get(base)
    if loc:
        return "localidad", base, True, loc, "el nombre es una localidad (revisar si es barrio homónimo)"
    return "barrio", base, True, None, ""

def clasificar_tabla(v9):
    d = v9.sort_values("guias", ascending=False).copy()
    r = d.zona_urbana.map(clasificar)
    d["tipo_zona"] = r.str[0]; d["nombre_base"] = r.str[1]; d["es_geografica"] = r.str[2]
    d["localidad_por_nombre"] = r.str[3]; d["nota_clasificacion"] = r.str[4]
    d["pct_acumulado"] = d.pct_guias.cumsum().round(2)
    return d

# COMMAND ----------

# 1. Clasificación de las zonas por su nombre
zonas = clasificar_tabla(pd.read_csv(RUTA_ZONAS))
total = zonas.guias.sum()
resumen_tipos = (zonas.groupby("tipo_zona").agg(zonas=("guias", "size"), guias=("guias", "sum"))
                 .assign(pct_guias=lambda x: (100 * x.guias / total).round(1)).sort_values("guias", ascending=False))
display(resumen_tipos)

# COMMAND ----------

# 2. Capas de IDECA. Databricks serverless no sale a internet: se suben a mano a CARPETA_CAPAS.
# Se acepta cualquier archivo cuyo nombre CONTENGA sector, upz o localidad (p. ej. pensionadosupz_042023.zip), en GeoJSON, JSON, ZIP de shapefile, GPKG o KML.
# Obligatoria: sector catastral. Localidades y UPZ son opcionales. Si falta localidades, se arma uniendo las UPZ por su localidad.
import glob
EXTENSIONES = (".geojson", ".json", ".zip", ".gpkg", ".kml", ".shp")
def buscar_archivo(prefijo):
    hallados = sorted(f for f in glob.glob(f"{CARPETA_CAPAS}*") if prefijo in os.path.basename(f).lower()
                      and f.lower().endswith(EXTENSIONES))
    return hallados[0] if hallados else None

def obtener_capa(prefijo, recurso):
    ruta = buscar_archivo(prefijo)
    if ruta is None:                                            # último intento: descarga (falla sin internet)
        try:
            url = json.load(urllib.request.urlopen(API_RECURSO + recurso, timeout=30))["result"]["url"]
            ruta = f"{CARPETA_CAPAS}{prefijo}_{os.path.basename(url)}"
            urllib.request.urlretrieve(url, f"/tmp/{os.path.basename(ruta)}")
            with open(f"/tmp/{os.path.basename(ruta)}", "rb") as fi, open(ruta, "wb") as fo: fo.write(fi.read())
        except Exception as e:
            print(f"Sin archivo «{prefijo}*» en {CARPETA_CAPAS} y sin descarga ({type(e).__name__})")
            return None
    if ruta.lower().endswith(".zip"):                           # el shapefile o GeoJSON puede venir dentro de una carpeta del zip
        import zipfile
        internos = [n for n in zipfile.ZipFile(ruta).namelist() if n.lower().endswith((".shp", ".geojson", ".json", ".gpkg", ".kml"))]
        assert internos, f"{ruta} no trae .shp ni .geojson"
        ruta_lectura = f"zip://{ruta}!{internos[0]}"
    else:
        ruta_lectura = ruta
    capa = gpd.read_file(ruta_lectura)
    capa = capa.set_crs(4326) if capa.crs is None else capa.to_crs(4326)
    print(os.path.basename(ruta), len(capa), "polígonos · columnas:", [c for c in capa.columns if c != "geometry"])
    return capa

sectores    = obtener_capa("sector", CAPAS["sector_catastral.geojson"])
upz         = obtener_capa("upz", CAPAS["upz.zip"])
localidades = obtener_capa("localidad", CAPAS["localidades.geojson"])
assert sectores is not None, "Falta la capa de sector catastral en CARPETA_CAPAS"
if localidades is None:
    print("AVISO: sin capa de localidades. Las zonas se ubican solo con el sector catastral (coordenadas y vecindad kNN);"
          " la localidad queda vacía salvo cuando el nombre de la zona ya es una localidad.")

# COMMAND ----------

# 3. Columna de nombre de cada capa (se detecta; si falla, escribirla a mano aquí)
def columna_nombre(capa, preferidas):
    for c in preferidas:
        for col in capa.columns:
            if col.upper() == c:
                return col
    candidatas = [c for c in capa.columns if c != "geometry" and capa[c].dtype == object and "NOM" in c.upper()]
    assert candidatas, f"No se encontró la columna de nombre; columnas: {list(capa.columns)}"
    return candidatas[0]

COL_SECTOR = columna_nombre(sectores, ["SCANOMBRE", "NOMBRE_SECTOR", "SECTOR", "NOMBRE"])
COL_UPZ    = columna_nombre(upz, ["NOMBRE", "UPZNOMBRE", "NOM_UPZ", "UPLNOMBRE"]) if upz is not None else None
COL_LOC = None
COLS_LOC_EN_UPZ = [c for c in (upz.columns if upz is not None else [])
                   if c.upper() in ("LOCNOMBRE", "NOM_LOC", "LOCALIDAD", "NOMBRE_LOC", "LOCCODIGO", "COD_LOC", "CODIGO_LOC")]
if localidades is None and COLS_LOC_EN_UPZ:
    # Localidades armadas desde las UPZ, solo si la capa de UPZ trae la columna de su localidad
    COL_LOC_UPZ = COLS_LOC_EN_UPZ[0]
    localidades = upz[[COL_LOC_UPZ, "geometry"]].dissolve(COL_LOC_UPZ).reset_index()
    COL_LOC = COL_LOC_UPZ
    print(f"Localidades armadas uniendo las UPZ por «{COL_LOC_UPZ}»: {len(localidades)}")
    if localidades[COL_LOC_UPZ].astype(str).str.fullmatch(r"\d+").all():
        CODIGOS_LOC = {i + 1: n for i, n in enumerate(LOCALIDADES)}   # código oficial 1–20, en el orden de LOCALIDADES
        localidades[COL_LOC_UPZ] = localidades[COL_LOC_UPZ].astype(int).map(CODIGOS_LOC)
TIENE_LOC  = localidades is not None
if TIENE_LOC:
    COL_LOC = COL_LOC or columna_nombre(localidades, ["LOCNOMBRE", "NOMBRE", "NOM_LOC", "LOCALIDAD"])
print("Sector:", COL_SECTOR, "| UPZ:", COL_UPZ, "| Localidad:", COL_LOC)

# COMMAND ----------

# 4. Centro de cada sector catastral y su UPZ y localidad
sec = sectores[[COL_SECTOR, "geometry"]].copy()
sec["nombre_norm"] = sec[COL_SECTOR].map(normalizar)
sec["geometry"] = sec.geometry.representative_point()          # punto dentro del polígono
if TIENE_LOC:
    sec = gpd.sjoin(sec, localidades[[COL_LOC, "geometry"]].rename(columns={COL_LOC: "localidad"}),
                    how="left", predicate="within").drop(columns="index_right")
else:
    sec["localidad"] = None                                     # sin capa de localidades
if upz is not None:
    sec = gpd.sjoin(sec, upz[[COL_UPZ, "geometry"]].rename(columns={COL_UPZ: "upz"}),
                    how="left", predicate="within").drop(columns="index_right")
else:
    sec["upz"] = None                                           # sin capa de UPZ: solo localidad
sec["localidad"] = sec.localidad.map(lambda x: normalizar(x) if pd.notna(x) else None)
sec["upz"] = sec.upz.map(lambda x: normalizar(x) if pd.notna(x) else None)
m3116 = sec.geometry.to_crs(3116)
sec["x_m"], sec["y_m"] = m3116.x, m3116.y
base_sec = sec.dropna(subset=["localidad"]) if TIENE_LOC else sec
por_nombre = (base_sec.groupby("nombre_norm")
              .agg(localidades=("localidad", lambda s: sorted(set(s.dropna()))), upzs=("upz", lambda s: sorted(set(s.dropna()))),
                   sectores=("nombre_norm", "size"),
                   lon=("geometry", lambda g: float(g.x.mean())), lat=("geometry", lambda g: float(g.y.mean())))
              .join(base_sec.groupby("nombre_norm")[["x_m", "y_m"]]
                    .apply(lambda g: float(np.hypot(g.x_m - g.x_m.mean(), g.y_m - g.y_m.mean()).max()))
                    .rename("dispersion_m")))
print(f"{len(sec):,} sectores · {len(por_nombre):,} nombres distintos")

# COMMAND ----------

# 5. Búsqueda del barrio de cada zona: nombre exacto, luego contenido, luego aproximado
NOMBRES = list(por_nombre.index)
def buscar(base):
    if base in por_nombre.index:
        return base, "exacto", 1.0
    contienen = [n for n in NOMBRES if re.search(rf"\b{re.escape(base)}\b", n)]
    if len(contienen) >= 1:
        return min(contienen, key=len), "contenido", round(len(base) / len(min(contienen, key=len)), 2)
    cerca = difflib.get_close_matches(base, NOMBRES, n=1, cutoff=UMBRAL_PARECIDO)
    if cerca:
        return cerca[0], "aproximado", round(difflib.SequenceMatcher(None, base, cerca[0]).ratio(), 2)
    return None, "sin_coincidencia", 0.0

filas = []
for z in zonas.itertuples():
    f = {"zona_urbana": z.zona_urbana, "tipo_zona": z.tipo_zona, "nombre_base": z.nombre_base, "guias": z.guias,
         "sector_encontrado": None, "metodo": None, "parecido": None, "localidad": None, "upz": None,
         "estado": "no_geografica", "localidades_posibles": None, "lon": None, "lat": None}
    if z.tipo_zona == "localidad":
        f.update(localidad=z.localidad_por_nombre, metodo="nombre de localidad", estado="asignada_localidad")
    elif z.tipo_zona == "barrio":
        nombre, metodo, parecido = buscar(z.nombre_base)
        f.update(sector_encontrado=nombre, metodo=metodo, parecido=parecido)
        if nombre is None:
            f["estado"] = "sin_coincidencia"
        else:
            locs, upzs = por_nombre.loc[nombre, "localidades"], por_nombre.loc[nombre, "upzs"]
            f["localidades_posibles"] = " | ".join(locs)
            # Sin capa de localidades, «una sola ubicación» = sectores homónimos a menos de DISPERSION_MAX_M del centro
            unica = len(locs) == 1 if TIENE_LOC else por_nombre.loc[nombre, "dispersion_m"] <= DISPERSION_MAX_M
            if unica:
                f.update(localidad=locs[0] if locs else None, upz=upzs[0] if len(upzs) == 1 else None,
                         lon=por_nombre.loc[nombre, "lon"], lat=por_nombre.loc[nombre, "lat"],
                         estado="asignada" if metodo == "exacto" else "asignada_revisar")
            else:
                f["estado"] = "ambigua"
    filas.append(f)
asig = pd.DataFrame(filas)
# Trazabilidad que pide Iván (P3): criterio, fuente, fecha y versión; la revisión cruzada la llena otro integrante
VERSION_TABLA = "v1"
asig["criterio"] = asig.metodo
asig["fuente"] = "IDECA: sector catastral, UPZ y localidad (Datos Abiertos Bogotá)"
asig["fecha_asignacion"] = pd.Timestamp.today().date().isoformat()
asig["version_tabla"] = VERSION_TABLA
asig["revisado_por"] = None

# COMMAND ----------

# 6. Correcciones manuales (opcional): reemplazan lo automático
if os.path.exists(RUTA_MANUAL):
    man = pd.read_csv(RUTA_MANUAL)
    man["localidad"] = man.localidad.map(normalizar)
    asig = asig.merge(man[["zona_urbana", "localidad", "upz"]].rename(columns={"localidad": "loc_m", "upz": "upz_m"}),
                      on="zona_urbana", how="left")
    m = asig.loc_m.notna()
    asig.loc[m, "localidad"] = asig.loc[m, "loc_m"]; asig.loc[m, "upz"] = asig.loc[m, "upz_m"]
    asig.loc[m, "estado"] = "asignada_manual"
    asig = asig.drop(columns=["loc_m", "upz_m"])
    print(f"{m.sum()} zonas corregidas a mano")
else:
    print("Sin asignacion_manual.csv: solo asignación automática")

# COMMAND ----------

# 7. Cobertura: qué porcentaje de las guías queda ubicado
cobertura = (asig.groupby("estado").agg(zonas=("guias", "size"), guias=("guias", "sum"))
             .assign(pct_guias=lambda x: (100 * x.guias / total).round(1)).sort_values("guias", ascending=False))
display(cobertura)
por_localidad = (asig.dropna(subset=["localidad"]).groupby("localidad")
                 .agg(zonas=("guias", "size"), guias=("guias", "sum")).sort_values("guias", ascending=False))
display(por_localidad)
asig.to_csv(f"{CARPETA_SALIDAS}P7_zona_localidad_{VERSION_TABLA}.csv", index=False)   # versionada: no se sobrescribe a mano
# Muestra para la revisión cruzada: las 15 asignadas con más guías y 15 más al azar entre las demás
auto = asig[asig.estado.isin(["asignada", "asignada_revisar"])].sort_values("guias", ascending=False)
resto = auto.iloc[15:]
muestra = pd.concat([auto.head(15), resto.sample(n=min(15, len(resto)), random_state=42)])
muestra[["zona_urbana", "nombre_base", "sector_encontrado", "localidad", "upz", "criterio", "revisado_por"]] \
    .to_csv(f"{CARPETA_SALIDAS}P7_muestra_revision_cruzada.csv", index=False)
cobertura.reset_index().to_csv(f"{CARPETA_SALIDAS}P7_cobertura.csv", index=False)
por_localidad.reset_index().to_csv(f"{CARPETA_SALIDAS}P7_guias_por_localidad.csv", index=False)

# COMMAND ----------

# 8. Pendientes de revisión a mano: las ambiguas y sin coincidencia con más guías
revisar = asig[asig.estado.isin(["ambigua", "sin_coincidencia", "asignada_revisar"])].sort_values("guias", ascending=False)
revisar[["zona_urbana", "nombre_base", "guias", "estado", "sector_encontrado", "localidades_posibles"]] \
    .to_csv(f"{CARPETA_SALIDAS}P7_para_revisar.csv", index=False)
print(f"{len(revisar):,} zonas por revisar, con {revisar.guias.sum() / total:.1%} de las guías")
display(revisar.head(40))

# COMMAND ----------

# 9. Matriz de vecindad entre unidades (contigüidad tipo reina: se tocan, aunque sea en un punto).
# Unidad: localidad si hay esa capa; si no, UPZ (Bogotá urbana). Sin ninguna de las dos, se omite.
if TIENE_LOC:
    unidad, capa_u, col_u = "localidad", localidades, COL_LOC
elif upz is not None:
    unidad, capa_u, col_u = "upz", upz, COL_UPZ
else:
    unidad = None
    print("Sin localidades ni UPZ: se omite esta matriz (la de zonas, celda 10, sí se calcula)")
if unidad:
    loc = capa_u[[col_u, "geometry"]].copy()
    loc["unidad"] = loc[col_u].map(normalizar)
    loc = loc.dissolve("unidad").reset_index()
    loc_m = loc.to_crs(3116)                                     # MAGNA-SIRGAS Bogotá, en metros
    pares = []
    for i, a in loc_m.iterrows():
        for j, b in loc_m.iterrows():
            if i < j and a.geometry.buffer(1).intersects(b.geometry):   # 1 m de tolerancia por bordes imprecisos
                pares.append((a.unidad, b.unidad))
    vec = pd.DataFrame(pares, columns=[f"{unidad}_a", f"{unidad}_b"])
    grado = pd.concat([vec.iloc[:, 0], vec.iloc[:, 1]]).value_counts().reindex(loc.unidad, fill_value=0)
    resumen_w = pd.DataFrame([{"unidad": unidad, "unidades": len(loc), "pares_vecinos": len(vec), "vecinos_promedio": round(grado.mean(), 1),
                               "minimo": int(grado.min()), "maximo": int(grado.max()), "sin_vecinos": int((grado == 0).sum())}])
    display(resumen_w)
    vec.to_csv(f"{CARPETA_SALIDAS}P7_vecindad_{unidad}es.csv" if unidad == "localidad" else f"{CARPETA_SALIDAS}P7_vecindad_upz.csv", index=False)
    resumen_w.to_csv(f"{CARPETA_SALIDAS}P7_resumen_vecindad.csv", index=False)

# COMMAND ----------

# 10. Vecindad entre ZONAS (unidad que pide Iván para el Moran): k vecinos más cercanos sobre el centro del barrio
K_VECINOS = 5
from sklearn.neighbors import NearestNeighbors
ub = asig.dropna(subset=["lon", "lat"]).reset_index(drop=True)
pts = gpd.GeoSeries(gpd.points_from_xy(ub.lon, ub.lat), crs=4326).to_crs(3116)
xy = list(zip(pts.x, pts.y))
nn = NearestNeighbors(n_neighbors=min(K_VECINOS + 1, len(ub))).fit(xy)
dist, idx = nn.kneighbors(xy)
wz = pd.DataFrame([{"zona_a": ub.zona_urbana[i], "zona_b": ub.zona_urbana[j], "distancia_m": round(d, 0)}
                   for i, (fila_d, fila_i) in enumerate(zip(dist, idx)) for d, j in zip(fila_d[1:], fila_i[1:])])
resumen_wz = pd.DataFrame([{"zonas_con_ubicacion": len(ub), "pct_guias_ubicadas": round(100 * ub.guias.sum() / total, 1),
                            "k": K_VECINOS, "distancia_mediana_m": wz.distancia_m.median(),
                            "zonas_mismo_barrio": int(ub.duplicated(["lon", "lat"], keep=False).sum())}])
display(resumen_wz)
wz.to_csv(f"{CARPETA_SALIDAS}P7_vecindad_zonas_knn.csv", index=False)
resumen_wz.to_csv(f"{CARPETA_SALIDAS}P7_resumen_vecindad_zonas.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué entregar y qué sigue
# MAGIC
# MAGIC - `P7_cobertura.csv` y `P7_guias_por_localidad.csv`: cuánto de las guías quedó ubicado y en qué localidad.
# MAGIC - `P7_para_revisar.csv`: zonas ambiguas o sin coincidencia, ordenadas por guías. Se resuelven a mano en `asignacion_manual.csv` (columnas `zona_urbana, localidad, upz, nota`), empezando por las de más guías, y se vuelve a correr.
# MAGIC - `P7_resumen_vecindad.csv` y `P7_vecindad_localidades.csv`: la matriz W entre localidades.
# MAGIC - `P7_vecindad_zonas_knn.csv`: la matriz W entre zonas (5 vecinos más cercanos), que es la unidad del Moran que pide Iván.
# MAGIC - `P7_muestra_revision_cruzada.csv`: 30 asignaciones para que otro integrante las revise y firme en `revisado_por`.
# MAGIC
# MAGIC Con `P7_zona_localidad_v1.csv` se calcula el I de Moran de los residuos de **validación** promediados por zona y mes (cuaderno siguiente), como pide Iván.