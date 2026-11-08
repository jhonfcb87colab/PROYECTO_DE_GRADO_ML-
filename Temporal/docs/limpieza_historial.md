# Limpieza del historial de Git (P9 de Iván)

**No se ha hecho.** Requiere reescribir el historial del repositorio y forzar el push, así que debe hacerlo John (dueño del repositorio), con todos los integrantes avisados, porque después cada uno tiene que volver a clonar.

## Qué hay en el historial

Revisión del 03-11-2026 sobre todas las versiones guardadas:

- `0200_AnalisisExploratorio.ipynb` (commits del 20-10-2026) tiene salidas con **correos de empleados de Servientrega** (`...@servientrega.com.co`).
- `0500_Proyecto_EJEMJPLO.ipynb` (primeros commits) tiene salidas con correos de un conjunto de datos de ejemplo ajeno al proyecto.
- `0100_ExtraccionInformacion.ipynb`, `0300_CleanTransformParquet.ipynb` y `0400_ModeladoPrediccion.ipynb` (commits hasta el 20-10-2026) tienen salidas guardadas; pueden mostrar filas de guías y movimientos.
- `MODULOS/conexion_sql.py` (versiones del 17-09 al 25-10-2026, la última con el nombre `interpretabilidad.py`) tiene el nombre del servidor y de la base de SQL Server de la empresa.

La versión actual del repositorio ya no tiene nada de eso.

## Pasos (con git-filter-repo)

**Hacerlo antes de subir el ZIP de la entrega.** El paso 1 borra del historial todos los `.ipynb`; hoy `main` solo tiene `.py`, así que no se pierde nada vigente. Después de limpiar se sube el ZIP, que trae los cuadernos `.ipynb` sin salidas.

```bash
pip install git-filter-repo
git clone --mirror https://github.com/jhonfcb87colab/proyecto_de_grado_ml-.git
cd proyecto_de_grado_ml-.git

# 1. Quitar del historial todos los .ipynb, .html y caches (en main hoy solo hay .py)
git filter-repo --invert-paths --path-glob '*.ipynb' --path-glob '*.html' --path-glob '*__pycache__*' --path temporal/

# 2. Reemplazar el servidor y la base en las versiones viejas de conexion_sql.py
cat > reemplazos.txt <<'TXT'
SERV-DBI01==>CAMBIAR_SERVIDOR
SIG_COLOMBIA_DW==>CAMBIAR_BASE
TXT
git filter-repo --replace-text reemplazos.txt

# 3. Comprobar que no queda nada
git log --all -p | grep -E -i "@servientrega|SERV-DBI01|SIG_COLOMBIA_DW" | head   # no debe imprimir nada

# 4. Subir (reescribe ramas y etiquetas en GitHub)
git push --force --mirror
```

Después:

0. Subir el ZIP de la entrega (ver COMO_SUBIR.md).

1. Cada integrante borra su copia local y vuelve a clonar.
2. Los pull requests viejos de GitHub conservan referencias a los commits anteriores: pedir a GitHub Support que elimine las referencias en caché, o crear un repositorio nuevo, privado, con solo la versión limpia (más simple).
3. Dejar el repositorio privado.
