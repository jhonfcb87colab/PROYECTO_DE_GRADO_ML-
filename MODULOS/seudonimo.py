"""Seudonimización con HMAC-SHA256 para las tablas que salen del Volume del equipo (0500).

Por qué HMAC y no md5(valor || sal):
- md5 con sal es un hash sin clave: quien conozca la sal (o la adivine) puede recalcular el seudónimo
  de cualquier guía o cliente que tenga y saber si está en la tabla. En 0300 la sal de ID_CLIENTE
  quedó escrita en el cuaderno, así que ese md5 no protege frente a quien lea el repositorio.
- HMAC-SHA256 usa una clave secreta de 32 bytes que nunca se escribe en el código ni en Git.
  Sin la clave no se puede recalcular ni comprobar ningún seudónimo.

La misma clave da el mismo seudónimo en todas las tablas, así que los cruces entre tablas compartidas
siguen funcionando. El seudónimo se aplica sobre el valor que ya hay en la tabla (el md5 de 0300/0330),
así que no hace falta volver a correr el pipeline ni cambia ningún resultado.
"""
import hmac, hashlib, os, secrets
import pandas as pd

ALGORITMO = "HMAC-SHA256"


def leer_o_crear_clave(ruta_archivo=None, dbutils=None, scope=None, nombre=None, crear_si_falta=True):
    """Clave en bytes. Si se pasa dbutils con scope y nombre, usa el secreto de Databricks.
    Si no, lee el archivo del Volume (fuera de Git) y, si no existe y crear_si_falta, crea ahí
    una clave aleatoria de 32 bytes. La clave nunca se imprime."""
    if dbutils is not None and scope and nombre:
        clave = dbutils.secrets.get(scope=scope, key=nombre).strip()
        try:
            return bytes.fromhex(clave)
        except ValueError:
            return clave.encode("utf-8")
    if ruta_archivo is None:
        raise ValueError("No hay secreto de Databricks ni archivo de clave")
    if not os.path.exists(ruta_archivo):
        if not crear_si_falta:
            raise FileNotFoundError(ruta_archivo)
        os.makedirs(os.path.dirname(ruta_archivo), exist_ok=True)
        with open(ruta_archivo, "w") as f:
            f.write(secrets.token_hex(32))
    with open(ruta_archivo) as f:
        clave = bytes.fromhex(f.read().strip())
    assert len(clave) >= 32, "La clave debe tener al menos 32 bytes"
    return clave


def hmac_sha256(valor, clave):
    """Seudónimo hexadecimal (64 caracteres) de un valor. Nulos quedan nulos."""
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return None
    return hmac.new(clave, str(valor).encode("utf-8"), hashlib.sha256).hexdigest()


def seudonimizar(df, columnas, clave):
    """Copia de df con cada columna de 'columnas' reemplazada por su HMAC-SHA256.
    Calcula una vez por valor distinto. Devuelve (tabla, controles)."""
    out, controles = df.copy(), []
    for c in columnas:
        if c not in out.columns:
            controles.append({"columna": c, "presente": False})
            continue
        distintos = out[c].dropna().unique()
        mapa = {v: hmac_sha256(v, clave) for v in distintos}
        nuevo = out[c].map(mapa)
        controles.append({
            "columna": c, "presente": True, "filas": len(out), "nulos": int(out[c].isna().sum()),
            "distintos_antes": len(distintos), "distintos_despues": int(nuevo.nunique()),
            "valores_originales_que_quedan": int(nuevo.isin(set(map(str, distintos))).sum()),
        })
        out[c] = nuevo
    return out, pd.DataFrame(controles)
