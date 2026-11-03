"""Conexión a SQL Server para 0100 y 0110 (se corren en el equipo de la empresa, no en Databricks).

El servidor y la base no se escriben en el repositorio: se leen de variables de entorno
del equipo donde se corre la extracción.

    SQL_SERVIDOR      nombre del servidor (lo da el área de TI)
    SQL_BASE          nombre de la base de datos
    SQL_SERVIDOR_SA   (opcional) nombre o alias del servidor para SQLAlchemy, si es distinto

La autenticación es la integrada de Windows (Trusted_Connection), así que no hay usuario ni clave.
"""
import os
import pyodbc
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

SERVIDOR = os.environ.get("SQL_SERVIDOR", "CAMBIAR_SERVIDOR")
BASE = os.environ.get("SQL_BASE", "CAMBIAR_BASE")
SERVIDOR_SA = os.environ.get("SQL_SERVIDOR_SA", SERVIDOR)
DRIVER = os.environ.get("SQL_DRIVER", "ODBC Driver 17 for SQL Server")


####################   PARA EJECUCION CON PROCEDIMIENTOS ALMACENADOS     ######################
def get_connection():
    try:
        conexion = pyodbc.connect(
            f"DRIVER={{{DRIVER}}};"
            f"SERVER={SERVIDOR};"
            f"DATABASE={BASE};"
            "Trusted_Connection=yes;",
            autocommit=False,
            timeout=0
        )
        print("Conexión establecida con SQL Server")
        return conexion
    except pyodbc.Error as e:
        print("Error de conexión:", e)
        return None

####################   PARA CONSULTAS E INSERCION EN DATA FRAMES     ######################

def get_sqlalchemy_engine():
    """Crea y devuelve un motor de conexión a SQL Server usando SQLAlchemy."""
    try:
        connection_string = (
            f"mssql+pyodbc://@{SERVIDOR_SA}/{BASE}?"
            f"driver={DRIVER.replace(' ', '+')}&trusted_connection=yes"
        )
        engine = create_engine(connection_string)
        print("Conexión establecida con SQL Server (SQLAlchemy)")
        return engine
    except Exception as e:
        print("Error de conexión (SQLAlchemy):", e)
        return None
