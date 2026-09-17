import pyodbc
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime


####################   PARA EJECUCION CON PROCEDIMIENTOS ALMACENADOS     ######################
def get_connection(): 
    try:
        conexion = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=SERV-DBI01;'
            'DATABASE=SIG_COLOMBIA_DW;'
            'Trusted_Connection=yes;',
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
            'mssql+pyodbc://@D_SERV-DBI01/SIG_COLOMBIA_DW?'
            'driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
        )
        engine = create_engine(connection_string)
        print("Conexión establecida con SQL Server (SQLAlchemy)")
        return engine
    except Exception as e:
        print("Error de conexión (SQLAlchemy):", e)
        return None

