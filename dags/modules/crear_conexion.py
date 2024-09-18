import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def crear_conexion():
    try:
        dbname = os.getenv("REDSHIFT_DB")
        host = os.getenv("REDSHIFT_HOST")
        user = os.getenv("REDSHIFT_USER")
        pwd = os.getenv("REDSHIFT_PWD")
        port = os.getenv("REDSHIFT_PORT")

        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=pwd,
            host=host,
            port=port
        )

        return conn

    except Exception as e:
        return print(f"No se puede establecer la conexion. Error {e}")
    
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
    
API_KEY = os.getenv("API_KEY")