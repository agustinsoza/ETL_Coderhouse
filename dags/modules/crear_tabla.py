from .crear_conexion import crear_conexion, REDSHIFT_SCHEMA

def crear_tabla():

    conn = crear_conexion()
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.clima_misiones(
                ID INT NOT NULL,
                Departamento VARCHAR(100),
                Temperatura FLOAT,
                Sensacion_Termica FLOAT,
                Temperatura_Min FLOAT,
                Temperatura_Max FLOAT,
                Humedad VARCHAR(5),
                Velocidad_Viento VARCHAR(20),
                Clima VARCHAR(100),
                Descripcion VARCHAR(100),
                Ultima_Actualizacion VARCHAR(100) NOT NULL,
                CONSTRAINT PK_clima_misiones PRIMARY KEY (ID, Ultima_Actualizacion));
            TRUNCATE TABLE {REDSHIFT_SCHEMA}.clima_misiones;
            """)
        conn.commit()
    conn.close()