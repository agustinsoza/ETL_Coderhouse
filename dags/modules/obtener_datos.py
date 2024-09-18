from .crear_conexion import crear_conexion, REDSHIFT_SCHEMA

def obtener_datos(**kwargs):

    departamento = 'San Javier'

    conn = crear_conexion()
    with conn.cursor() as cur:
        cur.execute(f"""SELECT departamento, sensacion_termica, temperatura_min, temperatura_max, clima, descripcion, ultima_actualizacion 
                        FROM {REDSHIFT_SCHEMA}.clima_misiones 
                        WHERE departamento = '{departamento}';
                    """)
        data = cur.fetchall()
    conn.close()

    kwargs['ti'].xcom_push(key='datos', value=data)
    
