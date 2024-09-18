from .crear_conexion import crear_conexion, REDSHIFT_SCHEMA
from .crear_df import crear_df
from psycopg2.extras import execute_values

def cargar_tabla():
    conn = crear_conexion()
    df = crear_df()
    with conn.cursor() as cur:
        try:
            execute_values(
                cur, f'INSERT INTO {REDSHIFT_SCHEMA}.clima_misiones VALUES %s',
                [tuple(row) for row in df.values],
                page_size=len(df)
            )
            conn.commit()
        except Exception as e:
            print(f'No se pueden ingresar datos. Error {e}')

    conn.close()