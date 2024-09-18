import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from modules import crear_tabla, cargar_tabla, obtener_datos, enviar_email

load_dotenv()

default_args={
    'owner': 'AgustinSoza',
    'retries':2,
    'retry_delay': timedelta(seconds=30),
    'email': [os.getenv("AIRFLOW_VAR_EMAIL")],
    'email_on_failure': True,
    'email_on_retry': False
}

with DAG(
    default_args=default_args,
    dag_id='ETL_Coderhouse',
    description= 'DAG para proyecto final de Coderhouse',
    start_date=datetime(2024,9,16),
    schedule_interval='30 9 * * *',
    catchup=False
    ) as dag:
    
    # Realiza la conexion a la base de datos y crea la tabla a utilizar.
    task1= PythonOperator(
        task_id='crear_tabla',
        python_callable= crear_tabla,
        dag=dag,
    )

    # Extraemos los datos de la API y los cargamos en la tabla creada en Redshift.
    task2= PythonOperator(
        task_id='cargar_tabla',
        python_callable= cargar_tabla,
        dag=dag,
    )

    # Obtenemos los datos que utilizaremos para enviar el correo de aviso.
    task3= PythonOperator(
        task_id='obtener_datos',
        python_callable= obtener_datos,
        dag=dag,
    )

    # Enviamos un mail con aviso del clima del dia en un departamento en especial.
    task4= PythonOperator(
        task_id='enviar_email',
        python_callable= enviar_email,
        dag=dag,
    )

    task1 >> task2 >> task3 >> task4
