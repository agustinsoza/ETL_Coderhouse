from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from main import crear_tabla, cargar_tabla


default_args={
    'owner': 'AgustinSoza',
    'retries':5,
    'retry_delay': timedelta(minutes=3)
}

dag_coder = DAG(
    default_args=default_args,
    dag_id='ETL_Coderhouse',
    description= 'DAG para proyecto final de Coderhouse',
    start_date=datetime(2024,9,4),
    schedule_interval='@daily'
    )
    
task1= PythonOperator(
    task_id='crear_tabla',
    python_callable= crear_tabla,
    dag=dag_coder,
)

task2= PythonOperator(
    task_id='cargar_tabla',
    python_callable= cargar_tabla,
    dag=dag_coder,
)

task1 >> task2
