# Usa una imagen base de Python 3.8 ligera
FROM python:3.8-slim

# Establece el directorio de trabajo
WORKDIR /app

# Instala dependencias necesarias, incluyendo Airflow
RUN pip install apache-airflow

# Copia el script de entrada al contenedor
COPY entrypoint.sh /entrypoint.sh

# Copia el DAG al directorio de Airflow
COPY ./dags /opt/airflow/dags

# Asegúrate de que el script sea ejecutable
RUN chmod +x /entrypoint.sh

# Expon el puerto 8080
EXPOSE 8080

RUN airflow db init

RUN airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Usa el script de shell como punto de entrada
ENTRYPOINT ["/entrypoint.sh"]