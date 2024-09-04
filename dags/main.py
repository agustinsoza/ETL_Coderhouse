import requests
import pandas as pd
import datetime
import psycopg2
from psycopg2.extras import execute_values
from airflow.models import Variable


def crear_conexion():
    try:
        dbname = Variable.get('dbname')
        host = Variable.get('host')
        user = Variable.get('user')
        pwd = Variable.get('pwd')
        port = Variable.get('port')

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


def crear_tabla():

    conn = crear_conexion()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agustinsoza964_coderhouse.clima_misiones(
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
            """)
        conn.commit()
    conn.close()


def crear_df():

    appid = '929e7a3a1b3cafec69b3c08796e6dae7'
    units = 'metric'
    localidades_misiones = {
        "nombre": [
            "Apóstoles", "Cainguás", "Candelaria", "Capital",
            "Concepción", "Eldorado", "General Manuel Belgrano",
            "Guaraní", "Iguazú", "Leandro N. Alem", "Libertador General San Martín",
            "Montecarlo", "Oberá", "San Ignacio", "San Javier", "25 de Mayo", "San Pedro"
        ],
        "lat": [
            -27.9087, -27.2059, -27.3933, -27.3671,
            -27.9812, -26.4083, -26.2625,
            -27.2970, -25.6111, -27.60174491, -26.8060,
            -26.5664, -27.4874, -27.2666, -27.884636001620983,
            -27.3749, -26.6196
        ],
        "lon": [
            -55.7514, -54.9795, -55.7532, -55.8935,
            -55.5209, -54.6984, -53.6482,
            -54.2014, -54.5737, -55.32662090, -55.0233,
            -54.7598, -55.1185, -55.5282, -55.11477950472755,
            -54.7458, -54.1083
        ]
    }
    clima = {
        'id': [],
        'name': [],
        'temperature': [],
        'feels_like': [],
        'temp_min': [],
        'temp_max': [],
        'humidity': [],
        'wind_speed': [],
        'weather': [],
        'weather_desc': [],
        'dt': []
    }

    for nombre, lat, lon in zip(localidades_misiones['nombre'], localidades_misiones['lat'], localidades_misiones['lon']):
        # Crear la URL con los valores de latitud y longitud
        url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={appid}&units={units}'
        # https://api.openweathermap.org/data/2.5/weather?lat=-27.4821&lon=-58.8313&appid=929e7a3a1b3cafec69b3c08796e6dae7&units=metric
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()

            ID = data['id']
            Name = nombre
            Temperature = data['main']['temp']
            Feels_Like = data['main']['feels_like']
            Temp_Min = data['main']['temp_min']
            Temp_Max = data['main']['temp_max']
            Humidity = f"{data['main']['humidity']}%"
            Wind_Speed = f"{data['wind']['speed']} km/h"
            Weather = data['weather'][0]['main']
            Weather_Desc = data['weather'][0]['description']
            DT = datetime.datetime.fromtimestamp(data['dt'])

            clima['id'].append(ID)
            clima['name'].append(Name)
            clima['temperature'].append(Temperature)
            clima['feels_like'].append(Feels_Like)
            clima['temp_min'].append(Temp_Min)
            clima['temp_max'].append(Temp_Max)
            clima['humidity'].append(Humidity)
            clima['wind_speed'].append(Wind_Speed)
            clima['weather'].append(Weather)
            clima['weather_desc'].append(Weather_Desc)
            clima['dt'].append(DT)

        elif response.status_code == 404:
            return print('Recurso no encontrado')

        else:
            return print(f'Error: Codigo de estado {response.status_code}')

    df = pd.DataFrame(clima)
    return df


def cargar_tabla():
    conn = crear_conexion()
    df = crear_df()
    with conn.cursor() as cur:
        try:
            execute_values(
                cur, 'INSERT INTO agustinsoza964_coderhouse.clima_misiones VALUES %s',
                [tuple(row) for row in df.values],
                page_size=len(df)
            )
            conn.commit()
        except Exception as e:
            print(f'No se pueden ingresar datos. Error {e}')

    conn.close()


