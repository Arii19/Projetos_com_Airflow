import requests
import pandas as pd
import hashlib
import pendulum 
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.models import Variable

@dag(
    schedule='@daily',
    start_date=pendulum.now('America/Sao_Paulo').subtract(days=5),
    catchup=True,
    tags=["open-meteo", "SQLServer", "API", "DadosMeteorologicos"],
)
def DadosMeteorologicosOpenMeteo():

    @task#(retries=3)
    def extract():
        url = (
            "https://api.open-meteo.com/v1/forecast?"
            "latitude=-21.6853&longitude=-51.0725&"
            "hourly=temperature_2m,relative_humidity_2m,rain,cloud_cover,visibility,"
            "wind_speed_10m,wind_direction_10m,soil_temperature_0cm,soil_moisture_0_to_1cm&"
            "forecast_days=3"
        )
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Erro ao acessar a API: {response.status_code}")
        dados = response.json()
        # Extrai os arrays de hora e dados meteorológicos
        hourly = dados["hourly"]
        n = len(hourly["time"])
        df = pd.DataFrame({
            "latitude": [dados["latitude"]]*n,
            "longitude": [dados["longitude"]]*n,
            "elevation": [dados["elevation"]]*n,
            "generationtime_ms": [dados["generationtime_ms"]]*n,
            "utc_offset_seconds": [dados["utc_offset_seconds"]]*n,
            "timezone": [dados["timezone"]]*n,
            "time": hourly["time"],
            "temperature_2m": hourly["temperature_2m"],
            "relative_humidity_2m": hourly["relative_humidity_2m"],
            "rain": hourly["rain"],
            "cloud_cover": hourly["cloud_cover"],
            "visibility": hourly["visibility"],
            "wind_speed_10m": hourly["wind_speed_10m"],
            "wind_direction_10m": hourly["wind_direction_10m"],
            "soil_temperature_0cm": hourly["soil_temperature_0cm"],
            "soil_moisture_0_to_1cm": hourly["soil_moisture_0_to_1cm"]
        })
        print("DataFrame extraído:")
        print(df.head())
        df.to_csv('C:/Users/Microsoft/Documents/Alura/dados_climaticos.csv', index=False)
        return df
        


    dados = extract()

DadosMeteorologicosOpenMeteo()