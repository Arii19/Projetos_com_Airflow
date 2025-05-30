from airflow.decorators import dag, task
import requests
import pandas as pd
import pendulum

@dag(
    schedule='@daily',
    start_date=pendulum.now('America/Sao_Paulo').subtract(days=5),
    catchup=True,
    tags=["open-meteo", "SQLServer", "API", "DadosMeteorologicos"],
)
def DadosMeteorologicosOpenMeteo():

    @task(retries=3)
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

        times = dados["hourly"]["time"]  # lista de horários no formato ISO

        df = pd.DataFrame({
            "time": times,
            "temperature_2m": dados["hourly"]["temperature_2m"],
            "relative_humidity_2m": dados["hourly"]["relative_humidity_2m"],
            "rain": dados["hourly"]["rain"],
            "cloud_cover": dados["hourly"]["cloud_cover"],
            "visibility": dados["hourly"]["visibility"],
            "wind_speed_10m": dados["hourly"]["wind_speed_10m"],
            "wind_direction_10m": dados["hourly"]["wind_direction_10m"],
            "soil_temperature_0cm": dados["hourly"]["soil_temperature_0cm"],
            "soil_moisture_0_to_1cm": dados["hourly"]["soil_moisture_0_to_1cm"]
        })

        df["latitude"] = dados["latitude"]
        df["longitude"] = dados["longitude"]
        df["generationtime_ms"] = dados["generationtime_ms"]
        df["utc_offset_seconds"] = dados["utc_offset_seconds"]
        df["timezone"] = dados["timezone"]
        df["elevation"] = dados["elevation"]

        print("DataFrame extraído:")
        print(df.head())
        return df

    dados = extract()

# Instancia a DAG para o Airflow registrar
DadosMeteorologicosOpenMeteo()