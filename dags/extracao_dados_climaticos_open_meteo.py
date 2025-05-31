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

    @task()
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        # Converter coluna time para datetime e renomear para DataHora
        df["DataHora"] = pd.to_datetime(df["time"])
    
        # Adicionar colunas fixas já presentes
        df["Latitude"] = df["latitude"]
        df["Longitude"] = df["longitude"]
        df["TempoGeracaoMs"] = df["generationtime_ms"]
        df["OffsetUtcSegundos"] = df["utc_offset_seconds"]
        df["FusoHorario"] = df["timezone"]
        df["Elevacao"] = df["elevation"]
    
        # Renomear as colunas de medição para os nomes do banco
        df = df.rename(columns={
            "temperature_2m": "Temperatura2m",
            "relative_humidity_2m": "UmidadeRelativa2m",
            "rain": "Chuva",
            "cloud_cover": "CoberturaNuvens",
            "visibility": "Visibilidade",
            "wind_speed_10m": "VelocidadeVento10m",
            "wind_direction_10m": "DirecaoVento10m",
            "soil_temperature_0cm": "TemperaturaSolo0cm",
            "soil_moisture_0_to_1cm": "UmidadeSolo0a1cm"
        })
    
        # Ajustar tipos — para as colunas que no banco são INT, converter se possível
        df["UmidadeRelativa2m"] = df["UmidadeRelativa2m"].astype('Int64')  # pode ser nulo, usa Int64 nullable
        df["CoberturaNuvens"] = df["CoberturaNuvens"].astype('Int64')
        df["Visibilidade"] = df["Visibilidade"].astype('Int64')
        df["DirecaoVento10m"] = df["DirecaoVento10m"].astype('Int64')
        df["OffsetUtcSegundos"] = df["OffsetUtcSegundos"].astype('Int64')
        df["Elevacao"] = df["Elevacao"].astype('Int64')
    
        # Remover colunas antigas que não serão usadas
        df = df.drop(columns=["time", "latitude", "longitude", "generationtime_ms", "utc_offset_seconds", "timezone", "elevation"])
    
        # Reordenar colunas conforme a tabela (sem o Id que é identity no banco)
        df = df[[
            "DataHora", "Latitude", "Longitude", "TempoGeracaoMs", "OffsetUtcSegundos",
            "FusoHorario", "Elevacao", "Temperatura2m", "UmidadeRelativa2m", "Chuva",
            "CoberturaNuvens", "Visibilidade", "VelocidadeVento10m", "DirecaoVento10m",
            "TemperaturaSolo0cm", "UmidadeSolo0a1cm"
        ]]
    
        print("DataFrame transformado:")
        print(df.head())

        return df


    dados = extract()
    transformed_data = transform(dados)

# Instancia a DAG para o Airflow registrar
DadosMeteorologicosOpenMeteo()

