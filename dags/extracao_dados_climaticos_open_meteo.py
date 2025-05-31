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

    @task
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
        df.to_csv('/opt/airflow/dags/dados_climaticos.csv', index=False)
        return df

    @task
    def transform(df):
        # Renomeia as colunas conforme solicitado
        df = df.rename(columns={
            "time": "DataHora",
            "latitude": "Latitude",
            "longitude": "Longitude",
            "generationtime_ms": "TempoGeracaoMs",
            "utc_offset_seconds": "OffsetUtcSegundos",
            "timezone": "FusoHorario",
            "elevation": "Elevacao",
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

        # Gera um Id único para cada linha
        df["Id"] = (
            df["DataHora"].astype(str) + "_" +
            df["Latitude"].astype(str) + "_" +
            df["Longitude"].astype(str)
        ).apply(lambda x: hashlib.md5(x.encode()).hexdigest())

        # Reordena as colunas
        df = df[
            ["Id", "DataHora", "Latitude", "Longitude", "TempoGeracaoMs", "OffsetUtcSegundos",
             "FusoHorario", "Elevacao", "Temperatura2m", "UmidadeRelativa2m", "Chuva",
             "CoberturaNuvens", "Visibilidade", "VelocidadeVento10m", "DirecaoVento10m",
             "TemperaturaSolo0cm", "UmidadeSolo0a1cm"]
        ]

        # Converte valores vazios para None (que vira NULL no SQL)
        df = df.where(pd.notnull(df), None)

        # Converte DataHora para datetime no formato aceito pelo SQL Server
        df["DataHora"] = df["DataHora"].dt.strftime('%Y-%m-%d %H:%M:%S')

        print("DataFrame transformado:")
        print(df.head())
        return df

    @task()
    def load(df):
        # Conecta ao SQL Server
        mssql_hook = MsSqlHook(mssql_conn_id='Airflow_Ariane')
        try:
            # Insere os dados no SQL Server
            mssql_hook.insert_rows(
                table='DadosClimaticos_OpenMeteo',
                rows=list(df.itertuples(index=False, name=None)),
                target_fields=list(df.columns),
                replace=True 
            )
            print("Dados inseridos com sucesso.")
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")
            raise

    # Encadeamento das tarefas
    dados = extract()
    dados_tratados = transform(dados)
    load(dados_tratados)

DadosMeteorologicosOpenMeteo()