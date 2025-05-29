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
    
    @task(retries=3)
    def extract():
        url = (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=-21.6853&longitude=-51.0725"
            "&daily=temperature_2m_max,temperature_2m_min"
            "&hourly=temperature_2m,rain"
            "&timezone=America%2FSao_Paulo"
        )
        
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Erro ao acessar a API: {response.status_code}")
        
        dados = response.json()

        # DataFrame com dados horários
        df_hourly = pd.DataFrame({
            "datetime": dados["hourly"]["time"],
            "temperature_2m": dados["hourly"]["temperature_2m"],
            "rain": dados["hourly"].get("rain", [None] * len(dados["hourly"]["time"]))
        })
        df_hourly["date"] = df_hourly["datetime"].str[:10]  # extrai data para agrupar

        # Agrega os dados horários por dia
        df_hourly_grouped = df_hourly.groupby("date").agg({
            "temperature_2m": "mean",
            "rain": "sum"
        }).reset_index()
        df_hourly_grouped.rename(columns={
            "temperature_2m": "Temperatura_2m",
            "rain": "Chuva"
        }, inplace=True)

        # DataFrame com dados diários
        df_daily = pd.DataFrame({
            "date": dados["daily"]["time"],
            "temperature_2m_max": dados["daily"]["temperature_2m_max"],
            "temperature_2m_min": dados["daily"]["temperature_2m_min"]
        })

        # Merge dos dois DataFrames
        df_merged = pd.merge(df_hourly_grouped, df_daily, on="date")

        # Mostra o resultado (sem conversões)
        print("DataFrame unificado:")
        print(df_merged.head())
        print("Tipos de dados:")
        print(df_merged.dtypes)

        return df_merged

    @task()
    def transform(dadosometeo):
        df = pd.DataFrame(dadosometeo)

        # Renomeia colunas para português
        df = df.rename(columns={
            "date": "Data",
            "avg_temperature_2m": "TemperaturaMedia_2m",
            "temperature_2m_max": "TemperaturaMax_2m",
            "temperature_2m_min": "TemperaturaMin_2m",
            "total_rain": "Chuva_mm"
        })

        # Remove linhas com dados faltantes
        df = df.dropna(subset=["Data", "TemperaturaMedia_2m", "TemperaturaMax_2m", "TemperaturaMin_2m", "Chuva_mm"])

        # Conversão de tipos
        df["Data"] = pd.to_datetime(df["Data"])
        df["TemperaturaMedia_2m"] = df["TemperaturaMedia_2m"].astype(float)
        df["Chuva_mm"] = df["Chuva_mm"].astype(float)
        df["TemperaturaMax_2m"] = df["TemperaturaMax_2m"].astype(float)
        df["TemperaturaMin_2m"] = df["TemperaturaMin_2m"].astype(float)

        # Cria um ID único
        df["id"] = df["Data"].dt.strftime("%Y%m%d") + "_" + df["TemperaturaMedia_2m"].astype(str)

        # Reordena as colunas
        df = df[["id", "Data", "TemperaturaMedia_2m", "TemperaturaMax_2m", "TemperaturaMin_2m", "Chuva_mm"]]

        print("DataFrame transformado:")
        print(df.head())
        return df

    
    @task()
    def load(dados):
        df = pd.DataFrame(dados)
        print("DataFrame reconstruído na load:")
        print(df.head())
        print(f"Quantidade de linhas: {len(df)}")

        if df.empty:
            print("DataFrame vazio — nada a inserir.")
            return

        mssql_hook = MsSqlHook(mssql_conn_id='SmartFlow_Ariane')
        print(mssql_hook.get_uri())
        try:
            mssql_hook.insert_rows(
                table='OpenMeteo', rows=list(df.itertuples(index=False, name=None)),target_fields=list(df.columns.values))
            print("Dados inseridos com sucesso!")
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")

    dadosometeo = extract()
    df_transformado = transform(dadosometeo)
    load(df_transformado)
    
DadosMeteorologicosOpenMeteo()