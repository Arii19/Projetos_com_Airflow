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
            "https://api.open-meteo.com/v1/forecast?"
            "latitude=-23.5505&longitude=-46.6333&"
            "hourly=temperature_2m,rain&timezone=America%2FSao_Paulo"
        )
        response = requests.get(url)
        dados = response.json()
        if response.status_code != 200:
            raise Exception(f"Erro ao acessar a API: {response.status_code}")
        df = pd.DataFrame({
            "time": dados["hourly"]["time"],
            "temperature_2m": dados["hourly"]["temperature_2m"],
            "rain": dados["hourly"].get("rain", [None]*len(dados["hourly"]["time"]))
        })
        print("DataFrame extraído:")
        print(df.head())
        return df

    @task()
    def transform(dadosometeo):
        # Garante que está trabalhando com DataFrame
        df = pd.DataFrame(dadosometeo)
        # Renomeia as colunas
        df = df.rename(columns={"time": "data_hora", "temperature_2m": "temperatura_2m", "rain": "chuva_mm"})
        # Limpa valores nulos
        df = df.dropna(subset=["data_hora", "temperatura_2m", "chuva_mm"])
        # Conversões de tipo
        df["data_hora"] = pd.to_datetime(df["data_hora"])
        df["temperatura_2m"] = df["temperatura_2m"].astype(float)
        df["chuva_mm"] = df["chuva_mm"].astype(float)
        # Gerar um ID simples
        df["id"] = df["data_hora"].dt.strftime("%Y%m%d%H%M") + "_" + df["temperatura_2m"].astype(str)
        # Reorganizar as colunas
        df = df[["id", "data_hora", "temperatura_2m", "chuva_mm"]]
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
        try:
            mssql_hook.insert_rows(
                table='OpenMeteo',
                rows=list(df.itertuples(index=False, name=None)),
                target_fields=list(df.columns.values)
            )
            print("Dados inseridos com sucesso!")
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")


    


    dadosometeo = extract()
    df_transformado = transform(dadosometeo)
    load(df_transformado)
    
DadosMeteorologicosOpenMeteo()