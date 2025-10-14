from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task  # Import necessário para usar @task
from airflow.macros import ds_add
import pendulum
from os.path import join
import pandas as pd
import os
import requests  

from airflow import DAG
from airflow.decorators import task
import pendulum
import requests

with DAG(
    dag_id="dados_incendios",
    start_date=pendulum.datetime(2025, 7, 26, tz="UTC"),
    schedule='0 0 * * 1',  # Toda segunda-feira
    catchup=False,
    tags=["nasa", "incendios"]
) as dag:


    @task
    def extract():
        try:
            url = "https://eonet.gsfc.nasa.gov/api/v2.1/categories/8?status=open"
            response = requests.get(url)
            if response.status_code != 200:
                raise Exception(f"Erro ao acessar a API: {response.status_code}")
            dados = response.json()
            print("Extração concluída")
            return dados
        except Exception as e:
            print(f"Erro durante extração dos dados: {e}")
            raise

    @task
    def transform(dados):
        try:
            eventos = dados.get("events", [])
            df = pd.json_normalize(eventos)

            df = df.explode('categories').reset_index(drop=True)
            df_categories = pd.json_normalize(df['categories']).rename(columns={'id': 'category_id', 'title': 'category_title'})
            df = pd.concat([df.drop(columns=['categories']), df_categories], axis=1)

            df = df.explode('sources').reset_index(drop=True)
            df_sources = pd.json_normalize(df['sources']).rename(columns={'id': 'source_id', 'url': 'source_url'})
            df = pd.concat([df.drop(columns=['sources']), df_sources], axis=1)

            df = df.explode('geometries').reset_index(drop=True)
            df_geometries = pd.json_normalize(df['geometries']).rename(columns={'date': 'geometry_date', 'type': 'geometry_type', 'coordinates': 'geometry_coordinates'})
            df = pd.concat([df.drop(columns=['geometries']), df_geometries], axis=1)

            df[['longitude', 'latitude']] = pd.DataFrame(df['geometry_coordinates'].tolist(), index=df.index)
            print(df)
            df.to_csv('/opt/airflow/dags/dados_incendio.csv', index=False)

            print("Transformação concluída")
            return df.to_dict(orient='records')
        except Exception as e:
            print(f"Erro durante transformação: {e}")
            raise

    # Definição do fluxo da DAG: cria as tasks e conecta o fluxo
    dados_extraidos = extract()
    dados_transformados = transform(dados_extraidos)

