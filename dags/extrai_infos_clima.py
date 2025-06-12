from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add
import pendulum
from os.path import join
import pandas as pd
import os

with DAG(
    "dados_climaticos",
    start_date=pendulum.datetime(2025, 2, 22, tz="UTC"),
    schedule='0 0 * * 1',  # executar toda segunda feira
) as dag:

    tarefa_1 = BashOperator(
        task_id='cria_pasta',
        bash_command='mkdir "{{ params.dir_path }}"',
        params={'dir_path': r'C:\Users\Microsoft\Documents\PYTHON\extra-_dados_climaticos\semana={{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    def extrai_dados(data_interval_end):
        city = 'Boston'
        key = 'ANZQ5K8QQP8BXZ85F4ZEQ2FPK'

        URL = join(
            'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv'
        )

        dados = pd.read_csv(URL)

        file_path = rf'C:\Users\Microsoft\Documents\PYTHON\extra-_dados_climaticos\semana={data_interval_end}\\'
        os.makedirs(file_path, exist_ok=True)

        dados.to_csv(file_path + 'dados_brutos.csv', index=False)
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv', index=False)
        dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv', index=False)

    tarefa_2 = PythonOperator(
        task_id='extrai_dados',
        python_callable=extrai_dados,
        op_kwargs={'data_interval_end': '{{ data_interval_end.strftime("%Y-%m-%d") }}'}
    )

    tarefa_1 >> tarefa_2

