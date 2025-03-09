from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import requests
from io import BytesIO

# Configurações Gerais
TENANT_ID = '57e83b7a-5313-4e94-8647-60ab90ad483a'
CLIENT_ID = '7b30dbdd-373c-4326-870e-5705e4f53e12'
CLIENT_SECRET = 'SHV8Q~F3vw53zlc6cVZ9d2Tl~QtmDi2HJXRZtcSS'
SCOPES = ['https://graph.microsoft.com/.default']
TOKEN_URL = f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token'
USER = 'automacao@smartbreeder.com.br'
PASSWORD = 'sT5G@cD6u!'
MAIL_FOLDER_ID = 'AAMkADYxM2NlOTI5LWY1MTktNGMyNy1hNjY1LWJhYWJiNWZiZTgyYwAuAAAAAAC5IPjWJ04VQpYSryQAlxGRAQANCyP7qFxARatkPsS4io6hAAFQX5S3AAA='

# Argumentos padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição do DAG
dag = DAG(
    'processar_emails_excel',
    default_args=default_args,
    schedule_interval='@Daily',  # Ajuste conforme necessário (e.g., '0 * * * *' para executar a cada hora)
    catchup=False,
    description='DAG para processar emails e anexos Excel do Microsoft Graph'
)

# Função para obter o token usando o fluxo ROPC
def get_access_token():
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'username': USER,
        'password': PASSWORD,
        'scope': 'https://graph.microsoft.com/.default',
        'grant_type': 'password'
    }
    
    print("Dados enviados para autenticação:")
    print(data)
    
    response = requests.post(TOKEN_URL, headers=headers, data=data)
    
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        print(f"Erro ao obter o token: {response.status_code}, {response.text}")
        return None

# Função para ler emails e anexos
def ler_emails_e_anexos(**kwargs):
    access_token = kwargs['ti'].xcom_pull(task_ids='obter_token')
    
    emails_url = f"https://graph.microsoft.com/v1.0/me/mailFolders/{MAIL_FOLDER_ID}/messages"
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    try:
        response = requests.get(emails_url, headers=headers)
        response.raise_for_status()
        emails = response.json().get('value', [])

        dfs = []

        for email in emails:
            email_id = email['id']
            attachments_url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments"

            try:
                attachments_response = requests.get(attachments_url, headers=headers)
                attachments_response.raise_for_status()
                attachments = attachments_response.json().get('value', [])

                for attachment in attachments:
                    if attachment.get('contentType') == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or attachment.get('contentType') == 'application/vnd.ms-excel':
                        attachment_content_url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments/{attachment['id']}/$value"
                        attachment_content_response = requests.get(attachment_content_url, headers=headers)
                        attachment_content_response.raise_for_status()
                        excel_data = attachment_content_response.content
                        excel_bytes = BytesIO(excel_data)

                        try:
                            df = pd.read_excel(excel_bytes, engine='openpyxl' if attachment.get('contentType') == 'application/vnd.ms-excel' else None)
                            dfs.append(df)
                            print(f"Arquivo Excel {attachment.get('name', 'sem nome')} lido com sucesso do email {email_id}.")
                        except Exception as e:
                            print(f"Erro ao ler o arquivo Excel {attachment.get('name', 'sem nome')} do email {email_id}: {e}")

            except requests.exceptions.RequestException as e:
                print(f"Erro ao obter anexos do email {email_id}: {e}")

    except requests.exceptions.RequestException as e:
        print(f"Erro ao obter emails: {e}")
        return None

    kwargs['ti'].xcom_push(key='dataframes', value=dfs) # Guardando dataframes no xcom

def concatenar_dataframes(**kwargs):
    dataframes = kwargs['ti'].xcom_pull(key='dataframes', task_ids='ler_emails')

    if dataframes:
        try:
            df_final = pd.concat(dataframes, ignore_index=True)
            print("\nDataFrame final (concatenado):")
            print(df_final.head())
            kwargs['ti'].xcom_push(key='df_final', value=df_final) # Guardando dataframe final no xcom
        except Exception as e:
            print(f"Erro ao concatenar dataframes: {e}")
    else:
        print("Nenhum DataFrame encontrado para concatenar.")

# Definição das tarefas
obter_token_task = PythonOperator(
    task_id='obter_token',
    python_callable=get_access_token,
    dag=dag
)

ler_emails_task = PythonOperator(
    task_id='ler_emails',
    python_callable=ler_emails_e_anexos,
    provide_context=True,
    dag=dag
)

concatenar_dataframes_task = PythonOperator(
    task_id='concatenar_dataframes',
    python_callable=concatenar_dataframes,
    provide_context=True,
    dag=dag
)

# Definição das dependências
obter_token_task >> ler_emails_task >> concatenar_dataframes_task