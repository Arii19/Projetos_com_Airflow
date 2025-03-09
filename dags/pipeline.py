from airflow import DAG  # Importa a classe DAG do Airflow para definir fluxos de trabalho
from airflow.operators.python import PythonOperator  # Importa o PythonOperator para executar funções Python como tarefas
from datetime import datetime, timedelta  # Importa datetime e timedelta para lidar com datas e durações
import os  # Importa o módulo os para interagir com o sistema operacional (não diretamente usado aqui)
import pandas as pd  # Importa pandas para manipulação de dados, usado para trabalhar com DataFrames
import requests  # Importa requests para fazer requisições HTTP, usado para interagir com a API do Microsoft Graph
from io import BytesIO  # Importa BytesIO para trabalhar com dados binários em memória

# Configurações Gerais
TENANT_ID = '57e83b7a-5313-4e94-8647-60ab90ad483a'  # Define o ID do locatário para autenticação no Microsoft Graph
CLIENT_ID = '7b30dbdd-373c-4326-870e-5705e4f53e12'  # Define o ID do cliente para autenticação no Microsoft Graph
CLIENT_SECRET = 'SHV8Q~F3vw53zlc6cVZ9d2Tl~QtmDi2HJXRZtcSS'  # Define o segredo do cliente para autenticação no Microsoft Graph
SCOPES = ['https://graph.microsoft.com/.default']  # Define os escopos de acesso para o token do Microsoft Graph
TOKEN_URL = f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token'  # Define a URL para obter o token de acesso
USER = 'automacao@smartbreeder.com.br'  # Define o nome de usuário para autenticação no Microsoft Graph
PASSWORD = 'sT5G@cD6u!'  # Define a senha para autenticação no Microsoft Graph
MAIL_FOLDER_ID = 'AAMkADYxM2NlOTI5LWY1MTktNGMyNy1hNjY1LWJhYWJiNWZiZTgyYwAuAAAAAAC5IPjWJ04VQpYSryQAlxGRAQANCyP7qFxARatkPsS4io6hAAFQX5S3AAA='  # Define o ID da pasta de email a ser acessada

# Argumentos padrão do DAG
default_args = {
    'owner': 'airflow',  # Define o proprietário do DAG
    'depends_on_past': False,  # Define se a tarefa depende de execuções anteriores
    'start_date': datetime(2023, 1, 1),  # Define a data de início do DAG
    'retries': 1,  # Define o número de tentativas em caso de falha
    'retry_delay': timedelta(minutes=5),  # Define o intervalo entre tentativas
}

# Definição do DAG
dag = DAG(
    'processar_emails_excel',  # Define o nome do DAG
    default_args=default_args,  # Define os argumentos padrão do DAG
    schedule_interval=None,  # Define o intervalo de agendamento (None para execução manual)
    catchup=False,  # Define se execuções retroativas devem ser feitas
    description='DAG para processar emails e anexos Excel do Microsoft Graph'  # Define a descrição do DAG
)

# Função para obter o token usando o fluxo ROPC
def get_access_token():
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'  # Define o tipo de conteúdo do cabeçalho da requisição
    }
    
    data = {
        'client_id': CLIENT_ID,  # Define o ID do cliente nos dados da requisição
        'client_secret': CLIENT_SECRET,  # Define o segredo do cliente nos dados da requisição
        'username': USER,  # Define o nome de usuário nos dados da requisição
        'password': PASSWORD,  # Define a senha nos dados da requisição
        'scope': 'https://graph.microsoft.com/.default',  # Define o escopo nos dados da requisição
        'grant_type': 'password'  # Define o tipo de concessão nos dados da requisição
    }
    
    print("Dados enviados para autenticação:")  # Imprime os dados enviados para autenticação
    print(data)  # Imprime os dados enviados para autenticação
    
    response = requests.post(TOKEN_URL, headers=headers, data=data)  # Faz a requisição POST para obter o token
    
    if response.status_code == 200:  # Verifica se a requisição foi bem-sucedida
        return response.json().get('access_token')  # Retorna o token de acesso
    else:
        print(f"Erro ao obter o token: {response.status_code}, {response.text}")  # Imprime uma mensagem de erro
        return None  # Retorna None em caso de erro

# Função para ler emails e anexos
def ler_emails_e_anexos(**kwargs):
    access_token = kwargs['ti'].xcom_pull(task_ids='obter_token')  # Recupera o token de acesso do XCom
    
    emails_url = f"https://graph.microsoft.com/v1.0/me/mailFolders/{MAIL_FOLDER_ID}/messages"  # Define a URL para obter os emails
    headers = {
        'Authorization': f'Bearer {access_token}'  # Define o cabeçalho de autorização com o token
    }

    try:
        response = requests.get(emails_url, headers=headers)  # Faz a requisição GET para obter os emails
        response.raise_for_status()  # Verifica se a requisição foi bem-sucedida
        emails = response.json().get('value', [])  # Obtém a lista de emails da resposta

        dfs = []  # Inicializa uma lista para armazenar os DataFrames

        for email in emails:  # Loop através dos emails
            email_id = email['id']  # Obtém o ID do email
            attachments_url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments"  # Define a URL para obter os anexos

            try:
                attachments_response = requests.get(attachments_url, headers=headers)  # Faz a requisição GET para obter os anexos
                attachments_response.raise_for_status()  # Verifica se a requisição foi bem-sucedida
                attachments = attachments_response.json().get('value', [])  # Obtém a lista de anexos da resposta

                for attachment in attachments:  # Loop através dos anexos
                    if attachment.get('contentType') == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or attachment.get('contentType') == 'application/vnd.ms-excel':  # Verifica o tipo de conteúdo do anexo
                        attachment_content_url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments/{attachment['id']}/$value"  # Define a URL para obter o conteúdo do anexo
                        attachment_content_response = requests.get(attachment_content_url, headers=headers)  # Faz a requisição GET para obter o conteúdo do anexo
                        attachment_content_response.raise_for_status()  # Verifica se a requisição foi bem-sucedida
                        excel_data = attachment_content_response.content  # Obtém o conteúdo do anexo
                        excel_bytes = BytesIO(excel_data)  # Cria um objeto BytesIO com o conteúdo do anexo

                        try:
                            df = pd.read_excel(excel_bytes, engine='openpyxl' if attachment.get('contentType') == 'application/vnd.ms-excel' else None)  # Lê o arquivo Excel para um DataFrame
                            dfs.append(df)  # Adiciona o DataFrame à lista
                            print(f"Arquivo Excel {attachment.get('name', 'sem nome')} lido com sucesso do email {email_id}.")  # Imprime uma mensagem de sucesso
                        except Exception as e:
                            print(f"Erro ao ler o arquivo Excel {attachment.get('name', 'sem nome')} do email {email_id}: {e}")  # Imprime uma mensagem de erro

            except requests.exceptions.RequestException as e:
                print(f"Erro ao obter anexos do email {email_id}: {e}")  # Imprime uma mensagem de erro

    except requests.exceptions.RequestException as e:
        print(f"Erro ao obter emails: {e}")  # Imprime uma mensagem de erro
        return None

    kwargs['ti'].xcom_push(key='dataframes', value=dfs)  # Armazena a lista de DataFrames no XCom

def concatenar