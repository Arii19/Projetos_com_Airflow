import os
import pandas as pd
import requests
import pyodbc
from openpyxl import load_workbook
from io import BytesIO

# Configurações Gerais
TENANT_ID = '57e83b7a-5313-4e94-8647-60ab90ad483a'
CLIENT_ID = '7b30dbdd-373c-4326-870e-5705e4f53e12'
CLIENT_SECRET = 'SHV8Q~F3vw53zlc6cVZ9d2Tl~QtmDi2HJXRZtcSS'
SCOPES = ['https://graph.microsoft.com/.default']
TOKEN_URL = f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token'
USER = 'automacao@smartbreeder.com.br'
PASSWORD = 'sT5G@cD6u!'
DB_SERVER = '10.0.1.33'
DB_NAME = 'SmartFlow_Ariane'
DB_USER = 'SmartFlow_Ariane'
DB_PASSWORD = 'yWKxZoewQ2'
MAIL_FOLDER_ID = 'AAMkADYxM2NlOTI5LWY1MTktNGMyNy1hNjY1LWJhYWJiNWZiZTgyYwAuAAAAAAC5IPjWJ04VQpYSryQAlxGRAQANCyP7qFxARatkPsS4io6hAAFQX5S3AAA='

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
    
    # Print da variável 'data' para verificar o que está sendo enviado para verficar se deu certo
    print("Dados enviados para autenticação:")
    print(data)
    
    response = requests.post(TOKEN_URL, headers=headers, data=data)
    
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        print(f"Erro ao obter o token: {response.status_code}, {response.text}")
        return None

# Fluxo principal
if __name__ == "__main__":
    token = get_access_token()
    
    if token:
        print("Token de acesso obtido com sucesso!")
    else:
        print("Erro ao obter o token de acesso.")

#----------------------------------------------------ATÉ AQUI DEU CERTO------------------------------------

# Função para autenticar no Microsoft Graph e obter um token de acesso
# Função para ler emails e anexos
def ler_emails_e_anexos(access_token):
    emails_url = f"https://graph.microsoft.com/v1.0/me/mailFolders/{MAIL_FOLDER_ID}/messages"
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    try:
        response = requests.get(emails_url, headers=headers)
        response.raise_for_status()
        emails = response.json().get('value', [])

        dfs = []  # Lista para armazenar os DataFrames

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
                            df = pd.read_excel(excel_bytes, engine='openpyxl' if attachment.get('contentType') == 'application/vnd.ms-excel' else None) # Lendo excel
                            dfs.append(df) # Armazenando dataframes
                            print(f"Arquivo Excel {attachment.get('name', 'sem nome')} lido com sucesso do email {email_id}.")
                        except Exception as e:
                            print(f"Erro ao ler o arquivo Excel {attachment.get('name', 'sem nome')} do email {email_id}: {e}")

            except requests.exceptions.RequestException as e:
                print(f"Erro ao obter anexos do email {email_id}: {e}")

    except requests.exceptions.RequestException as e:
        print(f"Erro ao obter emails: {e}")
        return None

    return dfs # Retorna a lista de dataframes

# Fluxo principal
if __name__ == "__main__":
    token = get_access_token()

    if token:
        print("Token de acesso obtido com sucesso!")
        dataframes = ler_emails_e_anexos(token) # Lendo emails e anexos

        if dataframes:
            print(f"Foram encontrados {len(dataframes)} arquivos Excel nos emails.")

            # Aqui você tem a lista de DataFrames 'dataframes', onde cada elemento é um DataFrame de um arquivo Excel.
            # Você pode agora trabalhar com esses DataFrames:
            for i, df in enumerate(dataframes):
                print(f"\nDataFrame {i+1}:")
                print(df.head()) # Exibindo as primeiras linhas de cada dataframe

            # Se quiser, pode concatenar todos os DataFrames em um único DataFrame:
            if dataframes: # Verifica se a lista não está vazia antes de concatenar
                try:
                    df_final = pd.concat(dataframes, ignore_index=True)
                    print("\nDataFrame final (concatenado):")
                    print(df_final.head())
                except Exception as e:
                    print(f"Erro ao concatenar dataframes: {e}")
        else:
            print("Nenhum arquivo Excel encontrado nos emails ou erro na leitura dos emails.")

    else:
        print("Erro ao obter o token de acesso.")