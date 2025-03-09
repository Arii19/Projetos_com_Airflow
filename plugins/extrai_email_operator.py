from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import pandas as pd
from io import BytesIO

class ProcessEmailExcelOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(ProcessEmailExcelOperator, self).__init__(*args, **kwargs)
        self.token_url = 'https://login.microsoftonline.com/57e83b7a-5313-4e94-8647-60ab90ad483a/oauth2/v2.0/token'
        self.client_id = '7b30dbdd-373c-4326-870e-5705e4f53e12'
        self.client_secret = 'SHV8Q~F3vw53zlc6cVZ9d2Tl~QtmDi2HJXRZtcSS'
        self.user = 'automacao@smartbreeder.com.br'
        self.password = 'sT5G@cD6u!'
        self.mail_folder_id = 'AAMkADYxM2NlOTI5LWY1MTktNGMyNy1hNjY1LWJhYWJiNWZiZTgyYwAuAAAAAAC5IPjWJ04VQpYSryQAlxGRAQANCyP7qFxARatkPsS4io6hAAFQX5S3AAA='

    def get_access_token(self):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'username': self.user,
            'password': self.password,
            'scope': 'https://graph.microsoft.com/.default',
            'grant_type': 'password'
        }
        response = requests.post(self.token_url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json().get('access_token')
        else:
            self.log.error(f"Erro ao obter o token: {response.status_code}, {response.text}")
            return None

    def get_emails(self, access_token):
        emails_url = f"https://graph.microsoft.com/v1.0/me/mailFolders/{self.mail_folder_id}/messages"
        headers = {'Authorization': f'Bearer {access_token}'}
        response = requests.get(emails_url, headers=headers)
        if response.status_code == 200:
            return response.json().get('value', [])
        else:
            self.log.error(f"Erro ao obter emails: {response.status_code}, {response.text}")
            return []

    def get_attachments(self, email_id, access_token):
        attachments_url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments"
        headers = {'Authorization': f'Bearer {access_token}'}
        response = requests.get(attachments_url, headers=headers)
        if response.status_code == 200:
            return response.json().get('value', [])
        else:
            self.log.error(f"Erro ao obter anexos do email {email_id}: {response.status_code}, {response.text}")
            return []

    def download_excel(self, attachment, email_id, access_token):
        content_url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments/{attachment['id']}/$value"
        headers = {'Authorization': f'Bearer {access_token}'}
        response = requests.get(content_url, headers=headers)
        if response.status_code == 200:
            return BytesIO(response.content)
        else:
            self.log.error(f"Erro ao baixar anexo {attachment.get('name')} do email {email_id}")
            return None

    def execute(self, context):
        # Step 1: Get access token
        access_token = self.get_access_token()
        if not access_token:
            return

        # Step 2: Get emails
        emails = self.get_emails(access_token)

        # Step 3: Process emails and attachments
        dfs = []
        for email in emails:
            email_id = email['id']
            attachments = self.get_attachments(email_id, access_token)

            for attachment in attachments:
                if attachment.get('contentType') in ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/vnd.ms-excel']:
                    excel_bytes = self.download_excel(attachment, email_id, access_token)
                    if excel_bytes:
                        try:
                            df = pd.read_excel(excel_bytes, engine='openpyxl' if attachment.get('contentType') == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' else None)
                            dfs.append(df)
                            self.log.info(f"Arquivo Excel {attachment.get('name')} lido com sucesso do email {email_id}.")
                        except Exception as e:
                            self.log.error(f"Erro ao ler o arquivo Excel {attachment.get('name')} do email {email_id}: {e}")

        # Step 4: Concatenate dataframes
        if dfs:
            try:
                df_final = pd.concat(dfs, ignore_index=True)
                self.log.info(f"DataFrame final (concatenado): {df_final.head()}")
            except Exception as e:
                self.log.error(f"Erro ao concatenar dataframes: {e}")
        else:
            self.log.info("Nenhum DataFrame encontrado para concatenar.")

# Exemplo de uso do Operator no DAG

from airflow import DAG
from datetime import datetime, timedelta

# Argumentos padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}