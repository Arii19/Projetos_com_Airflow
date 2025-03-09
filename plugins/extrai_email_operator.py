import requests
import pandas as pd
from io import BytesIO
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class ExtraiEmailOperator(BaseOperator):
    @apply_defaults  # argumentos que eu quero receber
    def __init__(self, tenant_id, client_id, client_secret, user, password, mail_folder_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.user = user
        self.password = password
        self.mail_folder_id = 'AAMkADYxM2NlOTI5LWY1MTktNGMyNy1hNjY1LWJhYWJiNWZiZTgyYwAuAAAAAAC5IPjWJ04VQpYSryQAlxGRAQANCyP7qFxARatkPsS4io6hAAFQX5S3AAA='
        self.token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'

    def get_access_token(self):
        # Autentica no Microsoft Graph e retorna um token de acesso.
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'username': self.user,
            'password': self.password,
            'scope': 'https://graph.microsoft.com/.default',
            'grant_type': 'password'
        }
        response = requests.post(self.token_url, headers=headers, data=data) #Corrigido, o response estava comentado.
        if response.status_code == 200:
            return response.json().get('access_token')
        else:
            raise Exception(f"Erro ao obter token: {response.text}")

    def execute(self, context):
        """Executa a extração de anexos do Outlook."""
        access_token = self.get_access_token()
        emails_url = f"https://graph.microsoft.com/v1.0/me/mailFolders/{self.mail_folder_id}/messages"
        headers = {'Authorization': f'Bearer {access_token}'}
        response = requests.get(emails_url, headers=headers)
        response.raise_for_status()
        emails = response.json().get('value', [])
        dataframes = []
        for email in emails:
            email_id = email['id']
            attachments_url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments"
            attachments_response = requests.get(attachments_url, headers=headers)
            attachments_response.raise_for_status()
            attachments = attachments_response.json().get('value', [])
            for attachment in attachments:
                if attachment.get('contentType') in ['application/vnd.openxmlformatsofficedocument.spreadsheetml.sheet', 'application/vnd.ms-excel']:
                    attachment_content_url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments/{attachment['id']}/$value"
                    attachment_content_response = requests.get(attachment_content_url, headers=headers)
                    attachment_content_response.raise_for_status()
                    excel_data = BytesIO(attachment_content_response.content)
                    try:
                        df = pd.read_excel(excel_data, engine='openpyxl')
                        dataframes.append(df)
                        self.log.info(f"Arquivo {attachment.get('name', 'sem nome')} extraído com sucesso.")
                    except Exception as e:
                        self.log.error(f"Erro ao ler arquivo: {e}")
        return dataframes