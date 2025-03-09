from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import pandas as pd
from io import BytesIO

class EmailAttachmentToDataFrameOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self, tenant_id, client_id, client_secret, user, password, mail_folder_id, *args, **kwargs):
        super(EmailAttachmentToDataFrameOperator, self).__init__(*args, **kwargs)
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.user = user
        self.password = password
        self.mail_folder_id = mail_folder_id
        self.token_url = f'https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token'

    # Função para obter o token de acesso via Microsoft Graph
    def get_access_token(self):
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
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

    # Função para ler emails e anexos
    def ler_emails_e_anexos(self, access_token):
        emails_url = f"https://graph.microsoft.com/v1.0/me/mailFolders/{self.mail_folder_id}/messages"
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        response = requests.get(emails_url, headers=headers)
        response.raise_for_status()
        emails = response.json().get('value', [])
        dfs = []

        for email in emails:
            email_id = email['id']
            attachments_url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments"
            attachments_response = requests.get(attachments_url, headers=headers)
            attachments_response.raise_for_status()
            attachments = attachments_response.json().get('value', [])

            for attachment in attachments:
                if attachment.get('contentType') in ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/vnd.ms-excel']:
                    attachment_content_url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments/{attachment['id']}/$value"
                    attachment_content_response = requests.get(attachment_content_url, headers=headers)
                    excel_data = attachment_content_response.content
                    excel_bytes = BytesIO(excel_data)

                    try:
                        df = pd.read_excel(excel_bytes, engine='openpyxl')
                        dfs.append(df)
                        self.log.info(f"Arquivo Excel {attachment.get('name')} lido com sucesso.")
                    except Exception as e:
                        self.log.error(f"Erro ao ler o arquivo Excel {attachment.get('name')}: {e}")

        return dfs

    def execute(self, context):
        # Obtenção do token
        token = self.get_access_token()
        if token:
            self.log.info("Token obtido com sucesso!")
            # Ler emails e anexos
            dataframes = self.ler_emails_e_anexos(token)
            if dataframes:
                self.log.info(f"Encontrados {len(dataframes)} arquivos Excel.")
                return dataframes  # Retorna lista de DataFrames
            else:
                self.log.info("Nenhum arquivo Excel encontrado.")
                return None
        else:
            raise ValueError("Erro ao obter o token de acesso.")
