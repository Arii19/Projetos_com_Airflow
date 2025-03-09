from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import pandas as pd
from io import BytesIO

class MicrosoftGraphEmail(BaseOperator):
    """
    Operador para ler emails e anexos Excel do Microsoft Graph e retornar DataFrames pandas.
    """

    template_fields = ('mail_folder_id',)

    @apply_defaults
    def __init__(self, tenant_id, client_id, client_secret, user, password, mail_folder_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.user = user
        self.password = password
        self.mail_folder_id = mail_folder_id

    def execute(self, context):
        token = self._get_access_token()
        return self._ler_emails_e_anexos(token)

    def _get_access_token(self):
        TOKEN_URL = f'https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'username': self.user,
            'password': self.password,
            'scope': 'https://graph.microsoft.com/.default',
            'grant_type': 'password'
        }
        response = requests.post(TOKEN_URL, headers=headers, data=data)
        if response.status_code == 200:
            return response.json().get('access_token')
        else:
            raise Exception(f"Erro ao obter o token: {response.status_code}, {response.text}")

    def _ler_emails_e_anexos(self, access_token):
        emails_url = f"https://graph.microsoft.com/v1.0/me/mailFolders/{self.mail_folder_id}/messages"
        headers = {'Authorization': f'Bearer {access_token}'}
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
                    attachment_content_response.raise_for_status()
                    excel_data = attachment_content_response.content
                    excel_bytes = BytesIO(excel_data)
                    df = pd.read_excel(excel_bytes, engine='openpyxl' if attachment.get('contentType') == 'application/vnd.ms-excel' else None)
                    dfs.append(df)
                    print(f"Arquivo Excel {attachment.get('name', 'sem nome')} lido do email {email_id}.")
        return dfs