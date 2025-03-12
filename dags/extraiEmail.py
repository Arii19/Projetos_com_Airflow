from airflow.models import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.dates import days_ago
import requests
import pandas as pd
from io import BytesIO
from datetime import timedelta

# Definindo o operador EmailOperator
class EmailOperator(BaseOperator):
    """
    Operador para ler emails e anexos Excel do Microsoft Graph e retornar DataFrames pandas.
    """

    @apply_defaults
    def __init__(self, name: str, tenant_id: str, client_id: str, client_secret: str, user: str, password: str, mail_folder_id: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.name = name
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.user = user
        self.password = password
        self.mail_folder_id = mail_folder_id

    def get_access_token(self):
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

    def ler_emails_e_anexos(self, access_token):
        emails_url = f"https://graph.microsoft.com/v1.0/me/mailFolders/{self.mail_folder_id}/messages"
        headers = {'Authorization': f'Bearer {access_token}'}
        try:
            response = requests.get(emails_url, headers=headers)
            response.raise_for_status()
            emails = response.json().get('value', [])
            print(f"{len(emails)} emails encontrados.")
            for email in emails:
                email_id = email['id']
                attachments_url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments"
                try:
                    attachments_response = requests.get(attachments_url, headers=headers)
                    attachments_response.raise_for_status()
                    attachments = attachments_response.json().get('value', [])
                    print(f"{len(attachments)} anexos encontrados no email {email_id}.")
                    for attachment in attachments:
                        if attachment.get('contentType') in ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/vnd.ms-excel']:
                            print(f"Lendo anexo Excel {attachment.get('name', 'sem nome')} do email {email_id}...")
                            attachment_content_url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments/{attachment['id']}/$value"
                            attachment_content_response = requests.get(attachment_content_url, headers=headers)
                            attachment_content_response.raise_for_status()
                            excel_data = attachment_content_response.content
                            excel_bytes = BytesIO(excel_data)
                            try:
                                df = pd.read_excel(excel_bytes)
                                print(f"Arquivo Excel {attachment.get('name', 'sem nome')} lido do email {email_id}.")
                                return df
                            except Exception as e:
                                print(f"Erro ao ler o arquivo Excel {attachment.get('name', 'sem nome')} do email {email_id}: {e}")
                except requests.exceptions.RequestException as e:
                    print(f"Erro ao obter anexos do email {email_id}: {e}")
        except requests.exceptions.RequestException as e:
            print(f"Erro ao obter emails: {e}")
        return None

    def execute(self, context):
        # Obtém o token de acesso
        access_token = self.get_access_token()
        
        # Lê os e-mails e anexos (Excel)
        df = self.ler_emails_e_anexos(access_token)
        
        # Retorna o DataFrame lido, se existir
        if df is not None:
            return df
        else:
            raise ValueError("Nenhum DataFrame foi retornado dos anexos.")

# Definindo o DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_leitura_email',
    default_args=default_args,
    description='DAG para ler emails e anexos Excel via Microsoft Graph',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    
    # Instanciando o operador EmailOperator
    ler_email = EmailOperator(
        task_id='ler_email_anexos',
        name="Leitura de Email e Anexos",
        tenant_id='seu-tenant-id',
        client_id='seu-client-id',
        client_secret='seu-client-secret',
        user='seu-usuario',
        password='sua-senha',
        mail_folder_id='folder-id-dos-emails',
    )

    ler_email
