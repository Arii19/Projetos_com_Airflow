from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import pandas as pd
from io import BytesIO
import msal
import requests

# Configurações do Microsoft Graph
TENANT_ID = "seu-tenant-id"
CLIENT_ID = "seu-client-id"
CLIENT_SECRET = "seu-client-secret"
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["https://graph.microsoft.com/.default"]

# Função para obter o token de acesso
def get_access_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
    )
    token = app.acquire_token_for_client(scopes=SCOPES)
    return token['access_token']

# Função para enviar o e-mail com anexo e conteúdo HTML
def send_email_with_attachment(receiver_email, subject, html_content, attachment_data, attachment_name):
    access_token = get_access_token()

    # URL da API de envio de e-mail no Microsoft Graph
    url = "https://graph.microsoft.com/v1.0/me/sendMail"

    # Cabeçalho da requisição com o token de acesso
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Corpo do e-mail (HTML) e dados do anexo
    email_body = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "HTML",
                "content": html_content
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": receiver_email
                    }
                }
            ],
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": attachment_name,
                    "contentBytes": attachment_data
                }
            ]
        },
        "saveToSentItems": "true"
    }

    # Envia o e-mail com o anexo
    response = requests.post(url, headers=headers, json=email_body)

    if response.status_code == 202:
        print("E-mail enviado com sucesso.")
    else:
        print(f"Erro ao enviar o e-mail: {response.status_code}")
        print(response.text)

# Exemplo de uso da função dentro do DAG Airflow após o processo de load
def process_load_completion():
    # Lógica de carregamento aqui
    # ...

    # Enviar e-mail após o sucesso do load
    receiver_email = "destinatario@exemplo.com"
    subject = "Processo de carga concluído com sucesso"
    html_content = "<h1>O processo foi concluído!</h1><p>O arquivo foi carregado com sucesso no sistema.</p>"
    
    # Exemplo de anexo (conteúdo em bytes, por exemplo, PDF ou Excel)
    attachment_data = "conteúdo-em-bytes-aqui".encode('base64')  # Codifique o anexo em base64
    attachment_name = "relatorio.pdf"
    
    send_email_with_attachment(receiver_email, subject, html_content, attachment_data, attachment_name)

# Essa função seria chamada no final do seu DAG ou como tarefa final no Airflow
process_load_completion()
