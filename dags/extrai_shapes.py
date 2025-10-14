 
from airflow.decorators import dag, task
import pandas as pd
from io import StringIO
from airflow.models import Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
from shapely import wkt
from airflow.utils.trigger_rule import TriggerRule

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
import zipfile
import base64

def extract_shp_from_email():
    # 🔹 Variáveis do Airflow
    client_id = Variable.get("ClientId_MicrosoftGraph_Automacao")
    client_secret = Variable.get("ClientSecret_MicrosoftGraph_Automacao")
    tenant_id = Variable.get("TentantId_MicrosoftGraph_Automacao")
    user_email = Variable.get("Email_MicrosoftGraph_Automacao")

    # 🔹 Autenticação no Microsoft Graph
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    }
    token_response = requests.post(token_url, data=token_data)
    token_response.raise_for_status()
    access_token = token_response.json()["access_token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    # 🔹 Listar mensagens com anexos
    messages_url = (
    f"https://graph.microsoft.com/v1.0/users/{user_email}/mailFolders/Inbox/messages?"
    f"$filter=hasAttachments eq true and isRead eq false"
)
    messages_response = requests.get(messages_url, headers=headers)
    messages_response.raise_for_status()
    messages = messages_response.json().get("value", [])

    if not messages:
        print("Nenhum e-mail com anexo encontrado.")
        return

    # 🔹 Criar pasta local para salvar os arquivos
    save_dir = "/opt/airflow/dags/files_shp"
    zip_dir = "/opt/airflow/dags/temp_zip"
    os.makedirs(save_dir, exist_ok=True)
    os.makedirs(zip_dir, exist_ok=True)

    for msg in messages:
        msg_id = msg["id"]
        attachments_url = f"https://graph.microsoft.com/v1.0/users/{user_email}/messages/{msg_id}/attachments"
        attachments_response = requests.get(attachments_url, headers=headers)
        attachments_response.raise_for_status()
        attachments = attachments_response.json().get("value", [])

        for attachment in attachments:
            # Verifica se o anexo é um arquivo ZIP ou SHP
            if attachment["@odata.type"] == "#microsoft.graph.fileAttachment":
                file_name = attachment["name"]
                file_content_b64 = attachment["contentBytes"]
                
                # Decodifica o conteúdo base64
                file_content = base64.b64decode(file_content_b64)
                
                if file_name.endswith(".zip"):
                    # Salva o arquivo ZIP temporariamente
                    zip_path = os.path.join(zip_dir, file_name)
                    with open(zip_path, "wb") as f:
                        f.write(file_content)
                    print(f"Arquivo ZIP salvo temporariamente: {zip_path}")
                    
                    # Extrai arquivos SHP do ZIP
                    try:
                        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                            for zip_info in zip_ref.infolist():
                                if zip_info.filename.endswith(('.shp', '.shx', '.dbf', '.prj')):
                                    # Extrai arquivo SHP e seus componentes
                                    extracted_path = zip_ref.extract(zip_info, save_dir)
                                    print(f"Arquivo extraído do ZIP: {extracted_path}")
                    except zipfile.BadZipFile:
                        print(f"Erro: {file_name} não é um arquivo ZIP válido")
                    
                    # Remove o arquivo ZIP temporário
                    os.remove(zip_path)
                    
                elif file_name.endswith(".shp"):
                    # Arquivo SHP direto (sem ZIP)
                    file_path = os.path.join(save_dir, file_name)
                    with open(file_path, "wb") as f:
                        f.write(file_content)
                    print(f"Arquivo SHP salvo: {file_path}")

    # Remove pasta temporária se estiver vazia
    try:
        os.rmdir(zip_dir)
    except OSError:
        pass

    print("Extração concluída com sucesso ✅")

# 🔹 Configuração da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="extract_shp_from_email_microsoft_graph",
    default_args=default_args,
    description="Extrai arquivos SHP de e-mails (ZIP ou diretos) usando Microsoft Graph",
    schedule_interval=None,
    start_date=datetime(2025, 10, 14),
    catchup=False,
    tags=["microsoft_graph", "shp", "zip", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_shp_from_email",
        python_callable=extract_shp_from_email,
    )

    extract_task
