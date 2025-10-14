 
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

def extract_shp_from_email():
    # 🔹 Variáveis do Airflow
    client_id = Variable.get("ClientId_MicrosoftGraph_Automacao")
    client_secret = Variable.get("ClientSecret_MicrosoftGraph_Automacao")
    tenant_id = Variable.get("TenantId_MicrosoftGraph_Automacao")
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
    os.makedirs(save_dir, exist_ok=True)

    for msg in messages:
        msg_id = msg["id"]
        attachments_url = f"https://graph.microsoft.com/v1.0/users/{user_email}/messages/{msg_id}/attachments"
        attachments_response = requests.get(attachments_url, headers=headers)
        attachments_response.raise_for_status()
        attachments = attachments_response.json().get("value", [])

        for attachment in attachments:
            # Verifica se o anexo é um arquivo e se é .shp
            if attachment["@odata.type"] == "#microsoft.graph.fileAttachment" and attachment["name"].endswith(".shp"):
                file_name = attachment["name"]
                file_content = attachment["contentBytes"]

                file_path = os.path.join(save_dir, file_name)
                with open(file_path, "wb") as f:
                    f.write(bytes(file_content, "utf-8"))
                print(f"Arquivo SHP salvo: {file_path}")

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
    description="Extrai arquivos SHP de e-mails usando Microsoft Graph",
    schedule_interval=None,
    start_date=datetime(2025, 10, 14),
    catchup=False,
    tags=["microsoft_graph", "shp", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_shp_from_email",
        python_callable=extract_shp_from_email,
    )

    extract_task
