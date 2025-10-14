 
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
import geopandas as gpd

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
        return None

    # 🔹 Criar pasta local para salvar os arquivos
    save_dir = "/opt/airflow/dags/files_shp"
    zip_dir = "/opt/airflow/dags/temp_zip"
    os.makedirs(save_dir, exist_ok=True)
    os.makedirs(zip_dir, exist_ok=True)

    shp_files = []  # Lista para armazenar caminhos dos arquivos SHP

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
                    
                    # Extrai apenas arquivos SHP do ZIP
                    try:
                        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                            for zip_info in zip_ref.infolist():
                                if zip_info.filename.endswith('.shp'):
                                    # Extrai apenas arquivo SHP
                                    extracted_path = zip_ref.extract(zip_info, save_dir)
                                    shp_files.append(extracted_path)
                                    print(f"Arquivo SHP extraído do ZIP: {extracted_path}")
                    except zipfile.BadZipFile:
                        print(f"Erro: {file_name} não é um arquivo ZIP válido")
                    
                    # Remove o arquivo ZIP temporário
                    os.remove(zip_path)
                    
                elif file_name.endswith(".shp"):
                    # Arquivo SHP direto (sem ZIP)
                    file_path = os.path.join(save_dir, file_name)
                    with open(file_path, "wb") as f:
                        f.write(file_content)
                    shp_files.append(file_path)
                    print(f"Arquivo SHP salvo: {file_path}")

    # Remove pasta temporária se estiver vazia
    try:
        os.rmdir(zip_dir)
    except OSError:
        pass

    # 🔹 Carregar arquivos SHP em um DataFrame
    if shp_files:
        # Se houver múltiplos arquivos SHP, concatena todos em um DataFrame
        shp_dataframes = []
        for shp_file in shp_files:
            try:
                df_temp = gpd.read_file(shp_file)
                df_temp['source_file'] = os.path.basename(shp_file)  # Adiciona coluna com nome do arquivo
                shp_dataframes.append(df_temp)
                print(f"Arquivo SHP carregado: {shp_file} - {len(df_temp)} registros")
            except Exception as e:
                print(f"Erro ao carregar {shp_file}: {str(e)}")
        
        if shp_dataframes:
            # Concatena todos os DataFrames em um só
            shp = pd.concat(shp_dataframes, ignore_index=True)
            print(f"DataFrame 'shp' criado com {len(shp)} registros totais")
            print(f"Colunas disponíveis: {list(shp.columns)}")
            return shp
        else:
            print("Nenhum arquivo SHP foi carregado com sucesso")
            return None
    else:
        print("Nenhum arquivo SHP encontrado")
        return None

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
        do_xcom_push=True,  # Permite que o retorno seja usado em outras tasks
    )

    extract_task
