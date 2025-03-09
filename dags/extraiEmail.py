from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

# Importe o operador personalizado
from extrai_email_operator  import MicrosoftGraphEmail

# Configurações Gerais
TENANT_ID = 'SEU_TENANT_ID'
CLIENT_ID = 'SEU_CLIENT_ID'
CLIENT_SECRET = 'SEU_CLIENT_SECRET'
USER = 'SEU_EMAIL'
PASSWORD = 'SUA_SENHA'
MAIL_FOLDER_ID = 'SEU_MAIL_FOLDER_ID'

# Argumentos padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição do DAG
dag = DAG(
    'processar_emails_excel_operador_personalizado',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='DAG para processar emails e anexos Excel com operador personalizado'
)

def concatenar_dataframes(**kwargs):
    dataframes = kwargs['ti'].xcom_pull(task_ids='ler_emails_microsoft_graph')
    if dataframes:
        df_final = pd.concat(dataframes, ignore_index=True)
        print("\nDataFrame final (concatenado):")
        print(df_final.head())
        kwargs['ti'].xcom_push(key='df_final', value=df_final)
    else:
        print("Nenhum DataFrame encontrado para concatenar.")

ler_emails_microsoft_graph = MicrosoftGraphEmailToPandasOperator(
    task_id='ler_emails_microsoft_graph',
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user=USER,
    password=PASSWORD,
    mail_folder_id=MAIL_FOLDER_ID,
    dag=dag
)

concatenar_dataframes_task = PythonOperator(
    task_id='concatenar_dataframes',
    python_callable=concatenar_dataframes,
    provide_context=True,
    dag=dag
)

ler_emails_microsoft_graph >> concatenar_dataframes_task