from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from extrai_email_operator import MicrosoftGraphEmailToPandasOperator # ajuste o nome do arquivo

# Configurações Gerais
TENANT_ID = '57e83b7a-5313-4e94-8647-60ab90ad483a'
CLIENT_ID = '7b30dbdd-373c-4326-870e-5705e4f53e12'
CLIENT_SECRET = 'SHV8Q~F3vw53zlc6cVZ9d2Tl~QtmDi2HJXRZtcSS'
USER = 'automacao@smartbreeder.com.br'
PASSWORD = 'sT5G@cD6u!'  
MAIL_FOLDER_ID = 'AAMkADYxM2NlOTI5LWY1MTktNGMyNy1hNjY1LWJhYWJiNWZiZTgyYwAuAAAAAAC5IPjWJ04VQpYSryQAlxGRAQANCyP7qFxARatkPsS4io6hAAFQX5S3AAA=' 

# Argumentos padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição do DAG
dag = DAG(
    'processar_emails_excel_operador',
    default_args=default_args,
    schedule_interval='@daily',  # ou outro intervalo de agendamento desejado
    catchup=False,
    description='DAG para processar emails e anexos Excel com operador personalizado'
)

def concatenar_dataframes(**kwargs):
    """Concatena os DataFrames recebidos via XCom."""
    dataframes = kwargs['ti'].xcom_pull(task_ids='ler_emails_microsoft')
    if dataframes:
        try:
            df_final = pd.concat(dataframes, ignore_index=True)
            print("\nDataFrame final (concatenado):")
            print(df_final.head())
            kwargs['ti'].xcom_push(key='df_final', value=df_final)  # Armazena DataFrame concatenado no XCom
        except Exception as e:
            print(f"Erro ao concatenar DataFrames: {e}")
    else:
        print("Nenhum DataFrame encontrado para concatenar.")

# Instanciação do operador personalizado
ler_emails_microsoft = MicrosoftGraphEmailToPandasOperator(
    task_id='ler_emails_microsoft',
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user=USER,
    password=PASSWORD,
    mail_folder_id=MAIL_FOLDER_ID,
    dag=dag
)

# Tarefa de concatenação
concatenar_dataframes_task = PythonOperator(
    task_id='concatenar_dataframes',
    python_callable=concatenar_dataframes,
    provide_context=True,
    dag=dag
)

# Definição das dependências
ler_emails_microsoft >> concatenar_dataframes_task