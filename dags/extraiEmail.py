from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from custom_operators import EmailAttachmentToDataFrameOperator

# Função para processar os DataFrames
def process_dataframes(**context):
    dataframes = context['task_instance'].xcom_pull(task_ids='extract_emails_and_attachments')
    
    if dataframes:
        for i, df in enumerate(dataframes):
            print(f"DataFrame {i+1}:")
            print(df.head())  # Processar cada DataFrame como necessário

# Definição do DAG
with DAG('email_excel_to_dataframe_dag',
         start_date=datetime(2025, 3, 9),
         schedule_interval='@daily',
         catchup=False) as dag:

    # Tarefa: Extrair emails e anexos para DataFrame
    extract_task = EmailAttachmentToDataFrameOperator(
        task_id='extract_emails_and_attachments',
        tenant_id='57e83b7a-5313-4e94-8647-60ab90ad483a',
        client_id='7b30dbdd-373c-4326-870e-5705e4f53e12',
        client_secret='SHV8Q~F3vw53zlc6cVZ9d2Tl~QtmDi2HJXRZtcSS',
        user='automacao@smartbreeder.com.br',
        password='sT5G@cD6u!',
        mail_folder_id='AAMkADYxM2NlOTI5LWY1MTktNGMyNy1hNjY1LWJhYWJiNWZiZTgyYwAuAAAAAAC5IPjWJ04VQpYSryQAlxGRAQANCyP7qFxARatkPsS4io6hAAFQX5S3AAA=',
    )

    # Tarefa: Processar os DataFrames extraídos
    process_task = PythonOperator(
        task_id='process_dataframes',
        python_callable=process_dataframes,
        provide_context=True
    )

    extract_task >> process_task
