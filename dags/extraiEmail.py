from airflow import DAG
from airflow.utils.dates import days_ago
from extrai_email_operator import ExtraiEmailOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "extract_outlook_attachments",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["outlook", "email", "etl"]
)

extract_emails = ExtraiEmailOperator(
    task_id="extract_emails",
    tenant_id="57e83b7a-5313-4e94-8647-60ab90ad483a",
    client_id="7b30dbdd-373c-4326-870e-5705e4f53e12",
    client_secret="SHV8Q~F3vw53zlc6cVZ9d2Tl~QtmDi2HJXRZtcSS",
    user="automacao@smartbreeder.com.br",
    password="sT5G@cD6u!",
    mail_folder_id="AAMkADYxM2NlOTI5LWY1MTktNGMyNy1hNjY1LWJhYWJiNWZiZTgyYwAuAAAAAAC5IPjWJ04VQpYSryQAlxGRAQANCyP7qFxARatkPsS4io6hAAFQX5S3AAA="
)

extract_emails