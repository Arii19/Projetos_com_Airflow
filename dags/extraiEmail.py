from airflow import DAG
from datetime import datetime, timedelta
from extrai_email_operator import ProcessEmailExcelOperator  # Substitua pelo caminho correto do arquivo onde o operator está salvo

# Argumentos padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição do DAG
with DAG(
    'processar_emails_excel_operator_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Ajuste conforme necessário
    catchup=False,
    description='DAG usando Operator customizado para processar emails e anexos Excel do Microsoft Graph'
) as dag:

    # Tarefa que usa o Operator customizado
    process_emails_task = ProcessEmailExcelOperator(
        task_id='process_emails_excel'
    )

    # Configuração da dependência (nesse caso, apenas uma tarefa)
    process_emails_task
