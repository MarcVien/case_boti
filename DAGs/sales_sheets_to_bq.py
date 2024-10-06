from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import requests

# Definir argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1
}

# Caminho do arquivo SQL que será executado
SQL_FILE_PATH = os.path.join(os.path.dirname(__file__), 'sql/create_table_raw_vendas.sql')

# Função para invocar o serviço Cloud Run
def invoke_cloud_run():
    # URL do serviço no Cloud Run
    cloud_run_url = 'https://us-central1-earnest-beacon-437623-b0.cloudfunctions.net/sales_sheets_to_bq'

    # Payload ou dados a serem enviados (se necessário)
    data = {}

    # Headers (se o serviço exigir autorização, adicione o token adequado)
    headers = {
        'Content-Type': 'application/json'
    }
    
    # Fazer a requisição POST
    response = requests.post(cloud_run_url, json=data, headers=headers)
    
    if response.status_code == 200:
        print("Cloud Run invoked successfully.")
    else:
        raise Exception(f"Cloud Run invocation failed: {response.status_code}, {response.text}")

# Definir DAG
with DAG(
    'sales_sheets_to_bq',  # Nome da DAG
    default_args=default_args,
    description='DAG para importar os dados das planilhas no GCS para o BigQuery',
    schedule_interval=None,  # A DAG será executada manualmente
    catchup=False
) as dag:

    # Step 1: Executar o código SQL no BigQuery
    create_raw_table = BigQueryInsertJobOperator(
        task_id='create_raw_table',
        configuration={
            'query': {
                'query': open(SQL_FILE_PATH).read(),  # Carregar a consulta SQL do arquivo
                'useLegacySql': False,  # Usar SQL padrão
            }
        },
        location='US',  # Região do BigQuery (ajuste conforme necessário)
    )

    # Step 2: Invocar o Cloud Run usando PythonOperator
    ingest_raw_sales_data = PythonOperator(
        task_id='ingest_raw_sales_data',
        python_callable=invoke_cloud_run
    )

    # Definir ordem dos passos
    create_raw_table >> ingest_raw_sales_data
