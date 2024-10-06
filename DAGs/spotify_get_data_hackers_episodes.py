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
SQL_FILE_PATH_RAW = os.path.join(os.path.dirname(__file__), 'sql/create_table_raw_data_hackers_episodes.sql')
SQL_FILE_PATH_PROCEDURE = os.path.join(os.path.dirname(__file__), 'sql/create_procedure_data_hackers_episodes_boticario.sql')


# Função para invocar o serviço Cloud Run
def invoke_cloud_run():
    # URL do serviço no Cloud Run
    cloud_run_url = 'https://us-central1-earnest-beacon-437623-b0.cloudfunctions.net/spotify_data_hackers'

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
    'spotify_get_data_hackers_episodes',  # Nome da DAG
    default_args=default_args,
    description='DAG para retornar todos os episodios do programa Data Hackers e listar somente os nos quais o Boticário participa',
    schedule_interval=None,  # A DAG será executada manualmente
    catchup=False
) as dag:

    # Step 1: Executar o código SQL no BigQuery
    create_raw_table = BigQueryInsertJobOperator(
        task_id='create_raw_table',
        configuration={
            'query': {
                'query': open(SQL_FILE_PATH_RAW).read(),  # Carregar a consulta SQL do arquivo
                'useLegacySql': False,  # Usar SQL padrão
            }
        },
        location='US',  # Região do BigQuery (ajuste conforme necessário)
    )

    # Step 2: Invocar o Cloud Run usando PythonOperator
    ingest_raw_podcasts_data = PythonOperator(
        task_id='ingest_raw_podcasts_data',
        python_callable=invoke_cloud_run
    )

    # Step 3: Criar a Procedure para gerar a trusted e buscar as participações do Boticário
    create_routine = BigQueryInsertJobOperator(
        task_id='create_routine',
        configuration={
            'query': {
                'query': open(SQL_FILE_PATH_PROCEDURE).read(),  # Carregar a consulta SQL do arquivo
                'useLegacySql': False,  # Usar SQL padrão
            }
        },
        location='US',  # Região do BigQuery (ajuste conforme necessário)
    )

    # Step 4: Executar a Routine
    bigquery_query = """
    CALL `procedure.refined_spotify`();
    """

    # Tarefa para executar a rotina salva no BigQuery
    execute_routine = BigQueryInsertJobOperator(
        task_id='execute_routine',
        configuration={
            "query": {
                "query": bigquery_query,
                "useLegacySql": False,  # Estamos usando SQL padrão, não legacy
            }
        },
        location='US',  # Localização do dataset BigQuery (exemplo: US, EU)
    )

    # Definir ordem dos passos
    create_raw_table >> ingest_raw_podcasts_data >> create_routine >> execute_routine