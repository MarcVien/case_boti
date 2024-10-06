from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os

# Definir argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1
}

# Caminho do arquivo SQL que será executado
SQL_FILE_PATH = os.path.join(os.path.dirname(__file__), 'sql/create_procedure_consolidados.sql')

# Definir DAG
with DAG(
    'procedure_consolidados',  # Nome da DAG
    default_args=default_args,
    description='DAG gerar a camada trusted e os dados consolidados das vendas',
    schedule_interval=None,  # A DAG será executada manualmente
    catchup=False
) as dag:

    # Step 1: Executar o código SQL no BigQuery
    create_routine = BigQueryInsertJobOperator(
        task_id='create_routine',
        configuration={
            'query': {
                'query': open(SQL_FILE_PATH).read(),  # Carregar a consulta SQL do arquivo
                'useLegacySql': False,  # Usar SQL padrão
            }
        },
        location='US',  # Região do BigQuery (ajuste conforme necessário)
    )

    # Definindo procedure a ser executada
    bigquery_query = """
    CALL `procedure.refined_consolidado`();
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
    create_routine >> execute_routine
