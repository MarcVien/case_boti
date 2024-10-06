import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from io import BytesIO

files = [
    'base_2017.xlsx',
    'base_2018.xlsx',
    'base_2019.xlsx'
]

# Função para ler arquivos XLSX do Google Cloud Storage
def read_xlsx_from_gcs(bucket_name, file_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_bytes()  # Lê o arquivo como bytes
    return pd.read_excel(BytesIO(data), engine='openpyxl')  # Lê o arquivo XLSX

def main(request):
  # Configurações de projeto
  project_id = 'earnest-beacon-437623-b0'
  dataset_id = 'raw_vendas'
  table_id = 'vendas'
  bucket_name = 'case-boti-sheets'
 
  dataframes = []

  # Lendo as planilhas e armazenando em dataframes
  for file in files:
    df = read_xlsx_from_gcs(bucket_name, file)
    df.ID_MARCA = df.ID_MARCA.astype(str)
    df.ID_LINHA = df.ID_LINHA.astype(str)
    df.DATA_VENDA = df.DATA_VENDA.astype(str)
    df.QTD_VENDA = df.QTD_VENDA.astype(str)
    dataframes.append(df)

  # Combinando os dataframes gerados
  combined_df = pd.concat(dataframes)

  # Inicializa o cliente BigQuery
  bq_client = bigquery.Client()

  # Configura o job de inserção no BigQuery
  job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
  )

  # Carrega os dados para o BigQuery
  table_full_id = f"{project_id}.{dataset_id}.{table_id}"
  load_job = bq_client.load_table_from_dataframe(
    combined_df,
    table_full_id,
    job_config=job_config,
  )

  # Aguarda a conclusão do job
  load_job.result()

  return "Dados das planilhas importados ao BQ com sucesso"