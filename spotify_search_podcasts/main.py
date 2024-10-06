import requests
import json
import pandas as pd
from google.cloud import bigquery

# Obter o token de autenticação do Spotify
def get_spotify_token(client_id, client_secret):
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    response = requests.post(url, headers=headers, data=data)
    
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        raise Exception(f"Failed to get token: {response.text}")

# Realizar busca de podcasts no Spotify
def search_podcasts(term, token, limit=50):
    url = "https://api.spotify.com/v1/search"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    params = {
        "q": term,
        "type": "show",  # 'show' é o tipo que identifica podcasts
        "market": "BR", # necessário passar um market, senão os resultados virão nulos
        "limit": limit  # máximo de 50
    }
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Search failed: {response.text}")

# Função para inserir dados no BigQuery
def insert_podcasts_into_bq(podcasts, project_id, table_full_id):
    # Conectar ao cliente BigQuery
    client = bigquery.Client()
    
    # Criar DataFrame com os dados dos podcasts
    podcast_data = []
    for podcast in podcasts:
        podcast_data.append({
            'id': podcast['id'],
            'name': podcast['name'],
            'description': podcast['description'],
            'total_episodes': str(podcast['total_episodes'])
        })
    
    df_podcasts = pd.DataFrame(podcast_data)
    
    # Inserir os dados no BigQuery
    job = client.load_table_from_dataframe(df_podcasts, table_full_id)
    job.result()  # Esperar a conclusão do job
    
    print(f"Inserção de {len(df_podcasts)} podcasts concluída com sucesso na tabela {table_full_id}.")


def main(requests):
    # Configurações de projeto
    project_id = 'earnest-beacon-437623-b0'
    dataset_id = 'raw_spotify'
    table_id = 'search_podcasts'

    # Configurações da API do Spotify
    client_id = "b66f3746266a4751a4466901490672c7"
    client_secret = "0f0a7dba9b974602ba4c957aea8f7877"
    
    try:
        # Obter token de acesso
        token = get_spotify_token(client_id, client_secret)

        # Realizar busca de podcasts com o termo "data hackers"
        search_results = search_podcasts("data hackers", token, limit=50)

        # Exibir os resultados (apenas títulos dos podcasts)
        podcasts = search_results.get("shows", {}).get("items", [])

        # Definir o ID completo da tabela no BigQuery
        table_full_id = f"{project_id}.{dataset_id}.{table_id}"

        # Inserir os resultados no BQ
        insert_podcasts_into_bq(podcasts, project_id, table_full_id)

        return "Pesquisa dos podcasts realizada com sucesso"

    except Exception as e:
        print(f"Erro: {e}")
