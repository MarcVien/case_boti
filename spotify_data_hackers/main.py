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

# Realizar busca no BigQuery para obter o ID do podcast com base no nome
def get_podcast_id_from_bq(podcast_name, table_full_id):
    client = bigquery.Client()
    
    query = f"""
    SELECT id
    FROM `{table_full_id}`
    WHERE name = '{podcast_name}'
    LIMIT 1
    """
    
    query_job = client.query(query)
    results = query_job.result()
    
    for row in results:
        return row['id']  # Retorna o primeiro ID encontrado

    raise Exception(f"Podcast '{podcast_name}' não encontrado no BigQuery.")


# Lista os episódios de um podcast
def list_episodes(podcast_id, token):
    url = f"https://api.spotify.com/v1/shows/{podcast_id}/episodes"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    params = {
        "market": "BR",  # mercado para os resultados
        "limit": 50      # limite de episódios por chamada
    }
    
    episodes = []
    while True:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            episodes.extend(data['items'])
            
            # Verifica se há mais episódios
            if data['next']:
                url = data['next']  # Atualiza a URL para a próxima página
            else:
                break  # Sai do loop se não houver mais episódios
        else:
            raise Exception(f"Failed to get episodes: {response.text}")

    return episodes

# Função para inserir dados no BigQuery
def insert_episodes_into_bq(episodes, table_full_id):
    # Conectar ao cliente BigQuery
    client = bigquery.Client()
    
    # Criar DataFrame com os dados dos podcasts
    episodes_data = []
    for episode in episodes:
        episodes_data.append({
            'id': str(episode['id']),
            'name': str(episode['name']),
            'description': str(episode['description']),
            'release_date': str(episode['release_date']),
            'duration_ms': str(episode['duration_ms']),
            'language': str(episode.get('language', 'unknown')),
            'explicit': str(episode['explicit']),
            'type': str(episode['type'])
        })
    
    df_episodes = pd.DataFrame(episodes_data)
    
    # Inserir os dados no BigQuery
    job = client.load_table_from_dataframe(df_episodes, table_full_id)
    job.result()  # Esperar a conclusão do job


def main(requests):
    # Configurações de projeto
    project_id = 'earnest-beacon-437623-b0'
    dataset_id = 'raw_spotify'
    table_id_episodes = 'data_hackers_episodes'
    table_id_podcasts = 'search_podcasts'

    # Configurações da API do Spotify
    client_id = "b66f3746266a4751a4466901490672c7"
    client_secret = "0f0a7dba9b974602ba4c957aea8f7877"
    
    try:
        # Obter token de acesso
        token = get_spotify_token(client_id, client_secret)

        # Definir o ID completo das tabelas no BigQuery
        table_full_id_episodes = f"{project_id}.{dataset_id}.{table_id_episodes}"
        table_full_id_podcasts = f"{project_id}.{dataset_id}.{table_id_podcasts}"

        # Obter o ID do Podcast Data Hackers
        podcast_id = get_podcast_id_from_bq("Data Hackers", table_full_id_podcasts)

        # Exibir os resultados (apenas títulos dos podcasts)
        episodes = list_episodes(podcast_id, token)

        # Inserir os resultados no BQ
        insert_episodes_into_bq(episodes, table_full_id_episodes)

        return(f"Inserção de {len(episodes)} podcasts concluída com sucesso na tabela {table_full_id_episodes}.")

    except Exception as e:
        print(f"Erro: {e}")
