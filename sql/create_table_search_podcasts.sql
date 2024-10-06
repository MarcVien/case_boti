CREATE OR REPLACE TABLE `earnest-beacon-437623-b0.raw_spotify.search_podcasts`
(
    id STRING OPTIONS(description = 'Identificador único do podcast'),
    name STRING OPTIONS(description = 'Nome do podcast'),
    description STRING OPTIONS(description = 'Descrição do podcast'),
    total_episodes STRING OPTIONS(description = 'Total de episódios lançados pelo podcast')
)
OPTIONS(description = 'Tabela contendo informações sobre podcasts recuperados por meio de busca no Spotify, incluindo nome, descrição e número de episódios')
;
