CREATE OR REPLACE TABLE `earnest-beacon-437623-b0.raw_spotify.data_hackers_episodes`
(
    id STRING NOT NULL OPTIONS(description = 'Identificação do episódio'),
    name STRING NOT NULL OPTIONS(description = 'Nome do episódio'),
    description STRING OPTIONS(description = 'Descrição do episódio'),
    release_date STRING OPTIONS(description = 'Data de lançamento do episódio'),
    duration_ms STRING OPTIONS(description = 'Duração em milissegundos do episódio'),
    language STRING OPTIONS(description = 'Idioma do episódio'),
    explicit STRING OPTIONS(description = 'Flag indicando se o episódio possui conteúdo explícito'),
    type STRING OPTIONS(description = 'Tipo de faixa de áudio (música, programa)')
)
OPTIONS(description = 'Tabela contendo informações sobre episódios de podcasts, incluindo detalhes como nome, descrição e duração');