CREATE OR REPLACE TABLE `earnest-beacon-437623-b0.raw_spotify.data_hackers_episodes` (
    id STRING NOT NULL,             -- Identificação do episódio
    name STRING NOT NULL,           -- Nome do episódio
    description STRING,             -- Descrição do episódio
    release_date STRING,            -- Data de lançamento do episódio 
    duration_ms STRING,             -- Duração em milissegundos do episódio
    language STRING,                -- Idioma do episódio
    explicit STRING,                -- Flag se o episódio possui conteúdo explícito
    type STRING                     -- O tipo de faixa de áudio (ex: música, programa)
);