CREATE OR REPLACE PROCEDURE procedure.refined_spotify() 
BEGIN 

-- Tabela trusted dos dados de podcasts do Data Hackers
CREATE OR REPLACE TABLE trusted_spotify.data_hackers_episodes AS(
    SELECT
        DISTINCT CAST(id AS STRING) AS id_podcast,
        CAST(name AS STRING) AS st_name,
        CAST(description AS STRING) AS st_description,
        CAST(release_date AS DATE) AS dt_release_date,
        CAST(duration_ms AS INT64) AS it_duration_ms,
        CAST(language AS STRING) as st_language,
        CAST(explicit AS BOOL) AS bl_explicit,
        CAST(type AS STRING) AS st_type
    FROM
        `earnest-beacon-437623-b0.raw_spotify.data_hackers_episodes`
);

-- Tabela com os podcasts onde o boticário participou
CREATE
OR REPLACE TABLE refined_spotify.data_hackers_episodes_boticario AS(
    SELECT
        *
    FROM
        `earnest-beacon-437623-b0.trusted_spotify.data_hackers_episodes`
    WHERE
        CONTAINS_SUBSTR(st_name, 'Boticário')
        OR CONTAINS_SUBSTR(st_description, 'Boticário')
);

END