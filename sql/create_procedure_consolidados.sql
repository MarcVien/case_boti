CREATE OR REPLACE PROCEDURE procedure.refined_consolidado()

BEGIN 

CREATE OR REPLACE TABLE trusted_vendas.vendas AS(
  SELECT
    DISTINCT CAST(id_marca AS INT64) AS id_marca,
    CAST(marca AS STRING) AS st_marca,
    CAST(id_linha AS INT64) AS id_linha,
    CAST(linha AS STRING) AS st_linha,
    CAST(data_venda AS DATE) AS dt_venda,
    CAST(qtd_venda AS INT64) AS vlr_qtd_venda
  FROM
    `earnest-beacon-437623-b0.raw_vendas.vendas`
);

--
-- Tabela 1: Consolidado de vendas por ano e mês;
CREATE OR REPLACE TABLE refined_vendas.vendas_ano_mes AS(
 SELECT
  EXTRACT(YEAR FROM dt_venda) AS ano,
  EXTRACT(MONTH FROM dt_venda) AS mes,
  SUM(vlr_qtd_venda) AS vlr_total_venda
FROM
  `earnest-beacon-437623-b0.trusted_vendas.vendas`
GROUP BY ALL
ORDER BY
  ano, mes
);

--
-- Tabela 2: Consolidado de vendas por marca e linha
CREATE OR REPLACE TABLE refined_vendas.vendas_marca_linha AS (
SELECT
  st_marca,
  st_linha,
  SUM(vlr_qtd_venda) AS vlr_total_venda
FROM
  `earnest-beacon-437623-b0.trusted_vendas.vendas`
GROUP BY ALL
ORDER BY st_marca, st_linha
);

--
-- Tabela 3: Consolidado de vendas por marca, ano e mês;
CREATE OR REPLACE TABLE refined_vendas.vendas_marca_ano_mes AS (
SELECT
  st_marca,
  EXTRACT(YEAR FROM dt_venda) AS ano,
  EXTRACT(MONTH FROM dt_venda) AS mes,
  SUM(vlr_qtd_venda) AS vlr_total_venda
FROM
  `earnest-beacon-437623-b0.trusted_vendas.vendas`
GROUP BY ALL
ORDER BY
  st_marca, ano, mes
);

--
-- Tabela 4: Consolidado de vendas por linha, ano e mês
CREATE OR REPLACE TABLE refined_vendas.vendas_linha_ano_mes AS (
SELECT
  st_linha,
  EXTRACT(YEAR FROM dt_venda) AS ano,
  EXTRACT(MONTH FROM dt_venda) AS mes,
  SUM(vlr_qtd_venda) AS vlr_total_venda
FROM
  `earnest-beacon-437623-b0.trusted_vendas.vendas`
GROUP BY ALL
  ORDER BY
    st_linha, ano, mes
);

END