CREATE OR REPLACE TABLE `earnest-beacon-437623-b0.raw_vendas.vendas`
(
    id_marca STRING OPTIONS(description = 'Identificador da marca'),
    marca STRING OPTIONS(description = 'Nome da marca'),
    id_linha STRING OPTIONS(description = 'Identificador da linha de produtos'),
    linha STRING OPTIONS(description = 'Nome da linha de produtos'),
    data_venda STRING OPTIONS(description = 'Data da venda'),
    qtd_venda STRING OPTIONS(description = 'Quantidade de vendas')
)
OPTIONS(
    description = 'Tabela contendo dados brutos de vendas, incluindo informações sobre marca, linha e quantidade vendida'
)
