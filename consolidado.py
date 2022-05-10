query_consolidado = """
-- consolidade de vendas por ano e mes
Create table case_consolidado.ano_mes as (
select 
sum(QTD_VENDA) as CONSOLIDADO_VENDAS 
,extract(YEAR FROM DATA_VENDA) AS ANO
,extract(MONTH FROM DATA_VENDA) AS MES
from case_raw.vendas
GROUP BY ANO,MES
ORDER BY ANO,MES
);

-- consolidade de vendas por marca e linha
Create table case_consolidado.marca_linha as (
select 
sum(QTD_VENDA) as CONSOLIDADO_VENDAS
,ID_MARCA
,MARCA
,ID_LINHA
,LINHA
from case_raw.vendas
GROUP BY ID_MARCA,MARCA,ID_LINHA,LINHA
ORDER BY ID_MARCA,MARCA,ID_LINHA,LINHA
);

--consolidado de vendas por marca, ano e mes
Create table case_consolidado.marca_ano_mes as (
select 
sum(QTD_VENDA) as CONSOLIDADO_VENDAS
,ID_MARCA
,MARCA
,extract(YEAR FROM DATA_VENDA) AS ANO
,extract(MONTH FROM DATA_VENDA) AS MES
from case_raw.vendas
GROUP BY ID_MARCA,MARCA,ANO,MES
ORDER BY ID_MARCA,MARCA,ANO,MES
);

--consolidado de vendas por linha, ano e mes
Create table case_consolidado.linha_ano_mes as (
select 
sum(QTD_VENDA) as CONSOLIDADO_VENDAS
,ID_LINHA
,LINHA
,extract(YEAR FROM DATA_VENDA) AS ANO
,extract(MONTH FROM DATA_VENDA) AS MES
from case_raw.vendas
GROUP BY ID_LINHA,LINHA,ANO,MES
ORDER BY ID_LINHA,LINHA,ANO,MES
);
"""