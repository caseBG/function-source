item_mais_vendido = """
select LINHA
from case_consolidado.linha_ano_mes
where ANO = 2019 and MES = 12
order by CONSOLIDADO_VENDAS DESC
LIMIT 1
"""