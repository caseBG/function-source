import base64
import pandas as pd
from imap_tools import MailBox, AND
from google.oauth2 import service_account
from google.cloud import bigquery
from io import BytesIO
import xlsxwriter
import fsspec
import gcsfs
import openpyxl
import db_dtypes
from parametros import * #email_usuario,email_senha,email_from,table_vendas,table_tweets
from consolidado import query_consolidado
from item_mais_vendido import item_mais_vendido
import tweepy as tw



def case_boticario(event, context):
    
    #conexao com o e_mail
    meu_email = MailBox('imap.gmail.com').login(email_usuario,email_senha)
    print('conexao realizada com sucesso')
    
    #localiza anexo
    lista_emails = meu_email.fetch(AND(from_= email_from))
    for email in lista_emails:
        if len(email.attachments) > 0:
            for anexo in email.attachments:
                if '2017' in anexo.filename: # or '2018' in anexo.filename or '2019' in anexo.filename:
                    file_2017_byte = anexo.payload
                    df_2017 = pd.read_excel(BytesIO(file_2017_byte))
                if '2018' in anexo.filename: # or '2018' in anexo.filename or '2019' in anexo.filename:
                    file_2018_byte = anexo.payload
                    df_2018 = pd.read_excel(BytesIO(file_2018_byte))
                if '2019' in anexo.filename: # or '2018' in anexo.filename or '2019' in anexo.filename:
                    file_2019_byte = anexo.payload
                    df_2019 = pd.read_excel(BytesIO(file_2019_byte))
    print('leitura dos anexos realizada')

    df_consolidado = pd.concat([df_2017,df_2018,df_2019])
    print('a base foi unificada com sucesso')


    ############# load bigquery
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition='WRITE_APPEND'
    )

    # Load data to BQ
    job = client.load_table_from_dataframe(df_consolidado, table_vendas, job_config=job_config)
    job.result()
    print('load no BQ dos arquivos de vendas realizado com sucesso')

    #criação das tabelas consolidadas
    client = bigquery.Client()
    jobproc = client.query(query_consolidado) # query_consolidado é onde está as regras de negócio para criação das tabelas
    jobproc.result()
    print('tabelas consolidadas criadas no BQ')

    #item mais vendido
    client = bigquery.Client()
    job_item_mais_vendido = client.query(item_mais_vendido) # item_mais_vendido é onde está a query para identificar o item mais vendido
    item = job_item_mais_vendido.result()
    item = item.to_dataframe()
    item = item.iloc[0]["LINHA"]
    print('O item mais vendido no período solicitado foi: {}'.format(item))

    ############### PEGANDO OS 50 TWEETS MAIS RECENTES COM A LIB TWEEPY ############

    #estabelecendo comunicação
    cliente = tw.Client(bearer_token= bearer_token,
                        consumer_key=consumer_key,
                        consumer_secret=consumer_secret,
                        access_token=access_token,
                        access_token_secret=access_token_secret)

    
    #pegando os 50 tweets mais recentes
    dados = cliente.search_recent_tweets(query=['Boticário OR {}'.format(item)],
                             max_results=50,
                             expansions=['author_id']
                             )

    
    #incluindo o nome do usuário
    users = {u['id']: u for u in dados.includes['users']}

    #pegando as informações de nome e text e salvando em um df
    df = []
    for tweet in dados.data:
      #if users[tweet.author_id]:
        user = users[tweet.author_id]
        #print(user.username)
        #print(tweet.text)
        df.append({
            'nome':user.username,
            'text':tweet.text
        })
    df_tweets = pd.DataFrame(df)
    

    #fazendo o load dos tweets no bigquery
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition='WRITE_APPEND'
    )

    # Load BQ   

    job = client.load_table_from_dataframe(df_tweets, table_tweets, job_config=job_config)
    job.result()
    print('load dos tweets no BQ realizado com sucesso')
    print('a aplicação foi executada com sucesso!')

