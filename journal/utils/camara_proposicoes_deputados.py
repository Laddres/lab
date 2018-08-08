import pandas as pd
from utils.camara_obter_dados import obter_dados
from utils.dataframe_to_file import dataframe_to_file

df_deputados = pd.read_csv(
    '../data/2018-07-16-camara-deputados'
)

deputados = (df_deputados
    .drop_duplicates('id', keep='last')
    .sort_values(by=['id'], ascending=True, na_position='first'))
deputados.index = deputados.reset_index().index

total_deputados = len(deputados)
falhas = []
df_deputado_proposicao = pd.DataFrame()
for deputado in deputados.itertuples(index=True):
    print('\nDeputado {}: {} de {}'.format(deputado.id, deputado[0], total_deputados))

    try:
        df_i = obter_dados(
            endpoint='/proposicoes',
            filtros=[
                'itens=100',
                'ordenarPor=ano',
                'ordem=ASC',
                'idAutor={}'.format(deputado.id)
            ]
        )
    except:
        print('{} falhou'.format(deputado.id))
        falhas.append(deputado.id)
        continue

    if len(df_i) == 0:
        continue

    df_i['idDeputado'] = deputado.id
    df_deputado_proposicao = df_deputado_proposicao.append(df_i[[
        'idDeputado',
        'id',
        'siglaTipo',
        'idTipo',
        'numero',
        'ano',
        'ementa'
    ]])

# Renomear coluna
df_deputado_proposicao = df_deputado_proposicao.rename(columns={'id': 'idProposicao'})
# Zerar o índice do dataframe
df_deputado_proposicao.index = df_deputado_proposicao.reset_index().index
# Transformar todos os ids em inteiros (deixar zero para id inválido)
df_deputado_proposicao['idDeputado'] = df_deputado_proposicao.idDeputado.fillna(0.0).astype(int)
df_deputado_proposicao['idProposicao'] = df_deputado_proposicao.idProposicao.fillna(0.0).astype(int)
# Salvar em arquivo
dataframe_to_file(
    dataframe=df_deputado_proposicao,
    nome='membros-orgaos',
    origem_dados='camara'
)
