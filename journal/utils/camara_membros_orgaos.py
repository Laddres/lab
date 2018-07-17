import pandas as pd
from utils.camara_obter_dados import obter_dados
from utils.dataframe_to_file import dataframe_to_file

orgaos = pd.read_csv(
    '../data/2018-07-16-camara-orgaos'
)

total_orgaos = len(orgaos)
falhas = []
membros_orgaos_df = pd.DataFrame()
for orgao in orgaos.itertuples(index=True):
    print('\nÓrgão {}: {} de {}'.format(orgao.id, orgao[0], total_orgaos))

    try:
        df_i = obter_dados(
            endpoint='/orgaos/{}/membros'.format(orgao.id),
            filtros=['itens=100']
        )
    except:
        print('{} falhou'.format(orgao.id))
        falhas.append(orgao.id)
        continue

    if len(df_i) == 0:
        continue

    df_i['idOrgao'] = orgao.id
    membros_orgaos_df = membros_orgaos_df.append(df_i[[
        'idLegislatura',
        'idOrgao',
        'id',
        'idPapel',
        'nomePapel',
        'dataInicio',
        'dataFim'
    ]])

# Renomear coluna
membros_orgaos_df = membros_orgaos_df.rename(columns={'id': 'idDeputado'})
# Zerar o índice do dataframe
membros_orgaos_df.index = membros_orgaos_df.reset_index().index
# Transformar todos os ids em inteiros (deixar zero para id inválido)
membros_orgaos_df['idDeputado'] = membros_orgaos_df.idDeputado.fillna(0.0).astype(int)
# Salvar em arquivo
dataframe_to_file(
    dataframe=membros_orgaos_df,
    nome='membros-orgaos',
    origem_dados='camara'
)
