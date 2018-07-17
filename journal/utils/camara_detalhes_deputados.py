import pandas as pd
import sys

from utils.camara_obter_dados import obter_detalhes
from utils.dataframe_to_file import dataframe_to_file

deputados_df = pd.read_csv(
    '../data/2018-07-16-camara-deputados'
)

deputados = (deputados_df
    .drop_duplicates('id', keep='last')
    .sort_values(by=['id'], ascending=True, na_position='first'))
deputados.index = deputados.reset_index().index

total_deputados = len(deputados)
falhas = []
detalhes_deputados_df = pd.DataFrame()
for deputado in deputados.itertuples(index=True):
    try:
        df_i = obter_detalhes(
            endpoint='/deputados',
            id=deputado.id
        )
    except:
        print('{} falhou'.format(deputado.id))
        falhas.append(deputado.id)
        continue

    detalhes_deputados_df = detalhes_deputados_df.append(df_i)

    sys.stdout.write('\r{} de {}'.format((deputado.Index + 1), total_deputados))
    sys.stdout.flush()

# Zerar o índice do dataframe
detalhes_deputados_df.index = detalhes_deputados_df.reset_index().index
# Salvar em arquivo
dataframe_to_file(
    dataframe=detalhes_deputados_df,
    nome='detalhes-deputados',
    origem_dados='camara'
)
