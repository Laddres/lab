import pandas as pd
import numpy as np
import requests
import sys
from time import sleep

from utils.database_to_dataframe import database_to_dataframe
from utils.dataframe_to_file import dataframe_to_file

df_candidatos_2018 = database_to_dataframe(
    tabela='candidatura',
    colunas_tabela=['candidato_id', 'sequencial_candidato', 'sigla'],
    condicao='LEFT JOIN estado e on candidatura.estado_id = e.id WHERE eleicao_id = 202 OR eleicao_id = 203'
)
df_candidatos_2018['sigla'] = df_candidatos_2018.sigla.fillna('BR').astype(str)

falhas = []
vazios = []
total = len(df_candidatos_2018)
df_fotos = pd.DataFrame()

for candidato in df_candidatos_2018.itertuples():
    sys.stdout.write('\rCandidato {} de {}'.format(candidato.Index, total))
    sys.stdout.flush()

    sleep_interval = 1
    while True:
        try:
            url = 'http://divulgacandcontas.tse.jus.br/divulga/rest/v1/candidatura/buscar/2018/{}/2022802018/candidato/{}'.format(
                candidato.sigla,
                candidato.sequencial_candidato
            )
            request = requests.get(url)

            if request.status_code != 200:
                raise Exception(candidato.candidato_id)

            response = request.json()
            df_i = pd.DataFrame(data = {
                'candidato_id': [candidato.candidato_id],
                'foto': [response['fotoUrl']]
            })

            if df_i.empty:
                vazios.append(candidato.candidato_id)

            df_fotos = df_fotos.append(df_i)
            break

        except:
            falhas.append(candidato.candidato_id)

            print('Sleeping at index {}...'.format(candidato.Index))
            sleep(sleep_interval)
            # Dar um intervalo antes de tentar a nova requisição
            sleep_interval = sleep_interval + 1

dataframe_to_file(
    dataframe=df_fotos,
    origem_dados='tse',
    nome='fotos-candidatos-2018'
)

# C:\Users\ranpaWinServer\Documents\UFS\tcc\repositórios\lab\journal (master)
# λ c:\Users\ranpaWinServer\Anaconda3\envs\laddres\python.exe tse_gerar_dataframe_fotos.py
