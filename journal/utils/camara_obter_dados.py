import pandas as pd
import re as regex
import requests
import sys

url_base = 'https://dadosabertos.camara.leg.br/api/v2'

def obter_numero_pagina(url):
    pattern = r'pagina=(\d+)'
    match = regex.findall(pattern, url)

    return 1 if len(match) == 0 else match[0]

def obter_numero_ultima_pagina(links):
    for link in links:
        if link['rel'] == 'last':
            return int(obter_numero_pagina(link['href']))

def gerar_url(endpoint, pagina = 1, filtros = []):
    return '{}{}?pagina={}{}{}'.format(
        url_base,
        endpoint,
        pagina,
        '&' if len(filtros) > 0 else '',
        '&'.join(filtros)
    )

def obter_dados(endpoint, filtros = []):
    def recursao(request, pagina = 1, dataframe = pd.DataFrame()):
        if request.status_code == 200:
            response = request.json()

            ultima_pagina = obter_numero_ultima_pagina(response['links'])

            sys.stdout.write('\rPágina {} de {}'.format(pagina, ultima_pagina))
            sys.stdout.flush()

            if (pagina == ultima_pagina) or (ultima_pagina == None):
                return dataframe.append(pd.DataFrame(response['dados']), ignore_index=True)

            return recursao(
                request=requests.get(gerar_url(endpoint, pagina + 1, filtros)),
                pagina=pagina + 1,
                dataframe=dataframe.append(pd.DataFrame(response['dados']), ignore_index=True)
            )
        else:
            print('Erro ao tentar carregar dados da API')
            return pd.DataFrame()

    url = gerar_url(endpoint=endpoint, filtros=filtros)
    return recursao(request=requests.get(url))

def obter_detalhes(endpoint, id):
    url = '{}{}/{}'.format(url_base, endpoint, id)

    request = requests.get(url)

    if request.status_code != 200:
        print('Erro ao tentar carregar dados da API')
        return pd.DataFrame()

    response = request.json()['dados']
    df = pd.DataFrame(
        [[
            response['id'],
            response['nomeCivil'],
            response['sexo'],
            response['urlWebsite'],
            response['dataNascimento'],
            response['dataFalecimento'],
            response['ufNascimento'],
            response['municipioNascimento'],
            response['ultimoStatus']['data'],
            response['ultimoStatus']['idLegislatura'],
            response['ultimoStatus']['siglaPartido'],
            response['ultimoStatus']['gabinete']['nome'],
            response['ultimoStatus']['gabinete']['predio'],
            response['ultimoStatus']['gabinete']['sala'],
            response['ultimoStatus']['gabinete']['andar'],
            response['ultimoStatus']['gabinete']['telefone'],
            response['ultimoStatus']['gabinete']['email']
        ]],
        columns=[
            'id',
            'nomeCivil',
            'sexo',
            'urlWebsite',
            'dataNascimento',
            'dataFalecimento',
            'ufNascimento',
            'municipioNascimento',
            'dataUltimoStatus',
            'idLegislatura',
            'siglaPartido',
            'gabinete_nome',
            'gabinete_predio',
            'gabinete_sala',
            'gabinete_andar',
            'gabinete_telefone',
            'gabinete_email'
        ]
    )
    return df
