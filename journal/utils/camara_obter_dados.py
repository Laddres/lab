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

def obter_detalhes(endpoint, id, map_response):
    url = '{}{}/{}'.format(url_base, endpoint, id)

    request = requests.get(url)

    if request.status_code != 200:
        print('Erro ao tentar carregar dados da API')
        return pd.DataFrame()

    response = request.json()['dados']
    return map_response(response)

def obter_votos(idVotacao):
    votos = obter_dados('/votacoes/{}/votos'.format(idVotacao))

    df = pd.DataFrame()
    for row in votos.itertuples():
        voto = pd.DataFrame(
            [[
                row.parlamentar['id'],
                row.parlamentar['nome'],
                row.voto,
            ]],
            columns=[
                'idParlamentar',
                'nomeParlamentar',
                'voto',
            ]
        )
        df = df.append(voto, ignore_index=True)

    return df


def map_deputados(response):
    return pd.DataFrame(
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

def map_proposicoes(response):
    return pd.DataFrame(
        [[
            response['id'],
            response['uri'],
            response['siglaTipo'],
            response['idTipo'],
            response['numero'],
            response['ano'],
            response['ementa'],
            response['dataApresentacao'],
            response['uriOrgaoNumerador'],
            response['uriUltimoRelator'],
            response['statusProposicao']['dataHora'],
            response['statusProposicao']['sequencia'],
            response['statusProposicao']['siglaOrgao'],
            response['statusProposicao']['uriOrgao'],
            response['statusProposicao']['regime'],
            response['statusProposicao']['descricaoTramitacao'],
            response['statusProposicao']['idTipoTramitacao'],
            response['statusProposicao']['descricaoSituacao'],
            response['statusProposicao']['idSituacao'],
            response['statusProposicao']['despacho'],
            response['statusProposicao']['url'],
            response['uriAutores'],
            response['descricaoTipo'],
            response['ementaDetalhada'],
            response['keywords'],
            response['uriPropPrincipal'],
            response['uriPropAnterior'],
            response['uriPropPosterior'],
            response['urlInteiroTeor'],
            response['urnFinal'],
            response['texto'],
            response['justificativa']
        ]],
        columns=[
            'id',
            'uri',
            'siglaTipo',
            'idTipo',
            'numero',
            'ano',
            'ementa',
            'dataApresentacao',
            'uriOrgaoNumerador',
            'uriUltimoRelator',
            'statusProposicao_dataHora',
            'statusProposicao_sequencia',
            'statusProposicao_siglaOrgao',
            'statusProposicao_uriOrgao',
            'statusProposicao_regime',
            'statusProposicao_descricaoTramitacao',
            'statusProposicao_idTipoTramitacao',
            'statusProposicao_descricaoSituacao',
            'statusProposicao_idSituacao',
            'statusProposicao_despacho',
            'statusProposicao_url',
            'uriAutores',
            'descricaoTipo',
            'ementaDetalhada',
            'keywords',
            'uriPropPrincipal',
            'uriPropAnterior',
            'uriPropPosterior',
            'urlInteiroTeor',
            'urnFinal',
            'texto',
            'justificativa'
        ]
    )
