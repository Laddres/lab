import pandas as pd
import numpy as np
import sys
import os
import urllib
import zipfile
import glob

from tempfile import mkdtemp
from utils.download import download

# Diretório temporário
# python3 tse_download_datasets.py ../temp
arg1 = sys.argv[1]
DIRETORIO_TEMPORARIO = os.path.join(arg1)

# Nome e endereço dos arquivos do diretório do TSE
PREFIXO_ARQUIVO_CANDIDATURAS = 'consulta_cand_'
URL_CANDIDATURAS = 'http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/'

# Setup de configuração para o arquivo de saída (nome do dataset)
HOJE = pd.datetime.today().date()
ARQUIVO_SAIDA_CANDIDATURAS = HOJE.isoformat() + '-candidaturas-tse.xz'
DATASET_SAIDA_CANDIDATURAS = os.path.join('../data', ARQUIVO_SAIDA_CANDIDATURAS)

anos_eleitorais = [str(year) for year in (range(2004, HOJE.year - 1, 2))]

for ano in anos_eleitorais:
    nome_arquivo = '{}{}.zip'.format(PREFIXO_ARQUIVO_CANDIDATURAS, ano)
    url_arquivo = URL_CANDIDATURAS + nome_arquivo
    arquivo_saida = os.path.join(DIRETORIO_TEMPORARIO, nome_arquivo)

    # urllib.request.urlretrieve(url_arquivo, arquivo_saida)
    download(url_arquivo, arquivo_saida)

    print('Arquivos de ' + ano + ' baixados com sucesso!')

for ano in anos_eleitorais:
    arquivo = PREFIXO_ARQUIVO_CANDIDATURAS + ano + '.zip'
    path = os.path.join(DIRETORIO_TEMPORARIO, arquivo)

    zip_ref = zipfile.ZipFile(path, 'r')
    zip_ref.extractall(DIRETORIO_TEMPORARIO)
    zip_ref.close()
