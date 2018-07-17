import pandas as pd
import os

# DataFrame -> File
def dataframe_to_file(dataframe, origem_dados, nome):
    # Setup de configuração para o arquivo de saída (nome do dataset)
    hoje = pd.datetime.today().date()
    arquivo_saida = '{}-{}-{}.xz'.format(hoje.isoformat(), origem_dados, nome)
    path_saida = os.path.join('../data', arquivo_saida)

    # Salvar DataFrame no caminho indicado
    dataframe.to_csv(
        path_saida,
        encoding='utf-8',
        compression='xz',
        header=True,
        index=False
    )
