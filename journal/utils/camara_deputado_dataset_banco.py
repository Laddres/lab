import pandas as pd
import sys
import mysql.connector as mysql

BD_CONFIG = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    'database': 'laddres'
}

# Gerar o dataframe que será usado pra popular a tabela 'deputado' do banco de dados
def gerar_dataframe_deputado(dataframe):
    total = len(dataframe)
    deputados_df = pd.DataFrame()
    for deputado in dataframe.itertuples():
        idCandidato = _get_id_candidato(deputado.nomeCivil, deputado.dataNascimento)

        df_i = pd.DataFrame([[
            deputado.id,
            idCandidato,
            deputado.nome,
            deputado.nomeCivil,
            deputado.dataNascimento,
            deputado.dataFalecimento,
            deputado.sexo,
            deputado.siglaPartido,
            deputado.siglaUf,
            deputado.municipioNascimento,
            deputado.ufNascimento,
            deputado.urlWebsite,
            deputado.dataUltimoStatus,
            deputado.gabinete_nome,
            deputado.gabinete_predio,
            deputado.gabinete_sala,
            deputado.gabinete_andar,
            deputado.gabinete_telefone,
            deputado.gabinete_email
        ]],
        columns=[
            'id',
            'candidato_id',
            'nome',
            'nomeCivil',
            'dataNascimento',
            'dataFalecimento',
            'sexo',
            'siglaPartido',
            'siglaUf',
            'municipioNascimento',
            'ufNascimento',
            'urlWebsite',
            'dataUltimoStatus',
            'gabinete_nome',
            'gabinete_predio',
            'gabinete_sala',
            'gabinete_andar',
            'gabinete_telefone',
            'gabinete_email'
        ])
        deputados_df = deputados_df.append(df_i)

        sys.stdout.write('\r{} de {}'.format((deputado.Index + 1), total))
        sys.stdout.flush()

    return deputados_df

def _get_id_candidato(nome, data_nascimento):
    cnx = mysql.connect(**BD_CONFIG)
    cursor = cnx.cursor()

    query = ("SELECT id FROM candidato WHERE "
             "nome = %(nomeCivil)s AND data_nascimento = %(dataNascimento)s")
    parametros = {
        'nomeCivil': nome,
        'dataNascimento': data_nascimento
    }

    try:
        cursor.execute(query, parametros)
    except:
        print(query)
        cursor.close()
        cnx.close()
        raise

    resultados = cursor.fetchall()

    if (len(resultados) > 1):
        print('Mais de um deputado com o mesmo nome: {}'.format(nome))

    id = resultados[0][0] if len(resultados) > 0 else None

    cursor.close()
    cnx.close()

    return id
