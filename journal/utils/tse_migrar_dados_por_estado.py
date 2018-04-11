import sys
import os
import glob
import time

import pandas as pd
import numpy as np
import mysql.connector as mysql

import utils.tse_parsers as parsers
import utils.tse_data_headers as headers

PREFIXO_ARQUIVO_CANDIDATURAS = 'consulta_cand_'
DIRETORIO_TEMPORARIO = os.path.join('../temp')
BD_CONFIG = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    'database': 'laddres'
}

def migrar_dados_candidato(ano):
    tempo_inicio_ano = time.time()

    arquivos = PREFIXO_ARQUIVO_CANDIDATURAS + str(ano) + '*.txt'
    pathes = os.path.join(DIRETORIO_TEMPORARIO, arquivos)
    arquivos_do_ano = sorted(glob.glob(pathes))

    for arquivo_estado in arquivos_do_ano:
        tempo_inicio_estado = time.time()
        print('Iniciando {}'.format(arquivo_estado))

        df_estado = pd.read_csv(
            arquivo_estado,
            sep = ';',
            header = None,
            dtype = np.str,
            names = headers.get_header(ano),
            encoding = 'iso-8859-1'
        )

        candidatos = (
            df_estado[get_colunas_candidato(ano)]
            .drop_duplicates(['CPF_CANDIDATO', 'NUM_TITULO_ELEITORAL_CANDIDATO'], keep='last')
            .rename(columns = get_map_colunas_candidato(ano))
        )

        total_candidatos = candidatos.shape[0]
        print('Total de candidatos em {}: {}'.format(ano, total_candidatos))


        cnx = mysql.connect(**BD_CONFIG)
        cursor = cnx.cursor(buffered=True)

        for candidato in candidatos.itertuples():
            # Verificar se o candidato já está cadastrado
            query = ("SELECT id FROM candidato WHERE "
                     "cpf = %(cpf)s AND titulo_eleitoral = %(titulo_eleitoral)s")
            atributos = {
                'cpf': parsers.parse_cpf(candidato.cpf),
                'titulo_eleitoral': parsers.parse_titulo_eleitoral(candidato.titulo_eleitoral)
            }

            try:
                cursor.execute(query, atributos)
            except:
                print(candidato)
                print(query)
                cursor.close()
                cnx.close()
                raise

            resultados = cursor.fetchall()
            candidato_id = resultados[0][0] if len(resultados) > 0 else 0

            # Buscar id da cidade
            cidade = parsers.parse_cidade(candidato.cidade)
            sigla_estado = parsers.parse_estado(candidato.estado)
            query = ("SELECT id FROM cidade WHERE "
                     "nome = \"" + cidade + "\" AND "
                     "estado_id = (SELECT id FROM estado WHERE sigla = '" + sigla_estado + "')")

            try:
                cursor.execute(query)
            except:
                print(candidato)
                print(query)
                cursor.close()
                cnx.close()
                raise

            cidades = cursor.fetchall()
            cidade_id = cidades[0][0] if len(cidades) > 0 else None

            email = parsers.parse_email(candidato.email) if ('email' in candidatos.columns) else None

            if (candidato_id == 0):
                # Cadastrar candidato
                query = ("INSERT INTO candidato "
                               "(nome, data_nascimento, cpf, titulo_eleitoral, email, cidade_id, ocupacao_id, nacionalidade_id, grau_instrucao_id) "
                               "VALUES (%(nome)s, %(data_nascimento)s, %(cpf)s, %(titulo_eleitoral)s, %(email)s, %(cidade_id)s, %(ocupacao)s, %(nacionalidade)s, %(grau_instrucao)s)")
                atributos = {
                    'nome': parsers.parse_nome(candidato.nome),
                    'data_nascimento': parsers.parse_data(candidato.data_nascimento),
                    'cpf': parsers.parse_cpf(candidato.cpf),
                    'titulo_eleitoral': parsers.parse_titulo_eleitoral(candidato.titulo_eleitoral),
                    'email': email,
                    'cidade_id': cidade_id,
                    'ocupacao': parsers.parse_ocupacao(candidato.ocupacao),
                    'nacionalidade': parsers.parse_nacionalidade(candidato.nacionalidade),
                    'grau_instrucao': parsers.parse_grau_instrucao(candidato.grau_instrucao)
                }
            else:
                # Atualizar dados do candidato
                query = (
                    "UPDATE candidato "
                    "SET "
                    "nome = %(nome)s, "
                    "data_nascimento = %(data_nascimento)s, "
                    "email = %(email)s, "
                    "cidade_id = %(cidade_id)s, "
                    "ocupacao_id = %(ocupacao)s, "
                    "nacionalidade_id = %(nacionalidade)s, "
                    "grau_instrucao_id = %(grau_instrucao)s "
                    "WHERE id = %(candidato_id)s"
                )
                atributos = {
                    'nome': parsers.parse_nome(candidato.nome),
                    'data_nascimento': parsers.parse_data(candidato.data_nascimento),
                    'email': email,
                    'cidade_id': cidade_id,
                    'ocupacao': parsers.parse_ocupacao(candidato.ocupacao),
                    'nacionalidade': parsers.parse_nacionalidade(candidato.nacionalidade),
                    'grau_instrucao': parsers.parse_grau_instrucao(candidato.grau_instrucao),
                    'candidato_id': candidato_id
                }

            try:
                cursor.execute(query, atributos)
                cnx.commit()
            except:
                print(candidato)
                cursor.close()
                cnx.close()
                raise

            sys.stdout.write('\r{} de {}'.format(candidato[0], total_candidatos))
            sys.stdout.flush()

        cursor.close()
        cnx.close()

        sys.stdout.write('\n')
        tempo_final_estado = time.time()
        print('Tempo total desse estado: {:.2f}s \n'.format((tempo_final_estado - tempo_inicio_estado)))

    tempo_final_ano = time.time()
    print('Tempo total em {}: {:.2f}s \n'.format(ano, (tempo_final_ano - tempo_inicio_ano)))

def migrar_dados_candidatura(ano):
    print('To be done!')



def get_colunas_candidato(ano):
    padrao = [
        'NOME_CANDIDATO',
        'DATA_NASCIMENTO',
        'CPF_CANDIDATO',
        'NUM_TITULO_ELEITORAL_CANDIDATO',
        'NOME_MUNICIPIO_NASCIMENTO',
        'SIGLA_UF_NASCIMENTO',
        'CODIGO_NACIONALIDADE',
        'COD_GRAU_INSTRUCAO',
        'CODIGO_OCUPACAO',
    ]

    return padrao if (ano < 2012) else (padrao + ['NM_EMAIL'])

def get_map_colunas_candidato(ano):
    sem_email = {
        'NOME_CANDIDATO': 'nome',
        'DATA_NASCIMENTO': 'data_nascimento',
        'CPF_CANDIDATO': 'cpf',
        'NUM_TITULO_ELEITORAL_CANDIDATO': 'titulo_eleitoral',
        'NOME_MUNICIPIO_NASCIMENTO': 'cidade',
        'SIGLA_UF_NASCIMENTO': 'estado',
        'CODIGO_NACIONALIDADE': 'nacionalidade',
        'COD_GRAU_INSTRUCAO': 'grau_instrucao',
        'CODIGO_OCUPACAO': 'ocupacao'
    }
    com_email = {
        'NOME_CANDIDATO': 'nome',
        'DATA_NASCIMENTO': 'data_nascimento',
        'CPF_CANDIDATO': 'cpf',
        'NUM_TITULO_ELEITORAL_CANDIDATO': 'titulo_eleitoral',
        'NM_EMAIL': 'email',
        'NOME_MUNICIPIO_NASCIMENTO': 'cidade',
        'SIGLA_UF_NASCIMENTO': 'estado',
        'CODIGO_NACIONALIDADE': 'nacionalidade',
        'COD_GRAU_INSTRUCAO': 'grau_instrucao',
        'CODIGO_OCUPACAO': 'ocupacao'
    }

    return sem_email if (ano < 2012) else com_email
