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

def migrar_dados(ano):
    tempo_inicio_ano = time.time()

    arquivos = PREFIXO_ARQUIVO_CANDIDATURAS + str(ano) + '*.csv'
    pathes = os.path.join(DIRETORIO_TEMPORARIO, arquivos)
    arquivos_do_ano = sorted(glob.glob(pathes))

    for arquivo_estado in arquivos_do_ano:
        tempo_inicio_estado = time.time()
        print('Iniciando {}'.format(arquivo_estado))

        df_estado = pd.read_csv(
            arquivo_estado,
            sep = ';',
            header = 0,
            dtype = np.str,
            names = headers.get_header(ano),
            encoding = 'iso-8859-1'
        )

        candidaturas = (
            df_estado[_get_colunas_candidatura(ano)]
            .rename(columns = _get_map_colunas_candidatura(ano))
        )

        total_candidaturas = candidaturas.shape[0]
        print('Total de candidaturas em {}: {}'.format(ano, total_candidaturas))

        for candidatura in candidaturas.itertuples():
            cnx = mysql.connect(**BD_CONFIG)
            cursor = cnx.cursor(buffered=True)

            candidato_id = _get_candidato_id(
                cnx=cnx,
                cursor=cursor,
                cpf=candidatura.cpf,
                titulo_eleitoral=candidatura.titulo_eleitoral)

            estado_id = _get_estado_id(
                cnx=cnx,
                cursor=cursor,
                sigla_estado=candidatura.sigla_estado_nascimento)
            cidade_id = None if (not estado_id) else _get_cidade_id(
                cnx=cnx,
                cursor=cursor,
                cidade=candidatura.cidade_nascimento,
                estado_id=estado_id)

            email = parsers.parse_email(candidatura.email) if ('email' in candidaturas.columns) else None

            if (candidato_id == 0):
                # Cadastrar candidato
                query = ("INSERT INTO candidato "
                               "(nome, data_nascimento, cpf, titulo_eleitoral, email, cidade_id, ocupacao_id, nacionalidade_id, grau_instrucao_id, genero_id, cor_raca_id) "
                               "VALUES (%(nome)s, %(data_nascimento)s, %(cpf)s, %(titulo_eleitoral)s, %(email)s, %(cidade_id)s, %(ocupacao)s, %(nacionalidade)s, %(grau_instrucao)s, %(genero)s, %(cor_raca)s)")
                atributos = {
                    'nome': parsers.parse_nome(candidatura.nome),
                    'data_nascimento': parsers.parse_data(candidatura.data_nascimento),
                    'cpf': parsers.parse_cpf(candidatura.cpf),
                    'titulo_eleitoral': parsers.parse_titulo_eleitoral(candidatura.titulo_eleitoral),
                    'email': email,
                    'cidade_id': cidade_id,
                    'ocupacao': parsers.parse_ocupacao(candidatura.ocupacao),
                    'nacionalidade': parsers.parse_nacionalidade(candidatura.nacionalidade),
                    'grau_instrucao': parsers.parse_grau_instrucao(candidatura.grau_instrucao),
                    'genero': parsers.parse_genero(candidatura.genero),
                    'cor_raca': parsers.parse_cor_raca(candidatura.cor_raca)
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
                    "grau_instrucao_id = %(grau_instrucao)s, "
                    "genero_id = %(genero)s, "
                    "cor_raca_id = %(cor_raca)s "
                    "WHERE id = %(candidato_id)s"
                )
                atributos = {
                    'nome': parsers.parse_nome(candidatura.nome),
                    'data_nascimento': parsers.parse_data(candidatura.data_nascimento),
                    'email': email,
                    'cidade_id': cidade_id,
                    'ocupacao': parsers.parse_ocupacao(candidatura.ocupacao),
                    'nacionalidade': parsers.parse_nacionalidade(candidatura.nacionalidade),
                    'grau_instrucao': parsers.parse_grau_instrucao(candidatura.grau_instrucao),
                    'genero': parsers.parse_genero(candidatura.genero),
                    'cor_raca': parsers.parse_cor_raca(candidatura.cor_raca),
                    'candidato_id': candidato_id
                }

            try:
                cursor.execute(query, atributos)
            except:
                print(candidatura)
                cursor.close()
                cnx.close()
                raise

            # Cadastrar candidatura
            id_candidato = cursor.lastrowid if (candidato_id == 0) else candidato_id
            eleicao_id = _get_eleicao_id(
                cnx=cnx,
                cursor=cursor,
                ano=candidatura.ano,
                descricao=candidatura.descricao_eleicao)
            estado_id = _get_estado_id(
                cnx=cnx,
                cursor=cursor,
                sigla_estado=candidatura.sigla_estado_eleicao)
            cidade_id = None if (not estado_id) else _get_cidade_id(
                cnx=cnx,
                cursor=cursor,
                cidade=candidatura.cidade_eleicao,
                estado_id=estado_id)
            partido_id = _get_partido_id(
                cnx=cnx,
                cursor=cursor,
                numero=candidatura.partido)

            query = ("INSERT INTO candidatura "
                     "(eleicao_id, turno, candidato_id, cidade_id, estado_id, numero_candidato, nome_urna, partido_id, legenda_nome, legenda_composicao, cargo_id, despesa_maxima, situacao_candidatura_id, resultado_candidatura_id, sequencial_candidato) "
                     "VALUES (%(eleicao)s, %(turno)s, %(candidato)s, %(cidade)s, %(estado)s, %(numero_candidato)s, %(nome_urna)s, %(partido)s, %(legenda_nome)s, %(legenda_composicao)s, %(cargo)s, %(despesa_maxima)s, %(situacao_candidatura)s, %(resultado_candidatura)s, %(sequencial_candidato)s)")
            atributos = {
                'eleicao': eleicao_id,
                'turno': parsers.parse_turno(candidatura.turno),
                'candidato': id_candidato,
                'cidade': cidade_id,
                'estado': estado_id,
                'numero_candidato': parsers.parse_numero_candidato(candidatura.numero_candidato),
                'nome_urna': parsers.parse_nome_urna(candidatura.nome_urna),
                'partido': partido_id,
                'legenda_nome': parsers.parse_legenda(candidatura.legenda_nome),
                'legenda_composicao': parsers.parse_legenda(candidatura.legenda_composicao),
                'cargo': parsers.parse_cargo(candidatura.cargo),
                'despesa_maxima': parsers.parse_despesa_maxima(candidatura.despesa_maxima),
                'situacao_candidatura': parsers.parse_situacao_candidatura(candidatura.situacao_candidatura),
                'resultado_candidatura': parsers.parse_resultado_candidatura(candidatura.resultado_candidatura),
                'sequencial_candidato': candidatura.sequencial_candidato
            }

            try:
                cursor.execute(query, atributos)
                cnx.commit()
            except:
                print(candidatura)
                cursor.close()
                cnx.close()
                raise

            cursor.close()
            cnx.close()

            sys.stdout.write('\r{} de {}'.format((candidatura[0] + 1), total_candidaturas))
            sys.stdout.flush()

        sys.stdout.write('\n')
        tempo_final_estado = time.time()
        print('Tempo total desse estado: {:.2f}s \n'.format((tempo_final_estado - tempo_inicio_estado)))

    tempo_final_ano = time.time()
    print('Tempo total em {}: {:.2f}s \n'.format(ano, (tempo_final_ano - tempo_inicio_ano)))

def _get_candidato_id(cpf, titulo_eleitoral, cnx, cursor):
    # Verificar se o candidato já está cadastrado
    query = ("SELECT id FROM candidato WHERE "
             "cpf = %(cpf)s AND titulo_eleitoral = %(titulo_eleitoral)s")
    parametros = {
        'cpf': parsers.parse_cpf(cpf),
        'titulo_eleitoral': parsers.parse_titulo_eleitoral(titulo_eleitoral)
    }

    try:
        cursor.execute(query, parametros)
    except:
        print(query)
        cursor.close()
        cnx.close()
        raise

    resultados = cursor.fetchall()
    candidato_id = resultados[0][0] if len(resultados) > 0 else 0

    return candidato_id

def _get_cidade_id(cidade, estado_id, cnx, cursor):
    # Buscar id da cidade
    query = ("SELECT id FROM cidade WHERE "
             "nome = %(cidade)s AND "
             "estado_id = %(estado_id)s")
    parametros = {
        'cidade': parsers.parse_cidade(cidade),
        'estado_id': estado_id
    }

    try:
        cursor.execute(query, parametros)
    except:
        print(query)
        cursor.close()
        cnx.close()
        raise

    cidades = cursor.fetchall()
    cidade_id = cidades[0][0] if len(cidades) > 0 else None

    return cidade_id

def _get_eleicao_id(cnx, cursor, ano, descricao):
    query = ("SELECT id FROM eleicao WHERE "
             "ano = %(ano)s AND descricao = %(descricao)s")
    parametros = {
        'ano': parsers.parse_ano(ano),
        'descricao': parsers.parse_descricao(descricao)
    }

    try:
        cursor.execute(query, parametros)
    except:
        print(query)
        cursor.close()
        cnx.close()
        raise

    resultados = cursor.fetchall()
    eleicao_id = resultados[0][0] if len(resultados) > 0 else None

    return eleicao_id

def _get_estado_id(cnx, cursor, sigla_estado):
    query = ("SELECT id FROM estado WHERE "
             "sigla = %(sigla_estado)s")
    parametros = {
        'sigla_estado': parsers.parse_estado(sigla_estado)
    }

    try:
        cursor.execute(query, parametros)
    except:
        print(query)
        cursor.close()
        cnx.close()
        raise

    resultados = cursor.fetchall()
    estado_id = resultados[0][0] if len(resultados) > 0 else None

    return estado_id

def _get_partido_id(cnx, cursor, numero):
    query = ("SELECT id FROM partido WHERE "
             "numero = %(numero_partido)s")
    parametros = {
        'numero_partido': parsers.parse_partido(numero)
    }

    try:
        cursor.execute(query, parametros)
    except:
        print(query)
        cursor.close()
        cnx.close()
        raise

    resultados = cursor.fetchall()
    partido_id = resultados[0][0] if len(resultados) > 0 else None

    return partido_id


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

def _get_colunas_candidatura(ano):
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

        'ANO_ELEICAO',
        'NUM_TURNO',
        'DESCRICAO_ELEICAO',
        'SIGLA_UE',
        'DESCRICAO_UE',
        'SIGLA_UF',
        'CODIGO_CARGO',
        'NUMERO_CANDIDATO',
        'NOME_URNA_CANDIDATO',
        'COD_SITUACAO_CANDIDATURA',
        'NUMERO_PARTIDO',
        'COMPOSICAO_LEGENDA',
        'NOME_LEGENDA',
        'DESPESA_MAX_CAMPANHA',
        'COD_SIT_TOT_TURNO'
    ]

    ano2018 = [
        'NM_CANDIDATO',
        'DT_NASCIMENTO',
        'NR_CPF_CANDIDATO',
        'NR_TITULO_ELEITORAL_CANDIDATO',
        'NM_EMAIL',
        'NM_MUNICIPIO_NASCIMENTO',
        'SG_UF_NASCIMENTO',
        'CD_NACIONALIDADE',
        'CD_GRAU_INSTRUCAO',
        'CD_OCUPACAO',
        'CD_GENERO',
        'CD_COR_RACA',

        'ANO_ELEICAO',
        'NR_TURNO',
        'DS_ELEICAO',
        'SG_UE',
        'NM_UE',
        'SG_UF',
        'CD_CARGO',
        'NR_CANDIDATO',
        'NM_URNA_CANDIDATO',
        'CD_SITUACAO_CANDIDATURA',
        'NR_PARTIDO',
        'DS_COMPOSICAO_COLIGACAO',
        'NM_COLIGACAO',
        'NR_DESPESA_MAX_CAMPANHA',
        'CD_SIT_TOT_TURNO',
        'SQ_CANDIDATO',
    ]

    if (ano < 2012):
        return padrao
    elif (ano == 2018):
        return ano2018
    else:
        return (padrao + ['NM_EMAIL'])

def _get_map_colunas_candidatura(ano):
    sem_email = {
        'NOME_CANDIDATO': 'nome',
        'DATA_NASCIMENTO': 'data_nascimento',
        'CPF_CANDIDATO': 'cpf',
        'NUM_TITULO_ELEITORAL_CANDIDATO': 'titulo_eleitoral',
        'NOME_MUNICIPIO_NASCIMENTO': 'cidade_nascimento',
        'SIGLA_UF_NASCIMENTO': 'sigla_estado_nascimento',
        'CODIGO_NACIONALIDADE': 'nacionalidade',
        'COD_GRAU_INSTRUCAO': 'grau_instrucao',
        'CODIGO_OCUPACAO': 'ocupacao',

        'ANO_ELEICAO': 'ano',
        'NUM_TURNO': 'turno',
        'DESCRICAO_ELEICAO': 'descricao_eleicao',
        'SIGLA_UE': 'sigla_cidade_eleicao',
        'DESCRICAO_UE': 'cidade_eleicao',
        'SIGLA_UF': 'sigla_estado_eleicao',
        'CODIGO_CARGO': 'cargo',
        'NUMERO_CANDIDATO': 'numero_candidato',
        'NOME_URNA_CANDIDATO': 'nome_urna',
        'COD_SITUACAO_CANDIDATURA': 'situacao_candidatura',
        'NUMERO_PARTIDO': 'partido',
        'COMPOSICAO_LEGENDA': 'legenda_composicao',
        'NOME_LEGENDA': 'legenda_nome',
        'DESPESA_MAX_CAMPANHA': 'despesa_maxima',
        'COD_SIT_TOT_TURNO': 'resultado_candidatura'
    }
    com_email = {
        'NOME_CANDIDATO': 'nome',
        'DATA_NASCIMENTO': 'data_nascimento',
        'CPF_CANDIDATO': 'cpf',
        'NUM_TITULO_ELEITORAL_CANDIDATO': 'titulo_eleitoral',
        'NM_EMAIL': 'email',
        'NOME_MUNICIPIO_NASCIMENTO': 'cidade_nascimento',
        'SIGLA_UF_NASCIMENTO': 'sigla_estado_nascimento',
        'CODIGO_NACIONALIDADE': 'nacionalidade',
        'COD_GRAU_INSTRUCAO': 'grau_instrucao',
        'CODIGO_OCUPACAO': 'ocupacao',

        'ANO_ELEICAO': 'ano',
        'NUM_TURNO': 'turno',
        'DESCRICAO_ELEICAO': 'descricao_eleicao',
        'SIGLA_UE': 'sigla_cidade_eleicao',
        'DESCRICAO_UE': 'cidade_eleicao',
        'SIGLA_UF': 'sigla_estado_eleicao',
        'CODIGO_CARGO': 'cargo',
        'NUMERO_CANDIDATO': 'numero_candidato',
        'NOME_URNA_CANDIDATO': 'nome_urna',
        'COD_SITUACAO_CANDIDATURA': 'situacao_candidatura',
        'NUMERO_PARTIDO': 'partido',
        'COMPOSICAO_LEGENDA': 'legenda_composicao',
        'NOME_LEGENDA': 'legenda_nome',
        'DESPESA_MAX_CAMPANHA': 'despesa_maxima',
        'COD_SIT_TOT_TURNO': 'resultado_candidatura'
    }

    ano2018 = {
        'NM_CANDIDATO': 'nome',
        'DT_NASCIMENTO': 'data_nascimento',
        'NR_CPF_CANDIDATO': 'cpf',
        'NR_TITULO_ELEITORAL_CANDIDATO': 'titulo_eleitoral',
        'NM_EMAIL': 'email',
        'NM_MUNICIPIO_NASCIMENTO': 'cidade_nascimento',
        'SG_UF_NASCIMENTO': 'sigla_estado_nascimento',
        'CD_NACIONALIDADE': 'nacionalidade',
        'CD_GRAU_INSTRUCAO': 'grau_instrucao',
        'CD_OCUPACAO': 'ocupacao',
        'CD_GENERO': 'genero',
        'CD_COR_RACA': 'cor_raca',

        'ANO_ELEICAO': 'ano' ,
        'NR_TURNO': 'turno' ,
        'DS_ELEICAO': 'descricao_eleicao' ,
        'SG_UE': 'sigla_cidade_eleicao' ,
        'NM_UE': 'cidade_eleicao' ,
        'SG_UF': 'sigla_estado_eleicao' ,
        'CD_CARGO': 'cargo' ,
        'NR_CANDIDATO': 'numero_candidato' ,
        'NM_URNA_CANDIDATO': 'nome_urna' ,
        'CD_SITUACAO_CANDIDATURA': 'situacao_candidatura' ,
        'NR_PARTIDO': 'partido' ,
        'DS_COMPOSICAO_COLIGACAO': 'legenda_composicao' ,
        'NM_COLIGACAO': 'legenda_nome' ,
        'NR_DESPESA_MAX_CAMPANHA': 'despesa_maxima' ,
        'CD_SIT_TOT_TURNO': 'resultado_candidatura' ,
        'SQ_CANDIDATO': 'sequencial_candidato' ,
    }

    if (ano < 2012):
        return sem_email
    elif (ano == 2018):
        return ano2018
    else:
        return com_email
