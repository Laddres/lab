import sys
import os
import glob
import time

import pandas as pd
import numpy as np
import mysql.connector as mysql

import utils.tse_parsers as parsers
import utils.tse_data_headers as headers

class MigracaoDadosTSE:
    PREFIXO_ARQUIVO_CANDIDATURAS = 'consulta_cand_'
    DIRETORIO_TEMPORARIO = os.path.join('../temp')
    BD_CONFIG = {
        'user': 'root',
        'password': '',
        'host': '127.0.0.1',
        'database': 'laddres'
    }

    # def __init__(self):
    #
    # def __del__(self):

    def migrar(self, ano):
        self.cnx = mysql.connect(**self.BD_CONFIG)

        tempo_inicio_ano = time.time()

        arquivos = self.PREFIXO_ARQUIVO_CANDIDATURAS + str(ano) + '*.txt'
        pathes = os.path.join(self.DIRETORIO_TEMPORARIO, arquivos)
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

            candidaturas = (
                df_estado[self._get_colunas_candidatura(ano)]
                .rename(columns = self._get_map_colunas_candidatura(ano))
            )

            total_candidaturas = candidaturas.shape[0]
            print('Total de candidaturas em {}: {}'.format(ano, total_candidaturas))

            for candidatura in candidaturas.itertuples():
                cursor = self.cnx.cursor(buffered=True)

                estado_id = self._get_estado_id(sigla_estado=candidatura.sigla_estado_nascimento)
                cidade_id = None if (not estado_id) else self._get_cidade_id(
                    cidade=candidatura.cidade_nascimento,
                    estado_id=estado_id)
                email = parsers.parse_email(candidatura.email) if ('email' in candidaturas.columns) else None
                id_candidato = self._get_candidato_id(
                    cpf=candidatura.cpf,
                    titulo_eleitoral=candidatura.titulo_eleitoral)

                if (id_candidato == 0):
                    # Cadastrar candidato
                    query = ("INSERT INTO candidato "
                                   "(nome, data_nascimento, cpf, titulo_eleitoral, email, cidade_id, ocupacao_id, nacionalidade_id, grau_instrucao_id) "
                                   "VALUES (%(nome)s, %(data_nascimento)s, %(cpf)s, %(titulo_eleitoral)s, %(email)s, %(cidade_id)s, %(ocupacao)s, %(nacionalidade)s, %(grau_instrucao)s)")
                    atributos = {
                        'nome': parsers.parse_nome(candidatura.nome),
                        'data_nascimento': parsers.parse_data(candidatura.data_nascimento),
                        'cpf': parsers.parse_cpf(candidatura.cpf),
                        'titulo_eleitoral': parsers.parse_titulo_eleitoral(candidatura.titulo_eleitoral),
                        'email': email,
                        'cidade_id': cidade_id,
                        'ocupacao': parsers.parse_ocupacao(candidatura.ocupacao),
                        'nacionalidade': parsers.parse_nacionalidade(candidatura.nacionalidade),
                        'grau_instrucao': parsers.parse_grau_instrucao(candidatura.grau_instrucao)
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
                        'nome': parsers.parse_nome(candidatura.nome),
                        'data_nascimento': parsers.parse_data(candidatura.data_nascimento),
                        'email': email,
                        'cidade_id': cidade_id,
                        'ocupacao': parsers.parse_ocupacao(candidatura.ocupacao),
                        'nacionalidade': parsers.parse_nacionalidade(candidatura.nacionalidade),
                        'grau_instrucao': parsers.parse_grau_instrucao(candidatura.grau_instrucao),
                        'candidato_id': id_candidato
                    }

                try:
                    cursor.execute(query, atributos)
                except Exception as e:
                    cursor.close()
                    self._handle_exception(exception=e, row=candidatura)

                # Cadastrar candidatura
                candidato_id = cursor.lastrowid if (id_candidato == 0) else id_candidato
                eleicao_id = self._get_eleicao_id(
                    ano=candidatura.ano,
                    descricao=candidatura.descricao_eleicao)
                estado_id = self._get_estado_id(
                    sigla_estado=candidatura.sigla_estado_eleicao)
                cidade_id = None if (not estado_id) else self._get_cidade_id(
                    cidade=candidatura.cidade_eleicao,
                    sigla_ue=candidatura.sigla_cidade_eleicao,
                    estado_id=estado_id)
                partido_id = self._get_partido_id(
                    numero=candidatura.partido)

                query = ("INSERT INTO candidatura "
                         "(eleicao_id, turno, candidato_id, cidade_id, estado_id, numero_candidato, nome_urna, partido_id, legenda_nome, legenda_composicao, cargo_id, despesa_maxima, situacao_candidatura_id, resultado_candidatura_id) "
                         "VALUES (%(eleicao)s, %(turno)s, %(candidato)s, %(cidade)s, %(estado)s, %(numero_candidato)s, %(nome_urna)s, %(partido)s, %(legenda_nome)s, %(legenda_composicao)s, %(cargo)s, %(despesa_maxima)s, %(situacao_candidatura)s, %(resultado_candidatura)s)")
                atributos = {
                    'eleicao': eleicao_id,
                    'turno': parsers.parse_turno(candidatura.turno),
                    'candidato': candidato_id,
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
                    'resultado_candidatura': parsers.parse_resultado_candidatura(candidatura.resultado_candidatura)
                }

                try:
                    cursor.execute(query, atributos)
                    self.cnx.commit()
                except:
                    cursor.close()
                    self._handle_exception(exception=e, row=candidatura)

                sys.stdout.write('\r{} de {}'.format((candidatura[0] + 1), total_candidaturas))
                sys.stdout.flush()

                cursor.close()

            sys.stdout.write('\n')
            tempo_final_estado = time.time()
            print('Tempo total desse estado: {:.2f}s \n'.format((tempo_final_estado - tempo_inicio_estado)))

        tempo_final_ano = time.time()
        print('Tempo total em {}: {:.2f}s \n'.format(ano, (tempo_final_ano - tempo_inicio_ano)))

        self.cnx.close()

    def _load(self, tabela):
        query = ("SELECT * FROM " + tabela)
        return pd.read_sql(query, self.cnx)
    def _execute_select(self, query, parametros):
        cursor = self.cnx.cursor(buffered=True)
        try:
            cursor.execute(query, parametros)
        except Exception as e:
            cursor.close()
            self._handle_exception(exception=e, row=query)
        resultados = cursor.fetchall()
        cursor.close()
        return resultados[0][0] if len(resultados) == 1 else None

    def _handle_exception(self, exception, row):
        print(row)
        self.cnx.close()
        raise exception

    def _get_candidato_id(self, cpf, titulo_eleitoral):
        query = ("SELECT id FROM candidato WHERE "
                 "cpf = %(cpf)s AND titulo_eleitoral = %(titulo_eleitoral)s")
        parametros = {
            'cpf': parsers.parse_cpf(cpf),
            'titulo_eleitoral': parsers.parse_titulo_eleitoral(titulo_eleitoral)
        }
        cursor = self.cnx.cursor(buffered=True)
        try:
            cursor.execute(query, parametros)
        except Exception as e:
            cursor.close()
            self._handle_exception(exception=e, row=query)
        resultados = cursor.fetchall()
        cursor.close()

        return resultados[0][0] if len(resultados) > 0 else 0
    def _get_cidade_id(self, cidade, estado_id, sigla_ue=0):
        query = ("SELECT id FROM cidade WHERE "
                 "(nome = %(cidade)s) OR (codigo_tse = %(sigla_ue)s) "
                 "AND (estado_id = %(estado_id)s)")
        parametros = {
            'cidade': parsers.parse_cidade(cidade),
            'sigla_ue': parsers.parse_sigla_ue(sigla_ue),
            'estado_id': estado_id
        }
        return self._execute_select(query=query, parametros=parametros)
    def _get_eleicao_id(self, ano, descricao):
        query = ("SELECT id FROM eleicao WHERE "
                 "ano = %(ano)s AND descricao = %(descricao)s")
        parametros = {
            'ano': parsers.parse_ano(ano),
            'descricao': parsers.parse_descricao(descricao)
        }
        self._execute_select(query=query, parametros=parametros)
    def _get_estado_id(self, sigla_estado):
        query = ("SELECT id FROM estado WHERE "
                 "sigla = %(sigla_estado)s")
        parametros = {
            'sigla_estado': parsers.parse_estado(sigla_estado)
        }
        return self._execute_select(query=query, parametros=parametros)
    def _get_partido_id(self, numero):
        query = ("SELECT id FROM partido WHERE "
                 "numero = %(numero_partido)s")
        parametros = {
            'numero_partido': parsers.parse_partido(numero)
        }
        return self._execute_select(query=query, parametros=parametros)

    def _get_colunas_candidatura(self, ano):
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

        return padrao if (ano < 2012) else (padrao + ['NM_EMAIL'])
    def _get_map_colunas_candidatura(self, ano):
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

        return sem_email if (ano < 2012) else com_email
