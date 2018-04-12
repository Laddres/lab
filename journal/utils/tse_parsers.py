import re

# Retorna um valor inteiro, se possível
def parse_inteiro(valor):
    try:
        valor_inteiro = int(valor)
        return valor_inteiro
    except:
        return None
# Elimina os caracteres especiais se for do tipo string
def limpar_string(string):
    if (type(string) == str):
        return re.sub(r'[\\"]', '', string)
    else:
        return None
# Verifica se um determinado ano é bissexto
def eh_ano_bissexto(ano):
    if ((ano%4 == 0) and (ano%100 != 0) or (ano%400 == 0)):
        return True
    else:
        return False

"""
Parsers para o dataset de Candidatos
"""
def parse_nome(nome):
    return limpar_string(nome)

def parse_data(data_original):
    meses = {
        'JAN': '01',
        'FEB': '02',
        'MAR': '03',
        'APR': '04',
        'MAY': '05',
        'JUN': '06',
        'JUL': '07',
        'AUG': '08',
        'SEP': '09',
        'OCT': '10',
        'NOV': '11',
        'DEC': '12',
    }

    ano = 0
    mes = 0
    dia = 0

    data = str(data_original)

    if '/' in data:
        ano = data[6:10]
        mes = data[3:5]
        dia = data[0:2]
    elif '-' in data:
        ano = ('19' + data[7:9]) if (int(data[7:9]) > 18) else ('20' + data[7:9])
        mes = meses[data[3:6]]
        dia = data[0:2]
    elif ' ' in data:
        return None
    elif len(data) == 8:
        ano = data[4:8]
        mes = data[2:4]
        dia = data[0:2]
    else:
        # print('Erro ao formatar data:', data)
        return None

    try:
        ano_int = int(ano)
        mes_int = int(mes)
        dia_int = int(dia)

        if ((mes_int <= 0 or mes_int > 12) or (dia_int <= 0 or dia_int > 31) or (ano_int <= 0)):
            return None
        if (
            (mes_int in [4, 6, 9, 11] and dia_int > 30) or
            (mes_int == 2 and eh_ano_bissexto(ano_int) and dia_int > 29) or
            (mes_int == 2 and not eh_ano_bissexto(ano_int) and dia_int > 28)
        ):
            return None

        return ano + '-' + mes + '-' + dia
    except:
        return None

def parse_cpf(cpf):
    try:
        cpf_inteiro = int(cpf)
        return cpf if (cpf_inteiro > 0) else None
    except:
        return None

def parse_titulo_eleitoral(titulo_eleitoral):
    try:
        titulo_eleitoral_inteiro = int(titulo_eleitoral)
        return titulo_eleitoral if (titulo_eleitoral_inteiro > 0) else None
    except:
        return None

def parse_email(email):
    string = limpar_string(email)
    return string if (string and '@' in string) else None

def parse_cidade(cidade):
    return str(limpar_string(cidade))

def parse_estado(estado):
    return str(limpar_string(estado))

def parse_ocupacao(ocupacao):
    id = parse_inteiro(ocupacao)
    return id if (id and (id >= 0 and id <= 999)) else 0

def parse_nacionalidade(nacionalidade):
    id = parse_inteiro(nacionalidade)
    return id if (id and (id >= 0 and id <= 4)) else 0

def parse_grau_instrucao(grau_instrucao):
    id = parse_inteiro(grau_instrucao)
    return id if (id and (id >= 0 and id <= 8)) else 0

"""
Parsers para o dataset de Candidaturas
"""
def parse_ano(ano):
    try:
        numero_inteiro = int(ano)
        return numero_inteiro if (numero_inteiro > 0) else 0
    except:
        return 0
def parse_cargo(cargo):
    id = parse_inteiro(cargo)
    return id if (id and (id >= 1 and id <= 13)) else None
def parse_descricao(descricao):
    return str(limpar_string(descricao))
def parse_despesa_maxima(despesa_maxima):
    try:
        numero_inteiro = int(despesa_maxima)
        return numero_inteiro if (numero_inteiro > 0 and numero_inteiro < 1000000000) else 0
    except:
        return 0
def parse_legenda(legenda):
    string = limpar_string(legenda)
    return string if (string and '#' not in string) else None
def parse_nome_urna(nome):
    return limpar_string(nome)
def parse_numero_candidato(numero):
    try:
        numero_inteiro = int(numero)
        return numero_inteiro if (numero_inteiro > 0) else None
    except:
        return None
def parse_partido(numero):
    try:
        numero_inteiro = int(numero)
        return numero_inteiro if (numero_inteiro > 0) else None
    except:
        return None
def parse_resultado_candidatura(resultado_candidatura):
    id = parse_inteiro(resultado_candidatura)
    return id if (id and (id >= 1 and id <= 12)) else None
def parse_situacao_candidatura(situacao_candidatura):
    id = parse_inteiro(situacao_candidatura)
    return id if (id and (id >= 2 and id <= 19)) else None
def parse_turno(turno):
    id = parse_inteiro(turno)
    return id if (id > 0) else None
