import pandas as pd
import mysql.connector as mysql

BD_CONFIG = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    'database': 'laddres'
}

def database_to_dataframe(tabela, colunas_tabela, condicao = ''):
    cnx = mysql.connect(**BD_CONFIG)

    columns = ', '.join(colunas_tabela) if colunas_tabela else '*'
    df = pd.read_sql(
        'SELECT {} FROM {} {}'.format(columns, tabela, condicao),
        con=cnx
    )

    cnx.close()

    return df
