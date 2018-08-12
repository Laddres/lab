import pandas as pd
import numpy as np
import sys
import mysql.connector as mysql

BD_CONFIG = {
    'user': 'root',
    'password': '',
    'host': '127.0.0.1',
    'database': 'laddres'
}

def dataframe_to_database(dataframe, tabela, colunas_tabela, colunas_dataframe):
    cnx = mysql.connect(**BD_CONFIG)
    cursor = cnx.cursor()

    total = len(dataframe)
    for row in dataframe.itertuples():
        columns = ', '.join(colunas_tabela)
        placeholders = ', '.join(np.full((1, len(colunas_tabela)), '%s')[0])

        sql = 'INSERT INTO {} ({}) VALUES ({})'.format(tabela, columns, placeholders)
        values = []
        for column in colunas_dataframe:
            values.append(getattr(row, column))

        try:
            cursor.execute(sql, tuple(values))
            cnx.commit()
        except:
            print(row)
            cursor.close()
            cnx.close()
            raise

        sys.stdout.write('\r{} de {}'.format((row.Index + 1), total))
        sys.stdout.flush()

    cursor.close()
    cnx.close()
