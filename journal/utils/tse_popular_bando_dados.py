import sys

import utils.tse_migracao_dados as tmd
import utils.tse_migrar_dados_por_estado as tmde

# python3 popular.py [ano]
arg1 = sys.argv[1]

try:
    ano = int(arg1)
except Exception:
    print('Digite um ano válido (entre 2004 e 2016)')
    raise

# povoamento = tmd.MigracaoDadosTSE()
# povoamento.migrar(ano)
tmde.migrar_dados(ano)
