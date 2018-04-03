# Laddres - Laboratório de Análise de Desempenho Dos REpresentantes da Sociedade

## Laboratório

Ambiente de trabalho para experimentação, em que, baseado no método de desenvolvimento guiado por hipóteses, permite que pesquisadores e contribuidores do projeto realizem experimentos, descubram novas possibilidades ou confirmem suas expectativas.


## Estrutura do Repositório

### Data

Diretório para os arquivos de dataset e seus respectivos arquivos fonte, escritos em python, com o código necessário para gerar um novo dataset.

_Obs.: Os arquivos de dataset serão ignorados pelo git e devem ser anexados separadamente._

Os arquivos devem ser prefixados com a data `YYYY-MM-DD-` e comprimidos em formato `.xz`.

Para montar o dataset com os dados dos candidatos oriundos do TSE, por exemplo, o resultado seria:

```
- data
  + 2018-02-15-candidatos-tse.xz
  + 2018-02-15-candidatos-tse.py
```

### Journal

Diretório para pesquisa, testes de novas hipóteses e descrição do passo a passo do desenvolvimento do projeto.

Os arquivos devem ter a estrutura `YYYY-MM-DD-[usuario-github]-[descrição 2-4 palavras].ipynb`.

---

Ludiico
Departamento de Computação
Universidade Federal de Sergipe
