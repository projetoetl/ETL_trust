# Processamento de dados da receita federal
A Receita Federal do Brasil disponibiliza bases com os dados públicos do cadastro nacional de pessoas jurídicas (CNPJ).
Esse projeto traz uma pipeline para baixar, extrair e manipular os dados do link  (https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/2025-01/) processamento realizando uma ETL para o uso da empresa Trust Work.

### Infraestrutura necessária:
- [Python 3.8](https://www.python.org/downloads/release/python-3810/)

### pacote utilizados no processo:
- pyarrow
- Pandas
- Zipfile
- Shutil
- Bs4 - BeautifulSoup
- Requests
- Os
- Glob
- Tqdm


### pacote de dados que contem na base:
- Para maiores informações, consulte o [layout](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/arquivos/NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf).
  - `empresa`: dados cadastrais da empresa em nível de matriz
  - `estabelecimento`: dados analíticos da empresa por unidade / estabelecimento (telefones, endereço, filial, etc)
  - `socios`: dados cadastrais dos sócios das empresas
  - `simples`: dados de MEI e Simples Nacional
  - `cnae`: código e descrição dos CNAEs
  - `quals`: tabela de qualificação das pessoas físicas - sócios, responsável e representante legal.
  - `natju`: tabela de naturezas jurídicas - código e descrição.
  - `moti`: tabela de motivos da situação cadastral - código e descrição.
  - `pais`: tabela de países - código e descrição.
  - `munic`: tabela de municípios - código e descrição.
