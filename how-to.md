

####  URL - EXTRAÇÃO   ###
- https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/{ano_atual}{mes_atual}/


####  conceito   ###
-  VIRTUAL ENVIROMENT  (VENV não está ativado)


####  PACOTES UTILIZADO   ###
- pyarrow
- Pandas
- Zipfile
- Shutil
- Bs4 - BeautifulSoup
- Requests
- Os
- Glob
- Tqdm


####  ETAPAS DO DADO  ###
- DATA RAW  - Dados baixados em formato zip precisando ser extraido 

- DATA READY - Alguns dados foram manipulados em formato ( parquert ) melhorando a eficiência do processamento, tendo em vista que o parquet trabalha com uma estrutura de leitura 
coluna por coluna.
Vantagens do Parquet para grandes arquivos:
 Compactação Eficiente
 Leitura mais Rápida
 Suporte a Tipos de Dados Complexos
 Leitura Paralela
 Suporte a Metadados

 ####  TRATAMENTO/ REGRAS ###
 - PREZAR PELA CONFIABILIDADE, INTEGRIDADE E RASTREABILIDADE DOS DADOS  (;_;)

