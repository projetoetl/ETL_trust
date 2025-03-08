

####  URL - EXTRAÇÃO   ###
- https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/{ano_atual}{mes_atual}/


<<<<<<< HEAD
####  PACOTE   ###
=======
####  conceito   ###
>>>>>>> c9f11c2d1dea44c69ef971651bba1ae1fdd3e382
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
<<<<<<< HEAD

=======
>>>>>>> c9f11c2d1dea44c69ef971651bba1ae1fdd3e382
Vantagens do Parquet para grandes arquivos:
 Compactação Eficiente
 Leitura mais Rápida
 Suporte a Tipos de Dados Complexos
 Leitura Paralela
 Suporte a Metadados

 ####  TRATAMENTO/ REGRAS ###
 - PREZAR PELA CONFIABILIDADE, INTEGRIDADE E RASTREABILIDADE DOS DADOS  (;_;)

<<<<<<< HEAD
 - ALGUMAS BASES JÁ FORAM MANIPULADAS DIRETAMENTE NO PROCECSSO DE LEITURA E TRANSFORMAÇÃO DO
 NOVO ARQUIVO, TRAZENDO MAIS AGILIDADE NO PROCESSO DE TRANSFORMAÇÃO.

 - PASTA * ETL_PRATA * CONTÉM OS ARQUIVOS PRONTOS PARA SEREM PROCESSADOS NA ÚTIMA 
 ETAPA, APÓS A REALIZAÇÃO DO PROCESSAMENTO, TODOS OS ARQUIVOS SÃO SALVOS DENTRO DA PASTA, PARA SEGUIR O PROCESSO DE MERGE ENTRE TODAS AS TABELAS PARA GERAR UM UNICO DATA FRAME.

 - PASTA * ETL_OURO *  CONTÉM O ARQUIVO FINAL CONTENDO A JUNÇÃO DE TODAS AS BASES EM UMA SÓ, ASSIM OS 
 ANALISTAS E CIENTISTAS DE DADOS DA EMPRESA VÃO PODER ANALISAR A BASE.

 
=======
>>>>>>> c9f11c2d1dea44c69ef971651bba1ae1fdd3e382
