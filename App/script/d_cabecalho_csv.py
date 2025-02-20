import pandas as pd
import os
import gc
import glob
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa
from App.script.caminhos_csv import caminho_cnae, caminho_motivo, caminho_simples, caminho_municipios,caminho_natureza,caminho_pais, caminho_qualificacao,caminho_socio, caminho_empresas, caminho_estabelecimento

def csvToSeriesIndex(filename):
    codigo = ""
    descricao = ""    

    for linha in pd.read_csv(filename,
                        sep=";", 
                        encoding="latin1",  
                        dtype=str, 
                        quotechar='"', 
                        header=None,  # Não usa a primeira linha como cabeçalho
                        chunksize=30000):
        
        linha.columns = [
            'CÓDIGO','DESCRIÇÃO'
        ]

        codigo +=  linha['CÓDIGO']
        descricao += linha['DESCRIÇÃO']

    lista = pd.Series(list(descricao), index=list(codigo))
    
    return lista

def add_cabecalho():
    caminho_arquivo = caminho_cnae()
    novo_arquivo = r"App/data/arquivos_extraidos/CNAE/CNAE_FORMATADO.CSV"  # Nome do novo arquivo

    # Lendo o arquivo sem cabeçalho
    df = pd.read_csv(
        caminho_arquivo, 
        sep=";", 
        encoding="latin1",  
        dtype=str, 
        quotechar='"', 
        header=None  # Indica que não há cabeçalho na primeira linha
    )

    # Definindo manualmente os nomes das colunas
    df.columns = ["CÓDIGO", "DESCRIÇÃO"]

    df["CÓDIGO"] = df["CÓDIGO"].str.replace(",", ".").astype(float)

    # Salvando em um novo CSV com os novos cabeçalhos
    df.to_csv(novo_arquivo, sep=";", index=False, encoding="latin1")



 # ---------MOTIVO 
    caminho_arquivo = caminho_motivo()
    novo_arquivo = "App/data/arquivos_extraidos/MOTIC/motivos_FORMATADO.csv"  # Nome do novo arquivo

    # Lendo o arquivo sem cabeçalho
    df = pd.read_csv(
        caminho_arquivo, 
        sep=";", 
        encoding="latin1",  
        dtype=str, 
        quotechar='"', 
        header=None  # Indica que não há cabeçalho na primeira linha
    )

    # Definindo manualmente os nomes das colunas
    df.columns = ["CÓDIGO", "MOTIVO"]

    # Tentando converter a coluna 'CÓDIGO' para inteiro, tratando erros de conversão
    df["CÓDIGO"] = pd.to_numeric(df["CÓDIGO"], errors='coerce')  # Converte para float, mantendo erros como NaN
    df["CÓDIGO"] = df["CÓDIGO"].fillna(0).astype(int)  # Preenche valores NaN com 0 e converte para inteiro

    # Salvando em um novo CSV com os novos cabeçalhos
    df.to_csv(novo_arquivo, sep=";", index=False, encoding="latin1", quoting=1) 
    
    
# ------------- MUNICIPIO
    caminho_arquivo = caminho_municipios()
    novo_arquivo = "App/data/arquivos_extraidos/MUNIC/municipios_FORMATADO.csv"  # Nome do novo arquivo

    # Lendo o arquivo sem cabeçalho
    df = pd.read_csv(
        caminho_arquivo, 
        sep=";", 
        encoding="latin1",  
        dtype=str, 
        quotechar='"', 
        header=None  # Indica que não há cabeçalho na primeira linha
    )

    # Definindo manualmente os nomes das colunas
    df.columns = ["CÓDIGO", "MUNICIPIOS"]

    # Tentando converter a coluna 'CÓDIGO' para inteiro, tratando erros de conversão
    df["CÓDIGO"] = pd.to_numeric(df["CÓDIGO"], errors='coerce')  # Converte para float, mantendo erros como NaN
    df["CÓDIGO"] = df["CÓDIGO"].fillna(0).astype(int)  # Preenche valores NaN com 0 e converte para inteiro

    # Salvando em um novo CSV com os novos cabeçalhos
    df.to_csv(novo_arquivo, sep=";", index=False, encoding="latin1", quoting=1)  
    
    
# ------------- NATUREZA
    caminho_arquivo = caminho_natureza()
    novo_arquivo = "App/data/arquivos_extraidos/NATJU/natureza_FORMATADO.csv"  # Nome do novo arquivo

    # Lendo o arquivo sem cabeçalho
    df = pd.read_csv(
        caminho_arquivo, 
        sep=";", 
        encoding="latin1",  
        dtype=str, 
        quotechar='"', 
        header=None  # Indica que não há cabeçalho na primeira linha
    )

    # Definindo manualmente os nomes das colunas
    df.columns = ["CÓDIGO", "DESCRIÇÃO"]

    # Tentando converter a coluna 'CÓDIGO' para inteiro, tratando erros de conversão
    df["CÓDIGO"] = pd.to_numeric(df["CÓDIGO"], errors='coerce')  # Converte para float, mantendo erros como NaN
    df["CÓDIGO"] = df["CÓDIGO"].fillna(0).astype(int)  # Preenche valores NaN com 0 e converte para inteiro

    # Salvando em um novo CSV com os novos cabeçalhos
    df.to_csv(novo_arquivo, sep=";", index=False, encoding="latin1", quoting=1) 

# ------------- QUALIFICAÇÃO

    caminho_arquivo = caminho_qualificacao()
    novo_arquivo = "App/data/arquivos_extraidos/QUALS/QUALIFICACAO_FORMATADO.csv"  # Nome do novo arquivo

    # Lendo o arquivo sem cabeçalho
    df = pd.read_csv(
        caminho_arquivo, 
        sep=";", 
        encoding="latin1",  
        dtype=str, 
        quotechar='"', 
        header=None  # Indica que não há cabeçalho na primeira linha
    )

    # Definindo manualmente os nomes das colunas
    df.columns = ["CÓDIGO", "DESCRIÇÃO"]

    # Tentando converter a coluna 'CÓDIGO' para inteiro, tratando erros de conversão
    df["CÓDIGO"] = pd.to_numeric(df["CÓDIGO"], errors='coerce')  # Converte para float, mantendo erros como NaN
    df["CÓDIGO"] = df["CÓDIGO"].fillna(0).astype(int)  # Preenche valores NaN com 0 e converte para inteiro

    # Salvando em um novo CSV com os novos cabeçalhos
    df.to_csv(novo_arquivo, sep=";", index=False, encoding="latin1", quoting=1)


    
 # ------------- PAIS
    caminho_arquivo = caminho_pais()
    novo_arquivo = "App/data/arquivos_extraidos/PAIS/pais_FORMATADO.csv"  # Nome do novo arquivo

    # Lendo o arquivo sem cabeçalho
    df = pd.read_csv(
        caminho_arquivo, 
        sep=";", 
        encoding="latin1",  
        dtype=str, 
        quotechar='"', 
        header=None  # Indica que não há cabeçalho na primeira linha
    )

    # Definindo manualmente os nomes das colunas
    df.columns = ["CÓDIGO", "PAIS"]

    # Tentando converter a coluna 'CÓDIGO' para inteiro, tratando erros de conversão
    df["CÓDIGO"] = pd.to_numeric(df["CÓDIGO"], errors='coerce')  # Converte para float, mantendo erros como NaN
    df["CÓDIGO"] = df["CÓDIGO"].fillna(0).astype(int)  # Preenche valores NaN com 0 e converte para inteiro

    # Salvando em um novo CSV com os novos cabeçalhos
    df.to_csv(novo_arquivo, sep=";", index=False, encoding="latin1", quoting=1) 


 # ------------- SIMPLES
    pastas_simples = caminho_simples()
    listar_todos_os_arquivos = glob.glob(os.path.join(pastas_simples, "*.csv"))

    # Tamanho dos chunks para leitura dos CSVs
    chunk_size = 10000
    saindo_do_novo_arquivo = "NOVO_SIMPLES.parquet"

    # Lista para armazenar todos os chunks processados
    lista_chunks = []

    # Barra de progresso para o processo de leitura e gravação dos arquivos
    with tqdm(total=len(listar_todos_os_arquivos), desc="Processando os dados", unit="arquivos") as pbar:
        for arquivos in listar_todos_os_arquivos:
            print(f"Lendo {arquivos}")

            for chunk in pd.read_csv(arquivos, 
                                    sep=";", 
                                    encoding="latin1",  
                                    dtype=str, 
                                    quotechar='"', 
                                    header=None, 
                                    chunksize=chunk_size):

                # Renomear as colunas
                chunk.columns = ['cnpj_basico',
                                'opcao_pelo_simples',
                                'data_opcao_simples',
                                'data_exclusao_simples',
                                'opcao_mei',
                                'data_opcao_mei',
                                'data_exclusao_mei']
                
                # Garantir que a coluna 'cnpj_basico' exista
                if "cnpj_basico" not in chunk.columns:
                    chunk["cnpj_basico"] = chunk["opcao_pelo_simples"].str[:20]
                
                # Limpar espaços em branco
                chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)

                # Converter as colunas de data para o formato datetime
                chunk['data_opcao_simples'] = pd.to_datetime(chunk['data_opcao_simples'], errors='coerce')
                chunk['data_exclusao_simples'] = pd.to_datetime(chunk['data_exclusao_simples'], errors='coerce')
                chunk['data_opcao_mei'] = pd.to_datetime(chunk['data_opcao_mei'], errors='coerce')
                chunk['data_exclusao_mei'] = pd.to_datetime(chunk['data_exclusao_mei'], errors='coerce')

                # Manter as datas como datetime no formato 'yyyy-mm-dd' sem hora
                chunk['data_opcao_simples'] = chunk['data_opcao_simples'].dt.date
                chunk['data_exclusao_simples'] = chunk['data_exclusao_simples'].dt.date
                chunk['data_opcao_mei'] = chunk['data_opcao_mei'].dt.date
                chunk['data_exclusao_mei'] = chunk['data_exclusao_mei'].dt.date

                # Organizar as colunas finais
                chunk_organizado = chunk[['cnpj_basico',
                                        'opcao_pelo_simples',
                                        'data_opcao_simples',
                                        'data_exclusao_simples',
                                        'opcao_mei',
                                        'data_opcao_mei',
                                        'data_exclusao_mei']]

                # Adicionar o chunk à lista
                lista_chunks.append(chunk_organizado)

            pbar.update(1)

    # Concatenar todos os chunks em um único DataFrame
            dados_completos = pd.concat(lista_chunks, ignore_index=True)

            # Criar a tabela do PyArrow
            tabela_completa = pa.Table.from_pandas(dados_completos)

            # Escrever todos os dados concatenados no arquivo Parquet
            pq.write_table(tabela_completa, saindo_do_novo_arquivo, compression="SNAPPY")

            
            
            # Verificação do arquivo final
            if os.path.exists(saindo_do_novo_arquivo):
                print("Arquivo Parquet salvo com sucesso!")
            else:
                print("O arquivo Parquet não foi gerado.")
                
            lista_chunks.clear()
        print('LIBERAÇÃO DE MEMÓRIA FEITA')
        print('SEGUINDO PARA O PROCESSO DOS ESTABELECIMENTO')

 # ------------- SOCIO

    pasta_socios = caminho_socio()

    # Usar glob para listar todos os arquivos CSV dentro da pasta
    arquivos_csv = glob.glob(os.path.join(pasta_socios, "*.csv"))

    # Lista para armazenar os DataFrames de cada arquivo
    lista_dfs = []
    tipo_socio = {'1':'Pessoa Jurídica', '2':'Pessoa Física', '3':'Estrangeiro'}
    paises = csvToSeriesIndex(caminho_pais)
    quals = csvToSeriesIndex(caminho_qualificacao)

    # Iterar sobre os arquivos CSV e ler cada um
    for arquivo in arquivos_csv:
        # Ler o arquivo CSV, agora utilizando a primeira linha como cabeçalho
        df = pd.read_csv(arquivo, 
                        sep=";", 
                        encoding="latin1",  
                        dtype=str, 
                        quotechar='"', 
                        header=None)  # Não usa a primeira linha como cabeçalho
        
        # Atribuir os nomes das colunas manualmente, com base na estrutura esperada
        colunas = [
            "cnpj_basico", "identificador_socio", "nome_socio_razao_social", 
            "cpf_cnpj_socio", "qualificacao_socio", "data_entrada_sociedade", 
            "pais", "representante_legal", "nome_do_representante", 
            "qualificacao_representante_legal", "faixa_etaria"
        ]
        
        # Atribuir as colunas ao DataFrame
        df.columns = colunas

        # Verificar os nomes das colunas para garantir que estão corretos
        print(f"Colunas do arquivo {arquivo}: {df.columns.tolist()}")  # Exibe as colunas do arquivo

        # Essas verificações abaixo estão na mesma linha que não faz sentido, CNPJ não pode ser substituido por nada e nem razão social, se não tiver deixar em branco (Rafael)

        # # Verificar se a coluna 'cnpj_basico' existe, e se não, criar com base no identificador_socio
        # if 'cnpj_basico' not in df.columns:
        #     print(f"Coluna 'cnpj_basico' não encontrada no arquivo {arquivo}, criando a partir de 'identificador_socio'.")
        #     df['cnpj_basico'] = df['identificador_socio'].str[:8]  # Criar a coluna com os 8 primeiros caracteres de 'identificador_socio'

        # # Verificar se a coluna 'nome_socio_razao_social' existe, e se não, criar com base no identificador_socio
        # if 'nome_socio_razao_social' not in df.columns:
        #     print(f"Coluna 'nome_socio_razao_social' não encontrada no arquivo {arquivo}, criando a partir de 'identificador_socio'.")
        #     df['nome_socio_razao_social'] = df['identificador_socio'].str[8:]  # Criar a coluna com o restante de 'identificador_socio'

        # Substituição dos valores na tabela
        df['identificador_socio'] = df['identificador_socio'].map(tipo_socio)
        df['qualificacao_socio'] = df['qualificacao_socio'].map(quals)
        df['pais'] = df['pais'].map(paises)

        # Remover espaços extras das colunas
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

        # Reordenar as colunas conforme o formato desejado
        df_reordenado = df[['cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio', 
                            'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal', 
                            'nome_do_representante', 'qualificacao_representante_legal', 'faixa_etaria']]

        # Adicionar o DataFrame à lista
        lista_dfs.append(df_reordenado)

    # Concatenar todos os DataFrames em um único
    df_completo = pd.concat(lista_dfs, ignore_index=True)

    # Salvar o DataFrame completo em um novo arquivo CSV
    df_completo.to_csv("socios_completo.csv", index=False, sep=";", 
                    encoding="latin1", quoting=1)  
    lista_dfs.clear()
    print('LIBERAÇÃO DE MEMÓRIA FEITA')
    print('SEGUINDO PARA O PROCESSO DAS EMPRESAS')
    


 # ------------- EMPRESAS

    pastas_das_empresas =  caminho_empresas()
    listando_os_arquivos = glob.glob(os.path.join(pastas_das_empresas, "*.csv"))

    # Define uma Series para substituicao
    natureza = csvToSeriesIndex(caminho_natureza)
    porte = {'00':'Não Informado', '01':'Micro Empresa', '03':'Empresa de Pequeno Porte', '05':'Demais'}

    # Definindo o tamanho dos chunks
    chunk_size = 7000
    saida_do_novo_arquivo = "empresas_geral.parquet"

    # Criando a estrutura de controle com a barra de progresso
    with tqdm(total=len(listando_os_arquivos), desc="Processando os arquivos", unit=" arquivos") as pbar:
        
        lista_tabelas = []  # Lista para armazenar os chunks antes de salvar

        for arquivo in listando_os_arquivos:
            print(f"Lendo arquivo {arquivo}")
            
            for chunk in pd.read_csv(arquivo, 
                            sep=";", 
                            encoding="latin1",  
                            dtype=str,  # Força todas as colunas a serem string
                            quotechar='"', 
                            header=None, 
                            chunksize=chunk_size):
                
                chunk.columns = [  
                    'cnpj_basico',
                    'razao_social',
                    'natureza_juridica',
                    'qualificacao_responsavel',
                    'capital_social',
                    'porte_empresa',
                    'ente_federativo_responsavel'
                ]

                if "cnpj_basico" not in chunk.columns:
                    chunk["cnpj_basico"] = chunk["razao_social"].str[:16]

                # Substituindo valores encontrados em outras tabelas
                chunk['natureza_juridica'] = chunk['natureza_juridica'].map(natureza)
                chunk['qualificacao_responsavel'] = chunk['qualificacao_responsavel'].map(quals)
                chunk['porte_empresa'] = chunk['porte_empresa'].map(porte)

                chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                
                chunk_organizado = chunk[[  
                    'cnpj_basico',
                    'razao_social',
                    'natureza_juridica',
                    'qualificacao_responsavel',
                    'capital_social',
                    'porte_empresa',
                    'ente_federativo_responsavel'
                ]]

                # Definindo o schema fixo para evitar erro de tipos diferentes
                schema = pa.schema([
                    ("cnpj_basico", pa.string()),
                    ("razao_social", pa.string()),
                    ("natureza_juridica", pa.string()),
                    ("qualificacao_responsavel", pa.string()),
                    ("capital_social", pa.string()),
                    ("porte_empresa", pa.string()),
                    ("ente_federativo_responsavel", pa.string())  # Aqui forçamos string
                ])

                # Convertendo para tabela PyArrow
                tabela = pa.Table.from_pandas(chunk_organizado, schema=schema)

                # Adicionando a tabela na lista
                lista_tabelas.append(tabela)

            pbar.update(1)

        # Concatenando todas as tabelas em uma única e salvando
        if lista_tabelas:
            tabela_final = pa.concat_tables(lista_tabelas)  
            pq.write_table(tabela_final, saida_do_novo_arquivo, compression="SNAPPY")
            
        lista_tabelas.clear()
        print('LIBERAÇÃO DE MEMÓRIA FEITA')
        print('SEGUINDO PARA O PROCESSO DOS ESTABELECIMENTO')
            
    print("✅ Todos os arquivos CSV foram combinados e salvos em um único Parquet com sucesso!")

   


# ------------- ESTABELECIMENTO

    estabelecimento = caminho_estabelecimento()

    arquivos_csv = glob.glob(os.path.join(estabelecimento, "*.csv"))

    # Lista para armazenar os DataFrames de cada arquivo
    lista_dfs = []
    situacao = {'01':'NULA', '02':'ATIVA', '03':'SUSPENSA','04':'INAPTA','08':'BAIXADA'}
    matriz = {'1':'MATRIZ', '2':'FILIAL'}
    motivo = csvToSeriesIndex(caminho_motivo)
    municipio = csvToSeriesIndex(caminho_municipios)

    chunk_size = 1000  
    saida_parquet = "estabelecimentos_completo.parquet"

    # Preparar o ParquetWriter para salvar os dados progressivamente
    with tqdm(total=len(arquivos_csv), desc="Processando arquivos", unit="arquivo") as pbar:
        for arquivo in arquivos_csv:
            print(f"Lendo arquivo {arquivo}")
            
            # Processar o arquivo em chunks
            for chunk in pd.read_csv(arquivo, 
                                    sep=";", 
                                    encoding="latin1",  
                                    dtype=str, 
                                    quotechar='"', 
                                    header=None, 
                                    chunksize=chunk_size):
                
                # Atribuir os nomes das colunas manualmente
                chunk.columns = ['cnpj_basico',
                                'cnpj_ordem',
                                'cnpj_dv',
                                'identificador_matriz_filial',
                                'nome_fantasia',
                                'situacao_cadastral',
                                'data_situacao_cadastral',
                                'motivo_situacao_cadastral',
                                'nome_cidade_exterior',
                                'pais',
                                'data_inicio_atividade',
                                'cnae_fiscal_principal',
                                'cnae_fiscal_secundaria',
                                'tipo_logradouro',
                                'logradouro',
                                'numero',
                                'complemento',
                                'bairro',
                                'cep',
                                'uf',
                                'municipio',
                                'ddd_1',
                                'telefone_1',
                                'ddd_2',
                                'telefone_2',
                                'ddd_fax',
                                'fax',
                                'correio_eletronico',
                                'situacao_especial',
                                'data_situacao_especial']
                
                # Verificar se a coluna 'cnpj_basico' existe, e se não, criar com base no 'cnpj_ordem'
                # if 'cnpj_basico' not in chunk.columns:
                #     chunk['cnpj_basico'] = chunk['cnpj_ordem'].str[:8]  

                # Substituindo valores na tabela com valores encontrados em outras tabelas
                chunk['situacao_cadastral'] = chunk['situacao_cadastral'].map(situacao)
                chunk['pais'] = chunk['pais'].map(paises)
                chunk['identificador_matriz_filial'] = chunk['identificador_matriz_filial'].map(matriz)
                chunk['motivo_situacao_cadastral'] = chunk['motivo_situacao_cadastral'].map(motivo)
                chunk['municipio'] = chunk['municipio'].map(municipio)

                # Remover espaços extras das colunas
                chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                
                # Reordenar o DataFrame (se necessário)
                chunk_reordenado = chunk[['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 
                                        'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral', 
                                        'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais', 
                                        'data_inicio_atividade', 'cnae_fiscal_principal', 
                                        'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro', 
                                        'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 
                                        'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax', 'fax', 
                                        'correio_eletronico', 'situacao_especial', 'data_situacao_especial']]

                # Verificar se o chunk tem dados antes de tentar convertê-lo para tabela Arrow
                if not chunk.empty:
                    schema = pa.schema([
                        ("cnpj_basico", pa.string()),
                        ("cnpj_ordem", pa.string()),
                        ("cnpj_dv", pa.string()),
                        ("identificador_matriz_filial", pa.string()),
                        ("nome_fantasia", pa.string()),
                        ("situacao_cadastral", pa.string()),
                        ("data_situacao_cadastral", pa.string()),
                        ("motivo_situacao_cadastral", pa.string()),
                        ("nome_cidade_exterior", pa.string()),
                        ("pais", pa.string()),
                        ("data_inicio_atividade", pa.string()),
                        ("cnae_fiscal_principal", pa.string()),
                        ("cnae_fiscal_secundaria", pa.string()),
                        ("tipo_logradouro", pa.string()),
                        ("logradouro", pa.string()),
                        ("numero", pa.string()),
                        ("complemento", pa.string()),
                        ("bairro", pa.string()),
                        ("cep", pa.string()),
                        ("uf", pa.string()),
                        ("municipio", pa.string()),
                        ("ddd_1", pa.string()),
                        ("telefone_1", pa.string()),
                        ("ddd_2", pa.string()),
                        ("telefone_2", pa.string()),
                        ("ddd_fax", pa.string()),
                        ("fax", pa.string()),
                        ("correio_eletronico", pa.string()),
                        ("situacao_especial", pa.string()),
                        ("data_situacao_especial", pa.string())
                    ])
                    
                    tabela = pa.Table.from_pandas(chunk, schema=schema)
                    lista_dfs.append(tabela)
                else:
                    print(f"Arquivo {arquivo} contém chunk vazio e não foi adicionado.")

            pbar.update(1)

            # Verificar se a lista de tabelas não está vazia antes de tentar concatenar
            if lista_dfs:
                tabela_final = pa.concat_tables(lista_dfs)
                pq.write_table(tabela_final, saida_parquet, compression="SNAPPY")
                print("✅ Todos os arquivos CSV foram combinados e salvos em um único Parquet estabelecimento!")
            else:
                print("Erro: Nenhuma tabela válida foi adicionada à lista.")
