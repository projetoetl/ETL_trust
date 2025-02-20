import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
import gc
from tqdm import tqdm  # Barra de progresso
import re


def limpa_cnpj(cnpj):
    return re.sub(r'\D', '', str(cnpj))


def DATA_FRAME():
    colunas_estabelecimento = [
        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
        'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
        'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais',
        'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
        'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro',
        'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2',
        'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial'
    ]

    colunas_empresa = [
        'cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel',
        'capital_social', 'porte_empresa', 'ente_federativo_responsavel'
    ]

    arquivo_saida = "arquivos_extraidos/novo_arquivo_test90.parquet"

    # Leitura completa do arquivo de empresas para reduzir múltiplas leituras
    df_empresas = pq.read_table("arquivos_extraidos/EMPRE/empresas_geral.parquet").to_pandas()
    df_empresas['cnpj_basico'] = df_empresas['cnpj_basico'].apply(limpa_cnpj)
    
    # Filtrando as colunas de empresas
    df_empresas = df_empresas[colunas_empresa]
    
    reader_estabelecimentos = pq.ParquetFile("arquivos_extraidos/ESTABELE/estabelecimentos_completo.parquet")
    num_row_groups_estabelecimentos = reader_estabelecimentos.num_row_groups
    
    print(f"Total de row_groups no arquivo 'estabelecimentos_completo.parquet': {num_row_groups_estabelecimentos}")
    
    primeiro_merge_mostrado = False
    
    # Verifique se o arquivo de saída já existe
    if os.path.exists(arquivo_saida):
        # Abre o arquivo existente em modo leitura para recuperar o schema
        tabela_existente = pq.read_table(arquivo_saida)
        schema_existente = tabela_existente.schema
    else:
        schema_existente = None  # Nenhum schema se o arquivo não existir

    with tqdm(total=num_row_groups_estabelecimentos, desc="Processando estabelecimentos", unit="row_group") as pbar:
        # Usar o ParquetWriter apenas uma vez no início
        writer = None
        
        for i in range(num_row_groups_estabelecimentos):
            df_estabelecimentos_chunk = reader_estabelecimentos.read_row_group(i).to_pandas()
            df_estabelecimentos_chunk['cnpj_basico'] = df_estabelecimentos_chunk['cnpj_basico'].apply(limpa_cnpj)
            
            # Filtrando as colunas de estabelecimentos
            df_estabelecimentos_chunk = df_estabelecimentos_chunk[colunas_estabelecimento]
            
            # Merge com as empresas
            df_resultado = pd.merge(df_estabelecimentos_chunk, df_empresas, on='cnpj_basico', how='inner')
            
            if not df_resultado.empty:
                # Exibir o primeiro merge bem-sucedido antes de continuar
                if not primeiro_merge_mostrado:
                    print("\nPrimeiro merge bem-sucedido:\n")
                    print(df_resultado.head())  # Exibe as primeiras linhas do merge
                    input("\nPressione ENTER para continuar o processamento...\n")  # Aguarda confirmação do usuário
                    primeiro_merge_mostrado = True
                
               
                table = pa.Table.from_pandas(df_resultado)
                
               
                if writer is None:
                    writer = pq.ParquetWriter(arquivo_saida, table.schema, compression='SNAPPY')
                
                
                writer.write_table(table)
                
                # Liberando a memória após cada iteração
                del df_estabelecimentos_chunk
                del df_resultado
                gc.collect()  
                
            pbar.update(1)

        
        if writer is not None:
            writer.close()

    print("Processamento concluído! Arquivo Parquet atualizado a cada merge.")

DATA_FRAME()
