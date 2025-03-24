import duckdb

# Conectando ao banco de dados (em memória por padrão)
conn = duckdb.connect()



def consultas():
    # Carregando os arquivos Parquet
    conn.execute("""
        CREATE TABLE empresas_geral AS 
        SELECT * FROM read_parquet('D:/projeto_novo/empresas_geral.parquet')
    """)
    conn.execute("""
        CREATE TABLE estabelecimentos_completo AS 
        SELECT * FROM read_parquet('D:/projeto_novo/estabelecimentos_completo.parquet')
    """)

    # Realizando o JOIN entre as duas tabelas usando cnpj_basico como chave
    result = conn.execute("""
        SELECT * 
        FROM empresas_geral e
        JOIN estabelecimentos_completo s
        ON e.cnpj_basico = s.cnpj_basico
        LIMIT 2
    """).fetchall()

    conn.execute("""
        CREATE TABLE socios AS 
        SELECT * FROM read_csv_auto('D:/projeto_novo/socios_completo.csv', encoding='latin-1')
    """)
    conn.execute("""
        CREATE TABLE resultado_merge AS 
        SELECT * FROM read_parquet('D:/projeto_novo/resultado_merge.parquet')
    """)

    # Realizando o JOIN entre as duas tabelas usando cnpj_basico como chave
    result = conn.execute("""
        SELECT * 
        FROM socios e
        JOIN resultado_merge s
        ON e.cnpj_basico = s.cnpj_basico
        LIMIT 2
    """).fetchall()

    conn.execute("""
        CREATE TABLE merge_2 AS 
        SELECT * FROM read_parquet('D:/projeto_novo/resultado_merge_2.parquet')
    """)
    conn.execute("""
        CREATE TABLE municipio AS 
        SELECT * FROM read_csv_auto('D:/projeto_novo/App/data/arquivos_extraidos/MUNIC/F.K03200$Z.D50208.MUNICCSV.csv', header=false)
    """)

    conn.execute("""
        ALTER TABLE municipio RENAME COLUMN column0 TO codigo;
        ALTER TABLE municipio RENAME COLUMN column1 TO descrição;
    """)

    # Realizando o JOIN entre as duas tabelas usando municipio como chave
    result = conn.execute("""
        SELECT * 
        FROM merge_2 e
        JOIN municipio s
        ON e.municipio = s.codigo
        LIMIT 2
    """).fetchall()

    conn.execute("""
        CREATE TABLE merge_3 AS 
        SELECT * FROM read_parquet('D:/projeto_novo/resultado_merge_3.parquet')
    """)
    conn.execute("""
        CREATE TABLE cnae AS 
        SELECT * FROM read_csv_auto('D:/projeto_novo/App/data/arquivos_extraidos/CNAE/F.K03200$Z.D50208.CNAECSV.csv', header=false, encoding='latin-1')
    """)

    conn.execute("""
        ALTER TABLE cnae RENAME COLUMN column0 TO codigo;
        ALTER TABLE cnae RENAME COLUMN column1 TO descrição;
    """)

    # Realizando o JOIN entre as duas tabelas usando cnae_fiscal_principal como chave
    result = conn.execute("""
        SELECT * 
        FROM merge_3 e
        JOIN cnae s
        ON e.cnae_fiscal_principal = s.codigo
        LIMIT 2
    """).fetchall()

    conn.execute("""
        COPY (
            SELECT * 
            FROM merge_3 e
            JOIN cnae s
            ON e.cnae_fiscal_principal = s.codigo
        ) TO 'D:/projeto_novo/PROJETO_FINAL.parquet' (FORMAT PARQUET)
    """)
    
    
    
consultas() 