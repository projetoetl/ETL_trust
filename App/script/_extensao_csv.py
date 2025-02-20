import os


def adicionando_ext_csv_():
    # Lista com os caminhos das pastas que você deseja processar
    pastas_socios = [
        "App\\data\\arquivos_extraidos\\CNAE",
        "App\\data\\arquivos_extraidos\\QUALS",
        "App\\data\\arquivos_extraidos\\PAIS",
        "App\\data\\arquivos_extraidos\\NATJU",
        "App\\data\\arquivos_extraidos\\MUNIC",
        "App\\data\\arquivos_extraidos\\MOTIC",
        "App\\data\\arquivos_extraidos\\SIMPLES",
        "App\\data\\arquivos_extraidos\\ESTABELE",
        "App\\data\\arquivos_extraidos\\EMPRE",
        "App\\data\\arquivos_extraidos\\SOCIO"
        
    ]

    # Iterando por cada pasta
    for pasta in pastas_socios:
        print(f"Processando a pasta: {pasta}")
        
        # Iterando por todos os arquivos da pasta atual
        for arquivo in os.listdir(pasta):
            caminho_arquivo = os.path.join(pasta, arquivo)
            
            # Verificando se o arquivo não tem a extensão .csv
            if not arquivo.endswith(".csv"):
                novo_nome = arquivo + ".csv"  # Adicionando a extensão .csv
                novo_caminho = os.path.join(pasta, novo_nome)
                
                # Renomeando o arquivo
                os.rename(caminho_arquivo, novo_caminho)
                print(f"Arquivo {arquivo} renomeado para {novo_nome}")
    print("Todos os arquivos Receberam .CSV ")
