import os
import requests
from bs4 import BeautifulSoup
import urllib.request
import zipfile
import sys
from  datetime import datetime
import shutil

def conexao_download_e_extracao():
    # Função para criar diretórios
    def makedirs(path):
        if not os.path.exists(path):
            os.makedirs(path)

    # Função para verificar se o arquivo já foi baixado
    def check_diff(url, file_name):
        if not os.path.isfile(file_name):
            return True  

        response = requests.head(url)
        new_size = int(response.headers.get('content-length', 0))
        old_size = os.path.getsize(file_name)
        if new_size != old_size:
            os.remove(file_name)
            return True  

        return False  # Arquivos são iguais

    # Função para fazer o download dos arquivos com progresso
    def download_file(url, file_name):
        print(f"Baixando: {file_name}")
        with open(file_name, 'wb') as f:
            
            # Fazer o download do arquivo em blocos de 1 KB
            with requests.get(url, stream=True) as r:
                total_size = int(r.headers.get('content-length', 0))
                downloaded = 0
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        
                        # Calcular a porcentagem de progresso
                        progress = (downloaded / total_size) * 100
                        sys.stdout.write(f"\rProgresso do download: {progress:.2f}%")
                        sys.stdout.flush()
        print("\nDownload concluído!")

    # Função para extrair arquivos zip com progresso
    def extract_zip(file_name, extract_path):
        print(f"Extraindo: {file_name}")
        
        # Abrir o arquivo ZIP
        with zipfile.ZipFile(file_name, 'r') as zip_ref:
            # Listar todos os arquivos dentro do ZIP
            zip_files = zip_ref.namelist()
            total_files = len(zip_files)
            
            for i, file in enumerate(zip_files):
                
                # Extrair o arquivo
                zip_ref.extract(file, extract_path)
                
                # Calcular o progresso da extração
                progress = (i + 1) * 100 / total_files
                sys.stdout.write(f"\rProgresso da extração: {progress:.2f}% - {file}")
                sys.stdout.flush()
        print("\nExtração concluída!")



    # Obtém o ano e mês atual automaticamente
    ano_atual = str(datetime.now().year)
    mes_atual = f"-{datetime.now().month:02d}"

    url_base = f'https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/{ano_atual}{mes_atual}/'


    # Caminhos de saída
    output_files = 'App/data/arquivos_download'
    extracted_files = 'App/data/arquivos_extraidos' 

    # Criação dos diretórios de saída
    makedirs(output_files)
    makedirs(extracted_files)

    # Acessando a página com os links dos arquivos
    response = requests.get(url_base)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Encontrar todos os links para arquivos .zip
    links = []
    for a_tag in soup.find_all('a', href=True):
        if a_tag['href'].endswith('.zip'):
            links.append(a_tag['href'])

    print(f"Arquivos encontrados: {len(links)}")



    for link in links:
        file_name = os.path.join(output_files, link.split('/')[-1])  
        file_url = url_base + link  

        # Verificar se o arquivo já foi baixado e se há diferenças
        if check_diff(file_url, file_name):
            download_file(file_url, file_name)
            
            
            #Extrair o arquivo .zip depois de baixar
            extract_zip(file_name, extracted_files) 
            
        else:
            print(f"O arquivo {file_name} já está atualizado.")
            
       # extracted_file_path = os.path.join(extracted_files, file_name.split('.')[0])  # Sem a extensão .zip
       # if not os.path.exists(extracted_file_path):
       #    print(f"Extraindo o arquivo {file_name}...")
       #     extract_zip(file_name, extracted_files)
       # else:
            print(f"O arquivo {file_name} já foi extraído.")

    pasta = "App/data/arquivos_extraidos"

    categorias = ["EMPRE", "SOCIO", "ESTABELE","MUNIC", "MOTIC","PAIS","NATJU","CNAE", "QUALS", "SIMPLES"]

    for categoria in categorias:
        rota_da_categoria = os.path.join(pasta,categoria)
        if not os.path.exists(rota_da_categoria):
            os.makedirs(rota_da_categoria)
            
    for arquivos in os.listdir(pasta):
        rota_arquivos = os.path.join(pasta, arquivos)
        

        if os.path.isfile(rota_arquivos):
            for categoria in categorias:
                if categoria.lower() in arquivos.lower():
                    
                    destino = os.path.join(pasta, categoria, arquivos)
                    shutil.move(rota_arquivos, destino)
                    print(f"Arquivo {arquivos} movido para a pasta {categoria}")
                    
                    break
    print("TODOS OS ARQUIVOS JÁ ESTÃO NAS SUAS PASTAS")
    print("Extração concluída.")
            
