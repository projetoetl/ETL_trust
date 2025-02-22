
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'C:\\Users\\macdounout\\Desktop\\ETL_COPY')))
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "script")))

from App.script._conexa_extracao import conexao_download_e_extracao_add_csv
from App.script._extensao_csv import adicionando_ext_csv_
from App.script.d_cabecalho_csv import add_cabecalho


 # Iniciando o download a extração e organização dos arquivos
conexao_download_e_extracao_add_csv()
    
 
# adicionando cabeçalho nos arquivos 
add_cabecalho()




#  python -m App._init__