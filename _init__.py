
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "script")))

from App.script._conexa_extracao import conexao_download_e_extracao_add_csv
from App.script.d_cabecalho_csv import  proc_sim,proc_socio, proc_empresas, proc_estab
from App.script.consultas_sql import consultas


#, proc_socios, proc_empresas, proc_estabelecimento


 # Iniciando o download a extração e organização dos arquivos
conexao_download_e_extracao_add_csv()
    

# processamento das bases
proc_sim()

proc_socio()

proc_empresas()

proc_estab()

# realizando joins
consultas()



#iniciando o projeto ##
#  python -m App._init__