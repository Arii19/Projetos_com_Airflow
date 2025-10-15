
from airflow.decorators import dag, task
import pandas as pd
from io import StringIO
from airflow.models import Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import sys
import os
import glob
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from file_operator import FileOperator as fo
from shapely import wkt
from airflow.utils.trigger_rule import TriggerRule
            

@dag(
    dag_id = 'dag_shp_santaadelia',
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 9, 9, tz='America/Sao_Paulo'),
    catchup=False,
    tags=["ExportacaoShapes", "SQLServer", "API", "MicrosoftGraph", "Extract_Shapes_SantaAdelia"],
)
def Importar_ShapeFiles():

    @task
    def ler_parametro(**context):
        # Função para ler os parâmetros de ID e email passados na execução da DAG
        try:
            ctx = context['dag_run'].conf
            id = ctx.get('id')
            email = ctx.get('email')
            return {"id": id, "email": email}
        except Exception as e:
            print('def=ler_parametro log_level=ERRO message="'+str(e)+'"')
            raise
 
    @task
    def extract(parametro):
        # Extração: É utilizado para extrair arquivos shapefiles de um email específico
        try:
            # Inicializa o FileOperator para processar emails e extrair anexos ZIP
            file_operator = fo(download_path="/tmp/arquivos_shp")
            
            # Executa o download e extração dos anexos ZIP dos emails não lidos
            file_operator.download_and_extract_zip_attachments()
            
            # Processa os arquivos SHP extraídos
            shp = []
        
            # Busca por todos os arquivos .shp no diretório de download
            shp_files = glob.glob(os.path.join("/tmp/arquivos_shp", "*.shp"))
            
            for shp_file in shp_files:
                # Lê cada arquivo shapefile usando o método estático
                shp_data = fo.reader_file_shp(shp_file)
                shp.append(shp_data)
            
            if shp:
                shp = pd.concat(shp, ignore_index=True)
                res = shp.groupby('nomearquivo').size().reset_index(name='Quantidade')            
                shp = shp.to_csv(index=False)  
                print(f'dag=dag_shp_santaadelia def=extract log_level=INFO message="Extração realizada com sucesso. Arquivos extraídos: {res.to_dict(orient="records")}"')
                return shp
            else:
                print('dag=dag_shp_santaadelia def=extract log_level=WARNING message="Nenhum arquivo shapefile encontrado"')
                return pd.DataFrame().to_csv(index=False)
            return shp
        except Exception as e:
            print(f'dag=dag_shp_santaadelia def=extract log_level=ERRO message="{str(e)}"')
            raise

    parametro = ler_parametro()  
    shp = extract(parametro)

Importar_ShapeFiles()