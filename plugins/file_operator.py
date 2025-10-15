from airflow.models import Variable
from office365.graph_client import GraphClient
from office365.identity.client_credential import ClientCredential
import os
import zipfile
import geopandas as gpd
import pandas as pd

class FileOperator:
    def __init__(self, download_path="/tmp/arquivos_shp"):
        # 🔑 Variáveis do Airflow
        self.tenant_id = Variable.get("TentantId_MicrosoftGraph_Automacao")
        self.client_id = Variable.get("ClientId_MicrosoftGraph_Automacao")
        self.client_secret = Variable.get("ClientSecret_MicrosoftGraph_Automacao")
        self.email_address = Variable.get("Email_MicrosoftGraph_Automacao")
        self.download_path = download_path
        
        # 📦 Cria pasta destino (caso não exista)
        os.makedirs(self.download_path, exist_ok=True)
    
    def connect_to_graph(self):
        """🔗 Estabelece conexão com Microsoft Graph"""
        credentials = ClientCredential(self.client_id, self.client_secret)
        return GraphClient.with_client_credentials(self.tenant_id, credentials)
    
    def download_and_extract_zip_attachments(self):
        """📧 Processa e-mails não lidos e extrai anexos ZIP"""
        client = self.connect_to_graph()
        
        # 📧 Busca e-mails não lidos
        messages = client.users[self.email_address].mail_folders["Inbox"].messages \
            .filter("isRead eq false") \
            .select(["id", "subject"]) \
            .get().execute_query()

        for message in messages:
            msg = client.users[self.email_address].messages[message.id]
            attachments = msg.attachments.get().execute_query()

            for attachment in attachments:
                # 📎 Processa apenas ZIPs
                if hasattr(attachment, "content_bytes") and attachment.name.lower().endswith(".zip"):
                    zip_path = os.path.join(self.download_path, attachment.name)
                    with open(zip_path, "wb") as f:
                        f.write(attachment.content_bytes)
                    print(f"📥 ZIP baixado: {zip_path}")

                    # 🗜️ Extrai ZIP
                    with zipfile.ZipFile(zip_path, "r") as zip_ref:
                        zip_ref.extractall(self.download_path)
                    print(f"🗂️ Conteúdo extraído em: {self.download_path}")

                    # 🔍 Filtra apenas arquivos .shp
                    shp_files = [
                        os.path.join(self.download_path, f)
                        for f in os.listdir(self.download_path)
                        if f.lower().endswith(".shp")
                    ]

                    if shp_files:
                        print("✅ Arquivos SHP encontrados:")
                        for shp in shp_files:
                            print(f"   - {shp}")
                    else:
                        print("⚠️ Nenhum arquivo .shp encontrado no ZIP.")

            # 🔖 Marca o e-mail como lido
            msg.update(isRead=True).execute_query()

        print("✅ Processo concluído.")
    
    @staticmethod
    def reader_file_shp(shp_file_path):
        """📊 Lê arquivo shapefile e retorna DataFrame com informações geográficas"""
        try:
            # Lê o arquivo shapefile usando geopandas
            gdf = gpd.read_file(shp_file_path)
            
            # Converte para DataFrame pandas
            df = pd.DataFrame(gdf)
            
            # Adiciona informações sobre o arquivo
            df['nomearquivo'] = os.path.basename(shp_file_path)
            df['caminho_arquivo'] = shp_file_path
            
            # Converte geometria para WKT se existir
            if 'geometry' in df.columns:
                df['geometry_wkt'] = df['geometry'].apply(lambda x: x.wkt if x is not None else None)
            
            print(f"📊 Arquivo shapefile lido: {shp_file_path} - {len(df)} registros")
            return df
            
        except Exception as e:
            print(f"❌ Erro ao ler arquivo shapefile {shp_file_path}: {str(e)}")
            # Retorna DataFrame vazio em caso de erro
            return pd.DataFrame({'nomearquivo': [os.path.basename(shp_file_path)], 
                               'caminho_arquivo': [shp_file_path], 
                               'erro': [str(e)]})
    
    def execute(self):
        """🚀 Método principal para executar o processamento"""
        return self.download_and_extract_zip_attachments()
