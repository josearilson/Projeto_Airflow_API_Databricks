import os
import pandas as pd
from include.scripts.common.config import Config 
from include.scripts.common.funcoes_delta import registrar_tabela_parquet
from include.scripts.common.funcoes_upload import enviar_arquivo_volume 

def processar_camada_bronze(entidade): 
    arquivo_csv = os.path.join(Config.DIR_LANDING, f"{entidade}.csv")
    arquivo_parquet = os.path.join(Config.DIR_BRONZE, f"{entidade}.parquet")
    
    # Caminho no Databricks Volumes
    caminho_nuvem = f"/Volumes/workspace/data_lake/01_bronze/{entidade}.parquet"

    # 1. Ingestão Local
    df = pd.read_csv(arquivo_csv)
    df.to_parquet(arquivo_parquet, index=False)

    # 2. Upload para Nuvem (Usando as credenciais da sua Config)
    enviar_arquivo_volume(Config.HOST, Config.TOKEN, arquivo_parquet, caminho_nuvem)

    # 3. Registro no Catálogo
    registrar_tabela_parquet(
        Config.CONN_ID, 
        caminho_nuvem, 
        "data_lake", 
        f"{entidade}_bronze"
    )
    
    return f"Tabela {entidade}_bronze criada."