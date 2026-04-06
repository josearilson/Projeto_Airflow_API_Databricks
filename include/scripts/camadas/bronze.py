import os
import pandas as pd
from include.scripts.common.config import Config 
from include.scripts.common.funcoes_delta import registrar_tabela_parquet
from include.scripts.common.funcoes_upload import enviar_arquivo_volume

def processar_camada_bronze(entidade="clientes"):
    # 1. Definição de caminhos baseada na entidade
    arquivo_csv = os.path.join(Config.PATH_LANDING, f"{entidade}.csv")
    arquivo_parquet = os.path.join(Config.PATH_BRONZE, f"{entidade}.parquet")
    caminho_nuvem = f"/Volumes/workspace/data_lake/01_bronze/{entidade}.parquet"

    # 2. Ingestão Local (CSV -> Parquet)
    df = pd.read_csv(arquivo_csv)
    df.to_parquet(arquivo_parquet, index=False)

    # 3. Carga (Cloud) usando o common
    enviar_arquivo_volume(Config.HOST, Config.TOKEN, arquivo_parquet, caminho_nuvem)

    # 4. Registro (SQL) usando o common
    registrar_tabela_parquet(Config.CONN_ID, caminho_nuvem, "data_lake", f"{entidade}_bronze")
    
    return f"Entidade {entidade} processada na Bronze"