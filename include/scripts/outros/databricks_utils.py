import os
import requests
import logging
import pandas as pd 
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

log = logging.getLogger("airflow.task")

# --- PASSO 1: LANDING PARA BRONZE LOCAL (INGESTÃO) ---
def ingestao_landing_para_bronze_local(caminho_csv_landing, caminho_parquet_bronze):
    """
    Realiza a ingestão técnica: Converte o dado original (Landing/Bruto) 
    para o formato otimizado (Parquet) na estrutura Bronze local.
    """
    if not os.path.exists(caminho_csv_landing):
        raise FileNotFoundError(f"❌ Arquivo na Landing Zone {caminho_csv_landing} não encontrado.")

    # Cria a pasta da camada bronze local se não existir
    os.makedirs(os.path.dirname(caminho_parquet_bronze), exist_ok=True)

    # Conversão técnica sem alteração de conteúdo (Regra da Camada Bronze)
    df = pd.read_csv(caminho_csv_landing)
    df.to_parquet(caminho_parquet_bronze, index=False)
    
    mensagem = f"Ingestão técnica concluída: {caminho_parquet_bronze}"
    log.info(f"✅ {mensagem}")
    return mensagem

 
# --- PASSO 2: CARGA PARA CLOUD STORAGE (VOLUME BRONZE) ---
def carregar_bronze_para_databricks(host, token, caminho_local, caminho_volume_remoto):
    """
    Move o dado da camada Bronze local para o armazenamento em nuvem (Volumes do Databricks).
    """
    headers = {"Authorization": f"Bearer {token}"}
    
    # Garante a estrutura de diretórios no Data Lake
    diretorio_remoto = os.path.dirname(caminho_volume_remoto)
    requests.post(f"{host}/api/2.0/fs/directories{diretorio_remoto}", headers=headers)

    # Upload para o FileSystem da Nuvem
    url_upload = f"{host}/api/2.0/fs/files{caminho_volume_remoto}?overwrite=true"
    with open(caminho_local, 'rb') as f:
        resposta = requests.put(url_upload, headers=headers, data=f)
    
    if resposta.status_code not in [200, 201, 204]:
        raise Exception(f"Falha na carga para o Cloud Storage: {resposta.text}")
    
    mensagem = f"Carga para Cloud Storage Bronze concluída: {caminho_volume_remoto}"
    log.info(f"✅ {mensagem}")
    return mensagem

 
# --- PASSO 3: REGISTRO NO CATALOGO (TABELA DELTA BRONZE) ---
def registrar_tabela_delta_bronze(conexao_id, caminho_volume_parquet, esquema_bronze, nome_tabela):
    """
    Registra o arquivo Parquet como uma tabela Delta oficial no Unity Catalog (Camada Bronze).
    """
    hook = DatabricksSqlHook(databricks_conn_id=conexao_id)
    
    # Padronização: catalogo.esquema.tabela
    tabela_completa = f"workspace.{esquema_bronze}.{nome_tabela}"
    
    sql = f"""
    CREATE OR REPLACE TABLE {tabela_completa} 
    USING DELTA
    AS SELECT * FROM parquet.`{caminho_volume_parquet}`;
    """
    
    hook.run(sql)
    
    mensagem = f"Tabela Bronze '{tabela_completa}' persistida no Delta Lake!"
    log.info(f"✅ {mensagem}")
    return mensagem