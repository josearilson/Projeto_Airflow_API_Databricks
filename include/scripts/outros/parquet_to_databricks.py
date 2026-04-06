import os
import requests
import logging
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

# Configurações Globais
CONEXAO_ID = "databricks_default"
log = logging.getLogger("airflow.task")

# --- 1. FUNÇÕES DE SUPORTE (AUXILIARES) ---

def enviar_arquivo_ao_volume(host, token, local, remoto):
    """Faz o upload físico do arquivo para o Unity Catalog Volumes."""
    headers = {"Authorization": f"Bearer {token}"}
    
    # Garante que a pasta exista
    diretorio = os.path.dirname(remoto)
    requests.post(f"{host}/api/2.0/fs/directories{diretorio}", headers=headers)

    # Faz o Upload
    url_upload = f"{host}/api/2.0/fs/files{remoto}?overwrite=true"
    with open(local, 'rb') as f:
        res = requests.put(url_upload, headers=headers, data=f)
    
    if res.status_code not in [200, 201, 204]:
        raise Exception(f"Falha no Upload: {res.text}")
    log.info(f"✅ Arquivo enviado para: {remoto}")


def registrar_tabela_sql(caminho_parquet, schema, tabela):
    """Cria a tabela dinamicamente usando o schema e nome fornecidos."""
    hook = DatabricksSqlHook(databricks_conn_id=CONEXAO_ID)
    
    sql = f"""
    CREATE OR REPLACE TABLE workspace.{schema}.{tabela}
    USING DELTA
    AS SELECT * FROM parquet.`{caminho_parquet}`;
    """
    
    hook.run(sql)
    log.info(f"✅ Tabela Delta 'workspace.{schema}.{tabela}' criada com sucesso!")

# --- 2. FUNÇÃO PRINCIPAL (ORQUESTRADORA) ---

def executa_fluxo_databricks():
    """Função que a DAG chama para coordenar o processo."""
    
    # Variáveis de Ambiente
    token = os.getenv('DATABRICKS_TOKEN')
    host  = os.getenv('DATABRICKS_HOST')
    Schema = "data_lake"
    Volume = "01_bronze"
    Arquivo = "clientes.parquet"
    Nome_Tabela = "clientes_bronze" 

    # Definição de Caminhos
    path_local  = f"/usr/local/airflow/include/bronze/{Arquivo}"
    path_remoto = f"/Volumes/workspace/{Schema}/{Volume}/{Arquivo}"
   
    # Execução Passo a Passo
    log.info("Iniciando processamento Databricks...")
    
    # Passo 1: Upload
    enviar_arquivo_ao_volume(host, token, path_local, path_remoto)
    
    # Passo 2: Registro SQL
    registrar_tabela_sql(path_remoto, Schema, Nome_Tabela)

    log.info("🚀 Processo concluído com sucesso!")