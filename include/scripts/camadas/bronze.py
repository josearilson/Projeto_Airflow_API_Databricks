import os
import requests
import logging
from include.scripts.common.config import Config 
from include.scripts.common.funcoes_delta import registrar_tabela_bronze
 
log = logging.getLogger("airflow.task")

def criar_tabela_bronze(entidade, catalog, schema):
       
    # --- 1. DEFINIÇÃO DE CAMINHOS ---
    
    # Define onde o arquivo está fisicamente no servidor do Airflow
    arquivo_local = os.path.join(Config.DIR_LANDING, f"{entidade}.csv")
    
    # Define o caminho de destino no "Volume" do Databricks (pasta RAW)
    caminhoRemoto_raw = f"/Volumes/{catalog}/{schema}/raw/{entidade}.csv"
    
    
    # --- 2. UPLOAD BINÁRIO (ALTA PERFORMANCE) ---
    
    # Prepara o "crachá" de acesso (Token) para que o Databricks aceite o arquivo
    headers = {"Authorization": f"Bearer {Config.TOKEN}"}
    
    # Monta o endereço da API do Databricks que recebe arquivos
    # O comando 'overwrite=true' garante que se o arquivo já existir, ele será atualizado
    url_upload = f"{Config.HOST}/api/2.0/fs/files{caminhoRemoto_raw}?overwrite=true"
    
    # 'with open' garante que o arquivo será fechado automaticamente após o envio
    # 'rb' (Read Binary) lê o arquivo bit por bit, garantindo que nenhum acento seja corrompido
    with open(arquivo_local, 'rb') as f:
        
        # O parâmetro 'data=f' é o segredo da velocidade: ele cria um "fluxo" direto do disco 
        # para o Databricks sem carregar o arquivo inteiro na memória RAM do Airflow
        resposta = requests.put(url_upload, headers=headers, data=f)
    
    # Verifica se o Databricks confirmou o recebimento (códigos 200, 201 ou 204)
    if resposta.status_code not in [200, 201, 204]:
        raise Exception(f"Erro no envio do arquivo: {resposta.text}")

    
    # --- 3. CONVERSÃO AUTOMÁTICA NO DATABRICKS ---
    
    log.info(f"Acionando o motor Photon do Databricks para converter {entidade}...")
    
    # Aqui o Airflow apenas envia um comando SQL 'COPY INTO'.
    # O trabalho pesado de transformar CSV em Parquet/Delta acontece dentro do Databricks.
    registrar_tabela_bronze(
        Config.CONN_ID,      # ID da conexão configurada no Airflow
        caminhoRemoto_raw,   # Local onde o CSV "pousou" no volume
        schema,              # Nome da base de dados (Schema)
        f"{entidade}_bronze",# Nome da tabela que será criada
        catalog              # Catálogo principal do Unity Catalog
    )
    
    # Mensagem final que indica que o processo terminou com sucesso
    return f"{entidade} processado com performance máxima!"