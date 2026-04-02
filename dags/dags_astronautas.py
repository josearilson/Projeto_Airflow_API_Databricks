from airflow.decorators import dag, task
from pendulum import datetime
import requests
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

# Nome da conexão configurada na interface do Airflow
CONEXAO_ID = "databricks_default"

@dag(
    dag_id="astronautas_para_databricks_final",
    start_date=datetime(2026, 4, 2),
    schedule="@daily",
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': 30, # segundos
    },
    tags=["databricks", "sucesso"],
)
def dag_astronautas_DadosSQL_Databricks():

    @task
    def buscar_astronautas_na_api():
        """
        PASSO 1: Busca os dados brutos da internet.
        """
        url = "http://api.open-notify.org/astros.json"
        resposta = requests.get(url)
        # Isso vai mostrar o JSON bruto no log do Airflow

        print(f'\n\n""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""')
              
        print(f"Conteúdo bruto da resposta: {resposta.text}")
        
        print(f'\n\n""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""') 
        resposta.raise_for_status()
        
        # IMPORTANTE: A API usa a palavra "people" dentro do json.
        return resposta.json()["people"]

    @task
    def carregar_no_databricks(lista_astronautas: list):
        """
        PASSO 2: Usa a CONEXAO_BANCO para enviar os dados.
        """
        # Criamos a conexão com o Databricks
        conexao_banco = DatabricksSqlHook(databricks_conn_id=CONEXAO_ID)
        
        # 1. Limpa a tabela
        conexao_banco.run("TRUNCATE TABLE workspace.ecommerce.current_astronauts")
        
        # 2. Formata os valores para o SQL
        valores_para_sql = ", ".join([str((p['name'], p['craft'])) for p in lista_astronautas])
        
        # 3. Montamos o comando para inserir os dados no Databricks
        inserirNoDatabricks = f"INSERT INTO workspace.ecommerce.current_astronauts VALUES {valores_para_sql}"
        
        # 4. Envia para o Databricks usando o nosso novo nome de variável "inserirNoDatabricks"
        conexao_banco.run(inserirNoDatabricks)
        
        print(f'\n\n""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""') 

        print(f"Sucesso! {len(lista_astronautas)} astronautas cadastrados no banco.")

        print(f'\n\n""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""') 

        
    # Fluxo de execução
    dados_api = buscar_astronautas_na_api()
    carregar_no_databricks(dados_api)

# Ativa a DAG
dag_astronautas_DadosSQL_Databricks()