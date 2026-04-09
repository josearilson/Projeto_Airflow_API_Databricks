from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta
from include.scripts.camadas.bronze import criar_tabela_bronze
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

# --- CONFIGURAÇÃO ---
CONF_CATALOG = "workspace"
CONF_SCHEMA = "meu_projeto_ecommerce" 
CONN_ID = "databricks_default"

def setup_databricks_env(**kwargs):
    hook = DatabricksSqlHook(databricks_conn_id=CONN_ID)
    schema_path = f"{CONF_CATALOG}.{CONF_SCHEMA}"
    
    hook.run([
        f"CREATE SCHEMA IF NOT EXISTS {schema_path}",
        f"CREATE VOLUME IF NOT EXISTS {schema_path}.raw"
    ])

default_args = {
    'owner': 'engenharia_dados',
    'start_date': datetime(2026, 4, 6),
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}
with DAG(
    dag_id='pipeline_ecommerce_medalhao',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_tasks=10, # Permite até 10 tarefas rodando ao mesmo tempo
    tags=['databricks', 'medalhao', 'bronze']
) as dag:

    task_setup = PythonOperator(
        task_id='setup_infraestrutura',
        python_callable=setup_databricks_env
    )
 
    shared_args = {'catalog': CONF_CATALOG, 'schema': CONF_SCHEMA}

    bronze_clientes = PythonOperator(
        task_id='bronze_clientes',
        python_callable=criar_tabela_bronze,
        op_kwargs={'entidade': 'clientes', **shared_args}
    )

    bronze_produtos = PythonOperator(
        task_id='bronze_produtos',
        python_callable=criar_tabela_bronze,
        op_kwargs={'entidade': 'produtos', **shared_args}
    )

    bronze_vendas = PythonOperator(
        task_id='bronze_vendas',
        python_callable=criar_tabela_bronze,
        op_kwargs={'entidade': 'vendas', **shared_args}
    )
 
    task_setup >> [bronze_clientes, bronze_produtos, bronze_vendas]