from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# Importamos apenas o orquestrador geral que gere as camadas
from include.scripts.orquestrador_geral import iniciar_pipeline_medalhao

# Configurações padrão da DAG
default_args = {
    'owner': 'engenharia_dados',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='pipeline_ecommerce_medalhao',
    default_args=default_args,
    schedule_interval='@daily',  # Executa todos os dias
    catch_date=False,
    tags=['databricks', 'medalhao', 'bronze']
) as dag:

    # Esta única Task executa todo o fluxo (Ingestão -> Carga -> Registo)
    # porque o Orquestrador já conhece os passos graças ao 'common'
    task_processar_clientes = PythonOperator(
        task_id='processar_clientes_bronze',
        python_callable=iniciar_pipeline_medalhao,
        op_kwargs={'entidade': 'clientes'} # Passamos o nome da tabela aqui
    )