from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# Importamos o orquestrador que agora usa a estrutura 'common'
from include.scripts.orquestrador_geral import iniciar_pipeline_medalhao

default_args = {
    'owner': 'engenharia_dados',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 6),
    'retries': 1
}

with DAG(
    dag_id='pipeline_ecommerce_medalhao',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False, # <-- O ERRO ESTAVA AQUI (era catch_date)
    tags=['databricks', 'medalhao', 'bronze']
) as dag:

    task_processar_clientes = PythonOperator(
        task_id='processar_clientes_bronze',
        python_callable=iniciar_pipeline_medalhao
    )