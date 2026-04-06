from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime 
# O caminho deve começar por 'include'
from include.scripts.orquestrador_bronze import executa_fluxo_completo_bronze

with DAG(
    dag_id='ecommerce_bronze_databricks',
    start_date=datetime(2026, 4, 6),
    schedule_interval=None,
    catchup=False
) as dag:

    task_databricks = PythonOperator(
        task_id='processar_bronze_databricks',
        python_callable=executa_fluxo_completo_bronze
    )