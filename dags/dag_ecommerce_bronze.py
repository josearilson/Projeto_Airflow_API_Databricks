from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
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
    catchup=False, 
    tags=['databricks', 'medalhao', 'bronze']
) as dag:

    # Task 1: Clientes
    task_processar_clientes = PythonOperator(
        task_id='processar_clientes_bronze',
        python_callable=iniciar_pipeline_medalhao,
        op_kwargs={'entidade': 'clientes'} 
    )
    
    # Task 2: Só inicia se Clientes terminar com sucesso
    task_processar_produtos = PythonOperator(
        task_id='processar_produtos_bronze',
        python_callable=iniciar_pipeline_medalhao,
        op_kwargs={'entidade': 'produtos'} 
    )
    
    # Task 3: Só inicia se Produtos terminar com sucesso
    task_processar_vendas = PythonOperator(
        task_id='processar_vendas_bronze',
        python_callable=iniciar_pipeline_medalhao,
        op_kwargs={'entidade': 'vendas'} 
    )

    # Definição da Dependência (Pipeline Sequencial)
    task_processar_clientes >> task_processar_produtos >> task_processar_vendas