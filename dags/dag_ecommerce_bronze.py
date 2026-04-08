from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from include.scripts.camadas.bronze import processar_csv, criar_tabela_bronze

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

    # ======= CLIENTES =======
    with TaskGroup("dados_clientes", tooltip="Pipeline Clientes") as tg_clientes:
        task_csv_clientes = PythonOperator(
            task_id='processar_csv_clientes',
            python_callable=processar_csv,
            op_kwargs={'entidade': 'clientes'}
        )
        task_bronze_clientes = PythonOperator(
            task_id='bronze_clientes',
            python_callable=criar_tabela_bronze,
            op_kwargs={'entidade': 'clientes'}
        )
        task_csv_clientes >> task_bronze_clientes

    # ======= PRODUTOS =======
    with TaskGroup("dados_produtos", tooltip="Pipeline Produtos") as tg_produtos:
        task_csv_produtos = PythonOperator(
            task_id='processar_csv_produtos',
            python_callable=processar_csv,
            op_kwargs={'entidade': 'produtos'}
        )
        task_bronze_produtos = PythonOperator(
            task_id='bronze_produtos',
            python_callable=criar_tabela_bronze,
            op_kwargs={'entidade': 'produtos'}
        )
        task_csv_produtos >> task_bronze_produtos

    # ======= VENDAS =======
    with TaskGroup("dados_vendas", tooltip="Pipeline Vendas") as tg_vendas:
        task_csv_vendas = PythonOperator(
            task_id='processar_csv_vendas',
            python_callable=processar_csv,
            op_kwargs={'entidade': 'vendas'}
        )
        task_bronze_vendas = PythonOperator(
            task_id='bronze_vendas',
            python_callable=criar_tabela_bronze,
            op_kwargs={'entidade': 'vendas'}
        )
        task_csv_vendas >> task_bronze_vendas
  