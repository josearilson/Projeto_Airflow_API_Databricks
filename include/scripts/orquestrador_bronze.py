import os
# Adicione 'include.' antes de cada importação para que o Python localize as pastas
from include.scripts.ingestao.conversor import executar_ingestao
from include.scripts.carga.upload_cloud import executar_carga
from include.scripts.registro.catalogo_delta import executar_registro_delta

def executa_fluxo_completo_bronze():
    # --- Configurações ---
    airflow_home = os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')
    path_landing = os.path.join(airflow_home, "include", "01_landing", "clientes.csv")
    path_bronze  = os.path.join(airflow_home, "include", "02_bronze", "clientes.parquet")
    
    # --- Execução ---
    step1 = executar_ingestao(path_landing, path_bronze)
    
    step2 = executar_carga(
        os.getenv('DATABRICKS_HOST'),
        os.getenv('DATABRICKS_TOKEN'),
        path_bronze,
        f"/Volumes/workspace/data_lake/01_bronze/clientes.parquet"
    )
    
    step3 = executar_registro_delta(
        "databricks_default",
        f"/Volumes/workspace/data_lake/01_bronze/clientes.parquet",
        "data_lake",
        "clientes_bronze"
    )

    return f"🚀 Pipeline Bronze finalizado: {step1} | {step2} | {step3}"