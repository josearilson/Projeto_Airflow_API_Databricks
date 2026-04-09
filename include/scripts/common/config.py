import os

class Config:
    # Credenciais
    HOST = os.getenv('DATABRICKS_HOST')
    TOKEN = os.getenv('DATABRICKS_TOKEN')
    CONN_ID = "databricks_default"
    
    # Pastas Locais (Somente a Landing é necessária agora)
    DIR_AIRFLOW_HOME = os.environ.get('DIR_AIRFLOW_HOME', '/usr/local/airflow')
    DIR_LANDING = os.path.join(DIR_AIRFLOW_HOME, "include", "01_landing")