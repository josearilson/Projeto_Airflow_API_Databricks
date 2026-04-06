import os

class Config:
    # Databricks
    HOST = os.getenv('DATABRICKS_HOST')
    TOKEN = os.getenv('DATABRICKS_TOKEN')
    CONN_ID = "databricks_default"
    
    # Pastas Locais
    AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')
    PATH_LANDING = os.path.join(AIRFLOW_HOME, "include", "01_landing")
    PATH_BRONZE = os.path.join(AIRFLOW_HOME, "include", "02_bronze")