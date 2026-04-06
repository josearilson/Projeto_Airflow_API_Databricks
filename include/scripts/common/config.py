import os

class Config:
    # Databricks
    HOST = os.getenv('DATABRICKS_HOST')
    TOKEN = os.getenv('DATABRICKS_TOKEN')
    CONN_ID = "databricks_default"
    
    # Pastas Locais
    DIR_AIRFLOW_HOME = os.environ.get('DIR_AIRFLOW_HOME', '/usr/local/airflow')

    DIR_LANDING = os.path.join(DIR_AIRFLOW_HOME, "include", "01_landing")
    DIR_BRONZE = os.path.join(DIR_AIRFLOW_HOME, "include", "02_bronze")