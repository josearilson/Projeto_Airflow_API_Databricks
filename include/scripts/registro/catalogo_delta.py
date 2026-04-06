import logging
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

log = logging.getLogger("airflow.task")

def executar_registro_delta(conn_id, path_parquet, schema, tabela):
    hook = DatabricksSqlHook(databricks_conn_id=conn_id)
    tabela_full = f"workspace.{schema}.{tabela}"
    
    sql = f"CREATE OR REPLACE TABLE {tabela_full} USING DELTA AS SELECT * FROM parquet.`{path_parquet}`;"
    hook.run(sql)
    
    log.info(f"✅ Tabela Delta registrada: {tabela_full}")
    return "Registro OK"