from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

def registrar_tabela_parquet(conn_id, path_parquet, schema, tabela):
    hook = DatabricksSqlHook(databricks_conn_id=conn_id)
    tabela_full = f"workspace.{schema}.{tabela}"
    
    sql = f"CREATE OR REPLACE TABLE {tabela_full} USING DELTA AS SELECT * FROM parquet.`{path_parquet}`;"
    hook.run(sql)