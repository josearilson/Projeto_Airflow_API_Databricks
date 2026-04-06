from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

def registrar_tabela_parquet(conn_id, path_parquet, schema, tabela):
    # 1. Primeiro criamos o objeto de conexão (o Hook)
    hook = DatabricksSqlHook(databricks_conn_id=conn_id)
    
    # 2. Agora que o 'hook' existe, garantimos que o Schema existe
    hook.run(f"CREATE SCHEMA IF NOT EXISTS workspace.{schema}")
    
    # 3. Definimos o nome completo da tabela
    tabela_full = f"workspace.{schema}.{tabela}"
     
    # 4. Preparamos o SQL com aspas triplas
    sql = f"""
        CREATE OR REPLACE TABLE {tabela_full}
        USING DELTA
        AS SELECT * FROM parquet.`{path_parquet}`;
    """
    
    # 5. Executamos a criação da tabela
    hook.run(sql)