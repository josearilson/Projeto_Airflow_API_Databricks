from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

def registrar_tabela_bronze(conn_id, path_csv, schema, tabela, catalog):
    hook = DatabricksSqlHook(databricks_conn_id=conn_id)
    tabela_full = f"{catalog}.{schema}.{tabela}"
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {tabela_full} USING DELTA;

    COPY INTO {tabela_full}
    FROM '{path_csv}'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'delimiter' = ',')
    COPY_OPTIONS ('mergeSchema' = 'true', 'force' = 'true');
    """
    hook.run(sql)