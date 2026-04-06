import os
import pandas as pd
   

def convert_csv_to_parquet_local():

    Dir_airflow_home = os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')
     

    dir_caminho_csv = os.path.join(Dir_airflow_home, "include", "raw_zone", "clientes.csv")

    dir_caminho_destino = os.path.join(Dir_airflow_home, "include", "bronze", "clientes.parquet")


    #se o caminho do csv não existir, lança um erro
    if not os.path.exists(dir_caminho_csv):

        raise FileNotFoundError(f"Arquivo {dir_caminho_csv} não encontrado.")



    #cria a pasta do destino caso não exista 
    os.makedirs(os.path.dirname(dir_caminho_destino), exist_ok=True)

   

    # Processamento local com Pandas

    df = pd.read_csv(dir_caminho_csv)

    df.to_parquet(dir_caminho_destino, index=False)

    print(f"Sucesso Local! Arquivo salvo em: {dir_caminho_destino}")