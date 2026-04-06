import os
import pandas as pd
import logging

log = logging.getLogger("airflow.task")

def executar_ingestao(origem, destino):
    if not os.path.exists(origem):
        raise FileNotFoundError(f"❌ Arquivo não encontrado: {origem}")
    
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    df = pd.read_csv(origem)
    df.to_parquet(destino, index=False)
    
    log.info(f"✅ Landing -> Bronze Local concluído: {destino}")
    return "Ingestão OK"