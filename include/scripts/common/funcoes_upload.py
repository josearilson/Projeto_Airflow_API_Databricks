import requests
import os
import logging

log = logging.getLogger("airflow.task")

# Verifique letra por letra este nome:
def enviar_arquivo_volume(host, token, local_path, remote_path):
    headers = {"Authorization": f"Bearer {token}"}
    dir_remoto = os.path.dirname(remote_path)
    
    requests.post(f"{host}/api/2.0/fs/directories{dir_remoto}", headers=headers)
    
    url = f"{host}/api/2.0/fs/files{remote_path}?overwrite=true"
    with open(local_path, 'rb') as f:
        res = requests.put(url, headers=headers, data=f)
    
    if res.status_code not in [200, 201, 204]:
        raise Exception(f"Falha no upload: {res.text}")
    
    log.info(f"✅ Upload concluído: {remote_path}")