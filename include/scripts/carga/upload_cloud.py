import os
import requests
import logging

log = logging.getLogger("airflow.task")

def executar_carga(host, token, local, remoto):
    headers = {"Authorization": f"Bearer {token}"}
    diretorio_remoto = os.path.dirname(remoto)
    requests.post(f"{host}/api/2.0/fs/directories{diretorio_remoto}", headers=headers)

    url_upload = f"{host}/api/2.0/fs/files{remoto}?overwrite=true"
    with open(local, 'rb') as f:
        res = requests.put(url_upload, headers=headers, data=f)
    
    if res.status_code not in [200, 201, 204]:
        raise Exception(f"Erro no upload: {res.text}")
    
    log.info(f"✅ Upload para Volume concluído: {remoto}")
    return "Carga OK"