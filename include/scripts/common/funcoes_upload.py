import requests
import os
import logging

log = logging.getLogger("airflow.task")

def enviar_arquivo_volume(host, token, local_path, remote_path):
    headers = {"Authorization": f"Bearer {token}"}
    dir_remoto = os.path.dirname(remote_path)
    
    # 1. Tenta criar o diretório, mas não trava se der erro (Volumes podem ser rígidos)
    try:
        requests.post(f"{host}/api/2.0/fs/directories{dir_remoto}", headers=headers)
    except Exception as e:
        log.warning(f"Aviso ao criar diretório (pode já existir): {e}")
    
    # 2. Upload do arquivo
    url = f"{host}/api/2.0/fs/files{remote_path}?overwrite=true"
    
    with open(local_path, 'rb') as f:
        # Enviamos o binário do arquivo diretamente no 'data'
        res = requests.put(url, headers=headers, data=f)
    
    if res.status_code not in [200, 201, 204]:
        raise Exception(f"Falha no upload: {res.status_code} - {res.text}")
    
    log.info(f"✅ Upload concluído: {remote_path}")