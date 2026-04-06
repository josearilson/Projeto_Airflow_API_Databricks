from include.scripts.camadas.bronze import processar_camada_bronze

def iniciar_pipeline_medalhao():
    # Aqui você pode chamar várias tabelas se quiser
    resultado = processar_camada_bronze(entidade="clientes")
    return f"🚀 Pipeline Finalizado: {resultado}"