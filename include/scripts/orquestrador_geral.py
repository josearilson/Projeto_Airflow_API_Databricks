from include.scripts.camadas.bronze import processar_camada_bronze

def iniciar_pipeline_medalhao(entidade):
    resultado = processar_camada_bronze(entidade=entidade)
    return f"🚀 Pipeline Finalizado: {resultado}"