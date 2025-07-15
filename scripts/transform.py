import pandas as pd
import spacy
import json

def transformar_dados(**kwargs):
    """
    Recebe os dados das tarefas de extração via XCom, consolida, limpa,
    enriquece com NLP e retorna um JSON com os dados finais para a próxima tarefa.
    """
    print("Iniciando o processo de transformação e enriquecimento via XCom...")
    
    # Objeto ti (Task Instance) que dá acesso aos XComs
    ti = kwargs['ti']
    
    # Puxa os resultados (as listas de notícias) das tarefas de extração
    # Os task_ids devem ser os mesmos definidos na DAG
    extracao_g1 = ti.xcom_pull(task_ids='extrair_noticias_g1') or []
    extracao_verge = ti.xcom_pull(task_ids='extrair_noticias_theverge') or []
    extracao_techcrunch = ti.xcom_pull(task_ids='extrair_noticias_techcrunch') or []
    
    # Une todas as listas de notícias em uma só
    todas_noticias = extracao_g1 + extracao_verge + extracao_techcrunch
    
    if not todas_noticias:
        print("Nenhuma notícia foi extraída das fontes.")
        return None # Retorna None para que as tarefas seguintes sejam puladas (skipped)

    df_consolidado = pd.DataFrame(todas_noticias)
    print(f"Total de notícias BRUTAS consolidadas: {len(df_consolidado)}")
    
    # --- ETAPA DE LIMPEZA ---
    # Normaliza a coluna de links e remove duplicatas
    df_consolidado['link'] = df_consolidado['link'].str.strip().str.rstrip('/')
    df_consolidado.drop_duplicates(subset=['link'], inplace=True, keep='first')
    
    print(f"Total de notícias ÚNICAS (após remover duplicatas): {len(df_consolidado)}")

    # --- ENRIQUECIMENTO COM NLP ---
    try:
        nlp_en = spacy.load("en_core_web_sm")
        nlp_pt = spacy.load("pt_core_news_sm")
    except IOError:
        print("ERRO: Modelos do spaCy não encontrados. Verifique a construção da imagem Docker.")
        raise

    def extrair_empresas(row):
        titulo = row['titulo']
        fonte = row['fonte']
        doc = nlp_pt(titulo) if fonte == 'G1 Tecnologia' else nlp_en(titulo)
        empresas = [ent.text for ent in doc.ents if ent.label_ == 'ORG']
        return ", ".join(empresas) if empresas else None

    print("Iniciando extração de entidades com spaCy...")
    df_consolidado['empresas_citadas'] = df_consolidado.apply(extrair_empresas, axis=1)
    print("Extração de entidades concluída.")

    # --- PREPARAÇÃO FINAL ---
    # Garante que as colunas de data tenham o tipo correto
    if 'data_publicacao' not in df_consolidado.columns:
        df_consolidado['data_publicacao'] = pd.NaT
    df_consolidado['data_publicacao'] = pd.to_datetime(df_consolidado['data_publicacao'], errors='coerce')
    df_consolidado['data_extracao'] = pd.to_datetime(df_consolidado['data_extracao'])
    
    colunas_finais = ['titulo', 'link', 'fonte', 'data_publicacao', 'data_extracao', 'empresas_citadas']
    df_final = df_consolidado[colunas_finais]

    print(f"Transformação finalizada. Total de {len(df_final)} notícias prontas para carga.")
    
    # Retorna o DataFrame final como uma string no formato JSON
    # O Airflow salvará isso como um XCom para a próxima tarefa
    return df_final.to_json(orient='records', date_format='iso')