import pandas as pd
import glob 
import os
import json
import spacy

def transformar_dados():
    """
    Lê os JSONs, consolida, limpa e enriquece os dados com NLP para
    extrair nomes de empresas, salvando o resultado em um arquivo Parquet
    """
    print("Iniciando a o processo de transformação e enriquecimento...")

    # Caminho base onde os arquivos JSON foram salvos
    caminho_base = '/opt/airflow/scripts/temp_data/'

    # Encontra todos os arquivos JSON no diretório
    arquivos_json = glob.glob(f"{caminho_base}*.json")

    if not arquivos_json:
        print("Nenhum arquivo JSON encontrado para transformação.")
        return
    
    print(f"Arquivos JSON encontrados: {arquivos_json}")

    # --- CARREGANDO OS MODELOS DO SPACY ---
    # Carregamos os dois modelos que baixamos no Dockerfile
    try:
        nlp_en = spacy.load("en_core_web_sm")
        nlp_pt = spacy.load("pt_core_news_sm")
        print("Modelos SpaCy carregados com sucesso.")
    except IOError:
        print("ERRO: Modelos do spaCy não encontrados. Reconstrua a imagem com 'docker compose up --build.")
        return

    lista_dfs = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    df_consolidado.drop_duplicates(subset=["link"], inplace=True, keep='first')

    print(f"Dados consolidados e limpos. Total de notícias: {len(df_consolidado)}")

    ## --- Enriquecendo com NLP ---
    def extrair_empresas(row):
        """
        Função que recebe uma linha do DataFrame e usa o modelo de NLP correto
        para extrair nomes de organizações (empresas) do título.
        """
        titulo = row['titulo']
        fonte = row['fonte']

        # Escolher o modelo de NLP baseado na fonte
        if fonte == "G1 Tecnologia":
            doc = nlp_pt(titulo)  # Usando o modelo em português para G1
        else: # Considerando as outras duas fontes
            doc = nlp_en(titulo)

        # Extrai o texto de todas as entidas que o spacy classificou como ORGANIZAÇÃO
        empresas = [ent.text for ent in doc.ents if ent.label_ == "ORG"]

        # Retorna uma lista de empresas ou uma string vazia se nenhuma for encontrada
        return ", ".join(empresas) if empresas else None
    
    print("Iniciando extração de entidades com spaCy (isso pode levar alguns segundos)...")
    
    # O método .apply() do Pandas executa a função 'extrair_empresas' para cada linha.
    # O resultado é usado para criar nossa nova coluna.
    df_consolidado["empresas_citadas"] = df_consolidado.apply(extrair_empresas, axis=1)

    print("Extração de entidades concluída.")
    # Mostrar as primeiras linhas do DataFrame enriquecido
    print(df_consolidado[["titulo", "fonte", "empresas_citadas"]].head(10))

    # --- Salvando o resultado final ---
    # Garantimos que a coluna de data de publicação exista antes de reordenar
    if 'data_publicacao' not in df_consolidado.columns:
        df_consolidado['data_publicacao'] = pd.NaT  # Preenche com NaT se não existir
    df_consolidado["data_publicacao"] = pd.to_datetime(df_consolidado["data_publicacao"], errors='coerce')
    df_consolidado["data_extracao"] = pd.to_datetime(df_consolidado["data_extracao"])

    # Definindo as colunas
    colunas = ["titulo", "link", "fonte", "data_publicacao", "data_extracao", "empresas_citadas"]
    df_final = df_consolidado[colunas]

    caminho_saida = os.path.join(caminho_base, 'noticias_transformadas.parquet')
    df_final.to_parquet(caminho_saida, index=False)
    
    print(f"Dados transformados e enriquecidos salvos em: {caminho_saida}")

if __name__ == '__main__':
    transformar_dados()