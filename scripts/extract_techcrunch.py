import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime 

def extract_the_verge():
    """
    Extrai as notícias da página de tecnologia do TechCrunch.
    """
    print("Iniciando a extração de dados do TechCrunch...")

    # Vou extrair as notícias da seção de startups que é o foco do projeto, mas se aplica para todo o site
    url = "https://techcrunch.com/category/startups/"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    try: 
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        print("Página carregada com sucesso.")
    except requests.RequestException as e:
        print(f"Erro ao acessar a página: {e}")
        return

    soup = BeautifulSoup(response.content, "html.parser")

    # --- Extração de notícias ---
    main_container = soup.find('div', class_='wp-block-query')
    
    if not main_container:
        print("Container principal de notícias ('wp-block-query') não foi encontrado.")
        return

    # Buscamos os posts apenas dentro do  container principal.
    lista_noticias = main_container.find_all('li', class_='wp-block-post')
    
    if not lista_noticias:
        print("Nenhum post ('li.wp-block-post') foi encontrado dentro do container principal.")
        return

    print(f"Encontrados {len(lista_noticias)} posts no container principal do TechCrunch.")
    
    noticias_extraidas = []

    for post_html in lista_noticias:
        titulo_container = post_html.find('h3', class_='loop-card__title')
        if titulo_container:
            link_element = titulo_container.find('a')
            if link_element and link_element.has_attr('href'):
                titulo = link_element.get_text(strip=True)
                link = link_element.get("href")
                
                if titulo and link:
                    noticias_extraidas.append({
                        "titulo": titulo,
                        "link": link,
                        "fonte": "TechCrunch",
                        "data_extracao": datetime.now().isoformat()
                    })

    # --- Salvando o arquivo ---
    if noticias_extraidas:
        caminho_base = '/opt/airflow/scripts/temp_data'
        os.makedirs(caminho_base, exist_ok=True)
        caminho_arquivo = os.path.join(caminho_base, "techcrunch_noticias.json")
        
        with open(caminho_arquivo, "w", encoding="utf-8") as f:
            json.dump(noticias_extraidas, f, ensure_ascii=False, indent=4)
        
        print(f"Dados salvos em {caminho_arquivo}")
        print(f"Total de notícias extraídas do TechCrunch: {len(noticias_extraidas)}")
    else:
        print("Nenhuma notícia foi efetivamente extraída do TechCrunch.")

    # --- Salvando o arquivo ---
    if noticias_extraidas:
        caminho_base = '/opt/airflow/scripts/temp_data'
        os.makedirs(caminho_base, exist_ok=True)
        caminho_arquivo = os.path.join(caminho_base, "techcrunch_noticias.json")
        
        with open(caminho_arquivo, "w", encoding="utf-8") as f:
            json.dump(noticias_extraidas, f, ensure_ascii=False, indent=4)
        
        print(f"Dados salvos em {caminho_arquivo}")
        print(f"Total de notícias extraídas do The Verge: {len(noticias_extraidas)}")
    else:
        print("Nenhuma notícia foi efetivamente extraída do TechCrunch.")

if __name__ == "__main__":
    extract_the_verge()