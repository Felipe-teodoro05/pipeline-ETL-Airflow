import requests
from bs4 import BeautifulSoup
import json
import os
import time
from datetime import datetime
import argparse

def extract_g1_data(num_pages: int):
    """
    Extrai notícias de tecnologia do G1 usando o feed de paginação HTML.
    """
    print(f"Iniciando a extração de dados do G1 para {num_pages} página(s)...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    noticias_extraidas = []

    # Loop para navegar pelas páginas do feed
    for page_num in range(1, num_pages + 1):
        url_pagina = f"https://g1.globo.com/tecnologia/index/feed/pagina-{page_num}.ghtml"
        print(f"Extraindo dados da página {page_num}: {url_pagina}")

        try:
            response = requests.get(url_pagina, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, "html.parser")
            lista_noticias_html = soup.find_all("div", class_="feed-post-body")
            
            if not lista_noticias_html:
                print(f"Nenhuma notícia encontrada na página {page_num}. A paginação pode ter terminado.")
                break # Se não há mais notícias, paramos o loop

            for noticia_html in lista_noticias_html:
                titulo_element = noticia_html.find("a", class_="feed-post-link")
                if titulo_element and titulo_element.has_attr('href'):
                    noticias_extraidas.append({
                        "titulo": titulo_element.get_text(strip=True),
                        "link": titulo_element.get("href"),
                        "fonte": "G1 Tecnologia",
                        "data_publicacao": None, # A data não está disponível neste feed
                        "data_extracao": datetime.now().isoformat()
                    })
            
            # Pausa entre as requisições
            if page_num < num_pages:
                print(f"Aguardando 1 segundo...")
                time.sleep(1)

        except requests.RequestException as e:
            print(f"Erro ao processar a página {page_num} do G1: {e}. Parando a paginação.")
            break

    # --- Salvando o arquivo com todas as notícias coletadas ---
    if noticias_extraidas:
        caminho_base = '/opt/airflow/scripts/temp_data'
        os.makedirs(caminho_base, exist_ok=True)
        caminho_arquivo = os.path.join(caminho_base, "g1_noticias.json")
        with open(caminho_arquivo, "w", encoding="utf-8") as f:
            json.dump(noticias_extraidas, f, ensure_ascii=False, indent=4)
        print(f"Dados salvos em {caminho_arquivo}")
        print(f"Total de notícias extraídas do G1: {len(noticias_extraidas)}")
    else:
        print("Nenhuma notícia foi efetivamente extraída do G1.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Script de extração de notícias do G1 com paginação.")
    parser.add_argument("--num-pages", type=int, default=1, help="Número de páginas para extrair. Padrão: 1.")
    args = parser.parse_args()
    
    extract_g1_data(num_pages=args.num_pages)