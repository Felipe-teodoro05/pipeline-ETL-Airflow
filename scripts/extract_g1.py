import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import argparse

def extract_g1_data(num_pages):
    """
    Extrai notícias de tecnologia do G1 usando o feed de paginação HTML.
    """
    num_pages = int(num_pages)
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

        except requests.RequestException as e:
            print(f"Erro ao processar a página {page_num} do G1: {e}. Parando a paginação.")
            break

        # Pausa entre as requisições
        if page_num < num_pages:
            print(f"Aguardando 1 segundo...")
            time.sleep(1)

    # --- Retornando todas as notícias coletadas ---
    print(f"G1: Extração concluída. {len(noticias_extraidas)} notícias encontradas.")
    return noticias_extraidas

if __name__ == '__main__':  
    parser = argparse.ArgumentParser(description="Script de extração de notícias do G1 com paginação.")
    parser.add_argument(
        "--num-pages", 
        type=int, 
        default=1, 
        help="Número de páginas para extrair. Padrão: 1."
    )
    
    args = parser.parse_args()
    
    # Chama a função principal com o argumento recebido
    extract_g1_data(num_pages=args.num_pages)