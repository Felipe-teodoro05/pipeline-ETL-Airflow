import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import argparse
import pandas as pd

def extract_theverge_data(num_pages):
    """
    Extrai notícias da seção de tecnologia do The Verge, com paginação,
    usando o seletor de container correto e removendo duplicatas no final.
    """
    num_pages = int(num_pages)
    print(f"Iniciando a extração de dados do The Verge para {num_pages} página(s)...")
    
    url_base = "https://www.theverge.com"
    url_secao_tech = f"{url_base}/tech"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    noticias_extraidas = []

    for page_num in range(1, num_pages + 1):
        if page_num == 1:
            url_pagina = url_secao_tech
        else:
            url_pagina = f"{url_secao_tech}/archives/{page_num}"

        print(f"Extraindo dados da página {page_num}: {url_pagina}")

        try: 
            response = requests.get(url_pagina, headers=headers, timeout=15)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Erro ao acessar a página {page_num} do The Verge: {e}. Parando a paginação.")
            break

        soup = BeautifulSoup(response.content, "html.parser")
        
        lista_noticias = soup.select('div[class*="duet--content-cards--content-card"]')

        if not lista_noticias:
            print(f"Nenhuma notícia encontrada na página {page_num}.")
            if page_num == 1:
                print("AVISO: A página principal pode ter mudado para carregamento dinâmico (JavaScript).")
            continue

        print(f"Encontradas {len(lista_noticias)} notícias na página {page_num}.")

        for noticia_html in lista_noticias:
            # Dentro de cada card, procuramos a primeira tag <a> com um href
            elemento_titulo = noticia_html.find("a", href=True)
            if elemento_titulo:
                titulo = elemento_titulo.get_text(strip=True)
                link_relativo = elemento_titulo.get("href")
                
                if titulo and link_relativo:
                    link_completo = link_relativo if link_relativo.startswith('http') else url_base + link_relativo
                    noticias_extraidas.append({
                        "titulo": titulo,
                        "link": link_completo,
                        "fonte": "The Verge",
                        "data_extracao": datetime.now().isoformat()
                    })
        
        if page_num < num_pages:
            print("Aguardando 1 segundo...")
            time.sleep(1)

    print(f"The Verge: Extração concluída. {len(noticias_extraidas)} notícias encontradas.")
    return noticias_extraidas

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Script de extração de notícias do The Verge com paginação.")
    parser.add_argument("--num-pages", type=int, default=1, help="Número de páginas para extrair. Padrão: 1.")
    args = parser.parse_args()
    
    extract_theverge_data(num_pages=args.num_pages)