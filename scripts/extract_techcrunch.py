# Importação das bibliotecas necessárias
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import argparse


def extract_techcrunch_data(num_pages):
    """
    Extrai as notícias da seção de Startups do TechCrunch, 
    navegando por um número customizado de páginas.
    """
    num_pages = int(num_pages)
    print(f"Iniciando a extração de dados do TechCrunch para {num_pages} página(s)...")

    url_base = "https://techcrunch.com/category/startups/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    # Esta lista agora vai acumular as notícias de todas as páginas.
    noticias_extraidas = []

    for page in range(1, num_pages + 1):
        if page == 1:
            url = url_base
        else:
            url = f"{url_base}page/{page}/"
        
        print(f"Extraindo dados da página {page}: {url}")

        try: 
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            print("Página carregada com sucesso.")
        except requests.RequestException as e:
            print(f"Erro ao acessar a página {page}: {e}")
            continue # Pula para a próxima página em caso de erro

        soup = BeautifulSoup(response.content, "html.parser")
        main_container = soup.find('div', class_='wp-block-query')
        
        if not main_container:
            print(f"Container principal não encontrado na página {page}. Parando a extração.")
            break

        lista_noticias = main_container.find_all('li', class_='wp-block-post')
        
        print(f"Encontradas {len(lista_noticias)} notícias na página {page}.")

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
        
        # Pausa  entre as requisições
        if page < num_pages:
            print("Aguardando 1 segundo antes da próxima página...")
            time.sleep(1)

    # --- Retorna os dados ---
    print(f"Total de notícias coletadas do TechCrunch: {len(noticias_extraidas)}")
    return noticias_extraidas

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script de extração de notícias do TechCrunch.")
    parser.add_argument("--num-pages", type=int, default=1, help="Número de páginas a serem extraídas (padrão: 1)")
    args = parser.parse_args()
    
    extract_techcrunch_data(num_pages=args.num_pages)