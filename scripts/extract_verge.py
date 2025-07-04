import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime 

def extract_the_verge():
    """
    Extrai todas as notícias da página de tecnologia do The Verge
    """
    print("Iniciando a extração de dados do The Verge...")

    # Por que usar url base e seção Tech separados?
    # Vou fazer isso pois ao extrair a url da notícia, os links são relativos, então vou precisar utilizar a url base para formar o link completo.
    url_base = "https://www.theverge.com"
    url_secao_tech = f"{url_base}/tech"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    try: 
        response = requests.get(url_secao_tech, headers=headers, timeout=10)
        response.raise_for_status()
        print("Página carregada com sucesso.")
    except requests.RequestException as e:
        print(f"Erro ao acessar a página: {e}")
        return

    soup = BeautifulSoup(response.content, "html.parser")

    # --- Extração de notícias ---
    lista_noticias = soup.select('div[class*="duet--content-cards--content-card"]')

    if not lista_noticias:
        print("Nenhuma notícia encontrada na página.")
        return
    
    print(f"Total de notícias encontradas: {len(lista_noticias)}")

    noticias_extraidas = []
    for noticias in lista_noticias:
        elemento_titulo = noticias.find("a", href=True)

        if elemento_titulo:
            titulo = elemento_titulo.get_text(strip= True)
            link_relativo = elemento_titulo.get("href")
            # Juntando link relativo com a url base
            link_completo = url_base+ link_relativo
            
            if titulo and link_completo:
                noticias_extraidas.append({
                    "titulo": titulo,
                    "link": link_completo,
                    "fonte": "The Verge Tecnologia",
                    "data_extracao": datetime.now().isoformat()
                })

    # --- Salvando o arquivo ---
    if noticias_extraidas:
        caminho_base = '/opt/airflow/scripts/temp_data'
        os.makedirs(caminho_base, exist_ok=True)
        caminho_arquivo = os.path.join(caminho_base, "theverge_noticias.json")
        
        with open(caminho_arquivo, "w", encoding="utf-8") as f:
            json.dump(noticias_extraidas, f, ensure_ascii=False, indent=4)
        
        print(f"Dados salvos em {caminho_arquivo}")
        print(f"Total de notícias extraídas do The Verge: {len(noticias_extraidas)}")
    else:
        print("Nenhuma notícia foi efetivamente extraída do G1.")

if __name__ == "__main__":
    extract_the_verge()