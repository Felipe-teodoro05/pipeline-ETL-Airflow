import time
import json
import os
from datetime import datetime
from bs4 import BeautifulSoup

# Importações do Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

def extract_g1_data():
    """
    Extrai todas as notícias da página de tecnologia do G1 usando Selenium
    para carregar o conteúdo dinâmico.
    """
    print("Iniciando a extração de dados do G1...")

    # --- Configuração do Selenium ---
    chrome_options = Options()
    # 'headless' faz o navegador rodar em segundo plano, sem abrir uma janela visual.
    # Essencial para rodar em servidores (e no nosso container).
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox") # Necessário para rodar como root no Docker
    chrome_options.add_argument("--disable-dev-shm-usage") # Necessário para rodar no Docker
    chrome_options.add_argument("--window-size=1920,1080")

    # Instala e gerencia o driver do Chrome automaticamente
    service = ChromeService()
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    try:
        url = "https://g1.globo.com/tecnologia/"
        print(f"Acessando a URL: {url}")
        driver.get(url)

        # --- Rolagem da Página ---
        print("Aguardando carregamento inicial e rolando a página...")
        # Damos um tempo inicial para a página carregar
        time.sleep(5) 
        
        # Rola a página para baixo 3 vezes para carregar mais notícias
        for i in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            print(f"Rolagem {i+1}/3...")
            time.sleep(3) # Espera 3 segundos para o conteúdo carregar

        # Pega o HTML da página DEPOIS de toda a rolagem
        html_content = driver.page_source
        print("HTML da página completa capturado.")

    finally:
        # Garante que o navegador seja fechado mesmo se ocorrer um erro
        driver.quit()
        print("Navegador Selenium fechado.")

    # --- Análise com BeautifulSoup ---
    soup = BeautifulSoup(html_content, "html.parser")
    lista_noticias = soup.find_all("div", class_="feed-post-body")
    
    if not lista_noticias:
        print("Nenhum container de notícia ('feed-post-body') foi encontrado após a rolagem.")
        return

    print(f"Encontradas {len(lista_noticias)} notícias no G1 após a rolagem.")
    
    noticias_extraidas = []
    for noticia_html in lista_noticias:
        titulo_element = noticia_html.find("a", class_="feed-post-link")
        
        if titulo_element and titulo_element.has_attr('href'):
            titulo = titulo_element.get_text(strip=True)
            link = titulo_element.get("href")
            
            if titulo and link:
                noticias_extraidas.append({
                    "titulo": titulo,
                    "link": link,
                    "fonte": "G1 Tecnologia",
                    "data_extracao": datetime.now().isoformat()
                })

    # --- Salvando o arquivo ---
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

if __name__ == "__main__":
    extract_g1_data()