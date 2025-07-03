# Partimos da imagem oficial do Airflow como nossa base
FROM apache/airflow:2.8.1

# Mudamos para o usuário 'root' para poder instalar pacotes do sistema
USER root

# Habilitamos repositórios extras e instalamos todas as dependências
# Adicionamos jq para processar JSON na linha de comando
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    unzip \
    curl \
    jq \
    libglib2.0-0 libnss3 libgconf-2-4 libfontconfig1 \
    libx11-6 libxcb1 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgbm1 libgtk-3-0 libasound2 libxtst6 \
    fonts-liberation libvulkan1 xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Baixa e instala a versão estável do Google Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm google-chrome-stable_current_amd64.deb

# Retornamos para o usuário padrão do Airflow
USER airflow

# Instalar dependências Python necessárias
# O webdriver-manager vai gerenciar automaticamente o ChromeDriver
RUN pip install --no-cache-dir selenium beautifulsoup4 requests webdriver-manager