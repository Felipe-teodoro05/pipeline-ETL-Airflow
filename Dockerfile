# Definimos versões como argumentos para fácil manutenção
ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_MAJOR_MINOR_VERSION=3.8

# Partimos da imagem oficial do Airflow como nossa base
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_MAJOR_MINOR_VERSION}

# Mudamos para o usuário 'root' para poder instalar pacotes do sistema
USER root

# Instala todas as dependências do sistema de uma só vez
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget gnupg unzip curl jq build-essential python3-dev \
    libglib2.0-0 libnss3 libgconf-2-4 libfontconfig1 \
    libx11-6 libxcb1 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgbm1 libgtk-3-0 libasound2 libxtst6 \
    fonts-liberation libvulkan1 xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Baixa e instala o Google Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm google-chrome-stable_current_amd64.deb

# Baixa e instala o chromedriver compatível
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}') && \
    CHROME_MAJOR_VERSION=$(echo $CHROME_VERSION | cut -d '.' -f1) && \
    CHROMEDRIVER_URL=$(curl -sS "https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json" | jq -r "[.versions[] | select(.version | startswith(\"${CHROME_MAJOR_VERSION}.\")) | .downloads.chromedriver[] | select(.platform == \"linux64\") | .url][0]") && \
    wget -q "${CHROMEDRIVER_URL}" -O chromedriver-linux64.zip && \
    unzip chromedriver-linux64.zip && \
    mv chromedriver-linux64/chromedriver /usr/local/bin/ && \
    chmod +x /usr/local/bin/chromedriver && \
    rm chromedriver-linux64.zip

# Retornamos para o usuário padrão do Airflow
USER airflow

# Definimos a URL de restrições para as dependências do Airflow
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.8.txt"

# Instala as dependências Python usando uma combinação de restrições e versões fixas
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" \
    "apache-airflow-providers-postgres" \
    "psycopg2-binary" \
    "requests" \
    "beautifulsoup4" \
    "selenium==4.20.0" \
    "pandas" \
    "spacy==3.7.2"

# Baixa os modelos de linguagem do spaCy
RUN python -m spacy download en_core_web_sm && \
    python -m spacy download pt_core_news_sm