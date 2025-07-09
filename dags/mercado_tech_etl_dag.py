from __future__ import annotations # Apenas como garantia de compatibilidade futura

import pendulum # Para data e hora, muito mais intuitivo que datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator # Para executar scripts Python
from airflow.operators.empty import EmptyOperator # Para marcar início e fim do DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.param import Param

# Definindo fuso horário
local_tz = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id = "mercado_tech_etl_dag",
    start_date = pendulum.datetime(2025,7,8, tz = local_tz),
    schedule_interval = "0 8 * * *", # Todo dia 8 da manhã
    catchup = False,
    tags = ["portfolio", "ETL", "dados"],
    params = {
        "num_pages_scrape": Param(
            1, 
            type = "integer",
            minimum = 1,
            title = "Número de páginas a serem raspadas",
            description = "Quantas páginas de cada fonte devem ser extraídas. Padrão é 1 para execuções agendadas (incrementais)"
        )
    },
    doc_md="""
    ## Pipeline de ETL de Notícias de Tecnologia
    
    Este pipeline automatizado realiza as seguintes tarefas:
    1. **Criação da Tabela**: Garante que a tabela de destino exista no PostgreSQL.
    2. **Extração**: Coleta notícias de G1, The Verge e TechCrunch em paralelo. A quantidade de páginas é parametrizável.
    3. **Transformação**: Unifica os dados, limpa, e enriquece com NLP para identificar empresas nos títulos.
    4. **Carga**: Insere os dados transformados no PostgreSQL.
    """
) as dag:
    # Marcar o começo do DAG
    start = EmptyOperator(task_id = "start")

    # Tarefa para garantir que a tabela exista antes de qualquer coisa
    # A idempotência é garantida pelo uso do CREATE TABLE IF NOT EXISTS
    create_table = PostgresOperator(
        task_id = "create_table",
        postgres_conn_id = "postgres_noticias", # Conexão padrão do Airflow com seu banco de metadados
        sql = """
            CREATE TABLE IF NOT EXISTS noticias_mercado (
                titulo TEXT,
                link TEXT PRIMARY KEY,
                fonte VARCHAR(50),
                data_publicacao TIMESTAMP,
                data_extracao TIMESTAMP,
                empresas_citadas TEXT
            );
        """
    )

    # --- Extração dos dados ---
    extract_g1 = BashOperator(
        task_id = "extrair_noticias_g1",
        bash_command = "python /opt/airflow/scripts/extract_g1.py --num-pages {{ params.num_pages_scrape }}",
    )

    extract_theverge = BashOperator(
        task_id = "extrair_noticias_theverge",
        bash_command = "python /opt/airflow/scripts/extract_verge.py --num-pages {{ params.num_pages_scrape }}",
    )

    extract_techcrunch = BashOperator(
        task_id = "extrair_noticias_techcrunch",
        bash_command = "python /opt/airflow/scripts/extract_techcrunch.py --num-pages {{ params.num_pages_scrape }}",
    )

    # --- Transformação dos dados ---
    # Executado após a extração de todas as fontes
    transform = BashOperator(
        task_id = "transformar_noticias",
        bash_command = "python /opt/airflow/scripts/transform.py",
    )

    # --- Carregamento dos dados ---
    # Executado após a transformação dos dados
    load = BashOperator(
        task_id = "carregar_noticias_postgres",
        bash_command = "python /opt/airflow/scripts/load.py",
    )

    # Marcar o fim do DAG
    end = EmptyOperator(task_id = "end")

    # Definindo a ordem de execução das tarefas
    start >> create_table >> [extract_g1, extract_theverge, extract_techcrunch] >> transform >> load >> end