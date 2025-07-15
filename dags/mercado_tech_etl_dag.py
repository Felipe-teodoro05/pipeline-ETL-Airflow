from __future__ import annotations # Apenas como garantia de compatibilidade futura

import pendulum # Para data e hora, muito mais intuitivo que datetime
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator # Para marcar início e fim do DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator # <-- Novo operador
from airflow.models.param import Param
from scripts.extract_g1 import extract_g1_data
from scripts.extract_verge import extract_theverge_data
from scripts.extract_techcrunch import extract_techcrunch_data
from scripts.transform import transformar_dados
from scripts.load import carregar_dados_no_postgres

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
        ),
        "truncate_table": Param(
            False,
            type = "boolean",
            title = "Limpar tabela antes de carregar?",
            description = "Se True, a tabela será limpa antes de inserir novos dados. Use apenas para cargas históricas ou quando necessário."
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

    truncate_table = PostgresOperator(
        task_id="truncate_table",
        postgres_conn_id="postgres_noticias",
        sql="""
        {% if params.truncate_table %}
            TRUNCATE TABLE noticias_mercado;
        {% else %}
            SELECT 1;
        {% endif %}
        """,
    )

    # --- Extração dos dados ---
    extract_g1 = PythonOperator(
        task_id="extrair_noticias_g1",
        python_callable=extract_g1_data,
        op_kwargs={"num_pages": "{{ params.num_pages_scrape }}"}
    )

    extract_theverge = PythonOperator(
        task_id="extrair_noticias_theverge",
        python_callable=extract_theverge_data,
        op_kwargs={"num_pages": "{{ params.num_pages_scrape }}"}
    )

    extract_techcrunch = PythonOperator(
        task_id="extrair_noticias_techcrunch",
        python_callable=extract_techcrunch_data,
        op_kwargs={"num_pages": "{{ params.num_pages_scrape }}"}
    )

    # --- Transformação dos dados ---
    # Executado após a extração de todas as fontes
    transform_data = PythonOperator(
        task_id="transformar_dados",
        python_callable=transformar_dados
    )

    # --- Carregamento dos dados ---
    # Executado após a transformação dos dados
    load_data = PythonOperator(
        task_id="carregar_dados_postgres",
        python_callable=carregar_dados_no_postgres
    )

    # Marcar o fim do DAG
    end = EmptyOperator(task_id = "end")

    # Definindo a ordem de execução das tarefas
    # A tarefa de setup principal 
    start >> create_table

    # A limpeza da tabela depende da sua existência
    create_table >> truncate_table

    # As extrações só começam depois da limpeza da tabela
    truncate_table >> [extract_g1, extract_theverge, extract_techcrunch]

    # A transformação espera por TODAS as extrações
    [extract_g1, extract_theverge, extract_techcrunch] >> transform_data

    # A carga espera pela transformação e, ao final, a tarefa end é executada
    transform_data >> load_data >> end