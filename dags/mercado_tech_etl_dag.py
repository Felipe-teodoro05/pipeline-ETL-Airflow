from __future__ import annotations # Apenas como garantia de compatibilidade futura

import pendulum # Para data e hora, muito mais intuitivo que datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator # Para executar scripts Python
from airflow.operators.empty import EmptyOperator # Para marcar início e fim do DAG

# Definindo fuso horário
local_tz = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id = "mercado_tech_etl_dag",
    start_date = pendulum.datetime(2025,7,8, tz = local_tz),
    schedule_interval = "0 8 * * *", # Todo dia 8 da manhã
    catchup = False,
    tags = ["portfolio", "ETL", "dados"],
    doc_md = """
    ## Pipeline de ETL de notícias do Mercado Tech

    Este pipeline extrai notícias de 3 fontes (G1, The Verge, Techcrunch),
    as transforma (Consolida, limpa e enriquece com NLP) e carrega em um banco de dados PostgreSQL.
    - **Autor**: Felipe Teodoro
    - **Contato**: teodorobfelipe@gmail.com
    """,
) as dag:
    # Marcar o começo do DAG
    start = EmptyOperator(task_id = "start")

    # --- Extração dos dados ---
    extract_g1 = BashOperator(
        task_id = "extrair_noticias_g1",
        bash_command = "python3 /opt/airflow/scripts/extract_g1.py",
    )

    extract_theverge = BashOperator(
        task_id = "extrair_noticias_theverge",
        bash_command = "python3 /opt/airflow/scripts/extract_verge.py",
    )

    extract_techcrunch = BashOperator(
        task_id = "extrair_noticias_techcrunch",
        bash_command = "python3 /opt/airflow/scripts/extract_techcrunch.py",
    )

    # --- Transformação dos dados ---
    # Executado após a extração de todas as fontes
    transform = BashOperator(
        task_id = "transformar_noticias",
        bash_command = "python3 /opt/airflow/scripts/transform.py",
    )

    # --- Carregamento dos dados ---
    # Executado após a transformação dos dados
    load = BashOperator(
        task_id = "carregar_noticias_postgres",
        bash_command = "python3 /opt/airflow/scripts/load.py",
    )

    # Marcar o fim do DAG
    end = EmptyOperator(task_id = "end")

    # Definindo a ordem de execução das tarefas
    start >> [extract_g1, extract_theverge, extract_techcrunch] >> transform >> load >> end