import pandas as pd
import os
from sqlalchemy import create_engine

def load_postgres():
    """
    Lê o arquivo parquet ede dados transformados e carrega no banco de dados PostgreSQL.
    """

    print("Iniciando o carregamento dos dados para o PostgreSQL...")

    # Caminho
    caminho_pqt = "/opt/airflow/scripts/temp_data/noticias_transformadas.parquet"

    if not os.path.exists(caminho_pqt):
        print(f"Arquivo {caminho_pqt} não encontrado.")
        return

    df = pd.read_parquet(caminho_pqt)

    # Conexão com o banco de dados 
    # Usamos as mesmas credenciais e nomes que definimos no docker-compose.yml
    db_user = 'airflow'
    db_password = 'airflow'
    db_name = 'airflow'
    db_host = 'postgres_db_noticias' # IMPORTANTE: Usar o nome do serviço Docker
    db_port = '5432'

    # Criação da string de conexão
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

    try:
        engine = create_engine(connection_string)
        print("Conexão com o banco de dados estabelecida com sucesso.")

        # Carregar dados
        df.to_sql(name="noticias_mercado", con=engine, if_exists="append", index=False)
        print(f"{len(df)} dados carregados com sucesso na tabela 'noticias_mercado")
    
    except Exception as e:
        print(f"Erro ao carregar os dados: {e}")

if __name__ == "__main__":
    load_postgres()