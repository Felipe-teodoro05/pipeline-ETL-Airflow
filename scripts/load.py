import pandas as pd
from sqlalchemy import create_engine
import json
import psycopg2

def carregar_dados_no_postgres(**kwargs):
    """
    Recebe os dados transformados via XCom e os carrega
    na tabela 'noticias_mercado' do banco de dados PostgreSQL.
    """
    print("Iniciando o processo de carga para o PostgreSQL via XCom...")
    
    ti = kwargs['ti']
    # Puxa a string JSON da tarefa de transformação
    noticias_json_str = ti.xcom_pull(task_ids='transformar_dados')

    if not noticias_json_str or noticias_json_str == 'None':
        print("Nenhum dado recebido da tarefa de transformação. Carga não será executada.")
        return
        
    # Converte a string JSON de volta para uma estrutura Python (lista de dicionários)
    noticias = json.loads(noticias_json_str)
    
    if not noticias:
        print("A lista de notícias está vazia. Carga não será executada.")
        return
        
    df = pd.DataFrame(noticias)
    
    # IMPORTANTE: Converte as colunas de data de volta para o tipo datetime,
    # pois a conversão para JSON as transforma em texto.
    df['data_publicacao'] = pd.to_datetime(df['data_publicacao'], errors='coerce')
    df['data_extracao'] = pd.to_datetime(df['data_extracao'])
    # Substituímos os pd.NaT por None, que o psycopg2 entende como NULL no SQL.
    # O método .astype(object) é necessário para permitir que a coluna contenha `None`.
    df_para_carga = df.astype(object).where(pd.notnull(df), None)

    # --- Conexão com o Banco de Dados ---
    db_user = 'airflow'
    db_password = 'airflow'
    db_name = 'airflow'
    db_host = 'postgres_db_noticias'
    db_port = '5432'

    conn_params = {
        "host": db_host,
        "database": db_name,
        "user": db_user,
        "password": db_password,
        "port": db_port
    }
    
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                print("Conexão com o PostgreSQL estabelecida.")

                # Prepara a query de inserção com a cláusula de conflito
                query = """
                INSERT INTO noticias_mercado (titulo, link, fonte, data_publicacao, data_extracao, empresas_citadas)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (link) DO NOTHING;
                """

                # Converte o DataFrame para uma lista de tuplas para a inserção
                dados_para_inserir = [tuple(x) for x in df_para_carga.to_numpy()]

                # Executa a inserção em massa
                psycopg2.extras.execute_batch(cur, query, dados_para_inserir)

                # conn.commit() é chamado automaticamente ao sair do bloco 'with'
                print(f"Processo de carga concluído. {cur.rowcount} novas linhas inseridas.")

    except Exception as e:
        print(f"ERRO ao carregar dados para o PostgreSQL: {e}")
        raise