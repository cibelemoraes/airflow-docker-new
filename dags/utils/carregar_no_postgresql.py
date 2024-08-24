import psycopg2
from google.cloud import bigquery


def load_data_from_postgres_to_bigquery(pg_conn_string, bq_dataset, bq_table):
    """
    Carrega dados de um banco de dados PostgreSQL para uma tabela no BigQuery.

    Args:
        pg_conn_string: String de conexão com o PostgreSQL.
        bq_dataset: Nome do dataset no BigQuery.
        bq_table: Nome da tabela no BigQuery.
    """

    # Conectar ao PostgreSQL
    conn = psycopg2.connect(pg_conn_string)
    cursor = conn.cursor()

    # Executar a query SQL para extrair os dados (ajuste a query conforme necessário)
    cursor.execute("SELECT * FROM lotes")
    rows = cursor.fetchall()

    # Transformar os dados em um formato adequado para o BigQuery
    # ... (código para transformar os dados em uma lista de dicionários)

    # Carregar os dados no BigQuery
    client = bigquery.Client()
    table_id = f"{bq_dataset}.{bq_table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = client.load_table_from_dataframe(rows, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete.


# Exemplo de uso
pg_conn_string = "postgresql://admin:admin@localhost:5432/pedrapagamentos"
bq_dataset = "desafio-tecnico-stone.pedrapagamentos"
bq_table = "lotes"

load_data_from_postgres_to_bigquery(pg_conn_string, bq_dataset, bq_table)