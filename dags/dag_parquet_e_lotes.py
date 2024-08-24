from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

from airflow.models.baseoperator import chain

# Importando as funções dos arquivos externos
from utils.download_parquet import download_parquet_from_url
from utils.manipular_parquet import manipular_parquet
from utils.verificar_novos_dados import verificar_novos_dados
from utils.carregar_no_postgresql import load_data_from_postgres_to_bigquery

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_parquet_e_lotes',
    default_args=default_args,
    description='DAG que baixa Parquet e processa lotes pendentes a cada 1 hora',
    schedule_interval=timedelta(hours=1),
    catchup=False
) as dag:

    baixar_parquet_task = PythonOperator(
        task_id='baixar_parquet',
        python_callable=download_parquet_from_url,
        op_kwargs={
            'bucket_name': 'desafio-eng-dados',
            'source_blob_name': '2024-03-06.pq',
            'destination_file_name': '/tmp/2024-03-06.pq'
        }
    )

    manipular_parquet_task = PythonOperator(
        task_id='manipular_parquet',
        python_callable=manipular_parquet,
        op_kwargs={'file_path': '/tmp/2024-03-06.pq'}
    )

    verificar_novos_dados_task = PythonOperator(
        task_id='verificar_novos_dados',
        python_callable= verificar_novos_dados
    )
    
     carregar_no_postgresql_task = PythonOperator(
        task_id='load_data_from_postgres_to_bigquery',
        python_callable= load_data_from_postgres_to_bigquery
    )

    baixar_parquet_task >> manipular_parquet_task >> processar_lotes_task >> extrair_dados_task >> transformar_dados_task >> carregar_dados_task