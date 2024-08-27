![alt text](<include/image/ETL Pipeline com Airflow, PostgreSQL, e BigQuery.jpg>)
#  ETL Pipeline com Airflow, PostgreSQL, e BigQuery

Este projeto implementa um pipeline ETL usando Apache Airflow para processar e mover dados entre Google Cloud Storage (GCS), PostgreSQL e BigQuery.

## Visão Geral
Este pipeline executa as seguintes etapas:

Download de Arquivos Parquet do GCS: Baixa e processa arquivos Parquet do Google Cloud Storage e os converte para CSV.
Carga de Dados no PostgreSQL: Insere os dados CSV em uma tabela PostgreSQL.
Transferência de Dados para o BigQuery: Carrega os dados do PostgreSQL para o BigQuery.
Verificações de Qualidade dos Dados: Executa validações nos dados processados.

## Utilização do Astro
Este projeto foi gerado com o Astronomer CLI e inclui pastas como dags (contendo DAGs do Airflow), include, plugins, 
além dos arquivos Dockerfile, packages.txt, requirements.txt e airflow_settings.yaml. Para rodar o Airflow localmente,
use astro dev start, que inicia contêineres Docker para o banco de dados, servidor web, agendador e triggerer. 
A interface do Airflow estará acessível em http://localhost:8080. Para implantar no Astronomer, consulte a documentação oficial.

## Estrutura do Projeto

DAGs: As DAGs são definidas no Airflow para gerenciar o fluxo de trabalho.
Tasks: As tarefas do pipeline incluem download, transformação e validações.
Conexões: O pipeline utiliza conexões com PostgreSQL e BigQuery.

### Pré-requisitos
Python 3.7+: Certifique-se de ter o Python instalado.
Airflow: Siga as instruções para instalar o Airflow e configurar o ambiente.
Google Cloud SDK: Para acessar o GCS e BigQuery, você precisa do SDK do Google Cloud.

## Instalação

Clone o repositório:
```bash
git clone https://github.com/seu-usuario/seu-repositorio.git
cd seu-repositorio
```
Crie um ambiente virtual e instale as dependências:
```bash
python -m venv venv
source venv/bin/activate  # No Windows: venv\Scripts\activate
pip install -r requirements.txt

```
Instale pacotes adicionais:
```bash
pip install -r packages.txt

```
Configure as credenciais do Google Cloud:
Salve o arquivo de conta de serviço JSON na raiz do projeto e defina a variável de ambiente:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service_account.json"
```
Atualize o arquivo .env com as credenciais do PostgreSQL:

```bash
POSTGRES_USER=seu_usuario
POSTGRES_PASSWORD=sua_senha
POSTGRES_DB=seu_db
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

```
## Execução
Para iniciar o pipeline:
```bash
airflow standalone  # Inicializa o Airflow no modo standalone
```
No Airflow UI, ative a DAG chamada dag_etl_data_process para iniciar o processo.

## Estrutura do Pipeline
O pipeline consiste nas seguintes etapas, organizadas em cadeia:
```bash
chain(
    download_parquet_bucketdef(),
    Conectar_ao_PostgreSQL(),
    importar_csv_para_bd,
    load_data_from_postgres_to_bigquery(),
    check_load(),
    check_transform(),
    check_report()
)

```
## Arquivos de Dependências

requirements.txt: Inclui as bibliotecas essenciais para o projeto.
packages.txt: Lista pacotes adicionais necessários para a execução.

## Atualizando Dependências

Certifique-se de que os arquivos requirements.txt e packages.txt incluam as bibliotecas necessárias. Você pode atualizá-los com:
```bash
pip freeze > requirements.txt

```



