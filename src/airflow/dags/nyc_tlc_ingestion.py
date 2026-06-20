"""
DAG de Ingestão de Dados NYC TLC.

Este módulo define uma DAG do Apache Airflow responsável por ingerir os dados
de viagens da New York City Taxi and Limousine Commission (NYC TLC). Os dados
são obtidos a partir de uma fonte HTTP pública no formato Parquet e transferidos
diretamente para um bucket S3 (camada Bronze do Data Lake).

A configuração para os datasets (por exemplo, yellow taxi) e as definições de
destino no S3 são carregadas dinamicamente a partir de um arquivo YAML externo
(`datasets.yaml`).

Este design segue as melhores práticas do Airflow ao:
- Separar configurações (YAML) do código fonte da DAG.
- Utilizar a geração dinâmica de tarefas.
- Documentar a DAG na UI do Airflow utilizando a variável `doc_md`.
"""

import logging
from pathlib import Path

import pendulum
import yaml
from airflow import DAG
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# Define os caminhos absolutos para os arquivos de configuração
DAGS_FOLDER = Path(__file__).parent
CONFIG_PATH = DAGS_FOLDER.parent / "include" / "nyc_tlc_datasets.yaml"

config = {}
try:
    # Tenta carregar as configurações do YAML
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file) or {}
    else:
        logging.warning("Arquivo de configuração não encontrado em: %s", CONFIG_PATH)
except Exception as e:
    # Captura e loga erros ao ler o YAML (ex: formato inválido)
    logging.error("Erro fatal ao ler o YAML: %s", e)

# Extrai configurações globais e lista de datasets
global_settings = config.get("global_settings", {})
s3_bucket = global_settings.get("s3_bucket")
s3_prefix = global_settings.get("s3_bronze_prefix")
datasets = config.get("datasets", [])


with DAG(
    dag_id="nyc_tlc_ingestion",
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    schedule="@monthly",
    catchup=False,
    tags=["nyc_tlc", "ingestion", "s3"],
    doc_md=__doc__,  # Exibe a docstring do módulo na UI do Airflow
) as dag:

    # Gera tarefas de ingestão dinamicamente com base no YAML
    for dataset in datasets:
        dataset_name = dataset.get("name")

        # Define a chave de destino no S3 particionada por ano e mês usando Jinja templating
        s3_key = (
            f"{s3_prefix}/{dataset_name}/"
            f"ano={{{{ logical_date.strftime('%Y') }}}}/"
            f"mes={{{{ logical_date.strftime('%m') }}}}/data.parquet"
        )

        ingest_task = HttpToS3Operator(
            task_id=f"ingest_{dataset_name}_data",
            http_conn_id="nyc_tlc_connection",
            endpoint=dataset.get("endpoint_template"),
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            replace=True,
        )

        # Define paths explicitly inside the DAG to keep the Databricks job agnostic
        silver_path = f"s3a://{s3_bucket}/silver/nyc_tlc/{dataset_name}/"
        table_name = f"default.silver_nyc_tlc_{dataset_name}"

        silver_task = DatabricksRunNowOperator(
            task_id=f"silver_{dataset_name}_data",
            databricks_conn_id="databricks_default",
            job_name="NYC TLC: Bronze to Silver Pipeline",
            job_parameters={
                "dataset": dataset_name,
                "bronze_path": f"s3a://{s3_bucket}/{s3_key}",
                "silver_path": silver_path,
                "table_name": table_name,
                "partition_keys": "ano,mes",
                "partition_values": "{{ logical_date.strftime('%Y') }},{{ logical_date.strftime('%m') }}"
            }
        )

        ingest_task >> silver_task
