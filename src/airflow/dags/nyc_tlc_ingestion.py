import logging
import yaml
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator

DAGS_FOLDER = Path(__file__).parent
CONFIG_PATH = DAGS_FOLDER.parent / "include" / "datasets.yaml"

config = {}
try:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r") as file:
            config = yaml.safe_load(file) or {}
    else:
        logging.warning(f"Arquivo de configuração não encontrado em: {CONFIG_PATH}")
except Exception as e:
    logging.error(f"Erro fatal ao ler o YAML: {e}")

global_settings = config.get("global_settings", {})
s3_bucket = global_settings.get("s3_bucket")
s3_prefix = global_settings.get("s3_bronze_prefix")
datasets = config.get("datasets", [])

with DAG(
    dag_id="nyc_tlc_ingestion",
    start_date = datetime(2023, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["nyc_tlc", "ingestion", "s3"],
) as dag:

    for dataset in datasets:
        dataset_name = dataset.get("name")

        s3_key = f"{s3_prefix}/{dataset_name}/ano={{{{ logical_date.strftime('%Y') }}}}/mes={{{{ logical_date.strftime('%m') }}}}/data.parquet"

        HttpToS3Operator(
            task_id=f"ingest_{dataset_name}_data",
            http_conn_id="nyc_tlc_connection",
            endpoint=dataset.get("endpoint_template"),
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            replace=True,
        )
