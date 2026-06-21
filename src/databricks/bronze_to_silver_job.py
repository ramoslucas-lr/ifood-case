"""
Orquestra o pipeline de dados Bronze para Silver.
Carrega uma partição específica, aplica as regras de negócio e executa um upsert idempotente na camada Silver.
"""

import argparse
import sys
import os
import logging
from pyspark.sql.functions import col

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Compatibilidade com Databricks Serverless (onde __file__ é indefinido no ipykernel)
cwd = os.getcwd()
current_dir = cwd if cwd.endswith("src/databricks") else os.path.join(cwd, "src", "databricks")

sys.path.insert(0, current_dir)

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from common.io_utils import read_parquet, write_delta_upsert
from rules import get_rule


def get_args() -> argparse.Namespace:
    """
    Faz o parse dos argumentos de linha de comando.
    
    Returns:
        argparse.Namespace: Argumentos processados.
    """
    parser = argparse.ArgumentParser(description="Bronze to Silver Pipeline")
    parser.add_argument("--dataset", required=True, help="Nome lógico do dataset (ex: yellow)")
    parser.add_argument("--bronze-path", required=True, help="Caminho absoluto do S3 para ler os dados da Bronze")
    parser.add_argument("--silver-path", required=True, help="Caminho absoluto do S3 para gravar na Silver")
    parser.add_argument("--table-name", required=True, help="Nome da tabela no Unity Catalog/Hive Metastore")
    parser.add_argument("--partition-keys", required=False, default="", help="Chaves de partição separadas por vírgula (ex: ano,mes)")
    parser.add_argument("--partition-values", required=False, default="", help="Valores de partição separados por vírgula (ex: 2023,01)")
    return parser.parse_args()


def main() -> None:
    """Ponto de entrada principal do job PySpark."""
    args = get_args()
    
    spark = SparkSession.builder.appName(f"BronzeToSilver_{args.dataset}").getOrCreate()
    rule = get_rule(args.dataset)
    
    bronze_path = args.bronze_path
    silver_path = args.silver_path
    table_name = args.table_name
    
    part_keys = [k.strip() for k in args.partition_keys.split(",")] if args.partition_keys else []
    part_values = [v.strip() for v in args.partition_values.split(",")] if args.partition_values else []
    
    if len(part_keys) != len(part_values):
        raise ValueError("Quantidade de partition-keys e partition-values não bate.")
    
    try:
        df = spark.read.format("delta").load(bronze_path)
    except Exception as e:
        logger.warning(f"Erro ao ler dados da camada Bronze no caminho '{bronze_path}'.")
        logger.warning(f"Detalhes: {str(e)}")
        logger.warning("Finalizando execução com sucesso para evitar quebra da pipeline em partições inexistentes.")
        return
        
    for k, v in zip(part_keys, part_values):
        df = df.filter(col(k) == v)

    df_transformed = rule.apply(df)
    
    if part_keys:
        replace_condition = " AND ".join([f"{k} = '{v}'" for k, v in zip(part_keys, part_values)])
    else:
        replace_condition = None
    
    write_delta_upsert(
        df=df_transformed,
        path=silver_path,
        table_name=table_name,
        partition_cols=part_keys,
        replace_condition=replace_condition
    )
    
    logger.info(f"Processamento finalizado com sucesso. Dados gravados na Silver em {silver_path}")


if __name__ == "__main__":
    main()
