"""
Orquestrador central do pipeline Silver -> Gold no Databricks.
Executa de forma genérica o carregamento da partição da Silver,
injeta a SparkSession no Data Mart para máxima flexibilidade e faz o upsert na Gold.
"""

import argparse
import sys
import os
import logging
from pyspark.sql.functions import col

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

cwd = os.getcwd()
current_dir = cwd if cwd.endswith("src/databricks") else os.path.join(cwd, "src", "databricks")
sys.path.insert(0, current_dir)

from pyspark.sql import SparkSession
from common.io_utils import write_delta_upsert
from marts import get_mart


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generic Silver to Gold Pipeline")
    parser.add_argument("--mart-name", required=True, help="Nome lógico do Data Mart a ser gerado (ex: monthly_revenue)")
    parser.add_argument("--silver-path", required=True, help="Caminho absoluto do S3 para ler a Fato da Silver")
    parser.add_argument("--gold-path", required=True, help="Caminho absoluto do S3 para gravar na Gold")
    parser.add_argument("--table-name", required=True, help="Nome da tabela no Unity Catalog/Hive Metastore")
    parser.add_argument("--partition-keys", required=False, default="", help="Chaves de partição em processamento (ex: ano,mes)")
    parser.add_argument("--partition-values", required=False, default="", help="Valores da partição em processamento (ex: 2023,01)")
    return parser.parse_args()


def main() -> None:
    args = get_args()
    
    spark = SparkSession.builder.appName(f"SilverToGold_{args.mart_name}").getOrCreate()
    mart = get_mart(args.mart_name)
    
    silver_path = args.silver_path
    gold_path = args.gold_path
    table_name = args.table_name
    
    part_keys = [k.strip() for k in args.partition_keys.split(",")] if args.partition_keys else []
    part_values = [v.strip() for v in args.partition_values.split(",")] if args.partition_values else []
    
    if len(part_keys) != len(part_values):
        raise ValueError("Quantidade de partition-keys e partition-values não bate.")
        
    try:
        df_fact = spark.read.format("delta").load(silver_path)
    except Exception as e:
        logger.warning(f"Erro ao ler Tabela Silver no caminho '{silver_path}'.")
        logger.warning(f"Detalhes: {str(e)}")
        logger.warning("Finalizando execução com sucesso para evitar quebra da pipeline.")
        return
        
    # Otimização: Pushdown Filter para ler apenas a partição relevante do mês
    for k, v in zip(part_keys, part_values):
        df_fact = df_fact.filter(col(k) == v)
        
    # Injeção da SparkSession e execução livre da regra do Mart
    df_gold = mart.build(spark, df_fact)
    
    if part_keys:
        replace_condition = " AND ".join([f"{k} = '{v}'" for k, v in zip(part_keys, part_values)])
    else:
        replace_condition = None
        
    write_delta_upsert(
        df=df_gold,
        path=gold_path,
        table_name=table_name,
        partition_cols=part_keys,
        replace_condition=replace_condition
    )
    
    logger.info(f"Processamento finalizado com sucesso. Data Mart gravado em {gold_path}")


if __name__ == "__main__":
    main()
