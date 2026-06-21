"""
Orquestrador central do pipeline Raw -> Bronze no Databricks.
Lê os arquivos originais (ex: Parquet) da camada Raw e ingere na camada Bronze
como uma tabela Delta, registrando o timestamp de processamento e as colunas de partição.
"""

import argparse
import sys
import os
import logging
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql.types import IntegerType, FloatType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

cwd = os.getcwd()
current_dir = cwd if cwd.endswith("src/databricks") else os.path.join(cwd, "src", "databricks")
sys.path.insert(0, current_dir)

from pyspark.sql import SparkSession
from common.io_utils import read_parquet, write_delta_upsert


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Raw to Bronze Pipeline")
    parser.add_argument("--dataset", required=True, help="Nome lógico do dataset (ex: yellow)")
    parser.add_argument("--raw-path", required=True, help="Caminho do arquivo na Raw")
    parser.add_argument("--bronze-path", required=True, help="Caminho raiz da tabela na Bronze")
    parser.add_argument("--table-name", required=True, help="Nome da tabela no Catalog")
    parser.add_argument("--partition-keys", required=False, default="", help="Chaves de partição (ex: ano,mes)")
    parser.add_argument("--partition-values", required=False, default="", help="Valores da partição (ex: 2023,01)")
    return parser.parse_args()


def main() -> None:
    args = get_args()
    
    spark = SparkSession.builder.appName(f"RawToBronze_{args.dataset}").getOrCreate()
    
    part_keys = [k.strip() for k in args.partition_keys.split(",")] if args.partition_keys else []
    part_values = [v.strip() for v in args.partition_values.split(",")] if args.partition_values else []
    
    if len(part_keys) != len(part_values):
        raise ValueError("Quantidade de partition-keys e partition-values não bate.")
        
    try:
        df = read_parquet(spark, args.raw_path)
    except Exception as e:
        logger.warning(f"Erro ao ler dados da camada Raw no caminho '{args.raw_path}'.")
        logger.warning(f"Detalhes: {str(e)}")
        logger.warning("Finalizando execução com sucesso para evitar quebra da pipeline.")
        return
        
    # [RESOLUÇÃO DE SCHEMA DRIFT AGNÓSTICA]
    # O dataset da TLC altera tipos (Int vs Long, Float vs Double) ao longo dos meses.
    # Para sermos 100% agnósticos e não fixarmos um schema no código, nós lemos
    # o schema da tabela Delta existente (se houver) e forçamos o DataFrame atual
    # a fazer o cast para os tipos que já estão na tabela.
    df = df.toDF(*[c.lower() for c in df.columns])
    
    try:
        existing_df = spark.table(args.table_name)
        existing_schema = {f.name.lower(): f.dataType for f in existing_df.schema.fields}
        
        for c in df.columns:
            if c in existing_schema:
                df = df.withColumn(c, col(c).cast(existing_schema[c]))
    except Exception:
        # A tabela Bronze ainda não existe (primeira execução), então mantemos os tipos da Raw
        pass

    # Injeta _ingestion_timestamp para auditoria
    df = df.withColumn("_ingestion_timestamp", current_timestamp())
    
    # Injeta colunas de partição dinamicamente
    for k, v in zip(part_keys, part_values):
        df = df.withColumn(k, lit(v))
        
    # Monta condição do replaceWhere dinamicamente (idempotência)
    if part_keys:
        replace_condition = " AND ".join([f"{k} = '{v}'" for k, v in zip(part_keys, part_values)])
    else:
        replace_condition = None
        
    write_delta_upsert(
        df=df,
        path=args.bronze_path,
        table_name=args.table_name,
        partition_cols=part_keys,
        replace_condition=replace_condition
    )
    
    logger.info(f"Processamento Raw->Bronze finalizado. Dados gravados em {args.bronze_path}")


if __name__ == "__main__":
    main()
