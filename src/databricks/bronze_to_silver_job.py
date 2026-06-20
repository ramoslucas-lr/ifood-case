import argparse
import sys
import os

# Compatibilidade com Databricks Serverless (onde __file__ é indefinido no ipykernel)
cwd = os.getcwd()
current_dir = cwd if cwd.endswith("src/databricks") else os.path.join(cwd, "src", "databricks")

sys.path.insert(0, current_dir)

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from common.io_utils import read_parquet, write_delta_upsert
from rules import get_rule

def get_args():
    parser = argparse.ArgumentParser(description="Generic Bronze to Silver Pipeline")
    parser.add_argument("--dataset", required=True, help="Nome lógico do dataset (ex: yellow_taxi)")
    parser.add_argument("--ano", required=True, help="Year of the data partition")
    parser.add_argument("--mes", required=True, help="Month of the data partition")
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket name")
    parser.add_argument("--bronze-prefix", required=True, help="Prefix for Bronze layer")
    parser.add_argument("--silver-prefix", required=True, help="Prefix for Silver layer")
    return parser.parse_args()

def main():
    args = get_args()
    
    spark = SparkSession.builder.appName(f"BronzeToSilver_{args.dataset}").getOrCreate()
    
    rule = get_rule(args.dataset)
    
    bronze_path = f"s3a://{args.s3_bucket}/{args.bronze_prefix}/{args.dataset}/ano={args.ano}/mes={args.mes}/data.parquet"
    silver_path = f"s3a://{args.s3_bucket}/{args.silver_prefix}/{args.dataset}/"
    table_name = f"default.silver_nyc_tlc_{args.dataset}"
    
    try:
        df = read_parquet(spark, bronze_path)
    except Exception as e:
        return
        
    df = df.withColumn("ano", lit(args.ano)).withColumn("mes", lit(args.mes))

    df_transformed = rule.apply(df)
    replace_condition = f"ano = '{args.ano}' AND mes = '{args.mes}'"
    
    write_delta_upsert(
        df=df_transformed,
        path=silver_path,
        table_name=table_name,
        partition_cols=["ano", "mes"],
        replace_condition=replace_condition
    )

if __name__ == "__main__":
    main()
