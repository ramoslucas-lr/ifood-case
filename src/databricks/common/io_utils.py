from pyspark.sql import DataFrame, SparkSession

def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)

def write_delta_upsert(
    df: DataFrame, 
    path: str, 
    table_name: str, 
    partition_cols: list, 
    replace_condition: str = None
):
    writer = (df.write
              .format("delta")
              .mode("overwrite")
              .partitionBy(*partition_cols))
    
    if replace_condition:
        writer = writer.option("replaceWhere", replace_condition)
        
    writer.option("path", path).saveAsTable(table_name)
