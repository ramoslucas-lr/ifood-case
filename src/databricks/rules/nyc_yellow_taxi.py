from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from rules.base_rule import TransformationRule

class NYCYellowTaxiRule(TransformationRule):
    def apply(self, df: DataFrame) -> DataFrame:
        df = df.toDF(*[c.lower() for c in df.columns])
        
        from pyspark.sql.functions import lit
        
        schema_esperado = {
            "vendorid": "long",
            "passenger_count": "double",
            "total_amount": "double",
            "tpep_pickup_datetime": "timestamp",
            "tpep_dropoff_datetime": "timestamp"
        }
        
        for col_name, col_type in schema_esperado.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(col_type))
            else:
                df = df.withColumn(col_name, lit(None).cast(col_type))
                
        df_silver = df.select(*list(schema_esperado.keys()), "ano", "mes")
        
        required_columns = [
            "vendorid", 
            "passenger_count", 
            "total_amount", 
            "tpep_pickup_datetime", 
            "tpep_dropoff_datetime"
        ]
        
        df_silver = df_silver.dropna(subset=required_columns)
        df_silver = df_silver.filter(col("passenger_count") > 0)
        df_silver = df_silver.filter(col("total_amount") >= 0)
        df_silver = df_silver.filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
        
        return df_silver
