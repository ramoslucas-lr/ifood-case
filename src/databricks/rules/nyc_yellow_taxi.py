"""
Regras de negócio e transformações específicas para o dataset NYC TLC Yellow Taxi.
"""

from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from rules.base_rule import TransformationRule


class NYCYellowTaxiRule(TransformationRule):
    """
    Regra de transformação para o dataset NYC TLC Yellow Taxi.

    Aplica normalização de colunas, cast rigoroso de schema e filtros de qualidade de dados.
    """

    def apply(self, df: DataFrame) -> DataFrame:
        """
        Aplica as transformações no DataFrame Bronze para prepará-lo para a camada Silver.
        
        Args:
            df (DataFrame): DataFrame de entrada da camada Bronze.
            
        Returns:
            DataFrame: DataFrame transformado.
        """
        df = df.toDF(*[c.lower() for c in df.columns])
        
        schema_esperado: Dict[str, str] = {
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
        
        required_columns: List[str] = [
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
