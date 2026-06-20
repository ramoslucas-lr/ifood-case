"""
Regras de negócio e transformações específicas para o dataset NYC TLC Yellow Taxi.
"""

from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from rules.base_rule import TransformationRule


class NYCYellowTaxiRule(TransformationRule):
    """
    Implementa as regras de limpeza, validação de schema e Data Quality para viagens de táxi.
    Trata Schema Drift (Type Drift e Missing Column Drift) e valida lógicas de negócio.
    """

    def apply(self, df: DataFrame) -> DataFrame:
        """
        Aplica o pipeline de transformações e qualidade no DataFrame Bronze.
        
        Processo:
        1. Normalização de colunas (case insensitivity).
        2. Aplicação estrita de Schema (Type Casting) e preenchimento de nulos.
        3. Drop de linhas essenciais nulas.
        4. Filtros de validade de negócio (ex: valores negativos, datas invertidas).
        
        Args:
            df (DataFrame): DataFrame bruto de entrada.
            
        Returns:
            DataFrame: DataFrame higienizado.
        """
        # Normalizar nomes de colunas para minúsculo (evita problemas de case sensitivity)
        df = df.toDF(*[c.lower() for c in df.columns])
        
        # Schema estrito esperado pela camada Silver
        schema_esperado: Dict[str, str] = {
            "vendorid": "long",
            "passenger_count": "double",
            "total_amount": "double",
            "tpep_pickup_datetime": "timestamp",
            "tpep_dropoff_datetime": "timestamp"
        }
        
        # Converter para os tipos corretos para blindar contra Type Drift
        # e preencher com nulo caso a coluna deixe de existir (Missing Column Drift)
        for col_name, col_type in schema_esperado.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(col_type))
            else:
                df = df.withColumn(col_name, lit(None).cast(col_type))
                
        # Selecionar apenas as colunas mapeadas + partições
        df_silver = df.select(*list(schema_esperado.keys()), "ano", "mes")
        
        required_columns: List[str] = [
            "vendorid", 
            "passenger_count", 
            "total_amount", 
            "tpep_pickup_datetime", 
            "tpep_dropoff_datetime"
        ]
        
        # Apply Data Quality Rules
        df_silver = df_silver.dropna(subset=required_columns)
        df_silver = df_silver.filter(col("passenger_count") > 0)
        df_silver = df_silver.filter(col("total_amount") >= 0)
        df_silver = df_silver.filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
        
        return df_silver
