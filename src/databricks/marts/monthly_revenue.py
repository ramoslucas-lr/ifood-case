"""
Data Mart: Receita Mensal.
Agrega a métrica total_amount por ano e mês.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum, count, avg, round
from marts.base_mart import DataMart


class MonthlyRevenueMart(DataMart):
    """
    Calcula as métricas mensais de receita a partir da tabela fato da camada Silver.
    """

    def build(self, spark: SparkSession, df_fact: DataFrame) -> DataFrame:
        df_gold = df_fact.groupBy("ano", "mes").agg(
            count("*").alias("total_trips"),
            sum("total_amount").alias("sum_total_amount"),
            round(avg("total_amount"), 2).alias("avg_total_amount")
        )
        return df_gold
