"""
Data Mart: Receita Mensal.
Responde à pergunta: Qual a média de valor total (total_amount) recebido em um mês?
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum, count, avg, round
from marts.base_mart import DataMart


class MonthlyRevenueMart(DataMart):
    """
    Agrega a tabela fato por Ano e Mês para gerar o consolidado financeiro.
    """

    def build(self, spark: SparkSession, df_fact: DataFrame) -> DataFrame:
        df_gold = df_fact.groupBy("ano", "mes").agg(
            count("*").alias("total_trips"),
            sum("total_amount").alias("sum_total_amount"),
            round(avg("total_amount"), 2).alias("avg_total_amount")
        )
        return df_gold
