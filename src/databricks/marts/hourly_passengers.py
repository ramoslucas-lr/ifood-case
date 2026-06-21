"""
Data Mart: Passageiros por Hora.
Agrega a contagem de passageiros por ano, mês e hora.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum, count, avg, round, hour, col
from marts.base_mart import DataMart


class HourlyPassengersMart(DataMart):
    """
    Calcula as métricas de passageiros por hora a partir da tabela fato da camada Silver.
    """

    def build(self, spark: SparkSession, df_fact: DataFrame) -> DataFrame:
        df_with_hour = df_fact.withColumn("hora", hour(col("tpep_pickup_datetime")))
        
        df_gold = df_with_hour.groupBy("ano", "mes", "hora").agg(
            count("*").alias("total_trips"),
            sum("passenger_count").alias("sum_passengers"),
            round(avg("passenger_count"), 2).alias("avg_passenger_count")
        )
        return df_gold
