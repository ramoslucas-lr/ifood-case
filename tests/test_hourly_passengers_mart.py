import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'databricks')))
from pyspark.sql import SparkSession
from pyspark.sql import Row

from marts.hourly_passengers import HourlyPassengersMart

def test_hourly_passengers_mart(spark: SparkSession):
    """
    Testa a agregação do Data Mart de Passageiros por Hora.
    Verifica se a contagem de passageiros é agrupada corretamente pelas janelas de ano, mês e hora.
    """
    # Cria os dados mockados da tabela fato (camada Silver)
    data = [
        Row(
            ano="2023", mes="01",
            tpep_pickup_datetime=datetime(2023, 1, 1, 14, 10, 0),
            passenger_count=2.0
        ),
        Row(
            ano="2023", mes="01",
            tpep_pickup_datetime=datetime(2023, 1, 1, 14, 45, 0),
            passenger_count=1.0
        ),
        Row(
            ano="2023", mes="01",
            tpep_pickup_datetime=datetime(2023, 1, 1, 15, 5, 0),
            passenger_count=4.0
        )
    ]
    df_fact = spark.createDataFrame(data)
    
    # Executa a construção do mart
    mart = HourlyPassengersMart()
    df_result = mart.build(spark, df_fact)
    
    # Coleta e ordena os resultados para asserção
    results = [row.asDict() for row in df_result.collect()]
    results.sort(key=lambda x: x["hora"])
    
    assert len(results) == 2, "A agregação deveria gerar apenas 2 janelas de horas (14h e 15h)"
    
    # Asserções da janela das 14h
    assert results[0]["ano"] == "2023"
    assert results[0]["mes"] == "01"
    assert results[0]["hora"] == 14
    assert results[0]["sum_passengers"] == 3.0  # 2.0 + 1.0 passageiros
    
    # Asserções da janela das 15h
    assert results[1]["ano"] == "2023"
    assert results[1]["mes"] == "01"
    assert results[1]["hora"] == 15
    assert results[1]["sum_passengers"] == 4.0
