import sys
import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'databricks')))

from marts.monthly_revenue import MonthlyRevenueMart


def test_monthly_revenue_mart(spark: SparkSession):
    """
    Tests the mathematical aggregation of the Monthly Revenue data mart.
    """
    schema = StructType([
        StructField("ano", StringType(), True),
        StructField("mes", StringType(), True),
        StructField("total_amount", DoubleType(), True)
    ])
    
    data = [
        Row("2023", "01", 10.50),
        Row("2023", "01", 20.00),
        Row("2023", "01", 5.00),
        Row("2023", "02", 50.00)
    ]
    df_fact = spark.createDataFrame(data, schema)
    
    mart = MonthlyRevenueMart()
    df_gold = mart.build(spark, df_fact)
    
    results = {row["mes"]: row for row in df_gold.collect()}
    
    # Assert January aggregations
    assert "01" in results
    assert results["01"]["total_trips"] == 3
    assert results["01"]["sum_total_amount"] == 35.50
    assert results["01"]["avg_total_amount"] == 11.83  # 35.50 / 3 = 11.833... rounded to 2
    
    # Assert February aggregations
    assert "02" in results
    assert results["02"]["total_trips"] == 1
    assert results["02"]["sum_total_amount"] == 50.00
    assert results["02"]["avg_total_amount"] == 50.00
