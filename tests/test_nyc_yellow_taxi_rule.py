import sys
import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampType
from datetime import datetime

# Add src to sys.path so we can import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'databricks')))

from rules.nyc_yellow_taxi import NYCYellowTaxiRule


def test_nyc_yellow_taxi_rule_casing_and_type_drift(spark: SparkSession):
    """
    Tests if the rule correctly lowercases column names and safely casts types.
    """
    schema = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("PASSENGER_count", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("ano", StringType(), True),
        StructField("mes", StringType(), True)
    ])
    
    data = [
        Row(
            VendorID=1, 
            PASSENGER_count=2, 
            total_amount=15.5, 
            tpep_pickup_datetime=datetime(2023, 1, 1, 10, 0, 0),
            tpep_dropoff_datetime=datetime(2023, 1, 1, 10, 30, 0),
            ano="2023",
            mes="01"
        )
    ]
    df = spark.createDataFrame(data, schema)
    
    rule = NYCYellowTaxiRule()
    df_transformed = rule.apply(df)
    
    # Assert columns were lowercased and matched to expected schema
    assert "vendorid" in df_transformed.columns
    assert "passenger_count" in df_transformed.columns
    
    # Assert types were safely cast to the target schema (Integer -> Long, Integer -> Double)
    dtypes = dict(df_transformed.dtypes)
    assert dtypes["vendorid"] == "bigint" # PySpark maps LongType to bigint
    assert dtypes["passenger_count"] == "double"


def test_nyc_yellow_taxi_rule_data_quality_filters(spark: SparkSession):
    """
    Tests data quality rules: drops negative amounts, non-positive passengers, and invalid dates.
    """
    schema = StructType([
        StructField("vendorid", LongType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("ano", StringType(), True),
        StructField("mes", StringType(), True)
    ])
    
    data = [
        # Valid row
        Row(1, 1.0, 10.0, datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 30), "2023", "01"),
        # Invalid passenger_count
        Row(2, 0.0, 10.0, datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 30), "2023", "01"),
        # Invalid total_amount
        Row(3, 1.0, -5.0, datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 30), "2023", "01"),
        # Invalid datetime (dropoff < pickup)
        Row(4, 1.0, 10.0, datetime(2023, 1, 1, 11, 0), datetime(2023, 1, 1, 10, 30), "2023", "01")
    ]
    df = spark.createDataFrame(data, schema)
    
    rule = NYCYellowTaxiRule()
    df_transformed = rule.apply(df)
    
    # Only 1 valid row should remain
    assert df_transformed.count() == 1
    assert df_transformed.collect()[0]["vendorid"] == 1
