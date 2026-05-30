import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, TimestampType
from pyspark.sql.functions import col, to_timestamp, date_format, explode, from_json
import datetime

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("UnitTests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

def test_bronze_transformation_logic(spark):
    # 1. Setup - Dados de exemplo simulando Kafka
    kafka_data = [
        (datetime.datetime.now(), '{"results": [{"symbol": "PETR4", "regularMarketPrice": 35.5, "regularMarketTime": "2023-10-27T10:00:00Z", "regularMarketChange": 0.5, "marketCap": 1000000.0}], "requestedAt": "2023-10-27T10:00:05Z"}')
    ]
    schema_kafka = StructType([
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("json_value", StringType(), True)
    ])
    df_input = spark.createDataFrame(kafka_data, schema_kafka)

    # Schema da Brapi
    brapi_schema = StructType([
        StructField("results", ArrayType(
            StructType([
                StructField("symbol", StringType(), True),
                StructField("regularMarketPrice", DoubleType(), True),
                StructField("regularMarketTime", StringType(), True),
                StructField("regularMarketChange", DoubleType(), True),
                StructField("marketCap", DoubleType(), True),
            ])
        ), True)
    ])

    # 2. Execução - Lógica do spark_consumer.py
    df_parsed = df_input.select(
        "kafka_timestamp",
        from_json(col("json_value"), brapi_schema).alias("data")
    ).select(
        "kafka_timestamp", 
        explode(col("data.results")).alias("quote")
    ).select(
        "kafka_timestamp",
        "quote.*"
    )

    # 3. Verificação
    result = df_parsed.collect()[0]
    assert result["symbol"] == "PETR4"
    assert result["regularMarketPrice"] == 35.5
    assert "kafka_timestamp" in df_parsed.columns

def test_silver_transformation_logic(spark):
    # 1. Setup - Dados simulando a Bronze
    bronze_data = [
        (datetime.datetime.now(), "PETR4", 35.5, "2023-10-27T10:00:00Z", 0.5, 1000000.0)
    ]
    schema_bronze = StructType([
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("symbol", StringType(), True),
        StructField("regularMarketPrice", DoubleType(), True),
        StructField("regularMarketTime", StringType(), True),
        StructField("regularMarketChange", DoubleType(), True),
        StructField("marketCap", DoubleType(), True)
    ])
    df_bronze = spark.createDataFrame(bronze_data, schema_bronze)

    # 2. Execução - Lógica do spark_silver.py
    df_silver = df_bronze.withColumn(
        "event_timestamp", 
        to_timestamp(col("regularMarketTime"))
    ).withColumn(
        "date", 
        date_format(col("event_timestamp"), "yyyy-MM-dd")
    ).select(
        "kafka_timestamp",
        col("symbol").alias("ticker"),
        col("regularMarketPrice").cast("double").alias("price"),
        "event_timestamp",
        "date"
    )

    # 3. Verificação
    result = df_silver.collect()[0]
    assert result["ticker"] == "PETR4"
    assert result["price"] == 35.5
    assert isinstance(result["event_timestamp"], datetime.datetime)
    assert result["date"] == "2023-10-27"
