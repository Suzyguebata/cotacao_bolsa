import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import datetime

from consumer.transformations import (
    transform_bronze_to_silver,
    transform_bronze_to_quarantine,
    transform_kafka_to_bronze,
    transform_silver_to_gold,
    transform_silver_to_gold_financial,
)

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
        StructField("timestamp", TimestampType(), True),
        StructField("value", StringType(), True)
    ])
    df_input = spark.createDataFrame(kafka_data, schema_kafka)

    # 2. Execução - função usada pelo streaming Bronze
    df_parsed = transform_kafka_to_bronze(df_input)

    # 3. Verificação
    result = df_parsed.collect()[0]
    assert result["symbol"] == "PETR4"
    assert result["regularMarketPrice"] == 35.5
    assert result["json_value"] == kafka_data[0][1]
    assert result["bronze_parse_status"] == "parsed"
    assert "kafka_timestamp" in df_parsed.columns
    assert "ingestion_timestamp" in df_parsed.columns


def test_bronze_preserves_kafka_metadata(spark):
    kafka_data = [
        ("cotacoes", 0, 42, "PETR4", datetime.datetime.now(), '{"results": [{"symbol": "PETR4", "regularMarketPrice": 35.5, "regularMarketTime": "2023-10-27T10:00:00Z"}]}')
    ]
    schema_kafka = StructType([
        StructField("topic", StringType(), True),
        StructField("partition", StringType(), True),
        StructField("offset", StringType(), True),
        StructField("key", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("value", StringType(), True)
    ])
    df_input = spark.createDataFrame(kafka_data, schema_kafka)

    result = transform_kafka_to_bronze(df_input).collect()[0]

    assert result["kafka_topic"] == "cotacoes"
    assert result["kafka_partition"] == 0
    assert result["kafka_offset"] == 42
    assert result["kafka_key"] == "PETR4"


def test_bronze_keeps_invalid_json_for_audit(spark):
    kafka_data = [
        (datetime.datetime.now(), '{"results": [')
    ]
    schema_kafka = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("value", StringType(), True)
    ])
    df_input = spark.createDataFrame(kafka_data, schema_kafka)

    result = transform_kafka_to_bronze(df_input).collect()[0]

    assert result["json_value"] == kafka_data[0][1]
    assert result["bronze_parse_status"] == "parse_error"
    assert result["symbol"] is None

def test_silver_transformation_logic(spark):
    # 1. Setup - Dados simulando a Bronze
    bronze_data = [
        (datetime.datetime.now(), "PETR4", 35.5, "2023-10-27T10:00:00Z", 0.5, 1000000.0, datetime.datetime.now())
    ]
    schema_bronze = StructType([
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("symbol", StringType(), True),
        StructField("regularMarketPrice", DoubleType(), True),
        StructField("regularMarketTime", StringType(), True),
        StructField("regularMarketChange", DoubleType(), True),
        StructField("marketCap", DoubleType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    df_bronze = spark.createDataFrame(bronze_data, schema_bronze)

    # 2. Execução - função usada pelo streaming Silver
    df_silver = transform_bronze_to_silver(df_bronze)

    # 3. Verificação
    result = df_silver.collect()[0]
    assert result["ticker"] == "PETR4"
    assert result["price"] == 35.5
    assert result["change"] == 0.5
    assert result["marketCap"] == 1000000.0
    assert isinstance(result["event_timestamp"], datetime.datetime)
    assert result["date"] == "2023-10-27"


def test_silver_filters_invalid_records(spark):
    now = datetime.datetime.now()
    bronze_data = [
        (now, "PETR4", 35.5, "2023-10-27T10:00:00Z", 0.5, 1000000.0, now),
        (now, None, 35.5, "2023-10-27T10:00:00Z", 0.5, 1000000.0, now),
        (now, "   ", 35.5, "2023-10-27T10:00:00Z", 0.5, 1000000.0, now),
        (now, "VALE3", None, "2023-10-27T10:00:00Z", 0.5, 1000000.0, now),
        (now, "ITUB4", 0.0, "2023-10-27T10:00:00Z", 0.5, 1000000.0, now),
        (now, "BBAS3", -1.0, "2023-10-27T10:00:00Z", 0.5, 1000000.0, now),
        (now, "MGLU3", 2.0, "data-invalida", 0.5, 1000000.0, now),
        (now, "BBDC4", 12.0, "2023-10-27T10:00:00Z", 0.5, 1000000.0, None),
    ]
    schema_bronze = StructType([
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("symbol", StringType(), True),
        StructField("regularMarketPrice", DoubleType(), True),
        StructField("regularMarketTime", StringType(), True),
        StructField("regularMarketChange", DoubleType(), True),
        StructField("marketCap", DoubleType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    df_bronze = spark.createDataFrame(bronze_data, schema_bronze)

    rows = transform_bronze_to_silver(df_bronze).collect()

    assert len(rows) == 1
    assert rows[0]["ticker"] == "PETR4"


def test_quarantine_keeps_rejected_records_with_reasons(spark):
    now = datetime.datetime.now()
    bronze_data = [
        (None, None, None, None, now, '{"results":[{"symbol":"PETR4"}]}', "parsed", "PETR4", 35.5, "2023-10-27T10:00:00Z", 0.5, 1000000.0, now),
        (None, None, None, None, now, '{"results":[{"symbol":null}]}', "parsed", None, 35.5, "2023-10-27T10:00:00Z", 0.5, 1000000.0, now),
        (None, None, None, None, now, '{"results":[{"symbol":"VALE3"}]}', "parsed", "VALE3", -1.0, "2023-10-27T10:00:00Z", 0.5, 1000000.0, now),
        (None, None, None, None, now, '{"results": [', "parse_error", None, None, None, None, None, now),
    ]
    schema_bronze = StructType([
        StructField("kafka_topic", StringType(), True),
        StructField("kafka_partition", StringType(), True),
        StructField("kafka_offset", StringType(), True),
        StructField("kafka_key", StringType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("json_value", StringType(), True),
        StructField("bronze_parse_status", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("regularMarketPrice", DoubleType(), True),
        StructField("regularMarketTime", StringType(), True),
        StructField("regularMarketChange", DoubleType(), True),
        StructField("marketCap", DoubleType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])
    df_bronze = spark.createDataFrame(bronze_data, schema_bronze)

    rows = transform_bronze_to_quarantine(df_bronze).collect()
    reasons = {row["json_value"]: row["rejection_reason"] for row in rows}

    assert len(rows) == 3
    assert reasons['{"results":[{"symbol":null}]}'] == "invalid_ticker"
    assert reasons['{"results":[{"symbol":"VALE3"}]}'] == "invalid_price"
    assert "parse_error" in reasons['{"results": [']


def test_silver_deduplicates_same_quote(spark):
    now = datetime.datetime.now()
    bronze_data = [
        (now, "PETR4", 35.5, "2023-10-27T10:00:00Z", 0.5, 1000000.0, now),
        (now + datetime.timedelta(seconds=1), "PETR4", 35.5, "2023-10-27T10:00:00Z", 0.5, 1000000.0, now + datetime.timedelta(seconds=1)),
        (now, "PETR4", 36.0, "2023-10-27T10:00:00Z", 1.0, 1000000.0, now),
        (now, "PETR4", 35.5, "2023-10-27T10:01:00Z", 0.5, 1000000.0, now),
    ]
    schema_bronze = StructType([
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("symbol", StringType(), True),
        StructField("regularMarketPrice", DoubleType(), True),
        StructField("regularMarketTime", StringType(), True),
        StructField("regularMarketChange", DoubleType(), True),
        StructField("marketCap", DoubleType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    df_bronze = spark.createDataFrame(bronze_data, schema_bronze)

    rows = transform_bronze_to_silver(df_bronze).collect()
    keys = {(row["ticker"], row["event_timestamp"], row["price"]) for row in rows}

    assert len(rows) == 3
    assert len(keys) == 3


def test_gold_transformation_logic(spark):
    ingestion_time = datetime.datetime(2023, 10, 27, 10, 2, 0)
    silver_data = [
        (ingestion_time, "PETR4", 35.0),
        (ingestion_time + datetime.timedelta(minutes=1), "PETR4", 37.0),
        (ingestion_time, "VALE3", 70.0),
    ]
    schema_silver = StructType([
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("ticker", StringType(), True),
        StructField("price", DoubleType(), True),
    ])
    df_silver = spark.createDataFrame(silver_data, schema_silver)

    df_gold = transform_silver_to_gold(df_silver)
    results = {row["ticker"]: row for row in df_gold.collect()}

    assert results["PETR4"]["avg_price"] == 36.0
    assert results["PETR4"]["min_price"] == 35.0
    assert results["PETR4"]["max_price"] == 37.0
    assert results["PETR4"]["sample_count"] == 2
    assert results["PETR4"]["window_basis"] == "ingestion_timestamp"
    assert results["VALE3"]["sample_count"] == 1


def test_financial_gold_uses_event_time_windows(spark):
    event_time = datetime.datetime(2023, 10, 27, 10, 2, 0)
    silver_data = [
        (event_time, datetime.datetime(2023, 10, 27, 10, 7, 0), "PETR4", 35.0),
        (event_time + datetime.timedelta(minutes=1), datetime.datetime(2023, 10, 27, 10, 8, 0), "PETR4", 37.0),
        (event_time, datetime.datetime(2023, 10, 27, 10, 7, 0), "VALE3", 70.0),
    ]
    schema_silver = StructType([
        StructField("event_timestamp", TimestampType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("ticker", StringType(), True),
        StructField("price", DoubleType(), True),
    ])
    df_silver = spark.createDataFrame(silver_data, schema_silver)

    df_gold = transform_silver_to_gold_financial(df_silver)
    results = {row["ticker"]: row for row in df_gold.collect()}

    assert results["PETR4"]["avg_price"] == 36.0
    assert results["PETR4"]["sample_count"] == 2
    assert results["PETR4"]["window_basis"] == "event_timestamp"
    assert results["PETR4"]["window_start"] == datetime.datetime(2023, 10, 27, 10, 0, 0)
    assert results["VALE3"]["sample_count"] == 1
