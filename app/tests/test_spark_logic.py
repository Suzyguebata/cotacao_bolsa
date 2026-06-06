import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, LongType
import datetime

from app.consumer.tratamento import (
    transformar_bronze_para_silver,
    transformar_bronze_para_quarantena,
    transformar_kafka_para_bronze,
    transformar_silver_para_gold_operacional,
    transformar_silver_para_gold_financeiro,
)

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("UnitTests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

def test_bronze_transformation_logic(spark):
    kafka_data = [
        (datetime.datetime.now(), '{"results": [{"symbol": "PETR4", "regularMarketPrice": 35.5, "regularMarketTime": "2023-10-27T10:00:00Z", "regularMarketChange": 0.5, "marketCap": 1000000.0}], "requestedAt": "2023-10-27T10:00:05Z"}')
    ]
    schema_kafka = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("value", StringType(), True)
    ])
    df_input = spark.createDataFrame(kafka_data, schema_kafka)

    df_parsed = transformar_kafka_para_bronze(df_input)

    result = df_parsed.collect()[0]
    assert result["symbol"] == "PETR4"
    assert result["regularMarketPrice"] == 35.5
    assert result["json_bruto"] == kafka_data[0][1]
    assert result["status_parse_bronze"] == "parse_ok"
    assert "data_hora_kafka" in df_parsed.columns
    assert "data_hora_ingestao" in df_parsed.columns


def test_bronze_preserves_kafka_metadata(spark):
    kafka_data = [
        ("cotacoes", 0, 42, "PETR4", datetime.datetime.now(), '{"results": [{"symbol": "PETR4", "regularMarketPrice": 35.5, "regularMarketTime": "2023-10-27T10:00:00Z"}]}')
    ]
    schema_kafka = StructType([
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("key", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("value", StringType(), True)
    ])
    
    df_input = spark.createDataFrame(kafka_data, schema_kafka)

    result = transformar_kafka_para_bronze(df_input).collect()[0]

    assert result["topico_kafka"] == "cotacoes"
    assert result["particao_kafka"] == 0
    assert result["offset_kafka"] == 42
    assert result["chave_kafka"] == "PETR4"


def test_bronze_keeps_invalid_json_for_audit(spark):
    kafka_data = [
        (datetime.datetime.now(), '{"results": [')
    ]
    schema_kafka = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("value", StringType(), True)
    ])
    df_input = spark.createDataFrame(kafka_data, schema_kafka)

    result = transformar_kafka_para_bronze(df_input).collect()[0]

    assert result["json_bruto"] == kafka_data[0][1]
    assert result["status_parse_bronze"] == "erro_parse"
    assert result["symbol"] is None

def create_full_bronze_row(
    data_hora_kafka="SENTINEL",
    symbol="PETR4",
    shortName="PETR4",
    longName="Petroleo Brasileiro SA Pfd",
    currency="BRL",
    regularMarketPrice=35.5,
    regularMarketDayHigh=36.0,
    regularMarketDayLow=34.0,
    regularMarketDayRange="34.0 - 36.0",
    regularMarketChange=0.5,
    regularMarketChangePercent=1.4,
    regularMarketTime="2023-10-27T10:00:00Z",
    marketCap=1000000.0,
    regularMarketVolume=1000000,
    regularMarketPreviousClose=35.0,
    regularMarketOpen=35.1,
    fiftyTwoWeekRange="20.0 - 40.0",
    fiftyTwoWeekLow=20.0,
    fiftyTwoWeekHigh=40.0,
    priceEarnings=5.0,
    earningsPerShare=7.0,
    logourl="http://logo.com",
    data_hora_ingestao="SENTINEL"
):
    now = datetime.datetime.now()
    dh_kafka = now if data_hora_kafka == "SENTINEL" else data_hora_kafka
    dh_ingestao = now if data_hora_ingestao == "SENTINEL" else data_hora_ingestao
    return (
        dh_kafka,
        symbol,
        shortName,
        longName,
        currency,
        regularMarketPrice,
        regularMarketDayHigh,
        regularMarketDayLow,
        regularMarketDayRange,
        regularMarketChange,
        regularMarketChangePercent,
        regularMarketTime,
        marketCap,
        regularMarketVolume,
        regularMarketPreviousClose,
        regularMarketOpen,
        fiftyTwoWeekRange,
        fiftyTwoWeekLow,
        fiftyTwoWeekHigh,
        priceEarnings,
        earningsPerShare,
        logourl,
        dh_ingestao
    )

def get_full_bronze_schema():
    return StructType([
        StructField("data_hora_kafka", TimestampType(), True),
        StructField("symbol", StringType(), True),
        StructField("shortName", StringType(), True),
        StructField("longName", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("regularMarketPrice", DoubleType(), True),
        StructField("regularMarketDayHigh", DoubleType(), True),
        StructField("regularMarketDayLow", DoubleType(), True),
        StructField("regularMarketDayRange", StringType(), True),
        StructField("regularMarketChange", DoubleType(), True),
        StructField("regularMarketChangePercent", DoubleType(), True),
        StructField("regularMarketTime", StringType(), True),
        StructField("marketCap", DoubleType(), True),
        StructField("regularMarketVolume", IntegerType(), True),
        StructField("regularMarketPreviousClose", DoubleType(), True),
        StructField("regularMarketOpen", DoubleType(), True),
        StructField("fiftyTwoWeekRange", StringType(), True),
        StructField("fiftyTwoWeekLow", DoubleType(), True),
        StructField("fiftyTwoWeekHigh", DoubleType(), True),
        StructField("priceEarnings", DoubleType(), True),
        StructField("earningsPerShare", DoubleType(), True),
        StructField("logourl", StringType(), True),
        StructField("data_hora_ingestao", TimestampType(), True)
    ])

def test_silver_transformation_logic(spark):
    bronze_data = [create_full_bronze_row()]
    df_bronze = spark.createDataFrame(bronze_data, get_full_bronze_schema())

    df_silver = transformar_bronze_para_silver(df_bronze)

    result = df_silver.collect()[0]
    assert result["ticket_ativo_b3"] == "PETR4"
    assert result["valor_atual"] == 35.5
    assert result["variacao_valor_dia_anterior"] == 0.5
    assert result["valor_mercado_total"] == 1000000.0
    assert isinstance(result["data_hora_atualizacao"], datetime.datetime)
    assert result["data"] == "2023-10-27"


def test_silver_filters_invalid_records(spark):
    now = datetime.datetime.now()
    bronze_data = [
        create_full_bronze_row(symbol="PETR4"),
        create_full_bronze_row(symbol=None),
        create_full_bronze_row(symbol="   "),
        create_full_bronze_row(symbol="VALE3", regularMarketPrice=None),
        create_full_bronze_row(symbol="ITUB4", regularMarketPrice=0.0),
        create_full_bronze_row(symbol="BBAS3", regularMarketPrice=-1.0),
        create_full_bronze_row(symbol="MGLU3", regularMarketTime="data-invalida"),
        create_full_bronze_row(symbol="BBDC4", data_hora_ingestao=None),
    ]
    df_bronze = spark.createDataFrame(bronze_data, get_full_bronze_schema())

    rows = transformar_bronze_para_silver(df_bronze).collect()

    assert len(rows) == 1
    assert rows[0]["ticket_ativo_b3"] == "PETR4"


def test_quarantine_keeps_rejected_records_with_reasons(spark):
    now = datetime.datetime.now()
    # Note: quarantine uses a slightly different input schema from silver 
    # as it needs the raw Kafka metadata too.
    
    bronze_data = [
        (None, None, None, None, now, '{"results":[{"symbol":"PETR4"}]}', "parse_ok", "PETR4", 35.5, "2023-10-27T10:00:00Z", 0.5, 1000000.0, now),
        (None, None, None, None, now, '{"results":[{"symbol":null}]}', "parse_ok", None, 35.5, "2023-10-27T10:00:00Z", 0.5, 1000000.0, now),
        (None, None, None, None, now, '{"results":[{"symbol":"VALE3"}]}', "parse_ok", "VALE3", -1.0, "2023-10-27T10:00:00Z", 0.5, 1000000.0, now),
        (None, None, None, None, now, '{"results": [', "erro_parse", None, None, None, None, None, now),
    ]
    schema_bronze = StructType([
        StructField("topico_kafka", StringType(), True),
        StructField("particao_kafka", IntegerType(), True),
        StructField("offset_kafka", LongType(), True),
        StructField("chave_kafka", StringType(), True),
        StructField("data_hora_kafka", TimestampType(), True),
        StructField("json_bruto", StringType(), True),
        StructField("status_parse_bronze", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("regularMarketPrice", DoubleType(), True),
        StructField("regularMarketTime", StringType(), True),
        StructField("regularMarketChange", DoubleType(), True),
        StructField("marketCap", DoubleType(), True),
        StructField("data_hora_ingestao", TimestampType(), True),
    ])
    
    df_bronze = spark.createDataFrame(bronze_data, schema_bronze)
    
    rows = transformar_bronze_para_quarantena(df_bronze).collect()
    reasons = {row["json_bruto"]: row["rejection_reason"] for row in rows}

    assert len(rows) == 3
    assert "ticket_invalido" in reasons['{"results":[{"symbol":null}]}']
    assert "valor_atual_invalido" in reasons['{"results":[{"symbol":"VALE3"}]}']
    assert "erro_parse" in reasons['{"results": [']


def test_silver_deduplicates_same_quote(spark):
    now = datetime.datetime.now()
    bronze_data = [
        create_full_bronze_row(symbol="PETR4", regularMarketTime="2023-10-27T10:00:00Z", regularMarketPrice=35.5),
        create_full_bronze_row(symbol="PETR4", regularMarketTime="2023-10-27T10:00:00Z", regularMarketPrice=35.5), # duplicate
        create_full_bronze_row(symbol="PETR4", regularMarketTime="2023-10-27T10:00:00Z", regularMarketPrice=36.0), # different price
        create_full_bronze_row(symbol="PETR4", regularMarketTime="2023-10-27T10:01:00Z", regularMarketPrice=35.5), # different time
    ]
    df_bronze = spark.createDataFrame(bronze_data, get_full_bronze_schema())

    rows = transformar_bronze_para_silver(df_bronze).collect()
    keys = {(row["ticket_ativo_b3"], row["data_hora_atualizacao"], row["valor_atual"]) for row in rows}

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
        StructField("data_hora_ingestao", TimestampType(), True),
        StructField("ticket_ativo_b3", StringType(), True),
        StructField("valor_atual", DoubleType(), True),
    ])
    df_silver = spark.createDataFrame(silver_data, schema_silver)

    df_gold = transformar_silver_para_gold_operacional(df_silver)
    results = {row["ticket_ativo_b3"]: row for row in df_gold.collect()}

    assert results["PETR4"]["preco_medio_periodo"] == 36.0
    assert results["PETR4"]["preco_minimo_periodo"] == 35.0
    assert results["PETR4"]["preco_maximo_periodo"] == 37.0
    assert results["PETR4"]["quantidade_amostras"] == 2
    assert results["PETR4"]["periodo_base"] == "data_hora_ingestao"
    assert results["VALE3"]["quantidade_amostras"] == 1


def test_financial_gold_uses_update_time_windows(spark):
    update_time = datetime.datetime(2023, 10, 27, 10, 2, 0)
    silver_data = [
        (update_time, datetime.datetime(2023, 10, 27, 10, 7, 0), "PETR4", 35.0),
        (update_time + datetime.timedelta(minutes=1), datetime.datetime(2023, 10, 27, 10, 8, 0), "PETR4", 37.0),
        (update_time, datetime.datetime(2023, 10, 27, 10, 7, 0), "VALE3", 70.0),
    ]
    schema_silver = StructType([
        StructField("data_hora_atualizacao", TimestampType(), True),
        StructField("data_hora_ingestao", TimestampType(), True),
        StructField("ticket_ativo_b3", StringType(), True),
        StructField("valor_atual", DoubleType(), True),
    ])
    df_silver = spark.createDataFrame(silver_data, schema_silver)

    df_gold = transformar_silver_para_gold_financeiro(df_silver)
    results = {row["ticket_ativo_b3"]: row for row in df_gold.collect()}

    assert results["PETR4"]["preco_medio_periodo"] == 36.0
    assert results["PETR4"]["quantidade_amostras"] == 2
    assert results["PETR4"]["periodo_base"] == "data_hora_atualizacao"
    assert results["PETR4"]["inicio_periodo"] == datetime.datetime(2023, 10, 27, 10, 0, 0)
    assert results["VALE3"]["quantidade_amostras"] == 1
