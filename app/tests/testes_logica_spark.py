import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, LongType
import datetime

from consumer.tratamento import (
    transformar_bronze_para_silver,
    transformar_bronze_para_quarentena,
    transformar_kafka_para_bronze,
    transformar_silver_para_gold_operacional,
    transformar_silver_para_gold_financeiro,
)

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("TestesUnitarios") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

def test_logica_transformacao_bronze(spark):
    dados_kafka = [
        (datetime.datetime.now(), '{"results": [{"symbol": "PETR4", "regularMarketPrice": 35.5, "regularMarketTime": "2023-10-27T10:00:00Z", "regularMarketChange": 0.5, "marketCap": 1000000.0}], "requestedAt": "2023-10-27T10:00:05Z"}')
    ]
    schema_kafka = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("value", StringType(), True)
    ])
    df_entrada = spark.createDataFrame(dados_kafka, schema_kafka)

    df_parseado = transformar_kafka_para_bronze(df_entrada)

    resultado = df_parseado.collect()[0]
    assert resultado["symbol"] == "PETR4"
    assert resultado["regularMarketPrice"] == 35.5
    assert resultado["json_bruto"] == dados_kafka[0][1]
    assert resultado["status_parse_bronze"] == "parse_ok"
    assert "data_hora_kafka" in df_parseado.columns
    assert "data_hora_ingestao" in df_parseado.columns


def test_bronze_preserva_metadados_kafka(spark):
    dados_kafka = [
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
    
    df_entrada = spark.createDataFrame(dados_kafka, schema_kafka)

    resultado = transformar_kafka_para_bronze(df_entrada).collect()[0]

    assert resultado["topico_kafka"] == "cotacoes"
    assert resultado["particao_kafka"] == 0
    assert resultado["offset_kafka"] == 42
    assert resultado["chave_kafka"] == "PETR4"


def test_bronze_mantem_json_invalido_para_auditoria(spark):
    dados_kafka = [
        (datetime.datetime.now(), '{"results": [')
    ]
    schema_kafka = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("value", StringType(), True)
    ])
    df_entrada = spark.createDataFrame(dados_kafka, schema_kafka)

    resultado = transformar_kafka_para_bronze(df_entrada).collect()[0]

    assert resultado["json_bruto"] == dados_kafka[0][1]
    assert resultado["status_parse_bronze"] == "erro_parse"
    assert resultado["symbol"] is None

def criar_linha_bronze_completa(
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
    agora = datetime.datetime.now()
    dh_kafka = agora if data_hora_kafka == "SENTINEL" else data_hora_kafka
    dh_ingestao = agora if data_hora_ingestao == "SENTINEL" else data_hora_ingestao
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

def obter_schema_bronze_completo():
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

def test_logica_transformacao_silver(spark):
    dados_bronze = [criar_linha_bronze_completa()]
    df_bronze = spark.createDataFrame(dados_bronze, obter_schema_bronze_completo())

    df_silver = transformar_bronze_para_silver(df_bronze)

    resultado = df_silver.collect()[0]
    assert resultado["ticket_ativo_b3"] == "PETR4"
    assert resultado["valor_atual"] == 35.5
    assert resultado["moeda"] == "BRL"
    assert resultado["variacao_valor_dia_anterior"] == 0.5
    assert resultado["valor_mercado_total"] == 1000000.0
    assert isinstance(resultado["data_hora_atualizacao_valor"], datetime.datetime)
    assert isinstance(resultado["data_hora_processamento_silver"], datetime.datetime)
    assert resultado["ano_mes_dia"] == "2023-10-27"


def test_silver_filtra_registros_invalidos(spark):
    agora = datetime.datetime.now()
    dados_bronze = [
        criar_linha_bronze_completa(symbol="PETR4"),
        criar_linha_bronze_completa(symbol=None),
        criar_linha_bronze_completa(symbol="   "),
        criar_linha_bronze_completa(symbol="VALE3", regularMarketPrice=None),
        criar_linha_bronze_completa(symbol="ITUB4", regularMarketPrice=0.0),
        criar_linha_bronze_completa(symbol="BBAS3", regularMarketPrice=-1.0),
        criar_linha_bronze_completa(symbol="MGLU3", regularMarketTime="data-invalida"),
        criar_linha_bronze_completa(symbol="BBDC4", data_hora_ingestao=None),
    ]
    df_bronze = spark.createDataFrame(dados_bronze, obter_schema_bronze_completo())

    linhas = transformar_bronze_para_silver(df_bronze).collect()

    assert len(linhas) == 1
    assert linhas[0]["ticket_ativo_b3"] == "PETR4"


def test_quarentena_mantem_registros_rejeitados_com_motivos(spark):
    agora = datetime.datetime.now()
    # Nota: quarentena usa um schema de entrada ligeiramente diferente da silver 
    # pois precisa dos metadados brutos do Kafka também.
    
    dados_bronze = [
        (None, None, None, None, agora, '{"results":[{"symbol":"PETR4"}]}', "parse_ok", "PETR4", 35.5, "2023-10-27T10:00:00Z", 0.5, 1000000.0, agora),
        (None, None, None, None, agora, '{"results":[{"symbol":null}]}', "parse_ok", None, 35.5, "2023-10-27T10:00:00Z", 0.5, 1000000.0, agora),
        (None, None, None, None, agora, '{"results":[{"symbol":"VALE3"}]}', "parse_ok", "VALE3", -1.0, "2023-10-27T10:00:00Z", 0.5, 1000000.0, agora),
        (None, None, None, None, agora, '{"results": [', "erro_parse", None, None, None, None, None, agora),
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
    
    df_bronze = spark.createDataFrame(dados_bronze, schema_bronze)
    
    linhas = transformar_bronze_para_quarentena(df_bronze).collect()
    motivos = {linha["json_bruto"]: linha["rejection_reason"] for linha in linhas}

    assert len(linhas) == 3
    assert "ticket_invalido" in motivos['{"results":[{"symbol":null}]}']
    assert "valor_atual_invalido" in motivos['{"results":[{"symbol":"VALE3"}]}']
    assert "erro_parse" in motivos['{"results": [']


def test_silver_deduplica_mesma_cotacao(spark):
    agora = datetime.datetime.now()
    dados_bronze = [
        criar_linha_bronze_completa(symbol="PETR4", regularMarketTime="2023-10-27T10:00:00Z", regularMarketPrice=35.5),
        criar_linha_bronze_completa(symbol="PETR4", regularMarketTime="2023-10-27T10:00:00Z", regularMarketPrice=35.5), # duplicata
        criar_linha_bronze_completa(symbol="PETR4", regularMarketTime="2023-10-27T10:00:00Z", regularMarketPrice=36.0), # preço diferente
        criar_linha_bronze_completa(symbol="PETR4", regularMarketTime="2023-10-27T10:01:00Z", regularMarketPrice=35.5), # horário diferente
    ]
    df_bronze = spark.createDataFrame(dados_bronze, obter_schema_bronze_completo())

    linhas = transformar_bronze_para_silver(df_bronze).collect()
    chaves = {(linha["ticket_ativo_b3"], linha["data_hora_atualizacao_valor"], linha["valor_atual"]) for linha in linhas}

    assert len(linhas) == 3
    assert len(chaves) == 3


def test_logica_transformacao_gold(spark):
    horario_ingestao = datetime.datetime(2023, 10, 27, 10, 2, 0)
    dados_silver = [
        (horario_ingestao, "PETR4", 35.0),
        (horario_ingestao + datetime.timedelta(minutes=1), "PETR4", 37.0),
        (horario_ingestao, "VALE3", 70.0),
    ]
    schema_silver = StructType([
        StructField("data_hora_ingestao", TimestampType(), True),
        StructField("ticket_ativo_b3", StringType(), True),
        StructField("valor_atual", DoubleType(), True),
    ])
    df_silver = spark.createDataFrame(dados_silver, schema_silver)

    df_gold = transformar_silver_para_gold_operacional(df_silver)
    resultados = {linha["ticket_ativo_b3"]: linha for linha in df_gold.collect()}

    assert resultados["PETR4"]["preco_medio_periodo"] == 36.0
    assert resultados["PETR4"]["preco_minimo_periodo"] == 35.0
    assert resultados["PETR4"]["preco_maximo_periodo"] == 37.0
    assert resultados["PETR4"]["quantidade_amostras"] == 2
    assert resultados["PETR4"]["periodo_base"] == "data_hora_ingestao"
    assert resultados["VALE3"]["quantidade_amostras"] == 1


def test_gold_financeira_usa_janelas_tempo_evento(spark):
    horario_atualizacao = datetime.datetime(2023, 10, 27, 10, 2, 0)
    dados_silver = [
        (horario_atualizacao, datetime.datetime(2023, 10, 27, 10, 7, 0), "PETR4", 35.0),
        (horario_atualizacao + datetime.timedelta(minutes=1), datetime.datetime(2023, 10, 27, 10, 8, 0), "PETR4", 37.0),
        (horario_atualizacao, datetime.datetime(2023, 10, 27, 10, 7, 0), "VALE3", 70.0),
    ]
    schema_silver = StructType([
        StructField("data_hora_atualizacao_valor", TimestampType(), True),
        StructField("data_hora_ingestao", TimestampType(), True),
        StructField("ticket_ativo_b3", StringType(), True),
        StructField("valor_atual", DoubleType(), True),
    ])
    df_silver = spark.createDataFrame(dados_silver, schema_silver)

    df_gold = transformar_silver_para_gold_financeiro(df_silver)
    resultados = {linha["ticket_ativo_b3"]: linha for linha in df_gold.collect()}

    assert resultados["PETR4"]["preco_medio_periodo"] == 36.0
    assert resultados["PETR4"]["quantidade_amostras"] == 2
    assert resultados["PETR4"]["periodo_base"] == "data_hora_atualizacao_valor"
    assert resultados["PETR4"]["inicio_periodo"] == datetime.datetime(2023, 10, 27, 10, 0, 0)
    assert resultados["VALE3"]["quantidade_amostras"] == 1
