from pyspark.sql.functions import avg, col, concat_ws, count, current_timestamp, date_format, explode_outer, from_json, length, lit, max, min, size, to_timestamp, trim, when, window
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType,IntegerType


BRAPI_SCHEMA = StructType([
    StructField("results", ArrayType(
        StructType([
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
        ])
    ), True),
    StructField("requestedAt", StringType(), True),
    StructField("took", IntegerType(), True),
    StructField("_registro_corrompido", StringType(), True),
])


def _optional_column(df, column_name, data_type=None):
    if column_name in df.columns:
        value = col(column_name)
        if data_type:
            value = value.cast(data_type)
        return value
    value = lit(None)
    if data_type:
        value = value.cast(data_type)
    return value


def transformar_kafka_para_bronze(df_kafka):

    df_bruto = df_kafka.select(
        _optional_column(df_kafka, "topic", "string").alias("topico_kafka"),
        _optional_column(df_kafka, "partition", "int").alias("particao_kafka"),
        _optional_column(df_kafka, "offset", "long").alias("offset_kafka"),
        _optional_column(df_kafka, "key").cast("string").alias("chave_kafka"),
        col("timestamp").alias("data_hora_kafka"),
        col("value").cast("string").alias("json_bruto"),
    ).withColumn(
        "dados_parseados",
        from_json(
            col("json_bruto"),
            BRAPI_SCHEMA,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_registro_corrompido"}
        )
    ).withColumn(
        "status_parse_bronze",
        when(col("dados_parseados").isNull() | col("dados_parseados._registro_corrompido").isNotNull(), lit("erro_parse"))
        .when(col("dados_parseados.results").isNull() | (size(col("dados_parseados.results")) == 0), lit("sem_resultado"))
        .otherwise(lit("parse_ok"))
    ).withColumn(
        "data_hora_ingestao",
        current_timestamp()
    )

    return df_bruto.select(
        "topico_kafka",
        "particao_kafka",
        "offset_kafka",
        "chave_kafka",
        "data_hora_kafka",
        "json_bruto",
        "status_parse_bronze",
        "data_hora_ingestao",
        explode_outer(col("dados_parseados.results")).alias("cotacao")
    ).select(
        "topico_kafka",
        "particao_kafka",
        "offset_kafka",
        "chave_kafka",
        "data_hora_kafka",
        "json_bruto",
        "status_parse_bronze",
        "data_hora_ingestao",
        "cotacao.*"
    )


def transformar_bronze_para_silver(df_bronze, watermark_duration="15 minutes"):
    df_prepared = df_bronze.withColumn(
        "data_hora_evento",
        to_timestamp(col("regularMarketTime"))
    ).withColumn(
        "data",
        date_format(col("data_hora_evento"), "yyyy-MM-dd")
    ).withWatermark("data_hora_evento", watermark_duration)

    return df_prepared.select(
        "data_hora_kafka",
        col("symbol").alias("ticket_ativo_b3"),
        col("shortName").alias("nome_abreviado_empresa"),
        col("longName").alias("nome_empresa_completo"),
        col("currency").cast("double").alias("moeda"),
        col("regularMarketPrice").cast("double").alias("valor_atual"),
        col("regularMarketDayHigh").cast("double").alias("valor_maximo_diario"),
        col("regularMarketDayLow").cast("double").alias("valor_minimo_diario"),
        col("regularMarketDayRange").alias("faixa_valor_diario"),
        col("regularMarketChange").cast("double").alias("variacao_valor_dia_anterior"),
        col("regularMarketChangePercent").cast("double").alias("variacao_percentual_valor_dia"),
        col("regularMarketTime").cast("timestamp").alias("data_hora_atualizacao"),
        col("marketCap").cast("double").alias("valor_mercado_total"),
        col("regularMarketVolume").cast("long").alias("volume_negocios"),
        col("regularMarketPreviousClose").cast("double").alias("valor_fechamento_anterior"),
        col("regularMarketOpen").cast("double").alias("valor_abertura"),
        col("fiftyTwoWeekRange").alias("variacao_valor_52_semanas"),
        col("fiftyTwoWeekLow").cast("double").alias("valor_minimo_52_semanas"),
        col("fiftyTwoWeekHigh").cast("double").alias("valor_maximo_52_semanas"),
        col("priceEarnings").cast("double").alias("indicador_lucro"),
        col("earningsPerShare").cast("double").alias("lucro_por_acao"),
        col("logourl").alias("url_logo_do_ativo"),
        "data_hora_evento",  
        "data",
        "data_hora_ingestao"
    ).filter(
        col("ticket_ativo_b3").isNotNull()
        & (length(trim(col("ticket_ativo_b3"))) > 0)
        & col("valor_atual").isNotNull()
        & (col("valor_atual") > 0)
        & col("data_hora_evento").isNotNull()
        & col("data_hora_ingestao").isNotNull()
    ).dropDuplicates(
        ["ticket_ativo_b3", "data_hora_evento", "valor_atual"]
    )


def transformar_bronze_para_quarantena(df_bronze):
    status_parse_bronze = _optional_column(df_bronze, "status_parse_bronze", "string")

    df_preparaco = df_bronze.withColumn(
        "data_hora_evento",
        to_timestamp(col("regularMarketTime"))
    ).withColumn(
        "valor_atual",
        col("regularMarketPrice").cast("double")
    ).withColumn(
        "ticket_ativo_b3",
        col("symbol")
    )

    return df_preparaco.withColumn(
        "rejection_reason",
        concat_ws(
            ",",
            when(status_parse_bronze == "erro_parse", lit("erro_parse")),
            when(status_parse_bronze == "sem_resultado", lit("sem_resultado")),
            when(col("ticket_ativo_b3").isNull() | (length(trim(col("ticket_ativo_b3"))) == 0), lit("ticket_invalido")),
            when(col("valor_atual").isNull() | (col("valor_atual") <= 0), lit("valor_atual_invalido")),
            when(col("data_hora_evento").isNull(), lit("data_hora_evento_invalida")),
            when(col("data_hora_ingestao").isNull(), lit("data_hora_ingestao_invalida"))
        )
    ).filter(
        length(col("rejection_reason")) > 0
    ).select(
        _optional_column(df_preparaco, "topico_kafka", "string").alias("topico_kafka"),
        _optional_column(df_preparaco, "particao_kafka", "int").alias("particao_kafka"),
        _optional_column(df_preparaco, "offset_kafka", "long").alias("offset_kafka"),
        _optional_column(df_preparaco, "chave_kafka", "string").alias("chave_kafka"),
        "data_hora_kafka",
        _optional_column(df_preparaco, "json_bruto", "string").alias("json_bruto"),
        _optional_column(df_preparaco, "status_parse_bronze", "string").alias("status_parse_bronze"),
        "symbol",
        "regularMarketPrice",
        "regularMarketTime",
        "regularMarketChange",
        "marketCap",
        "data_hora_evento",
        "data_hora_ingestao",
        "rejection_reason"
    )


def transformar_silver_para_gold_operacional(df_silver, window_duration="5 minutes", watermark_duration="5 minutes"):
    """Agrega preços por janelas de tempo baseadas na ingestão para métricas operacionais do pipeline."""
    return df_silver.withWatermark("data_hora_ingestao", watermark_duration) \
        .groupBy(
            window(col("data_hora_ingestao"), window_duration),
            col("ticket_ativo_b3")
        ).agg(
            avg("valor_atual").alias("preco_medio_periodo"),
            min("valor_atual").alias("preco_minimo_periodo"),
            max("valor_atual").alias("preco_maximo_periodo"),
            count("valor_atual").alias("quantidade_amostras")
        ).select(
            col("window.start").alias("inicio_periodo"),
            col("window.end").alias("fim_periodo"),
            "ticket_ativo_b3",
            "preco_medio_periodo",
            "preco_minimo_periodo",
            "preco_maximo_periodo",
            "quantidade_amostras"
        ).withColumn(
            "periodo_base",
            lit("data_hora_ingestao")
        ).withColumn("data_hora_processamento", current_timestamp())


def transformar_silver_para_gold_financeiro(df_silver, window_duration="5 minutes", watermark_duration="10 minutes"):
    """Agrega preços por períodos baseados no tempo do evento para análise financeira."""
    return df_silver.withWatermark("data_hora_evento", watermark_duration) \
        .groupBy(
            window(col("data_hora_evento"), window_duration),
            col("ticket_ativo_b3")
        ).agg(
            avg("valor_atual").alias("preco_medio_periodo"),
            min("valor_atual").alias("preco_minimo_periodo"),
            max("valor_atual").alias("preco_maximo_periodo"),
            count("valor_atual").alias("quantidade_amostras")
        ).select(
            col("window.start").alias("inicio_periodo"),
            col("window.end").alias("fim_periodo"),
            "ticket_ativo_b3",
            "preco_medio_periodo",
            "preco_minimo_periodo",
            "preco_maximo_periodo",
            "quantidade_amostras"
        ).withColumn(
            "periodo_base",
            lit("data_hora_evento")
        ).withColumn("data_hora_processamento", current_timestamp())


def transformar_silver_para_gold(df_silver, window_duration="5 minutes", watermark_duration="5 minutes"):
    return transformar_silver_para_gold_operacional(df_silver, window_duration, watermark_duration)