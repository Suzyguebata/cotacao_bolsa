from pyspark.sql.functions import avg, col, count, current_timestamp, date_format, explode, from_json, length, lit, max, min, to_timestamp, trim, window
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType


BRAPI_SCHEMA = StructType([
    StructField("results", ArrayType(
        StructType([
            StructField("symbol", StringType(), True),
            StructField("regularMarketPrice", DoubleType(), True),
            StructField("regularMarketTime", StringType(), True),
            StructField("regularMarketChange", DoubleType(), True),
            StructField("marketCap", DoubleType(), True),
        ])
    ), True),
    StructField("requestedAt", StringType(), True)
])


def transform_kafka_to_bronze(df_kafka):
    return df_kafka.select(
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("json_value")
    ).select(
        "kafka_timestamp",
        from_json(col("json_value"), BRAPI_SCHEMA).alias("data")
    ).select(
        "kafka_timestamp",
        explode(col("data.results")).alias("quote")
    ).select(
        "kafka_timestamp",
        "quote.*"
    ).withColumn("ingestion_timestamp", current_timestamp())


def transform_bronze_to_silver(df_bronze):
    return df_bronze.withColumn(
        "event_timestamp",
        to_timestamp(col("regularMarketTime"))
    ).withColumn(
        "date",
        date_format(col("event_timestamp"), "yyyy-MM-dd")
    ).select(
        "kafka_timestamp",
        col("symbol").alias("ticker"),
        col("regularMarketPrice").cast("double").alias("price"),
        col("regularMarketChange").cast("double").alias("change"),
        col("marketCap").cast("double"),
        "event_timestamp",
        "date",
        "ingestion_timestamp"
    ).filter(
        col("ticker").isNotNull()
        & (length(trim(col("ticker"))) > 0)
        & col("price").isNotNull()
        & (col("price") > 0)
        & col("event_timestamp").isNotNull()
        & col("ingestion_timestamp").isNotNull()
    ).dropDuplicates(
        ["ticker", "event_timestamp", "price"]
    )


def transform_silver_to_gold(df_silver, window_duration="5 minutes", watermark_duration="5 minutes"):
    """Aggregate prices by ingestion-time windows for operational pipeline metrics."""
    return df_silver.withWatermark("ingestion_timestamp", watermark_duration) \
        .groupBy(
            window(col("ingestion_timestamp"), window_duration),
            col("ticker")
        ).agg(
            avg("price").alias("avg_price"),
            min("price").alias("min_price"),
            max("price").alias("max_price"),
            count("price").alias("sample_count")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "ticker",
            "avg_price",
            "min_price",
            "max_price",
            "sample_count"
        ).withColumn(
            "window_basis",
            lit("ingestion_timestamp")
        ).withColumn("calculation_timestamp", current_timestamp())
