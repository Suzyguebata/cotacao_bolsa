from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, min, max, count, current_timestamp
import os

import time
from minio import Minio

# Configurações de Caminhos (Ajustadas para Docker)
MINIO_ENDPOINT_URL = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"

SILVER_PATH = "s3a://datalake/silver/cotacoes"
GOLD_PATH = "s3a://datalake/gold/media_precos_5min"
CHECKPOINT_GOLD = "s3a://datalake/checkpoints/gold_cotacoes"

# Aguardar a Silver ser inicializada
print("Aguardando inicialização da camada Silver...")
client = Minio(MINIO_ENDPOINT_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
while not client.bucket_exists("datalake") or len(list(client.list_objects("datalake", prefix="silver/cotacoes/_delta_log/"))) == 0:
    time.sleep(5)
print("Camada Silver detectada. Iniciando Gold...")


# Inicialização da Sessão Spark
spark = SparkSession.builder \
    .appName("Silver_to_Gold_Aggregations") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT_URL}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 1. Leitura em Stream da Camada Silver
df_silver = spark.readStream \
    .format("delta") \
    .load(SILVER_PATH)

# 2. Transformações da Camada Gold (Agregações de Negócio)
# Calculamos métricas em janelas de 5 minutos para cada ticker
# NOTA: Usando ingestion_timestamp para o teste integrado pois event_timestamp da API é estático
df_gold = df_silver \
    .withWatermark("ingestion_timestamp", "1 minute") \
    .groupBy(
        window(col("ingestion_timestamp"), "1 minute"),
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
    ).withColumn("calculation_timestamp", current_timestamp())

# 3. Escrita na Camada Gold
# Nota: Para agregações em streaming que salvam em Delta, usamos o outputMode("complete") ou "append" dependendo da lógica.
# Como estamos usando janelas com watermark, usamos "append" para salvar apenas janelas fechadas.
query = df_gold.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_GOLD) \
    .start(GOLD_PATH)

print(f"Processamento Gold iniciado. Gravando agregados em: {GOLD_PATH}")
query.awaitTermination()
