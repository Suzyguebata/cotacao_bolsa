from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_unixtime, date_format
import os

import time
from minio import Minio

# Configurações (Ajustadas para rodar dentro do Docker)
MINIO_ENDPOINT_URL = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"

BRONZE_PATH = "s3a://datalake/bronze/cotacoes"
SILVER_PATH = "s3a://datalake/silver/cotacoes"
CHECKPOINT_SILVER = "s3a://datalake/checkpoints/silver_cotacoes"

# Aguardar a Bronze ser inicializada (Delta precisa de metadata para o readStream)
print("Aguardando inicialização da camada Bronze...")
client = Minio(MINIO_ENDPOINT_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
while not client.bucket_exists("datalake") or len(list(client.list_objects("datalake", prefix="bronze/cotacoes/_delta_log/"))) == 0:
    time.sleep(5)
print("Camada Bronze detectada. Iniciando Silver...")

# Inicialização da Sessão Spark
spark = SparkSession.builder \
    .appName("Bronze_to_Silver_Refinement") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT_URL}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 1. Leitura em Stream da Camada Bronze (Delta Lake suporta ser fonte de stream)
df_bronze = spark.readStream \
    .format("delta") \
    .load(BRONZE_PATH)

# 2. Transformações da Camada Silver
# - Conversão de tipos (Preços para Double, Time para Timestamp)
# - Adição de coluna de data para particionamento
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
    col("regularMarketChange").cast("double").alias("change"),
    col("marketCap").cast("double"),
    "event_timestamp",
    "date",
    "ingestion_timestamp"
)

# 3. Escrita na Camada Silver
# Particionamos por 'ticker' e 'date' para acelerar consultas financeiras por ativo e período
query = df_silver.writeStream \
    .format("delta") \
    .outputMode("append") \
    .partitionBy("ticker", "date") \
    .option("checkpointLocation", CHECKPOINT_SILVER) \
    .start(SILVER_PATH)

print(f"Refinamento Silver iniciado. Gravando em: {SILVER_PATH}")
query.awaitTermination()
