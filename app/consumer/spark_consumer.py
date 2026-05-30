import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

from minio import Minio

# Configurações de conexão (Ajustadas para rodar dentro do Docker)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = "cotacoes"
MINIO_ENDPOINT_URL = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"

# Garantir que o bucket existe antes do Spark começar
try:
    client = Minio(MINIO_ENDPOINT_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    if not client.bucket_exists("datalake"):
        client.make_bucket("datalake")
        print("Bucket 'datalake' criado com sucesso.")
except Exception as e:
    print(f"Aviso ao verificar bucket: {e}")

# Caminhos no Data Lake (MinIO)
BRONZE_PATH = "s3a://datalake/bronze/cotacoes"
CHECKPOINT_PATH = "s3a://datalake/checkpoints/bronze_cotacoes"

# 1. Definição do Schema... (resto do código igual)

# 1. Definição do Schema (Estrutura da Brapi)
schema = StructType([
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

# 2. Inicialização da Sessão Spark
spark = SparkSession.builder \
    .appName("Brapi_Kafka_to_Bronze") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT_URL}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 3. Leitura do Stream do Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Transformação com captura de timestamp do Kafka
df_parsed = df_kafka.select(
    col("timestamp").alias("kafka_timestamp"),
    col("value").cast("string").alias("json_value")
).select(
    "kafka_timestamp",
    from_json(col("json_value"), schema).alias("data")
).select(
    "kafka_timestamp", 
    explode(col("data.results")).alias("quote")
).select(
    "kafka_timestamp",
    "quote.*"
).withColumn("ingestion_timestamp", current_timestamp())

# 5. Escrita no MinIO (Camada Bronze)
query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start(BRONZE_PATH)

print(f"Streaming Bronze iniciado. Gravando em: {BRONZE_PATH}")
query.awaitTermination()
