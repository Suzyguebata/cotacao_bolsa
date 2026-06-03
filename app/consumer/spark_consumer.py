import os
import sys
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minio import Minio
from consumer.transformations import transform_kafka_to_bronze

# Configurações de conexão (Ajustadas para rodar dentro do Docker)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cotacoes")
MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
DATA_LAKE_BUCKET = os.getenv("DATA_LAKE_BUCKET", "datalake")

# Caminhos no Data Lake (MinIO)
BRONZE_PATH = os.getenv("BRONZE_PATH", f"s3a://{DATA_LAKE_BUCKET}/bronze/cotacoes")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_BRONZE", f"s3a://{DATA_LAKE_BUCKET}/checkpoints/bronze_cotacoes")


def ensure_bucket():
    try:
        client = Minio(MINIO_ENDPOINT_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        if not client.bucket_exists(DATA_LAKE_BUCKET):
            client.make_bucket(DATA_LAKE_BUCKET)
            print(f"Bucket '{DATA_LAKE_BUCKET}' criado com sucesso.")
    except Exception as e:
        print(f"Aviso ao verificar bucket: {e}")


def build_spark_session():
    return SparkSession.builder \
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


def main():
    ensure_bucket()
    spark = build_spark_session()

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    df_parsed = transform_kafka_to_bronze(df_kafka)

    query = df_parsed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .start(BRONZE_PATH)

    print(f"Streaming Bronze iniciado. Gravando em: {BRONZE_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
