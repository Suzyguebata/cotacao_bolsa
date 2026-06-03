from pyspark.sql import SparkSession
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from minio import Minio
from consumer.transformations import transform_silver_to_gold

# Configurações de Caminhos (Ajustadas para Docker)
MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
DATA_LAKE_BUCKET = os.getenv("DATA_LAKE_BUCKET", "datalake")

SILVER_PATH = os.getenv("SILVER_PATH", f"s3a://{DATA_LAKE_BUCKET}/silver/cotacoes")
GOLD_PATH = os.getenv("GOLD_PATH", f"s3a://{DATA_LAKE_BUCKET}/gold/media_precos_5min")
CHECKPOINT_GOLD = os.getenv("CHECKPOINT_GOLD", f"s3a://{DATA_LAKE_BUCKET}/checkpoints/gold_cotacoes")


def wait_for_silver():
    print("Aguardando inicialização da camada Silver...")
    client = Minio(MINIO_ENDPOINT_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    while not client.bucket_exists(DATA_LAKE_BUCKET) or len(list(client.list_objects(DATA_LAKE_BUCKET, prefix="silver/cotacoes/_delta_log/"))) == 0:
        time.sleep(5)
    print("Camada Silver detectada. Iniciando Gold...")


def build_spark_session():
    return SparkSession.builder \
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


def main():
    wait_for_silver()
    spark = build_spark_session()

    df_silver = spark.readStream \
        .format("delta") \
        .load(SILVER_PATH)

    df_gold = transform_silver_to_gold(df_silver)

    query = df_gold.writeStream \
        .format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", CHECKPOINT_GOLD) \
        .start(GOLD_PATH)

    print(f"Processamento Gold iniciado. Gravando agregados em: {GOLD_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
